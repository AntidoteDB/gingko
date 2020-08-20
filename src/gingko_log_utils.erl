%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(gingko_log_utils).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-export([
    perform_tx_read/3,
    checkpoint/3,
    perform_journal_log_trimming/1,
    remove_unresolved_or_aborted_transactions_and_return_remaining_journal_entry_list/1,

    create_journal_mnesia_table/1,
    clear_journal_entries/1,
    add_journal_entry/2,
    add_journal_entry_list/2,
    delete_jsn_list/2,
    read_journal_entry/2,
    read_all_journal_entries/1,
    read_journal_entries_with_tx_id/2,
    read_journal_entries_with_multiple_tx_ids/2,
    read_journal_entries_with_dcid/2,
    match_journal_entries/2,

    read_all_checkpoint_entry_keys/0,
    read_checkpoint_entry/2,
    read_checkpoint_entries/2,
    add_or_update_checkpoint_entry/1,
    add_or_update_checkpoint_entries/1
]).

-dialyzer([{[no_match], read_journal_entries_with_tx_id/2}]).

-spec perform_tx_read(key_struct(), txid(), table_name()) -> {ok, snapshot()} | {error, reason()}.
perform_tx_read(KeyStruct, TxId, TableName) ->
    TxJournalEntryList = gingko_log_utils:read_journal_entries_with_tx_id(TxId, TableName),
    SortedTxJournalEntryList = gingko_utils:sort_journal_entries_of_same_tx(TxJournalEntryList),
    case SortedTxJournalEntryList of
        [] -> {error, "Tx Read cannot be performed without a running Txn!"};
        [#journal_entry{args = #begin_txn_args{dependency_vts = BeginVts}} | RestTxJournalEntryList] ->
            {ok, SnapshotBeforeTx} = gingko_dc_utils:call_gingko_sync_with_key(KeyStruct, ?GINGKO_CACHE, {get, KeyStruct, BeginVts}),
            AllUpdatesOfKey = gingko_utils:get_updates_of_key(RestTxJournalEntryList, KeyStruct),
            DownstreamOpsToBeAdded =
                lists:map(
                    fun(#journal_entry{args = #update_args{downstream_op = DownstreamOp}}) ->
                        DownstreamOp
                    end, AllUpdatesOfKey),
            {ok, gingko_materializer:materialize_tx_snapshot(SnapshotBeforeTx, DownstreamOpsToBeAdded)}
    end.

-spec checkpoint([journal_entry()], vectorclock(), #{dcid() => txn_tracking_num()}) -> ok | {error, reason()}.
checkpoint(ValidJournalEntryListBeforeCheckpoint, DependencyVts, NewCheckpointDcIdToLastTxnTrackingNumMap) ->
    CommitJournalEntries = gingko_utils:get_journal_entries_of_type(ValidJournalEntryListBeforeCheckpoint, commit_txn),
    ValidCommits =
        lists:filter(
            fun(#journal_entry{dcid = DcId, args = #commit_txn_args{txn_tracking_num = {TxnNum, _, _}}}) ->
                {LastTxnOrderNum, _, _} =
                    case maps:find(DcId, NewCheckpointDcIdToLastTxnTrackingNumMap) of
                        {ok, LastTxnNum} -> LastTxnNum;
                        error -> gingko_utils:get_default_txn_tracking_num()
                    end,
                TxnNum =< LastTxnOrderNum
            end, CommitJournalEntries),
    ValidTxns = sets:from_list(lists:map(fun(#journal_entry{tx_id = TxId}) -> TxId end, ValidCommits)),
    UpdatedKeyStructs =
        lists:filtermap(
            fun(#journal_entry{tx_id = TxId, args = Args}) ->
                case Args of
                    #update_args{key_struct = KeyStruct} ->
                        case sets:is_element(TxId, ValidTxns) of
                            true -> {true, KeyStruct};
                            false -> false
                        end;
                    _ -> false
                end
            end, ValidJournalEntryListBeforeCheckpoint),
    SnapshotsToStore =
        lists:map(
            fun(KeyStruct) ->
                {ok, Snapshot} = gingko_dc_utils:call_gingko_sync_with_key(KeyStruct, ?GINGKO_CACHE, {get, KeyStruct, DependencyVts, ValidJournalEntryListBeforeCheckpoint}),
                Snapshot
            end, UpdatedKeyStructs),
    gingko_log_utils:add_or_update_checkpoint_entries(SnapshotsToStore).

-spec perform_journal_log_trimming(table_name()) -> ok.
perform_journal_log_trimming(TableName) ->
    AllJournalEntries = gingko_log_utils:read_all_journal_entries(TableName),
    AllCommittedCheckpoints = gingko_utils:get_journal_entries_of_type(AllJournalEntries, checkpoint_commit),
    SortedByVts = gingko_utils:sort_same_journal_entry_type_list_by_vts(AllCommittedCheckpoints),
    case lists:reverse(SortedByVts) of
        [] -> ok;
        [_] -> ok;
        [#journal_entry{args = #checkpoint_args{dependency_vts = CheckpointVts1}}, #journal_entry{args = #checkpoint_args{dependency_vts = CheckpointVts2}} | _] ->
            JournalEntriesByTxId = maps:values(general_utils:group_by_map(fun(#journal_entry{tx_id = TxId}) ->
                TxId end, AllJournalEntries)),
            RelevantCheckpointVts = case gingko_env_utils:get_journal_trimming_mode() of
                                        keep_two_checkpoints -> CheckpointVts2;
                                        _ -> CheckpointVts1
                                    end,
            JsnListToRemove =
                lists:append(lists:filtermap(
                    fun(JournalEntryList) ->
                        SortedTxnJournalEntryList = gingko_utils:sort_journal_entries_of_same_tx(JournalEntryList),
                        LastJournalEntryOfTxn = lists:last(SortedTxnJournalEntryList),
                        Result =
                            case LastJournalEntryOfTxn of
                                #journal_entry{type = abort_txn} -> true; %%Aborts can be always trimmed
                                #journal_entry{args = #commit_txn_args{commit_vts = CommitVts}} ->
                                    vectorclock:lt(CommitVts, RelevantCheckpointVts);
                                #journal_entry{args = #checkpoint_args{dependency_vts = CheckpointVts}} ->
                                    vectorclock:lt(CheckpointVts, RelevantCheckpointVts);
                                _ -> false %%Unfinished transactions are not trimmed
                            end,
                        case Result of
                            false -> false;
                            true -> {true, lists:map(fun(#journal_entry{jsn = Jsn}) -> Jsn end, JournalEntryList)}
                        end
                    end, JournalEntriesByTxId)),
            gingko_log_utils:delete_jsn_list(JsnListToRemove, TableName)
    end.

%%TODO Aborted can be kept maybe but are not necessary
-spec remove_unresolved_or_aborted_transactions_and_return_remaining_journal_entry_list(table_name()) -> [journal_entry()].
remove_unresolved_or_aborted_transactions_and_return_remaining_journal_entry_list(TableName) ->
    JournalEntryList = read_all_journal_entries(TableName),
    TxJournalEntryListList = general_utils:group_by_only_values(fun(#journal_entry{tx_id = TxId}) ->
        TxId end, JournalEntryList),
    {JsnListToRemove, RemainingJournalEntryList} =
        lists:foldl(
            fun(TxJournalEntryList, {CurrentJsnListToRemove, CurrentRemainingJournalEntryList}) ->
                SortedTxnJournalEntryList = gingko_utils:sort_journal_entries_of_same_tx(TxJournalEntryList),
                LastJournalEntryOfTxn = lists:last(SortedTxnJournalEntryList),
                ToBeRemoved =
                    case LastJournalEntryOfTxn of
                        #journal_entry{type = abort_txn} -> true; %%Aborts can be always trimmed
                        #journal_entry{args = #commit_txn_args{}} -> false;
                        #journal_entry{args = #checkpoint_args{}} -> false;
                        _ -> true
                    end,
                case ToBeRemoved of
                    true ->
                        {lists:map(
                            fun(#journal_entry{jsn = Jsn}) ->
                                Jsn end, SortedTxnJournalEntryList) ++ CurrentJsnListToRemove, CurrentRemainingJournalEntryList};
                    false -> {CurrentJsnListToRemove, SortedTxnJournalEntryList ++ CurrentRemainingJournalEntryList}
                end
            end, {[], []}, TxJournalEntryListList),
    gingko_log_utils:delete_jsn_list(JsnListToRemove, TableName),
    RemainingJournalEntryList.

-spec create_journal_mnesia_table(table_name()) -> ok | {error, reason()}.
create_journal_mnesia_table(TableName) ->
    mnesia_utils:get_mnesia_result(
        mnesia:create_table(TableName,
            [{attributes, record_info(fields, journal_entry)},
                {index, [#journal_entry.tx_id]}, %%TODO maybe index of dcid and type (this may be too slow though)
                {disc_copies, [node()]}, %%TODO instead of index we could use ets calls which are very fast
                {record_name, journal_entry},
                {local_content, true}
            ])).

-spec clear_journal_entries(table_name()) -> ok | {error, reason()}.
clear_journal_entries(TableName) ->
    mnesia_utils:get_mnesia_result(mnesia:clear_table(TableName)).

-spec add_journal_entry(journal_entry(), table_name()) -> ok.
add_journal_entry(JournalEntry, TableName) ->
    F = fun() -> mnesia:write(TableName, JournalEntry, write) end,
    mnesia_utils:run_transaction(F).
    %%mnesia:dirty_write(TableName, JournalEntry).

-spec add_journal_entry_list([journal_entry()], table_name()) -> ok.
add_journal_entry_list(JournalEntryList, TableName) ->
    F = fun() -> lists:foreach(fun(JournalEntry) -> mnesia:write(TableName, JournalEntry, write) end, JournalEntryList) end,
    mnesia_utils:run_transaction(F).

-spec delete_jsn_list([jsn()], table_name()) -> ok.
delete_jsn_list(JsnList, TableName) ->
    lists:foreach(fun(Jsn) -> mnesia:dirty_delete({TableName, Jsn}) end, JsnList).

-spec read_journal_entry(jsn(), table_name()) -> journal_entry() | {error, not_found}.
read_journal_entry(Jsn, TableName) ->
    JournalEntryResult = mnesia:dirty_read(TableName, Jsn),
    case JournalEntryResult of
        [] -> {error, not_found};
        [JournalEntry] -> JournalEntry
    end.

-spec read_all_journal_entries(table_name()) -> [journal_entry()].
read_all_journal_entries(TableName) ->
    JournalEntryPattern = mnesia:table_info(TableName, wild_pattern),
    mnesia:dirty_match_object(TableName, JournalEntryPattern).

-spec read_journal_entries_with_tx_id(txid(), table_name()) -> [journal_entry()].
read_journal_entries_with_tx_id(TxId, TableName) ->
    mnesia:dirty_index_read(TableName, TxId, #journal_entry.tx_id).

-spec read_journal_entries_with_multiple_tx_ids([txid()], table_name()) -> [journal_entry()].
read_journal_entries_with_multiple_tx_ids(TxIdList, TableName) ->
    lists:append(
        lists:map(
            fun(TxId) ->
                read_journal_entries_with_tx_id(TxId, TableName)
            end, TxIdList)).

-spec read_journal_entries_with_dcid(dcid(), table_name()) -> [journal_entry()].
read_journal_entries_with_dcid(DcId, TableName) ->
    JournalEntryPattern = mnesia:table_info(TableName, wild_pattern),
    match_journal_entries(JournalEntryPattern#journal_entry{dcid = DcId}, TableName).

-spec match_journal_entries(journal_entry() | term(), table_name()) -> [journal_entry()].
match_journal_entries(MatchJournalEntry, TableName) ->
    mnesia:dirty_match_object(TableName, MatchJournalEntry).

-spec read_all_checkpoint_entry_keys() -> [key_struct()].
read_all_checkpoint_entry_keys() ->
    mnesia:dirty_all_keys(checkpoint_entry).

-spec read_checkpoint_entry(key_struct(), vectorclock()) -> snapshot().
read_checkpoint_entry(KeyStruct, DependencyVts) ->
    ReadCheckpointResult = mnesia:dirty_read(checkpoint_entry, KeyStruct),
    case ReadCheckpointResult of
        [] -> gingko_utils:create_new_snapshot(KeyStruct, DependencyVts);
        [CheckpointEntry] -> gingko_utils:create_snapshot_from_checkpoint_entry(CheckpointEntry, DependencyVts)
    end.

-spec read_checkpoint_entries([key_struct()], vectorclock()) -> [snapshot()].
read_checkpoint_entries(KeyStructList, DependencyVts) ->
    lists:map(
        fun(KeyStruct) ->
            read_checkpoint_entry(KeyStruct, DependencyVts)
        end, KeyStructList).

%%TODO consider sync_transactions for checkpoint writes
-spec add_or_update_checkpoint_entry(snapshot()) -> ok | {error, reason()}.
add_or_update_checkpoint_entry(Snapshot) ->
    add_or_update_checkpoint_entries([Snapshot]).

-spec add_or_update_checkpoint_entries([snapshot()]) -> ok | {error, reason()}.
add_or_update_checkpoint_entries(SnapshotList) ->
    F =
        fun() ->
            lists:foreach(
                fun(#snapshot{key_struct = KeyStruct, value = Value}) ->
                    mnesia:write(#checkpoint_entry{key_struct = KeyStruct, value = Value})
                end, SnapshotList)
        end,
    mnesia_utils:run_transaction(F).
