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
    persist_journal_entries/1,
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
    case TxJournalEntryList of
        {error, Reason} -> {error, Reason};
        _ ->
            SortedTxJournalEntryList = gingko_utils:sort_journal_entries_of_same_tx(TxJournalEntryList),
            case SortedTxJournalEntryList of
                [] -> {error, "Tx Read cannot be performed without a running Txn!"};
                [#journal_entry{args = #begin_txn_args{dependency_vts = BeginVts}} | RestTxJournalEntryList] ->
                    {ok, SnapshotBeforeTx} = gingko_utils:call_gingko_sync_with_key(KeyStruct, ?GINGKO_CACHE, {get, KeyStruct, BeginVts}),
                    AllUpdatesOfKey = gingko_utils:get_updates_of_key(RestTxJournalEntryList, KeyStruct),
                    DownstreamOpsToBeAdded =
                        lists:map(
                            fun(#journal_entry{args = #update_args{downstream_op = DownstreamOp}}) ->
                                DownstreamOp
                            end, AllUpdatesOfKey),
                    {ok, gingko_materializer:materialize_tx_snapshot(SnapshotBeforeTx, DownstreamOpsToBeAdded)}
            end
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
                {ok, Snapshot} = gingko_utils:call_gingko_sync_with_key(KeyStruct, ?GINGKO_CACHE, {get, KeyStruct, DependencyVts, ValidJournalEntryListBeforeCheckpoint}),
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
            RelevantCheckpointVts = case ?JOURNAL_TRIMMING_MODE of
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
            gingko_log_utils:delete_jsn_list(JsnListToRemove, TableName),
            gingko_log_utils:persist_journal_entries(TableName)
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
                            fun(#journal_entry{jsn = Jsn}) -> Jsn end, SortedTxnJournalEntryList) ++ CurrentJsnListToRemove, CurrentRemainingJournalEntryList};
                    false -> {CurrentJsnListToRemove, SortedTxnJournalEntryList ++ CurrentRemainingJournalEntryList}
                end
            end, {[], []}, TxJournalEntryListList),
    gingko_log_utils:delete_jsn_list(JsnListToRemove, TableName),
    gingko_log_utils:persist_journal_entries(TableName),
    RemainingJournalEntryList.

-spec create_journal_mnesia_table(table_name()) -> ok | {error, reason()}.
create_journal_mnesia_table(TableName) ->
    mnesia_utils:get_mnesia_result(mnesia:create_table(TableName,
        [{attributes, record_info(fields, journal_entry)},
            {index, [#journal_entry.tx_id]}, %%TODO maybe index of dcid and type (this may be too slow though)
            {ram_copies, [node()]}, %%TODO instead of index we could use ets calls which are very fast
            {record_name, journal_entry},
            {local_content, true}
        ])).

-spec persist_journal_entries(table_name()) -> ok | {error, reason()}.
persist_journal_entries(TableName) ->
    mnesia_utils:get_mnesia_result(mnesia:dump_tables([TableName])).

-spec clear_journal_entries(table_name()) -> ok | {error, reason()}.
clear_journal_entries(TableName) ->
    ClearTableResult = mnesia_utils:get_mnesia_result(mnesia:clear_table(TableName)),
    case ClearTableResult of
        ok -> persist_journal_entries(TableName);
        Error -> Error
    end.

-spec add_journal_entry(journal_entry(), table_name()) -> ok | {error, reason()}.
add_journal_entry(JournalEntry = #journal_entry{jsn = Jsn}, TableName) ->
    F =
        fun() ->
            case mnesia:read(TableName, Jsn) of
                [] -> mnesia:write(TableName, JournalEntry, write);
                ExistingJournalEntryList -> {error, {already_exists, ExistingJournalEntryList}}
            end
        end,
    mnesia_utils:run_transaction(F).

-spec add_journal_entry_list([journal_entry()], table_name()) -> ok | {error, reason()}.
add_journal_entry_list(JournalEntryList, TableName) ->
    F =
        fun() ->
            lists:foldl(
                fun(JournalEntry = #journal_entry{jsn = Jsn}, Result) ->
                    case mnesia:read(TableName, Jsn) of
                        [] ->
                            mnesia:write(TableName, JournalEntry, write),
                            Result;
                        ExistingJournalEntryList ->
                            {error, {already_exists, ExistingJournalEntryList}}
                    end
                end, ok, JournalEntryList)
        end,
    mnesia_utils:run_transaction(F).

-spec delete_jsn_list([jsn()], table_name()) -> ok | {error, reason()}.
delete_jsn_list(JsnList, TableName) ->
    F =
        fun() ->
            lists:foreach(fun(Jsn) -> mnesia:delete({TableName, Jsn}) end, JsnList)
        end,
    mnesia_utils:run_transaction(F).

-spec read_journal_entry(jsn(), table_name()) -> journal_entry() | {error, reason()}.
read_journal_entry(Jsn, TableName) ->
    F = fun() -> mnesia:read(TableName, Jsn) end,
    ReadJournalEntryResult = mnesia_utils:run_transaction(F),
    case ReadJournalEntryResult of
        {error, Reason} -> {error, Reason};
        [] -> {error, {"No Journal Entry found", Jsn}};
        [JournalEntry] -> JournalEntry;
        JournalEntryList -> {error, {"Multiple Journal Entries found", Jsn, JournalEntryList}}
    end.

-spec read_all_journal_entries(table_name()) -> [journal_entry()] | {error, reason()}.
read_all_journal_entries(TableName) ->
    F = fun() ->
        mnesia:foldl(fun(JournalEntry, JournalEntryAcc) -> [JournalEntry | JournalEntryAcc] end, [], TableName) end,
    mnesia_utils:run_transaction(F).

-spec read_journal_entries_with_tx_id(txid(), table_name()) -> [journal_entry()] | {error, reason()}.
read_journal_entries_with_tx_id(TxId, TableName) ->
    F = fun() -> mnesia:index_read(TableName, TxId, #journal_entry.tx_id) end,
    mnesia_utils:run_transaction(F).

-spec read_journal_entries_with_multiple_tx_ids([txid()], table_name()) -> [journal_entry()] | {error, reason()}.
read_journal_entries_with_multiple_tx_ids(TxIdList, TableName) ->
    F = fun() -> lists:append(lists:map(fun(TxId) ->
        mnesia:index_read(TableName, TxId, #journal_entry.tx_id) end, TxIdList)) end,
    mnesia_utils:run_transaction(F).

-spec read_journal_entries_with_dcid(dcid(), table_name()) -> [journal_entry()] | {error, reason()}.
read_journal_entries_with_dcid(DcId, TableName) ->
    JournalEntryPattern = mnesia:table_info(TableName, wild_pattern),
    match_journal_entries(JournalEntryPattern#journal_entry{dcid = DcId}, TableName).

-spec match_journal_entries(journal_entry() | term(), table_name()) -> [journal_entry()] | {error, reason()}.
match_journal_entries(MatchJournalEntry, TableName) ->
    F = fun() -> mnesia:match_object(TableName, MatchJournalEntry, read) end,
    mnesia_utils:run_transaction(F).

-spec read_all_checkpoint_entry_keys() -> [key_struct()] | {error, reason()}.
read_all_checkpoint_entry_keys() ->
    F = fun() -> mnesia:foldl(fun(#checkpoint_entry{key_struct = KeyStruct}, Acc) ->
        [KeyStruct | Acc] end, [], checkpoint_entry) end,
    mnesia_utils:run_transaction(F).

-spec read_checkpoint_entry(key_struct(), vectorclock()) -> snapshot() | {error, reason()}.
read_checkpoint_entry(KeyStruct, DependencyVts) ->
    F = fun() -> mnesia:read(checkpoint_entry, KeyStruct) end,
    ReadCheckpointResult = mnesia_utils:run_transaction(F),
    case ReadCheckpointResult of
        {error, Reason} -> {error, Reason};
        [] -> gingko_utils:create_new_snapshot(KeyStruct, DependencyVts);
        [CheckpointEntry] -> gingko_utils:create_snapshot_from_checkpoint_entry(CheckpointEntry, DependencyVts);
        CheckpointEntryList -> {error, {"Multiple Snapshots found", KeyStruct, CheckpointEntryList}}
    end.

%TODO testing necessary
-spec read_checkpoint_entries([key_struct()], vectorclock()) -> [snapshot()] | {error, reason()}.
read_checkpoint_entries(KeyStructList, DependencyVts) ->
    F = fun() ->
        lists:map(
            fun(KeyStruct) ->
                case mnesia:read(checkpoint_entry, KeyStruct) of
                    [] ->
                        gingko_utils:create_new_snapshot(KeyStruct, DependencyVts);
                    [CheckpointEntry] ->
                        gingko_utils:create_snapshot_from_checkpoint_entry(CheckpointEntry, DependencyVts);
                    CheckpointEntryList ->
                        {error, {"Multiple snapshots for one key found", KeyStruct, CheckpointEntryList}}
                end
            end, KeyStructList)
        end,
    ReadCheckpointEntryListResult = mnesia_utils:run_transaction(F),
    AnyErrors =
        lists:any(
            fun(CheckpointEntryResult) ->
                case CheckpointEntryResult of
                    {error, _} -> true;
                    _ -> false
                end
            end, ReadCheckpointEntryListResult),
    case AnyErrors of
        true -> {error, {"One or more snapshot keys caused an error", ReadCheckpointEntryListResult}};
        false -> ReadCheckpointEntryListResult
    end.

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
