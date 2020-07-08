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

    create_journal_mnesia_table/1,
    persist_journal_entries/1,
    clear_journal_entries/1,
    add_journal_entry/2,
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
                [] -> {ok, gingko_utils:create_new_snapshot(KeyStruct, vectorclock:new())};
                [#journal_entry{args = #begin_txn_args{dependency_vts = BeginVts}} | _] ->
                    #journal_entry{args = #begin_txn_args{dependency_vts = BeginVts}} = hd(SortedTxJournalEntryList), %%Crash if this is not true
                    {ok, SnapshotBeforeTx} = gingko_utils:call_gingko_sync_with_key(KeyStruct, ?GINGKO_CACHE, {get, KeyStruct, BeginVts}),
                    DownstreamOpsToBeAdded =
                        lists:filtermap(
                            fun(JournalEntry = #journal_entry{args = Args}) ->
                                case gingko_utils:is_update_of_keys(JournalEntry, [KeyStruct]) of
                                    true -> {true, Args#update_args.downstream_op};
                                    false -> false
                                end
                            end, SortedTxJournalEntryList),
                    gingko_materializer:materialize_tx_snapshot(SnapshotBeforeTx, DownstreamOpsToBeAdded)
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
%TODO cache cleanup
%%TODO txn tracking cleanup

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
