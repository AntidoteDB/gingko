%% -------------------------------------------------------------------
%%
%% Copyright 2020, Kevin Bartik <k_bartik12@cs.uni-kl.de>
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

-module(gingko_materializer).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get_committed_journal_entries_for_keys/2,
    materialize_snapshot/3,
    materialize_snapshot_temporarily/2]).

-spec separate_commit_from_update_journal_entries([journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries(JournalEntryList) ->
    separate_commit_from_update_journal_entries(JournalEntryList, []).
-spec separate_commit_from_update_journal_entries([journal_entry()], [journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries([CommitJournalEntry], JournalEntryList) ->
    {CommitJournalEntry, JournalEntryList};
separate_commit_from_update_journal_entries([UpdateJournalEntry | Rest], JournalEntryList) ->
    separate_commit_from_update_journal_entries(Rest, [UpdateJournalEntry | JournalEntryList]).

-spec get_committed_journal_entries_for_keys([journal_entry()], [key_struct()] | all_keys) -> [{journal_entry(), [journal_entry()]}].
%returns [{CommitJournalEntry, [UpdateJournalEntry]}]
get_committed_journal_entries_for_keys(SortedJournalEntryList, KeyStructFilter) ->
    FilteredJournalEntryList =
        lists:filter(
            fun(JournalEntry) ->
                gingko_utils:is_update_of_keys_or_commit(JournalEntry, KeyStructFilter)
            end, SortedJournalEntryList),
    TxIdToJournalEntryListDict = general_utils:group_by(fun(#journal_entry{tx_id = TxId}) -> TxId end, FilteredJournalEntryList),
    FilteredTxIdToJournalEntryListDict =
        dict:filter(
            fun(_TxId, JournalEntryList) ->
                gingko_utils:contains_journal_entry_type(JournalEntryList, commit_txn) andalso length(JournalEntryList) > 1 %Keep only transactions that were committed and have updates
            end, TxIdToJournalEntryListDict),
    SortedTxIdToJournalEntryListDict =
        dict:map(
            fun(_TxId, JournalEntryList) ->
                gingko_utils:sort_journal_entries_of_same_tx(JournalEntryList) %Last journal entry is commit then
            end, FilteredTxIdToJournalEntryListDict),
    CommitAndUpdateListTupleList =
        lists:map(
            fun({_TxId, JournalEntryList}) ->
                separate_commit_from_update_journal_entries(JournalEntryList) %{CommitJournalEntry, [UpdateJournalEntry]}
            end, dict:to_list(SortedTxIdToJournalEntryListDict)),
    SortedCommitToUpdateListTupleList = lists:sort(fun compare_commit_vts/2, CommitAndUpdateListTupleList),
    SortedCommitToUpdateListTupleList.

-spec compare_commit_vts({journal_entry(), [journal_entry()]}, {journal_entry(), [journal_entry()]}) -> boolean().
compare_commit_vts({#journal_entry{args = #commit_txn_args{commit_vts = CommitVts1}}, _JournalEntryList1}, {#journal_entry{args = #commit_txn_args{commit_vts = CommitVts2}}, _JournalEntryList2}) ->
    vectorclock:le(CommitVts1, CommitVts2).

-spec transform_to_update_payload(journal_entry(), journal_entry()) -> update_payload().
transform_to_update_payload(#journal_entry{tx_id = TxId, type = commit_txn, args = #commit_txn_args{commit_vts = CommitVts}}, #journal_entry{tx_id = TxId, type = update, args = #object_op_args{key_struct = KeyStruct, op_args = UpdateOp}}) ->
    #update_payload{
        key_struct = KeyStruct,
        update_op = UpdateOp,
        commit_vts = CommitVts,
        snapshot_vts = CommitVts,
        tx_id = TxId
    }.

-spec materialize_snapshot(snapshot(), [journal_entry()], vectorclock()) -> {ok, snapshot()} | {error, reason()}.
materialize_snapshot(Snapshot = #snapshot{key_struct = KeyStruct, snapshot_vts = SnapshotVts}, JournalEntryList, DependencyVts) ->
    ValidSnapshotTime = gingko_utils:is_in_vts_range(SnapshotVts, {none, DependencyVts}),
    case ValidSnapshotTime of
        true ->
            CommittedJournalEntryList = get_committed_journal_entries_for_keys(JournalEntryList, [KeyStruct]),
            RelevantCommittedJournalEntryList =
                lists:filter(
                    fun({#journal_entry{args = #commit_txn_args{commit_vts = CommitVts}}, _UpdateJournalEntryList}) ->
                        gingko_utils:is_in_vts_range(CommitVts, {SnapshotVts, DependencyVts})
                    end, CommittedJournalEntryList),
            UpdatePayloadListList =
                lists:map(
                    fun({CommitJournalEntry, UpdateJournalEntryList}) ->
                        lists:map(
                            fun(UpdateJournalEntry) ->
                                transform_to_update_payload(CommitJournalEntry, UpdateJournalEntry)
                            end, UpdateJournalEntryList)
                    end, RelevantCommittedJournalEntryList),
            UpdatePayloadList = lists:append(UpdatePayloadListList),
            {ok, NewSnapshot} = materialize_update_payload(Snapshot, UpdatePayloadList),
            {ok, NewSnapshot#snapshot{snapshot_vts = DependencyVts}};
        false -> {error, {"Invalid Snapshot Time", Snapshot, JournalEntryList, DependencyVts}}
    end.

-spec update_snapshot(snapshot(), downstream_op() | fun((Value :: term()) -> UpdatedValue :: term()), vectorclock(), vectorclock()) -> {ok, snapshot()} | {error, reason()}.
update_snapshot(Snapshot = #snapshot{value = SnapshotValue, key_struct = #key_struct{type = Type}}, DownstreamOp, CommitVts, SnapshotVts) ->
    IsCrdt = antidote_crdt:is_type(Type),
    case IsCrdt of
        true ->
            {ok, Value} = Type:update(DownstreamOp, SnapshotValue),
            {ok, Snapshot#snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts, value = Value}};
        false ->
            try
                {ok, Snapshot#snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts, value = DownstreamOp(SnapshotValue)}}
            catch
                _:_ ->
                    {error, {"Invalid Operation on Value", Snapshot, DownstreamOp, CommitVts, SnapshotVts}}
            end
    end.

-spec materialize_update_payload(snapshot(), [update_payload()]) -> {ok, snapshot()} | {error, reason()}.
materialize_update_payload(Snapshot, []) ->
    {ok, Snapshot};
materialize_update_payload(Snapshot, [#update_payload{update_op = DownstreamOp, commit_vts = CommitVts, snapshot_vts = SnapshotVts} | Rest]) ->
    case update_snapshot(Snapshot, DownstreamOp, CommitVts, SnapshotVts) of
        {error, Reason} ->
            {error, Reason};
        {ok, Result} ->
            materialize_update_payload(Result, Rest)
    end.

-spec materialize_snapshot_temporarily(snapshot(), [downstream_op()]) -> {ok, snapshot()} | {error, reason()}.
materialize_snapshot_temporarily(Snapshot, []) ->
    {ok, Snapshot};
materialize_snapshot_temporarily(Snapshot = #snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts}, [DownstreamOp | Rest]) ->
    case update_snapshot(Snapshot, DownstreamOp, CommitVts, SnapshotVts) of
        {error, Reason} ->
            {error, Reason};
        {ok, Result} ->
            materialize_snapshot_temporarily(Result, Rest)
    end.
