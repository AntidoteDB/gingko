%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(gingko_materializer).
-include("gingko.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([materialize_snapshot/3, materialize_snapshot_temporarily/2, materialize_multiple_snapshots/3, get_committed_journal_entries_for_keys/2]).


-spec separate_commit_from_update_journal_entries([journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries(JList) ->
  separate_commit_from_update_journal_entries(JList, []).
-spec separate_commit_from_update_journal_entries([journal_entry()], [journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries([CommitJournalEntry], JournalEntries) ->
  {CommitJournalEntry, JournalEntries};
separate_commit_from_update_journal_entries([UpdateJournalEntry | OtherJournalEntries], JournalEntries) ->
  separate_commit_from_update_journal_entries(OtherJournalEntries, [UpdateJournalEntry | JournalEntries]).

-spec get_committed_journal_entries_for_keys([journal_entry()], [key_struct()] | all_keys) -> [{journal_entry(), [journal_entry()]}].
%returns [{CommitJournalEntry, [UpdateJournalEntry]}]
get_committed_journal_entries_for_keys(SortedJournalEntries, KeyStructFilter) ->
  FilteredJournalEntries =
    lists:filter(
      fun(J) ->
        gingko_utils:is_update_of_keys_or_commit(J, KeyStructFilter)
      end, SortedJournalEntries),
  TxIdToJournalEntries = general_utils:group_by(fun(J) -> J#journal_entry.tx_id end, FilteredJournalEntries),
  FilteredTxIdToJournalEntries =
    dict:filter(
      fun(_TxId, JList) ->
        gingko_utils:contains_system_operation(JList, commit_txn) andalso length(JList) > 1 %Keep only transactions that were committed and have updates
      end, TxIdToJournalEntries),
  SortedTxIdToJournalEntries =
    dict:map(
      fun(_TxId, JList) ->
        gingko_utils:sort_by_jsn_number(JList) %Last journal entry is commit then
      end, FilteredTxIdToJournalEntries),
  ListOfCommitToUpdateListTuples =
    lists:map(
      fun({_TxId, JList}) ->
        separate_commit_from_update_journal_entries(JList) %{CommitJournalEntry, [UpdateJournalEntry]}
      end, dict:to_list(SortedTxIdToJournalEntries)),
  SortedListOfCommitToUpdateListTuples =
    lists:sort(
      fun({J1, _JList1}, {J2, _JList2}) ->
        J1#journal_entry.jsn < J2#journal_entry.jsn
      end, ListOfCommitToUpdateListTuples),
  SortedListOfCommitToUpdateListTuples.

-spec transform_to_update_payload(journal_entry(), journal_entry()) -> update_payload().
transform_to_update_payload(CommitJournalEntry, JournalEntry) ->
  #update_payload{
    key_struct = JournalEntry#journal_entry.operation#object_operation.key_struct,
    update_op = JournalEntry#journal_entry.operation#object_operation.op_args,
    snapshot_vts = CommitJournalEntry#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_vts,
    commit_vts = CommitJournalEntry#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_vts,
    tx_id = CommitJournalEntry#journal_entry.tx_id
  }.

-spec materialize_snapshot(snapshot(), [journal_entry()], vectorclock()) -> {ok, snapshot()} | {error, reason()}.
materialize_snapshot(Snapshot, SortedJournalEntries, DependencyVts) ->
  SnapshotVts = Snapshot#snapshot.snapshot_vts,
  ValidSnapshotTime = gingko_utils:is_in_vts_range(SnapshotVts, {none, DependencyVts}),
  case ValidSnapshotTime of
    true ->
      CommittedJournalEntries = get_committed_journal_entries_for_keys(SortedJournalEntries, [Snapshot#snapshot.key_struct]),
      RelevantCommittedJournalEntries =
        lists:filter(
          fun({CommitJ, _UpdateJList}) ->
            CommitVts1 = CommitJ#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_vts,
            gingko_utils:is_in_vts_range(CommitVts1, {SnapshotVts, DependencyVts})
          end, CommittedJournalEntries),
      UpdatePayloads =
        lists:map(
          fun({CommitJ, UpdateJList}) ->
            lists:map(
              fun(UpdateJ) -> transform_to_update_payload(CommitJ, UpdateJ) end, UpdateJList)
          end, RelevantCommittedJournalEntries),
      FlattenedUpdatePayloads = lists:flatten(UpdatePayloads),
      {ok, NewSnapshot} = materialize_update_payload(Snapshot, FlattenedUpdatePayloads),
      ValidSnapshotVts = get_latest_valid_snapshot_vts(SortedJournalEntries, CommittedJournalEntries, DependencyVts), %Optimization to set the latest SnapshotVts to avoid later reloads
      {ok, NewSnapshot#snapshot{snapshot_vts = ValidSnapshotVts}};
    false -> {error, {"Invalid Snapshot Time", Snapshot, SortedJournalEntries, DependencyVts}}
  end.

%TODO think about removing this method because it is complex to implement correctly
-spec materialize_multiple_snapshots([snapshot()], [journal_entry()], vectorclock()) -> {ok, [snapshot()]} | {error, reason()}.
materialize_multiple_snapshots(Snapshots, SortedJournalEntries, DependencyVts) ->
  KeyStructsToSnapshots = dict:from_list(lists:map(fun(S) -> {S#snapshot.key_struct, S} end, Snapshots)),
  ValidSnapshotTime = lists:all(fun(S) ->
    gingko_utils:is_in_vts_range(S#snapshot.snapshot_vts, {none, DependencyVts}) end, Snapshots),
  case ValidSnapshotTime of
    true ->
      CommittedJournalEntries = get_committed_journal_entries_for_keys(SortedJournalEntries, dict:fetch_keys(KeyStructsToSnapshots)),
      RelevantCommittedJournalEntries =
        lists:filter(
          fun({CommitJ, UpdateJList}) ->
            KeysInUpdates = sets:from_list(lists:map(fun(UpdateJ) ->
              UpdateJ#journal_entry.operation#object_operation.key_struct end, UpdateJList)),
            SnapshotVtsToCheck = lists:map(fun(K) -> dict:fetch(K, KeyStructsToSnapshots) end, KeysInUpdates),
            CommitVts1 = CommitJ#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_vts,
            lists:all(fun(SnapshotVts) ->
              gingko_utils:is_in_vts_range(CommitVts1, {SnapshotVts, DependencyVts}) end, SnapshotVtsToCheck)
          end, CommittedJournalEntries),
      UpdatePayloads =
        lists:map(
          fun({CommitJ, UpdateJList}) ->
            lists:map(fun(UpdateJ) -> transform_to_update_payload(CommitJ, UpdateJ) end, UpdateJList)
          end, RelevantCommittedJournalEntries),
      FlattenedUpdatePayloads = lists:flatten(UpdatePayloads),
      %TODO there might be keys that are valid longer
      ValidSnapshotVts = get_latest_valid_snapshot_vts(SortedJournalEntries, CommittedJournalEntries, DependencyVts),
      KeyToUpdatesDict = general_utils:group_by(fun(U) -> U#update_payload.key_struct end, FlattenedUpdatePayloads),
      %TODO watch out for dict errors (check again that all keys are present)
      Results =
        lists:map(
          fun(S) ->
            materialize_update_payload(S#snapshot.value, dict:fetch(S#snapshot.key_struct, KeyToUpdatesDict))
          end, Snapshots),
      {ok, lists:map(fun({ok, SV}) -> SV#snapshot{snapshot_vts = ValidSnapshotVts} end, Results)}; %TODO check for crashes
    false -> ok
  end.

-spec get_latest_valid_snapshot_vts([journal_entry()], [{journal_entry(), [journal_entry()]}], vectorclock()) -> vectorclock().
get_latest_valid_snapshot_vts(SortedJournalEntries, CommittedJournalEntries, DependencyVts) ->
  CommitsLaterThanDependencyVts =
    lists:filtermap(
      fun({CommitJ, _UpdateJList}) ->
        CommitVts2 = CommitJ#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_vts,
        case gingko_utils:is_in_vts_range(CommitVts2, {DependencyVts, none}) of
          true -> {true, CommitJ};
          false -> false
        end
      end, CommittedJournalEntries),
  ValidSnapshotVts =
    case CommitsLaterThanDependencyVts of
      [] ->
        gingko_utils:get_latest_vts(SortedJournalEntries);
      CommitList ->
        FirstCommitLaterThanMaxVts = hd(CommitList),
        RelevantSortedJournalEntries =
          lists:takewhile(
            fun(J) ->
              J#journal_entry.jsn < FirstCommitLaterThanMaxVts#journal_entry.jsn
            end, SortedJournalEntries),
        gingko_utils:get_latest_vts(RelevantSortedJournalEntries)
    end,
  ValidSnapshotVts.

-spec update_snapshot(snapshot(), downstream_op() | fun((Value :: term()) -> UpdatedValue :: term()), vectorclock(), vectorclock()) -> {ok, snapshot()} | {error, reason()}.
update_snapshot(Snapshot, Op, CommitVts, SnapshotVts) ->
  SnapshotValue = Snapshot#snapshot.value,
  Type = Snapshot#snapshot.key_struct#key_struct.type,
  IsCrdt = antidote_crdt:is_type(Type),
  case IsCrdt of
    true ->
      {ok, Value} = Type:update(Op, SnapshotValue),
      {ok, Snapshot#snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts, value = Value}};
    false ->
      try
        {ok, Snapshot#snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts, value = Op(SnapshotValue)}}
      catch
        _:_ ->
          {error, {"Invalid Operation on Value", Snapshot, Op, CommitVts, SnapshotVts}}
      end
  end.

-spec materialize_update_payload(snapshot(), [update_payload()]) -> {ok, snapshot()} | {error, reason()}.
materialize_update_payload(Snapshot, []) ->
  {ok, Snapshot};
materialize_update_payload(Snapshot, [UpdatePayload | Rest]) ->
  CommitVts = UpdatePayload#update_payload.commit_vts, %%TODO make sure they are ordered correctly
  SnapshotVts = UpdatePayload#update_payload.snapshot_vts,
  DownstreamOp = UpdatePayload#update_payload.update_op,
  case update_snapshot(Snapshot, DownstreamOp, CommitVts, SnapshotVts) of
    {error, Reason} ->
      {error, Reason};
    {ok, Result} ->
      materialize_update_payload(Result, Rest)
  end.

-spec materialize_snapshot_temporarily(snapshot(), [journal_entry()]) -> {ok, snapshot()} | {error, reason()}.
materialize_snapshot_temporarily(Snapshot, []) ->
  {ok, Snapshot};
materialize_snapshot_temporarily(Snapshot, [JournalEntry | Rest]) ->
  DownstreamOp = JournalEntry#journal_entry.operation#object_operation.op_args,
  case update_snapshot(Snapshot, DownstreamOp, Snapshot#snapshot.commit_vts, Snapshot#snapshot.snapshot_vts) of
    {error, Reason} ->
      {error, Reason};
    {ok, Result} ->
      materialize_snapshot_temporarily(Result, Rest)
  end.