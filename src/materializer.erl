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

-module(materializer).
-include("gingko.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
  create_snapshot/1,
  filter_relevant_journal_entries/2,
  update_snapshot/4,
  materialize_snapshot/2,
  materialize_snapshot/3, materialize_snapshot_temporarily/2, get_committed_journal_entries_for_key/2, materialize_multiple_snapshots/2]
).


-spec is_journal_entry_update_and_contains_key([key_struct()] | all_keys, journal_entry()) -> boolean().
is_journal_entry_update_and_contains_key(KeyStructFilter, JournalEntry) ->
  Operation = JournalEntry#journal_entry.operation,
  case is_record(Operation, object_operation) of
    true ->
      Operation#object_operation.op_type == update andalso (KeyStructFilter == all_keys orelse lists:member(Operation#object_operation.key_struct, KeyStructFilter));
    false -> false
  end.

-spec contains_journal_entry_of_specific_system_operation(system_operation_type(), [journal_entry()]) -> boolean().
contains_journal_entry_of_specific_system_operation(SystemOperationType, JournalEntries) ->
  lists:any(fun(J) ->
    is_journal_entry_of_specific_system_operation(SystemOperationType, J) end, JournalEntries).

-spec is_journal_entry_of_specific_system_operation(system_operation_type(), journal_entry()) -> boolean().
is_journal_entry_of_specific_system_operation(SystemOperationType, JournalEntry) ->
  Operation = JournalEntry#journal_entry.operation,
  case is_record(Operation, system_operation) of
    true -> Operation#system_operation.op_type == SystemOperationType;
    false -> false
  end.

-spec filter_relevant_journal_entries([key_struct()] | all_keys, journal_entry()) -> boolean().
filter_relevant_journal_entries(KeyStructFilter, JournalEntry) ->
  is_journal_entry_update_and_contains_key(KeyStructFilter, JournalEntry) orelse is_journal_entry_of_specific_system_operation(commit_txn, JournalEntry).

-spec separate_commit_from_update_journal_entries([journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries(JList) ->
  separate_commit_from_update_journal_entries(JList, []).
-spec separate_commit_from_update_journal_entries([journal_entry()], [journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries([CommitJournalEntry], JournalEntries) ->
  {CommitJournalEntry, JournalEntries};
separate_commit_from_update_journal_entries([UpdateJournalEntry | OtherJournalEntries], JournalEntries) ->
  separate_commit_from_update_journal_entries(OtherJournalEntries, [UpdateJournalEntry | JournalEntries]).

-spec get_committed_journal_entries_for_key([key_struct()] | all_keys, [journal_entry()]) -> [{journal_entry(), [journal_entry()]}].
get_committed_journal_entries_for_key(KeyStructFilter, JournalEntries) ->
  SortedJournalEntries = lists:sort(fun(J1, J2) ->
    gingko_utils:get_jsn_number(J1) < gingko_utils:get_jsn_number(J2) end, JournalEntries),
  TxIdToJournalEntries = gingko_utils:group_by(fun(J) -> J#journal_entry.tx_id end, SortedJournalEntries),
  FilteredTxIdToJournalEntries = dict:filter(fun(_TxId, JList) ->
    lists:any(fun(J) -> filter_relevant_journal_entries(KeyStructFilter, J) end, JList) end, TxIdToJournalEntries),
  SortedTxIdToJournalEntries = dict:map(fun(_TxId, JList) -> lists:sort(fun(J1, J2) ->
    gingko_utils:get_jsn_number(J1) < gingko_utils:get_jsn_number(J2) end, lists:filter(fun(J) -> filter_relevant_journal_entries(KeyStructFilter, J) end, JList)) end, FilteredTxIdToJournalEntries),
  ListOfCommitToUpdateListTuples = lists:map(fun({_TxId, JList}) ->
    separate_commit_from_update_journal_entries(JList) end, dict:to_list(SortedTxIdToJournalEntries)),

  SortedListOfCommitToUpdateListTuples = lists:sort(fun({J1, _JList1}, {J2, _JList2}) ->
    gingko_utils:get_jsn_number(J1) < gingko_utils:get_jsn_number(J2) end, ListOfCommitToUpdateListTuples),
  logger:error("SortedListOfCommitToUpdateListTuples: ~ts",[SortedListOfCommitToUpdateListTuples]),
  SortedListOfCommitToUpdateListTuples.

%%get_updated_values_for_checkpoint(CheckpointJournalEntry) ->
%%  %TODO safety for failures
%%  DependencyVts = CheckpointJournalEntry#journal_entry.operation#system_operation.op_args#checkpoint_args.dependency_vts,
%%  %TODO does it make sense to store all updated values in the cache
%%  %TODO only get till vorvorgehenden checkpoint of the journal (optimization later)
%%
%%  AllJournalEntries = gingko_log:read_all_journal_entries(),
%%  AllJournalEntries = lists:sort(fun(J1, J2) ->
%%    gingko_utils:get_jsn_number(J1) < gingko_utils:get_jsn_number(J2) end, AllJournalEntries),
%%  %TODO optimize (e.g. work with reversed list)
%%  LastCheckpoint = lists:search(fun(J) ->
%%    gingko_utils:is_system_operation(J, checkpoint) end, lists:reverse(AllJournalEntries)),
%%  LowestJsnNumber = case LastCheckpoint of
%%                      false -> 0;
%%                      {value, J} -> gingko_utils:get_jsn_number(J)
%%
%%                    end,
%%  CurrentTxBeginJournalEntry = lists:search(fun(J) ->
%%    gingko_utils:is_system_operation(J, begin_txn) andalso J#journal_entry.tx_id == JournalEntry#journal_entry.tx_id end, AllJournalEntries),
%%  HighestJsnNumber = case CurrentTxBeginJournalEntry of
%%                       false -> 0; %TODO big error
%%                       {value, J} -> gingko_utils:get_jsn_number(J)
%%                     end,
%%  CommitsInBetween = lists:filter(fun(J) ->
%%    JsnNumber = gingko_utils:get_jsn_number(J),
%%    LowestJsnNumber =< JsnNumber andalso HighestJsnNumber >= JsnNumber andalso gingko_utils:is_system_operation(J, commit_txn)
%%                                  end, AllJournalEntries),
%%  RelevantTxIds = sets:from_list(lists:map(fun(J) ->
%%    J#journal_entry.tx_id end, CommitsInBetween)), %TODO set transformation is probably not necessary since there should be only one commit per tx (maybe better performance lookup)
%%  RelevantTxIds = sets:add_element(JournalEntry#journal_entry.tx_id, RelevantTxIds),
%%  JournalEntriesThatAreRelevant = lists:filter(fun(J) ->
%%    sets:is_element(J#journal_entry.tx_id, RelevantTxIds) andalso (gingko_utils:is_system_operation(J, commit_txn) orelse gingko_utils:is_update_and_contains_key(J, KeyStruct)) end, AllJournalEntries),
%%  TxIdToJournalEntries = gingko_utils:group_by(fun(J) -> J#journal_entry.tx_id end, JournalEntriesThatAreRelevant),
%%  TxIdToJournalEntries = lists:map(fun({TxId, Js}) -> {TxId, lists:sort(fun(J1, J2) ->
%%    J1#journal_entry.jsn#jsn.number < J2#journal_entry.jsn#jsn.number end, Js)} end, TxIdToJournalEntries),
%%  TxIdToJournalEntries = lists:sort(
%%    fun({_TxId1, Js1}, {_TxId2, Js2}) ->
%%      Js1Last = lists:last(Js1),
%%      Js2Last = lists:last(Js2),
%%      V1 = gingko_utils:is_system_operation(Js1Last, commit_txn),
%%      V2 = gingko_utils:is_system_operation(Js2Last, commit_txn),
%%      case V1 of
%%        true -> case V2 of
%%                  true -> gingko_utils:get_jsn_number(Js1Last) < gingko_utils:get_jsn_number(Js2Last);
%%                  false -> true
%%                end;
%%        false -> case V2 of
%%                   true -> false;
%%                   false -> true
%%                 end
%%      end
%%    end, TxIdToJournalEntries),
%%  SnapshotValue = gingko_log:read_snapshot(KeyStruct),
%%  UpdatePayloads = lists:map(fun({J1, JList}) ->
%%    lists:map(fun(J) -> materializer:transform_to_update_payload(J1, J) end, JList)
%%                             end, CommittedJournalEntries),
%%  lists:merge(UpdatePayloads).

-spec transform_to_update_payload(journal_entry(), journal_entry()) -> update_payload().
transform_to_update_payload(CommitJournalEntry, JournalEntry) ->
  #update_payload{
    key_struct = JournalEntry#journal_entry.operation#object_operation.key_struct,
    op_param = JournalEntry#journal_entry.operation#object_operation.op_args,
    snapshot_vts = CommitJournalEntry#journal_entry.operation#system_operation.op_args#commit_txn_args.snapshot_vts,
    commit_vts = CommitJournalEntry#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_vts,
    tx_id = CommitJournalEntry#journal_entry.tx_id
  }.

%%TODO journal entries are presorted based on the current transaction and the snapshot vts
-spec materialize_snapshot(snapshot(), [journal_entry()]) -> {ok, snapshot()} | {error, reason()}.
materialize_snapshot(Snapshot, JournalEntries) ->
  materialize_snapshot(Snapshot, JournalEntries, {none, none}).

-spec materialize_snapshot(snapshot(), [journal_entry()], vts_range()) -> {ok, snapshot()} | {error, reason()}.
materialize_snapshot(Snapshot, JournalEntries, {MinVts, MaxVts}) ->
  SnapshotVts = Snapshot#snapshot.snapshot_vts,
  ValidSnapshotTime = gingko_utils:is_in_vts_range(SnapshotVts, {none, MaxVts}),
  case ValidSnapshotTime of
    true ->
      CommittedJournalEntries = get_committed_journal_entries_for_key([Snapshot#snapshot.key_struct], JournalEntries),
      logger:error("CommittedJournalEntries: ~ts",[CommittedJournalEntries]),
      UpdatePayloads = lists:map(fun({J1, JList}) ->
        lists:map(fun(J) -> transform_to_update_payload(J1, J) end, JList) end, CommittedJournalEntries),
      lists:merge(UpdatePayloads),
      materialize_update_payload(Snapshot, UpdatePayloads);
    false -> {error, {"Invalid Snapshot Time", Snapshot, JournalEntries, {MinVts, MaxVts}}}
  end.

-spec materialize_multiple_snapshots([snapshot()], [journal_entry()]) -> {ok, [snapshot()]} | {error, reason()}.
materialize_multiple_snapshots(Snapshots, JournalEntries) ->
  KeyStructs = lists:map(fun(S) -> S#snapshot.key_struct end, Snapshots),
  CommittedJournalEntries = get_committed_journal_entries_for_key(KeyStructs, JournalEntries),
  UpdatePayloads = lists:map(fun({J1, JList}) ->
    lists:map(fun(J) -> transform_to_update_payload(J1, J) end, JList) end, CommittedJournalEntries),
  UpdatePayloads = lists:flatten(UpdatePayloads),
  KeyToUpdatesDict = gingko_utils:group_by(fun(U) -> U#update_payload.key_struct end, UpdatePayloads),
  %TODO watch out for dict errors (check again that all keys are present)
  Results = lists:map(fun(S) -> materializer:materialize_update_payload(S#snapshot.value, dict:fetch(S#snapshot.key_struct, KeyToUpdatesDict)) end, Snapshots),
  {ok, lists:map(fun({ok, SV}) -> SV end, Results)}. %TODO fix
%%  case lists:all(fun(R) -> {ok, X} == R end, Results) of
%%    true -> {ok, lists:map(fun({ok, SV}) -> SV end, Results)};
%%    false -> {error, {"One or more failed", Results}}
%%  end.

-spec create_snapshot(key_struct()) -> snapshot().
create_snapshot(KeyStruct) ->
  Type = KeyStruct#key_struct.type,
  DefaultValue = gingko_utils:create_default_value(Type),
  #snapshot{key_struct = KeyStruct, commit_vts = vectorclock:new(), snapshot_vts = vectorclock:new(), value = DefaultValue}.

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
  Effect = UpdatePayload#update_payload.op_param,
  case update_snapshot(Snapshot, Effect, CommitVts, SnapshotVts) of
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