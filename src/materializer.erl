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
  filter_relevant_journal_entries/3,
  update_snapshot/4,
  materialize_snapshot/2,
  materialize_snapshot/3, materialize_snapshot_temporarily/2]
).

-spec is_journal_entry_update_and_contains_key(key_struct(), journal_entry(), clock_range()) -> boolean().
is_journal_entry_update_and_contains_key(KeyStruct, JournalEntry, ClockRange) ->
  Operation = JournalEntry#journal_entry.operation,
  if
    is_record(Operation, object_operation) ->
      Operation#object_operation.key_struct == KeyStruct andalso Operation#object_operation.op_type == update andalso gingko_utils:is_in_clock_range(JournalEntry#journal_entry.rt_timestamp, ClockRange);
    true -> false
  end.

-spec contains_journal_entry_of_specific_system_operation(system_operation_type(), [journal_entry()], clock_range()) -> boolean().
contains_journal_entry_of_specific_system_operation(SystemOperationType, JournalEntries, ClockRange) ->
  lists:any(fun(J) ->
    is_journal_entry_of_specific_system_operation(SystemOperationType, J, ClockRange) end, JournalEntries).

-spec is_journal_entry_of_specific_system_operation(system_operation_type(), journal_entry(), clock_range()) -> boolean().
is_journal_entry_of_specific_system_operation(SystemOperationType, JournalEntry, ClockRange) ->
  Operation = JournalEntry#journal_entry.operation,
  if
    is_record(Operation, system_operation) ->
      Operation#system_operation.op_type == SystemOperationType andalso gingko_utils:is_in_clock_range(JournalEntry#journal_entry.rt_timestamp, ClockRange);
    true -> false
  end.

-spec filter_relevant_journal_entries(key_struct(), journal_entry(), clock_range()) -> boolean().
filter_relevant_journal_entries(KeyStruct, JournalEntry, ClockRange) ->
  is_journal_entry_update_and_contains_key(KeyStruct, JournalEntry, ClockRange) orelse is_journal_entry_of_specific_system_operation(commit_txn, JournalEntry, ClockRange).

-spec get_indexed_journal_entries([journal_entry()]) -> [{non_neg_integer(), journal_entry()}].
get_indexed_journal_entries(JournalEntries) ->
  lists:mapfoldl(fun(J, Index) -> {{Index, J}, Index + 1} end, 0, JournalEntries).

-spec separate_commit_from_update_journal_entries([journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries(JList) ->
  separate_commit_from_update_journal_entries(JList, []).
-spec separate_commit_from_update_journal_entries([journal_entry()], [journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries([CommitJournalEntry], JournalEntries) ->
  {CommitJournalEntry, JournalEntries};
separate_commit_from_update_journal_entries([UpdateJournalEntry | OtherJournalEntries], JournalEntries) ->
  separate_commit_from_update_journal_entries(OtherJournalEntries, [UpdateJournalEntry | JournalEntries]).

-spec get_committed_journal_entries([journal_entry()]) -> [{journal_entry(), [journal_entry()]}].
get_committed_journal_entries(JournalEntries) ->
  IndexedJournalEntries = get_indexed_journal_entries(JournalEntries),
  JournalEntryToIndex = dict:from_list(lists:map(fun({Index, J}) -> {J, Index} end, IndexedJournalEntries)),
  TxIdToJournalEntries = gingko_utils:group_by(fun({_Index, J}) -> J#journal_entry.tx_id end, JournalEntries),
  TxIdToJournalEntries = dict:filter(fun({_TxId, JList}) ->
    contains_journal_entry_of_specific_system_operation(commit_txn, JList, {none, none}) end, TxIdToJournalEntries),
  TxIdToJournalEntries = dict:map(fun(_TxId, JList) -> lists:sort(fun(J1, J2) ->
    dict:find(J1, JournalEntryToIndex) > dict:find(J2, JournalEntryToIndex) end, JList) end, TxIdToJournalEntries),

  ListOfLists = lists:map(fun({_TxId, JList}) ->
    separate_commit_from_update_journal_entries(JList) end, dict:to_list(TxIdToJournalEntries)),
  ListOfLists = lists:sort(fun({J1, _JList1}, {J2, _JList2}) ->
    dict:find(J1, JournalEntryToIndex) > dict:find(J2, JournalEntryToIndex) end, ListOfLists),
  ListOfLists.

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
%%  LastCheckpoint = lists:search(fun(J) -> gingko_utils:is_system_operation(J, checkpoint) end, lists:reverse(AllJournalEntries)),
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
      SnapshotValue = Snapshot#snapshot.value,
      CommittedJournalEntries = get_committed_journal_entries(JournalEntries),
      UpdatePayloads = lists:map(fun({J1, JList}) ->
        lists:map(fun(J) -> transform_to_update_payload(J1, J) end, JList)
                                  end, CommittedJournalEntries),
      lists:merge(UpdatePayloads),
      materializer:materialize_update_payload(SnapshotValue, UpdatePayloads);
    _ -> {error, {"Invalid Snapshot Time", Snapshot, JournalEntries, {MinVts, MaxVts}}}
  end.

-spec create_snapshot(key_struct()) -> snapshot().
create_snapshot(KeyStruct) ->
  Type = KeyStruct#key_struct.type,
  DefaultValue = gingko_utils:create_default_value(Type),
  #snapshot{key_struct = KeyStruct, value = DefaultValue}.

-spec update_snapshot(snapshot(), effect() | fun((Value :: term()) -> UpdatedValue :: term()), vectorclock(), vectorclock()) -> {ok, snapshot()} | {error, reason()}.
update_snapshot(Snapshot, Op, CommitVts, SnapshotVts) ->
  SnapshotValue = Snapshot#snapshot.value,
  Type = Snapshot#snapshot.key_struct#key_struct.type,
  IsCrdt = antidote_crdt:is_type(Type),
  case IsCrdt of
    true ->
      IsValidOp = Type:is_operation(Op),
      case IsValidOp of
        true ->
          {ok, Snapshot#snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts, value = Type:update(Op, SnapshotValue)}};
        _ -> {error, {"Invalid Operation on CRDT", Snapshot, Op, CommitVts, SnapshotVts}}
      end;
    _ ->
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
  Effect = JournalEntry#update_payload.op_param,
  case update_snapshot(Snapshot, Effect, Snapshot#snapshot.commit_vts, Snapshot#snapshot.snapshot_vts) of
    {error, Reason} ->
      {error, Reason};
    {ok, Result} ->
      materialize_snapshot_temporarily(Result, Rest)
  end.