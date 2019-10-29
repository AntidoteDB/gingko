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
  update_cache_entry/2,
  materialize_snapshot/2,
  materialize_snapshot/3,
  materialize_clocksi_payload/3,
  materialize_eager/3,
  check_operations/1,
  check_operation/1]).

%%TODO complex handling of checkpoints in txns
-spec is_system_operation_that_is_not_checkpoint(journal_entry()) -> boolean().
is_system_operation_that_is_not_checkpoint(JournalEntry) ->
  Operation = JournalEntry#journal_entry.operation,
  if
    is_record(Operation, system_operation) -> Operation#system_operation.op_type /= checkpoint;
    true -> false
  end.

-spec is_journal_entry_update_and_contains_key(key_struct(), journal_entry()) -> boolean().
is_journal_entry_update_and_contains_key(KeyStruct, JournalEntry) ->
  Operation = JournalEntry#journal_entry.operation,
  if
    is_record(Operation, object_operation) ->
      Operation#object_operation.key_struct == KeyStruct andalso Operation#object_operation.op_type == update;
    true -> false
  end.

-spec contains_journal_entry_of_specific_system_operation(system_operation_type(), [journal_entry()], clock_time()) -> boolean().
contains_journal_entry_of_specific_system_operation(SystemOperationType, JournalEntries, AfterRtTimestamp) ->
  lists:any(fun(J) ->
    is_journal_entry_of_specific_system_operation(SystemOperationType, J, AfterRtTimestamp) end, JournalEntries).

-spec is_journal_entry_of_specific_system_operation(system_operation_type(), journal_entry(), clock_time()) -> boolean().
is_journal_entry_of_specific_system_operation(SystemOperationType, JournalEntry, AfterRtTimestamp) ->
  Operation = JournalEntry#journal_entry.operation,
  if
    is_record(Operation, system_operation) ->
      Operation#system_operation.op_type == SystemOperationType andalso JournalEntry#journal_entry.rt_timestamp > AfterRtTimestamp;
    true -> false
  end.

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

-spec transform_to_clock_si_payload(journal_entry(), journal_entry()) -> clocksi_payload().
transform_to_clock_si_payload(CommitJournalEntry, JournalEntry) ->
  #clocksi_payload{
    key_struct = JournalEntry#journal_entry.operation#object_operation.key_struct,
    op_param = JournalEntry#journal_entry.operation#object_operation.op_args,
    snapshot_time = CommitJournalEntry#journal_entry.operation#system_operation.op_args#commit_txn_args.snapshot_time,
    commit_time = CommitJournalEntry#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_time,
    tx_id = CommitJournalEntry#journal_entry.tx_id
  }.

-spec get_committed_journal_entries_for_key(key_struct(), [journal_entry()], clock_time()) -> [{journal_entry(), [journal_entry()]}].
get_committed_journal_entries_for_key(KeyStruct, JournalEntries, AfterRtTimestamp) ->
  RelevantJournalEntries = lists:filter(fun(J) ->
    is_journal_entry_update_and_contains_key(KeyStruct, J) orelse is_journal_entry_of_specific_system_operation(commit_txn, J, AfterRtTimestamp) end, JournalEntries),
  IndexedJournalEntries = get_indexed_journal_entries(RelevantJournalEntries),
  JournalEntryToIndex = dict:from_list(lists:map(fun({Index, J}) -> {J, Index} end, IndexedJournalEntries)),
  TxIdToJournalEntries = log_utilities:group_by(fun({_Index, J}) -> J#journal_entry.tx_id end, RelevantJournalEntries),
  TxIdToJournalEntries = dict:filter(fun({_TxId, JList}) ->
    contains_journal_entry_of_specific_system_operation(commit_txn, JList, AfterRtTimestamp) end, TxIdToJournalEntries),
  TxIdToJournalEntries = dict:map(fun(_TxId, JList) -> lists:sort(fun(J1, J2) ->
    dict:find(J1, JournalEntryToIndex) > dict:find(J2, JournalEntryToIndex) end, JList) end, TxIdToJournalEntries),

  ListOfLists = lists:map(fun({_TxId, JList}) ->
    separate_commit_from_update_journal_entries(JList) end, dict:to_list(TxIdToJournalEntries)),
  ListOfLists = lists:sort(fun({J1, _JList1}, {J2, _JList2}) ->
    dict:find(J1, JournalEntryToIndex) > dict:find(J2, JournalEntryToIndex) end, ListOfLists),
  ListOfLists.

-spec materialize_snapshot(checkpoint_entry(), [journal_entry()]) -> snapshot().
materialize_snapshot(CheckpointEntry, JournalEntries) ->
  KeyStruct = CheckpointEntry#snapshot.key_struct,
  CheckpointTimestamp = CheckpointEntry#snapshot.commit_vts,
  CheckpointValue = CheckpointEntry#snapshot.value,
  CommittedJournalEntries = get_committed_journal_entries_for_key(KeyStruct, JournalEntries, CheckpointTimestamp),
  ClockSIPayloads = lists:map(fun({J1, JList}) -> lists:map(fun(J) -> transform_to_clock_si_payload(J1, J) end, JList)
                              end, CommittedJournalEntries),
  lists:merge(ClockSIPayloads),
  materializer:materialize_clocksi_payload(CheckpointValue, ClockSIPayloads).

-spec materialize_snapshot(checkpoint_entry(), [journal_entry()], snapshot_time()) -> snapshot().
materialize_snapshot(CheckpointEntry, JournalEntries, SnapshotTime) ->
  KeyStruct = CheckpointEntry#snapshot.key_struct,
  CheckpointTimestamp = CheckpointEntry#snapshot.commit_vts,
  CheckpointSnapshotTime = CheckpointEntry#snapshot.snapshot_vts,
  ValidSnapshotTime =
    CheckpointValue = CheckpointEntry#snapshot.value,
  CommittedJournalEntries = get_committed_journal_entries_for_key(KeyStruct, JournalEntries, CheckpointTimestamp),
  ClockSIPayloads = lists:map(fun({J1, JList}) -> lists:map(fun(J) -> transform_to_clock_si_payload(J1, J) end, JList)
                              end, CommittedJournalEntries),
  lists:merge(ClockSIPayloads),
  materializer:materialize_clocksi_payload(CheckpointValue, ClockSIPayloads).


%% TODO
-type client_op() :: any().

%% @doc Creates an empty CRDT
-spec create_snapshot(type()) -> snapshot().
create_snapshot(Type) ->
  Type:new().

%% @doc Applies an downstream effect to a snapshot of a crdt.
%%      This function yields an error if the crdt does not have a corresponding update operation.
-spec update_cache_entry(cache_entry(), effect()) -> {ok, cache_entry()} | {error, reason()}.
update_cache_entry(CacheEntry, Op) ->
  try
    Snapshot = CacheEntry#cache_entry.blob,
    Type = CacheEntry#cache_entry.key_struct#key_struct.type,
    Snapshot = Type:update(Op, Snapshot),
    CacheEntry#cache_entry{blob = Snapshot}
  catch
    _:_ ->
      {error, {unexpected_operation, Op, Type}}
  end.

%% @doc
-spec materialize_clocksi_payload(key_struct(), cache_entry(), [clocksi_payload()]) -> cache_entry() | {error, {unexpected_operation, effect(), type()}}.
materialize_clocksi_payload(CacheEntry, []) ->
  CacheEntry;
materialize_clocksi_payload(CacheEntry, [ClocksiPayload | Rest]) ->
  Effect = ClocksiPayload#clocksi_payload.op_param,
  logger:info("Materialize: ~p", [Effect]),
  case update_cache_entry(CacheEntry, Effect) of
    {error, Reason} ->
      {error, Reason};
    {ok, Result} ->
      materialize_clocksi_payload(Result, Rest)
  end.

%% @doc Applies updates in given order without any checks, errors are simply propagated.
-spec materialize_eager(cache_entry(), [effect()]) -> cache_entry() | {error, {unexpected_operation, effect(), type()}}.
materialize_eager(CacheEntry, []) ->
    CacheEntry;
materialize_eager(CacheEntry, [Effect | Rest]) ->
  case update_cache_entry(CacheEntry, Effect) of
    {error, Reason} ->
      {error, Reason};
    {ok, Result} ->
      materialize_eager(Result, Rest)
  end.


%% @doc Check that in a list of client operations, all of them are correctly typed.
-spec check_operations([client_op()]) -> ok | {error, {type_check_failed, client_op()}}.
check_operations([]) ->
  ok;
check_operations([Op | Rest]) ->
  case check_operation(Op) of
    true ->
      check_operations(Rest);
    false ->
      {error, {type_check_failed, Op}}
  end.

%% @doc Check that an operation is correctly typed.
-spec check_operation(client_op()) -> boolean().
check_operation(Op) ->
  case Op of
    {update, {_, Type, Update}} ->
      antidote_crdt:is_type(Type) andalso
        Type:is_operation(Update);
    {read, {_, Type}} ->
      antidote_crdt:is_type(Type);
    _ ->
      false
  end.


%%-ifdef(TEST).
%%
%%%% Testing update with pn_counter.
%%update_pncounter_test() ->
%%    Type = antidote_crdt_counter_pn,
%%    Counter = create_snapshot(Type),
%%    ?assertEqual(0, Type:value(Counter)),
%%    Op = 1,
%%    {ok, Counter2} = update_snapshot(Type, Counter, Op),
%%    ?assertEqual(1, Type:value(Counter2)).
%%
%%%% Testing pn_counter with update log
%%materializer_counter_withlog_test() ->
%%    Type = antidote_crdt_counter_pn,
%%    Counter = create_snapshot(Type),
%%    ?assertEqual(0, Type:value(Counter)),
%%    Ops = [1,
%%           1,
%%           2,
%%           3
%%          ],
%%    Counter2 = materialize_eager(Type, Counter, Ops),
%%    ?assertEqual(7, Type:value(Counter2)).
%%
%%%% Testing counter with empty update log
%%materializer_counter_emptylog_test() ->
%%    Type = antidote_crdt_counter_pn,
%%    Counter = create_snapshot(Type),
%%    ?assertEqual(0, Type:value(Counter)),
%%    Ops = [],
%%    Counter2 = materialize_eager(Type, Counter, Ops),
%%    ?assertEqual(0, Type:value(Counter2)).
%%
%%%% Testing non-existing crdt
%%materializer_error_nocreate_test() ->
%%    ?assertException(error, undef, create_snapshot(bla)).
%%
%%%% Testing crdt with invalid update operation
%%materializer_error_invalidupdate_test() ->
%%    Type = antidote_crdt_counter_pn,
%%    Counter = create_snapshot(Type),
%%    ?assertEqual(0, Type:value(Counter)),
%%    Ops = [{non_existing_op_type, {non_existing_op, actor1}}],
%%    ?assertEqual({error, {unexpected_operation,
%%                    {non_existing_op_type, {non_existing_op, actor1}},
%%                    antidote_crdt_counter_pn}},
%%                 materialize_eager(Type, Counter, Ops)).
%%
%%%% Testing that the function check_operations works properly
%%check_operations_test() ->
%%    Operations =
%%        [{read, {key1, antidote_crdt_counter_pn}},
%%         {update, {key1, antidote_crdt_counter_pn, increment}}
%%        ],
%%    ?assertEqual(ok, check_operations(Operations)),
%%
%%    Operations2 = [{read, {key1, antidote_crdt_counter_pn}},
%%        {update, {key1, antidote_crdt_counter_pn, {{add, elem}, a}}},
%%        {update, {key2, antidote_crdt_counter_pn, {increment, a}}},
%%        {read, {key1, antidote_crdt_counter_pn}}],
%%    ?assertMatch({error, _}, check_operations(Operations2)).
%%
%%%% Testing belongs_to_snapshot returns true when a commit time
%%%% is smaller than a snapshot time
%%belongs_to_snapshot_test() ->
%%    CommitTime1a = 1,
%%    CommitTime2a = 1,
%%    CommitTime1b = 1,
%%    CommitTime2b = 7,
%%    SnapshotClockDC1 = 5,
%%    SnapshotClockDC2 = 5,
%%    CommitTime3a = 5,
%%    CommitTime4a = 5,
%%    CommitTime3b = 10,
%%    CommitTime4b = 10,
%%
%%    SnapshotVC=vectorclock:from_list([{1, SnapshotClockDC1}, {2, SnapshotClockDC2}]),
%%    ?assertEqual(true, belongs_to_snapshot_op(
%%                 vectorclock:from_list([{1, CommitTime1a}, {2, CommitTime1b}]), {1, SnapshotClockDC1}, SnapshotVC)),
%%    ?assertEqual(true, belongs_to_snapshot_op(
%%                 vectorclock:from_list([{1, CommitTime2a}, {2, CommitTime2b}]), {2, SnapshotClockDC2}, SnapshotVC)),
%%    ?assertEqual(false, belongs_to_snapshot_op(
%%                  vectorclock:from_list([{1, CommitTime3a}, {2, CommitTime3b}]), {1, SnapshotClockDC1}, SnapshotVC)),
%%    ?assertEqual(false, belongs_to_snapshot_op(
%%                  vectorclock:from_list([{1, CommitTime4a}, {2, CommitTime4b}]), {2, SnapshotClockDC2}, SnapshotVC)).
%%-endif.
