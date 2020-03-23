%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Okt 2019 17:45
%%%-------------------------------------------------------------------
-module(gingko_utils).
-author("kevin").
-include("gingko.hrl").

%% API
-export([is_in_vts_range/2, get_clock_range/2, is_in_clock_range/2, create_cache_entry/1, group_by/2, add_to_value_list_or_create_single_value_list/3, create_default_value/1, get_jsn_number/1, sort_by_jsn_number/1, is_system_operation/2, is_update_and_contains_key/2, is_update/1, get_keys_from_updates/1, generate_downstream_op/4, create_snapshot/1, get_timestamp/0]).

-spec get_timestamp() -> integer().
get_timestamp() ->
  {Mega, Sec, Micro} = os:timestamp(),
  (Mega*1000000 + Sec)*1000 + round(Micro/1000).

-spec group_by(fun((ListType :: term()) -> GroupKeyType :: term()), [ListType :: term()]) -> [{GroupKeyType :: term(), [ListType :: term()]}].%TODO this returns a dictionary!
group_by(Fun, List) ->
  lists:foldr(fun({Key, Value}, Dict) -> dict:append(Key, Value, Dict) end, dict:new(), [{Fun(X), X} || X <- List]).

-spec add_to_value_list_or_create_single_value_list(DictionaryTypeAToValueTypeBList :: term(), KeyTypeA :: term(), ValueTypeB :: term()) -> term().
add_to_value_list_or_create_single_value_list(DictionaryTypeAToValueTypeBList, KeyTypeA, ValueTypeB) ->
  dict:update(KeyTypeA, fun(ValueTypeBList) ->
    [ValueTypeB | ValueTypeBList] end, [ValueTypeB], DictionaryTypeAToValueTypeBList).

-spec is_in_vts_range(vectorclock(), vts_range()) -> boolean().
is_in_vts_range(Vts, VtsRange) ->
  case VtsRange of
    {none, none} -> true;
    {none, MaxVts} -> vectorclock:le(Vts, MaxVts);
    {MinVts, none} -> vectorclock:le(MinVts, Vts);
    {MinVts, MaxVts} -> vectorclock:le(MinVts, Vts) andalso vectorclock:le(Vts, MaxVts)
  end.

get_clock_range(DcId, VtsRange) ->
  case VtsRange of
    {none, none} -> {none, none};
    {none, MaxVts} -> {none, vectorclock:get_clock_of_dc(DcId, MaxVts)};
    {MinVts, none} -> {vectorclock:get_clock_of_dc(DcId, MinVts), none};
    {MinVts, MaxVts} -> {vectorclock:get_clock_of_dc(DcId, MinVts), vectorclock:get_clock_of_dc(DcId, MaxVts)}
  end.

-spec is_in_clock_range(clock_time(), clock_range()) -> boolean().
is_in_clock_range(ClockTime, ClockRange) ->
  case ClockRange of
    {none, none} -> true;
    {none, MaxClockTime} -> ClockTime =< MaxClockTime;
    {MinClockTime, none} -> MinClockTime >= ClockTime;
    {MinClockTime, MaxClockTime} -> MinClockTime >= ClockTime andalso ClockTime =< MaxClockTime
  end.

-spec create_cache_entry(snapshot()) -> cache_entry().
create_cache_entry(Snapshot) ->
  KeyStruct = Snapshot#snapshot.key_struct,
  CommitVts = Snapshot#snapshot.commit_vts,
  SnapshotVts = Snapshot#snapshot.snapshot_vts,
  Value = Snapshot#snapshot.value,
  #cache_entry{key_struct = KeyStruct, commit_vts = CommitVts, present = true, valid_vts = SnapshotVts, used = true, blob = Value}.

-spec create_snapshot(cache_entry()) -> snapshot().
create_snapshot(CacheEntry) ->
  KeyStruct = CacheEntry#cache_entry.key_struct,
  CommitVts = CacheEntry#cache_entry.commit_vts,
  SnapshotVts = CacheEntry#cache_entry.valid_vts,
  Value = CacheEntry#cache_entry.blob,
  #snapshot{key_struct = KeyStruct, commit_vts = CommitVts, snapshot_vts = SnapshotVts, value = Value}.

-spec create_default_value(type()) -> crdt() | none. %%TODO Consider other types
create_default_value(Type) ->
  IsCrdt = antidote_crdt:is_type(Type),
  case IsCrdt of
    true -> Type:new();
    _ -> none
  end.

is_system_operation(JournalEntry, Type) ->
  Operation = JournalEntry#journal_entry.operation,
  is_record(Operation, system_operation) andalso Operation#system_operation.op_type == Type.

-spec is_update_and_contains_key(journal_entry(), key_struct()) -> boolean().
is_update_and_contains_key(JournalEntry, KeyStruct) ->
  Operation = JournalEntry#journal_entry.operation,
  case
    is_record(Operation, object_operation) of true ->
    Operation#object_operation.key_struct == KeyStruct andalso Operation#object_operation.op_type == update;
    false -> false
  end.

-spec is_update(journal_entry()) -> boolean().
is_update(JournalEntry) ->
  Operation = JournalEntry#journal_entry.operation,
  case
    is_record(Operation, object_operation) of true -> Operation#object_operation.op_type == update;
    false -> false
  end.

get_keys_from_updates(JournalEntries) ->
  lists:filtermap(fun(J) ->
    case gingko_utils:is_update(J) of
      true -> {true, J#journal_entry.operation#object_operation.key_struct};
      false -> false
    end end, JournalEntries).

get_jsn_number(JournalEntry) ->
  JournalEntry#journal_entry.jsn#jsn.number.

sort_by_jsn_number(JournalEntries) ->
  lists:sort(fun(J1, J2) ->
    get_jsn_number(J1) < get_jsn_number(J2) end, JournalEntries).

recover_journal_log() ->
  AllJournalEntries = gingko_log:read_all_journal_entries(),
  AllJournalEntries = lists:sort(fun(J1, J2) ->
    gingko_utils:get_jsn_number(J1) < gingko_utils:get_jsn_number(J2) end, AllJournalEntries),

  CheckpointJournalEntries = g.

-spec generate_downstream_op(key_struct(), txid(), type_op(), pid()) ->
  {ok, downstream_op()} | {error, atom()}.
generate_downstream_op(KeyStruct, TxId, TypeOp, CacheServerPid) ->
  %% TODO: Check if read can be omitted for some types as registers
  Key = KeyStruct#key_struct.key,
  Type = KeyStruct#key_struct.type,
  NeedState = Type:require_state_downstream(TypeOp),
  Result =
    %% If state is needed to generate downstream, read it from the partition.
  case NeedState of
    true ->
      case gingko_log:perform_tx_read(KeyStruct, TxId, CacheServerPid) of
        {ok, S} ->
          S;
        {error, Reason} ->
          {error, {gen_downstream_read_failed, Reason}}
      end;
    false ->
      {ok, ignore} %Use a dummy value
  end,
  case Result of
    {error, R} ->
      {error, R}; %% {error, Reason} is returned here.
    Snapshot ->
      case Type of
        antidote_crdt_counter_b ->
          %% bcounter data-type. %%TODO bcounter!
          %%bcounter_mgr:generate_downstream(Key, TypeOp, Snapshot);
          {error, "BCounter not supported for now!"};
        _ ->
          Type:downstream(TypeOp, Snapshot)
      end
  end.