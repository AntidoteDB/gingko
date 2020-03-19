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
-export([is_in_vts_range/2, get_clock_range/2, is_in_clock_range/2, create_cache_entry/1, group_by/2, add_to_value_list_or_create_single_value_list/3, is_valid_vts/1, is_valid_snapshot/1, create_default_value/1, get_jsn_number/1, sort_by_jsn_number/1, is_system_operation/2, is_update_and_contains_key/2]).

-spec group_by(fun((ListType :: term()) -> GroupKeyType :: term()), [ListType :: term()]) -> [{GroupKeyType :: term(), [ListType :: term()]}].
group_by(Fun, List) -> lists:foldr(fun({Key, Value}, Dict) -> dict:append(Key, Value, Dict) end , dict:new(), [ {Fun(X), X} || X <- List ]).

-spec add_to_value_list_or_create_single_value_list(DictionaryTypeAToValueTypeBList :: term(), KeyTypeA :: term(), ValueTypeB :: term()) -> term().
add_to_value_list_or_create_single_value_list(DictionaryTypeAToValueTypeBList, KeyTypeA, ValueTypeB) ->
  dict:update(KeyTypeA, fun(ValueTypeBList) -> [ValueTypeB|ValueTypeBList] end, [ValueTypeB], DictionaryTypeAToValueTypeBList).

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

-spec is_valid_vts(vectorclock_or_none()) -> boolean().
is_valid_vts(VectorclockOrNone) ->
  case VectorclockOrNone of
    none -> false;
    _ -> true
  end.

-spec is_valid_snapshot(snapshot()) -> boolean().
is_valid_snapshot(Snapshot) ->
  %%TODO is crdt check!
  CommitVts = Snapshot#snapshot.commit_vts,
  SnapshotVts = Snapshot#snapshot.snapshot_vts,
  is_valid_vts(CommitVts) andalso is_valid_vts(SnapshotVts).


-spec create_default_value(key_struct()) -> crdt() | none. %%TODO Consider other types
create_default_value(KeyStruct) ->
  Type = KeyStruct#key_struct.type,
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
  if
    is_record(Operation, object_operation) ->
      Operation#object_operation.key_struct == KeyStruct andalso Operation#object_operation.op_type == update;
    true -> false
  end.

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





