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

-module(gingko_utils).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-export([get_timestamp/0,
    get_my_dcid/0,
    get_connected_dcids/0,
    get_my_dc_nodes/0,
    get_DCSf_vts/0,
    get_GCSf_vts/0,
    get_my_partitions/0,
    get_number_of_partitions/0,
    gingko_send_after/2,
    update_timer/5,

    check_registered/1,
    ensure_local_gingko_instance_running/1,
    ensure_gingko_instance_running/1,


    is_in_vts_range/2,
    get_clock_range/2,
    is_in_clock_range/2,

    create_new_snapshot/2,
    create_cache_entry/1,
    update_cache_usage/2,

    create_snapshot_from_cache_entry/1,
    create_snapshot_from_checkpoint_entry/2,

    create_default_value/1,
    is_system_operation/1,
    is_journal_entry_type/2,
    contains_journal_entry_type/2,
    get_object_operations/1,

    is_update_of_keys/2,
    is_update_of_keys_or_commit/2,
    is_update/1,
    get_journal_entries_of_type/2,

    get_keys_from_updates/1,
    sort_by_jsn_number/1,
    sort_journal_entries_of_same_tx/1,
    remove_existing_journal_entries_handoff/2,

    get_latest_vts/1,
    get_default_txn_num/0,
    generate_downstream_op/4,

    call_gingko_async/3,
    call_gingko_sync/3,
    call_gingko_async_with_key/3,
    call_gingko_sync_with_key/3,
    bcast_local_gingko_async/2,
    bcast_gingko_async/2,
    bcast_gingko_sync/2,
    bcast_gingko_sync_only_values/2,

    get_key_partition/1]).

%% Gets the current erlang timestamp and returns it as an integer that represents microseconds
%% Experimental: Uses the experimental_time_manager to get a positive representation of erlang monotonic time which is guaranteed to be monotonically increasing unlike the regular erlang timestamp
-spec get_timestamp() -> non_neg_integer().
get_timestamp() ->
    case ?USE_EXPERIMENTAL_TIMESTAMP of
        true ->
            case ?EXPERIMENTAL_TIMESTAMP_USE_MONOTONIC_TIME of
                true -> gingko_time_manager:get_positive_monotonic_time();
                false -> gingko_time_manager:get_monotonic_system_time()
            end;
        false ->
            general_utils:get_timestamp_in_microseconds() %TODO decide which timestamp to use
    end.

%% Gets the DCID
%% USE_SINGLE_SERVER: returns the current node because a DC can only have a single node
-spec get_my_dcid() -> dcid().
get_my_dcid() ->
    case ?USE_SINGLE_SERVER of
        true -> node();
        false -> antidote_utilities:get_my_dc_id()
    end.

%% Gets the connected DCIDs
-spec get_connected_dcids() -> [dcid()].
get_connected_dcids() ->
    inter_dc_meta_data_manager:get_connected_dcids().

%% Gets all nodes of the DC
%% USE_SINGLE_SERVER: returns the current node because a DC can only have a single node
-spec get_my_dc_nodes() -> [node()].
get_my_dc_nodes() ->
    case ?USE_SINGLE_SERVER of
        true -> [node()];
        false -> antidote_utilities:get_my_dc_nodes()
    end.

-spec get_DCSf_vts() -> vectorclock().
get_DCSf_vts() ->
    Vectorclocks = general_utils:get_values(bcast_gingko_sync_only_values(?GINGKO_LOG, get_current_dependency_vts)),
    vectorclock:min(Vectorclocks).

-spec get_GCSf_vts() -> vectorclock().
get_GCSf_vts() ->
    inter_dc_state_service:get_GCSf().

%% Gets all partitions of the caller node
%% USE_SINGLE_SERVER: returns the default SINGLE_SERVER_PARTITION in a list
-spec get_my_partitions() -> [partition_id()].
get_my_partitions() ->
    case ?USE_SINGLE_SERVER of
        true -> [?SINGLE_SERVER_PARTITION];
        false -> antidote_utilities:get_my_partitions()
    end.

%% Gets all partitions of the DC
%% USE_SINGLE_SERVER: returns the default SINGLE_SERVER_PARTITION in a list
-spec get_all_partitions() -> [partition_id()].
get_all_partitions() ->
    case ?USE_SINGLE_SERVER of
        true -> [?SINGLE_SERVER_PARTITION];
        false -> antidote_utilities:get_all_partitions()
    end.

%% Gets the number of all partitions of the DC
%% USE_SINGLE_SERVER: returns 1
-spec get_number_of_partitions() -> non_neg_integer().
get_number_of_partitions() ->
    case ?USE_SINGLE_SERVER of
        true -> 1;
        false -> antidote_utilities:get_number_of_partitions()
    end.

-spec gingko_send_after(millisecond(), term()) -> reference().
gingko_send_after(TimeMillis, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> erlang:send_after(TimeMillis, self(), Request);
        false -> riak_core_vnode:send_command_after(TimeMillis, Request)
    end.

-spec update_timer(reference(), boolean(), millisecond(), term(), boolean()) -> reference().
update_timer(CurrentTimerOrNone, UpdateExistingTimer, TimeMillis, Request, IsPotentialVNode) ->
    UpdateTimerFun =
        fun() ->
            case IsPotentialVNode of
                true -> gingko_utils:gingko_send_after(TimeMillis, Request);
                false -> erlang:send_after(TimeMillis, self(), Request)
            end
        end,
    case CurrentTimerOrNone of
        none ->
            UpdateTimerFun();
        TimerReference ->
            case UpdateExistingTimer of
                true ->
                    erlang:cancel_timer(TimerReference),
                    UpdateTimerFun();
                false -> TimerReference
            end
    end.


%% Waits until a process/vnode name is registered in erlang
        - spec check_registered({atom(), atom()} | atom()) -> ok.
check_registered({ServerName, VMaster}) ->
    case ?USE_SINGLE_SERVER of
        true ->
            antidote_utilities:check_registered(ServerName);
        false ->
            antidote_utilities:check_registered(VMaster)
    end;
check_registered(ServerName) ->
    antidote_utilities:check_registered(ServerName).

-spec ensure_local_gingko_instance_running({atom(), atom()} | atom()) -> ok.
ensure_local_gingko_instance_running({ServerName, VMaster}) ->
    check_registered({ServerName, VMaster}),
    bcast_gingko_instance_check_up({ServerName, VMaster}, get_my_partitions());
ensure_local_gingko_instance_running(ServerName) ->
    check_registered(ServerName),
    bcast_gingko_instance_check_up(ServerName, [0]).

-spec ensure_gingko_instance_running({atom(), atom()} | atom()) -> ok.
ensure_gingko_instance_running({ServerName, VMaster}) ->
    check_registered({ServerName, VMaster}),
    bcast_gingko_instance_check_up({ServerName, VMaster}, get_all_partitions());
ensure_gingko_instance_running(ServerName) ->
    ensure_local_gingko_instance_running(ServerName).

%% Internal function that loops until a given vnode type is running
-spec bcast_gingko_instance_check_up({atom(), atom()} | atom(), [partition_id()]) -> ok.
bcast_gingko_instance_check_up(_, []) ->
    ok;
bcast_gingko_instance_check_up(InstanceTuple = {_ServerName, _VMaster}, [Partition | Rest]) ->
    Error = try
                case call_gingko_sync(Partition, InstanceTuple, hello) of
                    ok -> false;
                    _Msg -> true
                end
            catch
                _Ex:_Res -> true
            end,
    case Error of
        true ->
            logger:debug("Vnode not up retrying, ~p, ~p", [InstanceTuple, Partition]),
            timer:sleep(?DEFAULT_WAIT_TIME_SHORT),
            bcast_gingko_instance_check_up(InstanceTuple, [Partition | Rest]);
        false ->
            bcast_gingko_instance_check_up(InstanceTuple, Rest)
    end;
bcast_gingko_instance_check_up(ServerName, [0]) ->
    Error = try
                case gen_server:call(ServerName, hello) of
                    ok -> false;
                    _Msg -> true
                end
            catch
                _Ex:_Res -> true
            end,
    case Error of
        true ->
            logger:debug("Server not up retrying, ~p", [ServerName]),
            timer:sleep(?DEFAULT_WAIT_TIME_SHORT),
            bcast_gingko_instance_check_up(ServerName, [0]);
        false ->
            ok
    end.

-spec is_in_vts_range(vectorclock(), vts_range()) -> boolean().
is_in_vts_range(Vts, VtsRange) ->
    case VtsRange of
        {none, none} -> true;
        {none, MaxVts} -> vectorclock:le(Vts, MaxVts);
        {MinVts, none} -> vectorclock:le(MinVts, Vts);
        {MinVts, MaxVts} -> vectorclock:le(MinVts, Vts) andalso vectorclock:le(Vts, MaxVts)
    end.

-spec get_clock_range(dcid(), vts_range()) -> clock_range().
get_clock_range(DcId, VtsRange) ->
    case VtsRange of
        {none, none} -> {none, none};
        {none, MaxVts} -> {none, vectorclock:get(DcId, MaxVts)};
        {MinVts, none} -> {vectorclock:get(DcId, MinVts), none};
        {MinVts, MaxVts} -> {vectorclock:get(DcId, MinVts), vectorclock:get(DcId, MaxVts)}
    end.

-spec is_in_clock_range(clock_time(), clock_range()) -> boolean().
is_in_clock_range(ClockTime, ClockRange) ->
    case ClockRange of
        {none, none} -> true;
        {none, MaxClockTime} -> ClockTime =< MaxClockTime;
        {MinClockTime, none} -> MinClockTime >= ClockTime;
        {MinClockTime, MaxClockTime} -> MinClockTime >= ClockTime andalso ClockTime =< MaxClockTime
    end.

-spec create_new_snapshot(key_struct(), vectorclock()) -> snapshot().
create_new_snapshot(KeyStruct = #key_struct{type = Type}, DependencyVts) ->
    DefaultValue = gingko_utils:create_default_value(Type),
    #snapshot{key_struct = KeyStruct, commit_vts = DependencyVts, snapshot_vts = DependencyVts, value = DefaultValue}.

-spec create_cache_entry(snapshot()) -> cache_entry().
create_cache_entry(#snapshot{key_struct = KeyStruct, commit_vts = CommitVts, snapshot_vts = SnapshotVts, value = Value}) ->
    #cache_entry{key_struct = KeyStruct, commit_vts = CommitVts, valid_vts = SnapshotVts, usage = #cache_usage{first_used = get_timestamp(), last_used = get_timestamp()}, blob = Value}.

-spec update_cache_usage(cache_entry(), boolean()) -> cache_entry().
update_cache_usage(CacheEntry = #cache_entry{usage = CacheUsage}, Used) ->
    NewCacheUsage = case Used of
                        true ->
                            CacheUsage#cache_usage{used = true, last_used = get_timestamp(), times_used = CacheUsage#cache_usage.times_used + 1};
                        false -> CacheUsage#cache_usage{used = false}
                    end,
    CacheEntry#cache_entry{usage = NewCacheUsage}.

-spec create_snapshot_from_cache_entry(cache_entry()) -> snapshot().
create_snapshot_from_cache_entry(#cache_entry{key_struct = KeyStruct, commit_vts = CommitVts, valid_vts = SnapshotVts, blob = Value}) ->
    #snapshot{key_struct = KeyStruct, commit_vts = CommitVts, snapshot_vts = SnapshotVts, value = Value}.

-spec create_snapshot_from_checkpoint_entry(checkpoint_entry(), vectorclock()) -> snapshot().
create_snapshot_from_checkpoint_entry(CheckpointEntry, DependencyVts) ->
    KeyStruct = CheckpointEntry#checkpoint_entry.key_struct,
    Value = CheckpointEntry#checkpoint_entry.value,
    #snapshot{key_struct = KeyStruct, commit_vts = DependencyVts, snapshot_vts = DependencyVts, value = Value}.

-spec create_default_value(type()) -> crdt() | none. %%TODO Consider other types
create_default_value(Type) ->
    IsCrdt = antidote_crdt:is_type(Type),
    case IsCrdt of
        true -> Type:new();
        false -> none
    end.

-spec is_system_operation(journal_entry_type() | journal_entry()) -> boolean().
is_system_operation(begin_txn) -> true;
is_system_operation(prepare_txn) -> true;
is_system_operation(commit_txn) -> true;
is_system_operation(abort_txn) -> true;
is_system_operation(checkpoint) -> true;
is_system_operation(JournalEntryType) when is_atom(JournalEntryType) -> false;
is_system_operation(#journal_entry{type = JournalEntryType}) -> is_system_operation(JournalEntryType);
is_system_operation(_) -> false.

-spec is_object_operation(journal_entry_type() | journal_entry()) -> boolean().
is_object_operation(JournalEntryType) when is_atom(JournalEntryType) -> not is_system_operation(JournalEntryType);
is_object_operation(#journal_entry{type = JournalEntryType}) -> not is_system_operation(JournalEntryType).

-spec is_journal_entry_type(journal_entry(), journal_entry_type() | [journal_entry_type()]) -> boolean().
is_journal_entry_type(#journal_entry{type = OpType}, OpType) -> true;
is_journal_entry_type(#journal_entry{type = OpType}, OpTypeList) when is_list(OpTypeList) ->
    lists:member(OpType, OpTypeList);
is_journal_entry_type(_, _) -> false.

-spec contains_journal_entry_type([journal_entry()], journal_entry_type()) -> boolean().
contains_journal_entry_type(JournalEntryList, OpType) ->
    lists:any(fun(JournalEntry) -> is_journal_entry_type(JournalEntry, OpType) end, JournalEntryList).

-spec get_object_operations([journal_entry()]) -> [journal_entry()].
get_object_operations(JournalEntryList) ->
    lists:filter(fun(JournalEntry) -> is_object_operation(JournalEntry) end, JournalEntryList).

-spec is_update_of_keys(journal_entry(), [key_struct()] | all_keys) -> boolean().
is_update_of_keys(#journal_entry{type = update, args = #object_op_args{key_struct = KeyStruct}}, KeyStructFilter) ->
    KeyStructFilter == all_keys orelse lists:member(KeyStruct, KeyStructFilter);
is_update_of_keys(_, _) -> false.

-spec is_update_of_keys_or_commit(journal_entry(), [key_struct()] | all_keys) -> boolean().
is_update_of_keys_or_commit(JournalEntry, KeyStructFilter) ->
    is_update_of_keys(JournalEntry, KeyStructFilter) orelse is_journal_entry_type(JournalEntry, commit_txn).

-spec is_update(journal_entry()) -> boolean().
is_update(JournalEntry) ->
    is_journal_entry_type(JournalEntry, update).

-spec get_journal_entries_of_type([journal_entry()], journal_entry_type() | [journal_entry_type()]) -> [journal_entry()].
get_journal_entries_of_type(JournalEntryList, GivenType) when is_atom(GivenType) ->
    lists:filter(fun(#journal_entry{type = Type}) -> Type == GivenType end, JournalEntryList);
get_journal_entries_of_type(JournalEntryList, GivenTypes) when is_list(GivenTypes) ->
    lists:filter(fun(#journal_entry{type = Type}) -> lists:member(Type, GivenTypes) end, JournalEntryList).

-spec get_keys_from_updates([journal_entry()]) -> [key_struct()].
get_keys_from_updates(JournalEntryList) ->
    lists:filtermap(
        fun(JournalEntry) ->
            case JournalEntry of
                #journal_entry{type = update, args = #object_op_args{key_struct = KeyStruct}} -> {true, KeyStruct};
                _ -> false
            end
        end, JournalEntryList).

-spec sort_by_jsn_number([journal_entry()]) -> [journal_entry()].
sort_by_jsn_number(JournalEntryList) ->
    lists:sort(fun(#journal_entry{jsn = Jsn1}, #journal_entry{jsn = Jsn2}) -> Jsn1 < Jsn2 end, JournalEntryList).

-spec sort_journal_entries_of_same_tx([journal_entry()]) -> [journal_entry()].
sort_journal_entries_of_same_tx(JournalEntryList) ->
    lists:sort(fun tx_sort_fun/2, JournalEntryList).

-spec tx_sort_fun(journal_entry(), journal_entry()) -> boolean().
tx_sort_fun(JournalEntry1, JournalEntry2) ->
    op_type_sort(get_op_type(JournalEntry1), get_op_type(JournalEntry2)).

-spec op_type_sort(atom() | non_neg_integer(), atom() | non_neg_integer()) -> boolean().
op_type_sort(begin_txn, _) -> true;
op_type_sort(_, begin_txn) -> false;
op_type_sort(TxOpNum, OpType) when is_integer(TxOpNum), is_atom(OpType) -> true;
op_type_sort(OpType, TxOpNum) when is_atom(OpType), is_integer(TxOpNum) -> false;
op_type_sort(TxOpNum1, TxOpNum2) when is_integer(TxOpNum1), is_integer(TxOpNum2) -> TxOpNum1 < TxOpNum2;
op_type_sort(prepare_txn, _) -> true;
op_type_sort(_, prepare_txn) -> false;
op_type_sort(commit_txn, _) -> true;
op_type_sort(_, commit_txn) -> false;
op_type_sort(abort_txn, _) -> true;
op_type_sort(_, abort_txn) -> false. %%TODO checkpoint is not needed

-spec get_op_type(journal_entry()) -> atom() | non_neg_integer().
get_op_type(#journal_entry{args = #object_op_args{tx_op_num = TxOpNum}}) -> TxOpNum;
get_op_type(#journal_entry{type = OpType}) -> OpType.

-spec remove_existing_journal_entries_handoff([journal_entry()], [journal_entry()]) -> [journal_entry()].
remove_existing_journal_entries_handoff(NewJournalEntryList, []) -> NewJournalEntryList;
remove_existing_journal_entries_handoff([#journal_entry{type = checkpoint}], [#journal_entry{type = checkpoint}]) -> [];
remove_existing_journal_entries_handoff([#journal_entry{type = begin_txn}, #journal_entry{type = commit_txn}], [#journal_entry{type = begin_txn}, #journal_entry{type = commit_txn}]) ->
    [];
remove_existing_journal_entries_handoff(NewJournalEntryList, ExistingJournalEntryList) ->
    FinalBegin = remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, begin_txn),
    FinalPrepare = remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, prepare_txn),
    FinalCommit = remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, commit_txn),
    FinalAbort = remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, abort_txn),
    FinalCheckpoint = remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, checkpoint),
    NewObjectJs = gingko_utils:get_object_operations(NewJournalEntryList),
    ExistingObjectJs = gingko_utils:get_object_operations(ExistingJournalEntryList),
    ExistingTxOpNum = lists:map(fun(#journal_entry{args = #object_op_args{tx_op_num = TxOpNum}}) ->
        TxOpNum end, ExistingObjectJs),
    FinalObjectJs = lists:filter(fun(#journal_entry{args = #object_op_args{tx_op_num = TxOpNum}}) ->
        not lists:member(TxOpNum, ExistingTxOpNum) end, NewObjectJs),
    FinalBegin ++ FinalObjectJs ++ FinalPrepare ++ FinalCommit ++ FinalAbort ++ FinalCheckpoint.

-spec remove_existing_journal_entry_handoff([journal_entry()], [journal_entry()], journal_entry_type()) -> [journal_entry()].
remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, SystemOperation) ->
    ExistingResult = get_journal_entries_of_type(ExistingJournalEntryList, SystemOperation),
    case length(ExistingResult) > 0 of
        true -> [];
        false -> get_journal_entries_of_type(NewJournalEntryList, SystemOperation)
    end.

%%TODO reimplement since this is invalid for outside journal entries
-spec get_latest_vts([journal_entry()]) -> vectorclock().
get_latest_vts([]) -> vectorclock:new();
get_latest_vts(SortedJournalEntryList) ->
    ReversedSortedJournalEntryList = lists:reverse(SortedJournalEntryList),
    %LastJournalEntry = hd(ReversedSortedJournalEntryList),
    LastJournalEntryWithVts = hd(
        lists:filter(
            fun(JournalEntry) ->
                gingko_utils:is_journal_entry_type(JournalEntry, begin_txn) orelse gingko_utils:is_journal_entry_type(JournalEntry, commit_txn)
            end, ReversedSortedJournalEntryList)),
    LastVts =
        case gingko_utils:is_journal_entry_type(LastJournalEntryWithVts, begin_txn) of
            true ->
                LastJournalEntryWithVts#journal_entry.args#begin_txn_args.dependency_vts;
            false ->
                LastJournalEntryWithVts#journal_entry.args#commit_txn_args.commit_vts
        end,
    LastVts.

-spec get_default_txn_num() -> txn_num().
get_default_txn_num() -> {0, none, 0}.

-spec generate_downstream_op(key_struct(), txid(), type_op(), atom()) ->
    {ok, downstream_op()} | {error, reason()}.
generate_downstream_op(KeyStruct = #key_struct{key = Key, type = Type}, TxId, TypeOp, TableName) ->
    %% TODO: Check if read can be omitted for some types as registers
    NeedState = Type:require_state_downstream(TypeOp),
    %% If state is needed to generate downstream, read it from the partition.
    case NeedState of
        true ->
            case gingko_log_utils:perform_tx_read(KeyStruct, TxId, TableName) of
                {ok, #snapshot{value = SnapshotValue}} ->
                    case Type of
                        antidote_crdt_counter_b ->
                            %% bcounter data-type.
                            bcounter_manager:generate_downstream(Key, TypeOp, SnapshotValue);
                        _ ->
                            Type:downstream(TypeOp, SnapshotValue)
                    end;
                {error, Reason} ->
                    {error, {gen_downstream_read_failed, Reason}}
            end;
        false ->
            Type:downstream(TypeOp, ignore) %Use a dummy value
    end.

-spec call_gingko_async(partition_id(), {atom(), atom()}, any()) -> ok.
call_gingko_async(Partition, {ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utilities:call_vnode_async(Partition, VMaster, Request)
    end.

-spec call_gingko_sync(partition_id(), {atom(), atom()}, any()) -> any().
call_gingko_sync(Partition, {ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:call(ServerName, Request);
        false -> antidote_utilities:call_vnode_sync(Partition, VMaster, Request)
    end.

-spec call_gingko_async_with_key(key(), {atom(), atom()}, any()) -> ok.
call_gingko_async_with_key(Key, {ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utilities:call_vnode_async_with_key(Key, VMaster, Request)
    end.

-spec call_gingko_sync_with_key(key(), {atom(), atom()}, any()) -> any().
call_gingko_sync_with_key(Key, {ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:call(ServerName, Request);
        false -> antidote_utilities:call_vnode_sync_with_key(Key, VMaster, Request)
    end.

-spec bcast_local_gingko_async({atom(), atom()}, any()) -> ok.
bcast_local_gingko_async({ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utilities:bcast_local_vnode_async(VMaster, Request)
    end.

%% Sends the same (asynchronous) command to all vnodes of a given type.
-spec bcast_gingko_async({atom(), atom()}, any()) -> ok.
bcast_gingko_async({ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utilities:bcast_vnode_async(VMaster, Request)
    end.

%% Sends the same (synchronous) command to all vnodes of a given type.
-spec bcast_gingko_sync({atom(), atom()}, any()) -> [{partition_id(), term()}].
bcast_gingko_sync({ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> [{0, gen_server:call(ServerName, Request)}];
        false -> antidote_utilities:bcast_vnode_sync(VMaster, Request)
    end.

%% Sends the same (synchronous) command to all vnodes of a given type.
-spec bcast_gingko_sync_only_values({atom(), atom()}, any()) -> [term()].
bcast_gingko_sync_only_values({ServerName, VMaster}, Request) ->
    general_utils:get_values(bcast_gingko_sync({ServerName, VMaster}, Request)).

-spec get_key_partition(term()) -> {partition_id(), node()}.
get_key_partition(Key) ->
    case ?USE_SINGLE_SERVER of
        true -> {0, node()};
        false -> antidote_utilities:get_key_partition(Key)
    end.


