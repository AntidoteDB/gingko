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
    get_my_dc_nodes/0,
    get_my_partitions/0,
    get_number_of_partitions/0,

    check_registered/1,
    ensure_local_gingko_instance_running/1,
    ensure_gingko_instance_running/1,

    get_DCSf_vts/0,
    get_GCSf_vts/0,

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
    is_system_operation/2,
    contains_system_operation/2,

    is_update_of_keys/2,
    is_update_of_keys_or_commit/2,
    is_update/1,

    get_keys_from_updates/1,
    sort_by_jsn_number/1,

    get_latest_vts/1,
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

-spec get_timestamp() -> non_neg_integer().
get_timestamp() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000000 + Sec) * 1000000 + Micro. %TODO check if this is a good solution

-spec get_my_dcid() -> dcid().
get_my_dcid() ->
    case ?USE_SINGLE_SERVER of
        true -> node();
        false -> antidote_utilities:get_my_dc_id()
    end.

-spec get_my_dc_nodes() -> [node()].
get_my_dc_nodes() ->
    case ?USE_SINGLE_SERVER of
        true -> [node()];
        false -> antidote_utilities:get_my_dc_nodes()
    end.

-spec get_my_partitions() -> [partition_id()].
get_my_partitions() ->
    case ?USE_SINGLE_SERVER of
        true -> [0];
        false -> antidote_utilities:get_my_partitions()
    end.

-spec get_all_partitions() -> [partition_id()].
get_all_partitions() ->
    case ?USE_SINGLE_SERVER of
        true -> [0];
        false -> antidote_utilities:get_all_partitions()
    end.

-spec get_number_of_partitions() -> non_neg_integer().
get_number_of_partitions() ->
    case ?USE_SINGLE_SERVER of
        true -> 1;
        false -> antidote_utilities:get_partitions_num()
    end.

check_registered({ServerName, VMaster}) ->
    case ?USE_SINGLE_SERVER of
        true ->
            antidote_utilities:check_registered(ServerName);
        false ->
            antidote_utilities:check_registered(VMaster)
    end;
check_registered(ServerName) ->
    antidote_utilities:check_registered(ServerName).


ensure_local_gingko_instance_running({ServerName, VMaster}) ->
    check_registered({ServerName, VMaster}),
    bcast_gingko_instance_check_up({ServerName, VMaster}, get_my_partitions());

ensure_local_gingko_instance_running(ServerName) ->
    check_registered(ServerName),
    bcast_gingko_instance_check_up(ServerName, [0]).

ensure_gingko_instance_running({ServerName, VMaster}) ->
    check_registered({ServerName, VMaster}),
    bcast_gingko_instance_check_up({ServerName, VMaster}, get_all_partitions());
ensure_gingko_instance_running(ServerName) ->
    ensure_local_gingko_instance_running(ServerName).

%% Internal function that loops until a given vnode type is running
-spec bcast_gingko_instance_check_up(atom() | {atom(), atom()}, [partition_id()]) -> ok.
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
            %TODO: Extract into configuration constant
            timer:sleep(1000),
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
            %TODO: Extract into configuration constant
            timer:sleep(1000),
            bcast_gingko_instance_check_up(ServerName, [0]);
        false ->
            ok
    end.

-spec get_DCSf_vts() -> vectorclock().
get_DCSf_vts() ->
    Vectorclocks = bcast_gingko_sync_only_values(?GINGKO_LOG, get_current_dependency_vts),
    vectorclock:min(Vectorclocks).

-spec get_GCSf_vts() -> vectorclock().
get_GCSf_vts() ->
    get_DCSf_vts(). %TODO implement (works only for one DC now)

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
create_new_snapshot(KeyStruct, DependencyVts) ->
    Type = KeyStruct#key_struct.type,
    DefaultValue = gingko_utils:create_default_value(Type),
    #snapshot{key_struct = KeyStruct, commit_vts = DependencyVts, snapshot_vts = DependencyVts, value = DefaultValue}.

-spec create_cache_entry(snapshot()) -> cache_entry().
create_cache_entry(Snapshot) ->
    KeyStruct = Snapshot#snapshot.key_struct,
    CommitVts = Snapshot#snapshot.commit_vts,
    SnapshotVts = Snapshot#snapshot.snapshot_vts,
    Value = Snapshot#snapshot.value,
    #cache_entry{key_struct = KeyStruct, commit_vts = CommitVts, valid_vts = SnapshotVts, usage = #cache_usage{first_used = get_timestamp(), last_used = get_timestamp()}, blob = Value}.

-spec update_cache_usage(cache_entry(), boolean()) -> cache_entry().
update_cache_usage(CacheEntry, Used) ->
    CacheUsage = CacheEntry#cache_entry.usage,
    NewCacheUsage = case Used of
                        true ->
                            CacheUsage#cache_usage{used = true, last_used = get_timestamp(), times_used = CacheUsage#cache_usage.times_used + 1};
                        false -> CacheUsage#cache_usage{used = false}
                    end,
    CacheEntry#cache_entry{usage = NewCacheUsage}.

-spec create_snapshot_from_cache_entry(cache_entry()) -> snapshot().
create_snapshot_from_cache_entry(CacheEntry) ->
    KeyStruct = CacheEntry#cache_entry.key_struct,
    CommitVts = CacheEntry#cache_entry.commit_vts,
    SnapshotVts = CacheEntry#cache_entry.valid_vts,
    Value = CacheEntry#cache_entry.blob,
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

-spec is_system_operation(journal_entry()) -> boolean().
is_system_operation(JournalEntry) ->
    Operation = JournalEntry#journal_entry.operation,
    is_record(Operation, system_operation).

-spec is_system_operation(journal_entry(), OpType :: atom()) -> boolean().
is_system_operation(JournalEntry, OpType) ->
    Operation = JournalEntry#journal_entry.operation,
    is_record(Operation, system_operation) andalso Operation#system_operation.op_type == OpType.

-spec contains_system_operation([journal_entry()], system_operation_type()) -> boolean().
contains_system_operation(JournalEntries, SystemOpType) ->
    lists:any(fun(J) -> is_system_operation(J, SystemOpType) end, JournalEntries).

-spec is_update_of_keys(journal_entry(), [key_struct()] | all_keys) -> boolean().
is_update_of_keys(JournalEntry, KeyStructFilter) ->
    Operation = JournalEntry#journal_entry.operation,
    case is_record(Operation, object_operation) of
        true ->
            Operation#object_operation.op_type == update andalso (KeyStructFilter == all_keys orelse lists:member(Operation#object_operation.key_struct, KeyStructFilter));
        false -> false
    end.

-spec is_update_of_keys_or_commit(journal_entry(), [key_struct()] | all_keys) -> boolean().
is_update_of_keys_or_commit(JournalEntry, KeyStructFilter) ->
    is_update_of_keys(JournalEntry, KeyStructFilter) orelse is_system_operation(JournalEntry, commit_txn).

-spec is_update(journal_entry()) -> boolean().
is_update(JournalEntry) ->
    is_update_of_keys(JournalEntry, all_keys).

-spec get_keys_from_updates([journal_entry()]) -> [key_struct()].
get_keys_from_updates(JournalEntries) ->
    lists:filtermap(
        fun(J) ->
            case is_update(J) of
                true -> {true, J#journal_entry.operation#object_operation.key_struct};
                false -> false
            end
        end, JournalEntries).

-spec sort_by_jsn_number([journal_entry()]) -> [journal_entry()].
sort_by_jsn_number(JournalEntries) ->
    lists:sort(fun(J1, J2) -> J1#journal_entry.jsn < J2#journal_entry.jsn end, JournalEntries).

%%TODO reimplement since this is invalid for outside journal entries
-spec get_latest_vts([journal_entry()]) -> vectorclock().
get_latest_vts(SortedJournalEntries) ->
    %TODO error when list is empty (check if relevant)
    ReversedSortedJournalEntries = lists:reverse(SortedJournalEntries),
    %LastJournalEntry = hd(ReversedSortedJournalEntries),
    LastJournalEntryWithVts = hd(
        lists:filter(
            fun(J) ->
                gingko_utils:is_system_operation(J, begin_txn) orelse gingko_utils:is_system_operation(J, commit_txn)
            end, ReversedSortedJournalEntries)),
    LastVts =
        case gingko_utils:is_system_operation(LastJournalEntryWithVts, begin_txn) of
            true ->
                LastJournalEntryWithVts#journal_entry.operation#system_operation.op_args#begin_txn_args.dependency_vts;
            false ->
                LastJournalEntryWithVts#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_vts
        end,
    LastVts.

-spec generate_downstream_op(key_struct(), txid(), type_op(), atom()) ->
    {ok, downstream_op()} | {error, atom()}.
generate_downstream_op(KeyStruct, TxId, TypeOp, TableName) ->
    %% TODO: Check if read can be omitted for some types as registers
    Type = KeyStruct#key_struct.type,
    NeedState = Type:require_state_downstream(TypeOp),
    %% If state is needed to generate downstream, read it from the partition.
    case NeedState of
        true ->
            case gingko_log_vnode:perform_tx_read(KeyStruct, TxId, TableName) of
                {ok, Snapshot} ->
                    case Type of
                        antidote_crdt_counter_b ->
                            %% bcounter data-type. %%TODO bcounter!
                            bcounter_manager:generate_downstream(KeyStruct#key_struct.key, TypeOp, Snapshot#snapshot.value);
                        _ ->
                            Type:downstream(TypeOp, Snapshot#snapshot.value)
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


