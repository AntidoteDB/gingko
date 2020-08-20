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

-module(gingko_dc_utils).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc.hrl").

%% API
-export([get_timestamp/0,
    get_running_tx_list/0,
    get_minimum_tx_dependency_vts/0,
    is_valid_tx_dependency_vts/1,
    add_tx_dependency_vts/2,
    remove_tx_dependency_vts/1,
    update_partition_commit_vts/2,
    get_DCSf_vts/0,
    get_GCSt_vts/0,

    get_my_dcid/0,
    get_connected_dcids/0,
    get_my_dc_nodes/0,
    get_my_partitions/0,
    get_number_of_partitions/0,

    gingko_send_after/2,
    update_timer/5,
    ensure_gingko_instance_running/1,

    call_gingko_async/3,
    call_gingko_sync/3,
    call_gingko_async_with_key/3,
    call_gingko_sync_with_key/3,
    bcast_local_gingko_async/2,
    bcast_local_gingko_sync/2,
    bcast_gingko_async/2,
    bcast_gingko_sync/2,

    get_key_partition/1,
    get_key_node/1,
    get_key_partition_node_tuple/1]).

%% Gets the current erlang timestamp and returns it as an integer that represents microseconds
-spec get_timestamp() -> non_neg_integer().
get_timestamp() ->
    %%TODO check performance
    case gingko_env_utils:get_use_gingko_timestamp() of
        true ->
            gingko_time_manager:get_monotonic_system_time();
        false ->
            general_utils:get_timestamp_in_microseconds() %TODO decide which timestamp to use
    end.

-spec get_running_tx_list() -> [tx_vts()].
get_running_tx_list() ->
    TxVtsPattern = mnesia:table_info(tx_vts, wild_pattern),
    mnesia:dirty_match_object(TxVtsPattern).

-spec get_minimum_tx_dependency_vts() -> vectorclock().
get_minimum_tx_dependency_vts() ->
    TxVtsList = get_running_tx_list(),
    case TxVtsList of
        [] -> get_DCSf_vts();
        _ ->
            Vectorclocks = lists:map(fun(#tx_vts{dependency_vts = DependencyVts}) -> DependencyVts end, TxVtsList),
            vectorclock:min(Vectorclocks)
    end.

-spec is_valid_tx_dependency_vts(vectorclock()) -> boolean().
is_valid_tx_dependency_vts(DependencyVts) ->
    MinimumTxDependencyVts = get_minimum_tx_dependency_vts(),
    vectorclock:ge(DependencyVts, MinimumTxDependencyVts).

-spec add_tx_dependency_vts(txid(), vectorclock()) -> ok.
add_tx_dependency_vts(TxId, DependencyVts) ->
    mnesia:dirty_write(tx_vts, #tx_vts{tx_id = TxId, dependency_vts = DependencyVts}).

-spec remove_tx_dependency_vts(txid()) -> ok.
remove_tx_dependency_vts(TxId) ->
    mnesia:dirty_delete(tx_vts, TxId).

-spec update_partition_commit_vts(partition(), vectorclock()) -> ok.
update_partition_commit_vts(Partition, CommitVts) ->
    mnesia:dirty_write(partition_vts, #partition_vts{partition = Partition, commit_vts = CommitVts}).

-spec get_DCSf_vts() -> vectorclock().
get_DCSf_vts() ->
    Vectorclocks = general_utils:get_values_times(bcast_gingko_sync(?GINGKO_LOG, get_current_dependency_vts), 2),
    vectorclock:min(Vectorclocks).

%%-spec get_DCSf_vts() -> vectorclock().
%%get_DCSf_vts() ->
%%    PartitionVtsPattern = mnesia:table_info(partition_vts, wild_pattern),
%%    PartitionVtsList = mnesia:dirty_match_object(PartitionVtsPattern),
%%    Vectorclocks = lists:map(fun(#partition_vts{commit_vts = CommitVts}) ->
%%        CommitVts end, PartitionVtsList),
%%    vectorclock:min(Vectorclocks).

%%TODO make sure that all nodes received the GCSt
-spec get_GCSt_vts() -> vectorclock().
get_GCSt_vts() ->
    inter_dc_state_service:get_GCSt().

%% Gets the DCID
%% USE_SINGLE_SERVER: returns the current node because a DC can only have a single node
-spec get_my_dcid() -> dcid().
get_my_dcid() ->
    case gingko_env_utils:get_use_single_server() of
        true -> node();
        false -> antidote_utils:get_my_dc_id()
    end.

%% Gets the connected DCIDs
-spec get_connected_dcids() -> [dcid()].
get_connected_dcids() ->
    inter_dc_meta_data_manager:get_connected_dcids().

%% Gets all nodes of the DC
%% USE_SINGLE_SERVER: returns the current node because a DC can only have a single node
-spec get_my_dc_nodes() -> [node()].
get_my_dc_nodes() ->
    case gingko_env_utils:get_use_single_server() of
        true -> [node()];
        false -> antidote_utils:get_my_dc_nodes()
    end.

%% Gets all partitions of the caller node
%% USE_SINGLE_SERVER: returns the default SINGLE_SERVER_PARTITION in a list
-spec get_my_partitions() -> [partition()].
get_my_partitions() ->
    case gingko_env_utils:get_use_single_server() of
        true -> [?SINGLE_SERVER_PARTITION];
        false -> antidote_utils:get_my_partitions()
    end.

%% Gets all partitions of the DC
%% USE_SINGLE_SERVER: returns the default SINGLE_SERVER_PARTITION in a list
-spec get_all_partitions() -> [partition()].
get_all_partitions() ->
    case gingko_env_utils:get_use_single_server() of
        true -> [?SINGLE_SERVER_PARTITION];
        false -> antidote_utils:get_all_partitions()
    end.

%% Gets the number of all partitions of the DC
%% USE_SINGLE_SERVER: returns 1
-spec get_number_of_partitions() -> non_neg_integer().
get_number_of_partitions() ->
    case gingko_env_utils:get_use_single_server() of
        true -> 1;
        false -> antidote_utils:get_number_of_partitions()
    end.

-spec gingko_send_after(millisecond(), term()) -> reference().
gingko_send_after(TimeMillis, Request) ->
    case gingko_env_utils:get_use_single_server() of
        true -> erlang:send_after(TimeMillis, self(), Request);
        false -> riak_core_vnode:send_command_after(TimeMillis, Request)
    end.

-spec update_timer(none | reference(), boolean(), millisecond(), term(), boolean()) -> reference().
update_timer(CurrentTimerOrNone, UpdateExistingTimer, TimeMillis, Request, IsPotentialVNode) ->
    UpdateTimerFun =
        fun() ->
            case IsPotentialVNode of
                true -> gingko_send_after(TimeMillis, Request);
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
-spec check_registered({atom(), atom()} | atom()) -> ok.
check_registered({ServerName, VMaster}) ->
    case gingko_env_utils:get_use_single_server() of
        true ->
            antidote_utils:check_registered(ServerName);
        false ->
            antidote_utils:check_registered(VMaster)
    end;
check_registered(ServerName) ->
    antidote_utils:check_registered(ServerName).

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
-spec bcast_gingko_instance_check_up({atom(), atom()} | atom(), [partition()]) -> ok.
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

-spec call_gingko_async(partition(), {atom(), atom()}, term()) -> ok.
call_gingko_async(Partition, {ServerName, VMaster}, Request) ->
    case gingko_env_utils:get_use_single_server() of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utils:call_vnode_async(Partition, VMaster, Request)
    end.

-spec call_gingko_sync(partition(), {atom(), atom()}, term()) -> term().
call_gingko_sync(Partition, {ServerName, VMaster}, Request) ->
    case gingko_env_utils:get_use_single_server() of
        true -> gen_server:call(ServerName, Request);
        false -> antidote_utils:call_vnode_sync(Partition, VMaster, Request)
    end.

-spec call_gingko_async_with_key(key_struct(), {atom(), atom()}, term()) -> ok.
call_gingko_async_with_key(Key, {ServerName, VMaster}, Request) ->
    case gingko_env_utils:get_use_single_server() of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utils:call_vnode_async_with_key(Key, VMaster, Request)
    end.

-spec call_gingko_sync_with_key(key_struct(), {atom(), atom()}, term()) -> term().
call_gingko_sync_with_key(Key, {ServerName, VMaster}, Request) ->
    case gingko_env_utils:get_use_single_server() of
        true -> gen_server:call(ServerName, Request);
        false -> antidote_utils:call_vnode_sync_with_key(Key, VMaster, Request)
    end.

-spec bcast_local_gingko_async({atom(), atom()}, term()) -> ok.
bcast_local_gingko_async({ServerName, VMaster}, Request) ->
    case gingko_env_utils:get_use_single_server() of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utils:bcast_local_vnode_async(VMaster, Request)
    end.

-spec bcast_local_gingko_sync({atom(), atom()}, term()) -> [{partition(), term()}].
bcast_local_gingko_sync({ServerName, VMaster}, Request) ->
    case gingko_env_utils:get_use_single_server() of
        true -> [{0, gen_server:call(ServerName, Request)}];
        false -> antidote_utils:bcast_local_vnode_sync(VMaster, Request)
    end.

%% Sends the same (asynchronous) command to all vnodes of a given type.
-spec bcast_gingko_async({atom(), atom()}, term()) -> ok.
bcast_gingko_async({ServerName, VMaster}, Request) ->
    case gingko_env_utils:get_use_single_server() of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utils:bcast_vnode_async(VMaster, Request)
    end.

%% Sends the same (synchronous) command to all vnodes of a given type.
-spec bcast_gingko_sync({atom(), atom()}, term()) -> [{partition(), term()}].
bcast_gingko_sync({ServerName, VMaster}, Request) ->
    case gingko_env_utils:get_use_single_server() of
        true -> [{0, gen_server:call(ServerName, Request)}];
        false -> antidote_utils:bcast_vnode_sync(VMaster, Request)
    end.

-spec get_key_partition(key_struct()) -> partition().
get_key_partition(KeyStruct) ->
    {Partition, _} = get_key_partition_node_tuple(KeyStruct),
    Partition.

-spec get_key_node(key_struct()) -> node().
get_key_node(KeyStruct) ->
    {_, Node} = get_key_partition_node_tuple(KeyStruct),
    Node.

-spec get_key_partition_node_tuple(key_struct()) -> {partition(), node()}.
get_key_partition_node_tuple(Key) ->
    case gingko_env_utils:get_use_single_server() of
        true -> {0, node()};
        false -> antidote_utils:get_key_partition_node_tuple(Key)
    end.
