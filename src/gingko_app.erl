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

-module(gingko_app).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc.hrl").
-behaviour(application).

-export([start/2,
    stop/1,
    setup_cluster/1]).

-spec(start(StartType :: normal | {takeover, node()} | {failover, node()},
    StartArgs :: term()) ->
    {ok, pid()} |
    {ok, pid(), State :: term()} |
    {error, Reason :: term()}).
start(_StartType, _StartArgs) ->
    case gingko_app_sup:start_link() of
        {ok, Pid} ->
            case gingko_env_utils:get_use_single_server() of
                true -> ok;
                false ->
                    ok = riak_core:register(?GINGKO_APP_NAME, [{vnode_module, inter_dc_log_vnode}]),
                    ok = riak_core_node_watcher:service_up(inter_dc_log, self()),

                    ok = riak_core:register(?GINGKO_APP_NAME, [{vnode_module, gingko_log_vnode}]),
                    ok = riak_core_node_watcher:service_up(gingko_log, self()),

                    ok = riak_core:register(?GINGKO_APP_NAME, [{vnode_module, gingko_log_helper_vnode}]),
                    ok = riak_core_node_watcher:service_up(gingko_log_helper, self()),

                    ok = riak_core:register(?GINGKO_APP_NAME, [{vnode_module, gingko_cache_vnode}]),
                    ok = riak_core_node_watcher:service_up(gingko_cache, self())
            end,
            wait_until_everything_is_running(),
            check_node_restart(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

-spec(stop(State :: term()) -> term()).
stop(_State) ->
    ok.

-spec setup_cluster([node()]) -> ok | {error, reason()}.
setup_cluster([]) -> {error, "At least one node required for setup!"};
setup_cluster(Nodes) ->
    case gingko_env_utils:get_use_single_server() of
        true -> setup_mnesia([hd(Nodes)]); %%Ignore other nodes
        false ->
            inter_dc_manager:add_nodes_to_dc(Nodes),
            setup_mnesia(Nodes)
    end.

check_node_restart() ->
    %%TODO wait until dc is back up completely
    %%TODO here we assume that the other nodes are already running and are connected to all remote dcs and the dc of this node is reestablished
    mnesia:stop(),
    mnesia:start(),
    TableNames = mnesia:system_info(tables),
    GlobalTableNames = get_global_table_names(),
    TablesExist = lists:all(fun(GlobalTableName) -> lists:member(GlobalTableName, TableNames) end, GlobalTableNames),
    case TablesExist of
        true ->
            mnesia:wait_for_tables(GlobalTableNames, 5000),
            true = general_utils:list_all_equal(ok, general_utils:get_values(gingko_dc_utils:bcast_gingko_sync(?GINGKO_LOG, initialize))),

            case inter_dc_meta_data_manager:has_dc_started_and_is_healthy() of
                true ->
                    logger:info("This node was previously configured, will restart from previous config"),
                    DcNodes = gingko_dc_utils:get_my_dc_nodes(),
                    ok = setup_mnesia(DcNodes),
                    MyNode = node(),

                    %% Reconnect this node to other DCs
                    ExistingDcDescriptors = inter_dc_meta_data_manager:get_dc_descriptors(),
                    ok = inter_dc_manager:reconnect_node_to_remote_dcs_after_restart(ExistingDcDescriptors, MyNode),
                    %% Ensure all connections were successful, crash otherwise
                    true;
                false ->
                    mnesia:stop(),
                    false
            end;
        false ->
            mnesia:stop(),
            false
    end.

-spec wait_until_everything_is_running() -> ok.
wait_until_everything_is_running() ->
    ok = gingko_dc_utils:ensure_gingko_instance_running(gingko_time_manager),
    ok = gingko_dc_utils:ensure_gingko_instance_running(?INTER_DC_LOG),
    ok = gingko_dc_utils:ensure_gingko_instance_running(?GINGKO_LOG),
    ok = gingko_dc_utils:ensure_gingko_instance_running(?GINGKO_LOG_HELPER),
    ok = gingko_dc_utils:ensure_gingko_instance_running(?GINGKO_CACHE),
    ok = gingko_dc_utils:ensure_gingko_instance_running(zmq_context),
    ok = gingko_dc_utils:ensure_gingko_instance_running(gingko_checkpoint_service),
    ok = gingko_dc_utils:ensure_gingko_instance_running(inter_dc_state_service),
    ok = gingko_dc_utils:ensure_gingko_instance_running(inter_dc_txn_receiver),
    ok = gingko_dc_utils:ensure_gingko_instance_running(inter_dc_txn_sender),
    ok = gingko_dc_utils:ensure_gingko_instance_running(inter_dc_request_responder),
    ok = gingko_dc_utils:ensure_gingko_instance_running(inter_dc_request_sender),
    ok = gingko_dc_utils:ensure_gingko_instance_running(bcounter_manager).

-spec add_new_nodes_to_mnesia(node(), [node()]) -> ok.
add_new_nodes_to_mnesia(_, []) -> ok;
add_new_nodes_to_mnesia(ExistingNode, NewNodes) ->
    rpc:call(ExistingNode, mnesia, change_config, [extra_db_nodes, NewNodes]),
    lists:foreach(
        fun(NewNode) ->
            rpc:call(ExistingNode, mnesia, change_table_copy_type, [schema, NewNode, disc_copies]),
            {atomic, ok} = mnesia:add_table_copy(checkpoint_entry, NewNode, disc_copies),
            {atomic, ok} = mnesia:add_table_copy(dc_info_entry, NewNode, disc_copies),
            {atomic, ok} = mnesia:add_table_copy(tx_vts, NewNode, ram_copies),
            {atomic, ok} = mnesia:add_table_copy(partition_vts, NewNode, disc_copies)
        end, NewNodes).

get_global_table_names() -> [dc_info_entry, checkpoint_entry, tx_vts, partition_vts].

%%TODO startup is currently fail directly
%%TODO keep this for later when inner dc replication becomes relevant (replicate between different nodes)
-spec setup_mnesia([node()]) -> ok | {error, reason()}.
setup_mnesia([]) -> {error, "At least one node is required!"};
setup_mnesia(Nodes) ->
    %%TODO maybe allow setup on nodes that do not include the current running node
    CallerNode = node(),
    case lists:member(CallerNode, Nodes) of
        false -> {error, "The currently running node must be a member of the initial startup nodes"};
        true ->
            IsRunningLocally = mnesia:system_info(is_running),
            AlreadyRunning =
                case IsRunningLocally of
                    yes ->
                        RunningDbNodes = mnesia:system_info(running_db_nodes),
                        case general_utils:set_equals_on_lists(Nodes, RunningDbNodes) of
                            true -> true; %%TODO check that setup is correct
                            false ->
                                NodesWithoutCallerNode = lists:delete(CallerNode, Nodes),
                                case general_utils:set_equals_on_lists(NodesWithoutCallerNode, RunningDbNodes) of
                                    true ->
                                        restart_mnesia_locally(); %%TODO check that setup is correct
                                    false ->
                                        error("Partial mnesia cluster not allowed currently!")
                                end
                        end;
                    no ->
                        RunningDbNodes = mnesia:system_info(running_db_nodes),
                        case length(RunningDbNodes) of
                            0 -> false;
                            _ ->
                                NodesWithoutCallerNode = lists:delete(CallerNode, Nodes),
                                case general_utils:set_equals_on_lists(NodesWithoutCallerNode, RunningDbNodes) of
                                    true ->
                                        restart_mnesia_locally(); %%TODO check that setup is correct
                                    false ->
                                        error("Partial mnesia cluster not allowed currently!")
                                end
                        end
                end,
            case AlreadyRunning of
                true -> ok;
                false -> setup_mnesia_fresh(Nodes)
            end
    end.

-spec restart_mnesia_locally() -> boolean().
restart_mnesia_locally() ->
    mnesia:stop(),
    mnesia:start(),
    ok = mnesia:wait_for_tables(get_global_table_names(), 5000), %%TODO error check
    general_utils:list_all_equal(ok, general_utils:get_values(gingko_dc_utils:bcast_local_gingko_sync(?GINGKO_LOG, initialize))).

-spec setup_mnesia_fresh([node()]) -> ok | {error, reason()}.
setup_mnesia_fresh(Nodes) ->
    try
        MulticallResultStop = rpc:multicall(Nodes, application, stop, [mnesia]),
        ok = general_utils:check_multicall_results(MulticallResultStop),
        NodesWithSchema =
            lists:filter(
                fun(Node) ->
                    mnesia_schema:ensure_no_schema([Node]) /= ok
                end, Nodes),
        case NodesWithSchema of
            [] ->
                ok = mnesia:create_schema(Nodes),
                MulticallResultStart = rpc:multicall(Nodes, application, start, [mnesia]),
                ok = general_utils:check_multicall_results(MulticallResultStart),
                ok = make_sure_global_stores_are_running(Nodes),
                true = general_utils:list_all_equal(ok, general_utils:get_values(gingko_dc_utils:bcast_gingko_sync(?GINGKO_LOG, initialize))),
                ok;
            %TODO setup fresh
            _ ->
                NodesWithoutSchema = general_utils:list_without_elements_from_other_list(Nodes, NodesWithSchema),
                MulticallResultDbNodes = {MnesiaNodesList, _} = rpc:multicall(NodesWithSchema, mnesia, system_info, [db_nodes]),
                ok = general_utils:check_multicall_results(MulticallResultDbNodes),
                NodesWithSchemaOrdSet = ordsets:from_list(NodesWithSchema),
                PerfectlyEqual = lists:all(fun(MnesiaNodes) ->
                    NodesWithSchemaOrdSet == ordsets:from_list(MnesiaNodes) end, MnesiaNodesList),
                case PerfectlyEqual of
                    true ->
                        %start mnesia on all nodes
                        MulticallResultStart = rpc:multicall(Nodes, application, start, [mnesia]),
                        ok = general_utils:check_multicall_results(MulticallResultStart),
                        case NodesWithoutSchema of
                            [] -> ok;
                            _ ->
                                add_new_nodes_to_mnesia(hd(NodesWithSchema), NodesWithoutSchema)
                        end,
                        ok = make_sure_global_stores_are_running(Nodes),
                        gingko_dc_utils:bcast_gingko_sync(?GINGKO_LOG, initialize),
                        ok; %TODO checkpoint setup and inform all vnodes
                    false ->
                        PartiallyPerfect = lists:all(
                            fun(MnesiaNodes) ->
                                ordsets:is_subset(ordsets:from_list(MnesiaNodes), NodesWithSchemaOrdSet) end, MnesiaNodesList),
                        case PartiallyPerfect of
                            true ->
                                %TODO fix setup (partially broken mnesia cluster)
                                {error, "Broken mnesia"};
                            false ->
                                %TODO fix setup (completely broken mnesia cluster)
                                {error, "Broken mnesia"}

                        end
                end
        end
    catch
        error:{badmatch, Error = {error, _}} -> Error
    end.

%%TODO maybe do check other properties of the table
%%TODO failure testing is required to check whether this works as intended
-spec make_sure_global_stores_are_running([node()]) -> ok | {error, reason()}.
make_sure_global_stores_are_running([]) -> {error, "At least one node is required!"};
make_sure_global_stores_are_running(SetupMnesiaNodes) ->
    %%TODO currently we only allow checkpoint_entry table to be disc_copies
    TableNames = get_global_table_names(),
    Tables = mnesia:system_info(tables),
    lists:foreach(
        fun(TableName) ->
            case lists:member(TableName, Tables) of
                true ->
                    ValidType =
                        case TableName of
                            tx_vts -> ram_copies;
                            _ -> disc_copies
                        end,
                    RamCopiesNodes = mnesia:table_info(TableName, ram_copies),
                    DiscCopiesNodes = mnesia:table_info(TableName, disc_copies),
                    DiscOnlyNodes = mnesia:table_info(TableName, disc_only_copies),
                    ListOfBadTypeNodes =
                        case ValidType of
                            ram_copies -> DiscCopiesNodes ++ DiscOnlyNodes;
                            disc_copies -> RamCopiesNodes ++ DiscOnlyNodes;
                            disc_only_copies -> RamCopiesNodes ++ DiscCopiesNodes
                        end,
                    lists:foreach(
                        fun(Node) ->
                            {atomic, ok} = mnesia:change_table_copy_type(TableName, Node, ValidType)
                        end, ListOfBadTypeNodes),
                    %%TODO maybe wait_for_tables?
                    NodesWhereTableIsSetup = mnesia:table_info(TableName, ValidType),
                    case length(NodesWhereTableIsSetup) of
                        0 ->
                            ok = create_tables(SetupMnesiaNodes, TableName);
                        _ ->
                            lists:foreach(
                                fun(Node) ->
                                    case lists:member(Node, NodesWhereTableIsSetup) of
                                        false -> {atomic, ok} = mnesia:add_table_copy(TableName, Node, ValidType);
                                        true -> ok
                                    end
                                end, SetupMnesiaNodes)
                    end;
                false ->
                    ok = create_tables(SetupMnesiaNodes, TableName)
            end
        end, TableNames),
    ok = mnesia:wait_for_tables(TableNames, 5000). %%TODO error handling

-spec create_tables([node()], table_name()) -> ok | {error, reason()}.
create_tables(SetupMnesiaNodes, TableName) ->
    case TableName of
        checkpoint_entry ->
            mnesia_utils:get_mnesia_result(mnesia:create_table(TableName,
                [{attributes, record_info(fields, checkpoint_entry)},
                    {disc_copies, SetupMnesiaNodes}]));
        dc_info_entry ->
            mnesia_utils:get_mnesia_result(mnesia:create_table(TableName,
                [{attributes, record_info(fields, dc_info_entry)},
                    {disc_copies, SetupMnesiaNodes}]));
        tx_vts ->
            mnesia_utils:get_mnesia_result(mnesia:create_table(TableName,
                [{attributes, record_info(fields, tx_vts)},
                    {ram_copies, SetupMnesiaNodes}]));
        partition_vts ->
            mnesia_utils:get_mnesia_result(mnesia:create_table(TableName,
                [{attributes, record_info(fields, partition_vts)},
                    {disc_copies, SetupMnesiaNodes}]))
    end.
