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

-export([start/2, stop/1, get_default_config/0, initial_startup_nodes/1, wait_until_everything_is_running/0]).

-spec(start(StartType :: normal | {takeover, node()} | {failover, node()},
    StartArgs :: term()) ->
    {ok, pid()} |
    {ok, pid(), State :: term()} |
    {error, Reason :: term()}).
start(_StartType, _StartArgs) ->
    case gingko_app_sup:start_link() of
        {ok, Pid} ->
            case ?USE_SINGLE_SERVER of
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

            %%TODO solve restart test _IsRestart = inter_dc_manager:check_node_restart(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

-spec(stop(State :: term()) -> term()).
stop(_State) ->
    ok.

wait_until_everything_is_running() ->
    ok = gingko_utils:ensure_gingko_instance_running(gingko_time_manager),
    ok = gingko_utils:ensure_gingko_instance_running(gingko_server),
    ok = gingko_utils:ensure_gingko_instance_running(?INTER_DC_LOG),
    ok = gingko_utils:ensure_gingko_instance_running(?GINGKO_LOG),
    ok = gingko_utils:ensure_gingko_instance_running(?GINGKO_LOG_HELPER),
    ok = gingko_utils:ensure_gingko_instance_running(?GINGKO_CACHE),
    ok = gingko_utils:ensure_gingko_instance_running(zmq_context),
    ok = gingko_utils:ensure_gingko_instance_running(gingko_checkpoint_service),
    ok = gingko_utils:ensure_gingko_instance_running(inter_dc_state_service),
    ok = gingko_utils:ensure_gingko_instance_running(inter_dc_txn_receiver),
    ok = gingko_utils:ensure_gingko_instance_running(inter_dc_txn_sender),
    ok = gingko_utils:ensure_gingko_instance_running(inter_dc_request_responder),
    ok = gingko_utils:ensure_gingko_instance_running(inter_dc_request_sender),
    ok = gingko_utils:ensure_gingko_instance_running(bcounter_manager).

get_default_config() ->
    [
        {max_occupancy, 100},
        {reset_used_interval_millis, 1000},
        {eviction_interval_millis, 2000},
        {eviction_threshold_in_percent, 90},
        {target_threshold_in_percent, 80},
        {eviction_strategy, interval}
    ].

add_new_nodes_to_mnesia(ExistingNode, NewNodes) ->
    rpc:call(ExistingNode, mnesia, change_config, [extra_db_nodes, NewNodes]),
    lists:foreach(
        fun(NewNode) ->
            rpc:call(ExistingNode, mnesia, change_table_copy_type, [schema, NewNode, disc_copies])
        end, NewNodes).
%[mnesia:add_table_copy(Table, node(), disc_copies) || Table <- mnesia:system_info(tables), Table =/= schema].

%%TODO startup is currently fail directly
%%TODO keep this for later when inner dc replication becomes relevant (replicate between different nodes)
-spec initial_startup_nodes([node()]) -> ok | {error, reason()}.
initial_startup_nodes([]) -> {error, "At least one node is required!"};
initial_startup_nodes(Nodes) ->
    %%TODO maybe allow setup on nodes that do not include the current running node
    case lists:member(node(), Nodes) of
        false -> {error, "The currently running node must be a member of the initial startup nodes"};
        true ->
            rpc:multicall(Nodes, application, stop, [mnesia]),
            NodesWithSchema = lists:filter(fun(Node) -> mnesia_schema:ensure_no_schema([Node]) /= ok end, Nodes),
            case NodesWithSchema of
                [] ->
                    ok = mnesia:create_schema(Nodes),
                    rpc:multicall(Nodes, application, start, [mnesia]),
                    ok = make_sure_global_stores_are_running([dc_info_entry, checkpoint_entry], Nodes),

                    true = general_utils:list_all_equal(ok, gingko_utils:bcast_gingko_sync_only_values(?GINGKO_LOG, initialize)),
                    TableNames = general_utils:get_values(gingko_utils:bcast_gingko_sync_only_values(?GINGKO_LOG, get_journal_mnesia_table_name)),
                    ok = mnesia:wait_for_tables([dc_info_entry, checkpoint_entry | TableNames], 5000), %TODO error handling
                    ok; %TODO setup fresh
                _ ->
                    NodesWithoutSchema = general_utils:list_without_elements_from_other_list(Nodes, NodesWithSchema),
                    {MnesiaNodesList, BadNodes} = rpc:multicall(NodesWithSchema, mnesia, system_info, [db_nodes]),
                    case BadNodes == [] of
                        false -> {error, {"One or more Nodes don't exist", BadNodes}};
                        true ->
                            BadRpcCalls = lists:filter(fun(MnesiaNodes) -> is_tuple(MnesiaNodes) end, MnesiaNodesList),
                            case BadRpcCalls == [] of
                                false -> {error, {"One or more Nodes did not respond correctly", [BadRpcCalls]}};
                                true ->
                                    NodesWithSchemaOrdSet = ordsets:from_list(NodesWithSchema),
                                    PerfectlyEqual = lists:all(fun(MnesiaNodes) ->
                                        NodesWithSchemaOrdSet == ordsets:from_list(MnesiaNodes) end, MnesiaNodesList),
                                    case PerfectlyEqual of
                                        true ->
                                            %start mnesia on all nodes
                                            rpc:multicall(Nodes, application, start, [mnesia]),
                                            case NodesWithoutSchema of
                                                [] -> ok;
                                                _ -> add_new_nodes_to_mnesia(hd(NodesWithSchema), NodesWithoutSchema)
                                            end,
                                            ok = make_sure_global_stores_are_running([dc_info_entry, checkpoint_entry], Nodes),
                                            gingko_utils:bcast_gingko_sync(?GINGKO_LOG, initialize),
                                            ok; %TODO checkpoint setup and inform all vnodes
                                        false ->
                                            PartiallyPerfect = lists:all(fun(MnesiaNodes) ->
                                                ordsets:is_subset(MnesiaNodes, NodesWithSchemaOrdSet) end, MnesiaNodesList),
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

                    end
            end
    end.

%%TODO maybe do check other properties of the table
%%TODO failure testing is required to check whether this works as intended
-spec make_sure_global_stores_are_running([atom()], [node()]) -> ok | {error, reason()}.
make_sure_global_stores_are_running(_TableNames, []) -> {error, "At least one node is required!"};
make_sure_global_stores_are_running([], _SetupMnesiaNodes) -> {error, "At least one table name is required!"};
make_sure_global_stores_are_running(TableNames, SetupMnesiaNodes) ->
    %%TODO currently we only allow checkpoint_entry table to be disc_copies
    Tables = mnesia:system_info(tables),
    lists:foreach(
        fun(TableName) ->
            case lists:member(TableName, Tables) of
                true ->
                    RamCopiesNodes = mnesia:table_info(TableName, ram_copies),
                    DiscOnlyNodes = mnesia:table_info(TableName, disc_only_copies),
                    lists:foreach(
                        fun(Node) ->
                            {atomic, ok} = mnesia:change_table_copy_type(TableName, Node, disc_copies)
                        end, RamCopiesNodes ++ DiscOnlyNodes),
                    %%TODO maybe wait_for_tables?
                    NodesWhereTableIsSetup = mnesia:table_info(TableName, disc_copies),
                    case length(NodesWhereTableIsSetup) of
                        0 ->
                            {atomic, ok} = create_tables(SetupMnesiaNodes, TableName);
                        _ ->
                            lists:foreach(
                                fun(Node) ->
                                    case lists:member(Node, NodesWhereTableIsSetup) of
                                        false -> {atomic, ok} = mnesia:add_table_copy(TableName, Node, disc_copies);
                                        true -> ok
                                    end
                                end, SetupMnesiaNodes)
                    end,
                    ok;
                false ->
                    {atomic, ok} = create_tables(SetupMnesiaNodes, TableName),
                    ok
            end
        end, TableNames).

create_tables(SetupMnesiaNodes, TableName) ->
    RecordInfo =
        case TableName of
            checkpoint_entry ->
                record_info(fields, checkpoint_entry);
            dc_info_entry ->
                record_info(fields, dc_info_entry)
        end,
    mnesia:create_table(TableName,
        [{attributes, RecordInfo},
            {disc_copies, SetupMnesiaNodes}]).
