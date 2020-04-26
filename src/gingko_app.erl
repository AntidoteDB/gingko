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
-include("gingko.hrl").

-behaviour(application).

-export([start/2, stop/1, get_default_config/0, initial_startup_nodes/1]).

-spec(start(StartType :: normal | {takeover, node()} | {failover, node()},
    StartArgs :: term()) ->
    {ok, pid()} |
    {ok, pid(), State :: term()} |
    {error, Reason :: term()}).
start(_StartType, _StartArgs) ->
    case gingko_sup:start_link() of
        {ok, Pid} ->
            case ?USE_SINGLE_SERVER of
                true -> ok;
                false ->
                    ok = riak_core:register([{vnode_module, gingko_log_vnode}]),
                    ok = riak_core_node_watcher:service_up(gingko_log, self()),
                    ok = riak_core:register([{vnode_module, gingko_vnode}]),
                    ok = riak_core_node_watcher:service_up(gingko, self()),
                    ok = riak_core:register([{vnode_module, gingko_cache_vnode}]),
                    ok = riak_core_node_watcher:service_up(gingko_cache, self()),
                    ok = antidote_utilities:ensure_all_vnodes_running_master(?GINGKO_VNODE_MASTER),
                    ok = antidote_utilities:ensure_all_vnodes_running_master(?GINGKO_LOG_VNODE_MASTER),
                    ok = antidote_utilities:ensure_all_vnodes_running_master(?GINGKO_CACHE_VNODE_MASTER)
            end,
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

-spec(stop(State :: term()) -> term()).
stop(_State) ->
    ok.

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
                    ok = make_sure_checkpoint_store_is_running(Nodes),
                    gingko_utils:bcast_gingko_sync(?GINGKO_LOG, setup_journal_mnesia_table),
                    TableNames = gingko_utils:bcast_gingko_sync(?GINGKO_LOG, get_journal_mnesia_table_name),
                    ok = mnesia:wait_for_tables([checkpoint_entry | TableNames], 5000), %TODO error handling
                    ok; %TODO setup fresh
                _ ->
                    NodesWithoutSchema = sets:to_list(sets:subtract(sets:from_list(Nodes), sets:from_list(NodesWithSchema))),
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
                                            ok = make_sure_checkpoint_store_is_running(Nodes),
                                            gingko_utils:bcast_gingko_sync(?GINGKO_LOG, setup_journal_mnesia_table),
                                            ok; %TODO checkpoint setup and inform all vnodes
                                        false ->
                                            PartiallyPerfect = lists:all(fun(MnesiaNodes) ->
                                                ordsets:is_subset(MnesiaNodes, NodesWithSchemaOrdSet) end, MnesiaNodesList),
                                            case PartiallyPerfect of
                                                true ->
                                                    %TODO fix setup (partially broken mnesia cluster)
                                                    ok;
                                                false ->
                                                    %TODO fix setup (completely broken mnesia cluster)
                                                    ok

                                            end
                                    end
                            end

                    end
            end
    end.

%%TODO maybe do check other properties of the table
%%TODO failure testing is required to check whether this works as intended
-spec make_sure_checkpoint_store_is_running([node()]) -> ok | {error, reason()}.
make_sure_checkpoint_store_is_running([]) -> {error, "At least one node is required!"};
make_sure_checkpoint_store_is_running(SetupMensiaNodes) ->
    %%TODO currently we only allow checkpoint_entry table to be disc_copies
    Tables = mnesia:system_info(tables),
    case lists:member(checkpoint_entry, Tables) of
        true ->
            RamCopiesNodes = mnesia:table_info(checkpoint_entry, ram_copies),
            DiscOnlyNodes = mnesia:table_info(checkpoint_entry, disc_only_copies),
            lists:foreach(
                fun(Node) ->
                    {atomic, ok} = mnesia:change_table_copy_type(checkpoint_entry, Node, disc_copies)
                end, RamCopiesNodes ++ DiscOnlyNodes),
            %%TODO maybe wait_for_tables?
            NodesWhereTableIsSetup = mnesia:table_info(checkpoint_entry, disc_copies),
            case length(NodesWhereTableIsSetup) of
                0 ->
                    {atomic, ok} = mnesia:create_table(checkpoint_entry,
                        [{attributes, record_info(fields, checkpoint_entry)},
                            {disc_copies, SetupMensiaNodes}]);
                _ ->
                    lists:foreach(
                        fun(Node) ->
                            case lists:member(Node, NodesWhereTableIsSetup) of
                                false -> {atomic, ok} = mnesia:add_table_copy(checkpoint_entry, Node, disc_copies);
                                true -> ok
                            end
                        end, SetupMensiaNodes)
            end,
            ok;
        false ->
            {atomic, ok} = mnesia:create_table(checkpoint_entry,
                [{attributes, record_info(fields, checkpoint_entry)},
                    {disc_copies, SetupMensiaNodes}]),
            ok
    end.


