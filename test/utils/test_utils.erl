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

-module(test_utils).

-include_lib("eunit/include/eunit.hrl").
-include("gingko.hrl").

-define(FORCE_KILL_TIMER, 1500).
-define(RIAK_SLEEP, 5000).

-export([
    at_init_testsuite/0,
    init_single_dc/2,
    init_multi_dc/2,
    get_node_name/1,
    restart_nodes/2,
    partition_cluster/2,
    heal_cluster/2,
    start_and_connect_clusters/2
]).

%% ===========================================
%% Node utilities
%% ===========================================

-export([
    start_node/2,
    kill_nodes/1,
    kill_and_restart_nodes/2,
    brutal_kill_nodes/1
]).

%% ===========================================
%% Common Test Initialization
%% ===========================================

-spec init_single_dc(atom(), ct_config()) -> ct_config().
init_single_dc(Suite, Config) ->
    ct:pal("Initializing [~p] (Single-DC)", [Suite]),
    at_init_testsuite(),
    ClusterConfiguration =
        case gingko_env_utils:get_use_single_server() of
            true -> [[dev1]];
            false -> [[dev1]]%[[dev1, dev2, dev3, dev4]]%, dev3, dev4, dev5, dev6, dev7, dev8]],
        end,
    Clusters = start_and_connect_clusters([{suite_name, ?MODULE} | Config], ClusterConfiguration),
    Nodes = hd(Clusters),
    [{clusters, [Nodes]} | [{nodes, Nodes} | [{node, hd(Nodes)} | Config]]].

-spec init_multi_dc(atom(), ct_config()) -> ct_config().
init_multi_dc(Suite, Config) ->
    ct:pal("Initializing [~p] (Multi-DC)", [Suite]),
    at_init_testsuite(),
    ClusterConfiguration =
        case gingko_env_utils:get_use_single_server() of
            true -> [[dev1], [dev2], [dev3], [dev4]];
            false -> [[dev1, dev2], [dev3, dev4, dev5], [dev6, dev7, dev8, dev9]]
        end,
    Clusters = start_and_connect_clusters([{suite_name, ?MODULE} | Config], ClusterConfiguration),
    Nodes = hd(Clusters),
    [{clusters, Clusters} | [{nodes, Nodes} | [{node, hd(Nodes)} | Config]]].

-spec at_init_testsuite() -> ok.
at_init_testsuite() ->
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, {{already_started, _}, _}} -> ok
    end.


%% ===========================================
%% Node utilities
%% ===========================================

%% Build clusters for all test suites.
-spec start_and_connect_clusters(ct_config(), [[node()]]) -> [[node()]].
start_and_connect_clusters(Config, ClusterConfiguration) ->
    StartClusterFunc =
        fun(UnstartedCluster) ->
            %% start each node
            StartedCluster = general_utils:parallel_map(
                fun(Node) ->
                    start_node(Node, Config)
                end,
                UnstartedCluster),
            [FirstNode | OtherNodes] = StartedCluster,

            ct:pal("Creating a ring for first node ~p and other nodes ~p", [FirstNode, OtherNodes]),
            ok = rpc:call(FirstNode, gingko_app, setup_cluster, [StartedCluster]),
            StartedCluster
        end,

    StartedClusters =
        general_utils:parallel_map(
            fun(Cluster) ->
                StartClusterFunc(Cluster)
            end, ClusterConfiguration),
    case StartedClusters of
        [] -> ct:pal("No Cluster", []);
        _ ->
            %%TODO implement for gingko
            %% DCs started, but not connected yet
            general_utils:parallel_map(
                fun(CurrentCluster = [MainNode | _]) ->
                    ct:pal("~p of ~p subscribing to other external DCs", [MainNode, CurrentCluster]),

                    Descriptors =
                        lists:map(
                            fun([FirstNode | _]) ->
                                rpc:call(FirstNode, inter_dc_manager, get_descriptor, [])
                            end, StartedClusters),

                    %% subscribe to descriptors of other dcs
                    ok = rpc:call(MainNode, inter_dc_manager, connect_to_remote_dcs_and_start_dc, [Descriptors])
                end, StartedClusters)
    end,

    ct:pal("Clusters joined and data centers connected connected: ~p", [ClusterConfiguration]),
    StartedClusters.

-spec start_node(atom(), ct_config()) -> node().
start_node(Name, Config) ->
    %% code path for compiled dependencies (ebin folders)
    {ok, Cwd} = file:get_cwd(),
    GingkoFolder = filename:dirname(filename:dirname(Cwd)),
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    ct:pal("Starting node ~p", [Name]),

    PrivDir = proplists:get_value(priv_dir, Config),
    NodeDir = filename:join([PrivDir, Name]) ++ "/",
    ok = filelib:ensure_dir(NodeDir),

    %% have the slave nodes monitor the runner node, so they can't outlive it
    NodeConfig =
        [
            %% have the slave nodes monitor the runner node, so they can't outlive it
            {monitor_master, true},

            %% set code path for dependencies
            {startup_functions, [{code, set_path, [CodePath]}]}
        ],
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            % load application to allow for configuring the environment before starting
            ok = rpc:call(Node, application, load, [riak_core]),
            ok = rpc:call(Node, application, load, [mnesia]),
            ok = rpc:call(Node, application, load, [?GINGKO_APP_NAME]),


            %% get remote working dir of node
            {ok, NodeWorkingDir} = rpc:call(Node, file, get_cwd, []),

            %% DATA DIRS
            ok = rpc:call(Node, application, set_env, [riak_core, ring_state_dir, filename:join([NodeWorkingDir, Node, "riak_data"])]),
            ok = rpc:call(Node, application, set_env, [riak_core, platform_data_dir, filename:join([NodeWorkingDir, Node, "riak_data"])]),
            ok = rpc:call(Node, application, set_env, [riak_core, schema_dirs, [GingkoFolder ++ "/_build/default/rel/gingko_app/lib/"]]),
            ok = rpc:call(Node, gingko_env_utils, set_data_dir, [filename:join([NodeWorkingDir, Node, "gingko_data"])]),

            Port = web_ports(Name),
            ok = rpc:call(Node, gingko_env_utils, set_request_port, [Port]),
            ok = rpc:call(Node, gingko_env_utils, set_txn_port, [Port + 1]),
            ok = rpc:call(Node, application, set_env, [riak_core, handoff_port, Port + 3]),
            ok = rpc:call(Node, application, set_env, [mnesia, debug, trace]),

            %% LOGGING Configuration
            %% add additional logging handlers to ensure easy access to remote node logs
            %% for each logging level
            LogRoot = filename:join([NodeWorkingDir, Node, "logs"]),
            %% set the logger configuration
            ok = rpc:call(Node, application, set_env, [?GINGKO_APP_NAME, logger, log_config(LogRoot)]),
            %% set primary output level, no filter
            rpc:call(Node, logger, set_primary_config, [level, all]),
            %% load additional logger handlers at remote node
            rpc:call(Node, logger, add_handlers, [?GINGKO_APP_NAME]),

            %% redirect slave logs to ct_master logs
            ok = rpc:call(Node, application, set_env, [?GINGKO_APP_NAME, ct_master, node()]),
            ConfLog = #{level => debug, formatter => {logger_formatter, #{single_line => false}}, config => #{type => standard_io}},
            _ = rpc:call(Node, logger, add_handler, [gingko_redirect_ct, ct_redirect_handler, ConfLog]),

            %% ANTIDOTE Configuration
            %% reduce number of actual log files created to 4, reduces start-up time of node
            ok = rpc:call(Node, application, set_env, [riak_core, ring_creation_size, 4]),
            {ok, _} = rpc:call(Node, application, ensure_all_started, [?GINGKO_APP_NAME]),
            ct:pal("Node ~p started", [Node]),
            Node;
        {error, already_started, Node} ->
            ct:pal("Node ~p already started, reusing node", [Node]),
            Node;
        {error, Reason, Node} ->
            ct:pal("Error starting node ~w, reason ~w, will retry", [Node, Reason]),
            ct_slave:stop(Name),
            time_utils:wait_until_offline(Node),
            start_node(Name, Config)
    end.

-spec web_ports(atom()) -> inet:port_number().
web_ports(dev1) -> 10015;
web_ports(dev2) -> 10025;
web_ports(dev3) -> 10035;
web_ports(dev4) -> 10045;
web_ports(dev5) -> 10055;
web_ports(dev6) -> 10065;
web_ports(dev7) -> 10075;
web_ports(dev8) -> 10085;
web_ports(dev9) -> 10095;
web_ports(dev10) -> 10105;
web_ports(dev11) -> 10115;
web_ports(dev12) -> 10125;
web_ports(dev13) -> 10135;
web_ports(dev14) -> 10145;
web_ports(dev15) -> 10155;
web_ports(dev16) -> 10165.

%% @doc Forces shutdown of nodes and restarts them again with given configuration
-spec kill_and_restart_nodes([node()], [tuple()]) -> [node()].
kill_and_restart_nodes(NodeList, Config) ->
    NewNodeList = brutal_kill_nodes(NodeList),
    restart_nodes(NewNodeList, Config).


%% @doc Kills all given nodes, crashes if one node cannot be stopped
-spec kill_nodes([node()]) -> [node()].
kill_nodes(NodeList) ->
    lists:map(fun(Node) -> {ok, Name} = ct_slave:stop(get_node_name(Node)), Name end, NodeList).


%% @doc Send force kill signals to all given nodes
-spec brutal_kill_nodes([node()]) -> [node()].
brutal_kill_nodes(NodeList) ->
    lists:map(
        fun(Node) ->
            ct:pal("Killing node ~p", [Node]),
            OSPidToKill = rpc:call(Node, os, getpid, []),
            %% try a normal kill first, but set a timer to
            %% kill -9 after X seconds just in case
            %%rpc:cast(Node, timer, apply_after,
            %%[?FORCE_KILL_TIMER, os, cmd, [io_lib:format("kill -9 ~s", [OSPidToKill])]]),
            ct_slave:stop(get_node_name(Node)),
            rpc:cast(Node, os, cmd, [io_lib:format("kill -15 ~s", [OSPidToKill])]),
            Node
        end, NodeList).


%% @doc Restart nodes with given configuration
-spec restart_nodes([node()], ct_config()) -> [node()].
restart_nodes(NodeList, Config) ->
    general_utils:parallel_map(
        fun(Node) ->
            ct:pal("Restarting node ~p", [Node]),

            ct:pal("Starting and waiting until vnodes are restarted at node ~w", [Node]),
            start_node(get_node_name(Node), Config),

            ct:pal("Waiting until ring converged @ ~p", [Node]),
            riak_utils:wait_until_ring_converged([Node])
        end, NodeList),
    NodeList.


%% @doc Convert node to node atom
-spec get_node_name(node()) -> atom().
get_node_name(NodeAtom) ->
    Node = atom_to_list(NodeAtom),
    {match, [{Pos, _Len}]} = re:run(Node, "@"),
    list_to_atom(string:substr(Node, 1, Pos)).

-spec partition_cluster([node()], [node()]) -> ok.
partition_cluster(ANodes, BNodes) ->
    general_utils:parallel_map(
        fun({Node1, Node2}) ->
            true = rpc:call(Node1, erlang, set_cookie, [Node2, canttouchthis]),
            true = rpc:call(Node1, erlang, disconnect_node, [Node2]),
            ok = time_utils:wait_until_disconnected(Node1, Node2)
        end, [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.

-spec heal_cluster([node()], [node()]) -> ok.
heal_cluster(ANodes, BNodes) ->
    GoodCookie = erlang:get_cookie(),
    general_utils:parallel_map(
        fun({Node1, Node2}) ->
            true = rpc:call(Node1, erlang, set_cookie, [Node2, GoodCookie]),
            ok = time_utils:wait_until_connected(Node1, Node2)
        end, [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.


%% logger configuration for each level
%% see http://erlang.org/doc/man/logger.html
-spec log_config(file:filename()) -> [logger:handler_config()].
log_config(LogDir) ->
    DebugConfig = #{level => debug,
        formatter => {logger_formatter, #{single_line => false}},
        config => #{type => {file, filename:join(LogDir, "debug.log")}}},

    InfoConfig = #{level => info,
        formatter => {logger_formatter, #{single_line => false}},
        config => #{type => {file, filename:join(LogDir, "info.log")}}},

    ErrorConfig = #{level => error,
        formatter => {logger_formatter, #{single_line => false}},
        config => #{type => {file, filename:join(LogDir, "error.log")}}},

    [
        {handler, debug_gingko, logger_std_h, DebugConfig},
        {handler, info_gingko, logger_std_h, InfoConfig},
        {handler, error_gingko, logger_std_h, ErrorConfig}
    ].
