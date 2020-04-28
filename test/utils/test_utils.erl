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
    bucket/1,
    init_single_dc/2,
    init_multi_dc/2,
    get_node_name/1,
    restart_nodes/2,
    partition_cluster/2,
    heal_cluster/2,
    set_up_clusters_common/1,
    unpack/1
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

init_single_dc(Suite, Config) ->
    ct:pal("[~p]", [Suite]),
    at_init_testsuite(),

    StartDCs = fun(Nodes) ->
        general_utils:parallel_map(fun(N) -> {_Status, Node} = start_node(N, Config), Node end, Nodes)
               end,
    [Nodes] = general_utils:parallel_map( fun(N) -> StartDCs(N) end, [[dev1]] ),
    [Node] = Nodes,

    [{clusters, [Nodes]} | [{nodes, Nodes} | [{node, Node} | Config]]].


init_multi_dc(Suite, Config) ->
    ct:pal("[~p]", [Suite]),

    at_init_testsuite(),
    Clusters = set_up_clusters_common([{suite_name, ?MODULE} | Config]),
    Nodes = hd(Clusters),
    [{clusters, Clusters} | [{nodes, Nodes} | Config]].


at_init_testsuite() ->
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, {{already_started, _}, _}} -> ok
    end.


%% ===========================================
%% Node utilities
%% ===========================================

start_node(Name, Config) ->
    %% code path for compiled dependencies (ebin folders)
    {ok, Cwd} = file:get_cwd(),
    GingkoFolder = filename:dirname(filename:dirname(Cwd)),
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    ct:pal("Starting node ~p", [Name]),


    PrivDir = proplists:get_value(priv_dir, Config),
    NodeDir = filename:join([PrivDir, Name]) ++ "/",
    filelib:ensure_dir(NodeDir),

    %% have the slave nodes monitor the runner node, so they can't outlive it
    NodeConfig = [
        %% have the slave nodes monitor the runner node, so they can't outlive it
        {monitor_master, true},

        %% set code path for dependencies
        {startup_functions, [ {code, set_path, [CodePath]} ]}],
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            % load application to allow for configuring the environment before starting
            ok = rpc:call(Node, application, load, [riak_core]),
            ok = rpc:call(Node, application, load, [mnesia]),
            ok = rpc:call(Node, application, load, [?GINGKO_APP_NAME]),


            %% get remote working dir of node
            {ok, NodeWorkingDir} = rpc:call(Node, file, get_cwd, []),

            %% DATA DIRS
            ok = rpc:call(Node, application, set_env, [?GINGKO_APP_NAME, data_dir, filename:join([NodeWorkingDir, Node, "gingko_data"])]),
            ok = rpc:call(Node, application, set_env, [riak_core, ring_state_dir, filename:join([NodeWorkingDir, Node, "data"])]),
            ok = rpc:call(Node, application, set_env, [riak_core, platform_data_dir, filename:join([NodeWorkingDir, Node, "data"])]),
            ok = rpc:call(Node, application, set_env, [riak_core, schema_dirs, [GingkoFolder ++ "/_build/default/rel/gingko_app/lib/"]]),
            ok = rpc:call(Node, application, set_env, [mnesia, dir, filename:join([NodeWorkingDir, Node, "gingko_data"])]),


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
            ConfLog = #{level => debug, formatter => {logger_formatter, #{single_line => true, max_size => 2048}}, config => #{type => standard_io}},
            _ = rpc:call(Node, logger, add_handler, [gingko_redirect_ct, ct_redirect_handler, ConfLog]),

            %% ANTIDOTE Configuration
            %% reduce number of actual log files created to 4, reduces start-up time of node
            ok = rpc:call(Node, application, set_env, [riak_core, ring_creation_size, 4]),
            {ok, _} = rpc:call(Node, application, ensure_all_started, [?GINGKO_APP_NAME]),
            ok = rpc:call(Node, gingko_app, initial_startup_nodes, [[Node]]),
            ct:pal("Node ~p started", [Node]),

            {connect, Node};
        {error, already_started, Node} ->
            ct:log("Node ~p already started, reusing node", [Node]),
            {ready, Node};
        {error, Reason, Node} ->
            ct:pal("Error starting node ~w, reason ~w, will retry", [Node, Reason]),
            ct_slave:stop(Name),
            time_utils:wait_until_offline(Node),
            start_node(Name, Config)
    end.


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
    lists:map(fun(Node) ->
                  ct:pal("Killing node ~p", [Node]),
                  OSPidToKill = rpc:call(Node, os, getpid, []),
                  %% try a normal kill first, but set a timer to
                  %% kill -9 after X seconds just in case
%%                  rpc:cast(Node, timer, apply_after,
%%                      [?FORCE_KILL_TIMER, os, cmd, [io_lib:format("kill -9 ~s", [OSPidToKill])]]),
                  ct_slave:stop(get_node_name(Node)),
                  rpc:cast(Node, os, cmd, [io_lib:format("kill -15 ~s", [OSPidToKill])]),
                  Node
              end, NodeList).


%% @doc Restart nodes with given configuration
-spec restart_nodes([node()], [tuple()]) -> [node()].
restart_nodes(NodeList, Config) ->
    general_utils:parallel_map(fun(Node) ->
        ct:pal("Restarting node ~p", [Node]),

        ct:log("Starting and waiting until vnodes are restarted at node ~w", [Node]),
        start_node(get_node_name(Node), Config),

        ct:log("Waiting until ring converged @ ~p", [Node]),
        riak_utils:wait_until_ring_converged([Node]),

        ct:log("Waiting until ready @ ~p", [Node]),
        time_utils:wait_until(Node, fun wait_init:check_ready/1),
        Node
         end, NodeList).


%% @doc Convert node to node atom
-spec get_node_name(node()) -> atom().
get_node_name(NodeAtom) ->
    Node = atom_to_list(NodeAtom),
    {match, [{Pos, _Len}]} = re:run(Node, "@"),
    list_to_atom(string:substr(Node, 1, Pos)).

partition_cluster(ANodes, BNodes) ->
    general_utils:parallel_map(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, canttouchthis]),
                true = rpc:call(Node1, erlang, disconnect_node, [Node2]),
                ok = time_utils:wait_until_disconnected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.


heal_cluster(ANodes, BNodes) ->
    GoodCookie = erlang:get_cookie(),
    general_utils:parallel_map(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, GoodCookie]),
                ok = time_utils:wait_until_connected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.


%% Build clusters for all test suites.
set_up_clusters_common(Config) ->
    ClusterAndDcConfiguration = [[dev1, dev2], [dev3], [dev4]],

    StartDCs = fun(Nodes) ->
        %% start each node
        Cl = general_utils:parallel_map(fun(N) ->
            start_node(N, Config)
                  end,
            Nodes),
        [{Status, Claimant} | OtherNodes] = Cl,

        %% check if node was reused or not
        case Status of
            ready -> ok;
            connect ->
                ct:pal("Creating a ring for claimant ~p and other nodes ~p", [Claimant, unpack(OtherNodes)]),
                ok = rpc:call(Claimant, antidote_dc_manager, add_nodes_to_dc, [unpack(Cl)])
        end,
        Cl
               end,

    Clusters = general_utils:parallel_map(fun(Cluster) ->
        StartDCs(Cluster)
                    end, ClusterAndDcConfiguration),

    %% DCs started, but not connected yet
    general_utils:parallel_map(fun([{Status, MainNode} | _] = CurrentCluster) ->
        case Status of
            ready -> ok;
            connect ->
                ct:pal("~p of ~p subscribing to other external DCs", [MainNode, unpack(CurrentCluster)]),

                Descriptors = lists:map(fun([{_Status, FirstNode} | _]) ->
                    {ok, Descriptor} = rpc:call(FirstNode, antidote_dc_manager, get_connection_descriptor, []),
                    Descriptor
                                        end, Clusters),

                %% subscribe to descriptors of other dcs
                ok = rpc:call(MainNode, antidote_dc_manager, subscribe_updates_from, [Descriptors])
        end
         end, Clusters),


    ct:log("Clusters joined and data centers connected connected: ~p", [ClusterAndDcConfiguration]),
    [unpack(DC) || DC <- Clusters].


bucket(BucketBaseAtom) ->
    BucketRandomSuffix = [rand:uniform(127)],
    Bucket = list_to_atom(atom_to_list(BucketBaseAtom) ++ BucketRandomSuffix),
    ct:log("Using random bucket: ~p", [Bucket]),
    Bucket.


%% logger configuration for each level
%% see http://erlang.org/doc/man/logger.html
log_config(LogDir) ->
    DebugConfig = #{level => debug,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "debug.log")}}},

    InfoConfig = #{level => info,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "info.log")}}},

    NoticeConfig = #{level => notice,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "notice.log")}}},

    WarningConfig = #{level => warning,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "warning.log")}}},

    ErrorConfig = #{level => error,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "error.log")}}},

    [
        {handler, debug_antidote, logger_std_h, DebugConfig},
        {handler, info_antidote, logger_std_h, InfoConfig},
        {handler, notice_antidote, logger_std_h, NoticeConfig},
        {handler, warning_antidote, logger_std_h, WarningConfig},
        {handler, error_antidote, logger_std_h, ErrorConfig}
    ].

-spec unpack([{ready | connect, atom()}]) -> [atom()].
unpack(NodesWithStatus) ->
    [Node || {_Status, Node} <- NodesWithStatus].
