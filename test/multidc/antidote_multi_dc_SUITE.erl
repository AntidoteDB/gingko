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

%% @doc antidote_SUITE:
%%    Test the basic api of antidote on multiple dcs
%%    static and interactive transactions with single and multiple Objects
%%    interactive transaction with abort
-module(antidote_multi_dc_SUITE).

%% common_test callbacks
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0
]).

%% tests
-export([
    recreate_dc/1,
    dummy_test/1,
    random_test/1,
    dc_count/1
]).

-include("gingko.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
    test_utils:init_multi_dc(?MODULE, Config).

end_per_suite(Config) ->
    Config.

init_per_testcase(Name, Config) ->
    ct:pal("[ STARTING ] ~p", [Name]),
    Config.

end_per_testcase(Name, _) ->
    ct:pal("[ OK ] ~p", [Name]),
    ok.

all() ->
    [
        recreate_dc,
        dc_count,
        dummy_test,
        random_test
    ].

%% Tests that add_nodes_to_dc is idempotent
%% calling it again on each node of a dc should have no effect
recreate_dc(Config) ->
    case gingko_env_utils:get_use_single_server() of
        true -> pass;
        false ->

            [Node1, Node2 | _Nodes] = proplists:get_value(nodes, Config),

            ok = rpc:call(Node1, antidote_dc_manager, add_nodes_to_dc, [[Node1, Node2]]),
            ok = rpc:call(Node1, antidote_dc_manager, add_nodes_to_dc, [[Node1, Node2]]),
            ok = rpc:call(Node2, antidote_dc_manager, add_nodes_to_dc, [[Node1, Node2]])
    end.

dc_count(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    AllNodes = lists:flatten(Clusters),
    [First | AllOtherDcids] =
        lists:map(
            fun(Node) ->
                Result = rpc:call(Node, inter_dc_meta_data_manager, get_connected_dcids_and_mine, []),
                logger:error("Result: ~p", [Result]),
                Result
            end, AllNodes),
    logger:error("First DCIDs: ~p", [First]),
    true =
        lists:all(
            fun(List) ->
                logger:error("DCIDs: ~p", [List]),
                general_utils:set_equals_on_lists(First, List)
            end, AllOtherDcids),
    ok.

dummy_test(Config) ->
    case gingko_env_utils:get_use_single_server() of
        true -> pass;
        false ->
            Bucket = ?BUCKET,
            [[Node1, Node2] | _] = proplists:get_value(clusters, Config),
            [Node1, Node2] = proplists:get_value(nodes, Config),
            Key = antidote_key,
            Type = antidote_crdt_counter_pn,
            Object = {Key, Type, Bucket},
            Update = {Object, increment, 1},

            {ok, _} = rpc:call(Node1, antidote, update_objects, [ignore, [], [Update]]),
            {ok, _} = rpc:call(Node1, antidote, update_objects, [ignore, [], [Update]]),
            {ok, _} = rpc:call(Node2, antidote, update_objects, [ignore, [], [Update]]),
            %% Propagation of updates
            F =
                fun() ->
                    {ok, [Val], _CommitTime} = rpc:call(Node2, antidote, read_objects, [ignore, [], [Object]]),
                    Val
                end,
            Delay = 100,
            Retry = 360000 div Delay, %wait for max 1 min
            ok = time_utils:wait_until_result(F, 3, Retry, Delay)
    end.


%% Test that perform NumWrites increments to the key:key1.
%%      Each increment is sent to a random node of a random DC.
%%      Test normal behavior of the antidote
%%      Performs a read to the first node of the cluster to check whether all the
%%      increment operations where successfully applied.
%%  Variables:  N:  Number of nodes
%%              Nodes: List of the nodes that belong to the built cluster
random_test(Config) ->
    Bucket = ?BUCKET,
    Nodes = lists:flatten(proplists:get_value(clusters, Config)),
    N = length(Nodes),

    % Distribute the updates randomly over all DCs
    NumWrites = 10,
    ListIds = [rand:uniform(N) || _ <- lists:seq(1, NumWrites)], % TODO avoid non-determinism in tests

    Obj = {log_test_key1, antidote_crdt_counter_pn, Bucket},
    F =
        fun(Elem) ->
            Node = lists:nth(Elem, Nodes),
            ct:pal("Increment at node: ~p", [Node]),
            {ok, _} = rpc:call(Node, antidote, update_objects,
                [ignore, [], [{Obj, increment, 1}]])
        end,
    lists:foreach(F, ListIds),

    FirstNode = hd(Nodes),

    G =
        fun() ->
            {ok, [Res], _} = rpc:call(FirstNode, antidote, read_objects, [ignore, [], [Obj]]),
            Res
        end,
    Delay = 1000,
    Retry = 360000 div Delay, %wait for max 1 min
    ok = time_utils:wait_until_result(G, NumWrites, Retry, Delay),
    pass.
