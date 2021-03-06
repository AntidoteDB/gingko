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

-module(log_recovery_SUITE).
-include("gingko.hrl").

%% common_test callbacks
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-export([read_pncounter_log_recovery_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
    test_utils:init_single_dc(?MODULE, Config).

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
        read_pncounter_log_recovery_test
    ].


%% First we remember the initial time of the counter (with value 0).
%% After 15 updates, we kill the nodes
%% We then restart the nodes, and read the value
%% being sure that all 15 updates were loaded from the log
read_pncounter_log_recovery_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = proplists:get_value(node, Config),
    Bucket = ?BUCKET,
    Type = antidote_crdt_counter_pn,
    Key = log_value_test,
    Obj = {Key, Type, Bucket},

    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    increment_counter(Node, Obj, 15),

    %% value from old snapshot is 0
    {ok, [ReadResult1]} = rpc:call(Node,
        antidote, read_objects, [[Obj], TxId]),
    ?assertEqual(0, ReadResult1),

    %% read value in txn is 15
    {ok, [ReadResult2], CommitTime} = rpc:call(Node,
        antidote, read_objects, [ignore, [], [Obj]]),

    ?assertEqual(15, ReadResult2),

    ct:pal("Killing and restarting the nodes"),
    %% Shut down the nodes
    Nodes = test_utils:kill_and_restart_nodes(Nodes, Config),
    ct:pal("Vnodes are started up"),

    %% Read the value again
    {ok, [ReadResult3], _CT} = rpc:call(Node, antidote, read_objects,
        [CommitTime, [], [Obj]]),
    ?assertEqual(15, ReadResult3),

    pass.


%% Auxiliary method to increment a counter N times.
increment_counter(_FirstNode, _Key, 0) ->
    ok;
increment_counter(FirstNode, Obj, N) ->
    WriteResult = rpc:call(FirstNode, antidote, update_objects,
        [ignore, [], [{Obj, increment, 1}]]),
    ?assertMatch({ok, _}, WriteResult),
    increment_counter(FirstNode, Obj, N - 1).
