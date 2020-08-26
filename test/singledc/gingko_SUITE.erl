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

-module(gingko_SUITE).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("gingko.hrl").
-export([init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2,
    all/0]).
-export([simple_integration_test/1, two_transactions/1, replication_test/1, checkpoint_test/1]).

all() ->
    [
        simple_integration_test,
        two_transactions,
        replication_test,
        checkpoint_test
    ].
%TODO reimplement
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

simple_integration_test(Config) ->
    [Cluster | _] = proplists:get_value(clusters, Config),
    Node = lists:nth(1, Cluster),
    ct:pal("Node: ~p", [Node]),
    TxId1 = rpc:call(Node, gingko, begin_txn, []),
    {Key1, Type1, TypeOp1} = {1, antidote_crdt_counter_pn, {increment, 1}},
    {Key2, Type2, TypeOp2} = {2, antidote_crdt_counter_pn, {increment, 1}},
    ok = rpc:call(Node, gingko, update, [{Key1, Type1}, TypeOp1, TxId1]),
    ok = rpc:call(Node, gingko, update, [{Key2, Type2}, TypeOp2, TxId1]),
    {ok, 1} = rpc:call(Node, gingko, read, [{Key1, Type1}, TxId1]),
    {ok, 1} = rpc:call(Node, gingko, read, [{Key2, Type2}, TxId1]),
    {ok, _CommitVts} = rpc:call(Node, gingko, prepare_and_commit_txn, [TxId1]).

simple_transaction(Node, Key, Type, TypeOp, ExpectedResult) ->
    TxId = rpc:call(Node, gingko, begin_txn, []),
    ok = rpc:call(Node, gingko, update, [{Key, Type}, TypeOp, TxId]),
    {ok, ExpectedResult} = rpc:call(Node, gingko, read, [{Key, Type}, TxId]),
    {ok, _CommitVts} = rpc:call(Node, gingko, prepare_and_commit_txn, [TxId]).

two_transactions(Config) ->
    Node = proplists:get_value(node, Config),
    TxId1 = rpc:call(Node, gingko, begin_txn, []),
    TxId2 = rpc:call(Node, gingko, begin_txn, []),

    {Key1, Type1, TypeOp1} = {1, antidote_crdt_counter_pn, {increment, 1}},
    ok = rpc:call(Node, gingko, update, [{Key1, Type1}, TypeOp1, TxId1]),
    {Key2, Type2, TypeOp2} = {1, antidote_crdt_counter_pn, {increment, 2}},
    ok = rpc:call(Node, gingko, update, [{Key2, Type2}, TypeOp2, TxId2]),

    {ok, 2} = rpc:call(Node, gingko, read, [{Key1, Type1}, TxId1]),
    {ok, 3} = rpc:call(Node, gingko, read, [{Key2, Type2}, TxId2]),

    {ok, _CommitVts1} = rpc:call(Node, gingko, prepare_and_commit_txn, [TxId1]),
    {ok, _CommitVts2} = rpc:call(Node, gingko, prepare_and_commit_txn, [TxId2]).

replication_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    case Clusters of
        [] -> error;
        [_SingleCluster1] -> ok;
        MultipleClusters1 ->
            general_utils:parallel_foreach(
                fun([FirstNode | _]) ->
                    simple_transaction(FirstNode, 3, antidote_crdt_counter_pn, {increment, 1}, 1)
                end, MultipleClusters1),
            timer:sleep(5000)
    end,

    case Clusters of
        [] -> error;
        [_SingleCluster2] -> ok;
        MultipleClusters2 ->
            NumberOfClusters = length(MultipleClusters2),
            general_utils:parallel_foreach(
                fun([FirstNode | _]) ->
                    simple_transaction(FirstNode, 3, antidote_crdt_counter_pn, {increment, 1}, NumberOfClusters + 1)
                end, MultipleClusters2)
    end.

checkpoint_test(Config) ->
    Node = proplists:get_value(node, Config),
    simple_transaction(Node, 3, antidote_crdt_counter_pn, {increment, 1}, 1),
    rpc:call(Node, gingko, checkpoint, []),
    simple_transaction(Node, 3, antidote_crdt_counter_pn, {increment, 1}, 2),
    rpc:call(Node, gingko, checkpoint, []),
    2 =  mnesia_utils:run_transaction(fun() -> mnesia:read({checkpoint_entry, 3}) end),
    ok.
