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

-module(bcountermgr_multi_dc_SUITE).
-include("gingko.hrl").

%% common_test callbacks
-export([init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-export([
    test_dec_success/1,
    test_dec_fail/1,
    test_dec_multi_success0/1,
    test_dec_multi_success1/1,
    conditional_write_test_run/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TYPE, antidote_crdt_counter_b).
-define(RETRY_COUNT, 10).

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
        test_dec_success,
        test_dec_fail,
        test_dec_multi_success0,
        test_dec_multi_success1,
        conditional_write_test_run
    ].


test_dec_success(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] = [hd(Cluster) || Cluster <- Clusters],
    Actor = dc,
    Key = bcounter2_mgr,

    {ok, _} = execute_op(Node1, increment, Key, 10, Actor, Bucket),
    {ok, CommitTime} = execute_op(Node1, decrement, Key, 4, Actor, Bucket),

    % FIXME why is this not working?
    %{Value, _} = antidote_test_utils:read_b_counter_commit(Node2, Key, Bucket, CommitTime),
    %?assertEqual(6, Value).
    check_read(Node2, Key, 6, CommitTime, Bucket).


test_dec_fail(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] = [hd(Cluster) || Cluster <- Clusters],
    Actor = dc,
    Key = bcounter3_mgr,

    {ok, CommitTime} = execute_op(Node1, increment, Key, 10, Actor, Bucket),
    _ForcePropagation = read_si(Node2, Key, CommitTime, Bucket),
    Result0 = execute_op_success(Node2, decrement, Key, 5, Actor, 0, Bucket),
    ?assertEqual({error, no_permissions}, Result0).


test_dec_multi_success0(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] = [hd(Cluster) || Cluster <- Clusters],
    Actor = dc,
    Key = bcounter4_mgr,

    {ok, _} = execute_op(Node1, increment, Key, 10, Actor, Bucket),
    {ok, CommitTime} = execute_op(Node2, decrement, Key, 5, Actor, Bucket),
    check_read(Node1, Key, 5, CommitTime, Bucket).


test_dec_multi_success1(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] = [hd(Cluster) || Cluster <- Clusters],
    Actor = dc,
    Key = bcounter5_mgr,

    {ok, _} = execute_op(Node1, increment, Key, 10, Actor, Bucket),
    {ok, _} = execute_op(Node2, decrement, Key, 5, Actor, Bucket),
    {error, no_permissions} = execute_op(Node1, decrement, Key, 6, Actor, Bucket),
    check_read(Node1, Key, 5, Bucket).


conditional_write_test_run(Config) ->
    case gingko_env_utils:get_use_single_server() of
        true -> pass;
        false ->
            Bucket = ?BUCKET,
            Nodes = proplists:get_value(nodes, Config),
            [Node1, Node2 | _OtherNodes] = Nodes,
            Type = antidote_crdt_counter_b,
            Key = bcounter6_mgr,
            BObj = {Key, Type, Bucket},

            {ok, AfterIncrement} = execute_op(Node1, increment, Key, 10, r1, Bucket),

            %% Start a transaction on the first node and perform a read operation.
            {ok, TxId1} = rpc:call(Node1, antidote, start_transaction, [AfterIncrement, []]),
            {ok, _} = rpc:call(Node1, antidote, read_objects, [[BObj], TxId1]),
            %% Execute a transaction on the last node which performs a write operation.
            {ok, TxId2} = rpc:call(Node2, antidote, start_transaction, [AfterIncrement, []]),
            ok = rpc:call(Node2, antidote, update_objects,
                [[{BObj, decrement, {3, r1}}], TxId2]),
            End1 = rpc:call(Node2, antidote, commit_transaction, [TxId2]),
            ?assertMatch({ok, _}, End1),
            {ok, AfterTxn2} = End1,
            %% Resume the first transaction and check that it fails.
            Result0 = rpc:call(Node1, antidote, update_objects,
                [[{BObj, decrement, {3, r1}}], TxId1]),
            ?assertEqual(ok, Result0),
            CommitResult = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
            ?assertMatch({error, aborted}, CommitResult),
            %% Test that the failed transaction didn't affect the `bcounter()'.
            check_read(Node1, Key, 7, AfterTxn2, Bucket)
    end.


%% TODO move to antidote_test_utils

execute_op(Node, Op, Key, Amount, Actor, Bucket) ->
    execute_op_success(Node, Op, Key, Amount, Actor, ?RETRY_COUNT, Bucket).


%%Auxiliary functions.
execute_op_success(Node, Op, Key, Amount, Actor, Try, Bucket) ->
    ct:pal("Execute OP ~p", [Key]),
    Result = rpc:call(Node, antidote, update_objects,
        [ignore, [],
            [{{Key, ?TYPE, Bucket}, Op, {Amount, Actor}}]
        ]
    ),
    case Result of
        {ok, CommitTime} -> {ok, CommitTime};
        Error when Try == 0 -> Error;
        _ ->
            timer:sleep(1000),
            execute_op_success(Node, Op, Key, Amount, Actor, Try - 1, Bucket)
    end.


read_si(Node, Key, CommitTime, Bucket) ->
    ct:pal("Read si ~p", [Key]),
    rpc:call(Node, antidote, read_objects, [CommitTime, [], [{Key, ?TYPE, Bucket}]]).


check_read(Node, Key, Expected, CommitTime, Bucket) ->
    {ok, [Obj], _CT} = read_si(Node, Key, CommitTime, Bucket),
    ?assertEqual(Expected, ?TYPE:permissions(Obj)).

check_read(Node, Key, Expected, Bucket) ->
    check_read(Node, Key, Expected, ignore, Bucket).
