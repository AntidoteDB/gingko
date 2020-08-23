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

-module(clocksi_multi_dc_SUITE).
-include("gingko.hrl").

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
    clocksi_read_time_test/1,
    clocksi_prepare_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

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
        clocksi_prepare_test,
        clocksi_read_time_test
    ].

%% @doc This test makes sure to block pending reads when a prepare is in progress
%% that could violate atomicity if not blocked
clocksi_prepare_test(Config) ->
    Bucket = ?BUCKET,
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    Key = clocksi_test_prepare_keydscsv1,

    IndexNode = rpc:call(FirstNode, gingko_dc_utils, get_key_partition_node_tuple, [aaa]),

    Key2 = antidote_test_utils:find_key_same_node(FirstNode, IndexNode, 1),

    {ok, TxId} = rpc:call(FirstNode, antidote, start_transaction, [ignore, []]),
    antidote_test_utils:check_read_key(FirstNode, Key, antidote_crdt_counter_pn, 0, ignore, TxId, Bucket),

    antidote_test_utils:update_counters(FirstNode, [Key], [1], ignore, TxId, Bucket),
    antidote_test_utils:check_read_key(FirstNode, Key, antidote_crdt_counter_pn, 1, ignore, TxId, Bucket),

    %% Start 2 phase commit, but do not commit
    ok = rpc:call(FirstNode, gingko, prepare_txn, [TxId]),

    timer:sleep(3000),

    {ok, TxIdRead} = rpc:call(FirstNode, antidote, start_transaction, [ignore, []]),

    timer:sleep(3000),
    %% start another transaction that updates the same partition
    {ok, TxId1} = rpc:call(FirstNode, antidote, start_transaction, [ignore, []]),
    antidote_test_utils:update_counters(FirstNode, [Key2], [1], ignore, TxId1, Bucket),
    antidote_test_utils:check_read_key(FirstNode, Key2, antidote_crdt_counter_pn, 1, ignore, TxId1, Bucket),

    %% Start 2 phase commit for second transaction, but do not commit.
    rpc:call(FirstNode, gingko, prepare_txn, [TxId1]),

    %% Commit transaction after a delay, 1 in parallel,
    spawn_link(antidote_test_utils, spawn_com, [FirstNode, TxId]),

    %% Read should return the value committed by transaction 1. But should not be
    %% blocked by the transaction 2 which is in commit phase, because it updates a different key.
    antidote_test_utils:check_read_key(FirstNode, Key, antidote_crdt_counter_pn, 1, ignore, TxIdRead, Bucket),

    End1 = rpc:call(FirstNode, ginkgo, commit_txn, [TxId1]),
    ?assertMatch({ok, _CausalSnapshot}, End1),

    pass.


%% @doc The following function tests that ClockSI waits, when reading,
%%      for a tx that has updated an element that it wants to read and
%%      has a smaller TxId, but has not yet committed.
clocksi_read_time_test(Config) ->
    Bucket = ?BUCKET,
    Nodes = proplists:get_value(nodes, Config),
    %% Start a new tx,  perform an update over key abc, and send prepare.
    Key1 = clocksi_test_read_time_key1,
    FirstNode = hd(Nodes),
    LastNode = lists:last(Nodes),

    {ok, TxId} = rpc:call(FirstNode, antidote, start_transaction, [ignore, []]),
    ct:pal("Tx1 Started, id : ~p", [TxId]),
    %% start a different tx and try to read key read_time.
    {ok, TxId1} = rpc:call(LastNode, antidote, start_transaction, [ignore, []]),

    ct:pal("Tx2 Started, id : ~p", [TxId1]),
    antidote_test_utils:update_counters(FirstNode, [Key1], [1], ignore, TxId, Bucket),
    ok = rpc:call(FirstNode, gingko, prepare_txn, [TxId]),
    %% try to read key read_time.
    %% commit the first tx.
    End = rpc:call(FirstNode, gingko, commit_txn, [TxId]),
    ?assertMatch({ok, _}, End),
    ct:pal("Tx1 Committed."),

    ct:pal("Tx2 Reading..."),
    antidote_test_utils:check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 0, ignore, TxId1, Bucket),

    %% prepare and commit the second transaction.
    {ok, _CommitTime} = rpc:call(FirstNode, antidote, commit_transaction, [TxId1]),
    pass.
