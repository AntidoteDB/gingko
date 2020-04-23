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
-export([init_per_suite/1, end_per_suite/1, end_per_testcase/2,
    all/0]).
-export([simple_integration_test/1, two_transactions/1]).

all() ->
    [
        simple_integration_test, two_transactions
    ].
%TODO reimplement
init_per_suite(Config) ->
    Priv = ?config(priv_dir, Config),
    application:load(mnesia),
    application:set_env(mnesia, dir, Priv),
    ok = application:start(gingko_app),
    gingko_app:initial_startup_nodes([node()]),
    Config.

end_per_suite(_Config) ->
    ok.

end_per_testcase(_, _Config) ->
    ok.

simple_integration_test(_Config) ->
    CurrentTime = gingko_utils:get_timestamp(),
    TxId1 = #tx_id{local_start_time = CurrentTime, server_pid = self()},
    VC1 = vectorclock:new(),
    VC2 = vectorclock:set(undefined, CurrentTime, VC1),
    ok = gingko:begin_txn(VC2, TxId1),
    {Key1, Type1, TypeOp1} = {1, antidote_crdt_counter_pn, {increment, 1}},
    ok = gingko:update({Key1, Type1}, TypeOp1, TxId1),
    {ok, 1} = gingko:read({Key1, Type1}, TxId1),
    ok = gingko:prepare_txn(100, TxId1),
    CommitTime = gingko_utils:get_timestamp() + 2,
    VC3 = vectorclock:set(undefined, CommitTime, VC2),
    ok = gingko:commit_txn(VC3, TxId1).

two_transactions(_Config) ->
    CurrentTime1 = gingko_utils:get_timestamp(),
    CurrentTime2 = gingko_utils:get_timestamp() + 1,
    TxId1 = #tx_id{local_start_time = CurrentTime1, server_pid = self()},
    TxId2 = #tx_id{local_start_time = CurrentTime2, server_pid = self()},
    VC1 = vectorclock:new(),
    VC2 = vectorclock:set(undefined, CurrentTime1, VC1),
    VC3 = vectorclock:set(undefined, CurrentTime2, VC1),
    ok = gingko:begin_txn(VC2, TxId1),
    ok = gingko:begin_txn(VC3, TxId2),

    {Key1, Type1, TypeOp1} = {1, antidote_crdt_counter_pn, {increment, 1}},
    ok = gingko:update({Key1, Type1}, TypeOp1, TxId1),
    {Key2, Type2, TypeOp2} = {1, antidote_crdt_counter_pn, {increment, 2}},
    ok = gingko:update({Key2, Type2}, TypeOp2, TxId2),

    {ok, 2} = gingko:read({Key1, Type1}, TxId1),
    {ok, 3} = gingko:read({Key2, Type2}, TxId2),

    ok = gingko:prepare_txn(100, TxId1),
    ok = gingko:prepare_txn(100, TxId2),

    CommitTime1 = gingko_utils:get_timestamp() + 2,
    CommitTime2 = gingko_utils:get_timestamp() + 3,
    VC4 = vectorclock:set(undefined, CommitTime1, VC2),
    VC5 = vectorclock:set(undefined, CommitTime2, VC3),
    ok = gingko:commit_txn(VC4, TxId1),
    ok = gingko:commit_txn(VC5, TxId2).

checkpoint(_Config) ->
    ok.
