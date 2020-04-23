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
    application:load(gingko_app),
    mnesia:create_schema([node()]),
    application:start(mnesia),
    case application:start(gingko_app) of
        ok ->
            WorkerId1 = gingko1,
            WorkerId2 = gingko2,
            DcId = undefined,
            GingkoConfig =
                [
                    {checkpoint_interval_millis, 10000},
                    {max_occupancy, 100},
                    {reset_used_interval_millis, 1000},
                    {eviction_interval_millis, 2000},
                    {eviction_threshold_in_percent, 90},
                    {target_threshold_in_percent, 80},
                    {eviction_strategy, interval}
                ],
            {ok, Pid1} = gen_server:call(gingko_master, {start_gingko_worker, {WorkerId1, DcId, GingkoConfig}}),
            {ok, Pid2} = gen_server:call(gingko_master, {start_gingko_worker, {WorkerId2, DcId, GingkoConfig}}),
            [{gingko_pids, [Pid1, Pid2 | []]} | Config];
        _ ->
            {ok, Workers} = gen_server:call(gingko_master, get_gingko_workers),
            [{gingko_pids, [lists:map(fun({_Id, Pid}) -> Pid end, Workers)]} | Config]
    end.

end_per_suite(_Config) ->
    ok = application:stop(mnesia).

end_per_testcase(_, _Config) ->
    ok.

simple_integration_test(Config) ->
    {atomic, ok} = gingko_log:clear_journal_entries(gingko2_journal_entry),
    Pid = lists:nth(1, ?config(gingko_pids, Config)),
    CurrentTime = gingko_utils:get_timestamp(),
    TxId1 = #tx_id{local_start_time = CurrentTime, server_pid = self()},
    VC1 = vectorclock:new(),
    VC2 = vectorclock:set(undefined, CurrentTime, VC1),
    ok = gen_server:call(Pid, {{begin_txn, VC2}, TxId1}),
    {Key1, Type1, TypeOp1} = {1, antidote_crdt_counter_pn, {increment, 1}},
    ok = gen_server:call(Pid, {{update, {Key1, Type1, TypeOp1}}, TxId1}),
    {ok, 1} = gen_server:call(Pid, {{read, {Key1, Type1}}, TxId1}),
    ok = gen_server:call(Pid, {{prepare_txn, 100}, TxId1}),
    CommitTime = gingko_utils:get_timestamp() + 2,
    VC3 = vectorclock:set(undefined, CommitTime, VC2),
    ok = gen_server:call(Pid, {{commit_txn, VC3}, TxId1}).

two_transactions(Config) ->
    Pid = lists:nth(1, ?config(gingko_pids, Config)),
    CurrentTime1 = gingko_utils:get_timestamp(),
    CurrentTime2 = gingko_utils:get_timestamp() + 1,
    TxId1 = #tx_id{local_start_time = CurrentTime1, server_pid = self()},
    TxId2 = #tx_id{local_start_time = CurrentTime2, server_pid = self()},
    VC1 = vectorclock:new(),
    VC2 = vectorclock:set(undefined, CurrentTime1, VC1),
    VC3 = vectorclock:set(undefined, CurrentTime2, VC1),
    ok = gen_server:call(Pid, {{begin_txn, VC2}, TxId1}),
    ok = gen_server:call(Pid, {{begin_txn, VC3}, TxId2}),

    {Key1, Type1, TypeOp1} = {1, antidote_crdt_counter_pn, {increment, 1}},
    ok = gen_server:call(Pid, {{update, {Key1, Type1, TypeOp1}}, TxId1}),
    {Key2, Type2, TypeOp2} = {1, antidote_crdt_counter_pn, {increment, 2}},
    ok = gen_server:call(Pid, {{update, {Key2, Type2, TypeOp2}}, TxId2}),

    {ok, 2} = gen_server:call(Pid, {{read, {Key1, Type1}}, TxId1}),
    {ok, 3} = gen_server:call(Pid, {{read, {Key2, Type2}}, TxId2}),

    ok = gen_server:call(Pid, {{prepare_txn, 100}, TxId1}),
    ok = gen_server:call(Pid, {{prepare_txn, 100}, TxId2}),

    CommitTime1 = gingko_utils:get_timestamp() + 2,
    CommitTime2 = gingko_utils:get_timestamp() + 3,
    VC4 = vectorclock:set(undefined, CommitTime1, VC2),
    VC5 = vectorclock:set(undefined, CommitTime2, VC3),
    ok = gen_server:call(Pid, {{commit_txn, VC4}, TxId1}),
    ok = gen_server:call(Pid, {{commit_txn, VC5}, TxId1}).

checkpoint(Config) ->
    Pid = ?config(gingko_app_pid, Config),
    %VC1 = vectorclock:new(),
    %VC2 = vectorclock:set(undefined, CurrentTime1, VC1),
    %ok = gen_server:call(Pid, {{begin_txn, VC2}, TxId1}),
    ok.
