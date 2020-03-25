%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Mär 2020 16:39
%%%-------------------------------------------------------------------
-module(gingko_SUITE).
-author("kevin").

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

init_per_suite(Config) ->
  Priv = ?config(priv_dir, Config),
  ok = application:load(mnesia),
  application:set_env(mnesia, dir, Priv),
  ok = application:load(gingko_app),
  gingko_app:install([node()]),
  application:start(mnesia),
  application:start(gingko_app),
  [{gingko_app_pid, gingko}|Config].

end_per_suite(_Config) ->
  ok = application:stop(mnesia).

end_per_testcase(_, _Config) ->
  ok.

simple_integration_test(Config) ->
  gingko_log:clear_journal_entries(),
  Pid = ?config(gingko_app_pid, Config),
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
  ok = gen_server:call(Pid, {{commit_txn, {VC3, VC3}}, TxId1}).

two_transactions(Config) ->
  Pid = ?config(gingko_app_pid, Config),
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
  ok = gen_server:call(Pid, {{commit_txn, {VC4, VC4}}, TxId1}),
  ok = gen_server:call(Pid, {{commit_txn, {VC5, VC5}}, TxId1}).