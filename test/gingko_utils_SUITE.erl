%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Mär 2020 16:39
%%%-------------------------------------------------------------------
-module(gingko_utils_SUITE).
-author("kevin").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("gingko.hrl").
-export([init_per_suite/1, end_per_suite/1, end_per_testcase/2,
  all/0]).
-export([
  %group_by_test/1
]).

all() ->
  [%group_by_test
  ].

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
      [{gingko_pids, [Pid1, Pid2 | []]}|Config];
    _ ->
      {ok, Workers} = gen_server:call(gingko_master, get_gingko_workers),
      [{gingko_pids, [lists:map(fun({_Id, Pid}) -> Pid end, Workers)]}|Config]
  end.

end_per_suite(_Config) ->
  ok.

end_per_testcase(_, _Config) ->
  ok.

group_by_test(_Config) ->
  ok.
  %Jsn1 = #jsn{number = 1, dcid = 'undefined'},
  %Jsn2 = #jsn{number = 2, dcid = 'undefined'},
  %Jsn3 = #jsn{number = 3, dcid = 'undefined'},
  %JournalEntry1 = #journal_entry{jsn = Jsn1, tx_id = 1},
  %JournalEntry2 = #journal_entry{jsn = Jsn2, tx_id = 1},
  %%JournalEntry3 = #journal_entry{jsn = Jsn3, tx_id = 2},
  %List = [JournalEntry1, JournalEntry2, JournalEntry2],
  %[{1, [JournalEntry1, JournalEntry2]}, {2, [JournalEntry3]}] = gingko_utils:group_by(fun(J) -> J#journal_entry.tx_id end, List).