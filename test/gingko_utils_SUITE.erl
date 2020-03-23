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
  ok = application:load(mnesia),
  application:set_env(mnesia, dir, Priv),
  ok = application:load(gingko_app),
  gingko_app:install([node()]),
  application:start(mnesia),
  application:start(gingko_app),
  [{gingko_app_pid, gingko}|Config].

end_per_suite(_Config) ->
  ok.

end_per_testcase(_, _Config) ->
  ok.

group_by_test(_Config) ->
  Jsn1 = #jsn{number = 1, dcid = 'undefined'},
  Jsn2 = #jsn{number = 2, dcid = 'undefined'},
  Jsn3 = #jsn{number = 3, dcid = 'undefined'},
  JournalEntry1 = #journal_entry{jsn = Jsn1, tx_id = 1},
  JournalEntry2 = #journal_entry{jsn = Jsn2, tx_id = 1},
  JournalEntry3 = #journal_entry{jsn = Jsn3, tx_id = 2},
  List = [JournalEntry1, JournalEntry2, JournalEntry2],
  [{1, [JournalEntry1, JournalEntry2]}, {2, [JournalEntry3]}] = gingko_utils:group_by(fun(J) -> J#journal_entry.tx_id end, List).