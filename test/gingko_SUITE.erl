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
-export([]).

all() ->
  [
  ].

init_per_suite(Config) ->
  Priv = ?config(priv_dir, Config),
  application:load(mnesia),
  application:set_env(mnesia, dir, Priv),
  application:load(gingko_app),
  gingko_app:install([node()]),
  application:start(mnesia),
  application:start(gingko_app),
  Config.

end_per_suite(_Config) ->
  application:stop(gingko_app),
  ok.

end_per_testcase(_, _Config) ->
  ok.