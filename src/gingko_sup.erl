-module(gingko_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
  supervisor:start_link(gingko_sup, []).

init(_Args) ->
  Worker = {gingko_op_log_server,
    {gingko_op_log_server, start_link, ["main_log", none]},
    permanent, 5000, worker, [gingko_op_log_server]},

  SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},


  MiddlewareConfig = [{mods,
    [{elli_prometheus, []}, {es_callback, []}]}],
  ElliOpts = [{callback, elli_middleware},
    {callback_args, MiddlewareConfig}, {port, 3001}],
  ElliSpec = {es_http, {elli, start_link, [ElliOpts]},
    permanent, 5000, worker, [elli]},


  {ok, {SupFlags, [Worker, ElliSpec]}}.
