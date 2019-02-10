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

  Metrics = #{
    id => metrics,
    start => {gingko_metrics_server, start_link, []},
    restart => permanent,
    shutdown => 1000,
    type => supervisor,
    modules => [gingko_metrics_server]},

  SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},

  {ok, {SupFlags, [Worker, Metrics]}}.
