-module(gingko_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
  supervisor:start_link(gingko_sup, []).

init(_Args) ->
  Worker = {gingko_log_server,
    {gingko_log_server, start_link, [{"journal_log", "checkpoint_log"}, none]},
    permanent, 5000, worker, [gingko_log_server]},

  SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
  {ok, {SupFlags, [Worker]}}.
