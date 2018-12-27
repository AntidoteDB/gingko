-module(gingko_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
  supervisor:start_link(gingko_sup, []).

init(_Args) ->
  logger:info("Starting gingko supervisor"),

%%  LoggingNode = {logging_node,
%%    {logging_node,  start_link,
%%      [logging_node]},
%%    permanent, 5000, worker, [logging_node]},

  SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
  {ok, {SupFlags, []}}.
