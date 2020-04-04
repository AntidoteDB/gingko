-module(gingko_sup).
-behaviour(supervisor).

-include("gingko.hrl").

-export([start_link/0, start_gingko_server/3]).
-export([init/1]).

-spec start_gingko_server(atom(), dcid(), map_list()) -> {ok, pid()} | {error, reason()}.
start_gingko_server(Id, DcId, Args) ->
  case supervisor:start_child(?MODULE, get_gingko_config(Id, [{id, Id}, {dcid, DcId} | Args])) of
    {ok, Pid1} -> {ok, Pid1};
    {ok, Pid2, _} -> {ok, Pid2};
    Error -> {error, Error}
  end.

-spec get_gingko_config(atom(), map_list()) -> supervisor:child_spec().
get_gingko_config(Id, Args) ->
  #{id => Id,
    start => {gingko, start_link, [Id, Args]},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [gingko]}.

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
  SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
  {ok, {SupFlags, []}}.
