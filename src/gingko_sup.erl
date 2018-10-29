-module(gingko_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link(gingko_sup, []).

init(_Args) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    LoggingMaster = #{id => logging_vnode_master,
                    start => {riak_core_vnode_master, start_link, [logging_vnode]},
                    restart => permanent,
                    shutdown => 5000,
                    type => worker,
                    modules => [riak_core_vnode_master]},
    {ok, {SupFlags, [LoggingMaster]}}.