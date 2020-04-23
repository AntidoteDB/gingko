%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(gingko_cache_server).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    gingko_cache_vnode:init([0]).

handle_call(Request, From, State) ->
    gingko_cache_vnode:handle_command(Request, From, State).

handle_cast(Request, State) ->
    {reply, _Result, NewState} = gingko_cache_vnode:handle_command(Request, self(), State),
    {noreply, NewState}.

handle_info(Request, State) ->
    gingko_cache_vnode:handle_info(Request, State).

terminate(Reason, State) ->
    logger:debug("terminate(~nReason: ~p~nState: ~p~n)", [Reason, State]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
