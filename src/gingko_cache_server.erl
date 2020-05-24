%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(gingko_cache_server).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").

-behaviour(gen_server).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%%%===================================================================
%%% Public API
%%%===================================================================

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    gingko_cache_vnode:init([0]).

handle_call(Request, {Pid, _Tag}, State) ->
    gingko_cache_vnode:handle_command(Request, {raw, undefined, Pid}, State).

handle_cast(Request, State) ->
    {reply, _Result, NewState} = gingko_cache_vnode:handle_command(Request, {raw, undefined, self()}, State),
    {noreply, NewState}.

handle_info(Info, State) ->
    {ok, NewState} = gingko_cache_vnode:handle_info(Info, State),
    {noreply, NewState}.

terminate(Reason, State) ->
    gingko_cache_vnode:terminate(Reason, State).

code_change(OldVsn, State, Extra) ->
    default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================
