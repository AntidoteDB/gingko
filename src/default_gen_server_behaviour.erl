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

-module(default_gen_server_behaviour).

-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").

-export([init/2,
    handle_call/4,
    handle_cast/3,
    handle_info/3,
    terminate/3,
    code_change/4]).

init(Module, Args) ->
    logger:debug("~p:init(~nArgs: ~p~n)", [Module, Args]),
    {ok, none}.

handle_call(Module, Request, From, State) ->
    logger:debug("~p:handle_call(~nRequest: ~p~nFrom: ~p~nState: ~p~n)", [Module, Request, From, State]),
    {reply, default_response, State}.

handle_cast(Module, Request, State) ->
    logger:debug("~p:handle_cast(~nRequest: ~p~nState: ~p~n)", [Module, Request, State]),
    {noreply, State}.

handle_info(Module, Info, State) ->
    logger:debug("~p:handle_info(~nInfo: ~p~nState: ~p~n)", [Module, Info, State]),
    {noreply, State}.

terminate(Module, Reason, State) ->
    logger:debug("~p:terminate(~nReason: ~p~nState: ~p~n)", [Module, Reason, State]),
    ok.

code_change(Module, OldVsn, State, Extra) ->
    logger:debug("~p:code_change(~nOldVsn: ~p~nState: ~p~nExtra: ~p~n)", [Module, OldVsn, State, Extra]),
    {ok, State}.
