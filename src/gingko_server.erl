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

-module(gingko_server).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    running_txid_to_partitions = dict:new() :: dict:dict(txid(), [partition_id()]),
    running_txid_to_ops = dict:new() :: dict:dict(txid(), list())
}).
-type state() :: #state{}.

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #state{}}.

handle_call({{Op, Args}, TxId} = Request, Sender, State) ->
    logger:debug("handle_call(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Request, Sender, State]),
    gingko_vnode:process_command(Request, Sender, State);

handle_call(Request, Sender, State) ->
    logger:debug("handle_call(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Request, Sender, State]),
    {reply, error, State}.

handle_cast(Request, State) ->
    logger:debug("handle_cast(~nRequest: ~p~nState: ~p~n)", [Request, State]),
    {noreply, State}.

handle_info(Request, State) ->
    logger:debug("handle_info(~nRequest: ~p~nState: ~p~n)", [Request, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
