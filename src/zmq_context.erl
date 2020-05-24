%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% ZMQ context manager
%% In order to use ZeroMQ, a common context instance is needed (http://api.zeromq.org/4-0:zmq-ctx-new).
%% The sole purpose of this gen_server is to provide this instance, and to terminate it gracefully.

-module(zmq_context).

-behaviour(gen_server).

-export([get/0]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-type zmq_context() :: term().

-record(state, {
    zmq_context :: zmq_context()
}).

%%%===================================================================
%%% Public API
%%%===================================================================

%% Context is a NIF object handle
get() ->
    gen_server:call(?MODULE, get_context).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    {ok, #state{zmq_context = erlzmq:context()}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request = get_context, From, State = #state{zmq_context = ZmqContext}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ZmqContext, State};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call(?MODULE, Request, From, State).
handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast(?MODULE, Request, State).
handle_info(Info, State) -> default_gen_server_behaviour:handle_info(?MODULE, Info, State).

terminate(Reason, State = #state{zmq_context = ZmqContext}) ->
    default_gen_server_behaviour:terminate(?MODULE, Reason, State),
    erlzmq:term(ZmqContext).

code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================
