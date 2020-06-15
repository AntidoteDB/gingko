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

-module(gingko_time_manager).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-behaviour(gen_server).

-export([get_positive_monotonic_time/0, get_monotonic_system_time/0]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    offset :: non_neg_integer(),
    monotonic_system_timestamp :: non_neg_integer()
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

-spec get_positive_monotonic_time() -> non_neg_integer().
get_positive_monotonic_time() ->
    gen_server:call(?MODULE, positive_monotonic_timestamp).

-spec get_monotonic_system_time() -> non_neg_integer().
get_monotonic_system_time() ->
    gen_server:call(?MODULE, monotonic_system_timestamp).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    MonotonicTime = erlang:monotonic_time(),
    Offset =
        case MonotonicTime > 0 of
            true -> erlang:monotonic_time() * (-1);
            false -> 0
        end,
    SystemTime = general_utils:get_timestamp_in_microseconds(),
    {ok, #state{offset = Offset, monotonic_system_timestamp = SystemTime}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request = positive_monotonic_timestamp, From, State = #state{offset = Offset}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    PositiveMonotonicTime = erlang:monotonic_time() + Offset,
    {reply, PositiveMonotonicTime, State};

handle_call(Request = monotonic_system_timestamp, From, State = #state{monotonic_system_timestamp = MonotonicSystemTimestamp}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    CurrentTimestamp = general_utils:get_timestamp_in_microseconds(),
    ResultTimestamp =
        case CurrentTimestamp =< MonotonicSystemTimestamp of
            true -> MonotonicSystemTimestamp + 1;
            false -> CurrentTimestamp
        end,
    {reply, ResultTimestamp, State#state{monotonic_system_timestamp = ResultTimestamp}};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call_crash(?MODULE, Request, From, State).
-spec handle_cast(term(), state()) -> no_return().
handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast_crash(?MODULE, Request, State).
-spec handle_info(term(), state()) -> no_return().
handle_info(Info, State) -> default_gen_server_behaviour:handle_info_crash(?MODULE, Info, State).
terminate(Reason, State) -> default_gen_server_behaviour:terminate(?MODULE, Reason, State).
code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================
