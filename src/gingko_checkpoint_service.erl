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

-module(gingko_checkpoint_service).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").
-behaviour(gen_server).

-export([update_checkpoint_service/2, get_checkpoint_interval_millis/0]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    active = false :: boolean(),
    checkpoint_interval_millis = ?DEFAULT_WAIT_TIME_SUPER_LONG :: non_neg_integer(),
    checkpoint_timer = none :: none | reference()
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

-spec update_checkpoint_service(boolean(), non_neg_integer()) -> ok.
update_checkpoint_service(Active, CheckpointIntervalMillis) ->
    gen_server:cast(?MODULE, {update_checkpoint_service, Active, CheckpointIntervalMillis}).

-spec get_checkpoint_interval_millis() -> millisecond().
get_checkpoint_interval_millis() ->
    gen_server:call(?MODULE, get_checkpoint_interval_millis).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    {ok, #state{}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request = get_checkpoint_interval_millis, From, State = #state{checkpoint_interval_millis = CheckpointIntervalMillis}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, CheckpointIntervalMillis, State};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call_crash(?MODULE, Request, From, State).

handle_cast(Request = {update_checkpoint_service, Active, CheckpointIntervalMillis}, State) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    {noreply, update_timer(State#state{active = Active, checkpoint_interval_millis = CheckpointIntervalMillis})};

handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast_crash(?MODULE, Request, State).

handle_info(Info = checkpoint, State = #state{checkpoint_timer = CheckpointTimer, checkpoint_interval_millis = CheckpointIntervalMillis}) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    erlang:cancel_timer(CheckpointTimer),
    DependencyVts = gingko_utils:get_GCSt_vts(),
    gingko:checkpoint(DependencyVts),
    NewCheckpointTimer = erlang:send_after(CheckpointIntervalMillis, self(), checkpoint),
    {noreply, State#state{checkpoint_timer = NewCheckpointTimer}};

handle_info(Info, State) -> default_gen_server_behaviour:handle_info_crash(?MODULE, Info, State).
terminate(Reason, State) -> default_gen_server_behaviour:terminate(?MODULE, Reason, State).
code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec update_timer(state()) -> state().
update_timer(State = #state{checkpoint_timer = CheckpointTimer, checkpoint_interval_millis = CheckpointIntervalMillis}) ->
    NewCheckpointTimer = gingko_utils:update_timer(CheckpointTimer, true, CheckpointIntervalMillis, checkpoint, false),
    State#state{checkpoint_timer = NewCheckpointTimer}.
