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
-include("inter_dc.hrl").
-behaviour(gen_server).

-export([update_checkpoint_service/1, checkpoint/0]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    active = false :: boolean(),
    checkpoint_timer = none :: none | reference()
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

-spec update_checkpoint_service(boolean()) -> ok.
update_checkpoint_service(Active) ->
    gen_server:cast(?MODULE, {update_checkpoint_service, Active}).

-spec checkpoint() -> ok.
checkpoint() -> gen_server:call(?MODULE, checkpoint).

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

handle_call(Request = checkpoint, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    internal_checkpoint(),
    {reply, ok, State};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call_crash(?MODULE, Request, From, State).

handle_cast(Request = {update_checkpoint_service, Active}, State) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    {noreply, update_timer(State#state{active = Active})};

handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast_crash(?MODULE, Request, State).

handle_info(Info = checkpoint, State = #state{checkpoint_timer = CheckpointTimer}) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    erlang:cancel_timer(CheckpointTimer),
    internal_checkpoint(),
    CheckpointIntervalMillis = gingko_env_utils:get_checkpoint_interval_millis(),
    NewCheckpointTimer = erlang:send_after(CheckpointIntervalMillis, self(), checkpoint),
    {noreply, State#state{checkpoint_timer = NewCheckpointTimer}};

handle_info(Info, State) -> default_gen_server_behaviour:handle_info_crash(?MODULE, Info, State).
terminate(Reason, State) -> default_gen_server_behaviour:terminate(?MODULE, Reason, State).
code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec update_timer(state()) -> state().
update_timer(State = #state{checkpoint_timer = CheckpointTimer}) ->
    CheckpointIntervalMillis = gingko_env_utils:get_checkpoint_interval_millis(),
    NewCheckpointTimer = gingko_dc_utils:update_timer(CheckpointTimer, true, CheckpointIntervalMillis, checkpoint, false),
    State#state{checkpoint_timer = NewCheckpointTimer}.

%%We check whether we need to abort local transactions to perform the checkpoint (transactions older than two checkpoint intervals will be aborted)
%%TODO make this varible so that old transactions may get some more time to finish!
-spec internal_checkpoint() -> ok.
internal_checkpoint() ->
    GCSt = gingko_dc_utils:get_GCSt_vts(),
    MinimumDependencyVts = gingko_dc_utils:get_minimum_tx_dependency_vts(),
    CheckpointVts = vectorclock:min([GCSt, MinimumDependencyVts]),
    gingko_dc_utils:bcast_local_gingko_sync(?GINGKO_LOG, {{checkpoint, CheckpointVts}, #tx_id{server_pid = self(), local_start_time = gingko_dc_utils:get_timestamp()}}), %%TODO check results
    ok.%%Checkpoints only get applied to local partitions because they are reoccurring processes on all nodes
