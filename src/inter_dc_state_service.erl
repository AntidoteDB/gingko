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

-module(inter_dc_state_service).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc.hrl").
-behaviour(gen_server).

-export([update_dc_state_service/2,
    update_dc_state/1,
    get_GCSt/0]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    active = false :: boolean(),
    dc_state_interval_millis = ?DEFAULT_WAIT_TIME_LONG :: non_neg_integer(),
    dc_state_timer = none :: none | reference(),
    dcid_to_dc_state = #{} :: #{dcid() => dc_state()}
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

-spec update_dc_state_service(boolean(), non_neg_integer()) -> ok.
update_dc_state_service(Active, DcStateIntervalMillis) ->
    gen_server:cast(?MODULE, {update_dc_state_service, Active, DcStateIntervalMillis}).

-spec update_dc_state(dc_state()) -> ok.
update_dc_state(DcState) ->
    gen_server:cast(?MODULE, {update_dc_state, DcState}).

get_GCSt() ->
    gen_server:call(?MODULE, get_gcst).

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

handle_call(Request = get_gcst, From, State = #state{dcid_to_dc_state = DCIDToDcState}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    ConnectedDCIDs = gingko_utils:get_connected_dcids(),
    ContainsAllDCIDs = general_utils:set_equals_on_lists(ConnectedDCIDs, maps:keys(DCIDToDcState)),
    GCSt =
        case ContainsAllDCIDs of
            true ->
                MyDCSf = gingko_utils:get_DCSf_vts(),
                DcStates = general_utils:get_values(DCIDToDcState),
                vectorclock:min([MyDCSf | lists:map(fun(#dc_state{dcsf = DCSf}) -> DCSf end, DcStates)]);
            false ->
                vectorclock:new()
        end,
    {reply, GCSt, State};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call_crash(?MODULE, Request, From, State).

handle_cast(Request = {update_dc_state_service, Active, DcStateIntervalMillis}, State) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    {noreply, update_timer(State#state{active = Active, dc_state_interval_millis = DcStateIntervalMillis})};

handle_cast(Request = {update_dc_state, DcState = #dc_state{dcid = DCID}}, State = #state{dcid_to_dc_state = DCIDToDcState}) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    UpdatedDCIDToDcState = DCIDToDcState#{DCID => DcState},
    {noreply, State#state{dcid_to_dc_state = UpdatedDCIDToDcState}};

handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast_crash(?MODULE, Request, State).

handle_info(Info = dc_state, State) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    OldTimer = State#state.dc_state_timer,
    erlang:cancel_timer(OldTimer),
    MyDcState = #dc_state{dcid = gingko_utils:get_my_dcid(), last_update = gingko_utils:get_timestamp(), dcsf = gingko_utils:get_DCSf_vts()},
    inter_dc_request_sender:perform_dc_state_request(MyDcState),
    NewTimer = erlang:send_after(State#state.dc_state_interval_millis, self(), dc_state),
    {noreply, State#state{dc_state_timer = NewTimer}};

handle_info(Info, State) -> default_gen_server_behaviour:handle_info_crash(?MODULE, Info, State).
terminate(Reason, State) -> default_gen_server_behaviour:terminate(?MODULE, Reason, State).
code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec update_timer(state()) -> state().
update_timer(State = #state{dc_state_timer = Timer, dc_state_interval_millis = DcStateIntervalMillis}) ->
    DcStateTimer = gingko_utils:update_timer(Timer, true, DcStateIntervalMillis, dc_state, false),
    State#state{dc_state_timer = DcStateTimer}.
