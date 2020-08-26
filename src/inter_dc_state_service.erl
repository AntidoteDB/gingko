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

-export([update_dc_state_service/1,
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
    dc_state_timer = none :: none | reference(),
    dcid_to_dc_state = #{} :: #{dcid() => dc_state()}
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

-spec update_dc_state_service(boolean()) -> ok.
update_dc_state_service(Active) ->
    gen_server:cast(?MODULE, {update_dc_state_service, Active}).

-spec update_dc_state(dc_state()) -> ok.
update_dc_state(DcState) ->
    gen_server:cast(?MODULE, {update_dc_state, DcState}).

-spec get_GCSt() -> vectorclock().
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
    ConnectedDCIDs = gingko_dc_utils:get_connected_dcids(),
    ContainsAllDCIDs = general_utils:set_equals_on_lists(ConnectedDCIDs, maps:keys(DCIDToDcState)),
    GCSt =
        case ContainsAllDCIDs of
            true ->
                MyDCSf = gingko_dc_utils:get_DCSf_vts(),
                DcStates = general_utils:get_values(DCIDToDcState),
                vectorclock:min([MyDCSf | lists:map(fun(#dc_state{dcsf = DCSf}) -> DCSf end, DcStates)]);
            false ->
                vectorclock:new()
        end,
    {reply, GCSt, State};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call_crash(?MODULE, Request, From, State).

handle_cast(Request = {update_dc_state_service, Active}, State) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    {noreply, update_timer(State#state{active = Active})};

handle_cast(Request = {update_dc_state, DcState = #dc_state{dcid = DCID}}, State = #state{dcid_to_dc_state = DCIDToDcState}) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    UpdatedDCIDToDcState = DCIDToDcState#{DCID => DcState},
    {noreply, State#state{dcid_to_dc_state = UpdatedDCIDToDcState}};

handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast_crash(?MODULE, Request, State).

handle_info(Info = dc_state, State = #state{active = Active}) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    case Active of
        true ->
            MyDcState = #dc_state{dcid = gingko_dc_utils:get_my_dcid(), last_update = gingko_dc_utils:get_timestamp(), dcsf = gingko_dc_utils:get_DCSf_vts()},
            inter_dc_request_sender:perform_dc_state_request(MyDcState);
        false -> ok
    end,
    {noreply, update_timer(State)};

handle_info(Info, State) -> default_gen_server_behaviour:handle_info_crash(?MODULE, Info, State).
terminate(Reason, State) -> default_gen_server_behaviour:terminate(?MODULE, Reason, State).
code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec update_timer(state()) -> state().
update_timer(State = #state{dc_state_timer = Timer}) ->
    DcStateIntervalMillis = gingko_env_utils:get_dc_state_interval_millis(),
    DcStateTimer = gingko_dc_utils:update_timer(Timer, true, DcStateIntervalMillis, dc_state, false),
    State#state{dc_state_timer = DcStateTimer}.
