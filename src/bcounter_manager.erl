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

%% Module for handling bounded counter operations.
%% Allows safe increment, decrement and transfer operations.
%% Basic inter-dc reservations manager only requests remote reservations
%% when necessary.
%% Transfer requests are throttled to prevent distribution unbalancing
%% (TODO: implement inter-dc transference policy E.g, round-robin).

-module(bcounter_manager).
-include("inter_dc.hrl").
-behaviour(gen_server).

-export([generate_downstream/3,
    process_transfer/1]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-type last_transfers() :: orddict:orddict({key(), dcid()}, erlang:timestamp()).
-type request_queue() :: orddict:orddict(key(), [{non_neg_integer(), erlang:timestamp()}]).

-record(state, {request_queue = orddict:new() :: request_queue(),
    last_transfers = orddict:new() :: last_transfers(),
    transfer_timer :: reference()}).
-define(DATA_TYPE, antidote_crdt_counter_b).

%%%===================================================================
%%% Public API
%%%===================================================================

%% @doc Processes a decrement operation for a bounded counter.
%% If the operation is unsafe (i.e. the value of the counter can go
%% below 0), operation fails, otherwise a downstream for the decrement
%% is generated.
generate_downstream(Key, {decrement, {Value, _DCID}}, BCounter) ->
    DCID = gingko_utils:get_my_dcid(),
    gen_server:call(?MODULE, {consume, Key, {decrement, {Value, DCID}}, BCounter});

%% @doc Processes an increment operation for the bounded counter.
%% Operation is always safe.
generate_downstream(_Key, {increment, {Amount, _DCID}}, BCounter) ->
    DCID = gingko_utils:get_my_dcid(),
    ?DATA_TYPE:downstream({increment, {Amount, DCID}}, BCounter);

%% @doc Processes a transfer operation between two owners of the
%% counter.
generate_downstream(_Key, {transfer, {Amount, ToDCID, FromDCID}}, BCounter) ->
    ?DATA_TYPE:downstream({transfer, {Amount, ToDCID, FromDCID}}, BCounter).

%% @doc Handles a remote transfer request.
-spec process_transfer({transfer, {key(), non_neg_integer(), dcid()}}) -> ok.
process_transfer({transfer, TransferOp = {_Key, _Amount, _RemoteDCID}}) ->
    gen_server:cast(?MODULE, {transfer, TransferOp}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    Timer = erlang:send_after(?TRANSFER_FREQ, self(), transfer_periodic),
    {ok, #state{transfer_timer = Timer}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request = {consume, Key, {Op, {Amount, _}}, BCounter}, From, State = #state{request_queue = RequestQueue}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    DCID = gingko_utils:get_my_dcid(),
    case ?DATA_TYPE:generate_downstream_check({Op, Amount}, DCID, BCounter, Amount) of
        {error, no_permissions} = FailedResult ->
            Available = ?DATA_TYPE:localPermissions(DCID, BCounter),
            UpdatedRequestQueue = queue_request(Key, Amount - Available, RequestQueue),
            {reply, FailedResult, State#state{request_queue = UpdatedRequestQueue}};
        Result ->
            {reply, Result, State}
    end.

handle_cast(Request = {transfer, {Key, Amount, RequesterDCID}}, State = #state{last_transfers = LastTransfers}) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    NewLastTransfers = cancel_consecutive_request(LastTransfers, ?GRACE_PERIOD),
    DCID = gingko_utils:get_my_dcid(),
    case can_process(Key, RequesterDCID, NewLastTransfers) of
        true ->
            BCounterKey = {Key, ?DATA_TYPE},
            % try to transfer locks, might return {error,no_permissions} if not enough permissions are available locally
            _ = gingko:update_txn({BCounterKey, {transfer, {Amount, RequesterDCID, DCID}}}),
            {noreply, State#state{last_transfers = orddict:store({Key, RequesterDCID}, erlang:timestamp(), NewLastTransfers)}};
        _ ->
            {noreply, State#state{last_transfers = NewLastTransfers}}
    end.

handle_info(Info = transfer_periodic, State = #state{request_queue = RequestQueue, transfer_timer = TransferTimer}) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    _ = erlang:cancel_timer(TransferTimer),
    ClearedRequestQueue = clear_pending_request(RequestQueue, ?REQUEST_TIMEOUT),
    NewRequestQueue =
        orddict:fold(
            fun(Key, Queue, RequestQueueAcc) ->
                case Queue of
                    [] -> RequestQueueAcc;
                    Queue ->
                        RequiredSum = lists:foldl(
                            fun({Request, _Timeout}, Sum) ->
                                Sum + Request
                            end, 0, Queue),
                        Remaining = request_remote(RequiredSum, Key),

                        %% No remote resources available, cancel further requests.
                        case Remaining == RequiredSum of
                            false -> queue_request(Key, Remaining, RequestQueueAcc);
                            true -> RequestQueueAcc
                        end
                end
            end, orddict:new(), ClearedRequestQueue),
    NewTransferTimer = erlang:send_after(?TRANSFER_FREQ, self(), transfer_periodic),
    {noreply, State#state{transfer_timer = NewTransferTimer, request_queue = NewRequestQueue}}.

terminate(Reason, State) ->
    default_gen_server_behaviour:terminate(?MODULE, Reason, State).

code_change(OldVsn, State, Extra) ->
    default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec queue_request(key(), non_neg_integer(), request_queue()) -> request_queue().
queue_request(_Key, 0, RequestQueue) -> RequestQueue;
queue_request(Key, Amount, RequestQueue) ->
    QueueForKey = case orddict:find(Key, RequestQueue) of
                      {ok, Value} -> Value;
                      error -> orddict:new()
                  end,
    CurrentTime = erlang:timestamp(),
    orddict:store(Key, [{Amount, CurrentTime} | QueueForKey], RequestQueue).

-spec request_remote(non_neg_integer(), key()) -> non_neg_integer().
request_remote(0, _Key) -> 0;
request_remote(RequiredSum, Key) ->
    DCID = gingko_utils:get_my_dcid(),
    BCounterKey = {Key, ?DATA_TYPE},
    {ok, BCounter} = gingko:read(BCounterKey),
    PrefList = pref_list(BCounter),
    lists:foldl(
        fun({RemoteDCID, AvailableRemotely}, Remaining0) ->
            case Remaining0 > 0 of
                true when AvailableRemotely > 0 ->
                    ToRequest = case AvailableRemotely - Remaining0 >= 0 of
                                    true -> Remaining0;
                                    false -> AvailableRemotely
                                end,
                    do_request(DCID, RemoteDCID, Key, ToRequest),
                    Remaining0 - ToRequest;
                _ -> Remaining0
            end
        end, RequiredSum, PrefList).

-spec do_request(dcid(), dcid(), key(), non_neg_integer()) -> ok | unknown_dc.
do_request(DCID, RemoteDCID, Key, Amount) ->
    {Partition, _} = gingko_utils:get_key_partition(Key),
    inter_dc_request_sender:perform_bcounter_permissions_request({RemoteDCID, Partition}, {transfer, {Key, Amount, DCID}}).

%% Orders the reservation of each DC, from high to low.
-spec pref_list(antidote_crdt_counter_b:antidote_crdt_counter_b()) -> [{dcid(), non_neg_integer()}].
pref_list(BCounter) ->
    DCID = gingko_utils:get_my_dcid(),
    OtherDCDescriptors = inter_dc_meta_data_manager:get_dc_descriptors(),
    OtherDCIDs = [DescriptorDCID || #descriptor{dcid = DescriptorDCID} <- OtherDCDescriptors, DescriptorDCID /= DCID],
    OtherDCPermissions = [{OtherDCID, ?DATA_TYPE:localPermissions(OtherDCID, BCounter)} || OtherDCID <- OtherDCIDs],
    lists:sort(fun({_, A}, {_, B}) -> A =< B end, OtherDCPermissions).

-spec cancel_consecutive_request(last_transfers(), microsecond()) -> last_transfers().
cancel_consecutive_request(LastTransfers, Period) ->
    CurrentTime = erlang:timestamp(),
    orddict:filter(
        fun(_, Timeout) ->
            timer:now_diff(Timeout, CurrentTime) < Period
        end, LastTransfers).

-spec clear_pending_request(request_queue(), microsecond()) -> request_queue().
clear_pending_request(LastRequestQueue, Period) ->
    CurrentTime = erlang:timestamp(),
    orddict:filter(
        fun(_, ListRequests) ->
            FilteredList =
                lists:filter(
                    fun({_, Timeout}) ->
                        timer:now_diff(Timeout, CurrentTime) < Period
                    end, ListRequests),
            length(FilteredList) /= 0
        end, LastRequestQueue).

-spec can_process(key(), dcid(), last_transfers()) -> boolean().
can_process(Key, RequesterDCID, LastTransfers) ->
    DCID = gingko_utils:get_my_dcid(),
    case RequesterDCID == DCID of
        false ->
            case orddict:find({Key, RequesterDCID}, LastTransfers) of
                {ok, _Timeout} -> true;
                error -> true
            end;
        true -> false
    end
.
