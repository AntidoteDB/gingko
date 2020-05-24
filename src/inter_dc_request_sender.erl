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

-module(inter_dc_request_sender).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc_repl.hrl").

-behaviour(gen_server).

-export([perform_journal_read_request/3,
    perform_bcounter_permissions_request/3,
    add_dc/2,
    delete_dc/1]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%%TODO assign sockets to partitions (needs updates during handoffs)

-record(state, {
    dcid_to_request_sockets = dict:new() :: dict:dict(dcid(), [zmq_socket()]),
    next_request_id :: non_neg_integer(),
    running_requests = dict:new() :: dict:dict(non_neg_integer(), request_entry()),
    last_health_check_results = dict:new() :: dict:dict(dcid(), ok | running)
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

-spec perform_journal_read_request(dcid_and_partition(), {atom(), term()}, fun((term(), request_entry()) -> ok))
        -> ok | unknown_dc.
perform_journal_read_request({TargetDCID, TargetPartition}, {JournalEntryFilterFuncName, JournalEntryFilterFuncArgs}, ReturnToSenderFunc) ->
    gen_server:call(?MODULE, {request, ?JOURNAL_READ_REQUEST, {TargetDCID, TargetPartition}, {read_journal_entries, {JournalEntryFilterFuncName, JournalEntryFilterFuncArgs}}, ReturnToSenderFunc}).

-spec perform_bcounter_permissions_request(dcid_and_partition(), {atom(), {key(), non_neg_integer(), dcid()}}, fun((term(), request_entry()) -> ok))
        -> ok | unknown_dc.
perform_bcounter_permissions_request({TargetDCID, TargetPartition}, {transfer, {Key, Amount, RequesterDCID}}, ReturnToSenderFunc) ->
    gen_server:call(?MODULE, {request, ?BCOUNTER_REQUEST, {TargetDCID, TargetPartition}, {request_permissions, {transfer, {Key, Amount, RequesterDCID}}}, ReturnToSenderFunc}).

perform_health_check_request(ReturnToSenderFunc) ->
    gen_server:call(?MODULE, {request, ?HEALTH_CHECK_MSG, {all, all}, health_check, ReturnToSenderFunc}).

-spec add_dc(dcid(), dc_address_list()) -> ok.
add_dc(DCID, DcAddressList) -> gen_server:call(?MODULE, {add_dc, DCID, DcAddressList}, ?COMM_TIMEOUT).

-spec delete_dc(dcid()) -> ok.
delete_dc(DCID) -> gen_server:call(?MODULE, {del_dc, DCID}, ?COMM_TIMEOUT).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    {ok, #state{next_request_id = 1}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

%% Handle an instruction to ask a remote DC.
handle_call(Request = {request, RequestType, {TargetDCID, TargetPartition}, GivenRequest, ReturnFunc}, From, State = #state{dcid_to_request_sockets = DCIDToRequestSocket, next_request_id = RequestId}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    case dict:find(TargetDCID, DCIDToRequestSocket) of
        %% If socket found
        %% Find the socket that is responsible for this partition
        {ok, Sockets} ->
            RequestRecord = inter_dc_request:create_request_record({RequestId, RequestType}, {TargetDCID, TargetPartition}, GivenRequest),
            %% Build the binary request
            RequestEntry = inter_dc_request:create_request_entry(RequestRecord, ReturnFunc),
            RequestRecordBinary = term_to_binary(RequestRecord),
            lists:foreach(fun(Socket) -> erlzmq:send(Socket, RequestRecordBinary) end, Sockets),
            {reply, ok, request_sent_state_update(RequestEntry, State)};
        %% If socket not found
        _ -> {reply, unknown_dc, State}
    end;

%% Handle the instruction to add a new DC.
handle_call(Request = {add_dc, DCID, DcAddressList}, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    %% Create a socket and store it
    %% The DC will contain a list of ip/ports each with a list of partition ids located at each node
    %% This will connect to each node and store in the cache the list of partitions located at each node
    %% so that a request goes directly to the node where the needed partition is located
    {_, NewState} = delete_dc(DCID, State),
    DCIDToRequestSockets = NewState#state.dcid_to_request_sockets,
    case connect_to_nodes(DcAddressList, []) of
        {ok, Sockets} ->
            {reply, ok, NewState#state{dcid_to_request_sockets = dict:store(DCID, Sockets, DCIDToRequestSockets)}};
        connection_error ->
            {reply, error, NewState}
    end;

%% Remove a DC. Unanswered queries are left untouched.
handle_call(Request = {del_dc, DCID}, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {_, NewState} = delete_dc(DCID, State),
    {reply, ok, NewState};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call(?MODULE, Request, From, State).
handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast(?MODULE, Request, State).

%% Handle a response from any of the connected sockets
%% Possible improvement - disconnect sockets unused for a defined period of time.
handle_info(Info = {zmq, _Socket, ResponseBinary, _Flags}, State = #state{running_requests = RunningRequests}) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    #response_record{request_record = RequestRecord, response = Response} = binary_to_term(ResponseBinary),
    RequestId = RequestRecord#request_record.request_id,
    %% Be sure this is a request from this socket
    NewRunningRequests =
        case dict:find(RequestId, RunningRequests) of
            {ok, RequestEntry = #request_entry{request_record = RequestRecord, return_func = ReturnFunc}} ->
                ReturnFunc(Response, RequestEntry),
                dict:erase(RequestId, RunningRequests);
            error ->
                logger:error("Got a bad (or repeated) request id: ~p", [RequestId]),
                RunningRequests
        end,
    {noreply, State#state{running_requests = NewRunningRequests}};
handle_info(Info, State) -> default_gen_server_behaviour:handle_info(?MODULE, Info, State).

terminate(Reason, State = #state{dcid_to_request_sockets = DCIDToRequestSocket}) ->
    default_gen_server_behaviour:terminate(?MODULE, Reason, State),
    dict:fold(
        fun(_, Dict, _) ->
            dict:fold(
                fun(_, Socket, _) ->
                    zmq_utils:close_socket(Socket)
                end, ok, Dict)
        end, ok, DCIDToRequestSocket).

code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec delete_dc(dcid(), state()) -> state().
delete_dc(DCID, State = #state{dcid_to_request_sockets = DCIDToRequestSocket}) ->
    case dict:find(DCID, DCIDToRequestSocket) of
        {ok, Sockets} ->
            lists:foreach(fun(Socket) -> zmq_utils:close_socket(Socket) end, Sockets),
            State#state{dcid_to_request_sockets = dict:erase(DCID, DCIDToRequestSocket)};
        error ->
            State
    end.

%% Saves the request in the state, so it can be resent if the DC was disconnected.
-spec request_sent_state_update(request_entry(), state()) -> state().
request_sent_state_update(RequestEntry = #request_entry{request_record = #request_record{request_id = RequestId}}, State = #state{next_request_id = PreviousRequestId, running_requests = RunningRequests}) ->
    NewRunningRequests = dict:store(RequestId, RequestEntry, RunningRequests),
    State#state{next_request_id = PreviousRequestId + 1, running_requests = NewRunningRequests}.

-spec connect_to_nodes(dc_address_list(), [zmq_socket()]) -> [zmq_socket()] | connection_error.
connect_to_nodes([], SocketAcc) ->
    {ok, SocketAcc};
connect_to_nodes([NodeAddressList | Rest], SocketAcc) ->
    case connect_to_node(NodeAddressList) of
        {ok, Socket} ->
            connect_to_nodes(Rest, [Socket | SocketAcc]);
        connection_error ->
            lists:foreach(fun zmq_utils:close_socket/1, SocketAcc),
            connection_error
    end.

%% A node is a list of addresses because it can have multiple interfaces
%% this just goes through the list and connects to the first interface that works
-spec connect_to_node(node_address_list()) -> {ok, zmq_socket()} | connection_error.
connect_to_node([]) ->
    logger:error("Unable to subscribe to DC log reader"),
    connection_error;
connect_to_node([Address | Rest]) ->
    %% Test the connection
    TemporarySocket = zmq_utils:create_connect_socket(req, false, Address),
    ok = erlzmq:setsockopt(TemporarySocket, rcvtimeo, ?ZMQ_TIMEOUT),
    %% Always use 0 as the id of the check up message
    RequestRecord = inter_dc_request:create_request_record({0, ?CHECK_UP_MSG}, {all, all}, ?CHECK_UP_MSG),
    ok = erlzmq:send(TemporarySocket, term_to_binary(RequestRecord)),
    Response = erlzmq:recv(TemporarySocket),
    ok = zmq_utils:close_socket(TemporarySocket),
    case Response of
        {ok, Binary} ->
            %% erlzmq:recv returns binary, its spec says iolist, but dialyzer compains that it is not a binary
            %% so I added this conversion, even though the result of recv is a binary anyway...
            ResponseBinary = iolist_to_binary(Binary),
            %% check that an ok msg was received
            #response_record{request_record = RequestRecord, response = ?OK_MSG} = binary_to_term(ResponseBinary),
            %% Create a subscriber socket for the specified DC
            Socket = zmq_utils:create_connect_socket(req, false, Address),
            %% For each partition in the current node:
            {ok, Socket};
        _ ->
            connect_to_node(Rest)
    end.


