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

-module(inter_dc_request_responder).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc.hrl").
-behaviour(gen_server).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    request_socket :: zmq_socket(),
    current_zmq_sender_id = none :: zmq_sender_id() | none,
    next_query_state = id :: id | blank | request
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    {_, RequestPort} = inter_dc_utils:get_request_address(),
    RequestSocket = zmq_utils:create_bind_socket(xrep, true, RequestPort),
    {ok, #state{request_socket = RequestSocket}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call_crash(?MODULE, Request, From, State).

handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast_crash(?MODULE, Request, State).

%% Handle the remote request
%% ZMQ requests come in 3 parts
%% 1st the Id of the sender, 2nd an empty binary, 3rd the binary msg
handle_info({zmq, _Socket, ZmqSenderId, [rcvmore]} = Info, State = #state{next_query_state = id}) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    {noreply, State#state{next_query_state = blank, current_zmq_sender_id = ZmqSenderId}};
handle_info(Info = {zmq, _Socket, <<>>, [rcvmore]}, State = #state{next_query_state = blank}) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    {noreply, State#state{next_query_state = request}};
handle_info(Info = {zmq, Socket, RequestBinary, _Flags}, State = #state{current_zmq_sender_id = ZmqSenderId, next_query_state = request}) ->
    %% Decode the message
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    RequestRecord = inter_dc_request:request_record_from_binary(RequestBinary),
    case inter_dc_request:is_relevant_request_for_responder(RequestRecord) of
        true ->
            process_request(RequestRecord, ZmqSenderId, Socket);
        false -> ok
    end,
    {noreply, State#state{next_query_state = id}};

handle_info(Info, State) -> default_gen_server_behaviour:handle_info_crash(?MODULE, Info, State).

terminate(Reason, State = #state{request_socket = RequestSocket}) ->
    default_gen_server_behaviour:terminate(?MODULE, Reason, State),
    zmq_utils:close_socket(RequestSocket).

code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

process_request(RequestRecord = #request_record{request_type = ?HEALTH_CHECK_MSG}, ZmqSenderId, Socket) ->
    send_response(?OK_MSG, RequestRecord, ZmqSenderId, Socket);
process_request(RequestRecord = #request_record{request_type = ?DCSF_MSG, request_args = RequestArgs}, ZmqSenderId, Socket) ->
    inter_dc_state_service:update_dc_state(RequestArgs),
    send_response(?OK_MSG, RequestRecord, ZmqSenderId, Socket);
process_request(RequestRecord = #request_record{request_type = ?JOURNAL_READ_REQUEST, target_partition = TargetPartition, request_args = RequestArgs}, ZmqSenderId, Socket) ->
    TxIdList = RequestArgs,
    {ok, JournalEntryList} = gingko_utils:call_gingko_sync(TargetPartition, ?GINGKO_LOG, {get_txns, TxIdList}),
    send_response(JournalEntryList, RequestRecord, ZmqSenderId, Socket);
process_request(RequestRecord = #request_record{request_type = ?BCOUNTER_REQUEST, request_args = RequestArgs}, ZmqSenderId, Socket) ->
    {transfer, {_Key, _Amount, _RemoteDCID}} = RequestArgs,
    ok = bcounter_manager:process_transfer(RequestArgs),
    send_response(?OK_MSG, RequestRecord, ZmqSenderId, Socket);
process_request(RequestRecord, ZmqSenderId, Socket) ->
    send_response(?REQUEST_NOT_SUPPORTED_MSG, RequestRecord, ZmqSenderId, Socket).

-spec send_response(term(), request_record(), zmq_sender_id(), zmq_socket()) -> ok.
send_response(Response, RequestRecord, ZmqSenderId, RequestSocket) ->
    %% Must send a response in 3 parts with ZMQ
    %% 1st Id, 2nd empty binary, 3rd the binary message
    ResponseRecord = #response_record{request_record = RequestRecord, response = Response},
    ResponseBinary = inter_dc_request:response_record_to_binary(ResponseRecord),
    ok = zmq_utils:try_send(RequestSocket, ZmqSenderId, [sndmore]),
    ok = zmq_utils:try_send(RequestSocket, <<>>, [sndmore]),
    ok = zmq_utils:try_send(RequestSocket, ResponseBinary).
