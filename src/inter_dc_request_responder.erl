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
-include("inter_dc_repl.hrl").

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
    request_queue :: queue:queue({zmq_sender_id(), Request :: term()}),
    current_zmq_sender_id :: zmq_sender_id() | none,
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

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call(?MODULE, Request, From, State).

handle_cast(Request = {read_journal_entries, TargetPartition, GivenRequest, RequestState}, State) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    {read_journal_entries, {JournalEntryFilterFuncName, JournalEntryFilterFuncArgs}} = GivenRequest,
    JournalEntries = get_journal_entries_internal(TargetPartition, {JournalEntryFilterFuncName, JournalEntryFilterFuncArgs}),
    send_response(JournalEntries, RequestState, State),
    {noreply, State};

handle_cast(Request = {request_permissions, GivenRequest, RequestState}, State) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    {request_permissions, Operation = {transfer, {_Key, _Amount, _RemoteDCID}}} = GivenRequest,
    ok = bcounter_manager:process_transfer(Operation),
    send_response(ok, RequestState, State),
    {noreply, State};

handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast(?MODULE, Request, State).

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
            #request_record{request_type = RequestType, target_partition = TargetPartition, request = Request} = RequestRecord,
            %% Create a response
            RequestState = #inter_dc_request_state{
                request_record = RequestRecord,
                zmq_sender_id = ZmqSenderId,
                local_pid = self()},
            case RequestType of
                ?CHECK_UP_MSG ->
                    ok = send_response(?OK_MSG, RequestRecord, ZmqSenderId, Socket);
                ?JOURNAL_READ_REQUEST ->
                    %%TODO accept all parameters for DCID and Partition
                    ok = gen_server:cast(?MODULE, {read_journal_entries, TargetPartition, Request, RequestState});
                ?BCOUNTER_REQUEST ->
                    ok = gen_server:cast(?MODULE, {request_permissions, Request, RequestState});
                _ ->
                    ok = send_response(?ERROR_MSG, RequestRecord, ZmqSenderId, Socket)
            end
    end,
    {noreply, State#state{next_query_state = id}};

handle_info(Info, State) -> default_gen_server_behaviour:handle_info(?MODULE, Info, State).

terminate(Reason, State = #state{request_socket = RequestSocket}) ->
    default_gen_server_behaviour:terminate(?MODULE, Reason, State),
    zmq_utils:close_socket(RequestSocket).

code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec send_response(term(), inter_dc_request_state(), state()) -> ok.
send_response(Response, #inter_dc_request_state{request_record = RequestRecord, zmq_sender_id = ZmqSenderId}, #state{request_socket = RequestSocket}) ->
    send_response(Response, RequestRecord, ZmqSenderId, RequestSocket).

-spec send_response(term(), request_record(), zmq_sender_id(), zmq_socket()) -> ok.
send_response(Response, RequestRecord, ZmqSenderId, RequestSocket) ->
    %% Must send a response in 3 parts with ZMQ
    %% 1st Id, 2nd empty binary, 3rd the binary message
    ResponseRecord = #response_record{request_record = RequestRecord, response = Response},
    ResponseBinary = inter_dc_request:response_record_to_binary(ResponseRecord),
    ok = erlzmq:send(RequestSocket, ZmqSenderId, [sndmore]),
    ok = erlzmq:send(RequestSocket, <<>>, [sndmore]),
    ok = erlzmq:send(RequestSocket, ResponseBinary).

-spec get_journal_entries_internal(partition_id(), {atom(), term()}) -> [journal_entry()].
get_journal_entries_internal(Partition, {JournalEntryFilterFuncName, JournalEntryFilterFuncArgs}) ->
    FilterFunc =
        fun(JournalEntry) ->
            inter_dc_filter_functions:JournalEntryFilterFuncName(JournalEntryFilterFuncArgs, JournalEntry)
        end,
    {ok, JournalEntries} = gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {get_journal_entries, gingko_utils:get_my_dcid(), FilterFunc}),
    JournalEntries.
