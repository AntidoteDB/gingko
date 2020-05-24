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

-module(inter_dc_txn_receiver).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc_repl.hrl").

-behaviour(gen_server).

-export([add_dc/2,
    delete_dc/1]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    dcid_to_journal_sockets = dict:new() :: dict:dict(dcid(), [zmq_socket()])
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

%% TODO: persist added DCs in case of a node failure, reconnect on node restart.
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
    {ok, #state{}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request = {add_dc, DCID, DcAddressList}, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    %% First delete the DC if it is alread connected
    NewState = delete_dc(DCID, State),
    DCIDToJournalSockets = NewState#state.dcid_to_journal_sockets,
    case connect_to_nodes(DcAddressList, []) of
        {ok, Sockets} ->
            {reply, ok, NewState#state{dcid_to_journal_sockets = dict:store(DCID, Sockets, DCIDToJournalSockets)}};
        connection_error ->
            {reply, error, NewState}
    end;

handle_call(Request = {del_dc, DCID}, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    NewState = delete_dc(DCID, State),
    {reply, ok, NewState};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call(?MODULE, Request, From, State).
handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast(?MODULE, Request, State).

%% handle an incoming interDC transaction from a remote node.
handle_info(Info = {zmq, _Socket, InterDcTxnBinary, _Flags}, State) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    %% decode the message
    InterDcTxn = inter_dc_txn:from_binary(InterDcTxnBinary),
    MyPartitions = gingko_utils:get_my_partitions(),
    RelevantInterDcJournalEntries = lists:filter(fun({Partition, _JournalEntry}) -> Partition == all orelse lists:member(Partition, MyPartitions) end, InterDcTxn#inter_dc_txn.inter_dc_journal_entries),
    lists:foreach(
        fun({Partition, JournalEntry}) ->
            case Partition of
                all -> gingko_utils:bcast_local_gingko_async(?GINGKO_LOG, {add_remote_journal_entry, JournalEntry});
                _ -> gingko_utils:call_gingko_async(Partition, ?GINGKO_LOG, {add_remote_journal_entry, JournalEntry})
            end
        end, RelevantInterDcJournalEntries),
    {noreply, State};

handle_info(Info, State) -> default_gen_server_behaviour:handle_info(?MODULE, Info, State).

terminate(Reason, State = #state{dcid_to_journal_sockets = DCIDToJournalSockets}) ->
    default_gen_server_behaviour:terminate(?MODULE, Reason, State),
    dict:fold(
        fun(_, Dict, _) ->
            dict:fold(
                fun(_, Socket, _) ->
                    zmq_utils:close_socket(Socket)
                end, ok, Dict)
        end, ok, DCIDToJournalSockets).

code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec delete_dc(dcid(), state()) -> state().
delete_dc(DCID, State = #state{dcid_to_journal_sockets = DCIDToJournalSockets}) ->
    case dict:find(DCID, DCIDToJournalSockets) of
        {ok, Sockets} ->
            lists:foreach(fun zmq_utils:close_socket/1, Sockets),
            State#state{dcid_to_journal_sockets = dict:erase(DCID, DCIDToJournalSockets)};
        error ->
            State
    end.

-spec connect_to_nodes(dc_address_list(), [zmq_socket()]) -> {ok, [zmq_socket()]} | connection_error.
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

-spec connect_to_node(node_address_list()) -> {ok, zmq_socket()} | connection_error.
connect_to_node([]) ->
    logger:error("Unable to subscribe to DC Node"),
    connection_error;
connect_to_node([NodeAddress | Rest]) ->
    %% Test the connection
    TemporarySocket = zmq_utils:create_connect_socket(sub, false, NodeAddress),
    ok = erlzmq:setsockopt(TemporarySocket, rcvtimeo, ?ZMQ_TIMEOUT),
    ok = zmq_utils:receive_filter(TemporarySocket, <<>>),
    Result = erlzmq:recv(TemporarySocket),
    ok = zmq_utils:close_socket(TemporarySocket),
    case Result of
        {ok, _} ->
            %% Create a subscriber socket for the specified DC
            Socket = zmq_utils:create_connect_socket(sub, false, NodeAddress),
            {ok, Socket};
        _ ->
            connect_to_node(Rest)
    end.

%TODO when handoff happens reapply_socket_filter() ->
