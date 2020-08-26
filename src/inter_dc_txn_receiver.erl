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
-include("inter_dc.hrl").
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
    dcid_to_txn_sockets = #{} :: #{dcid() => [zmq_socket()]},
    current_partition_filter = [] :: [partition()] %%TODO later
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

-spec add_dc(dcid(), dc_address_list()) -> ok | {error, reason()}.
add_dc(DCID, DcAddressList) -> gen_server:call(?MODULE, {add_dc, DCID, DcAddressList}, ?COMM_TIMEOUT).

-spec delete_dc(dcid()) -> ok.
delete_dc(DCID) -> gen_server:cast(?MODULE, {delete_dc, DCID}).

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
    %% First delete the DC if it is already connected
    NewState = delete_dc(DCID, State),
    DCIDToTxnSockets = NewState#state.dcid_to_txn_sockets,
    case connect_to_nodes(DcAddressList, []) of
        {ok, Sockets} ->
            {reply, ok, NewState#state{dcid_to_txn_sockets = DCIDToTxnSockets#{DCID => Sockets}}};
        Error ->
            {reply, Error, NewState}
    end;

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call_crash(?MODULE, Request, From, State).

handle_cast(Request = {delete_dc, DCID}, State) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    NewState = delete_dc(DCID, State),
    {noreply, NewState};

handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast_crash(?MODULE, Request, State).

%% handle an incoming interDC transaction from a remote node.
handle_info(Info = {zmq, _Socket, InterDcTxnBinary, _Flags}, State) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    InterDcTxn = inter_dc_txn:from_binary(InterDcTxnBinary),
    Partition = InterDcTxn#inter_dc_txn.partition,
    gingko_dc_utils:call_gingko_async(Partition, ?GINGKO_LOG, {add_remote_txn, InterDcTxn}),
    {noreply, State};

handle_info(Info, State) -> default_gen_server_behaviour:handle_info_crash(?MODULE, Info, State).

terminate(Reason, State = #state{dcid_to_txn_sockets = DCIDToTxnSockets}) ->
    default_gen_server_behaviour:terminate(?MODULE, Reason, State),
    maps:fold(
        fun(_, List, _) ->
            lists:foreach(
                fun(Socket) ->
                    zmq_utils:close_socket(Socket)
                end, List)
        end, ok, DCIDToTxnSockets).

code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec delete_dc(dcid(), state()) -> state().
delete_dc(DCID, State = #state{dcid_to_txn_sockets = DCIDToTxnSockets}) ->
    case maps:find(DCID, DCIDToTxnSockets) of
        {ok, Sockets} ->
            lists:foreach(fun zmq_utils:close_socket/1, Sockets),
            State#state{dcid_to_txn_sockets = maps:remove(DCID, DCIDToTxnSockets)};
        error ->
            State
    end.

-spec connect_to_nodes(dc_address_list(), [zmq_socket()]) -> {ok, [zmq_socket()]} | {error, reason()}.
connect_to_nodes([], SocketAcc) ->
    {ok, SocketAcc};
connect_to_nodes([NodeAddressList | Rest], SocketAcc) ->
    case connect_to_node(NodeAddressList) of
        {ok, Socket} ->
            connect_to_nodes(Rest, [Socket | SocketAcc]);
        {error, Reason} ->
            lists:foreach(fun zmq_utils:close_socket/1, SocketAcc),
            {error, Reason}
    end.

-spec connect_to_node(node_address_list()) -> {ok, zmq_socket()} | {error, reason()}.
connect_to_node({_, []}) ->
    logger:error("Unable to subscribe to DC Node"),
    {error, connection_error};
connect_to_node({Node, [Address | Rest]}) ->
    %% Test the connection
    TemporarySocket = zmq_utils:create_subscriber_connect_socket(false, Address),
    ok = zmq_utils:set_receive_timeout(TemporarySocket, ?ZMQ_TIMEOUT),
    ok = zmq_utils:set_receive_filter(TemporarySocket, <<>>),
    Result = zmq_utils:try_recv(TemporarySocket),
    ok = zmq_utils:close_socket(TemporarySocket),
    case Result of
        {ok, _} ->
            %% Create a subscriber socket for the specified DC
            Socket = zmq_utils:create_subscriber_connect_socket(true, Address),
            lists:foreach(
                fun(Partition) ->
                    %% Make the socket subscribe to messages prefixed with the given partition number
                    ok = zmq_utils:set_receive_filter(Socket, inter_dc_txn:partition_to_binary(Partition))
                end, gingko_dc_utils:get_my_partitions()),
            {ok, Socket};
        _ ->
            connect_to_node({Node, Rest})
    end.
