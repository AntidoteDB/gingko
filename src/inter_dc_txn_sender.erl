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

-module(inter_dc_txn_sender).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc.hrl").
-behaviour(gen_server).

-export([broadcast_ping/2,
    broadcast_txn/3]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    txn_socket :: zmq_socket()
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

-spec broadcast_ping(partition(), txn_tracking_num()) -> ok.
broadcast_ping(Partition, LastSentTxnTrackingNum) ->
    broadcast_txn(Partition, LastSentTxnTrackingNum, []).

-spec broadcast_txn(partition(), txn_tracking_num(), [journal_entry()]) -> ok.
broadcast_txn(Partition, LastSentTxnTrackingNum, JournalEntryList) ->
    gen_server:cast(?MODULE, {inter_dc_txn, Partition, LastSentTxnTrackingNum, JournalEntryList}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    {_, TxnPort} = inter_dc_utils:get_txn_address(),
    TxnSocket = zmq_utils:create_publisher_bind_socket(TxnPort),
    {ok, #state{txn_socket = TxnSocket}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call_crash(?MODULE, Request, From, State).

handle_cast(Request = {inter_dc_txn, Partition, LastSentTxnTrackingNum, JournalEntryList}, State = #state{txn_socket = TxnSocket}) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    InterDcTxn = inter_dc_txn:from_journal_entries(Partition, LastSentTxnTrackingNum, JournalEntryList),
    BinaryMessage = inter_dc_txn:to_binary(InterDcTxn),
    ok = zmq_utils:try_send(TxnSocket, BinaryMessage),
    {noreply, State};

handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast_crash(?MODULE, Request, State).

-spec handle_info(term(), state()) -> no_return().
handle_info(Info, State) -> default_gen_server_behaviour:handle_info_crash(?MODULE, Info, State).

terminate(Reason, State = #state{txn_socket = TxnSocket}) ->
    default_gen_server_behaviour:terminate(?MODULE, Reason, State),
    zmq_utils:close_socket(TxnSocket).

code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================
