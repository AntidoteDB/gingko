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
-include("inter_dc_repl.hrl").

-behaviour(gen_server).

-export([broadcast_journal_entries/1]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    journal_socket :: zmq_socket()
}).

%%%===================================================================
%%% Public API
%%%===================================================================

-spec broadcast_journal_entries([inter_dc_journal_entry()]) -> ok.
broadcast_journal_entries(InterDcJournalEntries) ->
    gen_server:call(?MODULE, {journal_entry, InterDcJournalEntries}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    {_, JournalPort} = inter_dc_utils:get_journal_address(),
    JournalSocket = zmq_utils:create_bind_socket(pub, false, JournalPort),
    {ok, #state{journal_socket = JournalSocket}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request = {journal_entry, InterDcJournalEntries}, From, State = #state{journal_socket = JournalSocket}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    InterDcTxn = inter_dc_txn:from_journal_entries(InterDcJournalEntries),
    BinaryMessage = inter_dc_txn:to_binary(InterDcTxn),
    {reply, erlzmq:send(JournalSocket, BinaryMessage), State};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call(?MODULE, Request, From, State).
handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast(?MODULE, Request, State).
handle_info(Info, State) -> default_gen_server_behaviour:handle_info(?MODULE, Info, State).

terminate(Reason, State = #state{journal_socket = JournalSocket}) ->
    default_gen_server_behaviour:terminate(?MODULE, Reason, State),
    zmq_utils:close_socket(JournalSocket).

code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================
