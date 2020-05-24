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

-module(inter_dc_txn_manager).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc_repl.hrl").

-behaviour(gen_server).

-export([buffer_journal_entry/2]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    begin_journal_entries = dict:new() :: dict:dict(txid(), {partition_id(), journal_entry()}),
    other_journal_entries = dict:new() :: dict:dict(txid(), [{partition_id(), journal_entry()}]),
    prepare_journal_entries = dict:new() :: dict:dict(txid(), {partition_id(), journal_entry()})
}).

%%%===================================================================
%%% Public API
%%%===================================================================

-spec buffer_journal_entry(journal_entry(), partition_id()) -> ok.
buffer_journal_entry(JournalEntry, Partition) ->
    gen_server:cast(?MODULE, {buffer_journal_entry, JournalEntry, Partition}).

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

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call(?MODULE, Request, From, State).

handle_cast(Request = {buffer_journal_entry, JournalEntry = #journal_entry{tx_id = TxId, operation = #object_operation{}}, Partition}, State = #state{other_journal_entries = OtherDict}) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    {noreply, State#state{other_journal_entries = general_utils:add_to_value_list_or_create_single_value_list(TxId, JournalEntry, OtherDict)}};

handle_cast(Request = {buffer_journal_entry, BeginJournalEntry = #journal_entry{tx_id = TxId, operation = #system_operation{op_type = begin_txn}}, Partition}, State = #state{begin_journal_entries = BeginDict}) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    {noreply, State#state{begin_journal_entries = dict:store(TxId, BeginJournalEntry, BeginDict)}};

handle_cast(Request = {buffer_journal_entry, PrepareJournalEntry = #journal_entry{tx_id = TxId, operation = #system_operation{op_type = prepare_txn}}, Partition}, State = #state{prepare_journal_entries = PrepareDict}) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    {noreply, State#state{begin_journal_entries = dict:store(TxId, PrepareJournalEntry, PrepareDict)}};

handle_cast(Request = {buffer_journal_entry, #journal_entry{tx_id = TxId, operation = #system_operation{op_type = abort_txn}}, Partition}, State = #state{begin_journal_entries = BeginDict, prepare_journal_entries = PrepareDict, other_journal_entries = OtherDict}) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    %%TODO right now these are not sent because these txns don't matter
    {noreply, State#state{begin_journal_entries = dict:erase(TxId, BeginDict), prepare_journal_entries = dict:erase(TxId, PrepareDict), other_journal_entries = dict:erase(TxId, OtherDict)}};

handle_cast(Request = {buffer_journal_entry, CommitJournalEntry = #journal_entry{tx_id = TxId, operation = #system_operation{op_type = commit_txn}}, Partition}, State = #state{begin_journal_entries = BeginDict, prepare_journal_entries = PrepareDict, other_journal_entries = OtherDict}) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    BeginJournalEntryResult = dict:find(TxId, BeginDict),
    PrepareJournalEntryResult = dict:find(TxId, PrepareDict),
    OtherJournalEntriesResult = dict:find(TxId, OtherDict),
    JournalEntriesOrError = check_and_merge_journal_entries(BeginJournalEntryResult, PrepareJournalEntryResult, OtherJournalEntriesResult, CommitJournalEntry),
    NewState =
        case JournalEntriesOrError of
            error -> State; %%TODO this should never happen
            JournalEntries ->
                inter_dc_txn_sender:broadcast_journal_entries(JournalEntries),
                State#state{begin_journal_entries = dict:erase(TxId, BeginDict), prepare_journal_entries = dict:erase(TxId, PrepareDict), other_journal_entries = dict:erase(TxId, OtherDict)}
        end,
    {noreply, NewState};

handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast(?MODULE, Request, State).
handle_info(Info, State) -> default_gen_server_behaviour:handle_info(?MODULE, Info, State).
terminate(Reason, State) -> default_gen_server_behaviour:terminate(?MODULE, Reason, State).
code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

check_and_merge_journal_entries({ok, BeginJournalEntry}, {ok, PrepareJournalEntry}, error, CommitJournalEntry) ->
    check_and_merge_journal_entries({ok, BeginJournalEntry}, {ok, PrepareJournalEntry}, {ok, []}, CommitJournalEntry);
check_and_merge_journal_entries({ok, BeginJournalEntry}, {ok, PrepareJournalEntry}, {ok, OtherJournalEntries}, CommitJournalEntry) ->
    [BeginJournalEntry | OtherJournalEntries] ++ [PrepareJournalEntry, CommitJournalEntry];
check_and_merge_journal_entries(_, _, _, _) -> error.
