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

-module(gingko_server).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").
-behaviour(gen_server).

-export([perform_request/1]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    running_txns = ordsets:new() :: ordsets:ordset(txid()),
    running_txid_to_ops = dict:new() :: dict:dict(txid(), dict:dict(partition_id(), [non_neg_integer()])),
    prepared_txns = ordsets:new() :: ordsets:ordset(txid())
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

-spec perform_request({{atom(), term()}, txid()}) -> ok | {ok, snapshot_value()} | {error, reason()}.
perform_request(Request = {{_Op, _Args}, _TxId}) ->
    gen_server:call(?MODULE, Request).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    {ok, #state{}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request = {{_Op, _Args}, _TxId}, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    process_command(Request, State);

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call_crash(?MODULE, Request, From, State).
-spec handle_cast(term(), term()) -> no_return().
handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast_crash(?MODULE, Request, State).
-spec handle_info(term(), term()) -> no_return().
handle_info(Info, State) -> default_gen_server_behaviour:handle_info_crash(?MODULE, Info, State).
terminate(Reason, State) -> default_gen_server_behaviour:terminate(?MODULE, Reason, State).
code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec process_command({{Op :: journal_entry_type(), Args :: term()}, txid()}, state()) -> {reply, term(), state()}.
process_command({{read, KeyStruct}, TxId}, State = #state{running_txid_to_ops = TxIdToOps}) ->
    case check_state(true, false, false, true, TxId, State) of
        ok ->
            {PartitionId, _Node} = gingko_utils:get_key_partition(KeyStruct),
            TxOpNumber = get_tx_op_number(TxId, TxIdToOps),
            UpdatedTxIdToOps = general_utils:append_inner_dict(TxId, PartitionId, TxOpNumber, TxIdToOps),
            Result = gingko_utils:call_gingko_sync_with_key(KeyStruct, ?GINGKO_LOG_HELPER, {{read, KeyStruct}, {TxId, TxOpNumber}}),
            {reply, Result, State#state{running_txid_to_ops = UpdatedTxIdToOps}};
        Error -> {reply, Error, State}
    end;

process_command({{update, {KeyStruct, TypeOp}}, TxId}, State = #state{running_txid_to_ops = TxIdToOps}) ->
    case check_state(true, false, false, true, TxId, State) of
        ok ->
            {PartitionId, _Node} = gingko_utils:get_key_partition(KeyStruct),
            TxOpNumber = get_tx_op_number(TxId, TxIdToOps),
            UpdatedTxIdToOps = general_utils:append_inner_dict(TxId, PartitionId, TxOpNumber, TxIdToOps),

            Result = gingko_utils:call_gingko_async_with_key(KeyStruct, ?GINGKO_LOG, {{update, {KeyStruct, TypeOp}}, {TxId, TxOpNumber}}),
            {reply, Result, State#state{running_txid_to_ops = UpdatedTxIdToOps}};
        Error -> {reply, Error, State}
    end;

process_command(Request = {{begin_txn, _DependencyVts}, TxId}, State) ->
    case check_state(false, true, false, true, TxId, State) of
        ok ->
            {reply, gingko_utils:bcast_gingko_async(?GINGKO_LOG, Request), add_running(TxId, State)};
        Error -> {reply, Error, State}
    end;

process_command({{prepare_txn, none}, TxId}, State = #state{running_txid_to_ops = TxIdToOps}) ->
    case check_state(true, false, false, true, TxId, State) of
        ok ->
            case dict:find(TxId, TxIdToOps) of
                error -> {reply, ok, add_prepared(TxId, State)};
                {ok, PartitionToRequestsDict} ->
                    Partitions = dict:fetch_keys(PartitionToRequestsDict),
                    PrepareResults =
                        general_utils:parallel_map(
                            fun(Partition) ->
                                Requests = dict:fetch(Partition, PartitionToRequestsDict),
                                gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{prepare_txn, none}, {TxId, Requests}})
                            end, Partitions),
                    AllOk = general_utils:list_all_equal(ok, PrepareResults),
                    case AllOk of
                        true -> {reply, ok, add_prepared(TxId, State)};
                        false -> {reply, {error, "Validation failed"}, State}
                    end
            end;
        Error -> {reply, Error, State}
    end;

process_command(Request = {{commit_txn, _CommitVts}, TxId}, State) ->
    case check_state(true, false, true, false, TxId, State) of
        ok ->
            NewState = clean_state(TxId, State),
            {reply, gingko_utils:bcast_gingko_async(?GINGKO_LOG, Request), NewState};
        Error -> {reply, Error, State}
    end;

process_command(Request = {{abort_txn, none}, TxId}, State) ->
    case check_state(true, false, false, false, TxId, State) of
        ok ->
            NewState = clean_state(TxId, State),
            {reply, gingko_utils:bcast_gingko_async(?GINGKO_LOG, Request), NewState};
        Error -> {reply, Error, State}
    end;

process_command(Request = {{checkpoint, _DependencyVts}, _TxId}, State) ->
    {reply, gingko_utils:bcast_gingko_sync(?GINGKO_LOG_HELPER, Request), State}.

get_tx_op_number(TxId, TxIdToOps) ->
    PartitionToRequestLists = general_utils:get_or_default_dict(TxId, TxIdToOps, dict:new()),
    RequestLists = general_utils:get_values(PartitionToRequestLists),
    length(lists:append(RequestLists)) + 1.

check_state(MustRun, MustNotRun, MustBePrepared, MustNotBePrepared, TxId, State) ->
    IsRunning = is_running(TxId, State),
    IsPrepared = is_prepared(TxId, State),
    case MustRun andalso (not IsRunning) of
        true -> {error, "Not Running"};
        false ->
            case MustNotRun andalso IsRunning of
                true -> {error, "Already Running"};
                false ->
                    case MustBePrepared andalso (not IsPrepared) of
                        true -> {error, "Not Prepared"};
                        false ->
                            case MustNotBePrepared andalso IsPrepared of
                                true -> {error, "Already Prepared"};
                                false -> ok
                            end
                    end
            end
    end.

is_running(TxId, #state{running_txns = RunningTxns}) ->
    ordsets:is_element(TxId, RunningTxns).

add_running(TxId, State = #state{running_txns = RunningTxns}) ->
    State#state{running_txns = ordsets:add_element(TxId, RunningTxns)}.

is_prepared(TxId, #state{prepared_txns = PreparedTxns}) ->
    ordsets:is_element(TxId, PreparedTxns).

add_prepared(TxId, State = #state{prepared_txns = PreparedTxns}) ->
    State#state{prepared_txns = ordsets:add_element(TxId, PreparedTxns)}.

clean_state(TxId, State = #state{running_txns = RunningTxns, running_txid_to_ops = TxIdToOps, prepared_txns = PreparedTxns}) ->
    UpdatedRunningTxns = ordsets:del_element(TxId, RunningTxns),
    UpdatedTxIdToOps = dict:erase(TxId, TxIdToOps),
    UpdatedPreparedTxns = ordsets:del_element(TxId, PreparedTxns),
    State#state{running_txns = UpdatedRunningTxns, running_txid_to_ops = UpdatedTxIdToOps, prepared_txns = UpdatedPreparedTxns}.

