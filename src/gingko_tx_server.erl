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
-module(gingko_tx_server).

-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc.hrl").
-behaviour(gen_server).

%%The gingko_server handles the requests sent from the public API from gingko.
%%All requests are executed synchronously
%%The node_minimum_dependency_vts is the smallest dependency vts of any running transactions on the node. It is monotonically increasing (can stay the same for a while). It used to as a lower bound for newly started transactions. Also it is shared with all nodes in the dc so that the dc_minimum_dependency_vts can be determined.
%%The dc_minimum_dependency_vts is the smallest dependency vts of any running transaction in the whole dc. It is monotonically increasing (can stay the same for a while). It used for checkpointing since a checkpoint can only be performed before the dc_minimum_dependency_vts (similar to GCSt). The way it works is that all nodes in a cluster store their node_minimum_dependency_vts (together with the node name as the key) in a dc replicated mnesia database and once all nodes in a dc are represented in the database, the lowest vts of all is the dc_minimum_dependency_vts and since it is monotonically increasing it is safe to checkpoint at this vts because no transaction can be started prior to it.

-export([perform_request/2,
    read/2,
    update/2,
    transaction/2,
    prepare_txn/1,
    commit_txn/1,
    abort_txn/1,
    get_tx_id/1]).

-export([start_link/1,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    tx_id :: txid(),
    dependency_vts :: vectorclock(),
    is_prepared = false :: boolean(),
    is_finished = false :: boolean(),
    partition_to_ops_map = #{} :: #{partition() => [tx_op_num()]},
    tx_end_timer = none :: none | reference(),
    dependency_vts_not_valid_retries = 3 :: non_neg_integer() %%TODO maybe environment variable
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

-spec perform_request(txid(), {atom(), term()}) -> ok | {ok, snapshot()} | {ok, vectorclock()} | {error, reason()}.
perform_request(TxId = #tx_id{server_pid = Pid}, Request = {_Op, _Args}) ->
    gen_server:call(Pid, {Request, TxId}).

-spec read(txid(), key_struct()) -> {ok, snapshot()} | {error, reason()}.
read(TxId, KeyStruct) ->
    perform_request(TxId, {read, KeyStruct}).

-spec update(txid(), {key_struct(), type_op()}) -> ok | {error, reason()}.
update(TxId, Update = {#key_struct{}, _TypeOp}) ->
    perform_request(TxId, {update, Update}).

-spec transaction(txid(), [{key_struct(), type_op()}]) -> {ok, vectorclock()} | {error, reason()}.
transaction(TxId, UpdateList) ->
    perform_request(TxId, {transaction, UpdateList}).

-spec prepare_txn(txid()) -> ok | {error, reason()}.
prepare_txn(TxId) ->
    perform_request(TxId, {prepare_txn, none}).

-spec commit_txn(txid()) -> {ok, vectorclock()} | {error, reason()}.
commit_txn(TxId) ->
    perform_request(TxId, {commit_txn, none}).

-spec abort_txn(txid()) -> ok | {error, reason()}.
abort_txn(TxId) ->
    perform_request(TxId, {abort_txn, none}).

-spec get_tx_id(pid()) -> txid().
get_tx_id(Pid) ->
    gen_server:call(Pid, get_tx_id).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
%%TODO think about invalid calls e.g. after is_finished
start_link(DependencyVts) ->
    gen_server:start_link(?MODULE, [DependencyVts], []).

init([DependencyVts]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    TxId = #tx_id{server_pid = self(), local_start_time = gingko_dc_utils:get_timestamp()},
    gingko_dc_utils:add_tx_dependency_vts(TxId, DependencyVts),
    {ok, update_timer(false, #state{tx_id = TxId, dependency_vts = DependencyVts})}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request = get_tx_id, From, State = #state{tx_id = TxId}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, TxId, State};

handle_call({Request = {_Op, _Args}, TxId}, From, State = #state{tx_id = TxId}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {Result, NewState} = process_command(Request, State),
    {reply, Result, NewState};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call_crash(?MODULE, Request, From, State).

-spec handle_cast(term(), term()) -> no_return().
handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast_crash(?MODULE, Request, State).

handle_info(Info = terminate_tx, State = #state{is_finished = IsFinished, tx_end_timer = TxEndTimer}) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    erlang:cancel_timer(TxEndTimer),
    {_, NewState} =
        case IsFinished of
            true -> {ok, State};
            false -> internal_abort(State)
        end,
    {stop, normal, NewState#state{tx_end_timer = none, is_finished = true}};

handle_info(Info, State) -> default_gen_server_behaviour:handle_info_crash(?MODULE, Info, State).

terminate(Reason, State = #state{tx_id = TxId}) ->
    default_gen_server_behaviour:terminate(?MODULE, Reason, State),
    gingko_dc_utils:remove_tx_dependency_vts(TxId).

code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_error({error, dependency_vts_not_valid}, Command, State = #state{dependency_vts_not_valid_retries = Retries}) when Retries > 0 ->
    timer:sleep(1000),
    process_command(Command, State#state{dependency_vts_not_valid_retries = Retries - 1});
handle_error(Error = {error, _}, _, State) -> {Error, finish_tx_error(State)}.

-spec process_command({Op :: operation_type(), Args :: term()}, state()) ->
    {ok | {ok, snapshot()} | {ok, vectorclock()} | {error, reason()}, state()}.
process_command(Command = {read, KeyStruct}, State = #state{tx_id = TxId, dependency_vts = DependencyVts, is_prepared = false, is_finished = false}) ->%%TODO key
    Partition = gingko_dc_utils:get_key_partition(KeyStruct),
    try
        {ok, Snapshot} =
            case is_running_on_partition(Partition, State) of
                false ->
                    gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_CACHE, {get, KeyStruct, DependencyVts});
                true ->
                    gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG_HELPER, {{read, KeyStruct}, TxId})
            end,
        {{ok, Snapshot}, State}
    catch
        error:{badmatch, Error = {error, _}} -> handle_error(Error, Command, State)
    end;

process_command(Command = {update, Update = {KeyStruct, _TypeOp}}, State = #state{tx_id = TxId, dependency_vts = DependencyVts, is_prepared = false, is_finished = false}) ->
    Partition = gingko_dc_utils:get_key_partition(KeyStruct),
    try
        ok =
            case is_running_on_partition(Partition, State) of
                false -> gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{begin_txn, DependencyVts}, TxId});
                true -> ok
            end,
        {TxOpNumber, NewState} = get_next_tx_op_number_and_update_state(Partition, State),
        ok = gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG_HELPER, {{update, {Update, TxOpNumber}}, TxId}),
        {ok, NewState}
    catch
        error:{badmatch, Error = {error, _}} -> handle_error(Error, Command, State)
    end;

process_command({transaction, []}, State = #state{is_prepared = false, is_finished = false, partition_to_ops_map = Map}) when map_size(Map) == 0 ->
    CommitVts = gingko_dc_utils:get_DCSf_vts(),
    Result = {ok, CommitVts},
    {Result, finish_tx_success(State)};

process_command(Command = {transaction, [Update = {KeyStruct, _TypeOp}]}, State = #state{tx_id = TxId, dependency_vts = DependencyVts, is_prepared = false, is_finished = false, partition_to_ops_map = Map}) when map_size(Map) == 0 ->
    Partition = gingko_dc_utils:get_key_partition(KeyStruct),
    try
        ok = gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{begin_txn, DependencyVts}, TxId}),
        ok = gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG_HELPER, {{update, {Update, 1}}, TxId}),
        ok = gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{prepare_txn, {[Partition], [1]}}, TxId}),
        CommitVts = gingko_dc_utils:get_DCSf_vts(),
        ok = gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{commit_txn, CommitVts}, TxId}),
        {{ok, CommitVts}, finish_tx_success(State)}
    catch
        error:{badmatch, Error = {error, _}} -> handle_error(Error, Command, State)
    end;

%%TODO maybe implement retries
process_command({transaction, Updates}, State = #state{tx_id = TxId, dependency_vts = DependencyVts, is_prepared = false, is_finished = false, partition_to_ops_map = Map}) when map_size(Map) == 0 ->
    {_, NewIndexedUpdates} =
        lists:foldl(
            fun(Update, {CurrentIndex, CurrentList}) ->
                {CurrentIndex + 1, [{CurrentIndex, Update} | CurrentList]}
            end, {1, []}, Updates),
    UnsortedPartitionToIndexedUpdatesMap =
        general_utils:group_by_map(
            fun({_, {KeyStruct, _TypeOp}}) ->
                gingko_dc_utils:get_key_partition(KeyStruct)
            end, NewIndexedUpdates),
    PartitionToIndexedUpdatesMap =
        maps:map(
            fun(_, IndexedUpdates) ->
                lists:sort(IndexedUpdates)
            end, UnsortedPartitionToIndexedUpdatesMap),
    PartitionToIndexedUpdatesList = maps:to_list(PartitionToIndexedUpdatesMap),
    UpdateResults =
        lists:append(
            general_utils:parallel_map(
                fun({Partition, IndexedUpdates}) ->
                    BeginResult = gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{begin_txn, DependencyVts}, TxId}),
                    case BeginResult of
                        ok ->
                            lists:map(
                                fun({Index, Update}) ->
                                    UpdateResult = gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG_HELPER, {{update, {Update, Index}}, TxId}),
                                    {Update, UpdateResult}
                                end, IndexedUpdates);
                        Error ->
                            lists:map(
                                fun({_, Update}) ->
                                    {Update, Error}
                                end, IndexedUpdates)
                    end
                end, PartitionToIndexedUpdatesList)),
    PossibleUpdateErrors =
        lists:filtermap(
            fun({Update, UpdateResult}) ->
                case UpdateResult of
                    ok -> false;
                    {error, Reason} -> {true, {Update, Reason}}
                end
            end, UpdateResults),
    case PossibleUpdateErrors of
        [] ->
            Partitions = maps:keys(PartitionToIndexedUpdatesMap),
            PrepareResults =
                general_utils:parallel_map(
                    fun({Partition, IndexedUpdates}) ->
                        TxnOpNumList = lists:map(
                            fun({Index, _}) ->
                                Index
                            end, IndexedUpdates),
                        gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{prepare_txn, {Partitions, TxnOpNumList}}, TxId})
                    end, PartitionToIndexedUpdatesList),
            AllOk = general_utils:list_all_equal(ok, PrepareResults),
            case AllOk of
                true ->
                    CommitVts = gingko_dc_utils:get_DCSf_vts(),
                    CommitResults =
                        general_utils:parallel_map(
                            fun(Partition) ->
                                gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{commit_txn, CommitVts}, TxId})
                            end, Partitions),
                    AnyOk =
                        lists:any(
                            fun(PartitionCommitResult) ->
                                PartitionCommitResult == ok
                            end, CommitResults),
                    case AnyOk of
                        true -> {{ok, CommitVts}, finish_tx_success(State)};
                        false ->
                            {{error, "Commit might have failed"}, finish_tx_success(State)} %%TODO behaviour undefined
                    end;
                false -> {{error, {"Prepare Validation failed", PrepareResults}}, finish_tx_error(State)}
            end;
        Errors -> {{error, Errors}, finish_tx_error(State)}
    end;

process_command({prepare_txn, none}, State = #state{is_prepared = false, is_finished = false, partition_to_ops_map = Map}) when map_size(Map) == 0 ->
    {ok, State#state{is_prepared = true}};%%this is fine because if the transaction did no updates then we don't need to log it

process_command({prepare_txn, none}, State = #state{tx_id = TxId, is_prepared = false, is_finished = false, partition_to_ops_map = PartitionToOpsMap}) ->
    Partitions = maps:keys(PartitionToOpsMap),
    PrepareResults =
        general_utils:parallel_map(
            fun({Partition, TxOpNumList}) ->
                gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{prepare_txn, {Partitions, TxOpNumList}}, TxId})
            end, maps:to_list(PartitionToOpsMap)),
    AllOk = general_utils:list_all_equal(ok, PrepareResults),
    case AllOk of
        true -> {ok, State#state{is_prepared = true}};
        false -> {{error, {"Prepare Validation failed", PrepareResults}}, finish_tx_error(State)}
    end;

process_command({commit_txn, none}, State = #state{is_prepared = true, is_finished = false, partition_to_ops_map = Map}) when map_size(Map) == 0 ->
    CommitVts = gingko_dc_utils:get_DCSf_vts(),
    {{ok, CommitVts}, finish_tx_success(State)};%%this is fine because if the transaction did no updates then we don't need to log it

process_command({commit_txn, none}, State = #state{tx_id = TxId, is_prepared = true, is_finished = false, partition_to_ops_map = PartitionToOpsMap}) ->
    CommitVts = gingko_dc_utils:get_DCSf_vts(),
    Partitions = maps:keys(PartitionToOpsMap),
    CommitResults =
        general_utils:parallel_map(
            fun(Partition) ->
                gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{commit_txn, CommitVts}, TxId})
            end, Partitions),
    AnyOk = CommitResults == [] orelse
        lists:any(
            fun(PartitionCommitResult) ->
                PartitionCommitResult == ok
            end, CommitResults),
    case AnyOk of
        true -> {{ok, CommitVts}, finish_tx_success(State)};
        false ->
            {{error, "Commit might have failed"}, finish_tx_success(State)} %%TODO behaviour undefined
    end;

process_command({abort_txn, none}, State = #state{is_finished = false}) ->
    {ok, NewState} = internal_abort(State),
    {ok, finish_tx_success(NewState)};

process_command(Request, State) ->
    {{error, {"Invalid Request! Check State to see what went wrong", Request, State}}, finish_tx_error(State)}.

internal_abort(State = #state{partition_to_ops_map = Map}) when map_size(Map) == 0 ->
    {ok, State#state{is_finished = true}};%%this is fine because if the transaction did no updates then we don't need to log it
internal_abort(State = #state{tx_id = TxId, partition_to_ops_map = PartitionToOpsMap}) ->
    Partitions = maps:keys(PartitionToOpsMap),
    AbortResults =
        general_utils:parallel_map(
            fun(Partition) ->
                gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{abort_txn, none}, TxId})
            end, Partitions),
    AnyOk = AbortResults == [] orelse
        lists:any(
            fun(PartitionAbortResult) ->
                PartitionAbortResult == ok
            end, AbortResults),
    case AnyOk of
        true -> {ok, State#state{is_finished = true}};
        false ->
            {{error, "Abort might have failed"}, State#state{is_finished = true}} %%TODO behaviour undefined
    end.

-spec get_next_tx_op_number_and_update_state(partition(), state()) -> {pos_integer(), state()}.
get_next_tx_op_number_and_update_state(Partition, State = #state{partition_to_ops_map = PartitionToOpsMap}) ->
    TxOpNumLists = general_utils:get_values(PartitionToOpsMap),
    NextTxOpNum = lists:foldl(fun(List, CurrentTxOpNum) -> CurrentTxOpNum + length(List) end, 1, TxOpNumLists),
    UpdatedMap = general_utils:maps_append(Partition, NextTxOpNum, PartitionToOpsMap),
    {NextTxOpNum, State#state{partition_to_ops_map = UpdatedMap}}.

-spec is_running_on_partition(partition(), state()) -> boolean().
is_running_on_partition(Partition, #state{partition_to_ops_map = PartitionToOpsMap}) ->
    maps:is_key(Partition, PartitionToOpsMap).

-spec finish_tx_success(state()) -> state().
finish_tx_success(State) ->
    update_timer(true, State#state{is_finished = true}).

-spec finish_tx_error(state()) -> state().
finish_tx_error(State) ->
    update_timer(true, State#state{is_finished = false}).

-spec update_timer(boolean(), state()) -> state().
update_timer(EndTxNow, State = #state{tx_end_timer = Timer}) ->
    MaxTxRunTimeMillis =
        case EndTxNow of
            true -> ?DEFAULT_WAIT_TIME_SHORT;
            false -> gingko_env_utils:get_max_tx_run_time_millis()
        end,
    TxEndTimer = gingko_dc_utils:update_timer(Timer, true, MaxTxRunTimeMillis, terminate_tx, false),
    State#state{tx_end_timer = TxEndTimer}.
