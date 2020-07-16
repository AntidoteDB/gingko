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
-include("inter_dc.hrl").
-behaviour(gen_server).

%%The gingko_server handles the requests sent from the public API from gingko.
%%All requests are executed synchronously
%%The node_minimum_dependency_vts is the smallest dependency vts of any running transactions on the node. It is monotonically increasing (can stay the same for a while). It used to as a lower bound for newly started transactions. Also it is shared with all nodes in the dc so that the dc_minimum_dependency_vts can be determined.
%%The dc_minimum_dependency_vts is the smallest dependency vts of any running transaction in the whole dc. It is monotonically increasing (can stay the same for a while). It used for checkpointing since a checkpoint can only be performed before the dc_minimum_dependency_vts (similar to GCSt). The way it works is that all nodes in a cluster store their node_minimum_dependency_vts (together with the node name as the key) in a dc replicated mnesia database and once all nodes in a dc are represented in the database, the lowest vts of all is the dc_minimum_dependency_vts and since it is monotonically increasing it is safe to checkpoint at this vts because no transaction can be started prior to it.

-export([perform_request/1]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    running_txns = #{} :: #{txid() => vectorclock()},
    running_txid_to_partition_tx_op_num_list_map = #{} :: #{txid() => #{partition_id() => [tx_op_num()]}},
    prepared_txns = ordsets:new() :: ordsets:ordset(txid()),
    node_minimum_dependency_vts = none :: none | vectorclock(),
    dc_minimum_dependency_vts = none :: none | vectorclock()
}).
-type state() :: #state{}.

-define(TABLE_NAME, distributed_vts).

%%%===================================================================
%%% Public API
%%%===================================================================

-spec perform_request({{atom(), term()}, txid()}) -> ok | {ok, snapshot()} | {error, reason()}.
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

handle_call(Request = initialize, From, State = #state{node_minimum_dependency_vts = none}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, initialize_node_minimum_dependency_vts(State)};

handle_call(Request, From, State = #state{node_minimum_dependency_vts = none}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, {error, "node_minimum_dependency_vts not initialized"}, State};

handle_call(Request, From, State = #state{dc_minimum_dependency_vts = none}) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    case get_dc_minimum_dependency_vts() of
        {ok, DcMinimumDependencyVts} ->
            handle_call(Request, From, State#state{dc_minimum_dependency_vts = DcMinimumDependencyVts});
        Error -> {reply, Error, State}
    end;

handle_call(Request = {{_Op, _Args}, _TxId}, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {Result, NewState} = process_command(Request, State),
    {reply, Result, NewState};

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

-spec process_command({{Op :: operation_type(), Args :: term()}, txid() | vectorclock()}, state()) -> {term(), state()}.
process_command({{read, KeyStruct}, TxId = #tx_id{}}, State) ->%%TODO key
    Result =
        case check_state(true, false, false, true, TxId, State) of
            ok ->
                Partition = gingko_utils:get_key_partition(KeyStruct),
                case is_running_on_partition(TxId, Partition, State) of
                    {false, BeginVts} ->
                        gingko_utils:call_gingko_sync(Partition, ?GINGKO_CACHE, {get, KeyStruct, BeginVts});
                    true ->
                        gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG_HELPER, {{read, KeyStruct}, TxId})
                end;
            Error -> Error
        end,
    {Result, State};
process_command({{read, KeyStruct}, ReadVts}, State) ->%%TODO key
    {gingko_utils:call_gingko_sync_with_key(KeyStruct, ?GINGKO_CACHE, {get, KeyStruct, ReadVts}), State};

process_command({{update, Update = {KeyStruct, _TypeOp}}, TxId}, State = #state{running_txid_to_partition_tx_op_num_list_map = TxIdToOps}) ->
    case check_state(true, false, false, true, TxId, State) of
        ok ->
            Partition = gingko_utils:get_key_partition(KeyStruct),
            BeginResult =
                case is_running_on_partition(TxId, Partition, State) of
                    {false, BeginVts} ->
                        gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{begin_txn, BeginVts}, TxId});
                    true -> ok
                end,
            case BeginResult of
                ok ->
                    TxOpNumber = get_next_tx_op_number(TxId, TxIdToOps),
                    UpdatedTxIdToOps = general_utils:maps_inner_append(TxId, Partition, TxOpNumber, TxIdToOps),
                    Result = gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG_HELPER, {{update, {Update, TxOpNumber}}, TxId}),
                    {Result, State#state{running_txid_to_partition_tx_op_num_list_map = UpdatedTxIdToOps}};
                Error -> {Error, State}
            end;
        Error -> {Error, State}
    end;
%%TODO abort failed transactions directly
process_command({{transaction, {BeginVts, [Update = {KeyStruct, _TypeOp}]}}, TxId}, State) ->
    Partition = gingko_utils:get_key_partition(KeyStruct),
    Result =
        case gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{begin_txn, BeginVts}, TxId}) of
            ok ->
                case gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG_HELPER, {{update, {Update, 1}}, TxId}) of
                    ok ->
                        case gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{prepare_txn, {[Partition], [1]}}, TxId}) of
                            ok ->
                                CommitVts = gingko_utils:get_DCSf_vts(),
                                case gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{commit_txn, CommitVts}, TxId}) of
                                    ok -> {ok, CommitVts};
                                    Error -> Error
                                end;
                            Error -> Error
                        end;
                    Error -> Error
                end;
            Error -> Error
        end,
    {Result, State};
process_command({{transaction, {BeginVts, Updates}}, TxId}, State) ->
    Result =
        case check_state(false, true, false, true, TxId, State) of
            ok ->
                {_, NewIndexedUpdates} =
                    lists:foldl(
                        fun(Update, {CurrentIndex, CurrentList}) ->
                            {CurrentIndex + 1, [{CurrentIndex, Update} | CurrentList]}
                        end, {1, []}, Updates),
                UnsortedPartitionToIndexedUpdatesMap =
                    general_utils:group_by_map(
                        fun({_, {KeyStruct, _TypeOp}}) ->
                            gingko_utils:get_key_partition(KeyStruct)
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
                                BeginResult = gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{begin_txn, BeginVts}, TxId}),
                                case BeginResult of
                                    ok ->
                                        lists:map(
                                            fun({Index, Update}) ->
                                                UpdateResult = gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG_HELPER, {{update, {Update, Index}}, TxId}),
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
                                    TxnOpNumList = lists:map(fun({Index, _}) -> Index end, IndexedUpdates),
                                    gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{prepare_txn, {Partitions, TxnOpNumList}}, TxId})
                                end, PartitionToIndexedUpdatesList),
                        AllOk = general_utils:list_all_equal(ok, PrepareResults),
                        case AllOk of
                            true ->
                                CommitVts = gingko_utils:get_DCSf_vts(),
                                CommitResults =
                                    general_utils:parallel_map(
                                        fun(Partition) ->
                                            gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{commit_txn, CommitVts}, TxId})
                                        end, Partitions),
                                AnyOk =
                                    lists:any(
                                        fun(PartitionCommitResult) ->
                                            PartitionCommitResult == ok
                                        end, CommitResults),
                                case AnyOk of
                                    true -> {ok, CommitVts};
                                    false -> {error, "Commit might have failed"} %%TODO behaviour undefined
                                end;
                            false -> {error, {"Prepare Validation failed", PrepareResults}}
                        end;
                    Errors -> {error, Errors}
                end;
            Error -> Error
        end,
    {Result, State};

process_command({{begin_txn, BeginVts}, TxId}, State = #state{dc_minimum_dependency_vts = GlobalMinimumDependencyVts}) ->
    NewGlobalMinimumDependencyVts =
        case GlobalMinimumDependencyVts of
            none -> get_dc_minimum_dependency_vts();
            _ -> GlobalMinimumDependencyVts
        end,
    case check_state(false, true, false, true, TxId, State) of
        ok ->
            NewThanMinimum = vectorclock:ge(BeginVts, NewGlobalMinimumDependencyVts),
            case NewThanMinimum of
                true ->
                    {ok, add_running(TxId, BeginVts, State#state{dc_minimum_dependency_vts = GlobalMinimumDependencyVts})};
                false -> {error, "A transaction cannot be started earlier than all running transactions"}
            end;
        Error -> {Error, State}
    end;

process_command({{prepare_txn, none}, TxId}, State = #state{running_txid_to_partition_tx_op_num_list_map = TxIdToOps}) ->
    case check_state(true, false, false, true, TxId, State) of
        ok ->
            case maps:find(TxId, TxIdToOps) of
                error ->
                    {ok, add_prepared(TxId, State)};%%this is fine because if the transaction did no updates then we don't need to log it
                {ok, PartitionToTxOpNumListMap} ->
                    Partitions = maps:keys(PartitionToTxOpNumListMap),
                    PrepareResults =
                        general_utils:parallel_map(
                            fun({Partition, TxOpNumList}) ->
                                gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{prepare_txn, {Partitions, TxOpNumList}}, TxId})
                            end, PartitionToTxOpNumListMap),
                    AllOk = general_utils:list_all_equal(ok, PrepareResults),
                    case AllOk of
                        true -> {ok, add_prepared(TxId, State)};
                        false -> {{error, {"Prepare Validation failed", PrepareResults}}, State}
                    end
            end;
        Error -> {Error, State}
    end;

process_command(Request = {{commit_txn, CommitVts}, TxId}, State = #state{running_txid_to_partition_tx_op_num_list_map = TxIdToOps}) ->
    case check_state(true, false, true, false, TxId, State) of
        ok ->
            case maps:find(TxId, TxIdToOps) of
                error ->
                    {{ok, CommitVts}, clean_state(TxId, State)};
                {ok, PartitionToTxOpNumListMap} ->
                    Partitions = maps:keys(PartitionToTxOpNumListMap),
                    CommitResults =
                        general_utils:parallel_map(
                            fun(Partition) ->
                                gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, Request)
                            end, Partitions),
                    AnyOk =
                        lists:any(
                            fun(PartitionCommitResult) ->
                                PartitionCommitResult == ok
                            end, CommitResults),
                    case AnyOk of
                        true -> {{ok, CommitVts}, clean_state(TxId, State)};
                        false -> {{error, "Commit might have failed"}, State} %%TODO behaviour undefined
                    end
            end;
        Error -> {Error, State}
    end;

process_command(Request = {{abort_txn, none}, TxId}, State = #state{running_txid_to_partition_tx_op_num_list_map = TxIdToOps}) ->
    case check_state(true, false, false, false, TxId, State) of
        ok ->
            case maps:find(TxId, TxIdToOps) of
                error ->
                    {ok, clean_state(TxId, State)};
                {ok, PartitionToTxOpNumListMap} ->
                    Partitions = maps:keys(PartitionToTxOpNumListMap),
                    AbortResults =
                        general_utils:parallel_map(
                            fun(Partition) ->
                                gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, Request)
                            end, Partitions),
                    AnyOk =
                        lists:any(
                            fun(PartitionAbortResult) ->
                                PartitionAbortResult == ok
                            end, AbortResults),
                    case AnyOk of
                        true -> {ok, clean_state(TxId, State)};
                        false -> {{error, "Abort might have failed"}, State} %%TODO behaviour undefined
                    end
            end;
        Error -> {Error, State}
    end;

%%We check whether we need to abort local transactions to perform the checkpoint (transactions older than two checkpoint intervals will be aborted)
%%TODO make this varible so that old transactions may get some more time to finish!
process_command({{checkpoint, DependencyVts}, TxId}, State = #state{running_txns = RunningTxns, dc_minimum_dependency_vts = GlobalMinimumDependencyVts}) ->
    MyDcId = gingko_utils:get_my_dcid(),
    MinimumDependencyClockTime = vectorclock:get(MyDcId, GlobalMinimumDependencyVts),
    DependencyClockTime = vectorclock:get(MyDcId, DependencyVts),
    CheckpointIntervalMillis = gingko_checkpoint_service:get_checkpoint_interval_millis(),
    AbortOldTransactions = (DependencyClockTime - MinimumDependencyClockTime) > CheckpointIntervalMillis * 2 * 1000,
    NewState =
        case AbortOldTransactions of
            false -> State;
            true ->
                maps:fold(
                    fun(RunningTxId, BeginVts, CurrentState) ->
                        LocalBeginClockTime = vectorclock:get(MyDcId, BeginVts),
                        AbortThisTransactions = (DependencyClockTime - LocalBeginClockTime) > CheckpointIntervalMillis * 2 * 1000,
                        {ok, NextState} = %%TODO potential error not handled
                        case AbortThisTransactions of
                            true -> process_command({{abort_txn, none}, RunningTxId}, CurrentState);
                            false -> CurrentState
                        end,
                        NextState
                    end, State, RunningTxns)
        end,
    update_dc_minimum_dependency_vts(NewState),
    NewDcMinimumDependencyVtsResult = get_dc_minimum_dependency_vts(),
    case NewDcMinimumDependencyVtsResult of
        {ok, NewDcMinimumDependencyVts} ->
            CheckpointVts = vectorclock:min([DependencyVts, NewDcMinimumDependencyVts]),
            CheckpointResult = gingko_utils:bcast_local_gingko_sync(?GINGKO_LOG, {{checkpoint, CheckpointVts}, TxId}),%%Checkpoints only get applied to local partitions because they are reoccurring processes on all nodes
            {CheckpointResult, NewState#state{dc_minimum_dependency_vts = NewDcMinimumDependencyVts}};
        Error -> Error
    end.

-spec get_next_tx_op_number(txid(), #{txid() => #{partition_id() => [non_neg_integer()]}}) -> pos_integer().
get_next_tx_op_number(TxId, TxIdToOps) ->
    PartitionToTxOpNumListMap = maps:get(TxId, TxIdToOps, #{}),
    TxOpNumLists = general_utils:get_values(PartitionToTxOpNumListMap),
    length(lists:append(TxOpNumLists)) + 1.

-spec check_state(boolean(), boolean(), boolean(), boolean(), txid(), state()) -> ok | {error, reason()}.
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

-spec initialize_node_minimum_dependency_vts(state()) -> state().
initialize_node_minimum_dependency_vts(State = #state{node_minimum_dependency_vts = none}) ->
    InitialMinimumDependencyVts = gingko_utils:get_DCSf_vts(),
    NewState = State#state{node_minimum_dependency_vts = InitialMinimumDependencyVts},
    update_dc_minimum_dependency_vts(NewState),
    NewState.

-spec get_dc_minimum_dependency_vts() -> {ok, vectorclock()} | {error, reason()}.
get_dc_minimum_dependency_vts() ->
    F = fun() ->
        mnesia:foldl(
            fun(DistributedVts, DistributedVtsAcc) ->
                [DistributedVts | DistributedVtsAcc]
            end, [], distributed_vts)
        end,
    Result = mnesia_utils:run_transaction(F),
    case Result of
        {error, Reason} -> {error, Reason};
        DistributedVtsList ->
            AllNodes = gingko_utils:get_my_dc_nodes(),
            AllNodesOfDistributedVts = lists:map(fun(#distributed_vts{node = Node}) -> Node end, DistributedVtsList),
            case general_utils:set_equals_on_lists(AllNodes, AllNodesOfDistributedVts) of
                true ->
                    {ok, vectorclock:min(lists:map(fun(#distributed_vts{vts = Vts}) -> Vts end, DistributedVtsList))};
                false ->
                    {error, "dc_minimum_dependency_vts not synchronized"}
            end
    end.

-spec update_dc_minimum_dependency_vts(state()) -> ok.
update_dc_minimum_dependency_vts(#state{node_minimum_dependency_vts = NodeMinimumDependencyVts}) ->
    F = fun() -> mnesia:write(?TABLE_NAME, #distributed_vts{node = node(), vts = NodeMinimumDependencyVts}, write) end,
    mnesia_utils:run_transaction(F).

-spec is_running(txid(), state()) -> boolean().
is_running(TxId, #state{running_txns = RunningTxns}) ->
    maps:is_key(TxId, RunningTxns).

-spec is_running_on_partition(txid(), partition_id(), state()) -> true | {false, vectorclock()}.
is_running_on_partition(TxId, Partition, #state{running_txns = RunningTxns, running_txid_to_partition_tx_op_num_list_map = TxIdToOps}) ->
    case maps:find(TxId, TxIdToOps) of
        error ->
            {false, maps:get(TxId, RunningTxns)};
        {ok, PartitionToOps} ->
            case maps:is_key(Partition, PartitionToOps) of
                true -> true;
                false -> {false, maps:get(TxId, RunningTxns)}
            end
    end.

-spec add_running(txid(), vectorclock(), state()) -> state().
add_running(TxId, BeginVts, State = #state{running_txns = RunningTxns}) ->
    State#state{running_txns = RunningTxns#{TxId => BeginVts}}.

-spec is_prepared(txid(), state()) -> boolean().
is_prepared(TxId, #state{prepared_txns = PreparedTxns}) ->
    ordsets:is_element(TxId, PreparedTxns).

-spec add_prepared(txid(), state()) -> state().
add_prepared(TxId, State = #state{prepared_txns = PreparedTxns}) ->
    State#state{prepared_txns = ordsets:add_element(TxId, PreparedTxns)}.

-spec clean_state(txid(), state()) -> state().
clean_state(TxId, State = #state{running_txns = RunningTxns, running_txid_to_partition_tx_op_num_list_map = TxIdToOps, prepared_txns = PreparedTxns}) ->
    UpdatedRunningTxns = maps:remove(TxId, RunningTxns),
    UpdatedTxIdToOps = maps:remove(TxId, TxIdToOps),
    UpdatedPreparedTxns = ordsets:del_element(TxId, PreparedTxns),
    NewMinimumDependencyVts =
        case maps:values(UpdatedRunningTxns) of
            [] -> gingko_utils:get_DCSf_vts();
            _ -> vectorclock:min(maps:values(UpdatedRunningTxns))
        end,
    State#state{running_txns = UpdatedRunningTxns, running_txid_to_partition_tx_op_num_list_map = UpdatedTxIdToOps, prepared_txns = UpdatedPreparedTxns, node_minimum_dependency_vts = NewMinimumDependencyVts}.

