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

-module(gingko_log_vnode).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").
-behaviour(riak_core_vnode).

-export([start_vnode/1,
    init/1,
    handle_command/3,
    handoff_starting/2,
    handoff_cancelled/1,
    handoff_finished/2,
    handle_handoff_command/3,
    handle_handoff_data/2,
    encode_handoff_item/2,
    is_empty/1,
    terminate/2,
    delete/1,
    handle_info/2,
    handle_exit/3,
    handle_coverage/4,
    handle_overload_command/3,
    handle_overload_info/2]).

-record(dc_txn_tracking_state, {
    last_checkpoint_txn_tracking_num = gingko_utils:get_default_txn_tracking_num() :: txn_tracking_num(),
    invalid_txn_tracking_num_gb_set = gb_sets:new() :: gb_sets:set(invalid_txn_tracking_num()),
    valid_txn_tracking_num_gb_set = gb_sets:new() :: gb_sets:set(txn_tracking_num())
}).
-type dc_txn_tracking_state() :: dc_txn_tracking_state().

-record(state, {
    partition = 0 :: partition(),
    table_name :: atom(),
    next_jsn = 0 :: non_neg_integer(),
    latest_vts = vectorclock:new() :: vectorclock(),
    dcid_to_dc_txn_tracking_state = #{} :: #{dcid() => dc_txn_tracking_state()},
    %%TODO truncate lists
    initialized = false :: boolean()
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

%%TODO prevent usage before initialized
%%%===================================================================
%%% Spawning and vnode implementation
%%%===================================================================

-spec start_vnode(integer()) -> any().
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    default_vnode_behaviour:init(?MODULE, [Partition]),
    TableName = general_utils:concat_and_make_atom([integer_to_list(Partition), '_journal_entry']),
    NewState = #state{partition = Partition, table_name = TableName},
    {_Reply, FinalState} = initialize_or_repair(NewState),
    {ok, FinalState}.

handle_command(Request = hello, Sender, State = #state{initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    Reply = case Initialized of
                false -> ok;
                true -> ok
            end,
    {reply, Reply, State};

handle_command(Request = get_journal_mnesia_table_name, Sender, State = #state{table_name = TableName, initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    Reply = case Initialized of
                false -> {error, not_initialized};
                true -> {ok, TableName}
            end,
    {reply, Reply, State};

handle_command(Request = get_current_dependency_vts, Sender, State = #state{latest_vts = LatestVts, initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {Reply, NewState} =
        case Initialized of
            false -> {{error, not_initialized}, State};
            true ->
                NewLatestVts = vectorclock:set(gingko_dc_utils:get_my_dcid(), gingko_dc_utils:get_timestamp(), LatestVts),
                {{ok, NewLatestVts}, State#state{latest_vts = NewLatestVts}}
        end,
    {reply, Reply, NewState};

%%TODO consider timing issues during start-up with large tables
%%TODO failure testing required
%%TODO maybe consider migration for later
%%TODO this needs to reexecutable
handle_command(Request = initialize, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {Reply, NewState} = initialize_or_repair(State),
    {reply, Reply, NewState};

handle_command(Request = {get_txns, TxnNumList}, Sender, State = #state{table_name = TableName, initialized = Initialized, dcid_to_dc_txn_tracking_state = DcIdToDcTxnTrackingState}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    Reply =
        case Initialized of
            false -> {error, not_initialized};
            true ->
                MyDcId = gingko_dc_utils:get_my_dcid(),
                %%TODO not optimized at all
                #dc_txn_tracking_state{valid_txn_tracking_num_gb_set = ValidSet} = maps:get(MyDcId, DcIdToDcTxnTrackingState, #dc_txn_tracking_state{}),
                TxIdList = lists:filtermap(
                    fun({TxnNum, TxId, _}) ->
                        case lists:member(TxnNum, TxnNumList) of
                            true -> {true, TxId};
                            false -> false
                        end
                    end, gb_sets:to_list(ValidSet)),
                %%TODO possible problem if transactions are requested that don't exist (shoud not happen though)
                JournalEntryList = gingko_log_utils:read_journal_entries_with_multiple_tx_ids(TxIdList, TableName),
                {ok, JournalEntryList}
        end,
    {reply, Reply, State};

handle_command(Request = {get_valid_journal_entries, DependencyVts}, Sender, State = #state{initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    Reply =
        case Initialized of
            false -> {error, not_initialized};
            true -> get_valid_journal_entries(DependencyVts, State)
        end,
    {reply, Reply, State};

handle_command(Request = {add_remote_txn, SortedTxJournalEntryList}, Sender, State = #state{initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {Reply, NewState} = case Initialized of
                            false -> {{error, not_initialized}, State};
                            true -> {ok, add_remote_txn(SortedTxJournalEntryList, State)}
                        end,
    {reply, Reply, NewState};

handle_command(Request = {{_Op, _Args}, #tx_id{}}, Sender, State = #state{initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {Result, NewState} =
        case Initialized of
            false -> {{error, not_initialized}, State};
            true -> process_command(Request, State)
        end,
    {reply, Result, NewState};

handle_command(Request, Sender, State) -> default_vnode_behaviour:handle_command_crash(?MODULE, Request, Sender, State).
handoff_starting(TargetNode, State) -> default_vnode_behaviour:handoff_starting(?MODULE, TargetNode, State).
handoff_cancelled(State) -> default_vnode_behaviour:handoff_cancelled(?MODULE, State).
handoff_finished(TargetNode, State) -> default_vnode_behaviour:handoff_finished(?MODULE, TargetNode, State).

handle_handoff_command(Request = #riak_core_fold_req_v2{foldfun = FoldFun, acc0 = Acc0}, Sender, State = #state{table_name = TableName}) ->
    %%TODO
    default_vnode_behaviour:handle_handoff_command(?MODULE, Request, Sender, State),
    JournalEntryList = gingko_log_utils:read_all_journal_entries(TableName),
    TxIdToJournalEntryListMap =
        general_utils:group_by_map(
            fun(#journal_entry{tx_id = TxId}) ->
                TxId
            end, JournalEntryList),

    TxIdToJournalEntryListList = maps:to_list(TxIdToJournalEntryListMap),
    SortedTxIdToJournalEntryListList =
        lists:sort(
            fun({#tx_id{local_start_time = LocalStartTime1}, _}, {#tx_id{local_start_time = LocalStartTime2}, _}) ->
                LocalStartTime1 < LocalStartTime2
            end, TxIdToJournalEntryListList),
    ApplyFoldFun = fun({TxId, TxJournalEntryList}, AccIn) ->
        FoldFun(TxId, TxJournalEntryList, AccIn)
                   end,
    Result = lists:foldl(ApplyFoldFun, Acc0, SortedTxIdToJournalEntryListList),
    {reply, Result, State};

handle_handoff_command(Request, Sender, State) ->
    default_vnode_behaviour:handle_handoff_command(?MODULE, Request, Sender, State).

handle_handoff_data(BinaryData, State = #state{table_name = TableName}) ->
    default_vnode_behaviour:handle_handoff_data(?MODULE, BinaryData, State),
    %%TODO handoff has some complexity because is messes with the txn_tracking_num system
    {TxId, NewJournalEntryList} = binary_to_term(BinaryData),
    SortedNewJournalEntryList = gingko_utils:sort_journal_entries_of_same_tx(NewJournalEntryList),
    ExistingJournalEntryList = gingko_log_utils:read_journal_entries_with_tx_id(TxId, TableName),
    SortedExistingJournalEntryList = gingko_utils:sort_journal_entries_of_same_tx(ExistingJournalEntryList),
    JournalEntryListToAdd = gingko_utils:remove_existing_journal_entries_handoff(SortedNewJournalEntryList, SortedExistingJournalEntryList),
    NewState =
        lists:foldl(
            fun(JournalEntry, StateAcc) ->
                add_handoff_journal_entry(JournalEntry, StateAcc)
            end, State, JournalEntryListToAdd),
    {reply, ok, NewState}.

encode_handoff_item(Key, Value) -> default_vnode_behaviour:encode_handoff_item(?MODULE, Key, Value).

is_empty(State = #state{table_name = TableName, initialized = Initialized}) ->
    default_vnode_behaviour:is_empty(?MODULE, State),
    %%TODO redo
    case Initialized of
        true ->
            IsEmpty = gingko_log_utils:read_all_journal_entries(TableName) == [],
            {IsEmpty, State};
        false ->
            {true, State}
    end.

terminate(Reason, State) -> default_vnode_behaviour:terminate(?MODULE, Reason, State).
delete(State) -> default_vnode_behaviour:delete(?MODULE, State).
-spec handle_info(term(), state()) -> no_return().
handle_info(Request, State) -> default_vnode_behaviour:handle_info_crash(?MODULE, Request, State).
handle_exit(Pid, Reason, State) -> default_vnode_behaviour:handle_exit(?MODULE, Pid, Reason, State).
handle_coverage(Request, KeySpaces, Sender, State) ->
    default_vnode_behaviour:handle_coverage(?MODULE, Request, KeySpaces, Sender, State).
handle_overload_command(Request, Sender, Partition) ->
    default_vnode_behaviour:handle_overload_command(?MODULE, Request, Sender, Partition).
handle_overload_info(Request, Partition) -> default_vnode_behaviour:handle_overload_info(?MODULE, Request, Partition).

%%%===================================================================
%%% Internal functions
%%%===================================================================

initialize_or_repair(State = #state{table_name = TableName, initialized = Initialized}) ->
    case mnesia:system_info(is_running) of
        yes ->
            %%TODO check that global tables are active
            case Initialized of
                false ->
                    Tables = mnesia:system_info(tables),
                    case lists:member(TableName, Tables) of
                        true ->
                            NodesWhereThisTableExists = mnesia:table_info(TableName, where_to_write),
                            %%TODO we will make sure that if this table exists then it will only run on our node after this call
                            case NodesWhereThisTableExists of
                                [] ->
                                    ok = gingko_log_utils:create_journal_mnesia_table(TableName);
                                Nodes ->
                                    %%TODO make sure we have ram copies and delete tables on other nodes
                                    case lists:member(node(), Nodes) of
                                        true ->
                                            DiscCopiesNodes = mnesia:table_info(TableName, disc_copies),
                                            case lists:member(node(), DiscCopiesNodes) of
                                                true -> ok;
                                                false ->
                                                    {atomic, ok} = mnesia:change_table_copy_type(TableName, node(), disc_copies)
                                            end;

                                        false ->
                                            {atomic, ok} = mnesia:add_table_copy(TableName, node(), disc_copies)
                                    end,
                                    %TODO maybe this will cause problems during handoff
                                    lists:foreach(
                                        fun(Node) ->
                                            case Node /= node() of
                                                true ->
                                                    {atomic, ok} = mnesia:del_table_copy(TableName, Node);
                                                false -> ok
                                            end
                                        end, Nodes)
                            end;
                        false ->
                            ok = gingko_log_utils:create_journal_mnesia_table(TableName)
                    end,
                    ok = mnesia:wait_for_tables([TableName], 5000),
                    {ok, initialize_state(State)};
                true ->
                    %%TODO repair potentially
                    {ok, State}
            end;
        _ -> {{error, mnesia_not_running}, State}
    end.

-spec process_command(Request :: {{journal_entry_type(), term()}, txid()}, state()) -> {ok | {ok, snapshot()} | {error, reason()}, state()}.
process_command({{update, {{KeyStruct, DownstreamOp}, TxOpNum}}, TxId}, State) ->
    {JsnState, NewState} = next_jsn(State),
    {Type, Args} = gingko_journal_utils:create_update_operation(KeyStruct, TxOpNum, DownstreamOp),
    create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),
    {ok, NewState};

process_command({{begin_txn, DependencyVts}, TxId}, State) ->
    {JsnState, NewState} = next_jsn(State),
    {Type, Args} = gingko_journal_utils:create_begin_operation(DependencyVts),
    create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),
    {ok, NewState};

process_command({{prepare_txn, {Partitions, GivenTxOpNumbers}}, TxId}, State = #state{table_name = TableName}) ->
    TxJournalEntryList = gingko_log_utils:read_journal_entries_with_tx_id(TxId, TableName),
    UpdateOperationJournalEntryList = gingko_utils:get_updates(TxJournalEntryList),
    FoundTxOpNumbers =
        lists:map(
            fun(#journal_entry{args = #update_args{tx_op_num = TxOpNum}}) ->
                TxOpNum
            end, UpdateOperationJournalEntryList),
    MatchingTxOpNumbers = general_utils:set_equals_on_lists(GivenTxOpNumbers, FoundTxOpNumbers),
    case MatchingTxOpNumbers of
        true ->
            {JsnState, NewState} = next_jsn(State),
            {Type, Args} = gingko_journal_utils:create_prepare_operation(Partitions),
            create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),
            {ok, NewState};
        false ->
            {{error, {"Inner Prepare Validation failed", TxJournalEntryList, GivenTxOpNumbers, FoundTxOpNumbers}}, State}
    end;

process_command({{commit_txn, CommitVts}, TxId}, State = #state{dcid_to_dc_txn_tracking_state = DcIdToDcTxnTrackingState}) ->
    {JsnState, NewState} = next_jsn(State),
    MyDcId = JsnState#jsn_state.dcid,
    DcTxnTrackingState = maps:get(MyDcId, DcIdToDcTxnTrackingState, #dc_txn_tracking_state{}),
    ValidTxnTrackingNumSet = DcTxnTrackingState#dc_txn_tracking_state.valid_txn_tracking_num_gb_set,
    {LastNum, _, _} =
        case gb_sets:is_empty(ValidTxnTrackingNumSet) of
            true -> gingko_utils:get_default_txn_tracking_num();
            false -> gb_sets:largest(DcTxnTrackingState#dc_txn_tracking_state.valid_txn_tracking_num_gb_set)
        end,
    NewTxnTrackingNum = {LastNum + 1, TxId, vectorclock:get(MyDcId, CommitVts)},
    NewDcTxnTrackingState = DcTxnTrackingState#dc_txn_tracking_state{valid_txn_tracking_num_gb_set = gb_sets:add_element(NewTxnTrackingNum, ValidTxnTrackingNumSet)},
    NewDcIdToDcTxnTrackingState = DcIdToDcTxnTrackingState#{MyDcId => NewDcTxnTrackingState},
    {Type, Args} = gingko_journal_utils:create_commit_operation(CommitVts, NewTxnTrackingNum),
    create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),
    {ok, NewState#state{dcid_to_dc_txn_tracking_state = NewDcIdToDcTxnTrackingState}};

process_command({{abort_txn, none}, TxId}, State) ->
    {JsnState, NewState} = next_jsn(State),
    {Type, Args} = gingko_journal_utils:create_abort_operation(),
    create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),
    {ok, NewState};

process_command({{checkpoint, DependencyVts}, TxId}, State = #state{partition = Partition}) ->
    {BeginJsnState, BeginState} = next_jsn(State),
    NewCheckpointDcIdToLastTxnTrackingNumMap = get_new_checkpoint_dcid_to_txn_tracking_num_map(DependencyVts, BeginState),
    ValidJournalEntryListBeforeCheckpointResult = get_valid_journal_entries(DependencyVts, BeginState),
    case ValidJournalEntryListBeforeCheckpointResult of
        {ok, ValidJournalEntryListBeforeCheckpoint} ->
            {Type, Args} = gingko_journal_utils:create_checkpoint_operation(DependencyVts, NewCheckpointDcIdToLastTxnTrackingNumMap),
            create_and_add_local_journal_entry(BeginJsnState, TxId, Type, Args, BeginState),
            CheckpointResult = gingko_log_utils:checkpoint(ValidJournalEntryListBeforeCheckpoint, DependencyVts, NewCheckpointDcIdToLastTxnTrackingNumMap),
            {ResultJsnState, ResultState} = next_jsn(BeginState),
            case CheckpointResult of
                ok ->
                    create_and_add_local_journal_entry(ResultJsnState, TxId, checkpoint_commit, Args, ResultState),
                    TrimState = perform_journal_log_trimming_and_add_checkpoint_to_state(NewCheckpointDcIdToLastTxnTrackingNumMap, ResultState),
                    gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_CACHE, {checkpoint_cache_cleanup, DependencyVts}),
                    {ok, TrimState};
                Error ->
                    {Error, ResultState}
            end;
        Error -> {Error, BeginState}
    end.
%%Must succeed otherwise crash

-spec create_and_add_local_journal_entry(jsn_state(), txid(), journal_entry_type(), journal_entry_args(), state()) -> journal_entry().
create_and_add_local_journal_entry(JsnState, TxId, Type, Args, #state{partition = Partition, table_name = TableName}) ->
    JournalEntry = gingko_journal_utils:create_local_journal_entry(JsnState, TxId, Type, Args),
    gingko_log_utils:add_journal_entry(JournalEntry, TableName),
    ok = gingko_dc_utils:call_gingko_sync(Partition, ?INTER_DC_LOG, {journal_entry, JournalEntry}),
    JournalEntry.

-spec add_remote_txn([journal_entry()], state()) -> state().
add_remote_txn([], State) -> State;
add_remote_txn(SortedTxJournalEntryList = [BeginJournalEntry | _], State = #state{next_jsn = NextJsn, table_name = TableName}) ->
    CommitJournalEntry = lists:last(SortedTxJournalEntryList),
    case transaction_already_exists(CommitJournalEntry, State) of
        true -> State;
        false ->
            NewNextJsn = NextJsn + length(SortedTxJournalEntryList),
            {_, FixedJournalEntries} =
                lists:foldl(
                    fun(JournalEntry, {CurrentNextJsn, CurrentJournalEntryList}) ->
                        {CurrentNextJsn + 1, [JournalEntry#journal_entry{jsn = CurrentNextJsn} | CurrentJournalEntryList]}
                    end, {NextJsn, []}, SortedTxJournalEntryList),
            %%Keep it sorted
            ReversedFixedJournalEntries = lists:reverse(FixedJournalEntries),
            ok = gingko_log_utils:add_journal_entry_list(ReversedFixedJournalEntries, TableName), %%TODO currently necessary to avoid potential GCSt bug in an extreme edge case
            add_commit(BeginJournalEntry, CommitJournalEntry, State#state{next_jsn = NewNextJsn})
    end.

-spec transaction_already_exists(journal_entry(), state()) -> boolean().
transaction_already_exists(#journal_entry{dcid = DcId, args = #commit_txn_args{txn_tracking_num = CommitTxnTrackingNum = {CommitTxnNum, _, _}}}, #state{dcid_to_dc_txn_tracking_state = DcIdToDcTxnTrackingState}) ->
    #dc_txn_tracking_state{last_checkpoint_txn_tracking_num = {CheckpointTxnNum, _, _}, valid_txn_tracking_num_gb_set = ValidSet, invalid_txn_tracking_num_gb_set = InvalidSet} = maps:get(DcId, DcIdToDcTxnTrackingState, #dc_txn_tracking_state{}),
    CommitTxnNum =< CheckpointTxnNum orelse gb_sets:is_member(CommitTxnTrackingNum, ValidSet) orelse gb_sets:is_member(CommitTxnTrackingNum, InvalidSet).

%%TODO add checks
-spec add_handoff_journal_entry(journal_entry(), state()) -> state().
add_handoff_journal_entry(#journal_entry{tx_id = TxId, type = Type, args = Args}, State) ->
    {JsnState, NewState} = next_jsn(State),
    create_and_add_local_journal_entry(JsnState, TxId, Type, Args, State),
    NewState.

-spec next_jsn(state()) -> {jsn_state(), state()}.
next_jsn(State = #state{next_jsn = NextJsn}) ->
    DcId = gingko_dc_utils:get_my_dcid(),
    JsnState = #jsn_state{next_jsn = NextJsn, dcid = DcId},
    {JsnState, State#state{next_jsn = NextJsn + 1}}.

-spec update_dc_txn_states_and_latest_vts(state()) -> state().
update_dc_txn_states_and_latest_vts(State = #state{partition = Partition, dcid_to_dc_txn_tracking_state = DcIdToDcTxnTrackingState, latest_vts = LatestVts}) ->
    LocalDcId = gingko_dc_utils:get_my_dcid(),
    NewDcIdToDcTxnTrackingState =
        maps:map(
            fun(DcId, DcTxnTrackingState = #dc_txn_tracking_state{last_checkpoint_txn_tracking_num = LastCheckpointTxnTrackingNum, invalid_txn_tracking_num_gb_set = InvalidSet, valid_txn_tracking_num_gb_set = ValidSet}) ->
                case gb_sets:is_empty(InvalidSet) of
                    true -> DcTxnTrackingState;
                    false ->
                        {LastValidTxnNum, _, _} =
                            case gb_sets:is_empty(ValidSet) of
                                true -> LastCheckpointTxnTrackingNum;
                                false -> gb_sets:largest(ValidSet)
                            end,
                        FirstInvalidTxnTrackingNum = {MaybeValidTxnTrackingNum = {FirstInvalidTxnNum, _, _}, BeginVts} = gb_sets:smallest(InvalidSet),
                        case LastValidTxnNum == (FirstInvalidTxnNum - 1) of
                            false -> DcTxnTrackingState;
                            true ->
                                ModifiedBeginVts = vectorclock:set(DcId, 0, BeginVts),
                                case vectorclock:le(ModifiedBeginVts, LatestVts) of
                                    false -> DcTxnTrackingState;
                                    true ->
                                        DcTxnTrackingState#dc_txn_tracking_state{
                                            invalid_txn_tracking_num_gb_set = gb_sets:del_element(FirstInvalidTxnTrackingNum, InvalidSet),
                                            valid_txn_tracking_num_gb_set = gb_sets:add_element(MaybeValidTxnTrackingNum, ValidSet)}
                                end
                        end
                end
            end, DcIdToDcTxnTrackingState),
    NewLatestVts =
        vectorclock:from_list(
            maps:fold(
                fun(DcId, #dc_txn_tracking_state{last_checkpoint_txn_tracking_num = LastCheckpointTxnTrackingNum, valid_txn_tracking_num_gb_set = ValidSet}, CurrentTupleList) ->
                    {_, _, CommitTime} =
                        case gb_sets:is_empty(ValidSet) of
                            true -> LastCheckpointTxnTrackingNum;
                            false -> gb_sets:largest(ValidSet)
                        end,
                    [{DcId, CommitTime} | CurrentTupleList]
                end, [], NewDcIdToDcTxnTrackingState)),
    MyDcTime = vectorclock:get(LocalDcId, NewLatestVts),
    CompareLatestVts = vectorclock:set(LocalDcId, MyDcTime, LatestVts),
    case vectorclock:eq(NewLatestVts, CompareLatestVts) of
        true ->
            FinalLatestVts = vectorclock:set(LocalDcId, gingko_dc_utils:get_timestamp(), NewLatestVts),
            gingko_dc_utils:update_partition_commit_vts(Partition, FinalLatestVts),
            State#state{latest_vts = FinalLatestVts}; %%TODO assumption that NewDcToLastValidTxnTrackingNum did not change from the old one
        false ->
            update_dc_txn_states_and_latest_vts(State#state{dcid_to_dc_txn_tracking_state = NewDcIdToDcTxnTrackingState, latest_vts = NewLatestVts})
    end.

%%TODO checkpoint recovery
-spec initialize_state(state()) -> state().
initialize_state(State = #state{partition = Partition, table_name = TableName}) ->
    MyDcId = gingko_dc_utils:get_my_dcid(),
    JournalEntryList = gingko_log_utils:remove_unresolved_or_aborted_transactions_and_return_remaining_journal_entry_list(TableName),
    NextJsn =
        case JournalEntryList of
            [] -> 1;
            _ ->
                #journal_entry{jsn = HighestJsn} = general_utils:max_by(fun(#journal_entry{jsn = Jsn}) ->
                    Jsn end, JournalEntryList),
                HighestJsn + 1
        end,
    DcIdToJournalEntryListMap = general_utils:group_by_map(fun(#journal_entry{dcid = DcId}) ->
        DcId end, JournalEntryList),
    LatestCheckpoints = gingko_utils:get_journal_entries_of_type(JournalEntryList, checkpoint_commit),
    SortedCheckpoints =
        lists:sort(
            fun(#journal_entry{args = #checkpoint_args{dependency_vts = DependencyVts1}}, #journal_entry{args = #checkpoint_args{dependency_vts = DependencyVts2}}) ->
                vectorclock:ge(DependencyVts1, DependencyVts2)
            end, LatestCheckpoints), %%Reverse sorting so the newest checkpoint is first
    DcIdToLastTxnTrackingNumMap =
        case SortedCheckpoints of
            [#journal_entry{args = #checkpoint_args{dcid_to_last_txn_tracking_num = DcIdToLastTxnTrackingNum}} | _] ->
                DcIdToLastTxnTrackingNum;
            [] ->
                maps:map(fun(_, _) -> gingko_utils:get_default_txn_tracking_num() end, DcIdToJournalEntryListMap)
        end,
    InitialDcIdToDcTxnTrackingStateMap =
        maps:map(
            fun(_DcId, LastTxnTrackingNum) ->
                #dc_txn_tracking_state{last_checkpoint_txn_tracking_num = LastTxnTrackingNum}
            end, DcIdToLastTxnTrackingNumMap),
    DcIdToInvalidTxnTrackingNumSetMap =
        maps:map(
            fun(DcId, DcJournalEntryList) ->
                BeginCommitJournalEntryList =
                    lists:filtermap(
                        fun(JList) ->
                            case JList of
                                [J1 = #journal_entry{type = Type}, J2] ->
                                    {true, case Type of
                                               begin_txn -> {J1, J2};
                                               commit_txn -> {J2, J1}
                                           end};
                                _ -> false
                            end
                        end,
                        general_utils:get_values(
                            general_utils:group_by_map(
                                fun(#journal_entry{tx_id = TxId}) ->
                                    TxId
                                end, gingko_utils:get_journal_entries_of_type(DcJournalEntryList, [begin_txn, commit_txn])))),
                LastTxnTrackingNum = maps:get(DcId, DcIdToLastTxnTrackingNumMap),
                gb_sets:from_list(
                    lists:filtermap(
                        fun({#journal_entry{args = #begin_txn_args{dependency_vts = DependencyVts}}, #journal_entry{args = #commit_txn_args{txn_tracking_num = DcTxnTrackingNum}}}) ->
                            case DcTxnTrackingNum > LastTxnTrackingNum of
                                true -> {true, {DcTxnTrackingNum, DependencyVts}};
                                false -> false
                            end
                        end, BeginCommitJournalEntryList))
            end, DcIdToJournalEntryListMap),
    NewDcIdToDcTxnTrackingStateMap =
        maps:map(
            fun(DcId, DcTxnTrackingState) ->
                InvalidTxnTrackingNumSet = maps:get(DcId, DcIdToInvalidTxnTrackingNumSetMap),
                case DcId == MyDcId of
                    true ->
                        DcTxnTrackingState#dc_txn_tracking_state{
                            valid_txn_tracking_num_gb_set =
                            gb_sets:fold(
                                fun({TxnTrackingNum, _}, CurrentValidTxnTrackingNumSet) ->
                                    gb_sets:add_element(TxnTrackingNum, CurrentValidTxnTrackingNumSet)
                                end, gb_sets:new(), InvalidTxnTrackingNumSet)};
                    false ->
                        DcTxnTrackingState#dc_txn_tracking_state{invalid_txn_tracking_num_gb_set = InvalidTxnTrackingNumSet}
                end
            end, InitialDcIdToDcTxnTrackingStateMap),
    DefaultItem = {gingko_utils:get_default_txn_tracking_num(), vectorclock:new()},
    DefaultInvalidTxnTrackingNumSet = gb_sets:add(DefaultItem, gb_sets:empty()),
    LocalInvalidTxnTrackingNumSet = maps:get(MyDcId, DcIdToInvalidTxnTrackingNumSetMap, DefaultInvalidTxnTrackingNumSet),
    {_, LatestLocalVts} =
        case gb_sets:is_empty(LocalInvalidTxnTrackingNumSet) of
            true ->
                DefaultItem;
            false ->
                gb_sets:largest(LocalInvalidTxnTrackingNumSet)
        end,
    logger:error("VTS: ~p", [LatestLocalVts]),
    gingko_dc_utils:update_partition_commit_vts(Partition, LatestLocalVts),
    NewState = State#state{latest_vts = LatestLocalVts, next_jsn = NextJsn, dcid_to_dc_txn_tracking_state = NewDcIdToDcTxnTrackingStateMap, initialized = true},
    update_dc_txn_states_and_latest_vts(NewState).

-spec get_new_checkpoint_dcid_to_txn_tracking_num_map(vectorclock(), state()) -> #{dcid() => txn_tracking_num()}.
get_new_checkpoint_dcid_to_txn_tracking_num_map(DependencyVts, #state{dcid_to_dc_txn_tracking_state = DcIdToDcTxnTrackingState}) ->
    maps:map(
        fun(DcId, #dc_txn_tracking_state{last_checkpoint_txn_tracking_num = LastCheckpointTxnTrackingNum, valid_txn_tracking_num_gb_set = ValidTxnTrackingNumOrdSet}) ->
            DcTimestamp = vectorclock:get(DcId, DependencyVts),
            find_checkpoint_txn_tracking_num(LastCheckpointTxnTrackingNum, DcTimestamp, gb_sets:to_list(ValidTxnTrackingNumOrdSet))
        end, DcIdToDcTxnTrackingState).

-spec find_checkpoint_txn_tracking_num(txn_tracking_num(), clock_time(), [txn_tracking_num()]) -> txn_tracking_num().
find_checkpoint_txn_tracking_num(LastValidTxnTrackingNum, _, []) -> LastValidTxnTrackingNum;
find_checkpoint_txn_tracking_num(LastValidTxnTrackingNum, DcTimestamp, [{_, _, Timestamp} | _]) when DcTimestamp < Timestamp ->
    LastValidTxnTrackingNum;
find_checkpoint_txn_tracking_num(_, DcTimestamp, [DcTxnTrackingNum | OtherDcTxnTrackingNum]) ->
    find_checkpoint_txn_tracking_num(DcTxnTrackingNum, DcTimestamp, OtherDcTxnTrackingNum). %Previous pattern assures DcTimestamp > Timestamp

-spec perform_journal_log_trimming_and_add_checkpoint_to_state(#{dcid() => txn_tracking_num()}, state()) -> state().
perform_journal_log_trimming_and_add_checkpoint_to_state(DcIdToLastTxnTrackingNumMap, State = #state{table_name = TableName, dcid_to_dc_txn_tracking_state = DcIdToDcTxnTrackingState}) ->
    ok =
        case gingko_env_utils:get_journal_trimming_mode() of
            keep_all_checkpoints -> ok;
            _ -> gingko_log_utils:perform_journal_log_trimming(TableName)
        end,
    NewDcIdToDcTxnTrackingState =
        maps:map(
            fun(DcId, DcTxnTrackingState = #dc_txn_tracking_state{valid_txn_tracking_num_gb_set = ValidTxnTrackingNumSet}) ->
                DcLastTxnTrackingNum = maps:get(DcId, DcIdToLastTxnTrackingNumMap, gingko_utils:get_default_txn_tracking_num()), %%TODO check this
                NewValidTxnTrackingNumSet = gb_sets:filter(fun(TxnTrackingNum) ->
                    DcLastTxnTrackingNum < TxnTrackingNum end, ValidTxnTrackingNumSet),
                DcTxnTrackingState#dc_txn_tracking_state{last_checkpoint_txn_tracking_num = DcLastTxnTrackingNum, valid_txn_tracking_num_gb_set = NewValidTxnTrackingNumSet}
            end, DcIdToDcTxnTrackingState),
    State#state{dcid_to_dc_txn_tracking_state = NewDcIdToDcTxnTrackingState}.

-spec add_commit(journal_entry(), journal_entry(), state()) -> state().
add_commit(#journal_entry{dcid = DcId, args = #begin_txn_args{dependency_vts = DependencyVts}}, #journal_entry{dcid = DcId, args = #commit_txn_args{txn_tracking_num = TxnTrackingNum}}, State = #state{dcid_to_dc_txn_tracking_state = DcIdToDcTxnTrackingState}) ->
    DcTxnTrackingStateResult = maps:find(DcId, DcIdToDcTxnTrackingState),
    NewDcTxnTrackingState =
        case DcTxnTrackingStateResult of
            {ok, DcTxnTrackingState = #dc_txn_tracking_state{invalid_txn_tracking_num_gb_set = InvalidTxnTrackingNumSet}} ->
                DcTxnTrackingState#dc_txn_tracking_state{invalid_txn_tracking_num_gb_set = gb_sets:add_element({TxnTrackingNum, DependencyVts}, InvalidTxnTrackingNumSet)};
            error ->
                #dc_txn_tracking_state{invalid_txn_tracking_num_gb_set = gb_sets:add_element({TxnTrackingNum, DependencyVts}, gb_sets:new())}
        end,
    update_dc_txn_states_and_latest_vts(State#state{dcid_to_dc_txn_tracking_state = DcIdToDcTxnTrackingState#{DcId => NewDcTxnTrackingState}}).

-spec get_valid_journal_entries(vectorclock(), state()) -> {ok, [journal_entry()]} | {error, reason()}.
get_valid_journal_entries(DependencyVts, #state{table_name = TableName, dcid_to_dc_txn_tracking_state = DcIdToDcTxnTrackingState, latest_vts = LatestVts}) ->
    case vectorclock:le(DependencyVts, LatestVts) of
        true ->
            ValidTxIdList =
                lists:append(
                    maps:fold(
                        fun(DcId, #dc_txn_tracking_state{valid_txn_tracking_num_gb_set = ValidTxnTrackingNumSet}, ValidTxIdListListAcc) ->
                            DcIdValidTxIdList =
                                case vectorclock:get(DcId, DependencyVts) of
                                    0 -> [];
                                    Timestamp ->
                                        gb_sets:fold(
                                            fun({_, TxId, DcTimestamp}, ValidTxIdListAcc) ->
                                                case DcTimestamp =< Timestamp of
                                                    true -> [TxId | ValidTxIdListAcc];
                                                    false -> ValidTxIdListAcc
                                                end
                                            end, [], ValidTxnTrackingNumSet)
                                end,
                            [DcIdValidTxIdList | ValidTxIdListListAcc]
                        end, [], DcIdToDcTxnTrackingState)),
            {ok, gingko_log_utils:read_journal_entries_with_multiple_tx_ids(ValidTxIdList, TableName)};
        false ->
            {error, "Partition Vts older than Dependency Vts"} %%We cannot allow this otherwise the reads may be inconsistent
    end.
