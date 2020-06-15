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

-record(dc_txn_state, {
    last_checkpoint_txn_num = gingko_utils:get_default_txn_num() :: txn_num(),
    valid_txn_num_ordset = [] :: ordsets:ordset(txn_num()),
    all_txn_num_ordset = [] :: ordsets:ordset(txn_num())
}).
-type dc_txn_state() :: dc_txn_state().

-record(state, {
    partition = 0 :: partition_id(),
    table_name :: atom(),
    latest_vts = vectorclock:new() :: vectorclock(),
    next_jsn = 0 :: non_neg_integer(),
    dcid_to_dc_txn_state = dict:new() :: dict:dict(dcid(), dc_txn_state()), %%TODO truncate lists
    %%TODO txn num to txid for other dc requests
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
    GingkoConfig = [{partition, Partition}, {table_name, TableName} | gingko_app:get_default_config()],
    NewState = apply_gingko_config(#state{}, GingkoConfig),
    {ok, NewState}.

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
    Reply = case Initialized of
                false -> {error, not_initialized};
                true -> {ok, vectorclock:set(gingko_utils:get_my_dcid(), gingko_utils:get_timestamp(), LatestVts)}
            end,
    {reply, Reply, State};

%%TODO consider timing issues during start-up with large tables
%%TODO failure testing required
%%TODO maybe consider migration for later
%%TODO this needs to reexecutable
handle_command(Request = initialize, Sender, State = #state{table_name = TableName, initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    NewState =
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
                                        RamCopiesNodes = mnesia:table_info(TableName, ram_copies),
                                        case lists:member(node(), RamCopiesNodes) of
                                            true -> ok;
                                            false ->
                                                {atomic, ok} = mnesia:change_table_copy_type(TableName, node(), ram_copies)
                                        end;

                                    false ->
                                        {atomic, ok} = mnesia:add_table_copy(TableName, node(), ram_copies)
                                end,
                                %TODO maybe this will cause problems during handoff
                                lists:foreach(
                                    fun(Node) ->
                                        case Node /= node() of
                                            true -> {atomic, ok} = mnesia:del_table_copy(TableName, Node);
                                            false -> ok
                                        end
                                    end, Nodes)
                        end;
                    false ->
                        ok = gingko_log_utils:create_journal_mnesia_table(TableName),
                        ok = mnesia:wait_for_tables([TableName], 5000)
                end,
                initialize_state(State);
            true -> State
        end,
    {reply, ok, NewState};

handle_command(Request = {get_journal_entries, DcIdOrAll, FilterFuncOrNone}, Sender, State = #state{table_name = TableName, initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    Reply =
        case Initialized of
            false -> {error, not_initialized};
            true ->
                JournalEntryList = gingko_log_utils:read_all_journal_entries(TableName),
                SortedJournalEntryList = gingko_utils:sort_by_jsn_number(JournalEntryList),
                case DcIdOrAll of
                    all -> {ok, SortedJournalEntryList};
                    DcId ->
                        Result = lists:filter(
                            fun(JournalEntry = #journal_entry{dcid = DCID}) ->
                                DcId == DCID andalso
                                    case FilterFuncOrNone of
                                        none -> true;
                                        FilterFunc -> FilterFunc(JournalEntry)
                                    end
                            end, SortedJournalEntryList),
                        {ok, Result}
                end
        end,
    {reply, Reply, State};

handle_command(Request = {get_valid_journal_entries, DependencyVts}, Sender, State = #state{initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    Reply =
        case Initialized of
            false -> {error, not_initialized};
            true -> {ok, get_valid_journal_entries(DependencyVts, State)}
        end,
    {reply, Reply, State};

handle_command(Request = {add_remote_txn, SortedTxJournalEntryList}, Sender, State = #state{initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {Reply, NewState} = case Initialized of
                            false -> {{error, not_initialized}, State};
                            true -> {ok, add_remote_txn(SortedTxJournalEntryList, State)}
                        end,
    {reply, Reply, NewState};

handle_command(Request = {{_Op, _Args}, {_TxId, _TxArgs}}, Sender, State = #state{initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    case Initialized of
        false -> {reply, {error, not_initialized}, State};
        true -> process_command(Request, State)
    end;

handle_command(Request = {{_Op, _Args}, _TxIdWithPossibleExtraArgs}, Sender, State = #state{initialized = Initialized}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    case Initialized of
        false -> {reply, {error, not_initialized}, State};
        true -> process_command(Request, State)
    end;

handle_command(Request, Sender, State) -> default_vnode_behaviour:handle_command_crash(?MODULE, Request, Sender, State).
handoff_starting(TargetNode, State) -> default_vnode_behaviour:handoff_starting(?MODULE, TargetNode, State).
handoff_cancelled(State) -> default_vnode_behaviour:handoff_cancelled(?MODULE, State).
handoff_finished(TargetNode, State) -> default_vnode_behaviour:handoff_finished(?MODULE, TargetNode, State).

handle_handoff_command(Request = #riak_core_fold_req_v2{foldfun = FoldFun, acc0 = Acc0}, Sender, State = #state{table_name = TableName}) ->
    %%TODO
    default_vnode_behaviour:handle_handoff_command(?MODULE, Request, Sender, State),
    JournalEntryList = gingko_log_utils:read_all_journal_entries(TableName),
    TxIdToJournalEntryListDict =
        general_utils:group_by(
            fun(#journal_entry{tx_id = TxId}) ->
                TxId
            end, JournalEntryList),

    TxIdToJournalEntryListList = dict:to_list(TxIdToJournalEntryListDict),
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
    %%TODO handoff has some complexity because is messes with the txn_num system
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

-spec apply_gingko_config(state(), [{atom(), term()}]) -> state().
apply_gingko_config(State, GingkoConfig) ->
    Partition = general_utils:get_or_default_map_list(partition, GingkoConfig, error),
    TableName = general_utils:get_or_default_map_list(table_name, GingkoConfig, error),
    State#state{partition = Partition, table_name = TableName}.

-spec process_command(Request :: term(), state()) -> {reply, ok | {ok, snapshot_value()} | {error, reason()}, state()}.
process_command({{read, KeyStruct}, {TxId, TxOpNumber}}, State = #state{table_name = TableName}) ->
    {JsnState, NewState} = next_jsn(State),
    {Type, Args} = gingko_journal_utils:create_read_operation(KeyStruct, TxOpNumber),
    create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),
    {reply, TableName, NewState};

process_command({{update, {KeyStruct, TypeOp}}, {TxId, TxOpNumber}}, State = #state{table_name = TableName}) ->
    DownstreamOpResult = gingko_utils:generate_downstream_op(KeyStruct, TxId, TypeOp, TableName),
    case DownstreamOpResult of
        {error, Reason} -> {reply, {error, Reason}, State};
        {ok, DownstreamOp} ->
            {JsnState, NewState} = next_jsn(State),
            {Type, Args} = gingko_journal_utils:create_update_operation(KeyStruct, TxOpNumber, DownstreamOp),
            create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),%TODO error check
            {reply, ok, NewState}
    end;

process_command({{begin_txn, DependencyVts}, TxId}, State) ->
    {JsnState, NewState} = next_jsn(State),
    {Type, Args} = gingko_journal_utils:create_begin_operation(DependencyVts),
    create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),%TODO error check
    {reply, ok, NewState};

process_command({{prepare_txn, none}, {TxId, GivenTxOpNumbers}}, State = #state{table_name = TableName}) ->
    TxJournalEntryList = gingko_log_utils:read_journal_entries_with_tx_id(TxId, TableName),
    ObjectOperationJournalEntryList = gingko_utils:get_object_operations(TxJournalEntryList),
    FoundTxOpNumbers =
        lists:map(
            fun(#journal_entry{args = #object_op_args{tx_op_num = TxOpNum}}) ->
                TxOpNum
            end, ObjectOperationJournalEntryList),
    MatchingTxOpNumbers = general_utils:set_equals_on_lists(GivenTxOpNumbers, FoundTxOpNumbers),
    case MatchingTxOpNumbers of
        true ->
            {JsnState, NewState} = next_jsn(State),
            {Type, Args} = gingko_journal_utils:create_prepare_operation(),
            create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),
            ok = gingko_log_utils:persist_journal_entries(TableName),%TODO error check
            {reply, ok, NewState};
        false ->
            {reply, {error, "Validation failed"}, State}
    end;

process_command({{commit_txn, CommitVts}, TxId}, State = #state{dcid_to_dc_txn_state = DcIdToDcTxnState}) ->
    {JsnState, NewState} = next_jsn(State),
    MyDcid = JsnState#jsn_state.dcid,
    DcTxnState = general_utils:get_or_default_dict(MyDcid, DcIdToDcTxnState, #dc_txn_state{}),
    {NextCommittedTxnNum, _, _} =
        case DcTxnState#dc_txn_state.all_txn_num_ordset of
            [] -> gingko_utils:get_default_txn_num();
            List -> lists:last(List)
        end,

    {Type, Args} = gingko_journal_utils:create_commit_operation(CommitVts, {NextCommittedTxnNum + 1, TxId, vectorclock:get(MyDcid, CommitVts)}),
    CommitJournalEntry = create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),%TODO error check
    {reply, ok, add_commit(CommitJournalEntry, NewState)};

process_command({{abort_txn, none}, TxId}, State) ->
    {JsnState, NewState} = next_jsn(State),
    {Type, Args} = gingko_journal_utils:create_abort_operation(),
    create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),%TODO error check
    {reply, ok, NewState};

process_command({{checkpoint, DependencyVts}, TxId}, State = #state{table_name = TableName}) ->
    {JsnState, NewState} = next_jsn(State),
    CheckpointDict = get_checkpoint_dict(DependencyVts, State), %%TODO rename
    ValidJournalEntriesBeforeCheckpoint = get_valid_journal_entries(DependencyVts, State),
    {Type, Args} = gingko_journal_utils:create_checkpoint_operation(DependencyVts, CheckpointDict),
    create_and_add_local_journal_entry(JsnState, TxId, Type, Args, NewState),%TODO error check
    ok = gingko_log_utils:persist_journal_entries(TableName),%TODO error check
    FinalState = checkpoint_state(CheckpointDict, NewState),
    {reply, {ValidJournalEntriesBeforeCheckpoint, CheckpointDict}, FinalState}.

-spec create_and_add_local_journal_entry(jsn_state(), txid(), journal_entry_type(), journal_entry_args(), state()) -> journal_entry().
create_and_add_local_journal_entry(JsnState, TxId, Type, Args, #state{partition = Partition, table_name = TableName}) ->
    JournalEntry = gingko_journal_utils:create_local_journal_entry(JsnState, TxId, Type, Args),
    %%TODO if add_journal_entry fails then we should crash because this non recoverable
    ok = gingko_log_utils:add_journal_entry(JournalEntry, TableName),
    gingko_utils:call_gingko_async(Partition, ?INTER_DC_LOG, {journal_entry, JournalEntry}),
    JournalEntry.

-spec add_remote_txn([journal_entry()], state()) -> state().
add_remote_txn(SortedTxJournalEntryList, State) ->
    lists:foldl(
        fun(JournalEntry, StateAcc) ->
            add_remote_journal_entry(JournalEntry, StateAcc)
        end, State, SortedTxJournalEntryList).

-spec add_remote_journal_entry(journal_entry(), state()) -> state().
add_remote_journal_entry(JournalEntry = #journal_entry{type = Type}, State = #state{next_jsn = NextJsn, table_name = TableName}) ->
    ok = gingko_log_utils:add_journal_entry(JournalEntry#journal_entry{jsn = NextJsn}, TableName),
    NewState = State#state{next_jsn = NextJsn + 1},
    case Type == commit_txn of
        true -> add_commit(JournalEntry, NewState);
        false -> NewState
    end.

%%TODO add checks
-spec add_handoff_journal_entry(journal_entry(), state()) -> state().
add_handoff_journal_entry(#journal_entry{tx_id = TxId, type = Type, args = Args}, State) ->
    {JsnState, NewState} = next_jsn(State),
    create_and_add_local_journal_entry(JsnState, TxId, Type, Args, State),
    NewState.

-spec next_jsn(state()) -> {jsn_state(), state()}.
next_jsn(State = #state{next_jsn = NextJsn, latest_vts = LatestVts}) ->
    DcId = gingko_utils:get_my_dcid(),
    RtTimestamp = gingko_utils:get_timestamp(),
    NewLatestVts = vectorclock:set(DcId, RtTimestamp, LatestVts),
    JsnState = #jsn_state{next_jsn = NextJsn, dcid = DcId, rt_timestamp = RtTimestamp},
    {JsnState, State#state{latest_vts = NewLatestVts, next_jsn = NextJsn + 1}}.

-spec initialize_state(state()) -> state().
initialize_state(State = #state{table_name = TableName}) ->
    JournalEntryList = gingko_log_utils:read_all_journal_entries(TableName),
    NextJsn =
        case JournalEntryList of
            [] -> 1;
            _ ->
                #journal_entry{jsn = HighestJsn} = general_utils:max_by(fun(#journal_entry{jsn = Jsn}) ->
                    Jsn end, JournalEntryList),
                HighestJsn + 1
        end,
    DcIdToJournalEntryListDict = general_utils:group_by(fun(#journal_entry{dcid = DcId}) -> DcId end, JournalEntryList),
    LatestCheckpoints = gingko_utils:get_journal_entries_of_type(JournalEntryList, checkpoint),
    SortedCheckpoints =
        lists:sort(
            fun(#journal_entry{args = #checkpoint_args{dependency_vts = DependencyVts1}}, #journal_entry{args = #checkpoint_args{dependency_vts = DependencyVts2}}) ->
                vectorclock:ge(DependencyVts1, DependencyVts2)
            end, LatestCheckpoints), %%Reverse sorting so the newest checkpoint is first
    DcIdToLastTxnNumDict =
        case SortedCheckpoints of
            [#journal_entry{args = #checkpoint_args{dcid_to_last_txn_num = DcIdToLastTxnNum}} | _] ->
                DcIdToLastTxnNum;
            [] ->
                dict:map(fun(_DcId, _) -> gingko_utils:get_default_txn_num() end, DcIdToJournalEntryListDict)
        end,
    InitialDcIdToDcTxnStateDict =
        dict:map(
            fun(_DcId, LastTxnNum) ->
                #dc_txn_state{last_checkpoint_txn_num = LastTxnNum}
            end, DcIdToLastTxnNumDict),
    DcIdToAllTxnNumOrdSetDict =
        dict:map(
            fun(DcId, DcJournalEntryList) ->
                DcCommitJournalEntryList = gingko_utils:get_journal_entries_of_type(DcJournalEntryList, commit_txn),
                LastTxnNum = dict:fetch(DcId, DcIdToLastTxnNumDict),
                ordsets:from_list(
                    lists:filtermap(
                        fun(#journal_entry{args = #commit_txn_args{local_txn_num = DcTxnNum}}) ->
                            case DcTxnNum > LastTxnNum of
                                true -> {true, DcTxnNum};
                                false -> false
                            end
                        end, DcCommitJournalEntryList))
            end, DcIdToJournalEntryListDict),
    FinalDcIdToDcTxnStateDict =
        dict:map(
            fun(DcId, DcTxnState = #dc_txn_state{last_checkpoint_txn_num = LastCheckpointTxnNum}) ->
                AllDcTxnOrdSet = dict:fetch(DcId, DcIdToAllTxnNumOrdSetDict),
                ValidDcTxnNumOrdSet = get_valid_txn_num(LastCheckpointTxnNum, AllDcTxnOrdSet),
                DcTxnState#dc_txn_state{valid_txn_num_ordset = ValidDcTxnNumOrdSet, all_txn_num_ordset = AllDcTxnOrdSet}
            end, InitialDcIdToDcTxnStateDict),
    DcIdToHighestTimestampDict =
        dict:map(
            fun(DcId, DcJournalEntryList) ->
                #dc_txn_state{valid_txn_num_ordset = ValidTxnNumOrdSet} = dict:fetch(DcId, FinalDcIdToDcTxnStateDict),
                DcCommitJournalEntryList = gingko_utils:get_journal_entries_of_type(DcJournalEntryList, commit_txn),
                case DcCommitJournalEntryList of
                    [] -> 0;
                    Commits ->
                        FilteredCommits = filter_valid_commits(ValidTxnNumOrdSet, Commits),
                        find_highest_commit_vts(DcId, FilteredCommits)
                end
            end, DcIdToJournalEntryListDict),
    LatestVts = vectorclock:from_list(dict:to_list(DcIdToHighestTimestampDict)),

    State#state{latest_vts = LatestVts, next_jsn = NextJsn, dcid_to_dc_txn_state = FinalDcIdToDcTxnStateDict, initialized = true}.

-spec filter_valid_commits([txn_num()], [journal_entry()]) -> [journal_entry()].
filter_valid_commits(ValidTxnNums, Commits) ->
    lists:filter(
        fun(#journal_entry{args = #commit_txn_args{local_txn_num = LocalTxnNum}}) ->
            ordsets:is_element(LocalTxnNum, ValidTxnNums)
        end, Commits).

-spec find_highest_commit_vts(dcid(), [journal_entry()]) -> non_neg_integer().
find_highest_commit_vts(_DcId, []) -> 0;
find_highest_commit_vts(DcId, [#journal_entry{args = #commit_txn_args{commit_vts = CommitVts}}]) ->
    vectorclock:get(DcId, CommitVts);
find_highest_commit_vts(DcId, [Commit1 = #journal_entry{args = #commit_txn_args{commit_vts = CommitVts1}}, Commit2 = #journal_entry{args = #commit_txn_args{commit_vts = CommitVts2}} | Other]) ->
    DcIdTimestamp1 = vectorclock:get(DcId, CommitVts1),
    DcIdTimestamp2 = vectorclock:get(DcId, CommitVts2),
    case DcIdTimestamp1 < DcIdTimestamp2 of
        true -> find_highest_commit_vts(DcId, [Commit2 | Other]);
        false -> find_highest_commit_vts(DcId, [Commit1 | Other])
    end.

-spec get_checkpoint_dict(vectorclock(), state()) -> dict:dict(dcid(), txn_num()).
get_checkpoint_dict(DependencyVts, #state{dcid_to_dc_txn_state = DcIdToDcTxnState}) ->
    dict:map(
        fun(DcId, #dc_txn_state{last_checkpoint_txn_num = LastCheckpointTxnNum, valid_txn_num_ordset = ValidTxnNumOrdSet}) ->
            DcTimestamp = vectorclock:get(DcId, DependencyVts),
            find_checkpoint_txn_num(LastCheckpointTxnNum, DcTimestamp, ValidTxnNumOrdSet)
        end, DcIdToDcTxnState).

-spec find_checkpoint_txn_num(txn_num(), clock_time(), [txn_num()]) -> txn_num().
find_checkpoint_txn_num(LastValidTxnNum, _, []) -> LastValidTxnNum;
find_checkpoint_txn_num(LastValidTxnNum, DcTimestamp, [{_, _, Timestamp} | _]) when DcTimestamp < Timestamp ->
    LastValidTxnNum;
find_checkpoint_txn_num(_, DcTimestamp, [DcTxnNum | OtherDcTxnNum]) ->
    find_checkpoint_txn_num(DcTxnNum, DcTimestamp, OtherDcTxnNum). %Previous pattern assures DcTimestamp > Timestamp

-spec checkpoint_state(dict:dict(dcid(), txn_num()), state()) -> state().
checkpoint_state(DcIdToLastTxnNumDict, State = #state{dcid_to_dc_txn_state = DcIdToDcTxnState}) ->
    NewDcIdToDcTxnState =
        dict:map(
            fun(DcId, DcTxnState = #dc_txn_state{all_txn_num_ordset = AllTxnNumOrdSet}) ->
                DcLastTxnNum = general_utils:get_or_default_dict(DcId, DcIdToLastTxnNumDict, gingko_utils:get_default_txn_num()), %%TODO check this
                NewAllTxnNumOrdSet = ordsets:filter(fun(TxnNum) -> DcLastTxnNum < TxnNum end, AllTxnNumOrdSet),
                NewValidTxnNumOrdSet = get_valid_txn_num(DcLastTxnNum, NewAllTxnNumOrdSet),
                DcTxnState#dc_txn_state{last_checkpoint_txn_num = DcLastTxnNum, valid_txn_num_ordset = NewValidTxnNumOrdSet, all_txn_num_ordset = NewAllTxnNumOrdSet}
            end, DcIdToDcTxnState),
    State#state{dcid_to_dc_txn_state = NewDcIdToDcTxnState}.

-spec add_commit(journal_entry(), state()) -> state().
add_commit(#journal_entry{dcid = DcId, args = #commit_txn_args{local_txn_num = LocalTxnNum}}, State = #state{latest_vts = LatestVts, dcid_to_dc_txn_state = DcIdToDcTxnState}) ->
    DcTxnStateResult = dict:find(DcId, DcIdToDcTxnState),
    {NewDcTxnState, NewLatestVts} =
        case DcTxnStateResult of
            {ok, DcTxnState = #dc_txn_state{last_checkpoint_txn_num = LastDcTxnNum, all_txn_num_ordset = AllTxnNumOrdSet}} ->
                NewAllTxnNumOrdSet1 = ordsets:add_element(LocalTxnNum, AllTxnNumOrdSet),
                NewValidTxnNumOrdSet1 = get_valid_txn_num(LastDcTxnNum, NewAllTxnNumOrdSet1),
                DcNewLatestVts1 =
                    case NewValidTxnNumOrdSet1 of
                        [] -> LatestVts;
                        _ ->
                            {_, _, DcTimestamp} = lists:last(NewValidTxnNumOrdSet1),
                            vectorclock:set(DcId, DcTimestamp, LatestVts)
                    end,
                {DcTxnState#dc_txn_state{all_txn_num_ordset = NewAllTxnNumOrdSet1,
                    valid_txn_num_ordset = NewValidTxnNumOrdSet1}, DcNewLatestVts1};
            error ->
                NewAllTxnNumOrdSet2 = [LocalTxnNum],
                NewValidTxnNumOrdSet2 = get_valid_txn_num(gingko_utils:get_default_txn_num(), NewAllTxnNumOrdSet2),
                DcNewLatestVts2 =
                    case NewValidTxnNumOrdSet2 of
                        [] -> LatestVts;
                        _ ->
                            {_, _, DcTimestamp} = lists:last(NewValidTxnNumOrdSet2),
                            vectorclock:set(DcId, DcTimestamp, LatestVts)
                    end,
                {#dc_txn_state{all_txn_num_ordset = [LocalTxnNum], valid_txn_num_ordset = get_valid_txn_num(gingko_utils:get_default_txn_num(), [LocalTxnNum])}, DcNewLatestVts2}
        end,
    State#state{latest_vts = NewLatestVts, dcid_to_dc_txn_state = dict:store(DcId, NewDcTxnState, DcIdToDcTxnState)}.

-spec get_valid_txn_num(txn_num(), ordsets:ordset(txn_num())) -> ordsets:ordset(txn_num()).
get_valid_txn_num({PreviousNum, _, _}, [First = {FirstNum, _, _} | OtherTxnNum]) when (PreviousNum + 1) == FirstNum ->
    [First | get_valid_txn_num(First, OtherTxnNum)];
get_valid_txn_num(_, _) -> [].

-spec get_valid_journal_entries(vectorclock(), state()) -> [journal_entry()].
get_valid_journal_entries(DependencyVts, #state{table_name = TableName, dcid_to_dc_txn_state = DcIdToDcTxnState}) ->
    ValidTxIdList =
        dict:fold(
            fun(DcId, #dc_txn_state{valid_txn_num_ordset = ValidTxnNumOrdSet}, ListAcc) ->
                lists:filtermap(
                    fun({_, TxId, DcTimestamp}) ->
                        case vectorclock:get(DcId, DependencyVts) of
                            0 -> false;
                            Timestamp ->
                                case DcTimestamp =< Timestamp of
                                    true -> {true, TxId};
                                    false -> false
                                end
                        end
                    end, ValidTxnNumOrdSet) ++ ListAcc
            end, [], DcIdToDcTxnState),
    gingko_log_utils:read_journal_entries_with_multiple_tx_ids(ValidTxIdList, TableName).







