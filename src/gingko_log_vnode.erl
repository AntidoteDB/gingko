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
-include_lib("riak_core/include/riak_core_vnode.hrl").
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

-export([process_command/4,
    perform_tx_read/3]).

-record(state, {
    partition :: partition_id(),
    table_name :: atom(),
    latest_vts = vectorclock:new() :: vectorclock(),
    next_jsn = 0 :: non_neg_integer(),
    dc_to_next_jsn_dict = dict:new() :: dict:dict(dcid(), non_neg_integer()), %%TODO uninitialized jsn is problematic because we cannot know if it is the first message received from a DC
    initialized = false :: boolean(),
    first_message_since_startup = true :: boolean()
}).
-type state() :: #state{}.

-record(jsn_helper, {
    next_jsn :: jsn(),
    next_dc_jsn :: jsn(),
    first_message_since_startup :: boolean(),
    rt_timestamp :: clock_time()
}).
-type jsn_helper() :: #jsn_helper{}.

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

handle_command(Request = hello, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {reply, ok, State};

handle_command(Request = get_journal_mnesia_table_name, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {reply, State#state.table_name, State};

handle_command(Request = get_current_dependency_vts, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    DependencyVts = vectorclock:set(gingko_utils:get_my_dcid(), gingko_utils:get_timestamp(), State#state.latest_vts),
    {reply, DependencyVts, State};

%%TODO consider timing issues during start-up with large tables
%%TODO failure testing required
%%TODO maybe consider migration for later
handle_command(Request = setup_journal_mnesia_table, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    TableName = State#state.table_name,
    Tables = mnesia:system_info(tables),
    case lists:member(TableName, Tables) of
        true ->
            NodesWhereThisTableExists = mnesia:table_info(TableName, where_to_write),
            %%TODO we will make sure that if this table exists then it will only run on our node after this call
            case NodesWhereThisTableExists of
                [] ->
                    {atomic, ok} = create_journal_mnesia_table(TableName);
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
            {atomic, ok} = create_journal_mnesia_table(TableName)
    end,
    {reply, ok, initialize_jsn(State)};

handle_command(Request = {get_journal_entries, DcIdOrAll, FilterFuncOrNone}, Sender, State = #state{table_name = TableName}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    SortedJournalEntries = gingko_log_utils:read_all_journal_entries_sorted(TableName),
    case DcIdOrAll of
        all -> {reply, {ok, SortedJournalEntries}, State};
        DcId ->
            Result = lists:filter(
                fun(JournalEntry = #journal_entry{dc_info = #dc_info{dcid = DCID}}) ->
                    DcId == DCID andalso
                        case FilterFuncOrNone of
                            none -> true;
                            FilterFunc -> FilterFunc(JournalEntry)
                        end
                end, SortedJournalEntries),
            {reply, {ok, Result}, State}
    end;

handle_command(Request = {add_remote_journal_entry, JournalEntry}, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    Result = add_remote_journal_entry(JournalEntry, State),
    {reply, Result, State};

handle_command(Request = {{_Op, _Args}, _TxId}, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {JsnHelper, NewState} = next_jsn(State),
    TableName = NewState#state.table_name,
    process_command({JsnHelper, TableName}, Request, Sender, NewState);

handle_command(Request, Sender, State) -> default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State).
handoff_starting(TargetNode, State) -> default_vnode_behaviour:handoff_starting(?MODULE, TargetNode, State).
handoff_cancelled(State) -> default_vnode_behaviour:handoff_cancelled(?MODULE, State).
handoff_finished(TargetNode, State) -> default_vnode_behaviour:handoff_finished(?MODULE, TargetNode, State).

handle_handoff_command(Request = #riak_core_fold_req_v2{foldfun = VisitFun, acc0 = Acc0}, Sender, State) ->
    %%TODO
    default_vnode_behaviour:handle_handoff_command(?MODULE, Request, Sender, State),
    {reply, ok, State};

handle_handoff_command(Request, Sender, State) ->
    default_vnode_behaviour:handle_handoff_command(?MODULE, Request, Sender, State).
handle_handoff_data(BinaryData, State) -> default_vnode_behaviour:handle_handoff_data(?MODULE, BinaryData, State).
encode_handoff_item(Key, Value) -> default_vnode_behaviour:encode_handoff_item(?MODULE, Key, Value).
is_empty(State) -> default_vnode_behaviour:is_empty(?MODULE, State).
terminate(Reason, State) -> default_vnode_behaviour:terminate(?MODULE, Reason, State).
delete(State) -> default_vnode_behaviour:delete(?MODULE, State).
handle_info(Request, State) -> default_vnode_behaviour:handle_info(?MODULE, Request, State).
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

-spec create_journal_mnesia_table(atom()) -> {atomic, ok} | {aborted, reason()}.
create_journal_mnesia_table(TableName) ->
    mnesia:create_table(TableName,
        [{attributes, record_info(fields, journal_entry)},
            %{index, [#journal_entry.jsn]},%TODO find out why index doesn't work here
            {ram_copies, [node()]},
            {record_name, journal_entry}
        ]).

-spec process_command({JsnHelper :: jsn_helper(), TableName :: atom()}, Request :: term(), pid() | atom(), state()) -> {reply, term(), state()}.
process_command({JsnHelper, TableName}, {{read, KeyStruct}, TxId}, _Sender, State) ->
    Operation = create_read_operation(KeyStruct, []),
    create_and_add_journal_entry(JsnHelper, TxId, Operation, TableName, State),
    {ok, Snapshot} = perform_tx_read(KeyStruct, TxId, TableName),
    {reply, {ok, Snapshot#snapshot.value}, State};

process_command({JsnHelper, TableName}, {{update, {KeyStruct, TypeOp}}, TxId}, _Sender, State) ->
    {ok, DownstreamOp} = gingko_utils:generate_downstream_op(KeyStruct, TxId, TypeOp, TableName),%TODO error check
    Operation = create_update_operation(KeyStruct, DownstreamOp),
    ok = create_and_add_journal_entry(JsnHelper, TxId, Operation, TableName, State),%TODO error check
    {reply, ok, State};

process_command({JsnHelper, TableName}, {{begin_txn, DependencyVts}, TxId}, _Sender, State) ->
    Operation = create_begin_operation(DependencyVts),
    ok = create_and_add_journal_entry(JsnHelper, TxId, Operation, TableName, State),%TODO error check
    {reply, ok, State};

process_command({JsnHelper, TableName}, {{{prepare_txn, PrepareTime}, TxId}, Ops}, _Sender, State) ->
    Operation = create_prepare_operation(PrepareTime),
    ok = create_and_add_journal_entry(JsnHelper, TxId, Operation, TableName, State),
    {atomic, ok} = gingko_log_utils:persist_journal_entries(TableName),%TODO error check
    %%TODO check that all ops are in the journal
    {reply, ok, State};

process_command({JsnHelper, TableName}, {{commit_txn, CommitTime}, TxId}, _Sender, State) ->
    Operation = create_commit_operation(CommitTime),
    ok = create_and_add_journal_entry(JsnHelper, TxId, Operation, TableName, State),%TODO error check
    {reply, ok, State};

process_command({JsnHelper, TableName}, {{abort_txn, _Args}, TxId}, _Sender, State) ->
    Operation = create_abort_operation(),
    ok = create_and_add_journal_entry(JsnHelper, TxId, Operation, TableName, State),%TODO error check
    {reply, ok, State};

process_command({JsnHelper, TableName}, {{checkpoint, DependencyVts}, TxId}, _Sender, State) ->
    Operation = create_checkpoint_operation(DependencyVts),
    ok = create_and_add_journal_entry(JsnHelper, TxId, Operation, TableName, State),%TODO error check
    {atomic, ok} = gingko_log_utils:persist_journal_entries(TableName),%TODO error check
    {reply, checkpoint(TableName), State}.

-spec checkpoint(atom()) -> ok.
checkpoint(TableName) ->
    SortedJournalEntries = gingko_log_utils:read_all_journal_entries_sorted(TableName),
    RelevantJournalList = lists:reverse(SortedJournalEntries),
    Checkpoints = lists:filter(fun(J) ->
        gingko_utils:is_system_operation(J, checkpoint) end, RelevantJournalList),
    {PreviousCheckpointVts, CurrentCheckpointVts} =
        case Checkpoints of
            [] -> {vectorclock:new(), vectorclock:new()}; %Should not happen really
            [CurrentCheckpoint1] ->
                {vectorclock:new(), CurrentCheckpoint1#journal_entry.operation#system_operation.op_args#checkpoint_args.dependency_vts};
            [CurrentCheckpoint2, PreviousCheckpoint | _] ->
                {PreviousCheckpoint#journal_entry.operation#system_operation.op_args#checkpoint_args.dependency_vts, CurrentCheckpoint2#journal_entry.operation#system_operation.op_args#checkpoint_args.dependency_vts}
        end,
    %Get all keys from previous checkpoint and their dependency vts
    CommittedJournalEntries = gingko_materializer:get_committed_journal_entries_for_keys(SortedJournalEntries, all_keys),
    RelevantKeysInJournal =
        sets:to_list(sets:from_list(
            lists:filtermap(
                fun({CommitJ, UpdateJList}) ->
                    CommitVts = CommitJ#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_vts,
                    case gingko_utils:is_in_vts_range(CommitVts, {PreviousCheckpointVts, CurrentCheckpointVts}) of
                        true -> {true, lists:map(fun(UpdateJ) ->
                            UpdateJ#journal_entry.operation#object_operation.key_struct end, UpdateJList)};
                        false -> false
                    end
                end, CommittedJournalEntries))),
    SnapshotsToStore =
        lists:map(
            fun(K) ->
                {ok, Snapshot} = gingko_utils:call_gingko_sync_with_key(K, ?GINGKO_CACHE, {get, K, CurrentCheckpointVts}),
                Snapshot
            end, RelevantKeysInJournal),
    gingko_log_utils:add_or_update_checkpoint_entries(SnapshotsToStore).
%TODO cache cleanup

-spec perform_tx_read(key_struct(), txid(), atom()) -> {ok, snapshot()} | {error, reason()}.
perform_tx_read(KeyStruct, TxId, TableName) ->
    CurrentTxJournalEntries = gingko_log_utils:read_journal_entries_with_tx_id_sorted(TxId, TableName),
    Begin = hd(CurrentTxJournalEntries),
    BeginVts = Begin#journal_entry.operation#system_operation.op_args#begin_txn_args.dependency_vts,
    {ok, SnapshotBeforeTx} = gingko_utils:call_gingko_sync_with_key(KeyStruct, ?GINGKO_CACHE, {get, KeyStruct, BeginVts}),
    UpdatesToBeAdded =
        lists:filter(
            fun(J) ->
                gingko_utils:is_update_of_keys(J, [KeyStruct])
            end, CurrentTxJournalEntries),
    gingko_materializer:materialize_snapshot_temporarily(SnapshotBeforeTx, UpdatesToBeAdded).

-spec create_and_add_journal_entry(jsn_helper(), txid(), operation(), atom(), state()) -> ok.
create_and_add_journal_entry(JsnHelper, TxId, Operation, TableName, #state{partition = Partition}) ->
    JournalEntry = create_journal_entry(JsnHelper, TxId, Operation),
    gingko_log_utils:add_journal_entry(JournalEntry, TableName),
    inter_dc_txn_manager:buffer_journal_entry(JournalEntry, Partition).

-spec add_remote_journal_entry(journal_entry(), state()) -> {ok, state()} | {error, {previous_journal_entry_missing, Info :: term()}}.
add_remote_journal_entry(JournalEntry, State) ->
    NextJsn = State#state.next_jsn,
    TableName = State#state.table_name,
    DcToNextJsnDict = State#state.dc_to_next_jsn_dict,
    DcId = JournalEntry#journal_entry.dc_info#dc_info.dcid,
    DcJsn = JournalEntry#journal_entry.dc_info#dc_info.jsn,
    DcRtTimestamp = JournalEntry#journal_entry.dc_info#dc_info.rt_timestamp,
    %%TODO special case if its the first received
    NextDcJsn = general_utils:get_or_default_dict(DcToNextJsnDict, DcId, 1),
    FirstMessage = JournalEntry#journal_entry.dc_info#dc_info.first_message_since_startup,

    case DcJsn == NextDcJsn orelse (FirstMessage andalso NextDcJsn == 1) of
        true ->
            gingko_log_utils:add_journal_entry(JournalEntry#journal_entry{jsn = NextJsn}, TableName),
            VC = vectorclock:set(DcId, DcRtTimestamp, State#state.latest_vts),
            NewState = State#state{latest_vts = VC, next_jsn = NextJsn + 1, dc_to_next_jsn_dict = dict:store(DcId, DcJsn + 1, DcToNextJsnDict)},
            {ok, NewState};%%TODO hidden special case
        false ->
            {error, {previous_journal_entry_missing, {JournalEntry, State}}}
    end.

-spec create_journal_entry(jsn_helper(), txid(), operation()) -> journal_entry().
create_journal_entry(JsnHelper, TxId, Operation) ->
    Jsn = JsnHelper#jsn_helper.next_jsn,
    DcJsn = JsnHelper#jsn_helper.next_dc_jsn,
    FirstMessage = JsnHelper#jsn_helper.first_message_since_startup,
    DcId = gingko_utils:get_my_dcid(),
    #journal_entry{
        jsn = Jsn,
        dc_info = #dc_info{dcid = DcId, rt_timestamp = gingko_utils:get_timestamp(), jsn = DcJsn, first_message_since_startup = FirstMessage},
        tx_id = TxId,
        operation = Operation
    }.

-spec create_read_operation(key_struct(), term()) -> object_operation().
create_read_operation(KeyStruct, Args) ->
    #object_operation{
        key_struct = KeyStruct,
        op_type = read,
        op_args = Args
    }.

-spec create_update_operation(key_struct(), downstream_op()) -> object_operation().
create_update_operation(KeyStruct, DownstreamOp) ->
    #object_operation{
        key_struct = KeyStruct,
        op_type = update,
        op_args = DownstreamOp
    }.

-spec create_begin_operation(vectorclock()) -> system_operation().
create_begin_operation(DependencyVts) ->
    #system_operation{
        op_type = begin_txn,
        op_args = #begin_txn_args{dependency_vts = DependencyVts}
    }.

-spec create_prepare_operation(non_neg_integer()) -> system_operation().
create_prepare_operation(PrepareTime) ->
    #system_operation{
        op_type = prepare_txn,
        op_args = #prepare_txn_args{prepare_time = PrepareTime}
    }.

-spec create_commit_operation(vectorclock()) -> system_operation().
create_commit_operation(CommitTime) ->
    #system_operation{
        op_type = commit_txn,
        op_args = #commit_txn_args{commit_vts = CommitTime}
    }.

-spec create_abort_operation() -> system_operation().
create_abort_operation() ->
    #system_operation{
        op_type = abort_txn,
        op_args = #abort_txn_args{}
    }.

-spec create_checkpoint_operation(vectorclock()) -> system_operation().
create_checkpoint_operation(DependencyVts) ->
    #system_operation{
        op_type = checkpoint,
        op_args = #checkpoint_args{dependency_vts = DependencyVts}
    }.

-spec next_jsn(state()) -> {jsn_helper(), state()}.
next_jsn(State) ->
    NextJsn = State#state.next_jsn,
    Dict = State#state.dc_to_next_jsn_dict,
    DcId = gingko_utils:get_my_dcid(),
    NextDcJsn = general_utils:get_or_default_dict(Dict, DcId, 1),
    UpdatedDict = dict:store(DcId, NextDcJsn + 1, Dict),
    RtTimestamp = gingko_utils:get_timestamp(),
    LatestVts = vectorclock:set(DcId, RtTimestamp, State#state.latest_vts),
    JsnHelper = #jsn_helper{next_jsn = NextJsn, next_dc_jsn = NextDcJsn, first_message_since_startup = State#state.first_message_since_startup, rt_timestamp = RtTimestamp},
    {JsnHelper, State#state{latest_vts = LatestVts, next_jsn = NextJsn + 1, dc_to_next_jsn_dict = UpdatedDict, first_message_since_startup = false}}.

-spec initialize_jsn(state()) -> state().
initialize_jsn(State) ->
    SortedJournalEntries = gingko_log_utils:read_all_journal_entries_sorted(State#state.table_name),
    NextJsn = case SortedJournalEntries of
                  [] -> 1;
                  _ ->
                      LastJournalEntry = lists:last(SortedJournalEntries),
                      LastJournalEntry#journal_entry.jsn + 1
              end,
    DcIdToJournalEntries = general_utils:group_by(fun(J) ->
        J#journal_entry.dc_info#dc_info.dcid end, SortedJournalEntries),
    %%TODO maybe simplify a little
    %%Guarantued: jsn1 < jsn2 => rt_timestamp1 < rt_timestamp2
    DcIdToLatestJournalEntryDict = dict:fold(
        fun(DcId, JournalEntries, AccIn) ->
            JournalEntryHighestJsn = general_utils:max_by(fun(J) ->
                J#journal_entry.dc_info#dc_info.jsn end, JournalEntries),
            dict:store(DcId, JournalEntryHighestJsn, AccIn)
        end, dict:new(), DcIdToJournalEntries),
    DcIdToNextJsnDict = dict:map(
        fun(_DcId, JournalEntry) ->
            JournalEntry#journal_entry.dc_info#dc_info.jsn + 1
        end, DcIdToLatestJournalEntryDict),
    VectorclockDict = dict:map(
        fun(_DcId, JournalEntry) ->
            JournalEntry#journal_entry.dc_info#dc_info.rt_timestamp
        end, DcIdToLatestJournalEntryDict),
    VC = vectorclock:from_list(dict:to_list(VectorclockDict)),

    State#state{latest_vts = VC, next_jsn = NextJsn, dc_to_next_jsn_dict = DcIdToNextJsnDict, initialized = true}.
