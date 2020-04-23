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
-include_lib("kernel/include/logger.hrl").
-behaviour(riak_core_vnode).

%% API
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

-record(state, {
    partition :: partition_id(),
    table_name :: atom(),
    next_jsn = 0 :: non_neg_integer(),
    initialized = false :: boolean()
}).
-type state() :: #state{}.

-spec start_vnode(integer()) -> any().
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    logger:debug("init(~nPartition: ~p~n)", [Partition]),
    TableName = general_utils:concat_and_make_atom([integer_to_list(Partition), '_journal_entry']),
    GingkoConfig = [{partition, Partition}, {table_name, TableName} | gingko_app:get_default_config()],
    NewState = apply_gingko_config(#state{}, GingkoConfig),
    {ok, NewState}.

handle_command(setup_new_mnesia_table = Request, Sender, State) ->
    logger:debug("handle_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Request, Sender, State]),
    TableName = State#state.table_name,
    mnesia:create_table(TableName,
        [{attributes, record_info(fields, journal_entry)},
            %{index, [#journal_entry.jsn]},%TODO find out why index doesn't work here
            {ram_copies, [node()]},
            {record_name, journal_entry}
        ]),
    Result = mnesia:wait_for_tables([TableName], 5000), %TODO error handling
    {reply, Result, initialize_jsn(State)};

handle_command({{Op, Args}, TxId} = Request, Sender, State) ->
    logger:debug("handle_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Request, Sender, State]),
    {NextJsn, NewState} = next_jsn(State),
    TableName = NewState#state.table_name,
    case Op of
        read ->
            KeyStruct = Args,
            Operation = create_read_operation(KeyStruct, []),
            create_and_add_journal_entry(NextJsn, TxId, Operation, TableName),
            {ok, Snapshot} = perform_tx_read(KeyStruct, TxId, TableName),
            {reply, {ok, Snapshot#snapshot.value}, NewState};
        update ->
            {KeyStruct, TypeOp} = Args,
            {ok, DownstreamOp} = gingko_utils:generate_downstream_op(KeyStruct, TxId, TypeOp, TableName),%TODO error check
            Operation = create_update_operation(KeyStruct, DownstreamOp),
            ok = create_and_add_journal_entry(NextJsn, TxId, Operation, TableName),%TODO error check
            {reply, ok, NewState};
        begin_txn ->
            DependencyVts = Args,
            Operation = create_begin_operation(DependencyVts),
            ok = create_and_add_journal_entry(NextJsn, TxId, Operation, TableName),%TODO error check
            {reply, ok, NewState};
        prepare_txn ->
            PrepareTime = Args,
            Operation = create_prepare_operation(PrepareTime),
            ok = create_and_add_journal_entry(NextJsn, TxId, Operation, TableName),
            {atomic, ok} = gingko_log_utils:persist_journal_entries(TableName),%TODO error check
            {reply, ok, NewState};
        commit_txn ->
            CommitTime = Args,
            Operation = create_commit_operation(CommitTime),
            ok = create_and_add_journal_entry(NextJsn, TxId, Operation, TableName),%TODO error check
            {reply, ok, NewState};
        abort_txn ->
            Operation = create_abort_operation(),
            ok = create_and_add_journal_entry(NextJsn, TxId, Operation, TableName),%TODO error check
            {reply, ok, NewState};
        checkpoint ->
            DependencyVts = Args,
            Operation = create_checkpoint_operation(DependencyVts),
            ok = create_and_add_journal_entry(NextJsn, TxId, Operation, TableName),%TODO error check
            {atomic, ok} = gingko_log_utils:persist_journal_entries(TableName),%TODO error check
            checkpoint(TableName),
            ok%TODO
    end;

handle_command(Request, Sender, State) ->
    logger:debug("handle_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Request, Sender, State]),
    {noreply, State}.

handoff_starting(TargetNode, State) ->
    logger:debug("handoff_starting(~nTargetNode: ~p~nState: ~p~n)", [TargetNode, State]),
    {true, State}.

handoff_cancelled(State) ->
    logger:debug("handoff_cancelled(~nState: ~p~n)", [State]),
    {ok, State}.

handoff_finished(TargetNode, State) ->
    logger:debug("handoff_finished(~nTargetNode: ~p~nState: ~p~n)", [TargetNode, State]),
    {ok, State}.

handle_handoff_command(?FOLD_REQ{foldfun = VisitFun, acc0 = Acc0} = Request, Sender, State) ->
    logger:debug("handle_handoff_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Request, Sender, State]),
    {reply, ok, State}; %TODO

handle_handoff_command(Request, Sender, State) ->
    logger:debug("handle_handoff_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Request, Sender, State]),
    {noreply, State}.

handle_handoff_data(BinaryData, State) ->
    logger:debug("handle_handoff_data(~nData: ~p~nState: ~p~n)", [binary_to_term(BinaryData), State]),
    {reply, ok, State}.

encode_handoff_item(Key, Value) ->
    logger:debug("encode_handoff_item(~nKey: ~p~nValue: ~p~n)", [Key, Value]),
    term_to_binary({Key, Value}).

is_empty(State) ->
    logger:debug("is_empty(~nState: ~p~n)", [State]),
    {true, State}.

terminate(Reason, State) ->
    logger:debug("terminate(~nReason: ~p~nState: ~p~n)", [Reason, State]),
    ok.

delete(State) ->
    logger:debug("delete(~nRequest: ~p~n)", [State]),
    {ok, State}.

handle_info(Request, State) ->
    logger:debug("handle_info(~nRequest: ~p~nState: ~p~n)", [Request, State]),
    {ok, State}.

handle_exit(Pid, Reason, State) ->
    logger:debug("handle_exit(~nPid: ~p~nReason: ~p~nState: ~p~n)", [Pid, Reason, State]),
    {noreply, State}.

handle_coverage(Request, KeySpaces, Sender, State) ->
    logger:debug("handle_coverage(~nRequest: ~p~nKeySpaces: ~p~nSender: ~p~nState: ~p~n)", [Request, KeySpaces, Sender, State]),
    {stop, not_implemented, State}.

handle_overload_command(Request, Sender, Partition) ->
    logger:debug("handle_overload_command(~nRequest: ~p~nSender: ~p~nPartition: ~p~n)", [Request, Sender, Partition]),
    ok.

handle_overload_info(Request, Partition) ->
    logger:debug("handle_overload_info(~nRequest: ~p~nPartition: ~p~n)", [Request, Partition]),
    ok.

-spec apply_gingko_config(state(), [{atom(), term()}]) -> state().
apply_gingko_config(State, GingkoConfig) ->
    Partition = general_utils:get_or_default_map_list(partition, GingkoConfig, error),
    TableName = general_utils:get_or_default_map_list(table_name, GingkoConfig, error),
    State#state{partition = Partition, table_name = TableName}.

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
                {ok, Snapshot} =
                    IndexNode = antidote_log_utilities:get_key_partition(K),
                    riak_core_vnode_master:sync_command(IndexNode,
                        {get, K, CurrentCheckpointVts},
                        gingko_cache_vnode_master,
                        infinity),
                Snapshot
            end, RelevantKeysInJournal),
    gingko_log_utils:add_or_update_checkpoint_entries(SnapshotsToStore).
    %TODO cache cleanup

-spec perform_tx_read(key_struct(), txid(), atom()) -> {ok, snapshot()} | {error, reason()}.
perform_tx_read(KeyStruct, TxId, TableName) ->
    CurrentTxJournalEntries = gingko_log_utils:read_journal_entries_with_tx_id_sorted(TxId, TableName),
    Begin = hd(CurrentTxJournalEntries),
    BeginVts = Begin#journal_entry.operation#system_operation.op_args#begin_txn_args.dependency_vts,
    IndexNode = antidote_log_utilities:get_key_partition(KeyStruct),
    {ok, SnapshotBeforeTx} = riak_core_vnode_master:sync_command(IndexNode,
        {get, KeyStruct, BeginVts},
        gingko_cache_vnode_master,
        infinity),
    UpdatesToBeAdded =
        lists:filter(
            fun(J) ->
                gingko_utils:is_update_of_keys(J, [KeyStruct])
            end, CurrentTxJournalEntries),
    gingko_materializer:materialize_snapshot_temporarily(SnapshotBeforeTx, UpdatesToBeAdded).

-spec create_and_add_journal_entry(jsn(), txid(), operation(), atom()) -> ok.
create_and_add_journal_entry(Jsn, TxId, Operation, TableName) ->
    JournalEntry = create_journal_entry(Jsn, TxId, Operation),
    gingko_log_utils:add_journal_entry(JournalEntry, TableName).

-spec create_journal_entry(jsn(), txid(), operation()) -> journal_entry().
create_journal_entry(Jsn, TxId, Operation) ->
    DcId = antidote_dc_utilities:get_my_dc_id(),
    #journal_entry{
        jsn = Jsn,
        dcid = DcId,
        rt_timestamp = gingko_utils:get_timestamp(),
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

-spec next_jsn(state()) -> {non_neg_integer(), state()}.
next_jsn(State) ->
    %TODO check performance
    Jsn = State#state.next_jsn,
    {Jsn, State#state{next_jsn = Jsn + 1}}.

-spec initialize_jsn(state()) -> state().
initialize_jsn(State) ->
    State#state{next_jsn = mnesia:table_info(State#state.table_name, size) + 1, initialized = true}.
