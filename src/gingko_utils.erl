%% -------------------------------------------------------------------
%%
%% Copyright 2020, Kevin Bartik <k_bartik12@cs.uni-kl.de>
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

-module(gingko_utils).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-export([get_timestamp/0,
    get_my_dcid/0,
    get_connected_dcids/0,
    get_my_dc_nodes/0,
    get_DCSf_vts/0,
    get_GCSt_vts/0,
    get_my_partitions/0,
    get_number_of_partitions/0,
    gingko_send_after/2,
    update_timer/5,

    check_registered/1,
    ensure_local_gingko_instance_running/1,
    ensure_gingko_instance_running/1,

    get_smallest_vts/1,
    get_largest_vts/1,
    get_smallest_by_vts/2,
    get_largest_by_vts/2,
    sort_vts_list/1,
    sort_by_vts/2,

    create_new_snapshot/2,
    create_cache_entry/1,
    update_cache_usage/2,

    create_snapshot_from_cache_entry/1,
    create_snapshot_from_checkpoint_entry/2,

    create_default_value/1,
    is_system_operation/1,
    is_journal_entry_type/2,
    contains_journal_entry_type/2,
    get_updates/1,
    get_updates_of_key/2,

    is_update_of_key/2,
    is_update_of_key_or_commit/2,
    is_update/1,
    get_journal_entries_of_type/2,

    get_keys_from_updates/1,
    sort_same_journal_entry_type_list_by_vts/1,
    sort_journal_entries_of_same_tx/1,
    remove_existing_journal_entries_handoff/2,

    get_default_txn_tracking_num/0,
    generate_downstream_op/4,

    call_gingko_async/3,
    call_gingko_sync/3,
    call_gingko_async_with_key/3,
    call_gingko_sync_with_key/3,
    bcast_local_gingko_async/2,
    bcast_local_gingko_sync/2,
    bcast_gingko_async/2,
    bcast_gingko_sync/2,

    get_key_partition/1]).

%% Gets the current erlang timestamp and returns it as an integer that represents microseconds
-spec get_timestamp() -> non_neg_integer().
get_timestamp() ->
    case ?USE_GINGKO_TIMESTAMP of
        true ->
            case ?EXPERIMENTAL_TIMESTAMP_USE_MONOTONIC_TIME of
                true -> gingko_time_manager:get_positive_monotonic_time();
                false -> gingko_time_manager:get_monotonic_system_time()
            end;
        false ->
            general_utils:get_timestamp_in_microseconds() %TODO decide which timestamp to use
    end.

%% Gets the DCID
%% USE_SINGLE_SERVER: returns the current node because a DC can only have a single node
-spec get_my_dcid() -> dcid().
get_my_dcid() ->
    case ?USE_SINGLE_SERVER of
        true -> node();
        false -> antidote_utils:get_my_dc_id()
    end.

%% Gets the connected DCIDs
-spec get_connected_dcids() -> [dcid()].
get_connected_dcids() ->
    inter_dc_meta_data_manager:get_connected_dcids().

%% Gets all nodes of the DC
%% USE_SINGLE_SERVER: returns the current node because a DC can only have a single node
-spec get_my_dc_nodes() -> [node()].
get_my_dc_nodes() ->
    case ?USE_SINGLE_SERVER of
        true -> [node()];
        false -> antidote_utils:get_my_dc_nodes()
    end.

-spec get_DCSf_vts() -> vectorclock().
get_DCSf_vts() ->
    Vectorclocks = general_utils:get_values_times(bcast_gingko_sync(?GINGKO_LOG, get_current_dependency_vts), 2),
    vectorclock:min(Vectorclocks).

-spec get_GCSt_vts() -> vectorclock().
get_GCSt_vts() ->
    inter_dc_state_service:get_GCSt().

%% Gets all partitions of the caller node
%% USE_SINGLE_SERVER: returns the default SINGLE_SERVER_PARTITION in a list
-spec get_my_partitions() -> [partition_id()].
get_my_partitions() ->
    case ?USE_SINGLE_SERVER of
        true -> [?SINGLE_SERVER_PARTITION];
        false -> antidote_utils:get_my_partitions()
    end.

%% Gets all partitions of the DC
%% USE_SINGLE_SERVER: returns the default SINGLE_SERVER_PARTITION in a list
-spec get_all_partitions() -> [partition_id()].
get_all_partitions() ->
    case ?USE_SINGLE_SERVER of
        true -> [?SINGLE_SERVER_PARTITION];
        false -> antidote_utils:get_all_partitions()
    end.

%% Gets the number of all partitions of the DC
%% USE_SINGLE_SERVER: returns 1
-spec get_number_of_partitions() -> non_neg_integer().
get_number_of_partitions() ->
    case ?USE_SINGLE_SERVER of
        true -> 1;
        false -> antidote_utils:get_number_of_partitions()
    end.

-spec gingko_send_after(millisecond(), term()) -> reference().
gingko_send_after(TimeMillis, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> erlang:send_after(TimeMillis, self(), Request);
        false -> riak_core_vnode:send_command_after(TimeMillis, Request)
    end.

-spec update_timer(none | reference(), boolean(), millisecond(), term(), boolean()) -> reference().
update_timer(CurrentTimerOrNone, UpdateExistingTimer, TimeMillis, Request, IsPotentialVNode) ->
    UpdateTimerFun =
        fun() ->
            case IsPotentialVNode of
                true -> gingko_send_after(TimeMillis, Request);
                false -> erlang:send_after(TimeMillis, self(), Request)
            end
        end,
    case CurrentTimerOrNone of
        none ->
            UpdateTimerFun();
        TimerReference ->
            case UpdateExistingTimer of
                true ->
                    erlang:cancel_timer(TimerReference),
                    UpdateTimerFun();
                false -> TimerReference
            end
    end.


%% Waits until a process/vnode name is registered in erlang
-spec check_registered({atom(), atom()} | atom()) -> ok.
check_registered({ServerName, VMaster}) ->
    case ?USE_SINGLE_SERVER of
        true ->
            antidote_utils:check_registered(ServerName);
        false ->
            antidote_utils:check_registered(VMaster)
    end;
check_registered(ServerName) ->
    antidote_utils:check_registered(ServerName).

-spec ensure_local_gingko_instance_running({atom(), atom()} | atom()) -> ok.
ensure_local_gingko_instance_running({ServerName, VMaster}) ->
    check_registered({ServerName, VMaster}),
    bcast_gingko_instance_check_up({ServerName, VMaster}, get_my_partitions());
ensure_local_gingko_instance_running(ServerName) ->
    check_registered(ServerName),
    bcast_gingko_instance_check_up(ServerName, [0]).

-spec ensure_gingko_instance_running({atom(), atom()} | atom()) -> ok.
ensure_gingko_instance_running({ServerName, VMaster}) ->
    check_registered({ServerName, VMaster}),
    bcast_gingko_instance_check_up({ServerName, VMaster}, get_all_partitions());
ensure_gingko_instance_running(ServerName) ->
    ensure_local_gingko_instance_running(ServerName).

%% Internal function that loops until a given vnode type is running
-spec bcast_gingko_instance_check_up({atom(), atom()} | atom(), [partition_id()]) -> ok.
bcast_gingko_instance_check_up(_, []) ->
    ok;
bcast_gingko_instance_check_up(InstanceTuple = {_ServerName, _VMaster}, [Partition | Rest]) ->
    Error = try
                case call_gingko_sync(Partition, InstanceTuple, hello) of
                    ok -> false;
                    _Msg -> true
                end
            catch
                _Ex:_Res -> true
            end,
    case Error of
        true ->
            logger:debug("Vnode not up retrying, ~p, ~p", [InstanceTuple, Partition]),
            timer:sleep(?DEFAULT_WAIT_TIME_SHORT),
            bcast_gingko_instance_check_up(InstanceTuple, [Partition | Rest]);
        false ->
            bcast_gingko_instance_check_up(InstanceTuple, Rest)
    end;
bcast_gingko_instance_check_up(ServerName, [0]) ->
    Error = try
                case gen_server:call(ServerName, hello) of
                    ok -> false;
                    _Msg -> true
                end
            catch
                _Ex:_Res -> true
            end,
    case Error of
        true ->
            logger:debug("Server not up retrying, ~p", [ServerName]),
            timer:sleep(?DEFAULT_WAIT_TIME_SHORT),
            bcast_gingko_instance_check_up(ServerName, [0]);
        false ->
            ok
    end.

get_smallest_vts(VtsList) ->
    get_smallest_by_vts(fun(Vts) -> Vts end, VtsList).

get_largest_vts(VtsList) ->
    get_largest_by_vts(fun(Vts) -> Vts end, VtsList).

get_smallest_by_vts(FunThatGetsVtsFromElement, ElementThatContainsVtsList) ->
    general_utils:list_first_match(
        fun(Vts) ->
            not lists:any(
                fun(OtherVts) ->
                    vectorclock:lt(FunThatGetsVtsFromElement(OtherVts), FunThatGetsVtsFromElement(Vts))
                end, ElementThatContainsVtsList)
        end, ElementThatContainsVtsList).

get_largest_by_vts(FunThatGetsVtsFromElement, ElementThatContainsVtsList) ->
    general_utils:list_first_match(
        fun(Vts) ->
            not lists:any(
                fun(OtherVts) ->
                    vectorclock:gt(FunThatGetsVtsFromElement(OtherVts), FunThatGetsVtsFromElement(Vts))
                end, ElementThatContainsVtsList)
        end, ElementThatContainsVtsList).

sort_vts_list([]) -> [];
sort_vts_list([Vts]) -> [Vts];
sort_vts_list(VtsList) ->
    sort_by_vts(fun(Vts) -> Vts end, VtsList).

sort_by_vts(_, []) -> [];
sort_by_vts(_, [ElementThatContainsVts]) -> [ElementThatContainsVts];
sort_by_vts(FunThatGetsVtsFromElement, ElementThatContainsVtsList) ->
    {SmallestElementThatContainsVtsList, RemainingElementThatContainsVtsList} =
        lists:foldl(
            fun(ElementThatContainsVts, {CurrentSmallestElementThatContainsVtsList, CurrentRemainingElementThatContainsVtsList}) ->
                AnySmaller =
                    lists:any(
                        fun(OtherElementThatContainsVts) ->
                            vectorclock:lt(FunThatGetsVtsFromElement(OtherElementThatContainsVts), FunThatGetsVtsFromElement(ElementThatContainsVts))
                        end, CurrentRemainingElementThatContainsVtsList),
                case AnySmaller of
                    true ->
                        {CurrentSmallestElementThatContainsVtsList, [ElementThatContainsVts | CurrentRemainingElementThatContainsVtsList]};
                    false ->
                        {[ElementThatContainsVts | CurrentSmallestElementThatContainsVtsList], CurrentRemainingElementThatContainsVtsList}
                end
            end, {[], []}, ElementThatContainsVtsList),
    SmallestElementThatContainsVtsList ++ sort_vts_list(RemainingElementThatContainsVtsList).


-spec create_new_snapshot(key_struct(), vectorclock()) -> snapshot().
create_new_snapshot(KeyStruct = #key_struct{type = Type}, DependencyVts) ->
    DefaultValue = create_default_value(Type),
    #snapshot{key_struct = KeyStruct, commit_vts = DependencyVts, snapshot_vts = DependencyVts, value = DefaultValue}.

-spec create_cache_entry(snapshot()) -> cache_entry().
create_cache_entry(Snapshot) ->
    #cache_entry{snapshot = Snapshot, usage = #cache_usage{first_used = get_timestamp(), last_used = get_timestamp()}}.

-spec update_cache_usage(cache_entry(), boolean()) -> cache_entry().
update_cache_usage(CacheEntry = #cache_entry{usage = CacheUsage}, Used) ->
    NewCacheUsage = case Used of
                        true ->
                            CacheUsage#cache_usage{used = true, last_used = get_timestamp(), times_used = CacheUsage#cache_usage.times_used + 1};
                        false -> CacheUsage#cache_usage{used = false}
                    end,
    CacheEntry#cache_entry{usage = NewCacheUsage}.

-spec create_snapshot_from_cache_entry(cache_entry()) -> snapshot().
create_snapshot_from_cache_entry(#cache_entry{snapshot = Snapshot}) ->
    Snapshot.

-spec create_snapshot_from_checkpoint_entry(checkpoint_entry(), vectorclock()) -> snapshot().
create_snapshot_from_checkpoint_entry(CheckpointEntry, DependencyVts) ->
    KeyStruct = CheckpointEntry#checkpoint_entry.key_struct,
    Value = CheckpointEntry#checkpoint_entry.value,
    #snapshot{key_struct = KeyStruct, commit_vts = DependencyVts, snapshot_vts = DependencyVts, value = Value}.

-spec create_default_value(type()) -> crdt().
create_default_value(Type) ->
    Type:new().

-spec is_system_operation(journal_entry_type() | journal_entry()) -> boolean().
is_system_operation(begin_txn) -> true;
is_system_operation(prepare_txn) -> true;
is_system_operation(commit_txn) -> true;
is_system_operation(abort_txn) -> true;
is_system_operation(checkpoint) -> true;
is_system_operation(JournalEntryType) when is_atom(JournalEntryType) -> false;
is_system_operation(#journal_entry{type = JournalEntryType}) -> is_system_operation(JournalEntryType);
is_system_operation(_) -> false.

-spec is_journal_entry_type(journal_entry(), journal_entry_type() | [journal_entry_type()]) -> boolean().
is_journal_entry_type(#journal_entry{type = OpType}, OpType) -> true;
is_journal_entry_type(#journal_entry{type = OpType}, OpTypeList) when is_list(OpTypeList) ->
    lists:member(OpType, OpTypeList);
is_journal_entry_type(_, _) -> false.

-spec contains_journal_entry_type([journal_entry()], journal_entry_type()) -> boolean().
contains_journal_entry_type(JournalEntryList, OpType) ->
    lists:any(fun(JournalEntry) -> is_journal_entry_type(JournalEntry, OpType) end, JournalEntryList).

-spec get_updates([journal_entry()]) -> [journal_entry()].
get_updates(JournalEntryList) ->
    lists:filter(fun(JournalEntry) -> is_update(JournalEntry) end, JournalEntryList).

-spec get_updates_of_key([journal_entry()], key_struct()) -> [journal_entry()].
get_updates_of_key(JournalEntryList, KeyStruct) ->
    lists:filter(fun(JournalEntry) -> is_update_of_key(JournalEntry, KeyStruct) end, JournalEntryList).

-spec is_update_of_key(journal_entry(), key_struct()) -> boolean().
is_update_of_key(#journal_entry{type = update, args = #update_args{key_struct = KeyStruct}}, KeyStruct) -> true;
is_update_of_key(_, _) -> false.

-spec is_update_of_key_or_commit(journal_entry(), key_struct()) -> boolean().
is_update_of_key_or_commit(JournalEntry, KeyStruct) ->
    is_update_of_key(JournalEntry, KeyStruct) orelse is_journal_entry_type(JournalEntry, commit_txn).

-spec is_update(journal_entry()) -> boolean().
is_update(#journal_entry{type = update}) -> true;
is_update(_) -> false.

-spec get_journal_entries_of_type([journal_entry()], journal_entry_type() | [journal_entry_type()]) -> [journal_entry()].
get_journal_entries_of_type(JournalEntryList, GivenType) when is_atom(GivenType) ->
    lists:filter(fun(#journal_entry{type = Type}) -> Type == GivenType end, JournalEntryList);
get_journal_entries_of_type(JournalEntryList, GivenTypes) when is_list(GivenTypes) ->
    lists:filter(fun(#journal_entry{type = Type}) -> lists:member(Type, GivenTypes) end, JournalEntryList).

-spec get_keys_from_updates([journal_entry()]) -> [key_struct()].
get_keys_from_updates(JournalEntryList) ->
    lists:filtermap(
        fun(JournalEntry) ->
            case JournalEntry of
                #journal_entry{type = update, args = #update_args{key_struct = KeyStruct}} -> {true, KeyStruct};
                _ -> false
            end
        end, JournalEntryList).

-spec sort_same_journal_entry_type_list_by_vts([journal_entry()]) -> [journal_entry()].
sort_same_journal_entry_type_list_by_vts(JournalEntryList) ->
    sort_by_vts(
        fun(#journal_entry{args = Args}) ->
            case Args of
                #begin_txn_args{dependency_vts = DependencyVts} ->
                    DependencyVts;
                #commit_txn_args{commit_vts = CommitVts} ->
                    CommitVts;
                #checkpoint_args{dependency_vts = CheckpointVts} ->
                    CheckpointVts
            end
        end, JournalEntryList).

-spec sort_journal_entries_of_same_tx([journal_entry()]) -> [journal_entry()].
sort_journal_entries_of_same_tx(JournalEntryList) ->
    lists:sort(fun tx_sort_fun/2, JournalEntryList).

-spec tx_sort_fun(journal_entry(), journal_entry()) -> boolean().
tx_sort_fun(JournalEntry1, JournalEntry2) ->
    op_type_sort(get_op_type(JournalEntry1), get_op_type(JournalEntry2)).

-spec op_type_sort(atom() | non_neg_integer(), atom() | non_neg_integer()) -> boolean().
op_type_sort(begin_txn, _) -> true;
op_type_sort(_, begin_txn) -> false;
op_type_sort(TxOpNum, OpType) when is_integer(TxOpNum), is_atom(OpType) -> true;
op_type_sort(OpType, TxOpNum) when is_atom(OpType), is_integer(TxOpNum) -> false;
op_type_sort(TxOpNum1, TxOpNum2) when is_integer(TxOpNum1), is_integer(TxOpNum2) -> TxOpNum1 < TxOpNum2;
op_type_sort(prepare_txn, _) -> true;
op_type_sort(_, prepare_txn) -> false;
op_type_sort(commit_txn, _) -> true;
op_type_sort(_, commit_txn) -> false;
op_type_sort(abort_txn, _) -> true;
op_type_sort(_, abort_txn) -> false;
op_type_sort(checkpoint, _) -> true;
op_type_sort(_, checkpoint) -> false;
op_type_sort(checkpoint_commit, _) -> true;
op_type_sort(_, checkpoint_commit) -> false.

-spec get_op_type(journal_entry()) -> atom() | non_neg_integer().
get_op_type(#journal_entry{args = #update_args{tx_op_num = TxOpNum}}) -> TxOpNum;
get_op_type(#journal_entry{type = OpType}) -> OpType.

-spec remove_existing_journal_entries_handoff([journal_entry()], [journal_entry()]) -> [journal_entry()].
remove_existing_journal_entries_handoff(NewJournalEntryList, []) -> NewJournalEntryList;
remove_existing_journal_entries_handoff([#journal_entry{type = checkpoint}], [#journal_entry{type = checkpoint}]) -> [];
remove_existing_journal_entries_handoff([#journal_entry{type = begin_txn}, #journal_entry{type = commit_txn}], [#journal_entry{type = begin_txn}, #journal_entry{type = commit_txn}]) ->
    [];
remove_existing_journal_entries_handoff(NewJournalEntryList, ExistingJournalEntryList) ->
    FinalBegin = remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, begin_txn),
    FinalPrepare = remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, prepare_txn),
    FinalCommit = remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, commit_txn),
    FinalAbort = remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, abort_txn),
    FinalCheckpoint = remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, checkpoint),
    FinalCheckpointCommits = remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, checkpoint_commit),
    NewObjectJs = get_updates(NewJournalEntryList),
    ExistingObjectJs = get_updates(ExistingJournalEntryList),
    ExistingTxOpNum = lists:map(fun(#journal_entry{args = #update_args{tx_op_num = TxOpNum}}) ->
        TxOpNum end, ExistingObjectJs),
    FinalObjectJs = lists:filter(fun(#journal_entry{args = #update_args{tx_op_num = TxOpNum}}) ->
        not lists:member(TxOpNum, ExistingTxOpNum) end, NewObjectJs),
    FinalBegin ++ FinalObjectJs ++ FinalPrepare ++ FinalCommit ++ FinalAbort ++ FinalCheckpoint ++ FinalCheckpointCommits.

-spec remove_existing_journal_entry_handoff([journal_entry()], [journal_entry()], journal_entry_type()) -> [journal_entry()].
remove_existing_journal_entry_handoff(NewJournalEntryList, ExistingJournalEntryList, SystemOperation) ->
    ExistingResult = get_journal_entries_of_type(ExistingJournalEntryList, SystemOperation),
    case length(ExistingResult) > 0 of
        true -> [];
        false -> get_journal_entries_of_type(NewJournalEntryList, SystemOperation)
    end.

-spec get_default_txn_tracking_num() -> txn_tracking_num().
get_default_txn_tracking_num() -> {0, none, 0}.

-spec generate_downstream_op(key_struct(), type_op(), txid(), atom()) ->
    {ok, downstream_op()} | {error, reason()}.
generate_downstream_op(KeyStruct = #key_struct{key = Key, type = Type}, TypeOp, TxId, TableName) ->
    %% TODO: Check if read can be omitted for some types as registers
    NeedState = Type:require_state_downstream(TypeOp),
    %% If state is needed to generate downstream, read it from the partition.
    case NeedState of
        true ->
            case gingko_log_utils:perform_tx_read(KeyStruct, TxId, TableName) of
                {ok, #snapshot{value = SnapshotValue}} ->
                    case Type of
                        antidote_crdt_counter_b ->
                            %% bcounter data-type.
                            bcounter_manager:generate_downstream(Key, TypeOp, SnapshotValue);
                        _ ->
                            Type:downstream(TypeOp, SnapshotValue)
                    end;
                {error, Reason} ->
                    {error, {gen_downstream_read_failed, Reason}}
            end;
        false ->
            Type:downstream(TypeOp, ignore) %Use a dummy value
    end.

-spec call_gingko_async(partition_id(), {atom(), atom()}, any()) -> ok.
call_gingko_async(Partition, {ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utils:call_vnode_async(Partition, VMaster, Request)
    end.

-spec call_gingko_sync(partition_id(), {atom(), atom()}, any()) -> any().
call_gingko_sync(Partition, {ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:call(ServerName, Request);
        false -> antidote_utils:call_vnode_sync(Partition, VMaster, Request)
    end.

-spec call_gingko_async_with_key(key(), {atom(), atom()}, any()) -> ok.
call_gingko_async_with_key(Key, {ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utils:call_vnode_async_with_key(Key, VMaster, Request)
    end.

-spec call_gingko_sync_with_key(key(), {atom(), atom()}, any()) -> any().
call_gingko_sync_with_key(Key, {ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:call(ServerName, Request);
        false -> antidote_utils:call_vnode_sync_with_key(Key, VMaster, Request)
    end.

-spec bcast_local_gingko_async({atom(), atom()}, any()) -> ok.
bcast_local_gingko_async({ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utils:bcast_local_vnode_async(VMaster, Request)
    end.

-spec bcast_local_gingko_sync({atom(), atom()}, any()) -> [{partition_id(), term()}].
bcast_local_gingko_sync({ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> [{0, gen_server:call(ServerName, Request)}];
        false -> antidote_utils:bcast_local_vnode_sync(VMaster, Request)
    end.

%% Sends the same (asynchronous) command to all vnodes of a given type.
-spec bcast_gingko_async({atom(), atom()}, any()) -> ok.
bcast_gingko_async({ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> gen_server:cast(ServerName, Request);
        false -> antidote_utils:bcast_vnode_async(VMaster, Request)
    end.

%% Sends the same (synchronous) command to all vnodes of a given type.
-spec bcast_gingko_sync({atom(), atom()}, any()) -> [{partition_id(), term()}].
bcast_gingko_sync({ServerName, VMaster}, Request) ->
    case ?USE_SINGLE_SERVER of
        true -> [{0, gen_server:call(ServerName, Request)}];
        false -> antidote_utils:bcast_vnode_sync(VMaster, Request)
    end.

-spec get_key_partition(term()) -> partition_id().
get_key_partition(Key) ->
    case ?USE_SINGLE_SERVER of
        true -> 0;
        false ->
            {Partition, _} = antidote_utils:get_key_partition_node_tuple(Key),
            Partition
    end.


