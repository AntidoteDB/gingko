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

-module(gingko_cache_vnode).
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

-type cache_dict() :: dict:dict(key_struct(), [cache_entry()]).

%TODO think of default values
-record(state, {
    partition :: partition_id(),
    table_name :: atom(),
    key_cache_entry_dict = dict:new() :: cache_dict(), %TODO double dict for optimization later
    max_occupancy = 100 :: non_neg_integer(),
    reset_used_interval_millis = 1000 :: non_neg_integer(),
    reset_used_timer = none :: none | reference(),
    eviction_interval_millis = 1000 :: non_neg_integer(),
    eviction_timer = none :: none | reference(),
    eviction_threshold_in_percent = 90 :: 0..100, %TODO values above 100 are simply 100
    target_threshold_in_percent = 80 :: 0..100, %TODO think about this one (currently based on the eviction threshold)
    eviction_strategy = interval :: interval | fifo | lru | lfu
    %TODO decide on parameters
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

%%%===================================================================
%%% Spawning and vnode implementation
%%%===================================================================

-spec start_vnode(integer()) -> any().
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Opens the persistent copy of the Log.
%%      The name of the Log in disk is a combination of the the word
%%      `log' and the partition identifier.
init([Partition]) ->
    default_vnode_behaviour:init(?MODULE, [Partition]),
    TableName = general_utils:concat_and_make_atom([integer_to_list(Partition), '_journal_entry']),
    CacheConfig = [{partition, Partition}, {table_name, TableName} | gingko_app:get_default_config()],
    NewState = apply_gingko_config(#state{}, CacheConfig),
    {ok, NewState#state{key_cache_entry_dict = dict:new()}}.

handle_command(Request = hello, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {reply, ok, State};

handle_command(Request = {get, KeyStruct, DependencyVts}, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {Reply, NewState} = get(KeyStruct, DependencyVts, State),
    {reply, Reply, NewState};

handle_command(Request = {update_cache_config, CacheConfig}, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {Reply, NewState} = {ok, apply_gingko_config(State, CacheConfig)},
    {reply, Reply, NewState};

handle_command(Request = {checkpoint_cache_cleanup, CheckpointVts}, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {Reply, NewState} = {ok, clean_up_cache_after_checkpoint(State, CheckpointVts)},
    {reply, Reply, NewState};

handle_command(Request, Sender, State) -> default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State).
handoff_starting(TargetNode, State) -> default_vnode_behaviour:handoff_starting(?MODULE, TargetNode, State).
handoff_cancelled(State) -> default_vnode_behaviour:handoff_cancelled(?MODULE, State).
handoff_finished(TargetNode, State) -> default_vnode_behaviour:handoff_finished(?MODULE, TargetNode, State).

handle_handoff_command(Request = #riak_core_fold_req_v2{foldfun = VisitFun, acc0 = Acc0}, Sender, State) ->
    %%TODO
    default_vnode_behaviour:handle_handoff_command(?MODULE, Request, Sender, State),
    {reply, ok, State};

handle_handoff_command(Request, Sender, State) -> default_vnode_behaviour:handle_handoff_command(?MODULE, Request, Sender, State).

handle_handoff_data(BinaryData, State) ->
    default_vnode_behaviour:handle_handoff_data(?MODULE, BinaryData, State),
    %%TODO
    {reply, ok, State}.

encode_handoff_item(Key, Value) -> default_vnode_behaviour:encode_handoff_item(?MODULE, Key, Value).

is_empty(State) ->
    default_vnode_behaviour:is_empty(?MODULE, State),
    %%TODO
    {true, State}.

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

-spec apply_gingko_config(state(), map_list()) -> state().
apply_gingko_config(State, GingkoConfig) ->
    Partition = general_utils:get_or_default_map_list(partition, GingkoConfig, error),
    TableName = general_utils:get_or_default_map_list(table_name, GingkoConfig, error),
    MaxOccupancy = general_utils:get_or_default_map_list(max_occupancy, GingkoConfig, State#state.max_occupancy),
    EvictionStrategy = general_utils:get_or_default_map_list(eviction_strategy, GingkoConfig, State#state.eviction_strategy),
    {UpdateResetUsedTimer, UsedResetIntervalMillis} =
        general_utils:get_or_default_map_list_check(reset_used_interval_millis, GingkoConfig, State#state.reset_used_interval_millis),
    {UpdateEvictionTimer, EvictionIntervalMillis} =
        general_utils:get_or_default_map_list_check(eviction_interval_millis, GingkoConfig, State#state.eviction_interval_millis),
    NewState = State#state{partition = Partition, table_name = TableName, max_occupancy = MaxOccupancy, reset_used_interval_millis = UsedResetIntervalMillis, eviction_interval_millis = EvictionIntervalMillis, eviction_strategy = EvictionStrategy},
    update_timers(NewState, UpdateResetUsedTimer, UpdateEvictionTimer).

%%TODO implement correctly
-spec update_timers(state(), boolean(), boolean()) -> state().
update_timers(State, UpdateResetUsedTimer, UpdateEvictionTimer) ->
    TimerResetUsed =
%%        case State#state.reset_used_timer of
%%            none ->
%%                erlang:send_after(State#state.reset_used_interval_millis, self(), reset_used_event);
%%            Reference1 ->
%%                case UpdateResetUsedTimer of
%%                    true ->
%%                        erlang:cancel_timer(Reference1),
%%                        erlang:send_after(State#state.reset_used_interval_millis, self(), reset_used_event);
%%                    false -> Reference1
%%                end
%%        end,
    TimerEviction = none,
%%        case State#state.eviction_timer of
%%            none ->
%%                erlang:send_after(State#state.eviction_interval_millis, self(), eviction_event);
%%            Reference2 ->
%%                case UpdateEvictionTimer of
%%                    true -> erlang:cancel_timer(Reference2),
%%                        erlang:send_after(State#state.eviction_interval_millis, self(), eviction_event);
%%                    false -> Reference2
%%                end
%%        end,
    State#state{reset_used_timer = TimerResetUsed, eviction_timer = TimerEviction}.

-spec get(key_struct(), vectorclock(), state()) -> {{ok, snapshot()}, state()} | {{error, reason()}, state()}.
get(KeyStruct, DependencyVts, State) ->
    Result = get_or_load_cache_entry(KeyStruct, DependencyVts, State, false, false),
    get_internal(Result, KeyStruct, DependencyVts).

-spec get_internal({{ok, [cache_entry()], boolean()}, key_struct(), state()} | {{error, Reason}, state()}, key_struct(), vectorclock()) -> {{ok, snapshot()}, state()} | {{error, Reason}, state()}.
get_internal(Result, KeyStruct, DependencyVts) ->
    case Result of
        {{error, Reason}, State} -> {{error, Reason}, State};
        {{ok, CacheEntries, CacheUpdated}, State} ->
            MatchingEntries = lists:filter(
                fun(C) ->
                    gingko_utils:is_in_vts_range(DependencyVts, {C#cache_entry.commit_vts, C#cache_entry.valid_vts})
                %%TODO check: this should also mean visible
                end, CacheEntries),
            case MatchingEntries of
                [] ->
                    case CacheUpdated of
                        true ->
                            {{error, "Bad Cache Update"}, State};
                        false ->
                            NewResult = get_or_load_cache_entry(KeyStruct, DependencyVts, State, false, true),
                            get_internal(NewResult, KeyStruct, DependencyVts)
                    end;
                [C] ->
                    UpdatedCacheEntry = gingko_utils:update_cache_usage(C, true),
                    {{ok, gingko_utils:create_snapshot_from_cache_entry(UpdatedCacheEntry)}, State};
                _Multiple ->
                    {{error, "Multiple cache entries with the same key and commit vts exist which should not happen!"}, State}
            end
    end.

-spec get_or_load_cache_entry(key_struct(), vectorclock(), state(), boolean(), boolean()) -> {{ok, [cache_entry()], boolean()}, state()} | {{error, reason()}, state()}.
get_or_load_cache_entry(KeyStruct, DependencyVts, State, CacheUpdated, ForceUpdate) ->
    case ForceUpdate of
        true ->
            NewState = load_key_into_cache(KeyStruct, State, DependencyVts),
            get_or_load_cache_entry(KeyStruct, DependencyVts, NewState, true, false);
        false ->
            FoundCacheEntries = dict:find(KeyStruct, State#state.key_cache_entry_dict),
            case FoundCacheEntries of
                error ->
                    case CacheUpdated of
                        true -> {{error, "Bad Cache Update"}, State};
                        false ->
                            get_or_load_cache_entry(KeyStruct, DependencyVts, State, false, true)
                    end;
                {ok, CacheEntries} ->
                    {{ok, CacheEntries, CacheUpdated}, State}
            end
    end.

-spec load_key_into_cache(key_struct(), state(), vectorclock()) -> state().
load_key_into_cache(KeyStruct, State, DependencyVts) ->
    SortedJournalEntries = gingko_log_utils:read_all_journal_entries_sorted(State#state.table_name),
    CheckpointJournalEntries = lists:filter(fun(J) ->
        gingko_utils:is_system_operation(J, checkpoint) end, lists:reverse(SortedJournalEntries)),
    {MostRecentSnapshot, NewState} =
        case CheckpointJournalEntries of
            [] ->
                CS1 = gingko_utils:create_new_snapshot(KeyStruct, vectorclock:new()),
                {CS1, update_cache_entry_in_state(gingko_utils:create_cache_entry(CS1), State)};
            [LastCheckpointJournalEntry | _Js] ->
                FoundCacheEntries = dict:find(KeyStruct, State#state.key_cache_entry_dict),
                CheckpointVts = LastCheckpointJournalEntry#journal_entry.operation#system_operation.op_args#checkpoint_args.dependency_vts,
                case FoundCacheEntries of
                    error ->
                        CS2 = gingko_log_utils:read_checkpoint_entry(KeyStruct, CheckpointVts),
                        {CS2, update_cache_entry_in_state(gingko_utils:create_cache_entry(CS2), State)};
                    {ok, CacheEntries} ->
                        ValidCacheEntries =
                            lists:filter(
                                fun(C) ->
                                    gingko_utils:is_in_vts_range(C#cache_entry.commit_vts, {none, DependencyVts}) andalso
                                        gingko_utils:is_in_vts_range(C#cache_entry.valid_vts, {CheckpointVts, DependencyVts})
                                end, CacheEntries),
                        case ValidCacheEntries of
                            [] ->
                                CS3 = gingko_log_utils:read_checkpoint_entry(KeyStruct, CheckpointVts),
                                {CS3, update_cache_entry_in_state(gingko_utils:create_cache_entry(CS3), State)};
                            [ValidCacheEntry | _FoundValidCacheEntries] ->
                                CS4 = gingko_utils:create_snapshot_from_cache_entry(ValidCacheEntry),
                                {CS4, State}
                        end
                end
        end,
    {ok, Snapshot} = gingko_materializer:materialize_snapshot(MostRecentSnapshot, SortedJournalEntries, DependencyVts),
    CacheEntry = gingko_utils:create_cache_entry(Snapshot),
    update_cache_entry_in_state(CacheEntry, NewState).

-spec update_cache_entry_in_state(cache_entry(), state()) -> state().
update_cache_entry_in_state(CacheEntry, State) ->
    KeyStruct = CacheEntry#cache_entry.key_struct,
    CommitVts = CacheEntry#cache_entry.commit_vts,
    ValidVts = CacheEntry#cache_entry.valid_vts,
    CacheDict = State#state.key_cache_entry_dict,
    CacheEntryList = general_utils:get_or_default_dict(CacheDict, CacheEntry#cache_entry.key_struct, []),
    MatchingEntries = lists:filter(fun(C) -> C#cache_entry.commit_vts == CommitVts end, CacheEntryList),
    UpdateNecessary =
        case MatchingEntries of
            [] -> true;
            [C] -> gingko_utils:is_in_vts_range(ValidVts, {C#cache_entry.valid_vts, none});
            _Multiple ->
                logger:error("Multiple cache entries with the same key and commit vts exist which should not happen!~nExisting Cache Entries:~n~p~nCache Entry Update:~n~p~n", [CacheEntryList, CacheEntry]),
                true
        end,
    case UpdateNecessary of
        true ->
            NewCacheList = [CacheEntry | lists:filter(fun(C) ->
                C#cache_entry.commit_vts /= CommitVts end, CacheEntryList)],
            NewCacheDict = dict:store(KeyStruct, NewCacheList, CacheDict),
            State#state{key_cache_entry_dict = NewCacheDict};
        false ->
            logger:error("Unnecessary cache update!~nExisting Cache Entries:~n~p~nCache Entry Update:~n~p~n", [CacheEntryList, CacheEntry]),
            State
    end.

-spec start_eviction_process(state()) -> state().
start_eviction_process(State) ->
    MaxOccupancy = State#state.max_occupancy,
    CurrentOccupancy =
        dict:fold(fun(_Key, CList, Number) -> Number + length(CList) end, 0, State#state.key_cache_entry_dict),
    EvictionThreshold = State#state.eviction_threshold_in_percent * MaxOccupancy div 100,
    TargetThreshold = State#state.target_threshold_in_percent * EvictionThreshold div 100,
    EvictionNeeded = CurrentOccupancy > EvictionThreshold,
    CacheDict = State#state.key_cache_entry_dict,
    NewCacheDict =
        case {EvictionNeeded, State#state.eviction_strategy} of
            {true, interval} -> %No preference on cache entries
                interval_evict(CacheDict, CurrentOccupancy, TargetThreshold);
            {true, fifo} ->
                fifo_evict(CacheDict, CurrentOccupancy, TargetThreshold);
            {true, lru} ->
                lru_evict(CacheDict, CurrentOccupancy, TargetThreshold);
            {true, lfu} ->
                lfu_evict(CacheDict, CurrentOccupancy, TargetThreshold);
            {false, _} ->
                CacheDict
        end,
    State#state{key_cache_entry_dict = NewCacheDict}.

-spec interval_evict(cache_dict(), non_neg_integer(), non_neg_integer()) -> cache_dict().
interval_evict(CacheDict, CurrentOccupancy, TargetThreshold) ->
    {RemainingOccupancy, NewUsedCacheDict} =
        dict:fold(
            fun(Key, CList, {Occupancy, NewCacheDictAcc}) ->
                {NewOccupancy, ListOfCacheEntries} =
                    lists:foldl(
                        fun(C, {InnOcc, CL}) ->
                            case InnOcc =< TargetThreshold orelse C#cache_entry.usage#cache_usage.used of
                                true -> {InnOcc, [C | CL]};
                                false -> {InnOcc - 1, CL}
                            end
                        end, {Occupancy, []}, CList),
                case ListOfCacheEntries of
                    [] -> {NewOccupancy, NewCacheDictAcc};
                    List -> {NewOccupancy, dict:store(Key, List, NewCacheDictAcc)}
                end
            end, {CurrentOccupancy, dict:new()}, CacheDict),
    case RemainingOccupancy >= TargetThreshold of
        true ->
            lru_evict(reset_used(NewUsedCacheDict, 0), RemainingOccupancy, TargetThreshold); %TODO default lru
        false -> NewUsedCacheDict
    end.

-spec fifo_evict(cache_dict(), non_neg_integer(), non_neg_integer()) -> cache_dict().
fifo_evict(CacheDict, CurrentOccupancy, TargetThreshold) ->
    ValueList = lists:flatten(lists:map(fun({_Key, Value}) -> Value end, dict:to_list(CacheDict))),
    SortedByFirstUsage = lists:sort(fun(C1, C2) ->
        C1#cache_entry.usage#cache_usage.first_used < C2#cache_entry.usage#cache_usage.first_used end, ValueList),
    {RemainingOccupancy, NewUsedCacheDict} =
        lists:foldl(
            fun(C, {OccIn, Dict}) ->
                case OccIn =< TargetThreshold orelse C#cache_entry.usage#cache_usage.used of
                    true ->
                        {OccIn, general_utils:add_to_value_list_or_create_single_value_list(C#cache_entry.key_struct, C, Dict)};
                    false -> {OccIn - 1, Dict}
                end
            end, {CurrentOccupancy, dict:new()}, SortedByFirstUsage),
    case RemainingOccupancy >= TargetThreshold of
        true ->
            fifo_evict(reset_used(NewUsedCacheDict, 0), RemainingOccupancy, TargetThreshold);
        false -> NewUsedCacheDict
    end.

-spec lru_evict(cache_dict(), non_neg_integer(), non_neg_integer()) -> cache_dict().
lru_evict(CacheDict, CurrentOccupancy, TargetThreshold) ->
    ValueList = lists:flatten(lists:map(fun({_Key, Value}) -> Value end, dict:to_list(CacheDict))),
    SortedByFirstUsage = lists:sort(fun(C1, C2) ->
        C1#cache_entry.usage#cache_usage.last_used < C2#cache_entry.usage#cache_usage.last_used end, ValueList),
    {RemainingOccupancy, NewUsedCacheDict} =
        lists:foldl(
            fun(C, {OccIn, Dict}) ->
                case OccIn =< TargetThreshold orelse C#cache_entry.usage#cache_usage.used of
                    true ->
                        {OccIn, general_utils:add_to_value_list_or_create_single_value_list(C#cache_entry.key_struct, C, Dict)};
                    false -> {OccIn - 1, Dict}
                end
            end, {CurrentOccupancy, dict:new()}, SortedByFirstUsage),
    case RemainingOccupancy >= TargetThreshold of
        true ->
            lru_evict(reset_used(NewUsedCacheDict, 0), RemainingOccupancy, TargetThreshold);
        false -> NewUsedCacheDict
    end.

-spec lfu_evict(cache_dict(), non_neg_integer(), non_neg_integer()) -> cache_dict().
lfu_evict(CacheDict, CurrentOccupancy, TargetThreshold) ->
    ValueList = lists:flatten(lists:map(fun({_Key, Value}) -> Value end, dict:to_list(CacheDict))),
    SortedByFirstUsage = lists:sort(fun(C1, C2) ->
        C1#cache_entry.usage#cache_usage.times_used < C2#cache_entry.usage#cache_usage.times_used end, ValueList),
    {RemainingOccupancy, NewUsedCacheDict} =
        lists:foldl(
            fun(C, {OccIn, Dict}) ->
                case OccIn =< TargetThreshold orelse C#cache_entry.usage#cache_usage.used of
                    true ->
                        {OccIn, general_utils:add_to_value_list_or_create_single_value_list(C#cache_entry.key_struct, C, Dict)};
                    false -> {OccIn - 1, Dict}
                end
            end, {CurrentOccupancy, dict:new()}, SortedByFirstUsage),
    case RemainingOccupancy >= TargetThreshold of
        true ->
            lfu_evict(reset_used(NewUsedCacheDict, 0), RemainingOccupancy, TargetThreshold);
        false -> NewUsedCacheDict
    end.

-spec reset_used(state()) -> state().
reset_used(State) ->
    ResetInterval = State#state.reset_used_interval_millis * 1000,
    CacheDict = State#state.key_cache_entry_dict,
    NewCacheDict = reset_used(CacheDict, ResetInterval),
    State#state{key_cache_entry_dict = NewCacheDict}.

-spec reset_used(cache_dict(), non_neg_integer()) -> cache_dict().
reset_used(CacheDict, ResetInterval) ->
    CurrentTime = gingko_utils:get_timestamp(),
    MatchTime = CurrentTime - ResetInterval,
    dict:map(
        fun(_Key, CList) ->
            lists:map(
                fun(C) ->
                    case C#cache_entry.usage#cache_usage.last_used < MatchTime of
                        true -> gingko_utils:update_cache_usage(C, false);
                        false -> C
                    end
                end, CList)
        end, CacheDict).

-spec clean_up_cache_after_checkpoint(state(), vectorclock()) -> state().
clean_up_cache_after_checkpoint(State, LastCheckpointVts) ->
    CacheDict = State#state.key_cache_entry_dict,
    NewCacheDict =
        dict:fold(
            fun(Key, CList, NewCacheDictAcc) ->
                ListOfCacheEntries =
                    lists:filter(
                        fun(C) ->
                            gingko_utils:is_in_vts_range(C#cache_entry.valid_vts, {LastCheckpointVts, none})
                        end, CList),
                case ListOfCacheEntries of
                    [] -> NewCacheDictAcc;
                    List -> dict:store(Key, List, NewCacheDictAcc)
                end
            end, dict:new(), CacheDict),
    State#state{key_cache_entry_dict = NewCacheDict}.
