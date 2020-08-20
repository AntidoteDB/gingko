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

-type cache_map() :: #{key_struct() => #{vectorclock() => cache_entry()}}.

%TODO think of default values
-record(state, {
    partition = 0 :: partition(),
    key_cache_entry_map = #{} :: cache_map(), %TODO double map for optimization later
    reset_used_timer = none :: none | reference(),
    eviction_timer = none :: none | reference()
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
    NewState = reset_timers(#state{partition = Partition}),
    {ok, NewState}.

handle_command(Request = hello, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {reply, ok, State};

handle_command(Request = {get, KeyStruct, DependencyVts}, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {Reply, NewState} = get_internal(KeyStruct, DependencyVts, load_from_log, State),
    {reply, Reply, NewState};

handle_command(Request = {get, KeyStruct, DependencyVts, ValidJournalEntries}, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {Reply, NewState} = get_internal(KeyStruct, DependencyVts, ValidJournalEntries, State),
    {reply, Reply, NewState};

handle_command(Request = reset_cache_timers, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {reply, ok, reset_timers(State)};

handle_command(Request = {checkpoint_cache_cleanup, CheckpointVts}, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {reply, ok, clean_up_cache_after_checkpoint(State, CheckpointVts)};

handle_command(Request = reset_used_event, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {reply, ok, reset_used(State)};

handle_command(Request = eviction_event, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {reply, ok, start_eviction_process(State)};

handle_command(Request, Sender, State) -> default_vnode_behaviour:handle_command_crash(?MODULE, Request, Sender, State).
handoff_starting(TargetNode, State) -> default_vnode_behaviour:handoff_starting(?MODULE, TargetNode, State).
handoff_cancelled(State) -> default_vnode_behaviour:handoff_cancelled(?MODULE, State).
handoff_finished(TargetNode, State) -> default_vnode_behaviour:handoff_finished(?MODULE, TargetNode, State).
handle_handoff_command(Request, Sender, State) ->
    default_vnode_behaviour:handle_handoff_command(?MODULE, Request, Sender, State).
handle_handoff_data(BinaryData, State) -> default_vnode_behaviour:handle_handoff_data(?MODULE, BinaryData, State).
encode_handoff_item(Key, Value) -> default_vnode_behaviour:encode_handoff_item(?MODULE, Key, Value).
is_empty(State) -> default_vnode_behaviour:is_empty(?MODULE, State).
terminate(Reason, State) -> default_vnode_behaviour:terminate(?MODULE, Reason, State).
delete(State) -> default_vnode_behaviour:delete(?MODULE, State).

handle_info(Request, State) ->
    default_vnode_behaviour:handle_info(?MODULE, Request, State),
    handle_command(Request, {raw, undefined, undefined}, State).

handle_exit(Pid, Reason, State) -> default_vnode_behaviour:handle_exit(?MODULE, Pid, Reason, State).
handle_coverage(Request, KeySpaces, Sender, State) ->
    default_vnode_behaviour:handle_coverage(?MODULE, Request, KeySpaces, Sender, State).
handle_overload_command(Request, Sender, Partition) ->
    default_vnode_behaviour:handle_overload_command(?MODULE, Request, Sender, Partition).
handle_overload_info(Request, Partition) -> default_vnode_behaviour:handle_overload_info(?MODULE, Request, Partition).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec reset_timers(state()) -> state().
reset_timers(State = #state{reset_used_timer = CurrentResetUsedTimer, eviction_timer = CurrentEvictionTimer}) ->
    ResetUsedIntervalMillis = gingko_env_utils:get_cache_reset_used_interval_millis(),
    EvictionIntervalMillis = gingko_env_utils:get_cache_eviction_interval_millis(),
    NewResetUsedTimer = gingko_dc_utils:update_timer(CurrentResetUsedTimer, true, ResetUsedIntervalMillis, reset_used_event, true),
    NewEvictionTimer = gingko_dc_utils:update_timer(CurrentEvictionTimer, true, EvictionIntervalMillis, eviction_event, true),
    State#state{reset_used_timer = NewResetUsedTimer, eviction_timer = NewEvictionTimer}.

-spec get_internal(key_struct(), vectorclock(), load_from_log | [journal_entry()], state()) -> {{ok, snapshot()}, state()} | {{error, reason()}, state()}.
get_internal(KeyStruct, DependencyVts, ValidJournalEntryListOrLoadFromLog, State) ->
    GetResult = get_or_load_cache_entry(KeyStruct, DependencyVts, ValidJournalEntryListOrLoadFromLog, State),
    case GetResult of
        {ok, #cache_entry{snapshot = Snapshot}, NewState} -> {{ok, Snapshot}, NewState};
        Error -> {Error, State}
    end.

-spec get_or_load_cache_entry(key_struct(), vectorclock(), load_from_log | [journal_entry()], state()) -> {ok, cache_entry(), state()} | {error, reason()}.
get_or_load_cache_entry(KeyStruct, DependencyVts, ValidJournalEntryListOrLoadFromLog, State = #state{key_cache_entry_map = KeyCacheEntryMap}) ->
    FoundCommitVtsCacheEntryMap = maps:find(KeyStruct, KeyCacheEntryMap),
    case FoundCommitVtsCacheEntryMap of
        error ->
            logger:debug("Cache Miss!"),
            load_key_into_cache(KeyStruct, DependencyVts, ValidJournalEntryListOrLoadFromLog, State);
        {ok, CommitVtsCacheEntryMap} ->
            MatchingCacheEntryList =
                lists:filter(
                    fun(#cache_entry{snapshot = #snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts}}) ->
                        vectorclock:le(CommitVts, DependencyVts) andalso vectorclock:ge(SnapshotVts, DependencyVts)
                    end, maps:values(CommitVtsCacheEntryMap)),
            case MatchingCacheEntryList of
                [] ->
                    logger:debug("Cache Miss!"), %%TODO metrics (maybe at least count hit and miss in state)
                    load_key_into_cache(KeyStruct, DependencyVts, ValidJournalEntryListOrLoadFromLog, State);
                [CacheEntry | _] ->
                    logger:debug("Cache Hit!"),
                    UpdatedCacheEntry = gingko_utils:update_cache_usage(CacheEntry, true),
                    {ok, UpdatedCacheEntry, update_cache_entry_in_state(UpdatedCacheEntry, State)}
            end
    end.

-spec load_key_into_cache(key_struct(), vectorclock(), load_from_log | [journal_entry()], state()) -> {ok, cache_entry(), state()} | {error, reason()}.
load_key_into_cache(KeyStruct, DependencyVts, ValidJournalEntryListOrLoadFromLog, State = #state{partition = Partition, key_cache_entry_map = KeyCacheEntryMap}) ->
    JournalEntryListResult =
        case ValidJournalEntryListOrLoadFromLog of
            load_from_log ->
                gingko_dc_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {get_valid_journal_entries, DependencyVts});
            ValidJournalEntryList -> {ok, ValidJournalEntryList}
        end,
    case JournalEntryListResult of
        {ok, JournalEntryList} ->
            CheckpointJournalEntryList = gingko_utils:get_journal_entries_of_type(JournalEntryList, checkpoint_commit),
            ReverseVtsSortedCheckpointJournalEntryList = lists:reverse(gingko_utils:sort_same_journal_entry_type_list_by_vts( CheckpointJournalEntryList)),
            CheckpointVtsOrNone =
                case ReverseVtsSortedCheckpointJournalEntryList of
                    [] -> none;
                    [#journal_entry{args = #checkpoint_args{dependency_vts = CheckpointVts}} | _] -> CheckpointVts
                end,
            FoundCommitVtsCacheEntryMap = maps:find(KeyStruct, KeyCacheEntryMap),
            {ReadCheckpoint, MostRecentSnapshot} =
                case {CheckpointVtsOrNone, FoundCommitVtsCacheEntryMap} of
                    {none, error} ->
                        {false, gingko_utils:create_new_snapshot(KeyStruct, vectorclock:new())};
                    {CheckpointVts1, error} ->
                        {true, gingko_log_utils:read_checkpoint_entry(KeyStruct, CheckpointVts1)};
                    {_, {ok, CommitVtsCacheEntryMap}} ->
                        ValidCommitVtsCacheEntryList =
                            maps:to_list(maps:filter(
                                fun(FoundCommitVts, #cache_entry{snapshot = #snapshot{commit_vts = FoundCommitVts, snapshot_vts = FoundSnapshotVts}}) ->
                                    vectorclock:le(FoundCommitVts, DependencyVts)
                                        andalso vectorclock:ge(FoundSnapshotVts, CheckpointVtsOrNone)
                                end, CommitVtsCacheEntryMap)),
                        case {CheckpointVtsOrNone, ValidCommitVtsCacheEntryList} of
                            {none, []} ->
                                {false, gingko_utils:create_new_snapshot(KeyStruct, vectorclock:new())};
                            {CheckpointVts2, []} ->
                                {true, gingko_log_utils:read_checkpoint_entry(KeyStruct, CheckpointVts2)};
                            {_, [{_, ValidCacheEntry} | _]} ->
                                %%TODO can be optimized by picking the best commit vts
                                {false, gingko_utils:create_snapshot_from_cache_entry(ValidCacheEntry)}
                        end
                end,
            %%TODO This is a optimization as we want to avoid reading checkpoints if possible since we don't know the performance characteristics later
            NewState =
                case ReadCheckpoint of
                    true ->
                        update_cache_entry_in_state(gingko_utils:create_cache_entry(MostRecentSnapshot), State);
                    false -> State
                end,
            UpdatedSnapshot = gingko_materializer:materialize_snapshot(MostRecentSnapshot, JournalEntryList, DependencyVts),
            ReturnCacheEntry = gingko_utils:create_cache_entry(UpdatedSnapshot),
            ReturnState = update_cache_entry_in_state(ReturnCacheEntry, NewState),
            {ok, ReturnCacheEntry, ReturnState};
        Error -> Error
    end.

-spec update_cache_entry_in_state(cache_entry(), state()) -> state().
update_cache_entry_in_state(CacheEntry = #cache_entry{snapshot = #snapshot{key_struct = KeyStruct, commit_vts = CommitVts, snapshot_vts = SnapshotVts}, usage = NewUsage}, State = #state{key_cache_entry_map = KeyCacheEntryMap}) ->
    CommitVtsToCacheEntryMap = maps:get(KeyStruct, KeyCacheEntryMap, #{}),
    MatchingCacheEntryResult = maps:find(CommitVts, CommitVtsToCacheEntryMap),
    UpdateNecessary =
        case MatchingCacheEntryResult of
            {ok, #cache_entry{snapshot = #snapshot{snapshot_vts = FoundSnapshotVts}, usage = ExistingUsage}} ->
                NewUsage /= ExistingUsage
                    orelse vectorclock:gt(SnapshotVts, FoundSnapshotVts);
            _ ->
                true
        end,
    case UpdateNecessary of
        true ->
            NewCommitVtsToCacheEntryMap = CommitVtsToCacheEntryMap#{CommitVts => CacheEntry},
            NewKeyCacheEntryMap = KeyCacheEntryMap#{KeyStruct => NewCommitVtsToCacheEntryMap},
            State#state{key_cache_entry_map = NewKeyCacheEntryMap};
        false ->
            State %%This should not really happen
    end.

-spec start_eviction_process(state()) -> state().
start_eviction_process(State = #state{key_cache_entry_map = KeyCacheEntryMap}) ->
    MaxOccupancy = gingko_env_utils:get_cache_max_occupancy(),
    EvictionThresholdInPercent = gingko_env_utils:get_cache_target_threshold_in_percent(),
    TargetThresholdInPercent = gingko_env_utils:get_cache_target_threshold_in_percent(),
    EvictionStrategy = gingko_env_utils:get_cache_eviction_strategy(),
    CurrentOccupancy =
        maps:fold(fun(_Key, CommitVtsCacheEntryMap, Number) ->
            Number + maps:size(CommitVtsCacheEntryMap) end, 0, KeyCacheEntryMap),
    EvictionThreshold = EvictionThresholdInPercent * MaxOccupancy div 100,
    TargetThreshold = TargetThresholdInPercent * EvictionThreshold div 100,
    EvictionNeeded = CurrentOccupancy > EvictionThreshold,
    NewCacheMap =
        case {EvictionNeeded, EvictionStrategy} of
            {true, interval} -> %No preference on cache entries
                interval_evict(KeyCacheEntryMap, CurrentOccupancy, TargetThreshold);
            {true, fifo} ->
                fifo_evict(KeyCacheEntryMap, CurrentOccupancy, TargetThreshold);
            {true, lru} ->
                lru_evict(KeyCacheEntryMap, CurrentOccupancy, TargetThreshold);
            {true, lfu} ->
                lfu_evict(KeyCacheEntryMap, CurrentOccupancy, TargetThreshold);
            {false, _} ->
                KeyCacheEntryMap
        end,
    State#state{key_cache_entry_map = NewCacheMap}.

-spec interval_evict(cache_map(), non_neg_integer(), non_neg_integer()) -> cache_map().
interval_evict(KeyCacheEntryMap, CurrentOccupancy, TargetThreshold) ->
    CacheEntryList = get_cache_entry_list(KeyCacheEntryMap),
    evict(CacheEntryList, CurrentOccupancy, TargetThreshold, fun interval_evict/3).

-spec fifo_evict(cache_map(), non_neg_integer(), non_neg_integer()) -> cache_map().
fifo_evict(KeyCacheEntryMap, CurrentOccupancy, TargetThreshold) ->
    CacheEntryList = get_cache_entry_list(KeyCacheEntryMap),
    SortedCacheEntryList = sort_by_first_used(CacheEntryList),
    evict(SortedCacheEntryList, CurrentOccupancy, TargetThreshold, fun fifo_evict/3).

-spec lru_evict(cache_map(), non_neg_integer(), non_neg_integer()) -> cache_map().
lru_evict(KeyCacheEntryMap, CurrentOccupancy, TargetThreshold) ->
    CacheEntryList = get_cache_entry_list(KeyCacheEntryMap),
    SortedCacheEntryList = sort_by_last_used(CacheEntryList),
    evict(SortedCacheEntryList, CurrentOccupancy, TargetThreshold, fun lru_evict/3).

-spec lfu_evict(cache_map(), non_neg_integer(), non_neg_integer()) -> cache_map().
lfu_evict(KeyCacheEntryMap, CurrentOccupancy, TargetThreshold) ->
    CacheEntryList = get_cache_entry_list(KeyCacheEntryMap),
    SortedCacheEntryList = sort_by_times_used(CacheEntryList),
    evict(SortedCacheEntryList, CurrentOccupancy, TargetThreshold, fun lfu_evict/3).

evict(SortedCacheEntryList, CurrentOccupancy, TargetThreshold, RecursionFun) ->
    {RemainingOccupancy, NewUsedKeyCacheEntryMap} =
        lists:foldl(
            fun(CacheEntry = #cache_entry{snapshot = #snapshot{key_struct = KeyStruct, commit_vts = CommitVts}, usage = #cache_usage{used = Used}}, {OccupancyAcc, CurrentKeyCacheEntryMap}) ->
                case OccupancyAcc =< TargetThreshold orelse Used of
                    true ->
                        CurrentCommitVtsCacheEntryMap = maps:get(KeyStruct, CurrentKeyCacheEntryMap, #{}),
                        {OccupancyAcc, CurrentKeyCacheEntryMap#{KeyStruct => CurrentCommitVtsCacheEntryMap#{CommitVts => CacheEntry}}};
                    false -> {OccupancyAcc - 1, CurrentKeyCacheEntryMap}
                end
            end, {CurrentOccupancy, #{}}, SortedCacheEntryList),
    case RemainingOccupancy >= TargetThreshold of
        true ->
            RecursionFun(reset_used(NewUsedKeyCacheEntryMap, 0), RemainingOccupancy, TargetThreshold);
        false -> NewUsedKeyCacheEntryMap
    end.

get_cache_entry_list(KeyCacheEntryMap) ->
    lists:append(lists:map(fun(CommitVtsCacheEntryMap) ->
        maps:values(CommitVtsCacheEntryMap) end, maps:values(KeyCacheEntryMap))).

sort_by_first_used(CacheEntryList) ->
    lists:sort(
        fun(#cache_entry{usage = #cache_usage{first_used = FirstUsed1}}, #cache_entry{usage = #cache_usage{first_used = FirstUsed2}}) ->
            FirstUsed1 < FirstUsed2
        end, CacheEntryList).

sort_by_last_used(CacheEntryList) ->
    lists:sort(
        fun(#cache_entry{usage = #cache_usage{last_used = LastUsed1}}, #cache_entry{usage = #cache_usage{last_used = LastUsed2}}) ->
            LastUsed1 < LastUsed2
        end, CacheEntryList).

sort_by_times_used(CacheEntryList) ->
    lists:sort(
        fun(#cache_entry{usage = #cache_usage{times_used = TimesUsed1}}, #cache_entry{usage = #cache_usage{times_used = TimesUsed2}}) ->
            TimesUsed1 < TimesUsed2
        end, CacheEntryList).

-spec reset_used(state()) -> state().
reset_used(State = #state{key_cache_entry_map = KeyCacheEntryMap}) ->
    ResetUsedIntervalMillis = gingko_env_utils:get_cache_reset_used_interval_millis(),
    ResetInterval = ResetUsedIntervalMillis * 1000,
    NewKeyCacheEntryMap = reset_used(KeyCacheEntryMap, ResetInterval),
    State#state{key_cache_entry_map = NewKeyCacheEntryMap}.

-spec reset_used(cache_map(), non_neg_integer()) -> cache_map().
reset_used(KeyCacheEntryMap, ResetInterval) ->
    CurrentTime = gingko_dc_utils:get_timestamp(),
    MatchTime = CurrentTime - ResetInterval,
    maps:map(
        fun(_KeyStruct, CommitVtsCacheEntryMap) ->
            maps:map(
                fun(_, CacheEntry = #cache_entry{usage = #cache_usage{last_used = LastUsed}}) ->
                    case LastUsed < MatchTime of
                        true -> gingko_utils:update_cache_usage(CacheEntry, false);
                        false -> CacheEntry
                    end
                end, CommitVtsCacheEntryMap)
        end, KeyCacheEntryMap).

-spec clean_up_cache_after_checkpoint(state(), vectorclock()) -> state().
clean_up_cache_after_checkpoint(State = #state{key_cache_entry_map = KeyCacheEntryMap}, LastCheckpointVts) ->
    NewKeyCacheEntryMap =
        maps:fold(
            fun(KeyStruct, CommitVtsCacheEntryMap, NewKeyCacheEntryMapAcc) ->
                ValidCacheEntryMap =
                    maps:filter(
                        fun(_, #cache_entry{snapshot = #snapshot{snapshot_vts = SnapshotVts}}) ->
                            vectorclock:ge(SnapshotVts, LastCheckpointVts)
                        end, CommitVtsCacheEntryMap),
                case maps:size(ValidCacheEntryMap) of
                    0 -> NewKeyCacheEntryMapAcc;
                    _ -> NewKeyCacheEntryMapAcc#{KeyStruct => ValidCacheEntryMap}
                end
            end, #{}, KeyCacheEntryMap),
    State#state{key_cache_entry_map = NewKeyCacheEntryMap}.
