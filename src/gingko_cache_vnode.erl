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

-type cache_dict() :: dict:dict(key_struct(), [cache_entry()]).

%TODO think of default values
-record(state, {
    partition = 0 :: partition_id(),
    key_cache_entry_dict = dict:new() :: cache_dict(), %TODO double dict for optimization later
    max_occupancy = 100 :: non_neg_integer(),
    reset_used_interval_millis = ?DEFAULT_WAIT_TIME_LONG :: non_neg_integer(),
    reset_used_timer = none :: none | reference(),
    eviction_interval_millis = ?DEFAULT_WAIT_TIME_SUPER_LONG :: non_neg_integer(),
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
    {ok, NewState}.

handle_command(Request = hello, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {reply, ok, State};

handle_command(Request = {get, KeyStruct, DependencyVts}, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {Reply, NewState} = get(KeyStruct, DependencyVts, State),
    {reply, Reply, NewState};

handle_command(Request = {update_cache_config, CacheConfig}, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {reply, ok, apply_gingko_config(State, CacheConfig)};

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

-spec apply_gingko_config(state(), map_list()) -> state().
apply_gingko_config(State, GingkoConfig) ->
    Partition = general_utils:get_or_default_map_list(partition, GingkoConfig, error),
    MaxOccupancy = general_utils:get_or_default_map_list(max_occupancy, GingkoConfig, State#state.max_occupancy),
    EvictionStrategy = general_utils:get_or_default_map_list(eviction_strategy, GingkoConfig, State#state.eviction_strategy),
    {UpdateResetUsedTimer, UsedResetIntervalMillis} =
        general_utils:get_or_default_map_list_check(reset_used_interval_millis, GingkoConfig, State#state.reset_used_interval_millis),
    {UpdateEvictionTimer, EvictionIntervalMillis} =
        general_utils:get_or_default_map_list_check(eviction_interval_millis, GingkoConfig, State#state.eviction_interval_millis),
    NewState = State#state{partition = Partition, max_occupancy = MaxOccupancy, reset_used_interval_millis = UsedResetIntervalMillis, eviction_interval_millis = EvictionIntervalMillis, eviction_strategy = EvictionStrategy},
    update_timers(NewState, UpdateResetUsedTimer, UpdateEvictionTimer).

-spec update_timers(state(), boolean(), boolean()) -> state().
update_timers(State = #state{reset_used_timer = CurrentResetUsedTimer, reset_used_interval_millis = ResetUsedIntervalMillis, eviction_timer = CurrentEvictionTimer, eviction_interval_millis = EvictionIntervalMillis}, UpdateResetUsedTimer, UpdateEvictionTimer) ->
    NewResetUsedTimer = gingko_utils:update_timer(CurrentResetUsedTimer, UpdateResetUsedTimer, ResetUsedIntervalMillis, reset_used_event, true),
    NewEvictionTimer = gingko_utils:update_timer(CurrentEvictionTimer, UpdateEvictionTimer, EvictionIntervalMillis, eviction_event, true),
    State#state{reset_used_timer = NewResetUsedTimer, eviction_timer = NewEvictionTimer}.

-spec get(key_struct(), vectorclock(), state()) -> {{ok, snapshot()}, state()} | {{error, reason()}, state()}.
get(KeyStruct, DependencyVts, State) ->
    Result = get_or_load_cache_entry(KeyStruct, DependencyVts, State, false, false),
    get_internal(Result, KeyStruct, DependencyVts).

-spec get_internal({{ok, [cache_entry()], boolean()}, key_struct(), state()} | {{error, Reason}, state()}, key_struct(), vectorclock()) -> {{ok, snapshot()}, state()} | {{error, Reason}, state()}.
get_internal(Result, KeyStruct, DependencyVts) ->
    case Result of
        {{error, Reason}, State} -> {{error, Reason}, State};
        {{ok, CacheEntryList, CacheUpdated}, State} ->
            MatchingCacheEntryList =
                lists:filter(
                    fun(#cache_entry{commit_vts = CommitVts, valid_vts = ValidVts}) ->
                        case CacheUpdated andalso {vectorclock:new(), vectorclock:new()} == {CommitVts, ValidVts} andalso length(CacheEntryList) == 1 of
                            true -> true; %In case we did not commit anything yet
                            false -> gingko_utils:is_in_vts_range(DependencyVts, {CommitVts, ValidVts})
                        end
                    %%TODO check: this should also mean visible
                    end, CacheEntryList),
            case MatchingCacheEntryList of
                [] ->
                    case CacheUpdated of
                        true ->
                            {{error, "Bad Cache Update1"}, State};
                        false ->
                            NewResult = get_or_load_cache_entry(KeyStruct, DependencyVts, State, false, true),
                            get_internal(NewResult, KeyStruct, DependencyVts)
                    end;
                [CacheEntry] ->
                    UpdatedCacheEntry = gingko_utils:update_cache_usage(CacheEntry, true),
                    {{ok, gingko_utils:create_snapshot_from_cache_entry(UpdatedCacheEntry)}, State};
                _Multiple ->
                    {{error, "Multiple cache entries with the same key and commit vts exist which should not happen!"}, State}
            end
    end.

-spec get_or_load_cache_entry(key_struct(), vectorclock(), state(), boolean(), boolean()) -> {{ok, [cache_entry()], boolean()}, state()} | {{error, reason()}, state()}.
get_or_load_cache_entry(KeyStruct, DependencyVts, State = #state{key_cache_entry_dict = KeyCacheEntryDict}, CacheUpdated, ForceUpdate) ->
    case ForceUpdate of
        true ->
            NewState = load_key_into_cache(KeyStruct, State, DependencyVts),
            get_or_load_cache_entry(KeyStruct, DependencyVts, NewState, true, false);
        false ->
            FoundCacheEntryList = dict:find(KeyStruct, KeyCacheEntryDict),
            case FoundCacheEntryList of
                error ->
                    case CacheUpdated of
                        true -> {{error, "Bad Cache Update2"}, State};
                        false ->
                            get_or_load_cache_entry(KeyStruct, DependencyVts, State, false, true)
                    end;
                {ok, CacheEntryList} ->
                    {{ok, CacheEntryList, CacheUpdated}, State}
            end
    end.

-spec load_key_into_cache(key_struct(), state(), vectorclock()) -> state().
load_key_into_cache(KeyStruct, State = #state{partition = Partition, key_cache_entry_dict = KeyCacheEntryDict}, DependencyVts) ->
    {ok, JournalEntryList} = gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {get_valid_journal_entries, DependencyVts}),
    CheckpointJournalEntryList = gingko_utils:get_journal_entries_of_type(JournalEntryList, checkpoint),
    SortCheckpointByVts = fun(#journal_entry{args = #checkpoint_args{dependency_vts = Vts1}}, #journal_entry{args = #checkpoint_args{dependency_vts = Vts2}}) -> vectorclock:le(Vts2, Vts1) end,
    ReverseVtsSortedCheckpointJournalEntryList = lists:sort(SortCheckpointByVts, CheckpointJournalEntryList),
    {MostRecentSnapshot, NewState} =
        case ReverseVtsSortedCheckpointJournalEntryList of
            [] ->
                Snapshot1 = gingko_utils:create_new_snapshot(KeyStruct, vectorclock:new()),
                CacheEntry1 = gingko_utils:create_cache_entry(Snapshot1),
                {Snapshot1, update_cache_entry_in_state(CacheEntry1, State)};
            [#journal_entry{args = #checkpoint_args{dependency_vts = CheckpointVts}} | _] ->
                FoundCacheEntryList = dict:find(KeyStruct, KeyCacheEntryDict),
                case FoundCacheEntryList of
                    error ->
                        Snapshot2 = gingko_log_utils:read_checkpoint_entry(KeyStruct, CheckpointVts),
                        CacheEntry2 = gingko_utils:create_cache_entry(Snapshot2),
                        {Snapshot2, update_cache_entry_in_state(CacheEntry2, State)};
                    {ok, CacheEntryList} ->
                        ValidCacheEntryList =
                            lists:filter(
                                fun(#cache_entry{commit_vts = FoundCommitVts, valid_vts = FoundValidVts}) ->
                                    gingko_utils:is_in_vts_range(FoundCommitVts, {none, DependencyVts}) andalso
                                        gingko_utils:is_in_vts_range(FoundValidVts, {CheckpointVts, DependencyVts})
                                end, CacheEntryList),
                        case ValidCacheEntryList of
                            [] ->
                                Snapshot3 = gingko_log_utils:read_checkpoint_entry(KeyStruct, CheckpointVts),
                                CacheEntry3 = gingko_utils:create_cache_entry(Snapshot3),
                                {Snapshot3, update_cache_entry_in_state(CacheEntry3, State)};
                            [ValidCacheEntry | _] ->
                                Snapshot4 = gingko_utils:create_snapshot_from_cache_entry(ValidCacheEntry),
                                {Snapshot4, State}
                        end
                end
        end,

    {ok, Snapshot} = gingko_materializer:materialize_snapshot(MostRecentSnapshot, JournalEntryList, DependencyVts),
    CacheEntry = gingko_utils:create_cache_entry(Snapshot),
    update_cache_entry_in_state(CacheEntry, NewState).

-spec update_cache_entry_in_state(cache_entry(), state()) -> state().
update_cache_entry_in_state(CacheEntry = #cache_entry{key_struct = KeyStruct, commit_vts = CommitVts, valid_vts = ValidVts}, State = #state{key_cache_entry_dict = KeyCacheEntryDict}) ->
    CacheEntryList = general_utils:get_or_default_dict(KeyStruct, KeyCacheEntryDict, []),
    MatchingCacheEntryList = lists:filter(
        fun(#cache_entry{commit_vts = FoundCommitVts}) ->
            FoundCommitVts == CommitVts
        end, CacheEntryList),
    UpdateNecessary =
        case MatchingCacheEntryList of
            [] -> true;
            [#cache_entry{valid_vts = FoundValidVts}] ->
                gingko_utils:is_in_vts_range(ValidVts, {FoundValidVts, none});
            _Multiple ->
                logger:error("Multiple cache entries with the same key and commit vts exist which should not happen!~nExisting Cache Entries:~n~p~nCache Entry Update:~n~p~n", [CacheEntryList, CacheEntry]),
                true
        end,
    case UpdateNecessary of
        true ->
            NewCacheEntryList = [CacheEntry | lists:filter(fun(#cache_entry{commit_vts = FoundCommitVts}) ->
                FoundCommitVts /= CommitVts end, CacheEntryList)],
            NewKeyCacheEntryDict = dict:store(KeyStruct, NewCacheEntryList, KeyCacheEntryDict),
            State#state{key_cache_entry_dict = NewKeyCacheEntryDict};
        false ->
            logger:error("Unnecessary cache update!~nExisting Cache Entries:~n~p~nCache Entry Update:~n~p~n", [CacheEntryList, CacheEntry]),
            State
    end.

-spec start_eviction_process(state()) -> state().
start_eviction_process(State = #state{max_occupancy = MaxOccupancy, key_cache_entry_dict = KeyCacheEntryDict, eviction_threshold_in_percent = EvictionThresholdInPercent, target_threshold_in_percent = TargetThresholdInPercent, eviction_strategy = EvictionStrategy}) ->
    CurrentOccupancy =
        dict:fold(fun(_Key, CacheEntryList, Number) -> Number + length(CacheEntryList) end, 0, KeyCacheEntryDict),
    EvictionThreshold = EvictionThresholdInPercent * MaxOccupancy div 100,
    TargetThreshold = TargetThresholdInPercent * EvictionThreshold div 100,
    EvictionNeeded = CurrentOccupancy > EvictionThreshold,
    NewCacheDict =
        case {EvictionNeeded, EvictionStrategy} of
            {true, interval} -> %No preference on cache entries
                interval_evict(KeyCacheEntryDict, CurrentOccupancy, TargetThreshold);
            {true, fifo} ->
                fifo_evict(KeyCacheEntryDict, CurrentOccupancy, TargetThreshold);
            {true, lru} ->
                lru_evict(KeyCacheEntryDict, CurrentOccupancy, TargetThreshold);
            {true, lfu} ->
                lfu_evict(KeyCacheEntryDict, CurrentOccupancy, TargetThreshold);
            {false, _} ->
                KeyCacheEntryDict
        end,
    State#state{key_cache_entry_dict = NewCacheDict}.

-spec interval_evict(cache_dict(), non_neg_integer(), non_neg_integer()) -> cache_dict().
interval_evict(KeyCacheEntryDict, CurrentOccupancy, TargetThreshold) ->
    CacheEntryList = get_cache_entry_list(KeyCacheEntryDict),
    evict(CacheEntryList, CurrentOccupancy, TargetThreshold, fun interval_evict/3).

-spec fifo_evict(cache_dict(), non_neg_integer(), non_neg_integer()) -> cache_dict().
fifo_evict(KeyCacheEntryDict, CurrentOccupancy, TargetThreshold) ->
    CacheEntryList = get_cache_entry_list(KeyCacheEntryDict),
    SortedCacheEntryList = sort_by_first_used(CacheEntryList),
    evict(SortedCacheEntryList, CurrentOccupancy, TargetThreshold, fun fifo_evict/3).

-spec lru_evict(cache_dict(), non_neg_integer(), non_neg_integer()) -> cache_dict().
lru_evict(KeyCacheEntryDict, CurrentOccupancy, TargetThreshold) ->
    CacheEntryList = get_cache_entry_list(KeyCacheEntryDict),
    SortedCacheEntryList = sort_by_last_used(CacheEntryList),
    evict(SortedCacheEntryList, CurrentOccupancy, TargetThreshold, fun lru_evict/3).

-spec lfu_evict(cache_dict(), non_neg_integer(), non_neg_integer()) -> cache_dict().
lfu_evict(KeyCacheEntryDict, CurrentOccupancy, TargetThreshold) ->
    CacheEntryList = get_cache_entry_list(KeyCacheEntryDict),
    SortedCacheEntryList = sort_by_times_used(CacheEntryList),
    evict(SortedCacheEntryList, CurrentOccupancy, TargetThreshold, fun lfu_evict/3).

evict(SortedCacheEntryList, CurrentOccupancy, TargetThreshold, RecursionFun) ->
    {RemainingOccupancy, NewUsedKeyCacheEntryDict} =
        lists:foldl(
            fun(CacheEntry = #cache_entry{key_struct = KeyStruct, usage = #cache_usage{used = Used}}, {OccupancyAcc, CurrentKeyCacheEntryDict}) ->
                case OccupancyAcc =< TargetThreshold orelse Used of
                    true ->
                        {OccupancyAcc, dict:append(KeyStruct, CacheEntry, CurrentKeyCacheEntryDict)};
                    false -> {OccupancyAcc - 1, CurrentKeyCacheEntryDict}
                end
            end, {CurrentOccupancy, dict:new()}, SortedCacheEntryList),
    case RemainingOccupancy >= TargetThreshold of
        true ->
            RecursionFun(reset_used(NewUsedKeyCacheEntryDict, 0), RemainingOccupancy, TargetThreshold);
        false -> NewUsedKeyCacheEntryDict
    end.

get_cache_entry_list(KeyCacheEntryDict) ->
    lists:append(lists:map(fun({_KeyStruct, CacheEntryList}) ->
        CacheEntryList end, dict:to_list(KeyCacheEntryDict))).

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
reset_used(State = #state{reset_used_interval_millis = ResetUsedIntervalMillis, key_cache_entry_dict = KeyCacheEntryDict}) ->
    ResetInterval = ResetUsedIntervalMillis * 1000,
    NewKeyCacheEntryDict = reset_used(KeyCacheEntryDict, ResetInterval),
    State#state{key_cache_entry_dict = NewKeyCacheEntryDict}.

-spec reset_used(cache_dict(), non_neg_integer()) -> cache_dict().
reset_used(KeyCacheEntryDict, ResetInterval) ->
    CurrentTime = gingko_utils:get_timestamp(),
    MatchTime = CurrentTime - ResetInterval,
    dict:map(
        fun(_KeyStruct, CacheEntryList) ->
            lists:map(
                fun(CacheEntry = #cache_entry{usage = #cache_usage{last_used = LastUsed}}) ->
                    case LastUsed < MatchTime of
                        true -> gingko_utils:update_cache_usage(CacheEntry, false);
                        false -> CacheEntry
                    end
                end, CacheEntryList)
        end, KeyCacheEntryDict).

-spec clean_up_cache_after_checkpoint(state(), vectorclock()) -> state().
clean_up_cache_after_checkpoint(State = #state{key_cache_entry_dict = KeyCacheEntryDict}, LastCheckpointVts) ->
    NewKeyCacheEntryDict =
        dict:fold(
            fun(KeyStruct, CacheEntryList, NewKeyCacheEntryDictAcc) ->
                ValidCacheEntryList =
                    lists:filter(
                        fun(#cache_entry{valid_vts = ValidVts}) ->
                            gingko_utils:is_in_vts_range(ValidVts, {LastCheckpointVts, none})
                        end, CacheEntryList),
                case ValidCacheEntryList of
                    [] -> NewKeyCacheEntryDictAcc;
                    _ -> dict:store(KeyStruct, ValidCacheEntryList, NewKeyCacheEntryDictAcc)
                end
            end, dict:new(), KeyCacheEntryDict),
    State#state{key_cache_entry_dict = NewKeyCacheEntryDict}.
