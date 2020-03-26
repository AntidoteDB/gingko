%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Okt 2019 16:50
%%%-------------------------------------------------------------------
-module(gingko_cache).
-include("gingko.hrl").
-author("kevin").

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

%TODO think of default values
-record(state, {
  dcid :: dcid(),
  key_cache_entry_dict :: dict:dict(key_struct(), [cache_entry()]), %TODO double dict for optimization later
  max_occupancy = 100 :: non_neg_integer(),
  occupancy = 0 :: non_neg_integer(),
  reset_used_interval_millis = 1000 :: non_neg_integer(),
  reset_used_timer = none :: none | reference(),
  number_of_evictions = 10 :: non_neg_integer(),
  eviction_interval_millis = 1000 :: non_neg_integer(),
  eviction_timer = none :: none | reference(),
  eviction_strategy = interval :: interval | fifo | lru | lfu,
  deletion_strategy = interval :: interval | only_mark | delete_directly
  %TODO decide on parameters
}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link({dcid(), pid(), [{atom(), term()}]}) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link({DcId, LogServerPid, CacheConfig}) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, {DcId, LogServerPid, CacheConfig}, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec apply_cache_config(state(), [{atom(), term()}]) -> state().
apply_cache_config(State, CacheConfig) ->
  MaxOccupancy =
    case lists:keyfind(max_occupancy, 1, CacheConfig) of
      {max_occupancy, Value1} -> Value1;
      false -> State#state.max_occupancy
    end,

  {UpdateResetUsedTimer, UsedResetIntervalMillis} =
    case lists:keyfind(reset_used_interval_millis, 1, CacheConfig) of
      {reset_used_interval_millis, Value2} -> {true, Value2};
      false -> {false, State#state.reset_used_interval_millis}
    end,
  NumberOfEvictions = case lists:keyfind(number_of_evictions, 1, CacheConfig) of
                        {number_of_evictions, Value3} -> Value3;
                        false -> State#state.number_of_evictions
                      end,
  {UpdateEvictionTimer, EvictionIntervalMillis} =
    case lists:keyfind(eviction_interval_millis, 1, CacheConfig) of
      {eviction_interval_millis, Value4} -> {true, Value4};
      false -> {false, State#state.eviction_interval_millis}
    end,
  EvictionStrategy = case lists:keyfind(eviction_strategy, 1, CacheConfig) of
                       {eviction_strategy, Value5} -> Value5;
                       false -> State#state.eviction_strategy
                     end,
  DeletionStrategy = case lists:keyfind(deletion_strategy, 1, CacheConfig) of
                       {deletion_strategy, Value6} -> Value6;
                       false -> State#state.deletion_strategy
                     end,
  NewState = State#state{max_occupancy = MaxOccupancy, reset_used_interval_millis = UsedResetIntervalMillis, number_of_evictions = NumberOfEvictions, eviction_interval_millis = EvictionIntervalMillis, eviction_strategy = EvictionStrategy, deletion_strategy = DeletionStrategy},
  update_timers(NewState, UpdateResetUsedTimer, UpdateEvictionTimer).

-spec update_timers(state(), boolean(), boolean()) -> state().
update_timers(State, UpdateResetUsedTimer, UpdateEvictionTimer) ->
  TimerResetUsed =
    case State#state.reset_used_timer of
      none ->
        erlang:send_after(State#state.reset_used_interval_millis, self(), reset_used_event);
      Reference1 ->
        case UpdateResetUsedTimer of
          true ->
            erlang:cancel_timer(Reference1),
            erlang:send_after(State#state.reset_used_interval_millis, self(), reset_used_event);
          false -> Reference1
        end
    end,
  TimerEviction =
    case State#state.eviction_timer of
      none ->
        erlang:send_after(State#state.eviction_interval_millis, self(), eviction_event);
      Reference2 ->
        case UpdateEvictionTimer of
          true -> erlang:cancel_timer(Reference2),
            erlang:send_after(State#state.eviction_interval_millis, self(), eviction_event);
          false -> Reference2
        end
    end,
  State#state{reset_used_timer = TimerResetUsed, eviction_timer = TimerEviction}.


-spec(init({dcid(), pid(), [{atom(), term()}]}) ->
  {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init({DcId, Pid, CacheConfig}) ->
  State = #state{
    dcid = DcId,
    key_cache_entry_dict = dict:new(),
    occupancy = 0
  },
  {ok, apply_cache_config(State, CacheConfig)}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
  {reply, Reply :: term(), NewState :: state()} |
  {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
  {stop, Reason :: term(), NewState :: state()}).
handle_call({get, KeyStruct, DependencyVts}, _From, State) ->
  {Reply, NewState} = get(KeyStruct, DependencyVts, State),
  {reply, Reply, NewState};

handle_call({clock, KeyStruct, CommitVts}, _From, State) ->
  {Reply, NewState} = clock(KeyStruct, CommitVts, State),
  {reply, Reply, NewState};

handle_call({evict, KeyStruct, CommitVts}, _From, State) ->
  {Reply, NewState} = evict(KeyStruct, CommitVts, State),
  {reply, Reply, NewState};

handle_call({inc, KeyStruct, CommitVts, UpdateVts}, _From, State) ->
  {Reply, NewState} = inc(KeyStruct, CommitVts, UpdateVts, State),
  {reply, Reply, NewState};

handle_call({load, KeyStruct, CommitVts, Crdt}, _From, State) ->
  {Reply, NewState} = load(KeyStruct, CommitVts, Crdt, State),
  {reply, Reply, NewState}.

-spec(handle_cast(Request :: term(), State :: state()) ->
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: state()}).
handle_cast(_Request, State) ->
  {noreply, State}.

-spec(handle_info(Info :: timeout() | term(), State :: state()) ->
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: state()}).
handle_info({eviction_event}, State) ->

  {noreply, State};
handle_info({reset_used_event}, State) ->
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term()).
terminate(_Reason, _State) ->
  ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) ->
  {ok, NewState :: state()} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get(key_struct(), vectorclock(), state()) -> {{ok, snapshot()}, state()} | {{error, reason()}, state()}.
get(KeyStruct, DependencyVts, State) ->
  Result = get_or_load_cache_entry(KeyStruct, State, false, false),
  get_internal(Result, KeyStruct, DependencyVts).

-spec get_internal({{ok, cache_entry(), boolean()}, state()} | {{error, Reason}, state()}, key_struct(), vectorclock()) -> {{ok, snapshot()}, state()} | {{error, Reason}, state()}.
get_internal(Result, KeyStruct, DependencyVts) ->
  case Result of
    {{error, Reason}, State} -> {{error, Reason}, State};
    {{ok, CacheEntry, CacheUpdated}, State} ->
      GreaterThanCommitVts = vectorclock:le(CacheEntry#cache_entry.commit_vts, DependencyVts),
      SmallerThanValidVts = vectorclock:le(DependencyVts, CacheEntry#cache_entry.valid_vts),
      Present = CacheEntry#cache_entry.present,
      %%TODO visible
      Conditions = GreaterThanCommitVts andalso SmallerThanValidVts andalso Present,
      case Conditions of
        true ->
          UpdatedCacheEntry = CacheEntry#cache_entry{usage = gingko_utils:update_cache_usage(CacheEntry, true)},
          {{ok, gingko_utils:create_snapshot(UpdatedCacheEntry)}, State};
        false ->
          case CacheUpdated of
            true ->
              {{error, "Bad Cache Update"}, State};
            false ->
              NewResult = get_or_load_cache_entry(KeyStruct, State, false, true),
              get_internal(NewResult, KeyStruct, DependencyVts)
          end
      end
  end.

-spec get_or_load_cache_entry(key_struct(), state(), boolean(), boolean()) -> {{ok, cache_entry(), boolean()}, state()} | {{error, reason()}, state()}.
get_or_load_cache_entry(KeyStruct, State, CacheUpdated, ForceUpdate) ->
  case ForceUpdate of
    true ->
      NewState = load_key_into_cache(KeyStruct, State),
      get_or_load_cache_entry(KeyStruct, NewState, true, false);
    false ->
      FoundCacheEntry = dict:find(KeyStruct, State#state.key_cache_entry_dict),
      case FoundCacheEntry of
        error ->
          case CacheUpdated of
            true -> {{error, "Bad Cache Update"}, State};
            _ ->
              get_or_load_cache_entry(KeyStruct, State, false, true)
          end;
        {ok, CacheEntry} ->
          {{ok, CacheEntry, CacheUpdated}, State}
      end
  end.

-spec load_key_into_cache(key_struct(), state()) -> state().
load_key_into_cache(KeyStruct, State) -> load_key_into_cache(KeyStruct, State, {none, none}).

-spec load_key_into_cache(key_struct(), state(), vts_range()) -> state().
load_key_into_cache(KeyStruct, State, VtsRange) ->
  SortedJournalEntries = gingko_log:read_all_journal_entries_sorted(),
  CheckpointEntry = gingko_log:read_snapshot(KeyStruct),
  {ok, Snapshot} = materializer:materialize_snapshot(CheckpointEntry, SortedJournalEntries, VtsRange),
  CacheEntry = gingko_utils:create_cache_entry(Snapshot),
  update_cache_entry_in_state(CacheEntry, State).

-spec update_cache_entry_in_state(cache_entry(), state()) -> state().
update_cache_entry_in_state(CacheEntry, State) ->
  CacheDict = State#state.key_cache_entry_dict,
  State#state{key_cache_entry_dict = dict:store(CacheEntry#cache_entry.key_struct, CacheEntry, CacheDict)}.


-spec clock(key_struct(), vectorclock(), state()) -> {ok, state()} | {error, state()}.
clock(KeyStruct, CommitVts, State) ->
  CacheEntryResult = dict:find(KeyStruct, State#state.key_cache_entry_dict),
  case CacheEntryResult of
    error ->
      {error, State};
    {ok, CacheEntry} ->
      Equal = vectorclock:eq(CacheEntry#cache_entry.commit_vts, CommitVts),
      case Equal of
        true ->
          CacheEntry = CacheEntry#cache_entry{usage = gingko_utils:update_cache_usage(CacheEntry, false)},
          {ok, update_cache_entry_in_state(CacheEntry, State)};
        false ->
          {error, State}
      end
  end.

-spec evict(key_struct(), vectorclock(), state()) -> {ok, state()} | {error, state()}.
evict(KeyStruct, CommitVts, State) ->
  %%TODO occupancy
  CacheEntryResult = dict:find(KeyStruct, State#state.key_cache_entry_dict),
  case CacheEntryResult of
    error ->
      {error, State};
    {ok, CacheEntry} ->
      CanBeEvicted = vectorclock:eq(CacheEntry#cache_entry.commit_vts, CommitVts) andalso not CacheEntry#cache_entry.usage,
      case CanBeEvicted of
        true ->
          CacheEntry = CacheEntry#cache_entry{present = false},
          {ok, update_cache_entry_in_state(CacheEntry, State)};
        false ->
          {error, State}
      end
  end.

inc(KeyStruct, CommitVts, UpdateVts, State) ->
  %%TODO
  ok.

load(KeyStruct, CommitVts, Value, State) ->
  %%TODO occupancy
  start_eviction_process(State), %TODO check performance of synchronized process
  CacheEntryResult = dict:find(KeyStruct, State#state.key_cache_entry_dict),
  case CacheEntryResult of
    error ->
      {error, State};
    {ok, CacheEntry} ->

      load_key_into_cache(KeyStruct, State, {CommitVts, CommitVts}),
      CanBeEvicted = vectorclock:eq(CacheEntry#cache_entry.commit_vts, CommitVts) andalso not CacheEntry#cache_entry.usage,
      case CanBeEvicted of
        true ->
          CacheEntry = CacheEntry#cache_entry{present = false},
          {ok, update_cache_entry_in_state(CacheEntry, State)};
        false ->
          {error, State}
      end
  end.

-spec start_eviction_process(state()) -> ok.
start_eviction_process(State) ->
  MaxOccupancy = State#state.max_occupancy,
  CurrentOccupancy = State#state.occupancy,
  NumberOfEvictions = State#state.number_of_evictions,

  case CurrentOccupancy >= MaxOccupancy of
    true -> ok;
    false -> ok
  end,
  ok.

%TODO rethink cache_entry definition (instead of used -> times used and last used for more precise cache management)
-spec reset_used(state()) -> state().
reset_used(State) ->
  Dict = State#state.key_cache_entry_dict,
  NewDict = dict:map(fun(_Key, CList) ->
    lists:map(fun(C) -> C#cache_entry{usage = gingko_utils:update_cache_usage(C, false)} end, CList) end, Dict),
  State#state{key_cache_entry_dict = NewDict}.