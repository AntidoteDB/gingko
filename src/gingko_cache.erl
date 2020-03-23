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

-record(state, {
  dcid :: dcid(),
  key_cache_entry_dict :: term(),%%dict(key_struct(), [cache_entry()]),
  max_occupancy :: non_neg_integer(),
  occupancy :: non_neg_integer()
}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link({dcid(), pid(), non_neg_integer()}) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link({DcId, LogServerPid, InitialMaxOccupancy}) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, {DcId, LogServerPid, InitialMaxOccupancy}, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init({dcid(), pid(), non_neg_integer()}) ->
  {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init({DcId, Pid, InitialMaxOccupancy}) ->
  {ok, #state{
    dcid = DcId,
    key_cache_entry_dict = dict:new(),
    occupancy = 0,
    max_occupancy = InitialMaxOccupancy
  }}.

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
      SmallerThanValidVts =  vectorclock:le(DependencyVts, CacheEntry#cache_entry.valid_vts) orelse (CacheUpdated andalso CacheEntry#cache_entry.valid_vts == vectorclock:new()),
      Present = CacheEntry#cache_entry.present,
      %%TODO visible
      Conditions = GreaterThanCommitVts andalso SmallerThanValidVts andalso Present,
      case Conditions of
        true ->
          UpdatedCacheEntry = CacheEntry#cache_entry{ used = true},
          {{ok, gingko_utils:create_snapshot(UpdatedCacheEntry)}, State};
        false ->
          case CacheUpdated of
            true ->
              {{error, "Bad Cache Update"}, State};
            false ->
              Result = get_or_load_cache_entry(KeyStruct, State, false, false),
              get_internal(Result, KeyStruct, DependencyVts)
          end
      end
  end.

-spec get_or_load_cache_entry(key_struct(), state(), boolean(), boolean()) -> {{ok, cache_entry(), boolean()}, state()} | {{error, reason()}, state()}.
get_or_load_cache_entry(KeyStruct, State, CacheUpdated, ForceUpdate) ->
  case ForceUpdate of
    true ->
      NewState = load_key_into_cache(KeyStruct, State),
      get_or_load_cache_entry(KeyStruct, NewState, true, false);
    _ ->
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
  JournalEntries = gingko_log:read_all_journal_entries(),
  CheckpointEntry = gingko_log:read_snapshot(KeyStruct),
  {ok, Snapshot} = materializer:materialize_snapshot(CheckpointEntry, JournalEntries, VtsRange),
  CacheEntry = gingko_utils:create_cache_entry(Snapshot),
  update_cache_entry_in_state(CacheEntry, State).

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
          CacheEntry = CacheEntry#cache_entry{ used = false },
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
      CanBeEvicted = vectorclock:eq(CacheEntry#cache_entry.commit_vts, CommitVts) andalso not CacheEntry#cache_entry.used,
      case CanBeEvicted of
        true ->
          CacheEntry = CacheEntry#cache_entry{ present = false },
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
  CacheEntryResult = dict:find(KeyStruct, State#state.key_cache_entry_dict),
  case CacheEntryResult of
    error ->
      {error, State};
    {ok, CacheEntry} ->

      load_key_into_cache(KeyStruct, State, {CommitVts, CommitVts}),
      CanBeEvicted = vectorclock:eq(CacheEntry#cache_entry.commit_vts, CommitVts) andalso not CacheEntry#cache_entry.used,
      case CanBeEvicted of
        true ->
          CacheEntry = CacheEntry#cache_entry{ present = false },
          {ok, update_cache_entry_in_state(CacheEntry, State)};
        false ->
          {error, State}
      end
  end.