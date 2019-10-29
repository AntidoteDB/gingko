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
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: cache_server_state()} | {ok, State :: cache_server_state(), timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  {ok, #cache_server_state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: cache_server_state()) ->
  {reply, Reply :: term(), NewState :: cache_server_state()} |
  {reply, Reply :: term(), NewState :: cache_server_state(), timeout() | hibernate} |
  {noreply, NewState :: cache_server_state()} |
  {noreply, NewState :: cache_server_state(), timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: cache_server_state()} |
  {stop, Reason :: term(), NewState :: cache_server_state()}).
handle_call(get, _From, State) ->
  {reply, ok, State};

handle_call(clock, _From, State) ->
  {reply, ok, State};

handle_call(evict, _From, State) ->
  {reply, ok, State};

handle_call(load, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: cache_server_state()) ->
  {noreply, NewState :: cache_server_state()} |
  {noreply, NewState :: cache_server_state(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: cache_server_state()}).
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: cache_server_state()) ->
  {noreply, NewState :: cache_server_state()} |
  {noreply, NewState :: cache_server_state(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: cache_server_state()}).
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: cache_server_state()) -> term()).
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: cache_server_state(),
    Extra :: term()) ->
  {ok, NewState :: cache_server_state()} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get(key_struct(), vectorclock(), cache_server_state()) -> {{ok, snapshot()}, cache_server_state()} | {{error, Reason}, cache_server_state()}.
get(KeyStruct, DependencyVectorClock, CacheServerState) ->
  CacheEntry = dict:find(KeyStruct, CacheServerState#cache_server_state.key_cache_entry_dict),
  case CacheEntry of
    error ->
      %%TODO fill cache

      %%TODO double check to avoid infinite recursion
      get(KeyStruct, DependencyVectorClock, CacheServerState);
    {ok, CacheEntry} ->
      First = vectorclock:le(CacheEntry#cache_entry.commit_vts, DependencyVectorClock),
      Second = vectorclock:le(DependencyVectorClock, CacheEntry#cache_entry.valid_vts),
      Present = CacheEntry#cache_entry.present,
      %%TODO visible
      Conditions = First andalso Second andalso Present,
      if
        Conditions -> {{ok, CacheEntry#cache_entry.blob}, CacheServerState};
        true ->
          error %%TODO fill cache write error message
      end
  end.

load_key(KeyStruct, CacheServerState) ->
  LogServer = CacheServerState#cache_server_state.log_server_pid,
  JournalEntries = gen_server:call(LogServer, get_journal_entries),
  CheckpointEntry = gen_server:call(LogServer, {get_checkpoint, KeyStruct}),
  Snapshot = materializer:materialize_snapshot(CheckpointEntry, JournalEntries),
  CacheEntry = #cache_entry{ key_struct = KeyStruct, commit_vts =  }
  OldDict = CacheServerState#cache_server_state.key_cache_entry_dict,
  CacheServerState = CacheServerState#cache_server_state{ key_cache_entry_dict = dict:store(KeyStruct, )}.

create_cache_entry(Snapshot) ->
  KeyStruct = Snapshot#snapshot.key_struct,
  CommitVts = Snapshot#snapshot.commit_vts,
  SnapshotVts = Snapshot#snapshot.snapshot_vts,
  Value = Snapshot#snapshot.value,
  #cache_entry{ key_struct = KeyStruct, commit_vts = CommitVts, present = true, valid_vts = SnapshotVts, used = true, blob = Value }.

clock(KeyStruct, CommitVectorClock, CacheServerState) ->
%%TODO
  ok.

evict(KeyStruct, CommitVectorClock, CacheServerState) ->
%%TODO
  ok.

inc(KeyStruct, CommitVectorClock, UpdateVectorClock, CacheServerState) ->
%%TODO
  ok.

load(KeyStruct, CommitVectorClock, Value, CacheServerState) ->
  %%TODO
ok.