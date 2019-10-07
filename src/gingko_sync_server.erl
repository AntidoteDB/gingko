%% @doc gingko sync server
%% @hidden
-module(gingko_sync_server).
-include("gingko.hrl").
%% API
-export([]).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, terminate/2, handle_info/2, code_change/3]).


%% @doc Starts the log sync timing server for given node
-spec start_link(node()) -> {ok, pid()}.
start_link({JournalLogName, CheckpointLogName}) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {JournalLogName, CheckpointLogName}, []).


%% @doc Initializes the internal server state
init({JournalLogName, CheckpointLogName}) ->
  logger:notice(#{
    action => "Starting log sync server",
    registered_as => ?MODULE,
    name => JournalLogName
  }),

  reset_if_flag_set(JournalLogName),
  reset_if_flag_set(CheckpointLogName),
  {ok, #sync_server_state{journal_log_name = JournalLogName, checkpoint_log_name = CheckpointLogName}}.


terminate(_Reason, State) ->
  logger:notice(#{
    action => "Shutdown log sync server",
    name => State#sync_server_state.journal_log_name
  }),

  close_logs(State),
  ok.


%% @doc opens the log given by the server name (second argument) and the target node (third argument)
handle_call(get_log, _From, State) ->
  logger:notice(#{
    action => "Open log"
  }),
  State = open_logs(State),
  {reply, State, State};

%% @doc Receives a syncing request. Depending on the strategy may or may not sync immediately
handle_call(sync_log, _From, State) ->
  logger:notice(#{
    action => "Sync log to disk",
    log => State#sync_server_state.journal_log_name
  }),
  State = sync_logs(State),
  {reply, State, State}.

%% @doc Receives a syncing request. Depending on the strategy may or may not sync immediately
handle_cast({sync_log, ReplyTo}, State) ->
  logger:notice(#{
    action => "Sync log to disk",
    log => State#sync_server_state.journal_log_name
  }),
  State = sync_logs(State),

  ReplyTo ! log_persisted,
  {noreply, State}.


handle_info(Msg, State) ->
  logger:warning(#{warning => "Unexpected Message", log => State#sync_server_state.journal_log_name, message => Msg}),
  {noreply, State}.


code_change(_OldVsn, _State, _Extra) ->
  erlang:error(not_implemented).


%%%===================================================================
%%% Private Functions Implementation
%%%===================================================================

%% @doc ensures directory where the log is expected and opens the log file.
%%      Recovers if required, logging found terms and bad bytes
-spec open_log(string()) -> {ok, log()}.
open_log(LogName) ->
  filelib:ensure_dir(log_dir_base(LogName)),

  LogFile = log_dir_base(LogName) ++ "OP_LOG",

  LogOptions = [{name, LogFile}, {file, LogFile}],
  case disk_log:open(LogOptions) of
    {ok, Log} -> {ok, Log};
    {repaired, Log, {recovered, Rec}, {badbytes, Bad}} ->
      logger:warning("Recovered bad log file, found ~p terms, ~p bad bytes", [Rec, Bad]),
      {ok, Log}
    %%_Error -> _Error %%TODO log error and throw
  end.


%% @doc Returns the base log dir path
-spec log_dir_base(node() | string()) -> string().
log_dir_base(LogName) when is_atom(LogName) ->
  log_dir_base(atom_to_list(LogName));
log_dir_base(LogName) ->
  % read path
  EnvLogDir = os:getenv("log_root"),
  case EnvLogDir of
    false -> LogDir = "data/op_log/"; % default value if not set
    LogDir -> LogDir
  end,
  LogDir ++ LogName ++ "/".


reset_if_flag_set(LogName) ->
  ResetLogFile = os:getenv("reset_log", "false"),
  case ResetLogFile of
    "true" ->
      LogFile = log_dir_base(LogName) ++ "OP_LOG",
      logger:notice(#{
        action => "Reset log file",
        log => LogName,
        path => LogFile
      }),
      file:delete(LogFile)
    ;
    _ -> ok
  end.

-spec open_logs(sync_server_state()) -> sync_server_state().
open_logs(State) ->
  CurrentJournalLog = State#sync_server_state.journal_log,
  {ok, NewJournalLog} = case CurrentJournalLog of
                          not_open -> open_log(State#sync_server_state.journal_log_name); %%TODO think about errors
                          _ -> CurrentJournalLog
                        end,
  CurrentCheckpointLog = State#sync_server_state.checkpoint_log,
  {ok, NewCheckpointLog} = case CurrentCheckpointLog of
                             not_open -> open_log(State#sync_server_state.checkpoint_log_name); %%TODO think about errors
                             _ -> CurrentCheckpointLog
                           end,

  State#sync_server_state{journal_log = NewJournalLog, checkpoint_log = NewCheckpointLog}.

-spec close_logs(sync_server_state()) -> sync_server_state().
close_logs(State) ->
  JournalLog = State#sync_server_state.journal_log,
  CheckpointLog = State#sync_server_state.checkpoint_log,
  if (JournalLog =:= not_open) -> disk_log:close(JournalLog) end,
  if (CheckpointLog =:= not_open) -> disk_log:close(CheckpointLog) end,
  State#sync_server_state{ journal_log = not_open, checkpoint_log = not_open }.

-spec sync_logs(sync_server_state()) -> sync_server_state().
sync_logs(State) ->
  JournalLog = State#sync_server_state.journal_log,
  CheckpointLog = State#sync_server_state.checkpoint_log,
  if (JournalLog =:= not_open) -> disk_log:sync(JournalLog) end,
  if (CheckpointLog =:= not_open) -> disk_log:sync(CheckpointLog) end,
  State.
