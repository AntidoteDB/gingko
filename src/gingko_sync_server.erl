%% @doc gingko sync server
%% @hidden
-module(gingko_sync_server).

%% API
-export([]).

-behaviour(gen_server).


-export([start_link/1]).
-export([log_dir_base/1]).

-export([init/1, handle_call/3, handle_cast/2, terminate/2,  handle_info/2, code_change/3]).

-include("gingko_sync_server_api.hrl").


start_link(LogName) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {LogName}, []).


init({LogName}) ->
  logger:notice(#{
    action => "Starting log sync server",
    registered_as => ?MODULE,
    name => LogName
  }),

  reset_if_flag_set(LogName),
  {ok, #state{ logs_to_close = sets:new(), log_name = LogName }}.


terminate(_Reason, State) ->
  logger:notice(#{
    action => "Shutdown log sync server",
    name => State#state.log_name
  }),

  % close all references to open logs
  CloseLog = fun(LogRef, _) -> disk_log:close(LogRef) end,
  sets:fold(CloseLog, void, State#state.logs_to_close),
  ok.


handle_call({get_log, LogName}, _From, State) ->
  logger:notice(#{
    action => "Open log",
    log => LogName
  }),

  {ok, Log} = open_log(LogName),
  {reply, {ok, Log}, State}.


handle_cast({sync_log, LogName, ReplyTo}, State) ->
  Log = open_log(LogName),

  logger:notice(#{
    action => "Sync log to disk",
    log => State#state.log_name
  }),
  disk_log:sync(Log),

  ReplyTo ! log_persisted,
  {noreply, State}.


handle_info(Msg, State) ->
  logger:warning(#{
    warning => "Unexpected Message",
    log => State#state.log_name,
    message => Msg
  }),

  {noreply, State}.


code_change(_OldVsn, _State, _Extra) ->
  erlang:error(not_implemented).


%%%===================================================================
%%% Private Functions Implementation
%%%===================================================================

open_log(LogName) ->
  filelib:ensure_dir(log_dir_base(LogName)),

  LogFile = log_dir_base(LogName) ++ "OP_LOG",

  LogOptions = [{name, LogFile}, {file, LogFile}],
  case disk_log:open(LogOptions) of
    {ok, Name} -> {ok, Name};
    {repaired, Name, {recovered, Rec}, {badbytes, Bad}} ->
      logger:warning("Recovered bad log file, found ~p terms, ~p bad bytes", [Rec, Bad]),
      {ok, Name}
  end.


log_dir_base(LogName) when is_atom(LogName)->
  log_dir_base(atom_to_list(LogName));
log_dir_base(LogName) ->
  % read path
  EnvLogDir = os:getenv("OP_LOG_DIR"),
  case EnvLogDir of
    false -> LogDir = "data/op_log/"; % default value if not set
    LogDir -> LogDir
  end,
  LogDir ++ LogName ++ "/".



reset_if_flag_set(LogName) ->
  ResetLogFile = os:getenv("RESET_LOG_FILE", "false"),
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
