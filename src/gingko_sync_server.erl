%%%-------------------------------------------------------------------
%%% @author antidote
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Dec 2018 17:33
%%%-------------------------------------------------------------------
-module(gingko_sync_server).
-author("antidote").

%% API
-export([]).

-behaviour(gen_server).


-export([start_link/1]).
-export([log_dir_base/1, log_file_path/2]).

-export([init/1, handle_call/3, handle_cast/2, terminate/2,  handle_info/2, code_change/3]).

-include("gingko_sync_server_api.hrl").


start_link(ServerName) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {ServerName}, []).


init({ServerName}) ->
  logger:info("[~p] log sync server ~p", [ServerName, self()]),
  {ok, #state{ logs_to_close = sets:new(), server_name = ServerName }}.


terminate(_Reason, State) ->
  logger:info("[~p] Closing all open logs", [State#state.server_name]),
  CloseLog = fun(LogRef, _) -> disk_log:close(LogRef) end,
  sets:fold(CloseLog, void, State#state.logs_to_close),
  ok.


handle_call({get_log, ServerName, Node}, _From, State) ->
  {ok, Log} = open_log(ServerName, Node),
  {reply, {ok, Log}, State}.


handle_cast({sync_log, ServerName, Node, ReplyTo}, State) ->
  Log = open_log(ServerName, Node),

  %% TODO better syncing strategy
  lager:info("[~p] Syncing log", [State#state.server_name]),
  disk_log:sync(Log),

  ReplyTo ! log_persisted,
  {noreply, State}.


handle_info(Msg, State) ->
  lager:warning("[~p] Swallowing unexpected message: ~p", [State#state.server_name, Msg]),
  {noreply, State}.


code_change(_OldVsn, _State, _Extra) ->
  erlang:error(not_implemented).


%%%===================================================================
%%% Private Functions Implementation
%%%===================================================================

open_log(ServerName, Node) ->
  filelib:ensure_dir(log_dir_base(ServerName)),

  LogFile = log_dir_base(ServerName) ++ atom_to_list(Node) ++ ".log",

  LogOptions = [{name, LogFile}, {file, LogFile}],
  case disk_log:open(LogOptions) of
    {ok, Name} -> {ok, Name};
    {repaired, Name, {recovered, Rec}, {badbytes, Bad}} ->
      lager:warning("Recovered bad log file, found ~p terms, ~p bad bytes", [Rec, Bad]),
      {ok, Name}
  end.


log_dir_base(ServerName) when is_atom(ServerName)->
  log_dir_base(atom_to_list(ServerName));
log_dir_base(ServerName) ->
  % read path
  EnvLogDir = os:getenv("OP_LOG_DIR"),
  case EnvLogDir of
    false -> LogDir = "data/op_log/"; % default value if not set
    LogDir -> LogDir
  end,
  LogDir ++ ServerName ++ "/".


log_file_path(ServerName, Node) ->
  log_dir_base(ServerName) ++ atom_to_list(Node) ++ ".log".
