%%%-------------------------------------------------------------------
%%% @author antidote
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Dec 2018 17:25
%%%-------------------------------------------------------------------
-module(gingko_op_log_server).
-include("gingko.hrl").

%% API
-export([]).

-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2,  handle_info/2]).

% internal state and type specifications
-include("gingko_op_log_server_api.hrl").

% log starts with this default index
-define(STARTING_INDEX, 0).

start_link(LogName, RecoveryReceiver) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {LogName, RecoveryReceiver}, []).


init({LogName, RecoveryReceiver}) ->
  logger:info(#{
    action => "Starting op log server",
    name => LogName,
    receiver => RecoveryReceiver
  }),

  {ok, LogServer} = gingko_sync_server:start_link(LogName),
  gen_server:cast(self(), start_recovery),
  {ok, #state{
    log_name = LogName,
    recovery_receiver = RecoveryReceiver,
    recovering = true,
    sync_server = LogServer
  }}.


%% ------------------
%% ASYNC LOG RECOVERY
%% ------------------

handle_cast(start_recovery, State) when State#state.recovering == true ->
  LogName = State#state.log_name,
  _Receiver = State#state.recovery_receiver,
  _LogServer = State#state.sync_server,
  logger:info("[~p] Async recovery started", [LogName]),

  GenServer = self(),
  AsyncRecovery = fun() ->
    %NextIndex = recover_all_logs(LogName, Receiver, LogServer),
    %% TODO recovery
    NextIndex = 0,
    gen_server:cast(GenServer, {finish_recovery, NextIndex})
                  end,
  spawn_link(AsyncRecovery),
  {noreply, State};

handle_cast({finish_recovery, NextIndexMap}, State) ->
  % TODO
  % reply to waiting processes to try their requests again
  %reply_retry_to_waiting(State#state.waiting_for_reply),

  % save write-able index map and finish recovery
  logger:info("[~p] Recovery process finished", [State#state.log_name]),
  {noreply, State#state{recovering = false, next_index = NextIndexMap, waiting_for_reply = []}};

handle_cast(Msg, State) ->
  logger:warning("[~p] Swallowing unexpected message: ~p", [State#state.log_name, Msg]),
  {noreply, State}.


terminate(_Reason, State) ->
  gen_server:stop(State#state.sync_server),
  ok.


handle_call({add_log_entry, {Index, _Data}}, From, State) when State#state.recovering == true ->
  logger:info("[~p] Add ~p, waiting for recovery", [State#state.log_name, Index]),
  Waiting = State#state.waiting_for_reply,
  {noreply, State#state{ waiting_for_reply = Waiting ++ [From] }};

handle_call({add_log_entry, {Index, Data}}, From, State) ->
  lager:info("[~p] Adding log entry", [State#state.log_name]),

  NextIndex = State#state.next_index,
  LogName = State#state.log_name,
  LogServer = State#state.sync_server,
  Waiting = State#state.waiting_for_reply,

  if
    Index < NextIndex ->
      logger:error("[~p] Index ~p already written, currently at index ~p",
        [LogName, Index, NextIndex - 1]),
      {reply, {error, index_already_written}, State};
    Index > NextIndex ->
      logger:info("[~p] Trying to write index ~p, but writable index is behind: ~p. Waiting.",
        [LogName, Index, NextIndex]),
      {noreply, State#state{ waiting_for_reply = Waiting ++ [From] }};
    true ->
      {ok, Log} = gen_server:call(LogServer, {get_log, LogName}),

      ok = disk_log:log(Log, {Index, Data}),

      % wait for sync reply
      gen_server:cast(LogServer, {sync_log, LogName, self()}),
      receive log_persisted -> ok end,

      logger:info("[~p] Log entry at ~p persisted",
        [State#state.log_name, Index]),

      % index of another request may be up to date, send retry messages
      reply_retry_to_waiting(Waiting),
      {reply, ok, State#state{
        % increase index counter for node by one
        next_index = Index + 1,
        % empty waiting queue
        waiting_for_reply = []
      }}
  end;


handle_call({read_log_entries, _Node, _FirstIndex, _LastIndex, _F, _Acc}, From, State)
  when State#state.recovering == true ->
  logger:info("[~p] Read, waiting for recovery", [State#state.log_name]),
  Waiting = State#state.waiting_for_reply,
  {noreply, State#state{ waiting_for_reply = Waiting ++ [From] }};

handle_call({read_log_entries, Node, FirstIndex, LastIndex, F, Acc}, _From, State) ->
  LogName = State#state.log_name,
  LogServer = State#state.sync_server,
  Waiting = State#state.waiting_for_reply,
  %% simple implementation, read ALL terms, then filter
  %% can be improved performance wise, stop at last index

  {ok, Log} = gen_server:call(LogServer, {get_log, LogName, Node}),
  %% TODO this will most likely cause a timeout to the gen_server caller, what to do?
  Terms = read_all(Log),

  % filter index
  FilterByIndex = fun({Index, _}) -> Index >= FirstIndex andalso ((LastIndex == all) or (Index =< LastIndex)) end,
  FilteredTerms = lists:filter(FilterByIndex, Terms),

  % apply given aggregator function
  ReplyAcc = lists:foldl(F, Acc, FilteredTerms),

  reply_retry_to_waiting(Waiting),
  {reply, {ok, ReplyAcc}, State#state{waiting_for_reply = []}}.


handle_info(Msg, State) ->
  logger:warning("Swallowing unexpected message: ~p", [Msg]),
  {noreply, State}.




%%%===================================================================
%%% Private Functions Implementation
%%%===================================================================

reply_retry_to_waiting(WaitingProcesses) ->
  Reply = fun(Process, _) -> gen_server:reply(Process, retry) end,
  lists:foldl(Reply, void, WaitingProcesses),
  ok.

recover_all_logs(LogName, Receiver, LogServer) when is_atom(LogName) ->
  recover_all_logs(atom_to_list(LogName), Receiver, LogServer);
recover_all_logs(LogName, Receiver, LogServer) ->
  % make sure the folder exists
  LogPath = gingko_sync_server:log_dir_base(LogName),
  filelib:ensure_dir(LogPath),

  ProcessLogFile = fun(LogFile, IndexMapAcc) ->
    logger:info("[~s] Recovering logfile ~p", [LogName, LogFile]),

    [NodeName, _] = string:split(LogFile, "."),
    Node = list_to_atom(NodeName),

    {ok, Log} = gen_server:call(LogServer, {get_log, LogName, Node}),

    % read all terms
    Terms = read_all(Log),

    % For each entry {log_recovery, Node, {Index, Data}} is sent
    SendTerm = fun({Index, Data}, _) -> Receiver ! {log_recovery, Node, {Index, Data}} end,
    lists:foldl(SendTerm, void, Terms),

    {LastIndex, _} = hd(lists:reverse(Terms)),

    maps:put(Node, LastIndex + 1, IndexMapAcc)
                   end,

  {ok, LogFiles} = file:list_dir(LogPath),
  % accumulate node -> next free index
  IndexMapAcc = #{},

  IndexMap = lists:foldl(ProcessLogFile, IndexMapAcc, LogFiles),

  Receiver ! log_recovery_done,

  IndexMap.


read_all(Log) ->
  read_all(Log, [], start).


read_all(Log, Terms, Cont) ->
  case disk_log:chunk(Log, Cont) of
    eof -> Terms;
    {Cont2, ReadTerms} -> read_all(Log, Terms ++ ReadTerms, Cont2)
  end.


%%%===================================================================
%%% Unit Tests
%%%===================================================================

%%-ifdef(TEST).
%%-include_lib("eunit/include/eunit.hrl").
%%
%%main_test_() ->
%%  {foreach,
%%    fun setup/0,
%%    fun cleanup/1,
%%    [
%%      fun simple_write/1,
%%      fun index_already_written/1,
%%      fun index_lagging/1,
%%      fun read_test/1,
%%      fun multi_read_test/1,
%%      fun empty_read_test/1
%%    ]}.
%%
%%% Setup and Cleanup
%%setup() ->
%%  % logger
%%  logger:start(),
%%  logger:set_loglevel(lager_console_backend, info),
%%
%%  % dummy receiver
%%  DummyReceive = fun() -> receive A -> logger:info("Receive: ~p", [A]) end end,
%%  DummyLoop = fun Loop() -> DummyReceive(), Loop() end,
%%  DummyReceiver = spawn_link(DummyLoop),
%%
%%  % start server
%%  {ok, Pid} = minidote_op_log_server:start_link(eunit, DummyReceiver),
%%  Pid.
%%
%%cleanup(Pid) ->
%%  gen_server:stop(Pid).
%%
%%start(Name) -> logger:info("=== " ++ atom_to_list(Name) ++ " ===").
%%fin(Name) -> logger:info("=== " ++ atom_to_list(Name) ++ " fin ===").
%%
%%
%%simple_write(Log) ->
%%  fun() ->
%%    Node = e_simple,
%%    start(Node),
%%    % add log entry {0, {data, eunit, 0}} for <eunit>
%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX, {data}}),
%%    fin(Node)
%%  end.
%%
%%index_already_written(Log) ->
%%  fun() ->
%%    Node = e_iaw,
%%    start(Node),
%%
%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX, {data}}),
%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX+1, {data}}),
%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX+2, {data}}),
%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX+3, {data}}),
%%
%%    % expect error for same index
%%    {error, _} = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX, {data}}),
%%    {error, _} = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX+3, {data}}),
%%
%%    % but valid for another node
%%    ok = minidote_op_log:add_log_entry(Log, i_iaw2, {?STARTING_INDEX, {data}}),
%%    fin(Node)
%%  end.
%%
%%index_lagging(Log) ->
%%  fun() ->
%%    Node = e_lagging,
%%    start(Node),
%%
%%    % write entries in bad order
%%    spawn_link(fun() -> minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX+1, {data}}) end),
%%    spawn_link(fun() -> minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX, {data}}) end),
%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX+2, {data}}),
%%    fin(Node)
%%  end.
%%
%%read_test(Log) ->
%%  fun() ->
%%    Node = e_read,
%%    start(Node),
%%
%%    % write one entry
%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX, {data}}),
%%
%%    % read entry
%%    {ok, [{?STARTING_INDEX, {data}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX, all),
%%    fin(Node)
%%  end.
%%
%%multi_read_test(Log) ->
%%  fun() ->
%%    Node = e_multi,
%%    start(Node),
%%
%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX, {data_1}}),
%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX + 1, {data_2}}),
%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX + 2, {data_3}}),
%%
%%    % read entry
%%    {ok, [{?STARTING_INDEX, {data_1}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX, ?STARTING_INDEX),
%%    {ok, [{?STARTING_INDEX + 1, {data_2}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX + 1, ?STARTING_INDEX + 1),
%%    {ok, [{?STARTING_INDEX + 2, {data_3}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX + 2, ?STARTING_INDEX + 2),
%%
%%    {ok, [{?STARTING_INDEX + 1, {data_2}}, {?STARTING_INDEX + 2, {data_3}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX + 1, ?STARTING_INDEX + 2),
%%    %% OOB
%%    {ok, [{?STARTING_INDEX + 1, {data_2}}, {?STARTING_INDEX + 2, {data_3}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX + 1, ?STARTING_INDEX + 3),
%%    %% all
%%    {ok, [{?STARTING_INDEX + 1, {data_2}}, {?STARTING_INDEX + 2, {data_3}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX + 1, all),
%%    fin(Node)
%%  end.
%%
%%empty_read_test(Log) ->
%%  fun() ->
%%    Node = e_empty,
%%    start(Node),
%%
%%    {ok, []} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX, all),
%%
%%    fin(Node)
%%  end.
%%
%%-endif.
