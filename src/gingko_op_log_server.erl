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
  logger:notice(#{
    action => "Starting op log server",
    registered_as => ?MODULE,
    name => LogName,
    receiver => RecoveryReceiver
  }),

  case RecoveryReceiver of
    none -> ActualReceiver =
      spawn(fun Loop() ->
        receive Message -> logger:notice("Received dummy message: ~p",[Message]) end,
        Loop()
      end);
    _ -> ActualReceiver = RecoveryReceiver
  end,


  {ok, LogServer} = gingko_sync_server:start_link(LogName),

  gen_server:cast(self(), start_recovery),
  {ok, #state{
    log_name = LogName,
    recovery_receiver = ActualReceiver,
    recovering = true,
    sync_server = LogServer
  }}.


%% ------------------
%% ASYNC LOG RECOVERY
%% ------------------

handle_cast(start_recovery, State) when State#state.recovering == true ->
  LogName = State#state.log_name,
  Receiver = State#state.recovery_receiver,
  LogServer = State#state.sync_server,
  logger:info("[~p] Async recovery started", [LogName]),

  GenServer = self(),
  AsyncRecovery = fun() ->
    NextIndex = recover_all_logs(LogName, Receiver, LogServer),
    %% TODO recovery
    NextIndex = 0,
    gen_server:cast(GenServer, {finish_recovery, NextIndex})
                  end,
  spawn_link(AsyncRecovery),
  {noreply, State};

handle_cast({finish_recovery, NextIndexMap}, State) ->
  % TODO
  % reply to waiting processes to try their requests again
  reply_retry_to_waiting(State#state.waiting_for_reply),

  % save write-able index map and finish recovery
  logger:info("[~p] Recovery process finished", [State#state.log_name]),
  {noreply, State#state{recovering = false, next_index = NextIndexMap, waiting_for_reply = []}};

handle_cast(Msg, State) ->
  logger:warning("[~p] Swallowing unexpected message: ~p", [State#state.log_name, Msg]),
  {noreply, State}.


terminate(_Reason, State) ->
  gen_server:stop(State#state.sync_server),
  ok.


handle_call({add_log_entry, _Data}, From, State) when State#state.recovering == true ->
  logger:notice("[~p] Waiting for recovery: ~p", [State#state.log_name, From]),
  Waiting = State#state.waiting_for_reply,
  {noreply, State#state{ waiting_for_reply = Waiting ++ [From] }};

handle_call({add_log_entry, Data}, From, State) ->
  logger:notice(#{
    action => "Append to log",
    name => State#state.log_name,
    data => Data,
    from => From
  }),


  NextIndex = State#state.next_index,
  LogName = State#state.log_name,
  LogServer = State#state.sync_server,
  Waiting = State#state.waiting_for_reply,

  {ok, Log} = gen_server:call(LogServer, {get_log, LogName}),
  logger:notice(#{
    action => "Logging",
    log => Log,
    index => NextIndex,
    data => Data
  }),

  ok = disk_log:log(Log, {NextIndex, Data}),

  % wait for sync reply
  gen_server:cast(LogServer, {sync_log, LogName, self()}),
  receive log_persisted -> ok end,

  logger:info("[~p] Log entry at ~p persisted",
    [State#state.log_name, NextIndex]),

  % index of another request may be up to date, send retry messages
  reply_retry_to_waiting(Waiting),
  {reply, ok, State#state{
    % increase index counter for node by one
    next_index = NextIndex + 1,
    % empty waiting queue
    waiting_for_reply = []
  }};


handle_call(_Request, From, State)
  when State#state.recovering == true ->
  logger:info("[~p] Read, waiting for recovery", [State#state.log_name]),
  Waiting = State#state.waiting_for_reply,
  {noreply, State#state{ waiting_for_reply = Waiting ++ [From] }};

handle_call({read_log_entries, FirstIndex, LastIndex, F, Acc}, _From, State) ->
  LogName = State#state.log_name,
  LogServer = State#state.sync_server,
  Waiting = State#state.waiting_for_reply,
  %% simple implementation, read ALL terms, then filter
  %% can be improved performance wise, stop at last index

  {ok, Log} = gen_server:call(LogServer, {get_log, LogName}),
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

%%noinspection ErlangUnboundVariable
recover_all_logs(LogName, Receiver, LogServer) when is_atom(LogName) ->
  recover_all_logs(atom_to_list(LogName), Receiver, LogServer);
recover_all_logs(LogName, Receiver, LogServer) ->
  % make sure the folder exists
  LogPath = gingko_sync_server:log_dir_base(LogName),
  filelib:ensure_dir(LogPath),

  ProcessLogFile = fun(LogFile, Index) ->
    logger:notice(#{
      action => "Recovering logfile",
      log => LogName,
      file => LogFile
    }),

    {ok, Log} = gen_server:call(LogServer, {get_log, LogName}),

    % read all terms
    Terms = read_all(Log),

    logger:notice(#{
      terms => Terms
    }),

    % For each entry {log_recovery, {Index, Data}} is sent
    SendTerm = fun({LogIndex, Data}, _) -> Receiver ! {log_recovery, {LogIndex, Data}} end,
    lists:foldl(SendTerm, void, Terms),

    case Terms of
      [] -> LastIndex = 0;
      _ -> {LastIndex, _} = hd(lists:reverse(Terms))
    end,

    case Index =< LastIndex of
      true -> logger:info("Jumping from ~p to ~p index", [Index, LastIndex]);
      _ -> logger:emergency("Index corrupt! ~p to ~p jump found", [Index, LastIndex])
    end,

    LastIndex
                   end,

  {ok, LogFiles} = file:list_dir(LogPath),

  % accumulate node -> next free index
  IndexAcc = 0,

  LastIndex = lists:foldl(ProcessLogFile, IndexAcc, LogFiles),

  logger:notice("Receiver: ~p", [Receiver]),
  Receiver ! log_recovery_done,

  LastIndex.


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
%%      fun read_test/1
%%    ]}.
%%
%%% Setup and Cleanup
%%setup() ->
%%  os:putenv("RESET_LOG_FILE", "true"),
%%  ok.
%%
%%cleanup(Pid) ->
%%  gen_server:stop(Pid).
%%
%%
%%read_test(_) ->
%%  fun() ->
%%    {ok, Pid} = gingko_op_log_server:start_link(?LOGGING_MASTER, none),
%%
%%    Entry = #log_operation{
%%      tx_id = 0,
%%      op_type = commit,
%%      log_payload = #update_log_payload{key = a, type = mv_reg , op = {1,1}}},
%%
%%    % write one entry
%%    ok = gingko_op_log:append(Pid, {data}),
%%    ok = gingko_op_log:append(Pid, {Entry}),
%%
%%    % read entry
%%    {ok, [{0, {data}}, {1, {Entry}}]} = gingko_op_log:read_log_entries(Pid, 0, all)
%%  end.
%%
%%%%multi_read_test(Log) ->
%%%%  fun() ->
%%%%    Node = e_multi,
%%%%    start(Node),
%%%%
%%%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX, {data_1}}),
%%%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX + 1, {data_2}}),
%%%%    ok = minidote_op_log:add_log_entry(Log, Node, {?STARTING_INDEX + 2, {data_3}}),
%%%%
%%%%    % read entry
%%%%    {ok, [{?STARTING_INDEX, {data_1}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX, ?STARTING_INDEX),
%%%%    {ok, [{?STARTING_INDEX + 1, {data_2}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX + 1, ?STARTING_INDEX + 1),
%%%%    {ok, [{?STARTING_INDEX + 2, {data_3}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX + 2, ?STARTING_INDEX + 2),
%%%%
%%%%    {ok, [{?STARTING_INDEX + 1, {data_2}}, {?STARTING_INDEX + 2, {data_3}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX + 1, ?STARTING_INDEX + 2),
%%%%    %% OOB
%%%%    {ok, [{?STARTING_INDEX + 1, {data_2}}, {?STARTING_INDEX + 2, {data_3}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX + 1, ?STARTING_INDEX + 3),
%%%%    %% all
%%%%    {ok, [{?STARTING_INDEX + 1, {data_2}}, {?STARTING_INDEX + 2, {data_3}}]} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX + 1, all),
%%%%    fin(Node)
%%%%  end.
%%%%
%%%%empty_read_test(Log) ->
%%%%  fun() ->
%%%%    Node = e_empty,
%%%%    start(Node),
%%%%
%%%%    {ok, []} = minidote_op_log:read_log_entries(Log, Node, ?STARTING_INDEX, all),
%%%%
%%%%    fin(Node)
%%%%  end.
%%
%%-endif.
