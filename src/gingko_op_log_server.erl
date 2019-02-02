%% @doc The operation log which receives requests and manages recovery of log files.
%% @hidden
-module(gingko_op_log_server).
-include("gingko.hrl").

-behaviour(gen_server).

%% TODO
-type server() :: any().
-type log() :: any().
-type log_entry() :: any().
-type gen_from() :: any().

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2,  handle_info/2]).

%% ==============
%% API
%% Internal api documentation and types for the gingko op log gen_server
%% ==============

%%%===================================================================
%%% State
%%%===================================================================

-record(state, {
  % recovery related state
  % receiver of the recovered log messages
  recovery_receiver :: pid(),
  % if recovering reply with busy status to requests
  recovering :: true | false,

  % log name, used for storing logs in a directory related to the name
  log_name :: atom(),
  % requests waiting until log is not recovering anymore or missing indices have been updated
  waiting_for_reply = [] :: [any()],
  % handles syncing and opening the log
  sync_server :: pid(),

  % stores current writable index
  next_index :: integer()
}).


% log starts with this default index
-define(STARTING_INDEX, 0).


%% @doc Starts the op log server for given server name and recovery receiver process
-spec start_link(term(), pid()) -> {ok, pid()}.
start_link(LogName, RecoveryReceiver) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {LogName, RecoveryReceiver}, []).


%% @doc Initializes the internal server state
-spec init({node(), pid()}) -> {ok, #state{}}.
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
    sync_server = LogServer,
    next_index = ?STARTING_INDEX
  }}.


%% ------------------
%% ASYNC LOG RECOVERY
%% ------------------

%% @doc Either
%% 1) starts async recovery and does not reply until recovery is finished
%% or
%% 2) finishes async recovery and replies to waiting processes
-spec handle_cast
    (start_recovery, #state{}) -> {noreply, #state{}};          %1)
    ({finish_recovery, #{}}, #state{}) -> {noreply, #state{}}.  %2)
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


%% @doc Either
%% 1) adds a log entry for given node and a {Index, Data} pair
%% or
%% 2) reads the log
-spec handle_call
    ({add_log_entry, log_entry()}, gen_from(), #state{}) ->
  {noreply, #state{}} | %% if still recovering
  {reply, {error, index_already_written}, #state{}} | %% if index is already written
  {reply, ok, #state{}}; %% entry is persisted
    ({read_log_entries, any(), integer(), integer(), fun((log_entry(), Acc) -> Acc), Acc}, gen_from(), #state{}) ->
  {noreply, #state{}} | %% if still recovering or index for given node behind
  {reply, {ok, Acc}, #state{}}. %% accumulated entries

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

%% @doc Replies a 'retry' message to all waiting process to retry their action again
-spec reply_retry_to_waiting([gen_from()]) -> ok.
reply_retry_to_waiting(WaitingProcesses) ->
  Reply = fun(Process, _) -> gen_server:reply(Process, retry) end,
  lists:foldl(Reply, void, WaitingProcesses),
  ok.


%% @doc recovers all logs for given server name
%%      the server name should be the local one
%%      also ensures that the directory actually exists
%%
%% sends pid() ! {log_recovery, Node, {Index, Data}}
%%      for each entry in one log
%%
%% sends pid() ! {log_recovery_done}
%%      once after processing finished
-spec recover_all_logs(server(), pid(), pid()) -> any().
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


%% @doc reads all terms from given log
-spec read_all(log()) -> [term()].
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
