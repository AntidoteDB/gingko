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

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2,  handle_info/2]).

%% ==============
%% API
%% Internal api documentation and types for the gingko op log gen_server
%% ==============

%%%===================================================================
%%% State
%%%===================================================================


% log starts with this default index
-define(STARTING_INDEX, 0).


%% @doc Starts the op log server for given server name and recovery receiver process
-spec start_link(term()) -> {ok, pid()}.
start_link({JournalLogName, CheckpointLogName}) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {JournalLogName, CheckpointLogName}, []).


%% @doc Initializes the internal server state
-spec init({{string(), string()}, pid()}) -> {ok, #log_server_state{}}.
init({JournalLogName, CheckpointLogName}) ->
  logger:notice(#{
    action => "Starting op log server",
    registered_as => ?MODULE,
    name => JournalLogName
  }),

  {ok, SyncServer} = gingko_sync_server:start_link({JournalLogName, CheckpointLogName}),
  SyncServerState = gen_server:call(SyncServer, get_log),
  InitialLogServerState = create_initial_state(JournalLogName, CheckpointLogName, SyncServer),
  UpdatedLogServerState = create_updated_state(InitialLogServerState, SyncServerState),
  {ok, UpdatedLogServerState}.

%% @doc Either
%% 1) adds a log entry for given node and a {Index, Data} pair
%% or
%% 2) reads the log
-spec handle_call
    ({add_log_entry, [journal_entry()]}, gen_from(), #log_server_state{}) ->
  {noreply, #log_server_state{}} | %% if still recovering
  {reply, {error, index_already_written}, #log_server_state{}} | %% if index is already written
  {reply, ok, #log_server_state{}}; %% entry is persisted
    ({read_log_entries, any(), integer(), integer(), fun((log_entry(), Acc) -> Acc), Acc}, gen_from(), #log_server_state{}) ->
  {noreply, #log_server_state{}} | %% if still recovering or index for given node behind
  {reply, {ok, Acc}, #log_server_state{}}. %% accumulated entries

handle_call({add_log_entry, JournalEntries}, From, State) ->
  logger:notice(#{
    action => "Append to log",
    name => State#log_server_state.journal_log_name,
    data => JournalEntries,
    from => From
  }),

  JournalLog = get_journal_log(State),
  SyncServer = State#log_server_state.sync_server,

  ok = disk_log:log_terms(JournalLog, JournalEntries),

  % wait for sync reply
  gen_server:call(SyncServer, sync_log),
  SyncServerState = gen_server:call(SyncServer, get_log),
  State = create_updated_state(State, SyncServerState), %%TODO can be done better
  logger:info("[~p] Log entry at ~p persisted", [State#log_server_state.journal_log_name]),

  {reply, ok, State};

handle_call({read_log_entries, Args}, _From, State) ->
  List = read_journal_log_efficient(State),
  %% TODO handle args consider checkpoints
  {reply, List, State}.

handle_cast(Msg, State) ->
  logger:warning("[~p] Swallowing unexpected message: ~p", [State#log_server_state.journal_log_name, Msg]),
  {noreply, State}.


terminate(_Reason, State) ->
  gen_server:stop(State#log_server_state.sync_server),
  ok.

handle_info(Msg, State) ->
  logger:warning("Swallowing unexpected message: ~p", [Msg]),
  {noreply, State}.




%%%===================================================================
%%% Private Functions Implementation
%%%===================================================================

-spec get_journal_log(log_server_state()) -> log().
get_journal_log(LogServerState) ->
  LogDataStructure = LogServerState#log_server_state.log_data_structure,
  LogDataStructure#log_data_structure.persistent_journal_log.

-spec get_checkpoint_log(log_server_state()) -> log().
get_checkpoint_log(LogServerState) ->
  LogDataStructure = LogServerState#log_server_state.log_data_structure,
  LogDataStructure#log_data_structure.persistent_checkpoint_log.

%% @doc reads all terms from given log
-spec read_journal_log_efficient(log_server_state()) -> [journal_entry()].
read_journal_log_efficient(LogServerState) ->
  LogDataStructure = LogServerState#log_server_state.log_data_structure,
  LogDataStructure#log_data_structure.journal_entry_list.

-spec get_checkpoints_efficient(log_server_state()) -> dict().
get_checkpoints_efficient(LogServerState) ->
  LogDataStructure = LogServerState#log_server_state.log_data_structure,
  LogDataStructure#log_data_structure.checkpoint_key_value_map.

%% @doc reads all terms from given log
-spec read_journal_log(log()) -> [journal_entry()].
read_journal_log(Log) ->
  read_journal_log(Log, [], start).

-spec read_journal_log(log(), [journal_entry()], continuation()) -> [journal_entry()].
read_journal_log(Log, Terms, Cont) ->
  case disk_log:chunk(Log, Cont) of
    eof -> Terms;
    {Cont2, ReadTerms} -> read_journal_log(Log, Terms ++ ReadTerms, Cont2)
  end.

-spec read_checkpoint_log(log()) -> dict().
read_checkpoint_log(Log) ->
  case disk_log:chunk(Log, start) of
    eof -> dict:new();
    {_Cont, ReadTerms} -> ReadTerms
  end.

-spec create_initial_state(string(), string(), pid()) -> log_server_state().
create_initial_state(JournalLogName, CheckpointLogName, LogServer) ->
  #log_server_state{
    journal_log_name = JournalLogName,
    checkpoint_log_name = CheckpointLogName,
    log_data_structure = not_open,
    sync_server = LogServer
  }.

-spec create_updated_state(log_server_state(), sync_server_state()) -> log_server_state().
create_updated_state(LogServerState, SyncServerState) ->
  JournalLog = SyncServerState#sync_server_state.journal_log,
  CheckpointLog = SyncServerState#sync_server_state.checkpoint_log,
  NewLogDataStructure = #log_data_structure{
                            persistent_journal_log = JournalLog,
                            journal_entry_list = read_journal_log(JournalLog),
                            persistent_checkpoint_log = CheckpointLog,
                            checkpoint_key_value_map = read_checkpoint_log(CheckpointLog) },
  LogServerState#log_server_state{
    log_data_structure = NewLogDataStructure
  }.



















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
