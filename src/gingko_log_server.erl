%% @doc The operation log which receives requests and manages recovery of log files.
%% @hidden
-module(gingko_log_server).
-include("gingko.hrl").

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2, handle_info/2]).

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
  LogServerState = create_initial_state_and_recover(JournalLogName, CheckpointLogName, SyncServer),
  {ok, LogServerState}.

-spec handle_call
    ({add_journal_entries, [journal_entry()]}, gen_from(), log_server_state()) ->
  {noreply, log_server_state()} |
  {reply, ok, log_server_state()};
    (get_journal_entries, gen_from(), log_server_state()) ->
  {noreply, log_server_state()} |
  {reply, [journal_entry()], log_server_state()};
    ({add_checkpoints, [checkpoint_entry()]}, gen_from(), log_server_state()) ->
  {noreply, log_server_state()} |
  {reply, ok, log_server_state()};
    ({get_checkpoints, [key_struct()]}, gen_from(), log_server_state()) ->
  {noreply, log_server_state()} |
  {reply, [checkpoint_entry()], log_server_state()}.

handle_call({add_journal_entries, JournalEntries}, _From, State) ->
  logger:info("Add Log Entry"),
  State = add_journal_entries(State, JournalEntries),
  {reply, ok, State};

handle_call(get_journal_entries, _From, State) ->
  List = read_journal_log(State),
  %% TODO handle args consider checkpoints
  {reply, List, State};

handle_call({add_checkpoints, CheckpointEntries}, _From, State) ->
  List = get_checkpoint_log(State),
  %% TODO handle args consider checkpoints
  {reply, List, State};

handle_call({get_checkpoint, KeyStruct}, _From, State) ->
  {reply, get_checkpoint(State, KeyStruct), State};

handle_call({get_checkpoints, KeyStructs}, _From, State) ->
  CheckpointEntries = lists:map(fun(KeyStruct) -> get_checkpoint(State, KeyStruct) end, KeyStructs),
  {reply, CheckpointEntries, State}.

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


-spec create_checkpoint(log_server_state()) -> ok.
create_checkpoint(LogServerState) ->
  %%TODO check journal entries

  ok.

-spec check_journal_entries_of_committed_tx(log_server_state(), [journal_entry()]) -> boolean().
check_journal_entries_of_committed_tx(LogServerState, [JournalEntries]) ->
  FirstJE = hd(JournalEntries),
  TxIdOfFirstJE = FirstJE#journal_entry.tx_id,
  AllTxIdsAreEqual = lists:all(fun(JE) -> JE#journal_entry.tx_id == TxIdOfFirstJE end, JournalEntries),
  CorrectTimeStamps = lists:foldl(fun(JE, {PreviousJE, AreTimestampsOrdered}) ->
    AreTimestampsOrdered andalso JE#journal_entry.rt_timestamp >= PreviousJE#journal_entry.rt_timestamp end, {FirstJE, true}, JournalEntries),
  StartsWithBeginTxn = materializer:is_journal_entry_of_specific_system_operation(begin_txn, hd(JournalEntries), 0),
  ReversedJEsWithoutBeginTxn = lists:reverse(tl(JournalEntries)),
  IsLastJECommitTxn = materializer:is_journal_entry_of_specific_system_operation(commit_txn, hd(ReversedJEsWithoutBeginTxn), 0),
  Is2ndLastJEPrepareTxn = materializer:is_journal_entry_of_specific_system_operation(prepare_txn, hd(tl(ReversedJEsWithoutBeginTxn)), 0),
  ReversedJEsWithoutBeginTxnPrepareTxnCommitTxn = tl(tl(ReversedJEsWithoutBeginTxn)),
  ContainsNonCheckpointSystemOperations = lists:any(fun(JE) ->
    materializer:is_system_operation_that_is_not_checkpoint(JE) end, ReversedJEsWithoutBeginTxnPrepareTxnCommitTxn),
  %%TODO check that keys exist in the current shard
  AllKeysBelongToShard = true,
  ObjectOperations = lists:filter(fun(JE) ->
    is_record(JE#journal_entry.operation, object_operation) end, ReversedJEsWithoutBeginTxn),
  ObjectOperations = lists:all(fun(JE) ->
    Type = JE#journal_entry.operation#object_operation.key_struct#key_struct.type,
    Op = JE#journal_entry.operation#object_operation.op_type,
    antidote_crdt:is_type(Type) andalso Type:is_operation(Op)
                               end, ObjectOperations),
  %%TODO check commit time
  CommitTimeIsInAllowedRange = true,
  AllTxIdsAreEqual andalso CorrectTimeStamps andalso StartsWithBeginTxn andalso IsLastJECommitTxn andalso Is2ndLastJEPrepareTxn andalso (not ContainsNonCheckpointSystemOperations) andalso AllKeysBelongToShard andalso ObjectOperations andalso CommitTimeIsInAllowedRange.

-spec get_current_value_for_key(log_server_state(), key_struct()) -> term().
get_current_value_for_key(LogServerState, KeyStruct) ->
  Checkpoint = get_checkpoint(LogServerState, KeyStruct), %%TODO what happens if checkpoint does not exist yet (create new snapshot!)
  JournalEntries = read_journal_log(LogServerState),
  materializer:materialize_snapshot(Checkpoint, JournalEntries).


-spec add_journal_entries(log_server_state(), [journal_entry()]) -> ok.
add_journal_entries(LogServerState, JournalEntries) ->
  JournalLog = get_journal_log(LogServerState),
  ok = disk_log:log_terms(JournalLog, JournalEntries),
  gen_server:call(LogServerState#log_server_state.sync_server, sync_log),
  ok.

-spec get_journal_log(log_server_state()) -> log().
get_journal_log(LogServerState) ->
  LogDataStructure = LogServerState#log_server_state.log_data_structure,
  LogDataStructure#log_data_structure.persistent_journal_log.

-spec add_checkpoint_entries(log_server_state(), [checkpoint_entry()]) -> ok.
add_checkpoint_entries(LogServerState, CheckpointEntries) ->
  CheckpointLog = get_checkpoint_log(LogServerState),
  ok = disk_log:log_terms(CheckpointLog, CheckpointEntries),
  gen_server:call(LogServerState#log_server_state.sync_server, sync_log),
  ok.

-spec get_checkpoint_log(log_server_state()) -> log().
get_checkpoint_log(LogServerState) ->
  LogDataStructure = LogServerState#log_server_state.log_data_structure,
  LogDataStructure#log_data_structure.persistent_checkpoint_log.

-spec get_checkpoint(log_server_state(), key_struct()) -> checkpoint_entry() | not_existing.
get_checkpoint(LogServerState, KeyStruct) ->
  CheckpointLog = get_checkpoint_log(LogServerState),
  CheckpointDict = read_checkpoint_log_from_disk(CheckpointLog), %%TODO change to find in log
  CheckpointEntry = dict:find(KeyStruct, CheckpointDict),
  case CheckpointEntry of
    {ok, Value} -> Value;
    error -> #checkpoint_entry{key_struct = KeyStruct, rt_timestamp = 0, snapshot = KeyStruct#key_struct.type:new()}
  end.

-spec read_journal_log(log_server_state()) -> [journal_entry()].
read_journal_log(LogServerState) ->
  LogServerState#log_data_structure.volatile_journal_log.

%% @doc reads all terms from given log
-spec read_journal_log_from_disk(log()) -> [journal_entry()].
read_journal_log_from_disk(JournalLog) ->
  read_journal_log_from_disk(JournalLog, [], start).

-spec read_journal_log_from_disk(log(), [journal_entry()], continuation()) -> [journal_entry()].
read_journal_log_from_disk(JournalLog, Terms, Cont) ->
  case disk_log:chunk(JournalLog, Cont) of
    eof -> Terms;
    {Cont2, ReadTerms} -> read_journal_log_from_disk(JournalLog, Terms ++ ReadTerms, Cont2)
  end.

-spec read_checkpoint_log_from_disk(log()) -> dict().
read_checkpoint_log_from_disk(CheckpointLog) ->
  case disk_log:chunk(CheckpointLog, start) of
    eof -> dict:new();
    {_Cont, ReadTerms} -> ReadTerms
  end.

-spec create_initial_state(string(), string(), pid()) -> log_server_state().
create_initial_state(JournalLogName, CheckpointLogName, SyncServer) ->
  #log_server_state{
    journal_log_name = JournalLogName,
    checkpoint_log_name = CheckpointLogName,
    log_data_structure = not_open,
    sync_server = SyncServer
  }.

-spec create_initial_state_and_recover(string(), string(), pid()) -> log_server_state().
create_initial_state_and_recover(JournalLogName, CheckpointLogName, SyncServer) ->
  LogServerState = create_initial_state(JournalLogName, CheckpointLogName, SyncServer),
  create_updated_state(LogServerState).

-spec create_updated_state(log_server_state()) -> log_server_state().
create_updated_state(LogServerState) ->
  SyncServerState = gen_server:call(LogServerState#log_server_state.sync_server, get_log),
  create_updated_state(LogServerState, SyncServerState).

-spec create_updated_state(log_server_state(), sync_server_state()) -> log_server_state().
create_updated_state(LogServerState, SyncServerState) ->
  JournalLog = SyncServerState#sync_server_state.journal_log,
  CheckpointLog = SyncServerState#sync_server_state.checkpoint_log,
  NewLogDataStructure = #log_data_structure{
    volatile_journal_log = read_journal_log_from_disk(JournalLog),
    persistent_journal_log = JournalLog,
    persistent_checkpoint_log = CheckpointLog},
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
