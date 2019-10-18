%% @doc The operation log which receives requests and manages recovery of log files.
%% @hidden
-module(gingko_log_server).
-include("gingko.hrl").
-include("materializer.erl").

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
  LogServerState = create_initial_state_and_recover(JournalLogName, CheckpointLogName, SyncServer),
  {ok, LogServerState}.

%% @doc Either
%% 1) adds a log entry for given node and a {Index, Data} pair
%% or
%% 2) reads the log
-spec handle_call
    ({add_journal_entries, [journal_entry()]}, gen_from(), #log_server_state{}) ->
  {noreply, #log_server_state{}} | %% if still recovering
  {reply, {error, index_already_written}, #log_server_state{}} | %% if index is already written
  {reply, ok, #log_server_state{}}; %% entry is persisted
    ({get_journal_entries, any(), integer(), integer(), fun((log_entry(), Acc) -> Acc), Acc}, gen_from(), #log_server_state{}) ->
  {noreply, #log_server_state{}} | %% if still recovering or index for given node behind
  {reply, {ok, Acc}, #log_server_state{}}. %% accumulated entries

handle_call({add_journal_entries, JournalEntries}, _From, State) ->
  logger:info("Add Log Entry"),
  State = append_journal_entries(State, JournalEntries),
  {reply, ok, State};

handle_call(get_journal_entries, _From, State) ->
  List = read_journal_log(State),
  %% TODO handle args consider checkpoints
  {reply, List, State};

handle_call({get_checkpoints, Keys}, _From, State) ->
  List = get_checkpoint_log(State),
  %% TODO handle args consider checkpoints
  {reply, List, State};

handle_call(get_all_checkpoints, _From, State) ->
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


-spec check_journal_entries_of_committed_tx(log_server_state(), [journal_entry()]) -> boolean().
check_journal_entries_of_committed_tx(LogServerState, [JournalEntries]) ->
  FirstJE = hd(JournalEntries),
  TxIdOfFirstJE = FirstJE#journal_entry.tx_id,
  AllTxIdsAreEqual = lists:all(fun(JE) -> JE#journal_entry.tx_id == TxIdOfFirstJE end, JournalEntries),
  CorrectTimeStamps = lists:foldl(fun(JE, {PreviousJE, AreTimestampsOrdered}) -> AreTimestampsOrdered andalso JE#journal_entry.rt_timestamp >= PreviousJE#journal_entry.rt_timestamp end, {FirstJE, true}, JournalEntries),
  StartsWithBeginTxn = is_journal_entry_of_specific_system_operation(begin_txn, hd(JournalEntries), 0),
  ReversedJEsWithoutBeginTxn = lists:reverse(tl(JournalEntries)),
  IsLastJECommitTxn = is_journal_entry_of_specific_system_operation(commit_txn, hd(ReversedJEsWithoutBeginTxn), 0),
  Is2ndLastJEPrepareTxn = is_journal_entry_of_specific_system_operation(prepare_txn, hd(tl(ReversedJEsWithoutBeginTxn)), 0),
  ReversedJEsWithoutBeginTxnPrepareTxnCommitTxn = tl(tl(ReversedJEsWithoutBeginTxn)),
  ContainsNonCheckpointSystemOperations = lists:any(fun(JE) -> is_system_operation_that_is_not_checkpoint(JE) end, ReversedJEsWithoutBeginTxnPrepareTxnCommitTxn),
  %%TODO check that keys exist in the current shard
  AllKeysBelongToShard = true,
  ObjectOperations = lists:filter(fun(JE) -> is_record(JE#journal_entry.operation, object_operation) end, ReversedJEsWithoutBeginTxn),
  ObjectOperations = lists:all(fun(JE) ->
    Type = JE#journal_entry.operation#object_operation.key_struct#key_struct.type,
    Op = JE#journal_entry.operation#object_operation.op_type,
    antidote_crdt:is_type(Type) andalso Type:is_operation(Op)
  end, ObjectOperations),
  %%TODO check commit time
  CommitTimeIsInAllowedRange = true,
  AllTxIdsAreEqual andalso CorrectTimeStamps andalso StartsWithBeginTxn andalso IsLastJECommitTxn andalso Is2ndLastJEPrepareTxn andalso (not ContainsNonCheckpointSystemOperations) andalso AllKeysBelongToShard andalso ObjectOperations andalso CommitTimeIsInAllowedRange.


%%TODO complex handling of checkpoints in txns
-spec is_system_operation_that_is_not_checkpoint(journal_entry()) -> boolean().
is_system_operation_that_is_not_checkpoint(JournalEntry) ->
  Operation = JournalEntry#journal_entry.operation,
  if
    is_record(Operation, system_operation) -> Operation#system_operation.op_type /= checkpoint;
    true -> false
  end.

-spec is_journal_entry_update_and_contains_key(key_struct(), journal_entry()) -> boolean().
is_journal_entry_update_and_contains_key(KeyStruct, JournalEntry) ->
  Operation = JournalEntry#journal_entry.operation,
  if
    is_record(Operation, object_operation) -> Operation#object_operation.key_struct == KeyStruct andalso Operation#object_operation.op_type == update;
    true -> false
  end.

-spec contains_journal_entry_of_specific_system_operation(system_operation_type(), [journal_entry()], clock_time()) -> boolean().
contains_journal_entry_of_specific_system_operation(SystemOperationType, JournalEntries, AfterRtTimestamp) ->
  lists:any(fun(J) -> is_journal_entry_of_specific_system_operation(SystemOperationType, J, AfterRtTimestamp) end, JournalEntries).

-spec is_journal_entry_of_specific_system_operation(system_operation_type(), journal_entry(), clock_time()) -> boolean().
is_journal_entry_of_specific_system_operation(SystemOperationType, JournalEntry, AfterRtTimestamp) ->
  Operation = JournalEntry#journal_entry.operation,
  if
    is_record(Operation, system_operation) -> Operation#system_operation.op_type == SystemOperationType andalso JournalEntry#journal_entry.rt_timestamp > AfterRtTimestamp;
    true -> false
  end.

-spec group_by(fun((ListType :: term()) -> GroupKeyType :: term()), [ListType :: term()]) -> [{GroupKeyType :: term(), [ListType :: term()]}].
group_by(Fun, List) -> lists:foldr(fun({Key, Value}, Dict) -> dict:append(Key, Value, Dict) end , dict:new(), [ {Fun(X), X} || X <- List ]).

-spec get_indexed_journal_entries([journal_entry()]) -> [{non_neg_integer(), journal_entry()}].
get_indexed_journal_entries(JournalEntries) ->
  lists:mapfoldl(fun(J, Index) -> {{Index, J}, Index + 1} end, 0, JournalEntries).

-spec separate_commit_from_update_journal_entries([journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries(JList) ->
  separate_commit_from_update_journal_entries(JList, []).
-spec separate_commit_from_update_journal_entries([journal_entry()], [journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries([CommitJournalEntry], JournalEntries) ->
  {CommitJournalEntry, JournalEntries};
separate_commit_from_update_journal_entries([UpdateJournalEntry | OtherJournalEntries], JournalEntries) ->
  separate_commit_from_update_journal_entries(OtherJournalEntries, [UpdateJournalEntry | JournalEntries]).

-spec transform_to_clock_si_payload(journal_entry(), journal_entry()) -> clocksi_payload().
transform_to_clock_si_payload(CommitJournalEntry, JournalEntry) ->
  #clocksi_payload{
    key_struct = JournalEntry#journal_entry.operation#object_operation.key_struct,
    op_param = JournalEntry#journal_entry.operation#object_operation.op_args,
    snapshot_time = CommitJournalEntry#journal_entry.operation#system_operation.op_args#commit_txn_args.snapshot_time,
    commit_time = CommitJournalEntry#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_time,
    tx_id = CommitJournalEntry#journal_entry.tx_id
  }.

-spec get_committed_journal_entries_for_key(key_struct(), [journal_entry()], clock_time()) -> [{journal_entry(), [journal_entry()]}].
get_committed_journal_entries_for_key(KeyStruct, JournalEntries, AfterRtTimestamp) ->
  RelevantJournalEntries = lists:filter(fun(J) -> is_journal_entry_update_and_contains_key(KeyStruct, J) orelse is_journal_entry_of_specific_system_operation(commit_txn, J, AfterRtTimestamp) end, JournalEntries),
  IndexedJournalEntries = get_indexed_journal_entries(RelevantJournalEntries),
  JournalEntryToIndex = dict:from_list(lists:map(fun({Index, J}) -> {J, Index} end, IndexedJournalEntries)),
  TxIdToJournalEntries = group_by(fun({_Index, J}) -> J#journal_entry.tx_id end, RelevantJournalEntries),
  TxIdToJournalEntries = dict:filter(fun({_TxId, JList}) -> contains_journal_entry_of_specific_system_operation(commit_txn, JList, AfterRtTimestamp) end, TxIdToJournalEntries),
  TxIdToJournalEntries = dict:map(fun(_TxId, JList) -> lists:sort(fun(J1, J2) -> dict:find(J1, JournalEntryToIndex) > dict:find(J2, JournalEntryToIndex) end, JList) end, TxIdToJournalEntries),

  ListOfLists = lists:map(fun({_TxId, JList}) -> separate_commit_from_update_journal_entries(JList) end, dict:to_list(TxIdToJournalEntries)),
  ListOfLists = lists:sort(fun({J1, _JList1}, {J2, _JList2}) -> dict:find(J1, JournalEntryToIndex) > dict:find(J2, JournalEntryToIndex) end, ListOfLists),
  ListOfLists.

-spec get_current_value_for_key(log_server_state(), key_struct()) -> term().
get_current_value_for_key(LogServerState, KeyStruct) ->
  Checkpoint = get_checkpoint(LogServerState, KeyStruct), %%TODO what happens if checkpoint does not exist yet (create new snapshot!)
  JournalEntries = read_journal_log(get_journal_log(LogServerState)),
  CommittedJournalEntries = get_committed_journal_entries_for_key(KeyStruct, JournalEntries, Checkpoint#checkpoint_entry.rt_timestamp),
  ClockSIPayloads = lists:map(fun({J1, JList}) -> lists:map(fun(J) -> transform_to_clock_si_payload(J1, J) end, JList)
                          end, CommittedJournalEntries),
  lists:merge(ClockSIPayloads),
  materializer:materialize_clocksi_payload(Checkpoint#checkpoint_entry.key_struct#key_struct.type, Checkpoint#checkpoint_entry.value, ClockSIPayloads).


-spec append_journal_entries(log_server_state(), [journal_entry()]) -> log_server_state().
append_journal_entries(LogServerState, JournalEntries) ->
  JournalLog = get_journal_log(LogServerState),
  ok = disk_log:log_terms(JournalLog, JournalEntries),
  gen_server:call(LogServerState#log_server_state.sync_server, sync_log).

-spec get_journal_log(log_server_state()) -> log().
get_journal_log(LogServerState) ->
  LogDataStructure = LogServerState#log_server_state.log_data_structure,
  LogDataStructure#log_data_structure.persistent_journal_log.

-spec get_checkpoint_log(log_server_state()) -> log().
get_checkpoint_log(LogServerState) ->
  LogDataStructure = LogServerState#log_server_state.log_data_structure,
  LogDataStructure#log_data_structure.persistent_checkpoint_log.

-spec get_checkpoint(log_server_state(), key()) -> checkpoint_entry() | not_existing.
get_checkpoint(LogServerState, Key) ->
  CheckpointLog = get_checkpoint_log(LogServerState),
  CheckpointDict = read_checkpoint_log(CheckpointLog),
  CheckpointEntry = dict:find(Key, CheckpointDict),
  case CheckpointEntry of
    {ok, Value} -> Value;
    error -> not_existing
  end.

%% @doc reads all terms from given log
-spec read_journal_log(log()) -> [journal_entry()].
read_journal_log(JournalLog) ->
  read_journal_log(JournalLog, [], start).

-spec read_journal_log(log(), [journal_entry()], continuation()) -> [journal_entry()].
read_journal_log(JournalLog, Terms, Cont) ->
  case disk_log:chunk(JournalLog, Cont) of
    eof -> Terms;
    {Cont2, ReadTerms} -> read_journal_log(JournalLog, Terms ++ ReadTerms, Cont2)
  end.

-spec read_checkpoint_log(log()) -> dict().
read_checkpoint_log(CheckpointLog) ->
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
