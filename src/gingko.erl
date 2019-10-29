%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Antidote Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(gingko).
-author("kevin").
-include("gingko.hrl").

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

-record(state, {}).

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
  {ok, State :: #state{}} |
  {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} |
  ignore).
init([]) ->
  {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
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
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
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
    State :: #state{}) -> term()).
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
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%====================================================================
%% API functions
%%====================================================================

-spec perform_transaction(tx_id(), [key_operation()]) -> ok.
perform_transaction(TxId, KeyOperations) ->
  BeginOperation = create_begin_operation(),
  UpdateOperations = lists:map(fun(KeyOperation) ->
  case KeyOperation of
    {read, KeyStruct} -> create_read_operation(KeyStruct, []);
    {update, KeyStruct, Effect} -> create_update_operation(KeyStruct, Effect)
  end
  end, KeyOperations),
  PrepareOperation = create_prepare_operation(),
  CommitOperation = create_commit_operation(),
  %%TODO
  ok.


update(KeyStruct, TxId, DownstreamOp) ->
  Operation = create_update_operation(KeyStruct, DownstreamOp),
  append_journal_entry(TxId, Operation).

%% @doc Retrieves a materialized version of the object at given key with expected given type.
%% If MaximumSnapshotTime is given, then the version is guaranteed to not be older than the given snapshot.
%%
%% Example usage:
%%
%% Operations of a counter @my_counter in the log: +1, +1, -1, +1(not committed), -1(not committed).
%%
%% 2 = get_version(my_counter, antidote_crdt_counter_pn, undefined)
%%
%% @param Key the Key under which the object is stored
%% @param Type the expected CRDT type of the object
%% @param MaximumSnapshotTime if not 'undefined', then retrieves the latest object version which is not older than this timestamp
-spec read(key_struct(), tx_id(), snapshot_time()) -> {ok, snapshot()}.
read(KeyStruct, TxId, MaximumSnapshotTime) ->

  logger:info("Read"),

  %% This part needs caching/optimization
  %% Currently the steps to materialize are as follows:
  %% * read ALL log entries from the persistent log file
  %% * filter log entries by key
  %% * filter furthermore only by committed operations
  %% * materialize operations into materialized version
  %% * return that materialized version

  %% TODO Get up to time SnapshotTime instead of all
  {ok, JournalEntries} = read_journal_entries(?LOGGING_MASTER, 0, all),
  logger:debug(#{step => "unfiltered log", payload => JournalEntries, snapshot_timestamp => MaximumSnapshotTime}),
  Key = KeyStruct#key_struct.key,
  Type = KeyStruct#key_struct.type,
  {Ops, CommittedOps} = log_utilities:filter_terms_for_key(JournalEntries, {key, Key}, undefined, MaximumSnapshotTime, dict:new(), dict:new()),
  logger:debug(#{step => "filtered terms", ops => Ops, committed => CommittedOps}),

  case dict:find(Key, CommittedOps) of
    {ok, PayloadForKey} -> PayloadForKey = PayloadForKey;
    error -> PayloadForKey = []
  end,

  MaterializedObject = materializer:materialize_clocksi_payload(Type, materializer:create_snapshot(Type), PayloadForKey),
  logger:info(#{step => "materialize", materialized => MaterializedObject}),

  {ok, MaterializedObject}.


%% @doc Applies an update for the given key for given transaction id with a calculated valid downstream operation.
%% It is currently not checked if the downstream operation is valid for given type.
%% Invalid downstream operations will corrupt a key, which will cause get_version to throw an error upon invocation.
%%
%% A update log record consists of the transaction id, the op_type 'update' and the actual payload.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%%
%% @param Key the Key under which the object is stored
%% @param Type the expected CRDT type of the object
%% @param TransactionId the id of the transaction this update belongs to
%% @param DownstreamOp the calculated downstream operation of a CRDT update
-spec update(key_struct(), tx_id(), op()) -> ok | {error, reason()}.
update(KeyStruct, TxId, DownstreamOp) ->
  logger:info("Update"),
  Operation = create_update_operation(KeyStruct, DownstreamOp),
  append_journal_entry(TxId, Operation).

-spec begin_txn(tx_id()) -> ok.
begin_txn(TxId) ->
  logger:info("Begin"),
  Operation = create_begin_operation(),
  append_journal_entry(TxId, Operation).

-spec prepare_txn(tx_id(), non_neg_integer()) -> ok.
prepare_txn(TxId, PrepareTime) ->
  logger:info("Prepare"),
  Operation = create_prepare_operation(PrepareTime),
  append_journal_entry(TxId, Operation).



%% @doc Commits all operations belonging to given transaction id for given list of keys.
%%
%% A commit log record consists of the transaction id, the op_type 'commit'
%% and the actual payload which consists of the commit time and the snapshot time.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%%
%% @param Keys list of keys to commit
%% @param TransactionId the id of the transaction this commit belongs to
%% @param CommitTime TODO
%% @param SnapshotTime TODO
-spec commit_txn(tx_id(), dc_and_commit_time(), snapshot_time()) -> ok.
commit_txn(TxId, CommitTime, SnapshotTime) ->
  logger:info("Commit"),
  Operation = create_commit_operation(CommitTime, SnapshotTime),
  append_journal_entry(TxId, Operation).


%% @doc Aborts all operations belonging to given transaction id for given list of keys.
%%
%% An abort log record consists of the transaction id, the op_type 'abort'
%% and the actual payload which is empty.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%%
%% @param Keys list of keys to abort a transaction
%% @param TransactionId the id of the transaction to abort
-spec abort_txn(tx_id()) -> ok.
abort_txn(TxId) ->
  logger:info("Abort"),
  Operation = create_abort_operation(),
  append_journal_entry(TxId, Operation).

%%TODO
-spec checkpoint() -> ok.
checkpoint() ->
  logger:info("Checkpoint").


-spec append_journal_entry(tx_id(), operation()) -> ok.
append_journal_entry(TxId, Operation) ->
  Entry = create_journal_entry(TxId, Operation),
  append([Entry]).

%% TODO add JSN (must be unique but they may be created concurrently)
-spec create_journal_entry(tx_id(), operation()) -> journal_entry().
create_journal_entry(TxId, Operation) ->
  #journal_entry{
    uuid = uuidgenerator:generate(),
    rt_timestamp = erlang:timestamp(),
    tx_id = TxId,
    operation = Operation
  }.

-spec create_read_operation(key_struct(), term()) -> object_operation().
create_read_operation(KeyStruct, Args) ->
  #object_operation{
    key_struct = KeyStruct,
    op_type = read,
    op_args = Args
  }.

-spec create_update_operation(key_struct(), term()) -> object_operation().
create_update_operation(KeyStruct, Args) ->
  #object_operation{
    key_struct = KeyStruct,
    op_type = update,
    op_args = Args
  }.

-spec create_begin_operation() -> system_operation().
create_begin_operation() ->
  #system_operation{
    op_type = begin_txn,
    op_args = #begin_txn_args{}
  }.

-spec create_prepare_operation(non_neg_integer()) -> system_operation().
create_prepare_operation(PrepareTime) ->
  #system_operation{
    op_type = prepare_txn,
    op_args = #prepare_txn_args{prepare_time = PrepareTime}
  }.

-spec create_commit_operation(dc_and_commit_time(), snapshot_time()) -> system_operation().
create_commit_operation(CommitTime, SnapshotTime) ->
  #system_operation{
    op_type = commit_txn,
    op_args = #commit_txn_args{commit_time = CommitTime, snapshot_time = SnapshotTime}
  }.

-spec create_abort_operation() -> system_operation().
create_abort_operation() ->
  #system_operation{
    op_type = abort_txn,
    op_args = #abort_txn_args{}
  }.

-spec append([journal_entry()]) -> dict(). %%PID to Genserver result
append(JournalEntries) ->
  ShardServers = get_all_shard_log_servers(),
  Map = dict:from_list(lists:map(fun(JournalEntry) -> {JournalEntry, find_shard_log_servers(JournalEntry, ShardServers)} end, JournalEntries)),
  List = dict:to_list(do_thing(dict:new(), Map)),
  List = lists:map(fun({ShardServerPid, JournalEntries}) -> {ShardServerPid, gen_server:call(ShardServerPid, {add_journal_entries, JournalEntries})} end, List).

do_thing(Dict, []) ->
  Dict;
do_thing(Dict, [{_JournalEntry, []}|OtherMappings]) ->
  do_thing(Dict, OtherMappings);
do_thing(Dict, [{JournalEntry, [ShardServer|ShardServers]}|OtherMappings]) ->
  Dict = log_utilities:add_to_value_list_or_create_single_value_list(Dict, ShardServer, JournalEntry),
  Dict = do_thing(Dict, [{JournalEntry, ShardServers}|OtherMappings]).

%%TODO implement
-spec get_all_shard_log_servers() -> [pid()].
get_all_shard_log_servers() ->
  [?LOGGING_MASTER].

%%TODO implement
-spec find_shard_log_servers(journal_entry(), [pid()]) -> [pid()].
find_shard_log_servers(KeyStruct, Pids) ->
  Pids.

-spec read_journal_entries(node()) -> [journal_entry()].
read_journal_entries(LogNode) ->
  case gen_server:call(LogNode, {get_journal_entries, all}) of
    retry -> logger:debug("Retrying request"), read_journal_entries(LogNode);
    Reply -> Reply
  end.

-spec read_journal_entries(node(), key()) -> [journal_entry()].
read_journal_entries(LogNode, Key) ->
  case gen_server:call(LogNode, {get_journal_entries, Key}) of
    retry -> logger:debug("Retrying request"), read_journal_entries(LogNode, Key);
    Reply -> Reply
  end.