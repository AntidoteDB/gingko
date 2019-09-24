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

%% @doc The main interface for the persistent backend for CRDT objects called gingko.
%% The API provides functions to update, commit, abort, and read (get_version) keys.
%% Stable snapshots are currently not implemented, thus set_stable is a NO-OP.
-module(gingko).
-include("gingko.hrl").


%%---------------- API -------------------%%
-export([
  update/4,
  commit/4,
  abort/2,
  get/3,
  create_journal_entry/2
]).


%%====================================================================
%% API functions
%%====================================================================

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
-spec get(key(), type(), snapshot_time()) -> {ok, snapshot()}.
get(Key, Type, MaximumSnapshotTime) ->
  logger:info(#{function => "GET_VERSION", key => Key, type => Type, snapshot_timestamp => MaximumSnapshotTime}),

  %% This part needs caching/optimization
  %% Currently the steps to materialize are as follows:
  %% * read ALL log entries from the persistent log file
  %% * filter log entries by key
  %% * filter furthermore only by committed operations
  %% * materialize operations into materialized version
  %% * return that materialized version

  %% TODO Get up to time SnapshotTime instead of all
  {ok, Data} = gingko_op_log:read_log_entries(?LOGGING_MASTER, 0, all),
  logger:debug(#{step => "unfiltered log", payload => Data, snapshot_timestamp => MaximumSnapshotTime}),

  {Ops, CommittedOps} = log_utilities:filter_terms_for_key(Data, {key, Key}, undefined, MaximumSnapshotTime, dict:new(), dict:new()),
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
-spec update(key(), type(), tx_id(), op()) -> ok | {error, reason()}.
update(Key, Type, TransactionId, DownstreamOp) ->
  logger:info(#{function => "UPDATE", key => Key, type => Type, transaction => TransactionId, op => DownstreamOp}),

  Entry = #log_operation{
      tx_id = TransactionId,
      op_type = update,
      log_payload = #update_log_payload{key = Key, type = Type , op = DownstreamOp}},
  LogRecord = #log_record {
    version = ?LOG_RECORD_VERSION,
    op_number = #op_number{},        % not used
    bucket_op_number = #op_number{}, % not used
    log_operation = Entry
  },

  gingko_op_log:append(?LOGGING_MASTER, LogRecord).


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
-spec commit([key()], tx_id(), dc_and_commit_time(), snapshot_time()) -> ok.
commit(Keys, TxId, CommitTime, SnapshotTime) ->
  logger:info("Commit"),
  Operation = create_commit_operation(CommitTime, SnapshotTime),
  Entry = create_journal_entry(TxId, Operation),

  gingko_op_log:append(?LOGGING_MASTER, Entry),
  ok.


%% @doc Aborts all operations belonging to given transaction id for given list of keys.
%%
%% An abort log record consists of the transaction id, the op_type 'abort'
%% and the actual payload which is empty.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%%
%% @param Keys list of keys to abort a transaction
%% @param TransactionId the id of the transaction to abort
-spec abort([key()], tx_id()) -> ok.
abort(Keys, TxId) ->
  logger:info("Abort"),
  Operation = create_abort_operation(),
  Entry = create_journal_entry(TxId, Operation),
  gingko_op_log:append(?LOGGING_MASTER, Entry),
  ok.

-spec checkpoint() -> ok.
checkpoint() ->
  %% TODO
  ok.

%% TODO add JSN (must be unique but they may be created concurrently)
-spec create_journal_entry(tx_id(), operation()) -> journal_entry().
create_journal_entry(TxId, Operation) ->
  #journal_entry{
    rt_timestamp = erlang:timestamp(),
    tx_id = TxId,
    operation = Operation
  }.

create_begin_operation() ->
  #system_operation{
    op_type = begin_txn,
    op_args = #begin_txn_args{}
  }.

create_prepare_operation(PrepareTime) ->
  #system_operation{
    op_type = prepare_txn,
    op_args = #prepare_txn_args{prepare_time = PrepareTime}
  }.

create_abort_operation() ->
  #system_operation{
    op_type = abort_txn,
    op_args = #abort_txn_args{}
  }.

create_commit_operation(CommitTime, SnapshotTime) ->
  #system_operation{
    op_type = commit_txn,
    op_args = #commit_txn_args{commit_time = CommitTime, snapshot_time = SnapshotTime}
  }.

create_checkpoint_operation() ->
  #system_operation{
    op_type = checkpoint,
    op_args = #checkpoint_args{}
  }.