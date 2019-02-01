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
%%
%%
%% <hr/>
%% The following environment flags are supported and can be provided on startup:
%%
%% <table border="1" width="100%">
%%  <tr>
%%    <th>Flag</th>
%%    <th>Value (Default)</th>
%%    <th>Example</th>
%%    <th>Description</th>
%%    <th>Location</th>
%%  </tr>
%%  <tr>
%%    <td>log_persistence</td>
%%    <td>true/false (true)</td>
%%    <td>true</td>
%%    <td>Enables logging to disk and recovering log on startup/crash if set to true</td>
%%    <td>not used</td>
%%  </tr>
%%  <tr>
%%    <td>log_root</td>
%%    <td>string ("data")</td>
%%    <td>"path/to/logdir"</td>
%%    <td>Specifies the base logging directory. Creates the directory tree if it does not exist</td>
%%    <td>not used</td>
%%  </tr>
%%</table>
-module(gingko).
-include("gingko.hrl").
-behaviour(application).


%%---------------- API -------------------%%
-export([
  update/4,
  commit/4,
  abort/2,
  get_version/2,
  get_version/3,
  set_stable/1
]).


%%---------------- application Callbacks -------------%%
-export([
  start/2,
  stop/1
]).


%%====================================================================
%% application callbacks
%%====================================================================

%% @doc Start the logging server. Wrapper function to start the gingko supervisor.
-spec start(term(), term()) -> {ok, pid()} | ignore | {error, term()}.
start(_Type, _Args) ->
  logger:notice(#{what => "START", process => "gingko application"}),
  gingko_sup:start_link().


%% @doc Stops the logging server.
-spec stop(term()) -> ok.
stop(_State) ->
  logger:notice(#{what => "SHUTDOWN", process => "gingko application"}),
  ok.


%%====================================================================
%% API functions
%%====================================================================

%% @doc Retrieves the current latest version of the object at given key with expected given type.
%% @see get_version/3
-spec get_version(key(), type()) -> {ok, snapshot()}.
get_version(Key, Type) ->
  get_version(Key, Type, undefined).


%% @doc Retrieves a materialized version of the object at given key with expected given type.
%% If MaximumSnapshotTime is given, then the version is guaranteed to not be older than the given snapshot.
%% <p>
%% Example usage:
%% </p>
%% Operations of a counter @my_counter in the log: +1, +1, -1, +1(not committed), -1(not committed).
%% <p>
%% 3 = get_version(my_counter, antidote_crdt_counter_pn, undefined)
%% </p>
-spec get_version(key(), type(), snapshot_time()) -> {ok, snapshot()}.
get_version(Key, Type, MaximumSnapshotTime) ->
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
%% <p>
%% A update log record consists of the transaction id, the op_type 'update' and the actual payload.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%% </p>
-spec update(key(), type(), txid(), op()) -> ok | {error, reason()}.
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
%% <p>
%% A commit log record consists of the transaction id, the op_type 'commit'
%% and the actual payload which consists of the commit time and the snapshot time.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%% </p>
-spec commit([key()], txid(), dc_and_commit_time(), snapshot_time()) -> ok.
%% TODO doc CommitTime
%% TODO doc SnapshotTime
commit(Keys, TransactionId, CommitTime, SnapshotTime) ->
  logger:info(#{function => "COMMIT", keys => Keys, transaction => TransactionId, commit_timestamp => CommitTime, snapshot_timestamp => SnapshotTime}),

  Entry = #log_operation{
      tx_id = TransactionId,
      op_type = commit,
      log_payload = #commit_log_payload{commit_time = CommitTime, snapshot_time = SnapshotTime}},

  LogRecord = #log_record {
    version = ?LOG_RECORD_VERSION,
    op_number = #op_number{},        % not used
    bucket_op_number = #op_number{}, % not used
    log_operation = Entry
  },

  lists:map(fun(_Key) -> gingko_op_log:append(?LOGGING_MASTER, LogRecord) end, Keys),
  ok.


%% @doc Aborts all operations belonging to given transaction id for given list of keys.
%% <p>
%% An abort log record consists of the transaction id, the op_type 'abort'
%% and the actual payload which is empty.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%% </p>
-spec abort([key()], txid()) -> ok.
abort(Keys, TransactionId) ->
  logger:info(#{function => "ABORT", keys => Keys, transaction => TransactionId}),

  Entry = #log_operation{
      tx_id = TransactionId,
      op_type = abort,
      log_payload = #abort_log_payload{}},

  LogRecord = #log_record {
    version = ?LOG_RECORD_VERSION,
    op_number = #op_number{},        % not used
    bucket_op_number = #op_number{}, % not used
    log_operation = Entry
  },

  lists:map(fun(_Key) -> gingko_op_log:append(?LOGGING_MASTER, LogRecord) end, Keys),
  ok.


%% @doc Sets a timestamp for when all operations below that timestamp are considered stable.
%% <p>
%% Currently not implemented.
%% </p>
-spec set_stable(snapshot_time()) -> ok.
set_stable(SnapshotTime) ->
  logger:warning(#{function => "SET_STABLE", timestamp => SnapshotTime, message => "not implemented"}),
  ok.
