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
-include("gingko.hrl").
-behaviour(application).


%% =====================
%% ENVIRONMENT VARIABLES
%% -------------
%%
%% ******************************
%% log_persistence = true | false
%%   Enables logging to disk and recovering log on startup if set to true
%%
%% ******************************
%% log_root = "path/to/logdir"
%%   Specifies the base logging directory.
%%   Defaults to "gingko/logs/"
%%
%% =====================

%% @doc
%% Internal State (in-memory)
% List of operations per object, [list per DC?]
% VC of oldest op in the list [VC_low = VC_snap]
% iterate from most recent

%% Snapshot cache
% -> Use dedicated cache module?? Problem: stores only one version?
% -> do one version first: Store snapshot at VC_Low
% [Optimization: store also the most recent snapshot?]
%
%% Log (persistence) -> cf module log.erl
%
%% Notes
% Is this still needed?? dc_meta_data_utilities:get_env_meta_data(sync_log, Value).
% Inter-DC communication needs to run in different process somewhere else -> not related to log logic

%%---------------- API -------------------%%
-export([commit/4,
         abort/2,
         get_version/3,
         update/4,
         set_stable/1]).

%%---------------- Callbacks -------------%%
-export([start/2,
         stop/1
        ]).


%%====================================================================
%% API functions
%%====================================================================

%% @doc Start the logging server.
-spec start(term(), term()) -> {ok, pid()} | ignore | {error, term()}.
start(_Type, _Args) ->
  logger:notice(#{process => "START", what => "gingko application"}),
  gingko_sup:start_link().


-spec stop(term()) -> ok.
stop(_State) ->
  logger:notice(#{process => "SHUTDOWN", what => "gingko application"}),
  ok.


%-spec get_version(key(), type(), snapshot_time(), txid())
%    -> {ok, snapshot()} | {error, reason()}.
get_version(Key, Type, SnapshotTime) ->
  logger:info(#{function => "GET_VERSION", key => Key, type => Type, snapshot_timestamp => SnapshotTime}),

  %% TODO Get up to time SnapshotTime instead of all
  {ok, Data} = gingko_op_log:read_log_entries(?LOGGING_MASTER, 0, all),
  logger:info(#{step => "unfiltered log", payload => Data, snapshot_timestamp => SnapshotTime}),

  {Ops, CommittedOps} = log_utilities:filter_terms_for_key(Data, {key, Key}, undefined, SnapshotTime, dict:new(), dict:new()),
  logger:info(#{step => "filtered terms", ops => Ops, committed => CommittedOps}),

  case dict:find(Key, CommittedOps) of
    {ok, PayloadForKey} -> PayloadForKey = PayloadForKey;
    error -> PayloadForKey = []
  end,

  MaterializedObject = materializer:materialize_clocksi_payload(Type, materializer:create_snapshot(Type), PayloadForKey),
  logger:info(#{step => "materialize", materialized => MaterializedObject}),

  {ok, MaterializedObject}.


% @doc Make the DownstreamOp persistent.
% key: The key to apply the update to
% type: CRDT Type of the object at given key
% txid: Transaction id the update belongs to
% downstreamop: Operation to be applied to the key
-spec update(key(), type(), txid(), op()) -> ok | {error, reason()}.
update(Key, Type, TxId, DownstreamOp) ->
  logger:info(#{function => "UPDATE", key => Key, type => Type, transaction => TxId, op => DownstreamOp}),

  Entry = #log_operation{
      tx_id = TxId,
      op_type = update,
      log_payload = #update_log_payload{key = Key, type = Type , op = DownstreamOp}},
  LogRecord = #log_record {
    version = 0,                     % for now hard-coded version 0
    op_number = #op_number{},        % TODO ?????
    bucket_op_number = #op_number{}, % TODO ?????
    log_operation = Entry
  },

  gingko_op_log:append(?LOGGING_MASTER, LogRecord).


commit(Keys, TxId, CommitTime, SnapshotTime) ->
  logger:info(#{function => "COMMIT", keys => Keys, transaction => TxId, commit_timestamp => CommitTime, snapshot_timestamp => SnapshotTime}),

  Entry = #log_operation{
      tx_id = TxId,
      op_type = commit,
      log_payload = #commit_log_payload{commit_time = CommitTime, snapshot_time = SnapshotTime}},

  LogRecord = #log_record {
    version = 0,                     % for now hard-coded version 0
    op_number = #op_number{},        % TODO ?????
    bucket_op_number = #op_number{}, % TODO ?????
    log_operation = Entry
  },

  lists:map(fun(_Key) -> gingko_op_log:append(?LOGGING_MASTER, LogRecord) end, Keys),
  ok.


abort(Keys, TxId) ->
  logger:info(#{function => "ABORT", keys => Keys, transaction => TxId}),

  Entry = #log_operation{
      tx_id = TxId,
      op_type = abort,
      log_payload = #abort_log_payload{}},

  LogRecord = #log_record {
    version = 0,                     % for now hard-coded version 0
    op_number = #op_number{},        % TODO ?????
    bucket_op_number = #op_number{}, % TODO ?????
    log_operation = Entry
  },


  lists:map(fun(_Key) -> gingko_op_log:append(?LOGGING_MASTER, LogRecord) end, Keys),
  ok.

    
set_stable(Vectorclock) ->
  logger:info(#{function => "SET_STABLE", timestamp => Vectorclock}),
  %% TODO
  ok.


