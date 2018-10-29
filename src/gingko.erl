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
%TODO -spec start([]) -> {ok, pid()} | ignore | {error, term()}.
start(_Type, _Args) ->
    gingkoo_sup:start_link().

%TODO -spec
stop(_State) ->
    ok.

%-spec get_version(key(), type(), snapshot_time(), txid())
%    -> {ok, snapshot()} | {error, reason()}.
get_version(Key, Type, SnapshotTime) ->
    LogId = log_utilities:get_logid_from_key(Key),
    Partition = log_utilities:get_key_partition(Key),
    PayloadList = logging_vnode:get_up_to_time(Partition, LogId, SnapshotTime, Type, Key),
    materializer:materialize(Type, PayloadList).

% @doc Make the DownstreamOp persistent.
%-spec write_update(key(), clocksi_payload()) -> ok | {error, reason()}.
update(Key, Type, TxId, DownstreamOp) ->
    Entry = #log_operation{
        tx_id = TxId, 
        op_type = commit, 
        log_payload = #update_log_payload{key = Key, type = Type , op = DownstreamOp}},
    {ok, _OpId} = logging_vnode:append(Key, Entry).

commit(Keys, TxId, CommitTime, SnapshotTime) ->
    Entry = #log_operation{
        tx_id = TxId, 
        op_type = commit, 
        log_payload = #commit_log_payload{commit_time = CommitTime, snapshot_time = SnapshotTime}},
    lists:map(fun(Key) -> logging_vnode:append_commit(Key, Entry) end, Keys).
    
abort(Keys, TxId) ->
    Entry = #log_operation{
        tx_id = TxId, 
        op_type = abort, 
        log_payload = #abort_log_payload{}},
    lists:map(fun(Key) -> logging_vnode:append(Key, Entry) end, Keys).

    
set_stable(_Vectorclock) ->
    ok.
