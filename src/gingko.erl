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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(gingko).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").
-include_lib("kernel/include/logger.hrl").

%% API
-export([read/2, read_multiple/2, update/3, update_multiple/2, begin_txn/2, prepare_txn/2, commit_txn/2, abort_txn/2, checkpoint/1]).

%%====================================================================
%% API functions
%%====================================================================

-spec read({key(), type()} | key_struct(), txid()) -> {ok, snapshot_value()} | {error, reason()}.
read({Key, Type}, TxId) -> read(#key_struct{key = Key, type = Type}, TxId);
read(KeyStruct, TxId) ->
    Read = {{read, KeyStruct}, TxId},
    gingko_utils:call_gingko_sync_with_key(TxId, ?GINGKO_SERVER, Read).

%TODO reconsider return type
-spec read_multiple([key_struct()], txid()) -> [{ok, snapshot_value()} | {error, reason()}].
read_multiple(KeyStructs, TxId) ->
    ok. %TODO implement


-spec update({key(), type()} | key_struct(), type_op(), txid()) -> ok.
update({Key, Type}, TypeOp, TxId) ->
    update(#key_struct{key = Key, type = Type}, TypeOp, TxId);
update(KeyStruct, TypeOp, TxId) ->
    Update = {{update, {KeyStruct, TypeOp}}, TxId},
    gingko_utils:call_gingko_async_with_key(TxId, ?GINGKO_SERVER, Update).

-spec update_multiple([{key_struct(), type_op()}], txid()) -> ok.
update_multiple(KeyStructTypeOpTuples, TxId) ->
    ok. %TODO implement

-spec begin_txn(vectorclock(), txid()) -> list().
begin_txn(DependencyVts, TxId) ->
    BeginTxn = {{begin_txn, DependencyVts}, TxId},
    gingko_utils:call_gingko_async_with_key(TxId, ?GINGKO_SERVER, BeginTxn).

-spec prepare_txn(non_neg_integer(), txid()) -> ok.
prepare_txn(PrepareTime, TxId) ->
    PrepareTxn = {{prepare_txn, PrepareTime}, TxId},
    gingko_utils:call_gingko_sync_with_key(TxId, ?GINGKO_SERVER, PrepareTxn).

-spec commit_txn(vectorclock(), txid()) -> ok.
commit_txn(CommitVts, TxId) ->
    CommitTxn = {{commit_txn, CommitVts}, TxId},
    gingko_utils:call_gingko_async_with_key(TxId, ?GINGKO_SERVER, CommitTxn).

-spec abort_txn(term(), txid()) -> ok.
abort_txn(AbortArgs, TxId) ->
    AbortTxn = {{abort_txn, AbortArgs}, TxId},
    gingko_utils:call_gingko_async_with_key(TxId, ?GINGKO_SERVER, AbortTxn).

-spec checkpoint(vectorclock()) -> ok.
checkpoint(DependenyVts) ->
    Checkpoint = {{checkpoint, DependenyVts}, TxId = #tx_id{local_start_time = gingko_utils:get_timestamp(), server_pid = gingko}},
    gingko_utils:call_gingko_sync_with_key(TxId, ?GINGKO_SERVER, Checkpoint).


