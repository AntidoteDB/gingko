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

%% API
-export([
    start/0,
    stop/0,
    get_new_tx_id/0,
    read/2,
    single_read_txn/1,
    read_multiple/2,
    update/3,
    single_update_txn/2,
    update_multiple/2,
    begin_txn/0,
    begin_txn/1,
    begin_txn/2,
    prepare_txn/2,
    commit_txn/1,
    commit_txn/2,
    prepare_and_commit_txn/1,
    abort_txn/1,
    abort_txn/2,
    checkpoint/1]).

%%====================================================================
%% API functions
%%====================================================================

-spec start() -> {ok, [atom()]} | {error, reason()}.
start() ->
    application:ensure_all_started(?GINGKO_APP_NAME).

-spec stop() -> ok | {error, reason()}.
stop() ->
    application:stop(?GINGKO_APP_NAME).

-spec get_new_tx_id() -> txid().
get_new_tx_id() ->
    CurrentTime = gingko_utils:get_timestamp(),
    #tx_id{local_start_time = CurrentTime, server_pid = self()}.

-spec read({key(), type()} | key_struct(), txid()) -> {ok, snapshot_value()} | {error, reason()}.
read({Key, Type}, TxId) -> read(#key_struct{key = Key, type = Type}, TxId);
read(KeyStruct, TxId) ->
    Read = {{read, KeyStruct}, TxId},
    gen_server:call(gingko_server, Read).

-spec single_read_txn({key(), type()} | key_struct()) -> {ok, snapshot_value()} | {error, reason()}.
single_read_txn({Key, Type}) -> single_read_txn(#key_struct{key = Key, type = Type});
single_read_txn(KeyStruct) ->
    TxId = begin_txn(),
    ReadResult = read(KeyStruct, TxId),
    prepare_and_commit_txn(TxId),
    ReadResult.

%TODO reconsider return type
-spec read_multiple([key_struct()], txid()) -> [{ok, snapshot_value()} | {error, reason()}].
read_multiple(KeyStructs, TxId) ->
    ok. %TODO implement

-spec update({key(), type()} | key_struct(), type_op(), txid()) -> ok.
update({Key, Type}, TypeOp, TxId) ->
    update(#key_struct{key = Key, type = Type}, TypeOp, TxId);
update(KeyStruct, TypeOp, TxId) ->
    Update = {{update, {KeyStruct, TypeOp}}, TxId},
    gen_server:cast(gingko_server, Update).

-spec single_update_txn({key(), type()} | key_struct(), type_op()) -> ok.
single_update_txn({Key, Type}, TypeOp) -> single_update_txn(#key_struct{key = Key, type = Type}, TypeOp);
single_update_txn(KeyStruct, TypeOp) ->
    TxId = begin_txn(),
    update(KeyStruct, TypeOp, TxId),
    prepare_and_commit_txn(TxId).

-spec update_multiple([{key_struct(), type_op()}], txid()) -> ok.
update_multiple(KeyStructTypeOpTuples, TxId) ->
    ok. %TODO implement

-spec begin_txn() -> txid().
begin_txn() ->
    DependencyVts = gingko_utils:get_DCSf_vts(),
    begin_txn(DependencyVts).

-spec begin_txn(vectorclock()) -> txid().
begin_txn(DependencyVts) ->
    TxId = get_new_tx_id(),
    begin_txn(DependencyVts, TxId),
    TxId.

-spec begin_txn(vectorclock(), txid()) -> ok.
begin_txn(DependencyVts, TxId) ->
    BeginTxn = {{begin_txn, DependencyVts}, TxId},
    gen_server:cast(gingko_server, BeginTxn).

-spec prepare_txn(txid()) -> ok.
prepare_txn(TxId) ->
    prepare_txn(0, TxId). %TODO find default value

-spec prepare_txn(non_neg_integer(), txid()) -> ok.
prepare_txn(PrepareTime, TxId) ->
    PrepareTxn = {{prepare_txn, PrepareTime}, TxId},
    gen_server:call(gingko_server, PrepareTxn).

-spec commit_txn(txid()) -> ok.
commit_txn(TxId) ->
    %%TODO find out if this is correct
    CommitVts = gingko_utils:get_DCSf_vts(),
    commit_txn(CommitVts, TxId).

-spec commit_txn(vectorclock(), txid()) -> ok.
commit_txn(CommitVts, TxId) ->
    CommitTxn = {{commit_txn, CommitVts}, TxId},
    gen_server:cast(gingko_server, CommitTxn).

-spec prepare_and_commit_txn(txid()) -> ok.
prepare_and_commit_txn(TxId) ->
    prepare_txn(TxId),
    commit_txn(TxId).

-spec abort_txn(txid()) -> ok.
abort_txn(TxId) ->
    abort_txn(no_args, TxId).

-spec abort_txn(term(), txid()) -> ok.
abort_txn(AbortArgs, TxId) ->
    AbortTxn = {{abort_txn, AbortArgs}, TxId},
    gen_server:cast(gingko_server, AbortTxn).

%% TODO: Dependency Vts is GCSf, also checkpoint is actually new and correct
-spec checkpoint(vectorclock()) -> ok.
checkpoint(DependencyVts) ->
    Checkpoint = {{checkpoint, DependencyVts}, TxId = #tx_id{local_start_time = gingko_utils:get_timestamp(), server_pid = gingko}},
    gen_server:call(gingko_server, Checkpoint).


