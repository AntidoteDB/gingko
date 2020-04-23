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
-define(USE_SINGLE_SERVER, false).
%% API
-export([read/2, read_multiple/2, update/3, update_multiple/2, begin_txn/2, prepare_txn/2, commit_txn/2, abort_txn/2, checkpoint/0]).

%%====================================================================
%% API functions
%%====================================================================

-spec read(key_struct(), txid()) -> {ok, snapshot_value()} | {error, reason()}.
read(KeyStruct, TxId) ->
    Read = {{read, KeyStruct}, TxId},
    case ?USE_SINGLE_SERVER of
        true ->
            gen_server:call(gingko_server, Read);
        false ->
            antidote_utilities:call_vnode_sync_with_key(TxId, gingko_vnode_master, Read)
    end.

%TODO reconsider return type
-spec read_multiple([key_struct()], txid()) -> [{ok, snapshot_value()} | {error, reason()}].
read_multiple(KeyStructs, TxId) ->
    ok. %TODO implement


-spec update(key_struct(), type_op(), txid()) -> ok.
update(KeyStruct, TypeOp, TxId) ->
    Update = {{update, {KeyStruct, TypeOp}}, TxId},
    case ?USE_SINGLE_SERVER of
        true ->
            gen_server:cast(gingko_server, Update);
        false ->
            antidote_utilities:call_vnode_with_key(TxId, gingko_vnode_master, Update)
    end.

-spec update_multiple([{key_struct(), type_op()}], txid()) -> ok.
update_multiple(KeyStructTypeOpTuples, TxId) ->
    ok. %TODO implement

-spec begin_txn(vectorclock(), txid()) -> list().
begin_txn(DependencyVts, TxId) ->
    BeginTxn = {{begin_txn, DependencyVts}, TxId},
    case ?USE_SINGLE_SERVER of
        true ->
            gen_server:cast(gingko_server, BeginTxn);
        false ->
            antidote_utilities:call_vnode_with_key(TxId, gingko_vnode_master, BeginTxn)
    end.

-spec prepare_txn(non_neg_integer(), txid()) -> ok.
prepare_txn(PrepareTime, TxId) ->
    PrepareTxn = {{prepare_txn, PrepareTime}, TxId},
    case ?USE_SINGLE_SERVER of
        true ->
            gen_server:call(gingko_server, PrepareTxn);
        false ->
            antidote_utilities:call_vnode_sync_with_key(TxId, gingko_vnode_master, PrepareTxn)
    end.

-spec commit_txn(vectorclock(), txid()) -> ok.
commit_txn(CommitVts, TxId) ->
    CommitTxn = {{commit_txn, CommitVts}, TxId},
    case ?USE_SINGLE_SERVER of
        true ->
            gen_server:cast(gingko_server, CommitTxn);
        false ->
            antidote_utilities:call_vnode_with_key(TxId, gingko_vnode_master, CommitTxn)
    end.

-spec abort_txn(term(), txid()) -> ok.
abort_txn(AbortArgs, TxId) ->
    AbortTxn = {{abort_txn, AbortArgs}, TxId},
    case ?USE_SINGLE_SERVER of
        true ->
            gen_server:cast(gingko_server, AbortTxn);
        false ->
            antidote_utilities:call_vnode_with_key(TxId, gingko_vnode_master, AbortTxn)
    end.

-spec checkpoint() -> ok.
checkpoint() ->
    ok. %TODO implement


