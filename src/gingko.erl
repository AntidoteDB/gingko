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
    single_read_txn/2,
    read_multiple/2,
    update/3,
    single_update_txn/2,
    single_update_txn/3,
    update_multiple/2,
    begin_txn/0,
    begin_txn/1,
    begin_txn/2,
    prepare_txn/1,
    commit_txn/1,
    commit_txn/2,
    prepare_and_commit_txn/1,
    abort_txn/1,
    checkpoint/0,
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
    gingko_server:perform_request(Read).

-spec single_read_txn({key(), type()} | key_struct()) -> {ok, snapshot_value()} | {error, reason()}.
single_read_txn({Key, Type}) -> single_read_txn(#key_struct{key = Key, type = Type});
single_read_txn(KeyStruct) ->
    case begin_txn() of
        {error, Reason1} -> {error, Reason1};
        TxId ->
            case read(KeyStruct, TxId) of
                {error, Reason2} -> {error, Reason2};
                ReadResult ->
                    case prepare_and_commit_txn(TxId) of
                        {ok, _CommitVts} -> ReadResult;
                        Error -> Error
                    end
            end
    end.

-spec single_read_txn({key(), type()} | key_struct(), snapshot_time()) -> {ok, snapshot_value(), vectorclock()} | {error, reason()}.
single_read_txn({Key, Type}, Clock) -> single_read_txn(#key_struct{key = Key, type = Type}, Clock);
single_read_txn(KeyStruct, Clock) ->
    TxId = case Clock of
               undefined -> begin_txn();
               Vts -> begin_txn(Vts)
           end,
    case TxId of
        {error, Reason1} -> {error, Reason1};
        _ ->
            case read(KeyStruct, TxId) of
                {error, Reason2} -> {error, Reason2};
                {ok, Value} ->
                    case prepare_and_commit_txn(TxId) of
                        {ok, CommitVts} -> {ok, Value, CommitVts};
                        Error -> Error
                    end
            end
    end.

-spec read_multiple([key_struct() | {key(), type()}], txid()) -> [{key_struct() | {key(), type()}, {ok, snapshot_value()} | {error, reason()}}].
read_multiple(KeyStructsOrTuples, TxId) ->
    lists:map(fun(KeyStructOrTuple) -> {KeyStructOrTuple, read(KeyStructOrTuple, TxId)} end, KeyStructsOrTuples).

-spec update({key(), type()} | key_struct(), type_op(), txid()) -> ok | {error, reason()}.
update({Key, Type}, TypeOp, TxId) ->
    update(#key_struct{key = Key, type = Type}, TypeOp, TxId);
update(KeyStruct, TypeOp, TxId) ->
    Update = {{update, {KeyStruct, TypeOp}}, TxId},
    gingko_server:perform_request(Update).

-spec single_update_txn({key(), type()} | key_struct(), type_op()) -> ok | {error, reason()}.
single_update_txn({Key, Type}, TypeOp) -> single_update_txn(#key_struct{key = Key, type = Type}, TypeOp);
single_update_txn(KeyStruct, TypeOp) ->
    case begin_txn() of
        {error, Reason1} -> {error, Reason1};
        TxId ->
            case update(KeyStruct, TypeOp, TxId) of
                {error, Reason2} -> {error, Reason2};
                _ ->
                    case prepare_and_commit_txn(TxId) of
                        {ok, _CommitVts} -> ok;
                        Error -> Error
                    end
            end
    end.

-spec single_update_txn({key(), type()} | key_struct(), type_op(), snapshot_time()) -> {ok, vectorclock()} | {error, reason()}.
single_update_txn({Key, Type}, TypeOp, Clock) -> single_update_txn(#key_struct{key = Key, type = Type}, TypeOp, Clock);
single_update_txn(KeyStruct, TypeOp, Clock) ->
    TxId = case Clock of
               undefined -> begin_txn();
               Vts -> begin_txn(Vts)
           end,
    case TxId of
        {error, Reason1} -> {error, Reason1};
        _ ->
            case update(KeyStruct, TypeOp, TxId) of
                {error, Reason2} -> {error, Reason2};
                _ -> prepare_and_commit_txn(TxId)
            end
    end.

-spec update_multiple([{key_struct() | {key(), type()}, type_op()}], txid()) -> [{key_struct() | {key(), type()}, ok | {error, reason()}}].
update_multiple(KeyStructTypeOpTuples, TxId) ->
    lists:map(
        fun({KeyStructOrTuple, TypeOp}) ->
            {KeyStructOrTuple, update(KeyStructOrTuple, TypeOp, TxId)}
        end, KeyStructTypeOpTuples).

-spec begin_txn() -> txid() | {error, reason()}.
begin_txn() ->
    DependencyVts = gingko_utils:get_DCSf_vts(),
    begin_txn(DependencyVts).

-spec begin_txn(vectorclock()) -> txid() | {error, reason()}.
begin_txn(DependencyVts) ->
    TxId = get_new_tx_id(),
    case begin_txn(DependencyVts, TxId) of
        ok -> TxId;
        Error -> Error
    end.

-spec begin_txn(vectorclock(), txid()) -> ok | {error, reason()}.
begin_txn(DependencyVts, TxId) ->
    BeginTxn = {{begin_txn, DependencyVts}, TxId},
    gingko_server:perform_request(BeginTxn).

-spec prepare_txn(txid()) -> ok | {error, reason()}.
prepare_txn(TxId) ->
    PrepareTxn = {{prepare_txn, none}, TxId},
    gingko_server:perform_request(PrepareTxn).

-spec commit_txn(txid()) -> {ok, vectorclock()} | {error, reason()}.
commit_txn(TxId) ->
    %%TODO find out if this is correct
    CommitVts = gingko_utils:get_DCSf_vts(),
    commit_txn(CommitVts, TxId).

-spec commit_txn(vectorclock(), txid()) -> {ok, vectorclock()} | {error, reason()}.
commit_txn(CommitVts, TxId) ->
    CommitTxn = {{commit_txn, CommitVts}, TxId},
    case gingko_server:perform_request(CommitTxn) of
        ok -> {ok, CommitVts};
        Error -> Error
    end.

-spec prepare_and_commit_txn(txid()) -> {ok, vectorclock()} | {error, reason()}.
prepare_and_commit_txn(TxId) ->
    case prepare_txn(TxId) of
        ok -> commit_txn(TxId);
        Error -> Error
    end.

-spec abort_txn(txid()) -> ok | {error, reason()}.
abort_txn(TxId) ->
    AbortTxn = {{abort_txn, none}, TxId},
    gingko_server:perform_request(AbortTxn).

-spec checkpoint() -> ok.
checkpoint() ->
    DependencyVts = gingko_utils:get_GCSf_vts(),
    checkpoint(DependencyVts).

%% TODO: Dependency Vts is GCSf, also checkpoint is actually new and correct
-spec checkpoint(vectorclock()) -> ok.
checkpoint(DependencyVts) ->
    Checkpoint = {{checkpoint, DependencyVts}, #tx_id{local_start_time = gingko_utils:get_timestamp(), server_pid = self()}},
    gingko_server:perform_request(Checkpoint).
