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
    read/1,
    read/2,
    read_snapshot/2,
    read_multiple/2,
    read_multiple_snapshots/2,
    update/3,
    update_multiple/2,
    update_txn/1,
    update_txn/2,
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

to_key_struct({Key, Type}) -> #key_struct{key = Key, type = Type};
to_key_struct(KeyStruct) -> KeyStruct.

-spec get_new_tx_id() -> txid().
get_new_tx_id() ->
    CurrentTime = gingko_utils:get_timestamp(),
    #tx_id{local_start_time = CurrentTime, server_pid = self()}.

-spec read({key(), type()} | key_struct()) -> {ok, crdt()} | {error, reason()}.
read(KeyType) -> read(KeyType, gingko_utils:get_DCSf_vts()).

-spec read({key(), type()} | key_struct(), txid() | snapshot_time()) -> {ok, crdt()} | {error, reason()}.
read(KeyStructOrKeyType, TxIdOrVts) ->
    case read_snapshot(to_key_struct(KeyStructOrKeyType), TxIdOrVts) of
        {ok, #snapshot{value = SnapshotValue}} -> {ok, SnapshotValue};
        Error -> Error
    end.

-spec read_snapshot({key(), type()} | key_struct(), txid() | snapshot_time()) -> {ok, snapshot()} | {error, reason()}.
read_snapshot(KeyType, Atom) when is_atom(Atom) -> read_snapshot(KeyType, gingko_utils:get_DCSf_vts());
read_snapshot(KeyStructOrKeyType, TxIdOrVts) ->
    Read = {{read, to_key_struct(KeyStructOrKeyType)}, TxIdOrVts},
    Fun = fun() -> gingko_server:perform_request(Read) end,
    general_utils:bench("Read", Fun).
%%    gingko_server:perform_request(Read).

-spec read_multiple([key_struct() | {key(), type()}], txid() | snapshot_time()) -> {ok | error, [crdt() | {error, reason()}]}.
read_multiple(KeyStructsOrTuples, TxIdOrVts) ->
    case read_multiple_snapshots(KeyStructsOrTuples, TxIdOrVts) of
        {ok, Results} ->
            {ok, lists:map(
                fun(#snapshot{value = Value}) ->
                    Value
                end, Results)};
        Error -> Error
    end.

-spec read_multiple_snapshots([key_struct() | {key(), type()}], txid() | snapshot_time()) -> {ok | error, [snapshot() | {error, reason()}]}.
read_multiple_snapshots(KeyStructsOrTuples, TxIdOrVts) ->
    lists:foldr(
        fun(KeyStructOrTuple, {CurrentOkOrError, CurrentResults}) ->
            case read_snapshot(KeyStructOrTuple, TxIdOrVts) of
                {ok, Snapshot} -> {CurrentOkOrError, [Snapshot | CurrentResults]};
                {error, Error} -> {error, [{error, {KeyStructOrTuple, Error}} | CurrentResults]}
            end
        end, {ok, []}, KeyStructsOrTuples).

-spec update({key(), type()} | key_struct(), type_op(), txid()) -> ok | {error, reason()}.
update(KeyStructOrKeyType, TypeOp, TxId) ->
    Update = {{update, {to_key_struct(KeyStructOrKeyType), TypeOp}}, TxId},
    Fun = fun() -> gingko_server:perform_request(Update) end,
    general_utils:bench("Update", Fun).
%%    gingko_server:perform_request(Update).

-spec update_multiple([{key_struct() | {key(), type()}, type_op()}], txid()) -> ok | {error, reason()}.
update_multiple(KeyStructTypeOpTuples, TxId) ->
    Result = lists:filtermap(
        fun({KeyStructOrTuple, TypeOp}) ->
            case update(to_key_struct(KeyStructOrTuple), TypeOp, TxId) of
                ok -> false;
                {error, Reason} -> {true, {KeyStructOrTuple, TypeOp, Reason}}
            end
        end, KeyStructTypeOpTuples),
    case Result of
        [] -> ok;
        _ -> {error, Result}
    end.

-spec update_txn([{key_struct() | {key(), type()}, type_op()}] | {key_struct() | {key(), type()}, type_op()}) -> ok | {error, reason()}.
update_txn(KeyStructTypeOpTuples) when is_list(KeyStructTypeOpTuples) ->
    case update_txn(KeyStructTypeOpTuples, ignore) of
        {ok, Results, _} -> {ok, Results};
        Error -> Error
    end;
update_txn(KeyStructTypeOpTuple) -> update_txn([KeyStructTypeOpTuple]).

-spec update_txn([{key_struct() | {key(), type()}, type_op()}] | {key_struct() | {key(), type()}, type_op()}, snapshot_time()) -> {ok, vectorclock()} | {error, reason()}.
update_txn(KeyStructTypeOpTuples, ignore) when is_list(KeyStructTypeOpTuples) ->
    update_txn(KeyStructTypeOpTuples, undefined);
update_txn(KeyStructTypeOpTuples, Clock) when is_list(KeyStructTypeOpTuples) ->
    TxId = get_new_tx_id(),
    BeginVts =
        case Clock of
            undefined -> gingko_utils:get_DCSf_vts();
            Vts -> Vts
        end,
    case KeyStructTypeOpTuples of
        [] ->
            {ok, BeginVts};
        _ ->
            KeyStructs = lists:map(fun({KeyStructOrTuple, TypeOp}) ->
                {to_key_struct(KeyStructOrTuple), TypeOp} end, KeyStructTypeOpTuples),
            Transaction = {{transaction, {BeginVts, KeyStructs}}, TxId},
            Fun = fun() -> gingko_server:perform_request(Transaction) end,
            general_utils:bench("Transaction", Fun)
    end;
%%    gingko_server:perform_request(Transaction);
update_txn(KeyStructTypeOpTuple, Clock) -> update_txn([KeyStructTypeOpTuple], Clock).

-spec begin_txn() -> {ok, txid()} | {error, reason()}.
begin_txn() ->
    DependencyVts = gingko_utils:get_DCSf_vts(),
    begin_txn(DependencyVts).

-spec begin_txn(snapshot_time()) -> {ok, txid()} | {error, reason()}.
begin_txn(ignore) -> begin_txn();
begin_txn(undefined) -> begin_txn();
begin_txn(DependencyVts) ->
    TxId = get_new_tx_id(),
    case begin_txn(DependencyVts, TxId) of
        ok -> {ok, TxId};
        Error -> Error
    end.

-spec begin_txn(vectorclock(), txid()) -> ok | {error, reason()}.
begin_txn(DependencyVts, TxId) ->
    BeginTxn = {{begin_txn, DependencyVts}, TxId},
    Fun = fun() -> gingko_server:perform_request(BeginTxn) end,
    general_utils:bench("BeginTxn", Fun).
%%    gingko_server:perform_request(BeginTxn).

-spec prepare_txn(txid()) -> ok | {error, reason()}.
prepare_txn(TxId) ->
    PrepareTxn = {{prepare_txn, none}, TxId},
    Fun = fun() -> gingko_server:perform_request(PrepareTxn) end,
    general_utils:bench("PrepareTxn", Fun).
%%    gingko_server:perform_request(PrepareTxn).

-spec commit_txn(txid()) -> {ok, vectorclock()} | {error, reason()}.
commit_txn(TxId) ->
    %%TODO find out if this is correct
    CommitVts = gingko_utils:get_DCSf_vts(),
    commit_txn(CommitVts, TxId).

-spec commit_txn(vectorclock(), txid()) -> {ok, vectorclock()} | {error, reason()}.
commit_txn(CommitVts, TxId) ->
    CommitTxn = {{commit_txn, CommitVts}, TxId},
    Fun = fun() -> gingko_server:perform_request(CommitTxn) end,
    general_utils:bench("CommitTxn", Fun).
%%    case gingko_server:perform_request(CommitTxn) of
%%        ok -> {ok, CommitVts};
%%        Error -> Error
%%    end.

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
    DependencyVts = gingko_utils:get_GCSt_vts(),
    checkpoint(DependencyVts).

%% TODO: Dependency Vts is GCSt, also checkpoint is actually new and correct
-spec checkpoint(vectorclock()) -> ok.
checkpoint(DependencyVts) ->
    Checkpoint = {{checkpoint, DependencyVts}, #tx_id{local_start_time = gingko_utils:get_timestamp(), server_pid = self()}},
    gingko_server:perform_request(Checkpoint).
