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
-export([read/1,
    read/2,
    read_snapshot/2,
    read_multiple/2,
    read_multiple_snapshots/2,
    update/2,
    update/3,
    update_txn/1,
    update_txn/2,
    begin_txn/0,
    begin_txn/1,
    prepare_txn/1,
    commit_txn/1,
    prepare_and_commit_txn/1,
    abort_txn/1,
    checkpoint/0]).

-type key_struct_or_tuple() :: key_struct() | {key(), type()}.
-type update_tuple() :: {key_struct_or_tuple(), type_op()}.

%%====================================================================
%% API functions
%%====================================================================

-spec to_key_struct(key_struct_or_tuple()) -> key_struct().
to_key_struct({Key, Type}) -> #key_struct{key = Key, type = Type};
to_key_struct(KeyStruct) -> KeyStruct.

-spec start_new_tx(vectorclock() | ignore) -> txid().
start_new_tx(ignore) -> start_new_tx(gingko_dc_utils:get_DCSf_vts());
start_new_tx(DependencyVts) ->
    gingko_tx_server_sup:start_tx_server(DependencyVts).

-spec read(key_struct_or_tuple()) -> {ok, crdt()} | {error, reason()}.
read(KeyType) -> read(KeyType, ignore).

-spec read(key_struct_or_tuple(), txid() | vectorclock() | ignore) -> {ok, crdt()} | {error, reason()}.
read(KeyStructOrKeyType, TxIdOrVts) ->
    case read_snapshot(KeyStructOrKeyType, TxIdOrVts) of
        {ok, #snapshot{value = SnapshotValue}} -> {ok, SnapshotValue};
        Error -> Error
    end.

-spec read_snapshot(key_struct_or_tuple(), txid() | vectorclock() | ignore) -> {ok, snapshot()} | {error, reason()}.
read_snapshot(KeyType, ignore) -> read_snapshot(KeyType, gingko_dc_utils:get_DCSf_vts());
read_snapshot({Key, Type}, TxIdOrVts) -> read_snapshot(#key_struct{key = Key, type = Type}, TxIdOrVts);
read_snapshot(KeyStruct = #key_struct{type = Type}, TxIdOrVts) ->
    true = check_type(Type),
    Fun =
        fun() ->
            case TxIdOrVts of
                TxId = #tx_id{} ->
                    gingko_tx_server:read(TxId, KeyStruct);
                DependencyVts when is_map(DependencyVts) ->
                    gingko_dc_utils:call_gingko_sync_with_key(KeyStruct, ?GINGKO_CACHE, {get, KeyStruct, DependencyVts})
            end
        end,
    general_utils:benchmark("Read", Fun).

-spec read_multiple([key_struct_or_tuple()], txid() | vectorclock() | ignore) -> {ok, [crdt()]} | {error, reason()}.
read_multiple(KeyStructsOrTuples, TxIdOrVts) ->
    case read_multiple_snapshots(KeyStructsOrTuples, TxIdOrVts) of
        {ok, Results} ->
            {ok, lists:map(
                fun(#snapshot{value = Value}) ->
                    Value
                end, Results)};
        Error -> Error
    end.

-spec read_multiple_snapshots([key_struct_or_tuple()], txid() | vectorclock() | ignore) -> {ok, [snapshot()]} | {error, reason()}.
read_multiple_snapshots(KeyStructsOrTuples, TxIdOrVts) ->
    lists:foldr(
        fun(KeyStructOrTuple, {CurrentOkOrError, CurrentResults}) ->
            case read_snapshot(KeyStructOrTuple, TxIdOrVts) of
                {ok, Snapshot} -> {CurrentOkOrError, [Snapshot | CurrentResults]};
                {error, Error} -> {error, [{error, {KeyStructOrTuple, Error}} | CurrentResults]}
            end
        end, {ok, []}, KeyStructsOrTuples).

-spec update(update_tuple() | [update_tuple()], txid()) -> ok | {error, reason()}.
update(KeyStructTypeOpTuple = {{_Key, _Type}, _TypeOp}, TxId) ->
    update([KeyStructTypeOpTuple], TxId);
update(KeyStructTypeOpTuple = {#key_struct{}, _TypeOp}, TxId) ->
    update([KeyStructTypeOpTuple], TxId);
update(KeyStructTypeOpTuples, TxId) when is_list(KeyStructTypeOpTuples) ->
    Result = lists:filtermap(
        fun({KeyStructOrTuple, TypeOp}) ->
            case update(KeyStructOrTuple, TypeOp, TxId) of
                ok -> false;
                {error, Reason} -> {true, {KeyStructOrTuple, TypeOp, Reason}}
            end
        end, KeyStructTypeOpTuples),
    case Result of
        [] -> ok;
        _ -> {error, Result}
    end.

-spec update(key_struct_or_tuple(), type_op(), txid()) -> ok | {error, reason()}.
update({Key, Type}, TypeOp, TxId) -> update(#key_struct{key = Key, type = Type}, TypeOp, TxId);
update(KeyStruct = #key_struct{type = Type}, TypeOp, TxId = #tx_id{}) ->
    true = check_type(Type, TypeOp),
    Update = {KeyStruct, TypeOp},
    Fun = fun() -> gingko_tx_server:update(TxId, Update) end,
    general_utils:benchmark("Update", Fun).

-spec update_txn(update_tuple() | [update_tuple()]) -> ok | {error, reason()}.
update_txn(KeyStructTypeOpTuples) ->
    case update_txn(KeyStructTypeOpTuples, ignore) of
        {ok, _CommitVts} -> ok;
        Error -> Error
    end.

-spec update_txn(update_tuple() | [update_tuple()], vectorclock() | ignore) -> {ok, vectorclock()} | {error, reason()}.
update_txn(KeyStructTypeOpTuple, DependencyVtsOrIgnore) when not is_list(KeyStructTypeOpTuple) ->
    update_txn([KeyStructTypeOpTuple], DependencyVtsOrIgnore);
update_txn(KeyStructTypeOpTuples, DependencyVtsOrIgnore) when is_list(KeyStructTypeOpTuples) andalso (is_map(DependencyVtsOrIgnore) orelse DependencyVtsOrIgnore == ignore) ->
    TxId = start_new_tx(DependencyVtsOrIgnore),
    KeyStructTypeOpTupleList =
        lists:map(
            fun({KeyStructOrTuple, TypeOp}) ->
                KeyStruct = #key_struct{type = Type} = to_key_struct(KeyStructOrTuple),
                true = check_type(Type, TypeOp),
                {KeyStruct, TypeOp}
            end, KeyStructTypeOpTuples),
    Fun = fun() -> gingko_tx_server:transaction(TxId, KeyStructTypeOpTupleList) end,
    general_utils:benchmark("Transaction", Fun).

-spec begin_txn() -> txid().
begin_txn() -> begin_txn(ignore).

-spec begin_txn(vectorclock() | ignore) -> txid().
begin_txn(DependencyVtsOrIgnore) when is_map(DependencyVtsOrIgnore) orelse DependencyVtsOrIgnore == ignore ->
    start_new_tx(DependencyVtsOrIgnore).

-spec prepare_txn(txid()) -> ok | {error, reason()}.
prepare_txn(TxId = #tx_id{}) ->
    Fun = fun() -> gingko_tx_server:prepare_txn(TxId) end,
    general_utils:benchmark("PrepareTxn", Fun).

-spec commit_txn(txid()) -> {ok, vectorclock()} | {error, reason()}.
commit_txn(TxId = #tx_id{}) ->
    Fun = fun() -> gingko_tx_server:commit_txn(TxId) end,
    general_utils:benchmark("CommitTxn", Fun).

-spec prepare_and_commit_txn(txid()) -> {ok, vectorclock()} | {error, reason()}.
prepare_and_commit_txn(TxId = #tx_id{}) ->
    case prepare_txn(TxId) of
        ok -> commit_txn(TxId);
        Error -> Error
    end.

-spec abort_txn(txid()) -> ok | {error, reason()}.
abort_txn(TxId = #tx_id{}) ->
    Fun = fun() -> gingko_tx_server:abort_txn(TxId) end,
    general_utils:benchmark("AbortTxn", Fun).

-spec checkpoint() -> ok.
checkpoint() ->
    Fun = fun() -> gingko_checkpoint_service:checkpoint() end,
    general_utils:benchmark("Checkpoint", Fun).

-spec check_type(term()) -> boolean().
check_type(CrdtType) ->
    antidote_crdt:is_type(CrdtType).
-spec check_type(term(), term()) -> boolean().
check_type(CrdtType, Operation) ->
    antidote_crdt:is_operation(CrdtType, Operation).
