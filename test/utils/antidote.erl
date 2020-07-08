%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jun 2020 20:34
%%%-------------------------------------------------------------------
-module(antidote).
-author("kevin").

-include("gingko.hrl").

-type op_name() :: atom().
-type op_param() :: term().
-type bucket() :: term().
-type bound_object() :: {key(), type(), bucket()}.

%% API
-export([start_transaction/2, abort_transaction/1, commit_transaction/1, read_objects/2, read_objects/3, get_objects/3, update_objects/2, update_objects/3]).

-spec start_transaction(Clock :: snapshot_time(), Properties :: txn_properties())
        -> {ok, txid()} | {error, reason()}.
start_transaction(Clock, _Properties) ->
    gingko:begin_txn(Clock).

-spec abort_transaction(TxId :: txid()) -> ok | {error, reason()}.
abort_transaction(TxId) ->
    gingko:abort_txn(TxId).

-spec commit_transaction(TxId :: txid()) ->
    {ok, snapshot_time()} | {error, reason()}.
commit_transaction(TxId) ->
    gingko:prepare_and_commit_txn(TxId).

-spec read_objects(Objects :: [bound_object()], TxId :: txid())
        -> {ok, [term()]} | {error, reason()}.
read_objects(Objects, TxId) ->
    Start = erlang:monotonic_time(millisecond),
    GingkoObjects = lists:map(fun({Key, Type, _Bucket}) -> {Key, Type} end, Objects),
    Result = gingko:read_multiple_snapshots(GingkoObjects, TxId),
    case Result of
        {ok, []} -> {ok, []};
        {ok, Res} ->
            NewRes = lists:map(
                fun(#snapshot{key_struct = #key_struct{type = Type}, value = Value}) ->
                    Type:value(Value)
                end, Res),
            End = erlang:monotonic_time(millisecond),
            logger:error("Tx Read Objects Time: ~p", [End - Start]),
            {ok, NewRes};
        Error -> Error
    end.

-spec read_objects(snapshot_time(), txn_properties(), [bound_object()])
        -> {ok, list(), vectorclock()} | {error, reason()}.
read_objects(Clock, _Properties, Objects) ->
    Start = erlang:monotonic_time(millisecond),
    GingkoObjects = lists:map(fun({Key, Type, _Bucket}) -> {Key, Type} end, Objects),
    Result = gingko:read_multiple_snapshots(GingkoObjects, Clock),
    case Result of
        {ok, []} -> {ok, [], gingko_utils:get_DCSf_vts()};
        {ok, Res = [#snapshot{snapshot_vts = Vts} | _]} ->
            NewRes = lists:map(
                fun(#snapshot{key_struct = #key_struct{type = Type}, value = Value}) ->
                    Type:value(Value)
                end, Res),
            End = erlang:monotonic_time(millisecond),
            logger:error("Static Read Objects Time: ~p", [End - Start]),
            {ok, NewRes, Vts};
        Error -> Error
    end.

%% Returns a list containing tuples of object state and commit time for each
%% of those objects
-spec get_objects(vectorclock(), txn_properties(), [bound_object()])
        -> {ok, list(), vectorclock()} | {error, reason()}.
get_objects(Clock, _Properties, Objects) ->
    GingkoObjects = lists:map(fun({Key, Type, _Bucket}) -> {Key, Type} end, Objects),
    Result = gingko:read_multiple_snapshots(GingkoObjects, Clock),
    case Result of
        {ok, []} -> {ok, [], gingko_utils:get_DCSf_vts()};
        {ok, Res = [#snapshot{snapshot_vts = Vts} | _]} ->
            NewRes = lists:map(
                fun(#snapshot{value = Value}) ->
                    Value
                end, Res),
            {ok, NewRes, Vts};
        Error -> Error
    end.

-spec update_objects([{bound_object(), op_name(), op_param()}], txid())
        -> ok | {error, reason()}.
update_objects(Updates, TxId) ->
    GingkoUpdates = lists:map(fun({{Key, Type, _Bucket}, OpName, OpParam}) ->
        {{Key, Type}, {OpName, OpParam}} end, Updates),
    gingko:update_multiple(GingkoUpdates, TxId).

%% For static transactions: bulk updates and bulk reads
-spec update_objects(snapshot_time() | ignore, txn_properties(), [{bound_object(), op_name(), op_param()}])
        -> {ok, snapshot_time()} | {error, reason()}.
update_objects(Clock, _Properties, Updates) ->
    Start = erlang:monotonic_time(millisecond),
    GingkoUpdates = lists:map(fun({{Key, Type, _Bucket}, OpName, OpParam}) ->
        {{Key, Type}, {OpName, OpParam}} end, Updates),
    Result = gingko:update_txn(GingkoUpdates, Clock),
    case Result of
        {ok, Vts} ->
            End = erlang:monotonic_time(millisecond),
            logger:error("Static Update Objects Time: ~p", [End - Start]),
            {ok, Vts};
        Error ->
            logger:error("Error: ~p", [Error]),
            Error
    end.
