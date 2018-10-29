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

-module(materializer).
-include("gingko.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([create_snapshot/1,
         apply_op/3,
         apply_ops/3,
         check_ops/1,
         check_op/1,
         materialize/2
        ]).

%% @doc Creates an empty CRDT
-spec create_snapshot(type()) -> snapshot().
create_snapshot(Type) ->
    Type:new().

%% @doc Applies an operation to a snapshot of a crdt.
%%      This function yields an error if the crdt does not have a corresponding update operation.
-spec apply_op(type(), snapshot(), op()) -> {ok, snapshot()} | {error, {unexpected_operation, op(), type()}}.
apply_op(Type, Snapshot, Op) ->
    try
        Type:update(Op, Snapshot)
    catch
        _:_ ->
            {error, {unexpected_operation, Op, Type}}
    end.

%% @doc Applies list of updates in given order without any checks, errors are simply propagated.
-spec apply_ops(type(), snapshot(), [downstream_record()]) -> snapshot() | {error, {unexpected_operation, op(), type()}}.
apply_ops(_Type, Snapshot, []) ->
    Snapshot;
apply_ops(Type, Snapshot, [Op | Rest]) ->
    case apply_op(Type, Snapshot, Op) of
        {error, Reason} ->
            {error, Reason};
        {ok, Result} ->
            apply_ops(Type, Result, Rest)
    end.

%% @doc Check that in a list of operations, all of them are correctly typed.
-spec check_ops([op()]) -> ok | {error, {type_check_failed, op()}}.
check_ops([]) ->
    ok;
check_ops([Op | Rest]) ->
    case check_op(Op) of
        true ->
            check_ops(Rest);
        false ->
            {error, {type_check, Op}}
    end.

%% @doc Check that an operation is correctly typed.
-spec check_op(op()) -> boolean().
check_op(Op) ->
    case Op of
        {update, {_Key, Type, Update}} ->
            antidote_crdt:is_type(Type) andalso
                Type:is_operation(Update);
        {read, {_Key, Type}} ->
            antidote_crdt:is_type(Type);
        _ ->
            false
    end.

%% @doc Applies the operation of a list to a previously created CRDT snapshot. Only the
%%      operations that are not already in the previous snapshot and
%%      with smaller timestamp than the specified
%%      are considered. Newer operations are discarded.
%%      
%%      Payloads: The list of operations to apply in causal order
-spec materialize(type(), [{op_num(), clocksi_payload()}]) -> {ok, snapshot()} | {error, reason()}.
materialize(Type, Payloads) ->
    S = lists:foldr(fun ({_OpNum, Payload}, Snapshot) -> 
                        Effect = Payload#clocksi_payload.op_param,   
                        {ok, Result} = Type:update(Effect, Snapshot),
                        Result
                    end, create_snapshot(Type), Payloads),
    {ok, S}.            
%TODO Error case

-ifdef(TEST).

materializer_test()->
    Type = antidote_crdt_counter_pn,
    PNCounter = create_snapshot(Type),
    ?assertEqual(0, Type:value(PNCounter)),
    %%  need to add the snapshot time for these for the test to pass
    Op1 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {1, 1}, txid = 1, snapshot_time=vectorclock:from_list([{1, 0}])},
    Op2 = #clocksi_payload{key = abc, type = Type,
                           op_param = 2,
                           commit_time = {1, 2}, txid = 2, snapshot_time=vectorclock:from_list([{1, 1}])},
    Op3 = #clocksi_payload{key = abc, type =Type,
                           op_param = 3,
                           commit_time = {1, 3}, txid = 3, snapshot_time=vectorclock:from_list([{1, 2}])},
    Op4 = #clocksi_payload{key = abc, type = Type,
                           op_param = 4,
                           commit_time = {1, 4}, txid = 4, snapshot_time=vectorclock:from_list([{1, 3}])},

    Ops = [{4, Op4}, {3, Op3}, {2, Op2}, {1, Op1}],

    {ok, PNCounter2} = materialize(Type, Ops),
    ?assertEqual(10, PNCounter2),
    ?assertEqual(10, Type:value(PNCounter2)).

materializer_register_test()->
    Type = antidote_crdt_register_mv,
    Initial = create_snapshot(Type),
    ?assertEqual([], Type:value(Initial)),
    %%  need to add the snapshot time for these for the test to pass
    Op1 = #clocksi_payload{key = abc, type = Type,
                           op_param = {1, 1, []},
                           commit_time = {1, 1}, txid = 1, snapshot_time=vectorclock:from_list([{0, 1}])},
    Op2 = #clocksi_payload{key = abc, type = Type,
                           op_param = {2, 2, [1]},
                           commit_time = {2, 1}, txid = 2, snapshot_time=vectorclock:from_list([{1, 1}])},
    Op3 = #clocksi_payload{key = abc, type = Type,
                           op_param = {3, 3, [2]},
                           commit_time = {3, 1}, txid = 3, snapshot_time=vectorclock:from_list([{2, 1}])},     
    Ops = [{3, Op3}, {2, Op2}, {1, Op1}],

    {ok, Final} = materialize(Type, Ops),
    ?assertEqual([3], Type:value(Final)).


%% Testing gcounter with empty update log
materializer_empty_test() ->
    Type = antidote_crdt_counter_pn,
    PNCounter = create_snapshot(Type),
    ?assertEqual(0, Type:value(PNCounter)),
    Ops = [],
    {ok, PNCounter2} = materialize(Type, Ops),
    ?assertEqual(0, Type:value(PNCounter2)).

%% Testing non-existing crdt
materializer_error_nocreate_test() ->
    ?assertException(error, undef, create_snapshot(bla)).

%% Testing crdt with invalid update operation
materializer_error_invalidupdate_test() ->
    Type = antidote_crdt_counter_pn,
    Counter = create_snapshot(Type),
    ?assertEqual(0, Type:value(Counter)),
    Ops = [{non_existing_op_type, {non_existing_op, actor1}}],
    Result = apply_ops(Type, Counter, Ops),
    ?assertEqual({error, {unexpected_operation,
            {non_existing_op_type, {non_existing_op, actor1}},
            antidote_crdt_counter_pn}}, Result).

%% Test for valid type of operation
check_operations_test() ->
    Operations =
        [{read, {key1, antidote_crdt_counter_pn}},
         {update, {key1, antidote_crdt_counter_pn, increment}}
        ],
    ?assertEqual(ok, check_ops(Operations)),

    Operations2 = [{read, {key1, antidote_crdt_counter_pn}},
        {update, {key1, antidote_crdt_counter_pn, {{add, elem}, a}}},
        {update, {key2, antidote_crdt_counter_pn, {increment, a}}},
        {read, {key1, antidote_crdt_counter_pn}}],
    ?assertMatch({error, _}, check_ops(Operations2)).

-endif.

% TODO Proper Test with MVR?