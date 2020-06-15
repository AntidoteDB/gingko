%% -------------------------------------------------------------------
%%
%% Copyright 2020, Kevin Bartik <k_bartik12@cs.uni-kl.de>
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

-module(general_utils).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

%% API
-export([run_function_with_unexpected_error/6,
    print_and_return_unexpected_error/5,
    get_timestamp_in_microseconds/0,
    list_all_equal/2,
    list_without_elements_from_other_list/2,
    set_equals_on_lists/2,
    remove_duplicates_and_lose_order/1,
    get_values/1,
    max_by/2,
    group_by/2,
    append_inner_dict/4,
    sorted_insert/3,
    get_or_default_dict/3,
    get_or_default_map_list/3,
    get_or_default_map_list_check/3,
    values_from_dict/1,
    concat_and_make_atom/1,
    atom_replace/3,
    parallel_map/2,
    parallel_foreach/2]).

-spec run_function_with_unexpected_error(term(), term(), atom(), atom(), list(), list()) -> term() | no_return().
run_function_with_unexpected_error(Result, ExpectedResult, Module, FunctionName, Args, ExtraArgs) ->
    case Result of
        ExpectedResult -> ExpectedResult;
        Error -> print_and_return_unexpected_error(Module, FunctionName, Args, Error, ExtraArgs)
    end.

-spec print_and_return_unexpected_error(atom(), atom(), list(), term(), list()) -> no_return().
print_and_return_unexpected_error(Module, FunctionName, Args, Error, ExtraArgs) ->
    logger:error("Unexpected Error!~nModule: ~p~nFunction: ~p~nArgs: ~p~nError: ~p~nExtraArgs: ~p~n", [Module, FunctionName, Args, Error, ExtraArgs]),
    error("Unexpected Error!").

-spec get_timestamp_in_microseconds() -> non_neg_integer().
get_timestamp_in_microseconds() ->
    {Mega, Sec, Micro} = erlang:timestamp(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

-spec list_all_equal(term(), list()) -> boolean().
list_all_equal(Result, List) ->
    [Result] == ordsets:from_list(List).

-spec list_without_elements_from_other_list(list(), list()) -> list().
list_without_elements_from_other_list(List, OtherList) ->
    lists:filter(fun(Member) -> not lists:member(Member, OtherList) end, List).

-spec set_equals_on_lists([TypeA :: term()], [TypeA :: term()]) -> boolean().
set_equals_on_lists(List, OtherList) ->
    ordsets:from_list(List) == ordsets:from_list(OtherList).

-spec remove_duplicates_and_lose_order(list()) -> list().
remove_duplicates_and_lose_order(List) ->
    ordsets:to_list(ordsets:from_list(List)).

-spec get_values([{Key :: term(), Value :: term()}] | dict:dict(Key :: term(), Value :: term())) -> ValueList :: [Value :: term()].
get_values(TupleList) when is_list(TupleList) ->
    lists:map(fun({_Key, Value}) -> Value end, TupleList);
get_values(Dict) ->
    get_values(dict:to_list(Dict)).

-spec max_by(fun((ListElem :: term()) -> integer()), list()) -> MaxListElem :: term().
max_by(Fun, List) ->
    hd(lists:sort(
        fun(ListElem1, ListElem2) ->
            Fun(ListElem1) > Fun(ListElem2)
        end, List)).

%% @doc Takes function that groups entries form the given list in a dictionary
%%      For example grouping a list of journal entries by txid to get all journal entries that belong to a certain txid
-spec group_by(fun((ListType :: term()) -> GroupKeyType :: term()), [ListType :: term()]) -> dict:dict(GroupKeyType :: term(), [ListType :: term()]).
group_by(Fun, List) ->
    lists:foldr(
        fun({Key, Value}, Dict) ->
            dict:append(Key, Value, Dict)
        end, dict:new(), [{Fun(X), X} || X <- List]).

-spec append_inner_dict(TypeA :: term(), TypeB :: term(), TypeC :: term(), dict:dict(TypeA :: term(), dict:dict(TypeB :: term(), [TypeC :: term()]))) -> dict:dict(TypeA :: term(), dict:dict(TypeB :: term(), [TypeC :: term()])).
append_inner_dict(OuterKey, InnerKey, InnerValue, Dict) ->
    InnerDict = get_or_default_dict(OuterKey, Dict, dict:new()),
    UpdatedInnerDict = dict:append(InnerKey, InnerValue, InnerDict),
    dict:store(OuterKey, UpdatedInnerDict, Dict).

-spec sorted_insert(TypeA :: term(), [TypeA :: term()], fun((TypeA :: term(), TypeA :: term()) -> boolean())) -> [TypeA :: term()].
sorted_insert(X, [], _Comparer) -> [X];
sorted_insert(X, L = [H | T], Comparer) ->
    case Comparer(X, H) of
        true -> [X | L];
        false -> [H | sorted_insert(X, T, Comparer)]
    end.

-spec get_or_default_dict(TypeA :: term(), dict:dict(TypeA :: term(), TypeB :: term()), TypeB :: term()) -> TypeB :: term().
get_or_default_dict(Key, Dict, Default) ->
    case dict:find(Key, Dict) of
        {ok, Value} -> Value;
        error -> Default
    end.

-spec get_or_default_map_list(Key :: term(), MapList :: map_list(), Default :: term()) -> ValueOrDefault :: term().
get_or_default_map_list(Key, MapList, Default) ->
    case lists:keyfind(Key, 1, MapList) of
        {Key, Value} -> Value;
        false -> Default
    end.

-spec get_or_default_map_list_check(Key :: term(), MapList :: map_list(), Default :: term()) -> {ValueDifferentToDefault :: boolean(), ValueOrDefault :: term()}.
get_or_default_map_list_check(Key, MapList, Default) ->
    Value = get_or_default_map_list(Key, MapList, Default),
    {Default /= Value, Value}.

-spec values_from_dict(dict:dict(KeyType :: term(), ValueType :: term())) -> [ValueType :: term()].
values_from_dict(Dict) ->
    dict:fold(fun(_Key, Value, ValueListAcc) -> [Value | ValueListAcc] end, [], Dict).

-spec concat_and_make_atom([string() | atom()]) -> atom().
concat_and_make_atom(StringOrAtomList) ->
    list_to_atom(lists:append(lists:map(fun(Item) ->
        case is_atom(Item) of
            true -> atom_to_list(Item);
            false -> Item
        end
                                        end, StringOrAtomList))).

-spec atom_replace(atom(), atom(), string()) -> atom().
atom_replace(Atom, AtomToReplace, ReplacementString) ->
    String = atom_to_list(Atom),
    StringToReplace = atom_to_list(AtomToReplace),
    list_to_atom(string:replace(String, StringToReplace, ReplacementString)).

%% Parallel version of lists:map/2
%% For each list element a new process is started that executes the given function
%% The results are ordered in the same way as the original list
%% Taken from antidote test_utils and updated readability
%% TODO test
-spec parallel_map(fun((TypeA) -> TypeB), [TypeA]) -> [TypeB].
parallel_map(Function, List) ->
    Parent = self(),
    lists:foldl(
        fun(ListElem, IndexAcc) ->
            spawn_link(fun() ->
                Parent ! {pmap, IndexAcc, Function(ListElem)}
                       end),
            IndexAcc + 1
        end, 0, List),
    ReceivedUnorderedList = [receive {pmap, Index, FunctionResult} -> {Index, FunctionResult} end || _ <- List],
    {_, ReceivedOrderedList} = lists:unzip(lists:keysort(1, ReceivedUnorderedList)),
    ReceivedOrderedList.

%% Parallel version of lists:foreach/2
%% For each list element a new process is started that executes the given function
-spec parallel_foreach(fun((TypeA) -> any()), [TypeA]) -> ok.
parallel_foreach(Function, List) ->
    lists:foreach(fun(ListElem) -> spawn_link(fun() -> Function(ListElem) end) end, List).


