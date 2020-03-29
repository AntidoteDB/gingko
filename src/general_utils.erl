%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Mär 2020 10:56
%%%-------------------------------------------------------------------
-module(general_utils).
-author("kevin").

%% API
-export([group_by/2, add_to_value_list_or_create_single_value_list/3, sorted_insert/3, get_or_default/3]).

-spec group_by(fun((ListType :: term()) -> GroupKeyType :: term()), [ListType :: term()]) -> dict:dict(GroupKeyType :: term(), ListType :: term()).
group_by(Fun, List) ->
  lists:foldr(
    fun({Key, Value}, Dict) ->
      dict:append(Key, Value, Dict)
    end, dict:new(), [{Fun(X), X} || X <- List]).

-spec add_to_value_list_or_create_single_value_list(DictionaryTypeAToValueTypeBList :: dict:dict(KeyTypeA :: term(), TypeBList :: [term()]), KeyTypeA :: term(), ValueTypeB :: term()) -> term().
add_to_value_list_or_create_single_value_list(DictionaryTypeAToValueTypeBList, KeyTypeA, ValueTypeB) ->
  dict:update(KeyTypeA, fun(ValueTypeBList) ->
    [ValueTypeB | ValueTypeBList] end, [ValueTypeB], DictionaryTypeAToValueTypeBList).

-spec sorted_insert(TypeA :: term(), [TypeA :: term()], fun((TypeA :: term(), TypeA :: term()) -> boolean())) -> [TypeA :: term()].
sorted_insert(X, [], _Comparer) -> [X];
sorted_insert(X, L = [H | T], Comparer) ->
  case Comparer(X, H) of
    true -> [X | L];
    false -> [H | sorted_insert(X, T, Comparer)]
  end.

-spec get_or_default(dict:dict(TypeA :: term(), TypeB :: term()), TypeA :: term(), TypeB :: term()) -> TypeB :: term().
get_or_default(Dict, Key, Default) ->
  case dict:find(Key, Dict) of
    {ok, Value} -> Value;
    error -> Default
  end.