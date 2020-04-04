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
-include("gingko.hrl").

%% API
-export([group_by/2, add_to_value_list_or_create_single_value_list/3, sorted_insert/3, get_or_default_dict/3, get_or_default_map_list/3, get_or_default_map_list_check/3, concat_and_make_atom/1, atom_replace/3]).

%% @doc Takes function that groups entries form the given list in a dictionary
%%      For example grouping a list of journal entries by txid to get all journal entries that belong to a certain txid
-spec group_by(fun((ListType :: term()) -> GroupKeyType :: term()), [ListType :: term()]) -> dict:dict(GroupKeyType :: term(), ListType :: term()).
group_by(Fun, List) ->
  lists:foldr(
    fun({Key, Value}, Dict) ->
      dict:append(Key, Value, Dict)
    end, dict:new(), [{Fun(X), X} || X <- List]).

%% @doc Updates a dictionary that stores lists as values
%%      If a list exists for the given key then the given value is added to the list otherwise a new list with the given value is added to the dictionary
-spec add_to_value_list_or_create_single_value_list(DictionaryTypeAToValueTypeBList :: dict:dict(KeyTypeA :: term(), TypeBList :: [term()]), KeyTypeA :: term(), ValueTypeB :: term()) -> dict:dict(KeyTypeA :: term(), TypeBList :: [term()]).
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

-spec get_or_default_dict(dict:dict(TypeA :: term(), TypeB :: term()), TypeA :: term(), TypeB :: term()) -> TypeB :: term().
get_or_default_dict(Dict, Key, Default) ->
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


