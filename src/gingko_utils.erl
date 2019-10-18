%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Okt 2019 17:45
%%%-------------------------------------------------------------------
-module(gingko_utils).
-author("kevin").
-include("gingko.hrl").

%% API
-export([]).

get_key(KeyStruct) when is_record(KeyStruct, key_struct) ->
  KeyStruct#key_struct.key;

get_key(ObjectOperation) when is_record(ObjectOperation, object_operation) ->
  get_key(ObjectOperation#object_operation.key_struct);

get_key(CheckpointEntry) when is_record(CheckpointEntry, object_operation) ->
  get_key(CheckpointEntry#object_operation.key_struct).

get_type(KeyStruct) ->
  KeyStruct#key_struct.type.


