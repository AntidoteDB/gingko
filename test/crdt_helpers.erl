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

-module(crdt_helpers).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

%% API
-export([get_counter_fat_decrement/2, get_counter_fat_increment/2, get_counter_fat_reset/1, get_counter_pn_decrement/2, get_counter_pn_increment/2, get_map_rr_remove_downstream/2, get_map_rr_reset_downstream/1, get_map_rr_update_downstream/3, get_set_remove_downstream/2, get_set_reset_downstream/2, get_set_rw_add_downstream/2]).

get_map_rr_update_downstream(KeyStruct, {InnerOpName, Value}, Map) ->
    Key = KeyStruct#key_struct.key,
    Type = KeyStruct#key_struct.type,
    {ok, DownstreamOp} = antidote_crdt_map_rr:downstream({update, {{Key, Type}, {InnerOpName, Value}}}, Map),
    DownstreamOp.

get_map_rr_remove_downstream(KeyStruct, Map) ->
    Key = KeyStruct#key_struct.key,
    Type = KeyStruct#key_struct.type,
    {ok, DownstreamOp} = antidote_crdt_map_rr:downstream({remove, {Key, Type}}, Map),
    DownstreamOp.

get_map_rr_reset_downstream(Map) ->
    {ok, DownstreamOp} = antidote_crdt_map_rr:downstream({reset, {}}, Map),
    DownstreamOp.

get_set_rw_add_downstream(Value, Set) ->
    {ok, DownstreamOp} = antidote_crdt_set_rw:downstream({add, Value}, Set),
    DownstreamOp.

get_set_remove_downstream(Value, Set) ->
    {ok, DownstreamOp} = antidote_crdt_set_rw:downstream({remove, Value}, Set),
    DownstreamOp.

get_set_reset_downstream(Value, Set) ->
    {ok, DownstreamOp} = antidote_crdt_set_rw:downstream({reset, {}}, Set),
    DownstreamOp.

get_counter_pn_increment(Value, PNCounter) ->
    {ok, DownstreamOp} = antidote_crdt_counter_pn:downstream({increment, Value}, PNCounter),
    DownstreamOp.

get_counter_pn_decrement(Value, PNCounter) ->
    {ok, DownstreamOp} = antidote_crdt_counter_pn:downstream({decrement, Value}, PNCounter),
    DownstreamOp.

get_counter_fat_increment(Value, FatCounter) ->
    {ok, DownstreamOp} = antidote_crdt_counter_fat:downstream({increment, Value}, FatCounter),
    DownstreamOp.

get_counter_fat_decrement(Value, FatCounter) ->
    {ok, DownstreamOp} = antidote_crdt_counter_fat:downstream({decrement, Value}, FatCounter),
    DownstreamOp.

get_counter_fat_reset(FatCounter) ->
    {ok, DownstreamOp} = antidote_crdt_counter_fat:downstream({reset, {}}, FatCounter),
    DownstreamOp.
