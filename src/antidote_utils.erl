%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% This file is combination of the dc_utilities and log_utilities
%% files from the antidote project https://github.com/AntidoteDB/antidote

-module(antidote_utils).
-include("gingko.hrl").

-export([get_my_dc_id/0,
    get_my_dc_nodes/0,

    partition_to_indexnode/1,
    get_all_partitions/0,
    get_all_partitions_nodes/0,
    get_my_partitions/0,
    get_number_of_partitions/0,

    call_vnode_async/3,
    call_vnode_sync/3,
    call_local_vnode_async/3,
    call_local_vnode_sync/3,
    call_vnode_async_with_key/3,
    call_vnode_sync_with_key/3,
    bcast_local_vnode_async/2,
    bcast_local_vnode_sync/2,
    bcast_vnode_async/2,
    bcast_vnode_sync/2,

    get_key_partition/1,
    get_preflist_from_key/1,

    check_registered/1,

    is_ring_ready/1]).

%% Returns the ID of the current DC.
-spec get_my_dc_id() -> dcid().
get_my_dc_id() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:cluster_name(Ring).

%% Returns the list of all node addresses in the cluster.
-spec get_my_dc_nodes() -> [node()].
get_my_dc_nodes() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:all_members(Ring).

%% Returns the IndexNode tuple used by riak_core_vnode_master:command functions.
-spec partition_to_indexnode(partition_id()) -> {partition_id(), node()}.
partition_to_indexnode(Partition) ->
    {Partition, get_node_of_partition(Partition)}.

-spec get_node_of_partition(partition_id()) -> node().
get_node_of_partition(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:index_owner(Ring, Partition).

%% Returns a list of all partition indices in the cluster.
%% The partitions indices are 160-bit numbers that equally division the keyspace.
%% For example, for a cluster with 8 partitions, the indices would take following values:
%% 0, 1 * 2^157, 2 * 2^157, 3 * 2^157, 4 * 2^157, 5 * 2^157, 6 * 2^157, 7 * 2^157.
%% The partition numbers are erlang integers. To obtain the binary representation of the index,
%% use the inter_dc_txn:partition_to_bin/1 function.
-spec get_all_partitions() -> [partition_id()].
get_all_partitions() ->
    try
        {ok, Ring} = riak_core_ring_manager:get_my_ring(),
        CHash = riak_core_ring:chash(Ring),
        Nodes = chash:nodes(CHash),
        [I || {I, _} <- Nodes]
    catch
        _Ex:Res ->
            logger:debug("Error loading partition names: ~p, will retry", [Res]),
            get_all_partitions()
    end.

%% Returns a list of all partition indcies plus the node each
%% belongs to
-spec get_all_partitions_nodes() -> [{partition_id(), node()}].
get_all_partitions_nodes() ->
    try
        {ok, Ring} = riak_core_ring_manager:get_my_ring(),
        CHash = riak_core_ring:chash(Ring),
        chash:nodes(CHash)
    catch
        _Ex:Res ->
            logger:debug("Error loading partition-node names ~p, will retry", [Res]),
            get_all_partitions_nodes()
    end.

%% Returns the partition indices hosted by the local (caller) node.
-spec get_my_partitions() -> [partition_id()].
get_my_partitions() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:my_indices(Ring).

%% Returns the number of partitions.
-spec get_number_of_partitions() -> non_neg_integer().
get_number_of_partitions() -> length(get_all_partitions()).


%% Sends the asynchronous command to a vnode of a specified type and responsible for a specified partition number.
-spec call_vnode_async(partition_id(), atom(), any()) -> ok.
call_vnode_async(Partition, VMaster, Request) ->
    riak_core_vnode_master:command(partition_to_indexnode(Partition), Request, VMaster).

%% Sends the synchronous command to a vnode of a specified type and responsible for a specified partition number.
-spec call_vnode_sync(partition_id(), atom(), any()) -> any().
call_vnode_sync(Partition, VMaster, Request) ->
    riak_core_vnode_master:sync_spawn_command(partition_to_indexnode(Partition), Request, VMaster).

%% Sends the asynchronous command to a vnode of a specified type and responsible for a specified partition number,
%% the partition must be on the same node that the command is run on
-spec call_local_vnode_async(partition_id(), atom(), any()) -> ok.
call_local_vnode_async(Partition, VMaster, Request) ->
    riak_core_vnode_master:command({Partition, node()}, Request, VMaster).

-spec call_local_vnode_sync(partition_id(), atom(), any()) -> any().
call_local_vnode_sync(Partition, VMaster, Request) ->
    riak_core_vnode_master:sync_spawn_command({Partition, node()}, Request, VMaster).

%% Sends the asynchronous command to a vnode of a specified type and responsible for a specified partition number.
-spec call_vnode_async_with_key(key(), atom(), any()) -> ok.
call_vnode_async_with_key(Key, VMaster, Request) ->
    IndexNode = get_key_partition(Key),
    riak_core_vnode_master:command(IndexNode, Request, VMaster).

%% Sends the synchronous command to a vnode of a specified type and responsible for a specified partition number.
-spec call_vnode_sync_with_key(key(), atom(), any()) -> any().
call_vnode_sync_with_key(Key, VMaster, Request) ->
    IndexNode = get_key_partition(Key),
    riak_core_vnode_master:sync_spawn_command(IndexNode, Request, VMaster).

-spec bcast_local_vnode_async(atom(), any()) -> ok.
bcast_local_vnode_async(VMaster, Request) ->
    general_utils:parallel_foreach(fun(P) -> call_vnode_async(P, VMaster, Request) end, get_my_partitions()).

-spec bcast_local_vnode_sync(atom(), any()) -> ok.
bcast_local_vnode_sync(VMaster, Request) ->
    general_utils:parallel_map(fun(P) -> {P, call_vnode_sync(P, VMaster, Request)} end, get_my_partitions()).

%% Sends the same (asynchronous) command to all vnodes of a given type.
-spec bcast_vnode_async(atom(), any()) -> ok.
bcast_vnode_async(VMaster, Request) ->
    general_utils:parallel_foreach(fun(P) -> call_vnode_async(P, VMaster, Request) end, get_all_partitions()).

%% Sends the same (synchronous) command to all vnodes of a given type.
-spec bcast_vnode_sync(atom(), any()) -> [{partition_id(), term()}].
bcast_vnode_sync(VMaster, Request) ->
    general_utils:parallel_map(fun(P) -> {P, call_vnode_sync(P, VMaster, Request)} end, get_all_partitions()).

%% Loops until a process with the given name is registered locally
-spec check_registered(atom()) -> ok.
check_registered(Name) ->
    case whereis(Name) of
        undefined ->
            logger:debug("Wait for ~p to register", [Name]),
            timer:sleep(?DEFAULT_WAIT_TIME_SHORT),
            check_registered(Name);
        _ ->
            ok
    end.

%% @doc get_key_partition returns the most probable node where a given
%%      key's logfile will be located.
-spec get_key_partition(key()) -> index_node().
get_key_partition(Key) ->
    IndexNode = hd(get_preflist_from_key(Key)),
    IndexNode.

%% @doc get_preflist_from_key returns a preference list where a given
%%      key's logfile will be located.
-spec get_preflist_from_key(key()) -> preflist().
get_preflist_from_key(Key) ->
    ConvertedKey = convert_key(Key),
    get_primaries_preflist(ConvertedKey).

%% @doc get_primaries_preflist returns the preflist with the primary
%%      vnodes. No matter they are up or down.
%%      Input:  A hashed key
%%      Return: The primaries preflist
%%
-spec get_primaries_preflist(non_neg_integer()) -> preflist().
get_primaries_preflist(Key) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {NumPartitions, ListOfPartitions} = riak_core_ring:chash(Ring),
    Pos = Key rem NumPartitions + 1,
    {Index, Node} = lists:nth(Pos, ListOfPartitions),
    [{Index, Node}].

%% @doc Convert key. If the key is integer(or integer in form of binary),
%% directly use it to get the partition. If it is not integer, convert it
%% to integer using hash.
-spec convert_key(key()) -> non_neg_integer().
convert_key(Key) ->
    case is_binary(Key) of
        true ->
            KeyInt = (catch list_to_integer(binary_to_list(Key))),
            case is_integer(KeyInt) of
                true -> abs(KeyInt);
                false ->
                    HashedKey = riak_core_util:chash_key({?BUCKET, Key}),
                    abs(crypto:bytes_to_integer(HashedKey))
            end;
        false ->
            case is_integer(Key) of
                true ->
                    abs(Key);
                false ->
                    HashedKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
                    abs(crypto:bytes_to_integer(HashedKey))
            end
    end.

%% @doc Calls the riak core ring manager to check if the ring of the given node is ready
-spec is_ring_ready(node()) -> boolean().
is_ring_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} -> riak_core_ring:ring_ready(Ring);
        _ -> false
    end.


