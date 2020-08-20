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

-module(inter_dc_manager).
-include("inter_dc.hrl").

-export([leave_dc/0,
    create_dc/1,
    add_nodes_to_dc/1,
    connect_to_remote_dcs_and_start_dc/1,
    get_descriptor/0,
    connect_all_nodes_to_remote_dcs/1,
    reconnect_node_to_remote_dcs_after_restart/2,
    disconnect_remote_dcs_from_all_nodes/1]).

%% Command this node to leave the current data center
-spec leave_dc() -> ok | {error, term()}.
leave_dc() -> riak_core:leave().

%% backwards compatible function for add_nodes_to_dc
-spec create_dc([node()]) -> ok | {error, ring_not_ready}.
create_dc(Nodes) -> add_nodes_to_dc(Nodes).

%% Build a ring of Nodes forming a data center
-spec add_nodes_to_dc([node()]) -> ok | {error, reason()}.
add_nodes_to_dc(Nodes) ->
    %% check if ring is ready first
    case riak_core_ring:ring_ready() of
        true ->
            join_new_nodes(Nodes);
        _ ->
            {error, ring_not_ready}
    end.

%% ---------- Internal Functions --------------

-spec join_new_nodes([node()]) -> ok.
join_new_nodes(Nodes) ->
    %% get the current ring
    {ok, CurrentRing} = riak_core_ring_manager:get_my_ring(),

    %% filter nodes that are not already in this nodes ring
    CurrentNodeMembers = riak_core_ring:all_members(CurrentRing),

    NewNodeMembers = general_utils:list_without_elements_from_other_list(Nodes, CurrentNodeMembers),
    plan_and_commit(NewNodeMembers).


-spec plan_and_commit([node()]) -> ok.
plan_and_commit([]) -> logger:warning("No new nodes added to the ring of ~p", [node()]);
plan_and_commit(NewNodeMembers) ->
    lists:foreach(fun(Node) ->
        logger:info("Checking if Node ~p is reachable (from ~p)", [Node, node()]),
        pong = net_adm:ping(Node)
                  end, NewNodeMembers),

    lists:foreach(fun(Node) ->
        logger:info("Node ~p is joining my ring (~p)", [Node, node()]),
        ok = rpc:call(Node, riak_core, staged_join, [node()])
                  end, NewNodeMembers),

    lists:foreach(fun(Node) ->
        logger:info("Checking if node ring is ready (~p)", [Node]),
        wait_until_ring_ready(Node)
                  end, NewNodeMembers),

    {ok, Actions, Transitions} = riak_core_claimant:plan(),
    logger:debug("Actions planned: ~p", [Actions]),
    logger:debug("Ring transitions planned: ~p", [Transitions]),

    %% only after commit returns ok the ring structure will change
    %% even if nothing changes, it returns {error, nothing_planned} indicating some serious error
    %% could return {error, nothing_planned} if staged joins are disabled
    ok = riak_core_claimant:commit(),
    logger:debug("Ring committed and ring structure is changing. New ring members: ~p", [NewNodeMembers]),

    %% wait until ring is ready
    wait_until_ring_ready(node()),

    %% wait until ring has no pending changes
    %% this prevents writing to a ring which has not finished its balancing yet and therefore causes
    %% handoffs to be triggered
    %% FIXME this can be removed when #401 and #203 is fixed
    %%riak_core_handoff_manager:set_concurrency(16),

    %%riak_core_handoff_manager ! management_tick,
    wait_until_ring_no_pending_changes(),
    application:set_env(riak_core,
        vnode_management_timer,
        10000),
    ok.


%% @doc Wait until all nodes in this ring believe there are no
%% on-going or pending ownership transfers.
-spec wait_until_ring_no_pending_changes() -> ok.
wait_until_ring_no_pending_changes() ->
    {ok, CurrentRing} = riak_core_ring_manager:get_my_ring(),
    Nodes = riak_core_ring:all_members(CurrentRing),

    logger:debug("Wait until no pending changes on ~p", [Nodes]),
    F =
        fun() ->
            rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
            {Rings, BadNodes} = rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
            Changes = [riak_core_ring:pending_changes(Ring) =:= [] || {ok, Ring} <- Rings],
            BadNodes =:= [] andalso length(Changes) =:= length(Nodes) andalso lists:all(fun(T) -> T end, Changes)
        end,
    case F() of
        true -> ok;
        _ -> timer:sleep(?DEFAULT_WAIT_TIME_MEDIUM), wait_until_ring_no_pending_changes()
    end.


-spec wait_until_ring_ready(node()) -> ok.
wait_until_ring_ready(Node) ->
    Status = rpc:call(Node, riak_core_ring, ring_ready, []),
    logger:debug("Ring Status: ~p", [Status]),
    case Status of
        true -> ok;
        false -> timer:sleep(?DEFAULT_WAIT_TIME_SHORT), wait_until_ring_ready(Node)
    end.

%% Start receiving updates from other DCs
-spec connect_to_remote_dcs_and_start_dc([descriptor()]) -> ok.
connect_to_remote_dcs_and_start_dc(DCDescriptors) ->
    ok = connect_all_nodes_to_remote_dcs(DCDescriptors),
    ok = inter_dc_meta_data_manager:start_dc(),
    %%TODO Check return for errors
    true = inter_dc_meta_data_manager:has_dc_started_and_is_healthy(),
    ok.

-spec get_descriptor() -> descriptor().
get_descriptor() ->
    %% Wait until all needed vnodes are spawned, so that the heartbeats are already being sent
    Nodes = gingko_dc_utils:get_my_dc_nodes(),
    {TxnNodeAddressListList, []} = rpc:multicall(Nodes, inter_dc_utils, get_txn_address_list, []),
    TxnDcAddressList = lists:zip(Nodes, TxnNodeAddressListList),
    {RequestNodeAddressListList, []} = rpc:multicall(Nodes, inter_dc_utils, get_request_address_list, []),
    RequestDcAddressList = lists:zip(Nodes, RequestNodeAddressListList),
    #descriptor{
        dcid = gingko_dc_utils:get_my_dcid(),
        number_of_partitions = gingko_dc_utils:get_number_of_partitions(),
        txn_dc_address_list = TxnDcAddressList,
        request_dc_address_list = RequestDcAddressList
    }.

%% This will connect the list of local nodes to the DC given by the descriptor
%% When a connecting to a new DC, Nodes will be all the nodes in the local DC
%% Otherwise this will be called with a single node that is reconnecting (for example after one of the nodes in the DC crashes and restarts)
%% Note this is an internal function, to instruct the local DC to connect to a new DC the observe_dcs_sync(Descriptors) function should be used
-spec connect_nodes_to_remote_dc([node()], descriptor()) -> ok | {error, reason()}.
connect_nodes_to_remote_dc(Nodes, Descriptor = #descriptor{dcid = DCID, number_of_partitions = RemoteNumberOfPartitions}) ->
    LocalNumberOfPartitions = gingko_dc_utils:get_number_of_partitions(),
    case RemoteNumberOfPartitions == LocalNumberOfPartitions of
        false ->
            logger:info("Cannot observe remote DC: partition number mismatch"),
            {error, {number_of_partitions_mismatch, RemoteNumberOfPartitions, LocalNumberOfPartitions}};
        true ->
            case DCID == gingko_dc_utils:get_my_dcid() of
                true -> ok;
                false ->
                    logger:info("Observing DC ~p", [DCID]),
                    %% Announce the new publisher addresses to all subscribers in this DC.
                    %% Equivalently, we could just pick one node in the DC and delegate all the subscription work to it.
                    %% But we want to balance the work, so all nodes take part in subscribing.
                    connect_nodes_to_remote_dc(Nodes, Descriptor, ?DC_CONNECT_RETRIES)
            end
    end.

-spec connect_nodes_to_remote_dc([node()], descriptor(), non_neg_integer()) -> ok | {error, reason()}.
connect_nodes_to_remote_dc([], _Descriptor, _Retries) ->
    ok;
connect_nodes_to_remote_dc(_Nodes, Descriptor, 0) ->
    ok = disconnect_remote_dcs_from_all_nodes([Descriptor]),
    {error, connection_error};
connect_nodes_to_remote_dc([Node | Rest], Descriptor = #descriptor{dcid = DCID, txn_dc_address_list = TxnDcAddressList, request_dc_address_list = RequestDcAddressList}, Retries) ->
    case rpc:call(Node, inter_dc_request_sender, add_dc, [DCID, RequestDcAddressList], ?COMM_TIMEOUT) of
        ok ->
            case rpc:call(Node, inter_dc_txn_receiver, add_dc, [DCID, TxnDcAddressList], ?COMM_TIMEOUT) of
                ok ->
                    connect_nodes_to_remote_dc(Rest, Descriptor, ?DC_CONNECT_RETRIES);
                _ ->
                    timer:sleep(?DC_CONNECT_RETRY_SLEEP),
                    logger:error("Unable to connect to publisher ~p", [DCID]),
                    connect_nodes_to_remote_dc([Node | Rest], Descriptor, Retries - 1)
            end;
        _ ->
            timer:sleep(?DC_CONNECT_RETRY_SLEEP),
            logger:error("Unable to connect to log reader ~p", [DCID]),
            connect_nodes_to_remote_dc([Node | Rest], Descriptor, Retries - 1)
    end.

-spec reconnect_node_to_remote_dcs_after_restart([descriptor()], node()) -> ok | {error, reason()}.
reconnect_node_to_remote_dcs_after_restart(Descriptors, Node) ->
    disconnect_remote_dcs_from_nodes(Descriptors, [Node]),
    connect_nodes_to_remote_dcs(Descriptors, [Node]).

-spec connect_all_nodes_to_remote_dcs([descriptor()]) -> ok | {error, reason()}.
connect_all_nodes_to_remote_dcs(Descriptors) ->
    Nodes = gingko_dc_utils:get_my_dc_nodes(),
    connect_nodes_to_remote_dcs(Descriptors, Nodes).

-spec connect_nodes_to_remote_dcs([descriptor()], [node()]) -> ok | {error, reason()}.
connect_nodes_to_remote_dcs(Descriptors, Nodes) ->
    try
        lists:foreach(
                fun(DC) ->
                    ok = connect_nodes_to_remote_dc(Nodes, DC)
                end, Descriptors),
        inter_dc_meta_data_manager:store_dc_descriptors(Descriptors)
    catch
        error:{badmatch, Error = {error, _}} -> Error
    end.

-spec disconnect_remote_dc_from_nodes(descriptor(), [node()]) -> ok.
disconnect_remote_dc_from_nodes(#descriptor{dcid = DcId}, Nodes) ->
    case DcId == gingko_dc_utils:get_my_dcid() of
        true -> ok;
        false ->
            logger:notice("Forgetting DC ~p", [DcId]),
            lists:foreach(fun(Node) -> ok = rpc:call(Node, inter_dc_request_sender, delete_dc, [DcId]) end, Nodes),
            lists:foreach(fun(Node) -> ok = rpc:call(Node, inter_dc_txn_receiver, delete_dc, [DcId]) end, Nodes)
    end.

-spec disconnect_remote_dcs_from_all_nodes([descriptor()]) -> ok.
disconnect_remote_dcs_from_all_nodes(Descriptors) ->
    Nodes = gingko_dc_utils:get_my_dc_nodes(),
    disconnect_remote_dcs_from_nodes(Descriptors, Nodes).

-spec disconnect_remote_dcs_from_nodes([descriptor()], [node()]) -> ok.
disconnect_remote_dcs_from_nodes(Descriptors, Nodes) ->
    lists:foreach(
        fun(Descriptor) ->
            disconnect_remote_dc_from_nodes(Descriptor, Nodes)
        end, Descriptors).
