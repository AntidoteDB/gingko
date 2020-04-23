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

-module(gingko_vnode).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").
-include_lib("kernel/include/logger.hrl").
-behaviour(riak_core_vnode).

%% API
-export([start_vnode/1,
    init/1,
    handle_command/3,
    handoff_starting/2,
    handoff_cancelled/1,
    handoff_finished/2,
    handle_handoff_command/3,
    handle_handoff_data/2,
    encode_handoff_item/2,
    is_empty/1,
    terminate/2,
    delete/1,
    handle_info/2,
    handle_exit/3,
    handle_coverage/4,
    handle_overload_command/3,
    handle_overload_info/2]).
-export([process_command/3]).

-record(state, {
    partition :: partition_id(),
    running_txid_to_partitions = dict:new() :: dict:dict(txid(), [partition_id()]),
    running_txid_to_ops = dict:new() :: dict:dict(txid(), list())
}).
-type state() :: #state{}.

-spec start_vnode(integer()) -> any().
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    logger:debug("init(~nPartition: ~p~n)", [Partition]),
    {ok, #state{partition = Partition}}.

handle_command({{_Op, _Args}, _TxId} = Request, Sender, State) ->
    logger:debug("handle_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Request, Sender, State]),
    process_command(Request, Sender, State);

handle_command(Request, Sender, State) ->
    logger:debug("handle_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Request, Sender, State]),
    {noreply, State}.

handoff_starting(TargetNode, State) ->
    logger:debug("handoff_starting(~nTargetNode: ~p~nState: ~p~n)", [TargetNode, State]),
    {true, State}.

handoff_cancelled(State) ->
    logger:debug("handoff_cancelled(~nState: ~p~n)", [State]),
    {ok, State}.

handoff_finished(TargetNode, State) ->
    logger:debug("handoff_finished(~nTargetNode: ~p~nState: ~p~n)", [TargetNode, State]),
    {ok, State}.

handle_handoff_command(Request, Sender, State) ->
    logger:debug("handle_handoff_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Request, Sender, State]),
    {noreply, State}.

handle_handoff_data(BinaryData, State) ->
    logger:debug("handle_handoff_data(~nData: ~p~nState: ~p~n)", [binary_to_term(BinaryData), State]),
    {reply, ok, State}.

encode_handoff_item(Key, Value) ->
    logger:debug("encode_handoff_item(~nKey: ~p~nValue: ~p~n)", [Key, Value]),
    term_to_binary({Key, Value}).

is_empty(State) ->
    logger:debug("is_empty(~nState: ~p~n)", [State]),
    {true, State}.

terminate(Reason, State) ->
    logger:debug("terminate(~nReason: ~p~nState: ~p~n)", [Reason, State]),
    ok.

delete(State) ->
    logger:debug("delete(~nRequest: ~p~n)", [State]),
    {ok, State}.

handle_info(Request, State) ->
    logger:debug("handle_info(~nRequest: ~p~nState: ~p~n)", [Request, State]),
    {ok, State}.

handle_exit(Pid, Reason, State) ->
    logger:debug("handle_exit(~nPid: ~p~nReason: ~p~nState: ~p~n)", [Pid, Reason, State]),
    {noreply, State}.

handle_coverage(Request, KeySpaces, Sender, State) ->
    logger:debug("handle_coverage(~nRequest: ~p~nKeySpaces: ~p~nSender: ~p~nState: ~p~n)", [Request, KeySpaces, Sender, State]),
    {stop, not_implemented, State}.

handle_overload_command(Request, Sender, Partition) ->
    logger:debug("handle_overload_command(~nRequest: ~p~nSender: ~p~nPartition: ~p~n)", [Request, Sender, Partition]),
    ok.

handle_overload_info(Request, Partition) ->
    logger:debug("handle_overload_info(~nRequest: ~p~nPartition: ~p~n)", [Request, Partition]),
    ok.

process_command({{read, KeyStruct}, TxId}  = Request, _Sender, State) ->
    {PartitionId, _Node} = antidote_log_utilities:get_key_partition(KeyStruct),
    UpdatedDict1 = general_utils:add_to_value_list_or_create_single_value_list(State#state.running_txid_to_partitions, TxId, PartitionId),
    UpdatedDict2 = general_utils:add_to_value_list_or_create_single_value_list(State#state.running_txid_to_ops, TxId, Request),
    Result = antidote_dc_utilities:call_vnode_sync_with_key(KeyStruct, gingko_log_vnode_master, Request),
    {reply, Result, State#state{running_txid_to_partitions = UpdatedDict1, running_txid_to_ops = UpdatedDict2}};

process_command({{update, KeyStruct, _TypeOp}, TxId} = Request, _Sender, State) ->
    {PartitionId, _Node} = antidote_log_utilities:get_key_partition(KeyStruct),
    UpdatedDict1 = general_utils:add_to_value_list_or_create_single_value_list(State#state.running_txid_to_partitions, TxId, PartitionId),
    UpdatedDict2 = general_utils:add_to_value_list_or_create_single_value_list(State#state.running_txid_to_ops, TxId, Request),
    Result = antidote_dc_utilities:call_vnode_sync_with_key(KeyStruct, gingko_log_vnode_master, Request),
    {reply, Result, State#state{running_txid_to_partitions = UpdatedDict1, running_txid_to_ops = UpdatedDict2}};

process_command({{begin_txn, _DependencyVts}, _TxId} = Request, _Sender, State) ->
    {reply, antidote_dc_utilities:bcast_vnode(gingko_log_vnode_master, Request), State};

process_command({{prepare_txn, _Args}, TxId} = Request, _Sender, State) ->
    UpdatedPartitionsResult = dict:find(TxId, State#state.running_txid_to_partitions),
    OpsResult = dict:find(TxId, State#state.running_txid_to_ops),
    case {UpdatedPartitionsResult, OpsResult} of
        {error, error} -> {reply, ok, State};
        {error, {ok, _}} -> {reply, {error, "Bad State"}, State}; %TODO
        {{ok, _}, error} -> {reply, {error, "Bad State"}, State};
        {{ok, UpdatedPartitions}, {ok, Ops}} ->
            PrepareResults = lists:map(fun(Partition) ->
                antidote_dc_utilities:call_vnode_sync(Partition, gingko_log_vnode_master, {Request, Ops}) end, UpdatedPartitions),
            AllOk = sets:size(sets:del_element(ok, sets:from_list(PrepareResults))) == 0,
            case AllOk of
                true -> {reply, ok, State};
                false -> {error, "Validation failed"}
            end
    end;

process_command({{commit_txn, _Args}, TxId} = Request, _Sender, State) ->
    UpdatedDict1 = dict:erase(TxId, State#state.running_txid_to_partitions),
    UpdatedDict2 = dict:erase(TxId, State#state.running_txid_to_ops),
    {reply, antidote_dc_utilities:bcast_vnode(gingko_log_vnode_master, Request), State#state{running_txid_to_partitions = UpdatedDict1, running_txid_to_ops = UpdatedDict2}};

process_command({{abort_txn, _Args}, TxId} = Request, _Sender, State) ->
    UpdatedDict1 = dict:erase(TxId, State#state.running_txid_to_partitions),
    UpdatedDict2 = dict:erase(TxId, State#state.running_txid_to_ops),
    {reply, antidote_dc_utilities:bcast_vnode(gingko_log_vnode_master, Request), State#state{running_txid_to_partitions = UpdatedDict1, running_txid_to_ops = UpdatedDict2}}.
