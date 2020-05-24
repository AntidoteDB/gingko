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

-module(gingko_server).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-behaviour(gen_server).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    running_txid_to_partitions = dict:new() :: dict:dict(txid(), [partition_id()]),
    running_txid_to_ops = dict:new() :: dict:dict(txid(), list())
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    {ok, #state{}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request = {{_Op, _Args}, _TxId}, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    process_command(Request, State);

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call(?MODULE, Request, From, State).

handle_cast(Request = {{_Op, _Args}, _TxId}, State) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    {reply, _Result, NewState} = process_command(Request, State),
    {noreply, NewState};

handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast(?MODULE, Request, State).
handle_info(Info, State) -> default_gen_server_behaviour:handle_info(?MODULE, Info, State).
terminate(Reason, State) -> default_gen_server_behaviour:terminate(?MODULE, Reason, State).
code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec process_command({{Op :: atom(), Args :: term()}, txid()}, state()) -> {reply, term(), state()}.
process_command({{read, KeyStruct}, TxId} = Request, State) ->
    {PartitionId, _Node} = gingko_utils:get_key_partition(KeyStruct),
    UpdatedDict1 = general_utils:add_to_value_list_or_create_single_value_list(TxId, PartitionId, State#state.running_txid_to_partitions),
    UpdatedDict2 = general_utils:add_to_value_list_or_create_single_value_list(TxId, Request, State#state.running_txid_to_ops),
    Result = gingko_utils:call_gingko_sync_with_key(KeyStruct, ?GINGKO_LOG, Request),
    {reply, Result, State#state{running_txid_to_partitions = UpdatedDict1, running_txid_to_ops = UpdatedDict2}};

process_command({{update, {KeyStruct, _TypeOp}}, TxId} = Request, State) ->
    {PartitionId, _Node} = gingko_utils:get_key_partition(KeyStruct),
    UpdatedDict1 = general_utils:add_to_value_list_or_create_single_value_list(TxId, PartitionId, State#state.running_txid_to_partitions),
    UpdatedDict2 = general_utils:add_to_value_list_or_create_single_value_list(TxId, Request, State#state.running_txid_to_ops),
    Result = gingko_utils:call_gingko_sync_with_key(KeyStruct, ?GINGKO_LOG, Request),
    {reply, Result, State#state{running_txid_to_partitions = UpdatedDict1, running_txid_to_ops = UpdatedDict2}};

process_command({{begin_txn, _DependencyVts}, _TxId} = Request, State) ->
    {reply, gingko_utils:bcast_gingko_async(?GINGKO_LOG, Request), State};

process_command({{prepare_txn, _Args}, TxId} = Request, State) ->
    UpdatedPartitionsResult = dict:find(TxId, State#state.running_txid_to_partitions),
    OpsResult = dict:find(TxId, State#state.running_txid_to_ops),
    case {UpdatedPartitionsResult, OpsResult} of
        {error, error} -> {reply, ok, State};
        {error, {ok, _}} -> {reply, {error, "Bad State"}, State}; %TODO
        {{ok, _}, error} -> {reply, {error, "Bad State"}, State};
        {{ok, UpdatedPartitions}, {ok, Ops}} ->
            PrepareResults = general_utils:parallel_map(fun(Partition) ->
                gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {Request, Ops}) end, UpdatedPartitions),
            AllOk = sets:size(sets:del_element(ok, sets:from_list(PrepareResults))) == 0,
            case AllOk of
                true -> {reply, ok, State};
                false -> {error, "Validation failed"}
            end
    end;

process_command({{commit_txn, _Args}, TxId} = Request, State = #state{running_txid_to_partitions = PartitionDict, running_txid_to_ops = OpsDict}) ->
    UpdatedDict1 = dict:erase(TxId, PartitionDict),
    UpdatedDict2 = dict:erase(TxId, OpsDict),
    {reply, gingko_utils:bcast_gingko_async(?GINGKO_LOG, Request), State#state{running_txid_to_partitions = UpdatedDict1, running_txid_to_ops = UpdatedDict2}};

process_command({{abort_txn, _Args}, TxId} = Request, State) ->
    UpdatedDict1 = dict:erase(TxId, State#state.running_txid_to_partitions),
    UpdatedDict2 = dict:erase(TxId, State#state.running_txid_to_ops),
    {reply, gingko_utils:bcast_gingko_async(?GINGKO_LOG, Request), State#state{running_txid_to_partitions = UpdatedDict1, running_txid_to_ops = UpdatedDict2}};

process_command({{checkpoint, _Args}, _TxId} = Request, State) ->
    {reply, gingko_utils:bcast_gingko_sync(?GINGKO_LOG, Request), State}.
