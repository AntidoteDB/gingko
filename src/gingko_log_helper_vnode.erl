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

-module(gingko_log_helper_vnode).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").
-behaviour(riak_core_vnode).

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

-record(state, {
    partition = 0 :: partition_id(),
    table_name :: atom()
}).
-type state() :: #state{}.

%%%===================================================================
%%% Public API
%%%===================================================================

%%%===================================================================
%%% Spawning and vnode implementation
%%%===================================================================

-spec start_vnode(integer()) -> any().
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    default_vnode_behaviour:init(?MODULE, [Partition]),
    TableName = general_utils:concat_and_make_atom([integer_to_list(Partition), '_journal_entry']),
    {ok, #state{partition = Partition, table_name = TableName}}.

handle_command(Request = hello, Sender, State) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    {reply, ok, State};

handle_command(Request = {{read, KeyStruct}, TxId}, Sender, State = #state{table_name = TableName}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    Result = gingko_log_utils:perform_tx_read(KeyStruct, TxId, TableName),
    {reply, Result, State};

handle_command(Request = {{update, {{KeyStruct, TypeOp}, TxOpNum}}, TxId}, Sender, State = #state{partition = Partition, table_name = TableName}) ->
    default_vnode_behaviour:handle_command(?MODULE, Request, Sender, State),
    Result =
        case gingko_utils:generate_downstream_op(KeyStruct, TypeOp, TxId, TableName) of
            {ok, DownstreamOp} ->
                gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {{update, {{KeyStruct, DownstreamOp}, TxOpNum}}, TxId});
            Error -> Error
        end,
    {reply, Result, State};

handle_command(Request, Sender, State) -> default_vnode_behaviour:handle_command_crash(?MODULE, Request, Sender, State).
handoff_starting(TargetNode, State) -> default_vnode_behaviour:handoff_starting(?MODULE, TargetNode, State).
handoff_cancelled(State) -> default_vnode_behaviour:handoff_cancelled(?MODULE, State).
handoff_finished(TargetNode, State) -> default_vnode_behaviour:handoff_finished(?MODULE, TargetNode, State).
handle_handoff_command(Request, Sender, State) ->
    default_vnode_behaviour:handle_handoff_command(?MODULE, Request, Sender, State).
handle_handoff_data(BinaryData, State) -> default_vnode_behaviour:handle_handoff_data(?MODULE, BinaryData, State).
encode_handoff_item(Key, Value) -> default_vnode_behaviour:encode_handoff_item(?MODULE, Key, Value).
is_empty(State) -> default_vnode_behaviour:is_empty(?MODULE, State).
terminate(Reason, State) -> default_vnode_behaviour:terminate(?MODULE, Reason, State).
delete(State) -> default_vnode_behaviour:delete(?MODULE, State).
-spec handle_info(term(), state()) -> no_return().
handle_info(Request, State) -> default_vnode_behaviour:handle_info_crash(?MODULE, Request, State).
handle_exit(Pid, Reason, State) -> default_vnode_behaviour:handle_exit(?MODULE, Pid, Reason, State).
handle_coverage(Request, KeySpaces, Sender, State) ->
    default_vnode_behaviour:handle_coverage(?MODULE, Request, KeySpaces, Sender, State).
handle_overload_command(Request, Sender, Partition) ->
    default_vnode_behaviour:handle_overload_command(?MODULE, Request, Sender, Partition).
handle_overload_info(Request, Partition) -> default_vnode_behaviour:handle_overload_info(?MODULE, Request, Partition).

%%%===================================================================
%%% Internal functions
%%%===================================================================

