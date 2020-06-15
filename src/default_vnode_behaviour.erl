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

-module(default_vnode_behaviour).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-export([init/2,
    handle_command/4,
    handle_command_crash/4,
    handoff_starting/3,
    handoff_cancelled/2,
    handoff_finished/3,
    handle_handoff_command/4,
    handle_handoff_data/3,
    encode_handoff_item/3,
    is_empty/2,
    terminate/3,
    delete/2,
    handle_info/3,
    handle_info_crash/3,
    handle_exit/4,
    handle_coverage/5,
    handle_overload_command/4,
    handle_overload_info/3]).

-spec init(atom(), [non_neg_integer()]) -> {ok, none}.
init(Module, [Partition]) ->
    logger:debug("~p:init(~nPartition: ~p~n)", [Module, Partition]),
    {ok, none}.

-spec handle_command(atom(), term(), term(), term()) -> {reply, default_response, term()}.
handle_command(Module, Request, Sender, State) ->
    logger:debug("~p:handle_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Module, Request, Sender, State]),
    {reply, default_response, State}.

-spec handle_command_crash(atom(), term(), term(), term()) -> no_return().
handle_command_crash(Module, Request, Sender, State) ->
    handle_command(Module, Request, Sender, State),
    error("handle_command_crash").

-spec handoff_starting(atom(), term(), term()) -> {true, term()}.
handoff_starting(Module, TargetNode, State) ->
    logger:debug("~p:handoff_starting(~nTargetNode: ~p~nState: ~p~n)", [Module, TargetNode, State]),
    {true, State}.

-spec handoff_cancelled(atom(), term()) -> {ok, term()}.
handoff_cancelled(Module, State) ->
    logger:debug("~p:handoff_cancelled(~nState: ~p~n)", [Module, State]),
    {ok, State}.

-spec handoff_finished(atom(), term(), term()) -> {ok, term()}.
handoff_finished(Module, TargetNode, State) ->
    logger:debug("~p:handoff_finished(~nTargetNode: ~p~nState: ~p~n)", [Module, TargetNode, State]),
    {ok, State}.

-spec handle_handoff_command(atom(), term(), term(), term()) -> {reply, default_response, term()}.
handle_handoff_command(Module, Request, Sender, State) ->
    logger:debug("~p:handle_handoff_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Module, Request, Sender, State]),
    {reply, default_response, State}.

-spec handle_handoff_data(atom(), binary(), term()) -> {reply, ok, term()}.
handle_handoff_data(Module, BinaryData, State) ->
    logger:debug("~p:handle_handoff_data(~nData: ~p~nState: ~p~n)", [Module, binary_to_term(BinaryData), State]),
    {reply, ok, State}.

-spec encode_handoff_item(atom(), term(), term()) -> binary().
encode_handoff_item(Module, Key, Value) ->
    logger:debug("~p:encode_handoff_item(~nKey: ~p~nValue: ~p~n)", [Module, Key, Value]),
    term_to_binary({Key, Value}).

-spec is_empty(atom(), term()) -> {true, term()}.
is_empty(Module, State) ->
    logger:debug("~p:is_empty(~nState: ~p~n)", [Module, State]),
    {true, State}.

-spec terminate(atom(), term(), term()) -> ok.
terminate(Module, Reason, State) ->
    logger:debug("~p:terminate(~nReason: ~p~nState: ~p~n)", [Module, Reason, State]),
    ok.

-spec delete(atom(), term()) -> {ok, term()}.
delete(Module, State) ->
    logger:debug("~p:delete(~nRequest: ~p~n)", [Module, State]),
    {ok, State}.

-spec handle_info(atom(), term(), term()) -> {ok, term()}.
handle_info(Module, Request, State) ->
    logger:debug("~p:handle_info(~nRequest: ~p~nState: ~p~n)", [Module, Request, State]),
    {ok, State}.

-spec handle_info_crash(atom(), term(), term()) -> no_return().
handle_info_crash(Module, Request, State) ->
    handle_info(Module, Request, State),
    error("handle_info_crash").

-spec handle_exit(atom(), pid(), term(), term()) -> {noreply, term()}.
handle_exit(Module, Pid, Reason, State) ->
    logger:debug("~p:handle_exit(~nPid: ~p~nReason: ~p~nState: ~p~n)", [Module, Pid, Reason, State]),
    {noreply, State}.

-spec handle_coverage(atom(), term(), term(), term(), term()) -> {stop, not_implemented, term()}.
handle_coverage(Module, Request, KeySpaces, Sender, State) ->
    logger:debug("~p:handle_coverage(~nRequest: ~p~nKeySpaces: ~p~nSender: ~p~nState: ~p~n)", [Module, Request, KeySpaces, Sender, State]),
    {stop, not_implemented, State}.

-spec handle_overload_command(atom(), term(), term(), partition_id()) -> ok.
handle_overload_command(Module, Request, Sender, Partition) ->
    logger:debug("~p:handle_overload_command(~nRequest: ~p~nSender: ~p~nPartition: ~p~n)", [Module, Request, Sender, Partition]),
    ok.

-spec handle_overload_info(atom(), term(), partition_id()) -> ok.
handle_overload_info(Module, Request, Partition) ->
    logger:debug("~p:handle_overload_info(~nRequest: ~p~nPartition: ~p~n)", [Module, Request, Partition]),
    ok.
