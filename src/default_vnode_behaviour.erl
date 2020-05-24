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

-export([init/2,
    handle_command/4,
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
    handle_exit/4,
    handle_coverage/5,
    handle_overload_command/4,
    handle_overload_info/3]).

init(Module, [Partition]) ->
    logger:debug("~p:init(~nPartition: ~p~n)", [Module, Partition]),
    {ok, none}.

handle_command(Module, Request, Sender, State) ->
    logger:debug("~p:handle_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Module, Request, Sender, State]),
    {reply, default_response, State}.

handoff_starting(Module, TargetNode, State) ->
    logger:debug("~p:handoff_starting(~nTargetNode: ~p~nState: ~p~n)", [Module, TargetNode, State]),
    {true, State}.

handoff_cancelled(Module, State) ->
    logger:debug("~p:handoff_cancelled(~nState: ~p~n)", [Module, State]),
    {ok, State}.

handoff_finished(Module, TargetNode, State) ->
    logger:debug("~p:handoff_finished(~nTargetNode: ~p~nState: ~p~n)", [Module, TargetNode, State]),
    {ok, State}.

handle_handoff_command(Module, Request, Sender, State) ->
    logger:debug("~p:handle_handoff_command(~nRequest: ~p~nSender: ~p~nState: ~p~n)", [Module, Request, Sender, State]),
    {reply, default_response, State}.

handle_handoff_data(Module, BinaryData, State) ->
    logger:debug("~p:handle_handoff_data(~nData: ~p~nState: ~p~n)", [Module, binary_to_term(BinaryData), State]),
    {reply, ok, State}.

encode_handoff_item(Module, Key, Value) ->
    logger:debug("~p:encode_handoff_item(~nKey: ~p~nValue: ~p~n)", [Module, Key, Value]),
    term_to_binary({Key, Value}).

is_empty(Module, State) ->
    logger:debug("~p:is_empty(~nState: ~p~n)", [Module, State]),
    {true, State}.

terminate(Module, Reason, State) ->
    logger:debug("~p:terminate(~nReason: ~p~nState: ~p~n)", [Module, Reason, State]),
    ok.

delete(Module, State) ->
    logger:debug("~p:delete(~nRequest: ~p~n)", [Module, State]),
    {ok, State}.

handle_info(Module, Request, State) ->
    logger:debug("~p:handle_info(~nRequest: ~p~nState: ~p~n)", [Module, Request, State]),
    {ok, State}.

handle_exit(Module, Pid, Reason, State) ->
    logger:debug("~p:handle_exit(~nPid: ~p~nReason: ~p~nState: ~p~n)", [Module, Pid, Reason, State]),
    {noreply, State}.

handle_coverage(Module, Request, KeySpaces, Sender, State) ->
    logger:debug("~p:handle_coverage(~nRequest: ~p~nKeySpaces: ~p~nSender: ~p~nState: ~p~n)", [Module, Request, KeySpaces, Sender, State]),
    {stop, not_implemented, State}.

handle_overload_command(Module, Request, Sender, Partition) ->
    logger:debug("~p:handle_overload_command(~nRequest: ~p~nSender: ~p~nPartition: ~p~n)", [Module, Request, Sender, Partition]),
    ok.

handle_overload_info(Module, Request, Partition) ->
    logger:debug("~p:handle_overload_info(~nRequest: ~p~nPartition: ~p~n)", [Module, Request, Partition]),
    ok.
