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

-module(inter_dc_txn_sender).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc.hrl").
-behaviour(gen_server).

-export([broadcast_txn/2]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    journal_socket :: zmq_socket(),
    partition_to_last_send_time = #{} :: #{partition_id() => timestamp()},
    ping_timer :: reference()
}).

%%%===================================================================
%%% Public API
%%%===================================================================

-spec broadcast_txn(partition_id(), [journal_entry()]) -> ok.
broadcast_txn(Partition, JournalEntryList) ->
    gen_server:cast(?MODULE, {inter_dc_txn, Partition, JournalEntryList}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    default_gen_server_behaviour:init(?MODULE, []),
    {_, JournalPort} = inter_dc_utils:get_journal_address(),
    JournalSocket = zmq_utils:create_bind_socket(pub, false, JournalPort),
    logger:info("Publisher started on port ~p", [JournalPort]),
    PingTimer = erlang:send_after(?TXN_PING_FREQ, self(), send_ping),
    CurrentTime = gingko_utils:get_timestamp(),
    MyPartitions = gingko_utils:get_my_partitions(),
    PartitionToLastSendTime = lists:foldl(fun(Partition, MapAcc) -> MapAcc#{Partition => CurrentTime} end, #{}, MyPartitions),
    {ok, #state{journal_socket = JournalSocket, ping_timer = PingTimer, partition_to_last_send_time = PartitionToLastSendTime}}.

handle_call(Request = hello, From, State) ->
    default_gen_server_behaviour:handle_call(?MODULE, Request, From, State),
    {reply, ok, State};

handle_call(Request, From, State) -> default_gen_server_behaviour:handle_call_crash(?MODULE, Request, From, State).

handle_cast(Request = {inter_dc_txn, Partition, JournalEntryList}, State = #state{journal_socket = JournalSocket, partition_to_last_send_time = PartitionToLastSendTime}) ->
    default_gen_server_behaviour:handle_cast(?MODULE, Request, State),
    NewPartitionToLastSendTime = PartitionToLastSendTime#{Partition => gingko_utils:get_timestamp()},
    InterDcTxn = inter_dc_txn:from_journal_entries(Partition, JournalEntryList),
    BinaryMessage = inter_dc_txn:to_binary(InterDcTxn),
    ok = zmq_utils:try_send(JournalSocket, BinaryMessage),
    {noreply, State#state{partition_to_last_send_time = NewPartitionToLastSendTime}};

handle_cast(Request, State) -> default_gen_server_behaviour:handle_cast_crash(?MODULE, Request, State).

handle_info(Info = send_ping, State = #state{journal_socket = JournalSocket, ping_timer = PingTimer, partition_to_last_send_time = PartitionToLastSendTime}) ->
    default_gen_server_behaviour:handle_info(?MODULE, Info, State),
    erlang:cancel_timer(PingTimer),
    CurrentTime = gingko_utils:get_timestamp(),
    NewPartitionToLastSendTime =
        maps:map(
            fun(Partition, Timestamp) -> %%TODO redo time difference (currently microseconds)
                case (CurrentTime - Timestamp) > 1000000 of
                    true ->
                        InterDcTxn = inter_dc_txn:from_journal_entries(Partition, []),
                        BinaryMessage = inter_dc_txn:to_binary(InterDcTxn),
                        ok = zmq_utils:try_send(JournalSocket, BinaryMessage),
                        CurrentTime;
                    false -> CurrentTime
                end
            end, PartitionToLastSendTime),

    NewPingTimer = erlang:send_after(?TXN_PING_FREQ, self(), send_ping),
    {noreply, State#state{ping_timer = NewPingTimer, partition_to_last_send_time = NewPartitionToLastSendTime}};

handle_info(Info, State) -> default_gen_server_behaviour:handle_info_crash(?MODULE, Info, State).

terminate(Reason, State = #state{journal_socket = JournalSocket}) ->
    default_gen_server_behaviour:terminate(?MODULE, Reason, State),
    zmq_utils:close_socket(JournalSocket).

code_change(OldVsn, State, Extra) -> default_gen_server_behaviour:code_change(?MODULE, OldVsn, State, Extra).

%%%===================================================================
%%% Internal functions
%%%===================================================================
