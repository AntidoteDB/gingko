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

-module(gingko_app_sup).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").
-behaviour(supervisor).

-export([start_link/0,
    init/1]).

-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).
-define(VNODE(I, M), {I, {riak_core_vnode_master, start_link, [M]}, permanent, 5000, worker, [riak_core_vnode_master]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    GingkoMaster = ?CHILD(gingko_server, worker, []),
    {GingkoLogMaster, GingkoLogHelperMaster, GingkoCacheMaster, InterDcLogMaster} =
        case ?USE_SINGLE_SERVER of
            true ->
                {?CHILD(gingko_log_server, worker, []), ?CHILD(gingko_log_helper_server, worker, []), ?CHILD(gingko_cache_server, worker, []), ?CHILD(inter_dc_log_server, worker, [])};
            false ->
                {?VNODE(?GINGKO_LOG_VNODE_MASTER, gingko_log_vnode), ?VNODE(?GINGKO_LOG_HELPER_VNODE_MASTER, gingko_log_helper_vnode), ?VNODE(?GINGKO_CACHE_VNODE_MASTER, gingko_cache_vnode), ?VNODE(?INTER_DC_LOG_VNODE_MASTER, inter_dc_log_vnode)}
        end,
    BCounterManager = ?CHILD(bcounter_manager, worker, []),
    TimeManager = ?CHILD(gingko_time_manager, worker, []),
    CheckpointService = ?CHILD(gingko_checkpoint_service, worker, []),
    StateService = ?CHILD(inter_dc_state_service, worker, []),
    ZMQContextManager = ?CHILD(zmq_context, worker, []),
    InterDcJournalSender = ?CHILD(inter_dc_txn_sender, worker, []),
    InterDcJournalReceiver = ?CHILD(inter_dc_txn_receiver, worker, []),
    InterDcRequestSender = ?CHILD(inter_dc_request_sender, worker, []),
    InterDcRequestResponder = ?CHILD(inter_dc_request_responder, worker, []),

    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    {ok, {SupFlags, [
        TimeManager,
        GingkoMaster,
        InterDcLogMaster,
        GingkoLogMaster,
        GingkoLogHelperMaster,
        GingkoCacheMaster,
        ZMQContextManager,
        CheckpointService,
        StateService,
        InterDcJournalSender,
        InterDcJournalReceiver,
        InterDcRequestSender,
        InterDcRequestResponder,
        BCounterManager]}}.
