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

-define(WORKER(ModuleName),
    #{
        id => ModuleName,
        start => {ModuleName, start_link, []}
    }).
-define(SUPERVISOR(ModuleName),
    #{
        id => ModuleName,
        start => {ModuleName, start_link, []},
        type => supervisor
    }).
-define(VNODE(MasterName, ModuleName),
    #{
        id => MasterName,
        start => {riak_core_vnode_master, start_link, [ModuleName]},
        modules => [riak_core_vnode_master]
    }).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    TimeManager = ?WORKER(gingko_time_manager),
    {GingkoLogMaster, GingkoLogHelperMaster, GingkoCacheMaster, InterDcLogMaster} =
        case gingko_env_utils:get_use_single_server() of
            true ->
                {
                    ?WORKER(gingko_log_server),
                    ?WORKER(gingko_log_helper_server),
                    ?WORKER(gingko_cache_server),
                    ?WORKER(inter_dc_log_server)
                };
            false ->
                {
                    ?VNODE(?GINGKO_LOG_VNODE_MASTER, gingko_log_vnode),
                    ?VNODE(?GINGKO_LOG_HELPER_VNODE_MASTER, gingko_log_helper_vnode),
                    ?VNODE(?GINGKO_CACHE_VNODE_MASTER, gingko_cache_vnode),
                    ?VNODE(?INTER_DC_LOG_VNODE_MASTER, inter_dc_log_vnode)
                }
        end,
    ZMQContextManager = ?WORKER(zmq_context),
    CheckpointService = ?WORKER(gingko_checkpoint_service),
    StateService = ?WORKER(inter_dc_state_service),
    InterDcTxnSender = ?WORKER(inter_dc_txn_sender),
    InterDcJournalReceiver = ?WORKER(inter_dc_txn_receiver),
    InterDcRequestSender = ?WORKER(inter_dc_request_sender),
    InterDcRequestResponder = ?WORKER(inter_dc_request_responder),
    BCounterManager = ?WORKER(bcounter_manager),
    TxServerSup = ?SUPERVISOR(gingko_tx_server_sup),

    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    {ok, {SupFlags, [
        TimeManager,
        InterDcLogMaster,
        GingkoLogMaster,
        GingkoLogHelperMaster,
        GingkoCacheMaster,
        ZMQContextManager,
        CheckpointService,
        StateService,
        InterDcTxnSender,
        InterDcJournalReceiver,
        InterDcRequestSender,
        InterDcRequestResponder,
        BCounterManager,
        TxServerSup]}}.
