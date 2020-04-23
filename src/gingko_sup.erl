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

-module(gingko_sup).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-behaviour(supervisor).

-include("gingko.hrl").

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    GingkoMaster = {gingko_vnode_master,
        {riak_core_vnode_master, start_link, [gingko_vnode]},
        permanent, 5000, worker, [riak_core_vnode_master]},

    GingkoSingleServer = {gingko_server, {gingko_server, start_link, []}, permanent, 5000, worker, [gingko_server]},

    GingkoLogMaster = {gingko_log_vnode_master,
        {riak_core_vnode_master, start_link,
            [gingko_log_vnode]},
        permanent, 5000, worker, [riak_core_vnode_master]},

    GingkoCacheMaster = {gingko_cache_vnode_master,
        {riak_core_vnode_master, start_link, [gingko_cache_vnode]},
        permanent, 5000, worker, [riak_core_vnode_master]},

    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    {ok, {SupFlags, [GingkoMaster, GingkoSingleServer, GingkoLogMaster, GingkoCacheMaster]}}.
