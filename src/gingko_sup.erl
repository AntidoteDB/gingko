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

-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).
-define(VNODE(I, M), {I, {riak_core_vnode_master, start_link, [M]}, permanent, 5000, worker, [riak_core_vnode_master]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->

    {GingkoMaster, GingkoLogMaster, GingkoCacheMaster} =
        case ?USE_SINGLE_SERVER of
            true ->
                {?CHILD(gingko_server, worker, []), ?CHILD(gingko_log_server, worker, []), ?CHILD(gingko_cache_server, worker, [])};
            false ->
                {?VNODE(gingko_vnode_master, gingko_vnode), ?VNODE(gingko_log_vnode_master, gingko_log_vnode), ?VNODE(gingko_cache_vnode_master, gingko_cache_vnode)}
        end,
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    {ok, {SupFlags, [GingkoMaster, GingkoLogMaster, GingkoCacheMaster]}}.
