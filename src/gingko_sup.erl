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

-export([start_link/0, start_gingko_server/3]).
-export([init/1]).

-spec start_gingko_server(atom(), dcid(), map_list()) -> {ok, pid()} | {error, reason()}.
start_gingko_server(Id, DcId, Args) ->
    case supervisor:start_child(?MODULE, get_gingko_config(Id, [{id, Id}, {dcid, DcId} | Args])) of
        {ok, Pid1} -> {ok, Pid1};
        {ok, Pid2, _} -> {ok, Pid2};
        Error -> {error, Error}
    end.

-spec get_gingko_config(atom(), map_list()) -> supervisor:child_spec().
get_gingko_config(Id, Args) ->
    #{id => Id,
        start => {gingko, start_link, [Id, Args]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [gingko]}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    {ok, {SupFlags, []}}.
