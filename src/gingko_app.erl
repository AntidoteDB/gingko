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

-module(gingko_app).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-behaviour(application).

-export([start/2, stop/1]).

-spec(start(StartType :: normal | {takeover, node()} | {failover, node()},
    StartArgs :: term()) ->
    {ok, pid()} |
    {ok, pid(), State :: term()} |
    {error, Reason :: term()}).
start(_StartType, _StartArgs) ->
    {atomic, ok} = mnesia:create_table(checkpoint_entry,
        [{attributes, record_info(fields, checkpoint_entry)},
            %{index, [#checkpoint_entry.index]},%TODO find out why index doesn't work here
            {ram_copies, [node()]}]),
    ok = mnesia:wait_for_tables([checkpoint_entry], 5000),
    gingko_master:start_link().

-spec(stop(State :: term()) -> term()).
stop(_State) ->
    mnesia:stop(),
    ok.


%%TODO keep this for later when inner dc replication becomes relevant (replicate between different nodes)
%% install(Nodes) ->
%%  case mnesia:create_schema(Nodes) of
%%    ok ->
%%      true = false,
%%      rpc:multicall(Nodes, application, start, [mnesia]),
%%      {atomic, ok} = mnesia:create_table(journal_entry,
%%        [{attributes, record_info(fields, journal_entry)},
%%          %{index, [#journal_entry.jsn]},%TODO find out why index doesn't work here
%%          {ram_copies, Nodes}
%%        ]),
%%      {atomic, ok} = mnesia:create_table(checkpoint_entry,
%%        [{attributes, record_info(fields, checkpoint_entry)},
%%          %{index, [#snapshot.key_struct]},TODO find out why index doesn't work here
%%          {disc_copies, Nodes}]),
%%      rpc:multicall(Nodes, application, stop, [mnesia]);
%%    Error ->
%%      ok
%%  end.


