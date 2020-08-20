%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(append_multi_dc_SUITE).
-include("gingko.hrl").

%% common_test callbacks
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0
]).

%% tests
-export([
    append_failure_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
    test_utils:init_multi_dc(?MODULE, Config).


end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
%%    append_failure_test
].


append_failure_test(Config) ->
    case gingko_env_utils:get_use_single_server() of
        true -> pass;
        false ->
            Bucket = ?BUCKET,
            Nodes = proplists:get_value(nodes, Config),
            KeyStruct = #key_struct{key = append_failure, type = antidote_crdt_counter_pn},
            KeyNode = gingko_dc_utils:get_key_partition_node_tuple(KeyStruct),
            %% Identify preference list for a given key.
            KeyNodeList = [KeyNode],
            %% Perform successful write and read.
            antidote_test_utils:increment_pn_counter(KeyNode, KeyStruct, Bucket),
            {Val1, _} = antidote_test_utils:read_pn_counter(KeyNode, KeyStruct, Bucket),
            ?assertEqual(1, Val1),

            %% Partition the network.
            ct:log("About to partition: ~p from: ~p", [KeyNodeList, Nodes -- KeyNodeList]),
            test_utils:partition_cluster(KeyNodeList, Nodes -- KeyNodeList),

            %% Heal the partition.
            test_utils:heal_cluster(KeyNodeList, Nodes -- KeyNodeList),

            %% Read after the partition has been healed.
            {Val2, _} = antidote_test_utils:read_pn_counter(KeyNode, KeyStruct, Bucket),
            ?assertEqual(1, Val2)
    end.
