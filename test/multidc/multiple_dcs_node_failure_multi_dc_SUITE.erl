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

-module(multiple_dcs_node_failure_multi_dc_SUITE).
-include("gingko.hrl").

%% If logging is disabled these tests will fail and some reads will
%% block as DCs will be waiting for missing messages, so add a
%% timeout to these calls so the test suite can finish
-define(RPC_TIMEOUT, 10000).

%% common_test callbacks
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

-export([
    multiple_cluster_failure_test/1,
    cluster_failure_test/1,
    update_during_cluster_failure_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
    test_utils:init_multi_dc(?MODULE, Config).

end_per_suite(Config) ->
    Config.

init_per_testcase(Name, Config) ->
    ct:pal("[ STARTING ] ~p", [Name]),
    Config.

end_per_testcase(Name, _) ->
    ct:pal("[ OK ] ~p", [Name]),
    ok.

all() ->
    [
        cluster_failure_test,
        multiple_cluster_failure_test,
        update_during_cluster_failure_test
    ].

%% In this test there are 3 DCs each with 1 node
%% The test starts by performing some updates, ensuring they are propagated
%% The it kills the node of the first DC
%% Once restarted it checks that updates are still performed safely
%% and propagated to other DCs
cluster_failure_test(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] = [hd(Cluster) || Cluster <- Clusters],
    Key = cluster_failure_test,
    Type = antidote_crdt_counter_pn,

    update_counters(Node1, [Key], [1], ignore, static, Bucket),
    update_counters(Node1, [Key], [1], ignore, static, Bucket),
    {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static, Bucket),

    check_read_key(Node1, Key, Type, 3, ignore, static, Bucket),

    %% Kill and restart a node and be sure everything works
    ct:pal("Killing and restarting node ~w", [Node1]),
    [Node1] = test_utils:kill_and_restart_nodes([Node1], Config),

    ct:pal("Done append in Node1"),
    check_read_key(Node3, Key, Type, 3, CommitTime, static, Bucket),
    ct:pal("Done read in Node3"),
    check_read_key(Node2, Key, Type, 3, CommitTime, static, Bucket),

    ct:pal("Done first round of read, I am gonna append"),
    {ok, CommitTime2} = update_counters(Node2, [Key], [1], CommitTime, static, Bucket),
    ct:pal("Done append in Node2"),
    {ok, CommitTime3} = update_counters(Node3, [Key], [1], CommitTime2, static, Bucket),
    ct:pal("Done append in Node3"),
    ct:pal("Done waiting, I am gonna read"),

    SnapshotTime = CommitTime3,
    check_read_key(Node1, Key, Type, 5, SnapshotTime, static, Bucket),
    ct:pal("Done read in Node1"),
    check_read_key(Node2, Key, Type, 5, SnapshotTime, static, Bucket),
    ct:pal("Done read in Node2"),
    check_read_key(Node3, Key, Type, 5, SnapshotTime, static, Bucket),
    pass.


%% In this test there are 2 DCs, the first has 2 nodes the second has 1
%% The test starts by performing some updates, ensuring they are propagated
%% The it kills one of the nodes in the 1st DC and restarts it
%% Once restarted it checks that updates are still performed safely
%% and propagated to other DCs
multiple_cluster_failure_test(Config) ->
    Bucket = ?BUCKET,
    [Cluster1, Cluster2 | _Rest] = proplists:get_value(clusters, Config),
    case gingko_env_utils:get_use_single_server() of
        true -> pass;
        false ->
            [Node1, Node3 | _] = Cluster1,
            Node2 = hd(Cluster2),
            Key = multiple_cluster_failure_test,
            Type = antidote_crdt_counter_pn,

            update_counters(Node1, [Key], [1], ignore, static, Bucket),
            update_counters(Node1, [Key], [1], ignore, static, Bucket),
            {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static, Bucket),
            check_read_key(Node1, Key, Type, 3, CommitTime, static, Bucket),

            %% Kill and restart a node and be sure everything works
            ct:pal("Killing and restarting node ~w", [Node1]),
            [Node1] = test_utils:kill_and_restart_nodes([Node1], Config),

            ct:pal("Done append in Node1"),
            check_read_key(Node2, Key, Type, 3, CommitTime, static, Bucket),
            check_read_key(Node3, Key, Type, 3, CommitTime, static, Bucket),

            ct:pal("Done first round of read, I am gonna append"),
            {ok, CommitTime2} = update_counters(Node2, [Key], [1], ignore, static, Bucket),
            {ok, CommitTime3} = update_counters(Node3, [Key], [1], CommitTime2, static, Bucket),
            ct:pal("Done waiting, I am gonna read"),

            SnapshotTime = CommitTime3,
            check_read_key(Node1, Key, Type, 5, SnapshotTime, static, Bucket),
            check_read_key(Node2, Key, Type, 5, SnapshotTime, static, Bucket),
            check_read_key(Node3, Key, Type, 5, SnapshotTime, static, Bucket),
            pass
    end.

%% In this test there are 3 DCs each with 1 node
%% The test starts by performing some updates, ensuring they are propagated
%% The it kills the node of the first DC
%% It then performs an update and read in the other DCs
%% It then starts the killed node back up
%% Once restarted it checks that updates are still performed safely
%% and propagated to other DCs
update_during_cluster_failure_test(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] = [hd(Cluster) || Cluster <- Clusters],
    Key = update_during_cluster_failure_test,
    Type = antidote_crdt_counter_pn,

    update_counters(Node1, [Key], [1], ignore, static, Bucket),
    update_counters(Node1, [Key], [1], ignore, static, Bucket),
    {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static, Bucket),
    check_read_key(Node1, Key, Type, 3, CommitTime, static, Bucket),
    ct:pal("Done append in Node1"),

    %% Kill a node
    ct:pal("Killing node ~w", [Node1]),
    [Node1] = test_utils:brutal_kill_nodes([Node1]),

    %% Be sure the other DC works while the node is down
    {ok, CommitTime3a} = update_counters(Node2, [Key], [1], ignore, static, Bucket),

    %% Start the node back up and be sure everything works
    ct:pal("Restarting node ~w", [Node1]),
    [Node1] = test_utils:restart_nodes([Node1], Config),

    %% Take the max of the commit times to be sure
    %% to read all updates
    Time = vectorclock:max([CommitTime, CommitTime3a]),

    check_read_key(Node1, Key, Type, 4, Time, static, Bucket),
    ct:pal("Done Read in Node1"),

    check_read_key(Node3, Key, Type, 4, Time, static, Bucket),
    ct:pal("Done Read in Node3"),
    check_read_key(Node2, Key, Type, 4, Time, static, Bucket),
    ct:pal("Done first round of read, I am gonna append"),

    {ok, CommitTime2} = update_counters(Node2, [Key], [1], Time, static, Bucket),
    {ok, CommitTime3} = update_counters(Node3, [Key], [1], CommitTime2, static, Bucket),

    SnapshotTime = CommitTime3,
    check_read_key(Node1, Key, Type, 6, SnapshotTime, static, Bucket),
    check_read_key(Node2, Key, Type, 6, SnapshotTime, static, Bucket),
    check_read_key(Node3, Key, Type, 6, SnapshotTime, static, Bucket),
    pass.

check_read_key(Node, Key, Type, Expected, Clock, TxId, Bucket) ->
    check_read(Node, [{Key, Type, Bucket}], [Expected], Clock, TxId).

check_read(Node, Objects, Expected, Clock, TxId) ->
    case TxId of
        static ->
            {ok, Res, CT} = rpc:call(Node, antidote, read_objects, [Clock, [], Objects], ?RPC_TIMEOUT),
            ?assertEqual(Expected, Res),
            {ok, Res, CT};
        _ ->
            {ok, Res} = rpc:call(Node, antidote, read_objects, [Objects, TxId], ?RPC_TIMEOUT),
            ?assertEqual(Expected, Res),
            {ok, Res}
    end.

update_counters(Node, Keys, IncValues, Clock, TxId, Bucket) ->
    Updates =
        lists:map(
            fun({Key, Inc}) ->
                {{Key, antidote_crdt_counter_pn, Bucket}, increment, Inc}
            end, lists:zip(Keys, IncValues)),

    case TxId of
        static ->
            {ok, CT} = rpc:call(Node, antidote, update_objects, [Clock, [], Updates], ?RPC_TIMEOUT),
            {ok, CT};
        _ ->
            ok = rpc:call(Node, antidote, update_objects, [Updates, TxId], ?RPC_TIMEOUT),
            ok
    end.
