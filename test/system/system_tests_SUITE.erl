-module(system_tests_SUITE).

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
  dummy_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
  ct:print("[ OK ] ~p", [dev1]),
  start_node(dev1, Config),
  Config.

end_per_suite(Config) ->
  Config.

init_per_testcase(_Name, Config) ->
  Config.

end_per_testcase(Name, _) ->
  ct:print("[ OK ] ~p", [Name]),
  ok.

all() ->
  [
    dummy_test
  ].


dummy_test(_Config) ->
  ct:log("Starting node ~p", [helo]),
  ok.

start_node(Name, _Config) ->
  ct:log("Starting node ~p", [Name]),

  %% have the slave nodes monitor the runner node, so they can't outlive it
  %% also include current code path for compiled erlang source files
  CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
  NodeConfig = [
    {monitor_master, true},
    {startup_functions, [
      {code, set_path, [CodePath]}
    ]}],

  case ct_slave:start(Name, NodeConfig) of
    {ok, Node} ->

      ct:log("Starting gingko"),
      {ok, _GingkoProc} = rpc:call(Node, gingko_sup, start_link, []),
%%      ok = rpc:call(Node, application, set_env, [antidote, pubsub_port, web_ports(Name) + 1]),
%%      ok = rpc:call(Node, application, set_env, [antidote, logreader_port, web_ports(Name)]),


      timer:sleep(timer:seconds(10)),
      Node;
    {error, Reason, Node} ->
      ct:pal("Error starting node ~w, reason ~w", [Node, Reason]),
      ct_slave:stop(Name)
  end.



web_ports(dev1) -> 11015;
web_ports(dev2) -> 11025;
web_ports(dev3) -> 11035;
web_ports(dev4) -> 11045.
