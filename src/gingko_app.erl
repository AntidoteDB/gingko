%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Dez 2019 11:14
%%%-------------------------------------------------------------------
-module(gingko_app).
-author("kevin").
-include("gingko.hrl").

-behaviour(application).

-export([start/2, stop/1]).

-spec(start(StartType :: normal | {takeover, node()} | {failover, node()},
    StartArgs :: term()) ->
  {ok, pid()} |
  {ok, pid(), State :: term()} |
  {error, Reason :: term()}).
start(_StartType, _StartArgs) ->
  mnesia:create_table(checkpoint_entry,
    [{attributes, record_info(fields, checkpoint_entry)},
      %{index, [#snapshot.key_struct]},TODO find out why index doesn't work here
      {disc_copies, [node()]}]),
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


