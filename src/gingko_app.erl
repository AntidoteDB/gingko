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

-export([start/2, stop/1, install/1]).

-spec(start(StartType :: normal | {takeover, node()} | {failover, node()},
    StartArgs :: term()) ->
  {ok, pid()} |
  {ok, pid(), State :: term()} |
  {error, Reason :: term()}).
start(_StartType, _StartArgs) ->
  mnesia:wait_for_tables([journal_entry, snapshot], 5000),
  case gingko_sup:start_link() of
    {ok, Pid} ->
      {ok, Pid};
    Error ->
      Error
  end.

-spec(stop(State :: term()) -> term()).
stop(_State) ->
  mnesia:stop(),
  ok.

install(Nodes) ->
  ok = mnesia:create_schema(Nodes),
  rpc:multicall(Nodes, application, start, [mnesia]),
  {atomic, ok} = mnesia:create_table(journal_entry,
    [{attributes, record_info(fields, journal_entry)},
      %{index, [#journal_entry.jsn]},TODO find out why index doesn't work here
      {ram_copies, Nodes}
    ]),
  {atomic, ok} = mnesia:create_table(snapshot,
    [{attributes, record_info(fields, snapshot)},
      %{index, [#snapshot.key_struct]},TODO find out why index doesn't work here
      {disc_copies, Nodes}]),
  rpc:multicall(Nodes, application, stop, [mnesia]).


