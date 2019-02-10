%% @doc The metrics server
-module(gingko_metrics_server).
-include("gingko.hrl").

-behaviour(gen_server).

-export([update/0]).
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2,  handle_info/2]).

%%%===================================================================
%%% Metrics API
%%%===================================================================

update() ->
  logger:notice(#{
    action => "-==================================================",
    registered_as => ?MODULE
  }),
  gen_server:cast(?MODULE, update_counter).

%%%===================================================================
%%% State (stateless, prometheus)
%%%===================================================================

-record(state, { collecting = true}).

%% @doc Starts the op log server for given server name and recovery receiver process
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).


%% @doc Initializes the internal server state
-spec init({node(), pid()}) -> {ok, #state{}}.
init({}) ->
  logger:notice(#{
    action => "Starting metrics server",
    registered_as => ?MODULE
  }),

  MetricsEnabled = os:getenv("prometheus_metrics", "true"),
  case MetricsEnabled of
    "true" ->
      init_prometheus(),
      {ok, #state{ collecting = true}};
    _ ->
      {ok, #state{ collecting = false}}
  end.



%% ------------------
%% ASYNC METRICS
%% ------------------

handle_cast(update_counter, State) ->
  logger:warning(#{
    action => "WARNGNG",
    registered_as => ?MODULE
  }),
  prometheus_counter:inc(updates_total),
  io:format(user, prometheus_text_format:format(), []),
  {noreply, State}.


terminate(_Reason, _State) -> ok.

handle_call(_Request, _From, State) ->
  {noreply, State}.


handle_info(Msg, State) ->
  logger:warning("Swallowing unexpected message: ~p", [Msg]),
  {noreply, State}.




%%%===================================================================
%%% Private Functions Implementation
%%%===================================================================

init_prometheus() ->
  prometheus:start(),
  prometheus_counter:declare([{name, updates_total}, {help, "Total update requests"}]).
