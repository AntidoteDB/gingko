-module(gingko_metrics).

%% @doc Exposes HTTP responses and timings as Prometheus metrics.
%%
%% Defines two metrics:
%%
%%        * the counter http_requests_total, with the label statuscode.
%%
%%        * the histogram http_response_microseconds
%%
%% The duration measures the time, in microseconds, between when
%% the request line was received and the response was sent. This
%% is as close we can get to the actual time of the request as
%%  seen by the user.

-behaviour(elli_handler).

-include_lib("elli/include/elli.hrl").

-export([handle/2, handle_event/3]).

% Expose /metrics for Prometheus to pull.
handle(Req, _Args) ->
  handle(Req#req.method, elli_request:path(Req), Req).

handle('GET', [<<"metrics">>], _Req) ->
  {ok, [], prometheus_text_format:format()};
% All other requests are passed to normal handler.
handle(_Verb, _Path, _Req) -> ignore.

% Return path, minus any query string, as a binary.
rawpath(#req{raw_path = Path}) ->
  case binary:split(Path, [<<"?">>]) of
    [P, _] -> P;
    [P] -> P
  end.


handle_event(request_complete,
    [Req, ResponseCode, _ResponseHeaders, _ResponseBody,
      Timings],
    _Config) ->
  prometheus_counter:inc(http_requests_total,
    [ResponseCode, rawpath(Req)]),
  Start = proplists:get_value(request_start, Timings),
  End = proplists:get_value(request_end, Timings),
  Delta = timer:now_diff(End, Start),
  io:format("handle_event: ~p  ~p  ~p~n",
    [ResponseCode,
      erlang:convert_time_unit(Delta, native,
        micro_seconds),
      rawpath(Req)]),
  % The "_microseconds" suffix in the metric name is magic.
  % Prometheus.erl converts the Erlang native time difference to microseconds.
  prometheus_histogram:observe(response_time_in_microseconds,
    [rawpath(Req)], Delta),
  ok;
handle_event(chunk_complete,
    [Req, ResponseCode, ResponseHeaders, _ClosingEnd, Timings],
    Config) ->
  handle_event(request_complete,
    [Req, ResponseCode, ResponseHeaders, <<>>, Timings],
    Config);
handle_event(Event, [Req, _Exc, _Stack], _Config)
  when Event =:= request_throw;
  Event =:= request_exit;
  Event =:= request_error;
  Event =:= request_parse_error;
  Event =:= bad_request;
  Event =:= client_closed;
  Event =:= client_timeout ->
  prometheus_counter:inc(http_requests_total,
    [Event, rawpath(Req)]),
  ok;
handle_event(elli_startup, [], _Config) ->
  prometheus_counter:new([{name, http_requests_total},
    {help, "Total HTTP requests"},
    {labels, [status, path]}]),
  prometheus_histogram:new([{name,
    response_time_in_microseconds},
    {labels, [path]},
    {buckets,
      [10, 100, 1000, 10000, 100000, 300000, 500000,
        750000, 1000000, 1500000, 2000000, 3000000]},
    {help,
      "Microseconds between request receipt "
      "and response send."}]),
  ok;
handle_event(_, _, _) ->
  %% Future-proof.
  ok.

