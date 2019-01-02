-module(simple_tests).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

start() ->
    os:putenv("RESET_LOG_FILE", "true"),
    logger:set_primary_config(#{level => info}),

    application:ensure_all_started(gingko),
    [].

stop(_Config) ->
    application:stop(gingko).


fixture_test_() ->
    {foreach,
        fun start/0,
        [
            fun writeupdate_test/1,
            fun write_and_commit_test/1
        ]
    }.


write_and_commit_test(_Config) ->
    TransactionId = 1,
    Type = antidote_crdt_register_mv,
    DownstreamOp = {<<"a">>, <<"b">>, []},

    gingko:update(a, Type, TransactionId, DownstreamOp),
    gingko:commit([a], TransactionId, {1, 1234}, vectorclock:new()),

    {ok, Data} = gingko:get_version(a, Type, vectorclock:new()),

    ?_assertEqual(Data, [{<<"a">>,<<"b">>}]).


%% updated but not committed operations result in empty version
writeupdate_test(_Config) ->
    Type = antidote_crdt_register_mv,
    DownstreamOp = {1, 1, []},
    DownstreamOp2 = {1, 1, []},

    gingko:update(b, Type, 1, DownstreamOp),
    gingko:update(b, Type, 2, DownstreamOp2),

    {ok, Data} = gingko:get_version(b, Type, vectorclock:new()),
    ?_assertEqual(Data, []).
