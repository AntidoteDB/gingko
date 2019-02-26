-module(simple_tests).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

start() ->
    os:putenv("reset_log", "true"),
    logger:set_primary_config(#{level => info}),
    {ok, GingkoSup} = gingko_sup:start_link(),
    GingkoSup.

stop(Sup) ->
    exit(Sup, normal),
    Ref = monitor(process, Sup),
    receive
        {'DOWN', Ref, process, Sup, _Reason} ->
            logger:info("Gingko shutdown successful"),
            ok
    after 1000 ->
        error(exit_timeout)
    end.


fixture_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
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

    ?_assertEqual(Data, [{<<"a">>, <<"b">>}]).


%% updated but not committed operations result in empty version
writeupdate_test(_Config) ->
    Type = antidote_crdt_register_mv,
    DownstreamOp = {1, 1, []},
    DownstreamOp2 = {1, 1, []},

    gingko:update(b, Type, 1, DownstreamOp),
    gingko:update(b, Type, 2, DownstreamOp2),

    {ok, Data} = gingko:get_version(b, Type, vectorclock:new()),
    ?_assertEqual(Data, []).
