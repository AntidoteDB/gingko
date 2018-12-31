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
            fun writeupdate_test/1
        ]
    }.


writeupdate_test(_Config) ->
    gingko:update(a, antidote_crdt_register_mv, 1, {1, 1, []}),

    Data = gingko:get_version(a, antidote_crdt_register_mv, vectorclock:new()),
    logger:warning(#{
        action => "Get version",
        data => Data
    }),

%%    gingko:commit([a], 1, {1,1}, undefined),

    ?_assert(true).