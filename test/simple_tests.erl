-module(simple_tests).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

start() ->
    logger:set_primary_config(#{level => info}),
    {ok, ID} = application:ensure_all_started(gingko),
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
    logger:info(#{
        id => "1", in => "writeupdate", what => "start",
        result => "start", reason => "unauthorized",
        "user" => #{"id" => 42, "name" => "best_test_user", "role" => "test"}
    }),

%%    gingko:update(a, antidote_crdt_register_mv, 1, {1, 1, []}),
%%    gingko:commit([a], 1, {1,1}, undefined),
%%
%%    application:stop(gingko),
    ?_assert(true).