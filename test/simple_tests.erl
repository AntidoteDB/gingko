-module(simple_tests).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

writeupdate_test_() ->
    application:start(gingko),
    gingko:update(a, antidote_crdt_register_mv, 1, {1, 1, []}),
    gingko:commit([a], 1, {1,1}, undefined),
    
    application:stop(gingko),
    ?_assert(true).