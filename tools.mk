REBAR ?= $(shell pwd)/rebar3

test: compile
	mkdir -p logs
	${REBAR} eunit skip_deps=true
	${REBAR} cover

dialyzer:
	${REBAR} dialyzer

edoc:
	${REBAR} edoc
	rm doc/erlang.png
	cp doc/ext/gingko.png doc/erlang.png


system-test: compile
	rm -f test/system/*.beam
	mkdir -p logs
	ct_run -pa ./_build/default/lib/*/ebin -logdir logs -dir test/system
