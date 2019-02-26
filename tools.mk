REBAR ?= $(shell pwd)/rebar3

test:
	mkdir -p logs
	${REBAR} eunit skip_deps=true
	${REBAR} cover

dialyzer:
	${REBAR} dialyzer

edoc:
	${REBAR} edoc
	rm doc/erlang.png
	cp doc/ext/gingko.png doc/erlang.png
