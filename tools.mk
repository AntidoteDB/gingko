REBAR ?= $(shell pwd)/rebar3

test:
	mkdir -p logs
	${REBAR} eunit skip_deps=true
	${REBAR} cover

dialyzer:
	${REBAR} dialyzer
