REBAR = $(shell pwd)/rebar3
COVERPATH = $(shell pwd)/_build/test/cover
.PHONY: rel test

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

distclean: clean relclean
	$(REBAR) clean --all

shell:
	$(REBAR) shell --name='gingko@127.0.0.1'

rel:
	$(REBAR) release

relclean:
	rm -rf _build/default/rel

# style checks
lint:
	${REBAR} as lint lint

check: distclean test dialyzer lint

stage :
	$(REBAR) release -d

test:
	${REBAR} eunit skip_deps=true

coverage:
	${REBAR} cover --verbose

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer:
	${REBAR} dialyzer
