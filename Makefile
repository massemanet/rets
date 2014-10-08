REBAR   ?= $(shell which rebar 2> /dev/null || which ./rebar)

.PHONY: all compile clean get-deps deps
.PHONY: release release_patch release_minor release_major
.PHONY: test eunit xref dialyzer

all: compile

compile: get-deps
	@$(REBAR) compile

clean:
	@find . -name "*~" -exec rm {} \;
	@$(REBAR) clean

deps:
	@$(REBAR) update-deps
	@$(REBAR) get-deps

get-deps:
	@$(REBAR) get-deps

release: release_patch

release_major: test
	./bin/release.sh major

release_minor: test
	./bin/release.sh minor

release_patch: test
	./bin/release.sh patch

test: eunit xref

eunit:
	@$(REBAR) eunit skip_deps=true

xref: all
	@$(REBAR) compile xref skip_deps=true

~/.dialyzer_plt:
	dialyzer --output_plt ~/.dialyzer_plt --build_plt \
	   --apps erts kernel stdlib crypto ssl public_key inets \
	          eunit xmerl compiler runtime_tools mnesia

dialyze: ~/.dialyzer_plt compile
	$(shell [ -d .eunit ] && rm -rf .eunit)
	dialyzer --plt ~/.dialyzer_plt -nn -r .
