## -*- mode: Makefile; fill-column: 80; comment-column: 67; -*-

REBAR   ?= $(shell which rebar 2> /dev/null || which ./rebar)

.PHONY: all clean test compile rpm get-deps update-deps
.PHONY: eunit xref dialyze
.PHONY: release release_minor release_major

all: compile

compile: get-deps
	@$(REBAR) compile skip_deps=true

compile-all: get-deps
	@$(REBAR) compile

get-deps:
	@$(REBAR) get-deps

update-deps:
	@$(REBAR) update-deps
	@$(REBAR) get-deps

cleanish:
	@rm -rf .eunit

clean:
	@find . -name "*~" -exec rm {} \;
	@$(REBAR) clean

test: eunit xref dialyze

#############################################################################
## release stuff

release_major: test
	./bin/release.sh major

release_minor: test
	./bin/release.sh minor

release_patch: test
	./bin/release.sh patch

release: release_patch

#############################################################################
## testing

eunit: compile-all
	@$(REBAR) eunit skip_deps=true

xref: compile-all
	@$(REBAR) compile xref skip_deps=true

~/.dialyzer_plt:
	-dialyzer --output_plt ${@} --build_plt \
           --apps erts kernel stdlib crypto ssl public_key inets \
                  eunit xmerl compiler runtime_tools mnesia syntax_tools

deps/.dialyzer_plt: ~/.dialyzer_plt
	-dialyzer --add_to_plt --plt ~/.dialyzer_plt --output_plt ${@} -r deps

dialyze: compile deps/.dialyzer_plt
	$(shell [ -d .eunit ] && rm -rf .eunit)
	dialyzer ebin -nn --plt deps/.dialyzer_plt
