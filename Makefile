REBAR = ./rebar

.PHONY: all clean deps eunit xref release

all:
	@$(REBAR) compile

clean:
	@find . -name "*~" -exec rm {} \;
	@$(REBAR) clean

eunit:
	@$(REBAR) eunit skip_deps=true

xref: all
	@$(REBAR) xref skip_deps=true

release: xref eunit
	./release.sh

deps:
	@$(REBAR) get-deps
	@$(REBAR) update-deps
