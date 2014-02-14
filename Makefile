REBAR = ./rebar

.PHONY: all clean deps eunit xref release release_minor release_major

all:
	@$(REBAR) compile

clean:
	@find . -name "*~" -exec rm {} \;
	@$(REBAR) clean

eunit:
	@$(REBAR) eunit skip_deps=true

xref: all
	@$(REBAR) xref skip_deps=true

release_major: xref eunit
	./release.sh major

release_minor: xref eunit
	./release.sh minor

release: xref eunit
	./release.sh patch

deps:
	@$(REBAR) get-deps
	@$(REBAR) update-deps
