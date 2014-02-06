REBAR = ./rebar

.PHONY: all clean deps

all:
	@$(REBAR) compile

clean:
	@find . -name "*~" -exec rm {} \;
	@$(REBAR) clean

eunit:
	@$(REBAR) eunit skip_deps=true

deps:
	./rebar get-deps
	./rebar update-deps
