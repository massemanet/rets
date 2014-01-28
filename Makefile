REBAR = ./rebar

.PHONY: all clean deps

all:
	@$(REBAR) compile

clean:
	@find . -name "*~" -exec rm {} \;
	@$(REBAR) clean

deps:
	./rebar get-deps
	./rebar update-deps
