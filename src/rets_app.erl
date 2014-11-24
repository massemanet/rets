-module(rets_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  start_cowboy_instance(),
  rets_sup:start_link([application:get_all_env(rets)]).

stop(_State) ->
  ok.

start_cowboy_instance() ->
  start_cowboy().

start_cowboy() ->
  Opts = rets:cowboy_opts(),
  Dispatch = cowboy_router:compile(proplists:get_value(routes,Opts)),
  cowboy:start_http(proplists:get_value(name,Opts),
                    proplists:get_value(acceptors,Opts),
                    proplists:get_value(opts,Opts),
                    [{env,[{dispatch,Dispatch}]}]).
