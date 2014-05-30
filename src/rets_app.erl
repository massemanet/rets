-module(rets_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  start_cowboy_instance(),
  rets_sup:start_link().

stop(_State) ->
  ok.

start_cowboy_instance() ->
  Opts = rets:cowboy_opts(),
  Dispatch = cowboy_router:compile(proplists:get_value(routes,Opts)),
  {ok,_} = cowboy:start_http(proplists:get_value(name,Opts),
                             proplists:get_value(acceptors,Opts),
                             proplists:get_value(opts,Opts),
                             [{env,[{dispatch,Dispatch}]}]).
