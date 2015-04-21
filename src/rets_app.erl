%% -*- mode: erlang; erlang-indent-level: 2 -*-
-module(rets_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  start_cowboy_instance(),
  case rets_sup:start_link([application:get_all_env(rets)]) of
    {ok, _Pid} = OK ->
      OK;
    Error ->
      stop_cowboy_instance(),
      Error
  end.

stop(_State) ->
  stop_cowboy_instance(),
  ok.

start_cowboy_instance() ->
  {ok, _} = start_cowboy().

stop_cowboy_instance() ->
  Opts = rets:cowboy_opts(),
  cowboy:stop_listener(proplists:get_value(name,Opts)).

start_cowboy() ->
  Opts = rets:cowboy_opts(),
  Dispatch = cowboy_router:compile(proplists:get_value(routes,Opts)),
  cowboy:start_http(proplists:get_value(name,Opts),
                    proplists:get_value(acceptors,Opts),
                    proplists:get_value(opts,Opts),
                    [{env,[{dispatch,Dispatch}]}]).
