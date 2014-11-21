-module(rets_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Args) ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(Args) ->
  {ok, {{one_for_one, 5, 10},
        [child(rets_handler,Args)]}}.

child(Mod,Args) ->
  {Mod, {Mod, start_link, Args}, permanent, 5000, worker, [Mod]}.
