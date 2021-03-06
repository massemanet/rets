%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 20 Jan 2014 by mats cronqvist <masse@klarna.com>

%% @doc
%% @end

-module(rets_handler).
-author('mats cronqvist').

%% the API
-export([state/0]).

%% for application supervisor
-export([start_link/1]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1,terminate/2,code_change/3,
         handle_call/3,handle_cast/2,handle_info/2]).

%% the API
state() ->
  gen_server:call(?MODULE,state).

%% for application supervisor
start_link(Args) ->
  gen_server:start_link({local,?MODULE},?MODULE,Args,[]).

%% gen_server callbacks
init(Args) ->
  process_flag(trap_exit, true),
  do_init(Args).

terminate(shutdown,State) ->
  do_terminate(State).

code_change(_OldVsn,State,_Extra) ->
  {ok,State}.

handle_call(state,_From,State) ->
  {reply,expand_recs(State),State};
handle_call(stop,_From,State) ->
  {stop,normal,stopping,State};
handle_call(What,_From,State) ->
  do_handle_call(What,State).

handle_cast(What,State) ->
  erlang:display({cast,What}),
  {noreply,State}.

handle_info(What,State) ->
  erlang:display({info,What}),
  {noreply,State}.

%% utility to print state
expand_recs(List) when is_list(List) ->
  [expand_recs(I) || I <- List];
expand_recs(Tup) when is_tuple(Tup) ->
  case tuple_size(Tup) of
    L when L < 1 -> Tup;
    L ->
      try Fields = rec_info(element(1,Tup)),
          L = length(Fields)+1,
          lists:zip(Fields,expand_recs(tl(tuple_to_list(Tup))))
      catch _:_ ->
          list_to_tuple(expand_recs(tuple_to_list(Tup)))
      end
  end;
expand_recs(Term) ->
  Term.

%% boilerplate ends here

%% declare the state
-record(state,{
          %% Settable parameters
          %% Set from erl start command (erl -rets backend leveldb)
          backend   = leveldb, %% leveldb|ets
          env       = [],      %% result of application:get_all_env(rets)
          table_dir = "/tmp/rets/db",
          keep_db   = false,

          %% Non-settable paramaters
          cb_mod,  %% rets BE callback module
          cb_state %% BE callback state
         }).

rec_info(state) -> record_info(fields,state).

do_init(Args) ->
  S  = #state{},
  BE = getv(backend,Args,S#state.backend),
  KD = getv(keep_db,Args,S#state.keep_db),
  BD = getv(table_dir,Args,S#state.table_dir),
  TD = filename:join(BD,BE),
  CB = list_to_atom("rets_"++atom_to_list(BE)),
  filelib:ensure_dir(filename:join(TD,dummy)),
  {ok,S#state{
        backend   = BE,
        table_dir = TD,
        keep_db   = KD,
        env       = Args,
        cb_mod    = CB,
        cb_state  = CB:init(TD,files(KD,TD))}}.

files(false,TableDir) ->
  rets_file:delete_recursively(TableDir),
  filelib:ensure_dir(filename:join(TableDir,dummy)),
  [];
files(true,TableDir) ->
  {ok,Files} = file:list_dir(TableDir),
  Files.

do_terminate(S) ->
  (S#state.cb_mod):terminate(S#state.keep_db,S#state.cb_state).

do_handle_call({F,Args},State) ->
  try
    {Reply,CBS} = (State#state.cb_mod):F(State#state.cb_state,Args),
    {reply,{ok,Reply},State#state{cb_state=CBS}}
  catch
    throw:{Status,Term} -> {reply,{Status,Term},State}
  end.

getv(K,PL,Def) ->
  proplists:get_value(K,PL,Def).
