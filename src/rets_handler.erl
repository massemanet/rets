%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 20 Jan 2014 by mats cronqvist <masse@klarna.com>

%% @doc
%% @end

-module(rets_handler).
-author('mats cronqvist').

%% the API
-export([state/0,
         get_value/2
        ]).

%% for application supervisor
-export([start_link/1]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1,terminate/2,code_change/3,
         handle_call/3,handle_cast/2,handle_info/2]).

%% the API
state() ->
  gen_server:call(?MODULE,state).

get_value(Key, Env) ->
  proplists:get_value(Key, Env, default(Key)).

default(backend)   -> ets;
default(table_dir) -> "/tmp/rets/db";
default(keep_db)   -> false;
default(_)         -> undefined.

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
          backend,  %% leveldb|ets
          env,      %% result of application:get_all_env(rets)

          %% Non-settable paramaters
          cb_mod,  %% rets BE callback module
          cb_state %% BE callback state
         }).

rec_info(state) -> record_info(fields,state).

do_init(Args) ->
  keep_or_delete_db(Args),
  BE = get_value(backend, Args),
  CB = list_to_atom("rets_"++atom_to_list(BE)),
  {ok,#state{backend  = BE,
             env      = Args,
             cb_mod   = CB,
             cb_state = CB:init(Args)}}.

do_terminate(State = #state{env = Env}) ->
  KeepDB = get_value(keep_db, Env),
  (State#state.cb_mod):terminate(State#state.cb_state, KeepDB),
  keep_or_delete_db(KeepDB, get_value(table_dir, Env)).

do_handle_call({F,Args},State) ->
  try
    {Reply,CBS} = (State#state.cb_mod):F(State#state.cb_state,Args),
    {reply,{ok,Reply},State#state{cb_state=CBS}}
  catch
    throw:{Status,Term} -> {reply,{Status,Term},State}
  end.

keep_or_delete_db(Env) ->
  keep_or_delete_db(get_value(keep_db,   Env),
                    get_value(table_dir, Env)).

keep_or_delete_db(true, _TableDir) ->
  ok;
keep_or_delete_db(false, TableDir) ->
  IsFile = filelib:is_file(TableDir),
  if IsFile -> delete_recursively(TableDir);
     true   -> ok
  end.

delete_recursively(File) ->
  case filelib:is_dir(File) of
    true ->
      {ok,Fs} = file:list_dir(File),
      Del = fun(F) -> delete_recursively(filename:join(File,F)) end,
      lists:foreach(Del,Fs),
      delete_file(del_dir,File);
    false->
      delete_file(delete,File)
  end.

delete_file(Op,File) ->
  case file:Op(File) of
    ok -> ok;
    {error,Err} -> throw({500,{file_delete_error,{Err,File}}})
  end.
