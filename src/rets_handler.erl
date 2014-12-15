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
  do_terminate(State);
terminate(Crash,State) ->
  print_crash(Crash),
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

%% utility to print a crash
print_crash({Reason,Stack}) ->
  io:fwrite("~n"),
  io:fwrite("CRASH in ~w: ~w~n",[?MODULE,Reason]),
  io:fwrite("~n"),
  lists:foreach(fun print_stack/1,Stack),
  io:fwrite("~n").

print_stack({M,F,A,I}) ->
  case is_integer(A) of
    true  -> io:fwrite("~w:~w/~w",[M,F,A]);
    false ->
      As = string:join([io_lib:fwrite("~p",[E]) || E <- A],","),
      io:fwrite("~w:~w(~s)",[M,F,As])
  end,
  File = proplists:get_value(file,I,""),
  case proplists:get_value(line,I,"") of
    ""    -> io:fwrite("~n");
    Line  -> io:fwrite("  (~s:~w)~n",[File,Line])
  end.

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
          backend, %% leveldb|ets
          env,     %% result of application:get_all_env(rets)

          %% Non-settable paramaters
          cb_mod,  %% rets BE callback module
          cb_state %% BE callback state
         }).

rec_info(state) -> record_info(fields,state).

do_init(Args) ->
  BE = proplists:get_value(backend,Args,leveldb),
  CB = list_to_atom("rets_"++atom_to_list(BE)),
  {ok,#state{backend  = BE,
             env      = Args,
             cb_mod   = CB,
             cb_state = CB:init(Args)}}.

do_terminate(State) ->
  (State#state.cb_mod):terminate(State#state.cb_state).

do_handle_call({F,Ops},State) ->
  try
    {Reply,CBS} = (State#state.cb_mod):F(State#state.cb_state,Ops),
    {reply,{ok,Reply},State#state{cb_state=CBS}}
  catch
    throw:{Status,Term} -> {reply,{Status,Term},State}
  end.
