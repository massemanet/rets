%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 21 Oct 2014 by Mats Cronqvist <masse@klarna.com>

%% @doc
%% @end

-module('rets_ets').
-author('Mats Cronqvist').
-export([init/1,
         terminate/1,
         create/2,
         delete/2,
         sizes/2,
         keys/2,
         insert/2,
         bump/2,
         reset/2,
         next/2,
         prev/2,
         multi/2,
         single/2]).

-record(state, {tables=[],
                props=[named_table,ordered_set,public]}).

init(_Env)        -> #state{}.
terminate(_State) -> ok.

%% ::(#state{},list(term(Args)) -> {jiffyable(Reply),#state{}}
create(S ,[Tab])       -> creat(S,Tab).
delete(S ,[Tab])       -> delet(S,Tab);
delete(S ,[Tab,Key])   -> {deleter(tab(Tab),key_e2i(i,Key)),S}.
sizes(S  ,[])          -> {siz(S),S}.
keys(S   ,[Tab])       -> {key_getter(tab(Tab)),S}.
insert(S ,[Tab,KVs])   -> {ins(tab(Tab),[{key_e2i(i,K),V} || {K,V} <- KVs]),S};
insert(S ,[Tab,K,V])   -> {ins(tab(Tab),[{key_e2i(i,K),V}]),S}.
bump(S   ,[Tab,Key,I]) -> {update_counter(tab(Tab),key_e2i(i,Key),I),S}.
reset(S  ,[Tab,Key,I]) -> {reset_counter(tab(Tab),key_e2i(i,Key),I),S}.
next(S   ,[Tab,Key])   -> {nextprev(next,tab(Tab),key_e2i(i,Key)),S}.
prev(S   ,[Tab,Key])   -> {nextprev(prev,tab(Tab),key_e2i(i,Key)),S}.
multi(S  ,[Tab,Key])   -> {getter(multi,tab(Tab),key_e2i(l,Key)),S}.
single(S ,[Tab,Key])   -> {getter(single,tab(Tab),key_e2i(l,Key)),S}.

creat(S,Tab) ->
  case lists:member(Tab,S#state.tables) of
    true ->
      {false,S};
    false->
      ets:new(list_to_atom(Tab),S#state.props),
      {true,S#state{tables=lists:sort([Tab|S#state.tables])}}
  end.

delet(S = #state{tables=Ts},Tab) ->
  case lists:member(Tab,Ts) of
    true ->
      ets:delete(list_to_atom(Tab)),
      {true,S#state{tables=S#state.tables--[Tab]}};
    false->
      {false,S}
  end.

siz(S) ->
  case [tab(T) || T <- S#state.tables] of
    [] -> [];
    Ts -> {[{T,get_size(T)} || T <- Ts]}
  end.

key_getter(Tab) ->
  lists:sort(ets:foldr(fun({K,_},A) -> [key_i2e(K)|A] end,[],Tab)).

ins(Tab,KVs) ->
  case ets:insert_new(Tab,KVs) of
    false-> throw({409,{key_exists,{Tab,[K||{K,_}<-KVs]}}});
    true -> true
  end.

nextprev(OP,Tab,Key) ->
  case ets:lookup(Tab,ets:OP(Tab,Key)) of
    [{K,V}] -> {[{key_i2e(K),V}]};
    []      -> throw({409,end_of_table})
  end.

getter(single,Tab,Key) ->
  case getter(multi,Tab,Key) of
    {[{_,V}]} -> V;
    _         -> throw({404,multiple_hits})
  end;
getter(multi,Tab,Key) ->
  case ets:select(Tab,[{{Key,'_'},[],['$_']}]) of
    []   -> throw({404,no_such_key});
    Hits -> {lists:map(fun({K,V}) -> {key_i2e(K),V} end,Hits)}
  end.

deleter(Tab,Key) ->
  ets:delete(Tab,Key).

update_counter(Tab,Key,Incr) ->
  try ets:update_counter(Tab,Key,Incr)
  catch _:_ -> reset_counter(Tab,Key,Incr)
  end.

reset_counter(Tab,Key,Begin) ->
  ets:insert(Tab,{Key,Begin}),
  Begin.

%% key handling
%% the external form of a key is the path part of an url, basically
%% any number of slash-separated elements, each of which is
%% ALPHA / DIGIT / "-" / "." / "_" / "~"
%%  "a/ddd/b/a_b_/1/3.14/x"
%% an element can not be a single period, ".". the single period
%% is used as a wildcard in lookups.
%% the internal representation is a tuple of string binaries;
%%  {<<"a">>,<<"ddd">>,<<"b">>,<<"a_b_">>,<<"1">>,<<"3.14">>,<<"x">>}

%% transform key, external to internal. there are two styles;
%% "i", for inserts/deletes; "." is forbidden
%% "l", for lookups; "." is a wildcard

key_e2i(Style,L) -> list_to_tuple([elem(Style,E) || E <- L]).

elem(l,".") -> '_';
elem(_,E)   -> list_to_binary(E).

%% transform key, internal to external.
key_i2e(T) ->
  list_to_binary(join(tuple_to_list(T),<<"/">>)).

join([E],_) -> [E];
join([E|R],D) -> [E,D|join(R,D)].

%% table names
tab(L) ->
  try
    T = list_to_existing_atom(L),
    true = is_integer(get_size(T)),
    T
  catch
    _:_ -> throw({404,no_such_table})
  end.

get_size(Tab) ->
  ets:info(Tab,size).
