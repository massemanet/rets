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
         single/2,
         via/2
        ]).

-record(state, {tables=[],
                start_tables = [],
                props=[named_table,ordered_set,public],
                keep_db=false,
                dir,
                idx
               }).

index_filename() -> "idx.term".

init(Env) ->
  Dir = proplists:get_value(table_dir,Env),
  KeepDB = proplists:get_value(keep_db,Env),
  Idx = filename:join(Dir,index_filename()),
  ok = filelib:ensure_dir(Idx),
  load_db(#state{dir = Dir,idx = Idx,keep_db=KeepDB}).

load_db(State = #state{dir = Dir, idx = Idx}) ->
  case file:consult(Idx) of
    {ok, Ts} ->
      Tables = lists:sort([load_table(Dir, T) || T <- Ts, is_atom(T)]),
      State#state{tables = Tables,start_tables = Tables};
    {error, _} ->
      State
  end.

load_table(Dir,T) when is_atom(T) ->
  Tab  = atom_to_list(T),
  File = tab_file_name(Dir, Tab),
  {ok, T} = ets:file2tab(File),
  Tab.

terminate(State) ->
  case State#state.keep_db of
    true  -> save_db(State);
    false -> ok
  end.

save_db(#state{tables       = Tabs,
               start_tables = StartTabs,
               dir          = Dir,
               idx          = Idx}) ->
  %% Save all ETS tables and the index
  Ts = [save_table(Dir, Tab) || Tab <- Tabs],
  ok = file:write_file(Idx, [io_lib:format("~p.~n", [T]) || T <- Ts]),

  %% Delete those tabs that were saved the last time the backend was
  %% stopped but no longer exists
  [delete_tab(Dir,Tab) || Tab <- ordsets:subtract(StartTabs,Tabs)],
  ok.

delete_tab(Dir,Tab) ->
  ok = file:delete(tab_file_name(Dir,Tab)).

save_table(Dir, Tab) when is_list(Tab) ->
  T = list_to_atom(Tab),
  File = tab_file_name(Dir, Tab),
  ok = ets:tab2file(T, File),
  T.

tab_file_name(Dir, Tab) when is_list(Tab) ->
  filename:join(Dir, Tab ++ ".tab").

%% ::(#state{},list(term(Args)) -> {jiffyable(Reply),#state{}}
create(S ,[Tab])          -> creat(S,Tab).
delete(S ,[Tab])          -> delet(S,Tab);
delete(S ,[Tab,Key])      -> {deleter(tab(Tab),key_e2i(i,Key)),S}.
sizes(S  ,[])             -> {siz(S),S}.
keys(S   ,[Tab])          -> {key_getter(tab(Tab)),S}.
insert(S ,[Tab,KVs])      -> {ins(tab(Tab),[{key_e2i(i,K),V}||{K,V}<-KVs]),S};
insert(S ,[Tab,K,V])      -> {ins(tab(Tab),[{key_e2i(i,K),V}]),S}.
bump(S   ,[Tab,Key,I])    -> {update_counter(tab(Tab),key_e2i(i,Key),I),S};
bump(S   ,[Tab,Key,L,H])  -> {update_counter(tab(Tab),key_e2i(i,Key),L,H),S}.
reset(S  ,[Tab,Key,I])    -> {reset_counter(tab(Tab),key_e2i(i,Key),I),S}.
next(S   ,[Tab,Key])      -> {nextprev(next,tab(Tab),key_e2i(i,Key)),S}.
prev(S   ,[Tab,Key])      -> {nextprev(prev,tab(Tab),key_e2i(i,Key)),S}.
multi(S  ,[Tab,Key])      -> {getter(multi,tab(Tab),key_e2i(l,Key)),S}.
single(S ,[Tab,Key])      -> {getter(single,tab(Tab),key_e2i(l,Key)),S}.
via(S    ,[Tab,Key,TabI]) -> {via(tab(TabI),tab(Tab),key_e2i(i,Key)),S}.

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
  case ets:lookup(Tab,Key) of
    []        -> null;
    [{Key,V}] -> ets:delete(Tab,Key),V
  end.

update_counter(Tab,Key,Incr) ->
  try ets:update_counter(Tab,Key,Incr)
  catch _:_ -> reset_counter(Tab,Key,Incr)
  end.

update_counter(Tab,Key,Low,High) ->
  try ets:update_counter(Tab,Key,{2,1,High,Low})
  catch _:_ -> reset_counter(Tab,Key,Low)
  end.

reset_counter(Tab,Key,Begin) ->
  ets:insert(Tab,{Key,Begin}),
  Begin.

via(Tab1,Tab2,Key2) ->
  %% Key2 in Tab2 holds the last visited key of Tab1. This operation
  %% gets the next key and value from Tab1, updating the pointer under
  %% Key2.
  %%
  %% It is important to pay attention to internal and external
  %% representation of keys:
  %% - Key2 is already in internal format
  %% - Tab2 holds Key1 in external format (otherwise a simple get from
  %%   Tab2 @ Key2 would crash)
  %% - Key1 therefore needs to be converted to internal format before
  %%   using it for a lookup in Tab1
  %% - Once the next Key1 is found, it needs to be converted to
  %%   external format before saving in Tab2
  case ets:lookup(Tab2,Key2) of
    [{Key2,EKey}] -> next_via(Tab1,key_e2i(EKey),Tab2,Key2);
    []            -> first_via(Tab1,Tab2,Key2)
  end.

next_via(Tab1,Key1,Tab2,Key2) ->
  case ets:lookup(Tab1,ets:next(Tab1,Key1)) of
    [Rec] -> found_via(Tab2,Key2,Rec);
    []    -> first_via(Tab1,Tab2,Key2)
  end.

first_via(Tab1,Tab2,Key2) ->
  case ets:lookup(Tab1,ets:first(Tab1)) of
    [Rec] -> found_via(Tab2,Key2,Rec);
    []    -> throw({409,end_of_table})
  end.

found_via(Tab2,Key2,{Key1,V}) ->
  ets:insert(Tab2,{Key2,key_i2e(Key1)}),
  {[{key_i2e(Key1),V}]}.

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

%% the reverse of key_i2e/1
key_e2i(T) ->
  list_to_tuple(binary:split(T,<<"/">>,[global])).

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
