%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 27 Jun 2012 by mats cronqvist <masse@klarna.com>

%% @doc
%% a rest wrapper around ets
%% @end

-module(rets).
-author('mats cronqvist').

-export([start/0]).       % start the application

-behavior(cowboy_http_handler).
-export([handle/2,
         init/3,
         terminate/3]).

-export([cowboy_opts/0]).

%% main application starter
start() ->
  application:ensure_all_started(rets).

%% called from rets_app
cowboy_opts() ->
  [{name,rets_listener},
   {acceptors,100},
   {opts,[{port, 7890}]},
   {routes,[{'_', [{"/[...]", rets, []}]}]}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% cowboy callbacks
init({tcp, http}, Req, []) ->
  {ok, Req, init}.

terminate(_Reason, _Req, _State) ->
  ok.

%% called from cowboy. Req is the cowboy opaque state object.
handle(Req,State) ->
  {ok,Req2} =
    try
      cow_reply(200,<<"application/json">>,reply(Req),Req)
    catch
      {Status,R} -> cow_reply(Status,<<"text/plain">>,reply(Status,R),Req);
      C:R        -> cow_reply(500,<<"text/plain">>,reply(500,{C,R}),Req)
    end,
  {ok,Req2,State}.

reply(500,R) -> flat({R,erlang:get_stacktrace()});
reply(409,R) -> flat(R);
reply(404,R) -> flat(R).

cow_reply(Status,ContentType,Body,Req) ->
  Headers = [{<<"content-type">>, ContentType}],
  cowboy_req:reply(Status,Headers,Body,Req).

reply(Req) ->
  case {method(Req),uri(Req),true_headers(Req)} of
    {"PUT",   [Tab]    ,[]}          -> je(gcall({create,Tab}));
    {"PUT",   [Tab|Key],[]}          -> je(ets({insert,Tab,Key,body(Req)}));
    {"PUT",   [Tab|Key],["counter"]} -> je(ets({counter,Tab,Key}));
    {"PUT",   [Tab|Key],["reset"]}   -> je(ets({reset,Tab,Key}));
    {"GET",   []       ,[]}          -> je(ets({sizes,gcall({all,[]})}));
    {"GET",   [Tab]    ,[]}          -> je(ets({keys,Tab}));
    {"GET",   [Tab|Key],[]}          -> je(ets({single,Tab,Key}));
    {"GET",   [Tab|Key],["multi"]}   -> je(ets({multi,Tab,Key}));
    {"GET",   [Tab|Key],["next"]}    -> je(ets({next,Tab,Key}));
    {"GET",   [Tab|Key],["prev"]}    -> je(ets({prev,Tab,Key}));
    {"POST",  [Tab]    ,[]}          -> je(ets({insert,Tab,body(Req)}));
    {"DELETE",[Tab]    ,[]}          -> je(gcall({delete,Tab}));
    {"DELETE",[Tab|Key],[]}          -> je(ets({delete,Tab,Key}));
    X                                -> throw({404,X})
  end.

body(Req) ->
  {ok,Body,_} =
    case cowboy_req:has_body(Req) of
      true -> cowboy_req:body(Req);
      false-> {ok,[],[]}
    end,
  jd(Body).

method(Req) ->
  {Method,_} = cowboy_req:method(Req),
  binary_to_list(Method).

uri(Req) ->
  {URI,_} = cowboy_req:path_info(Req),
  [binary_to_list(B) || B <- URI].

headers(Req) ->
  {Headers,_}= cowboy_req:headers(Req),
  Headers.

true_headers(Req) ->
  [binary_to_list(K) || {K,<<"true">>} <- headers(Req)].

ets({sizes,Tabs})        -> size_getter([tab(T) || T <- Tabs]);
ets({keys,Tab})          -> key_getter(tab(Tab));
ets({insert,Tab,K,V})    -> inserter(tab(Tab),{K,V});
ets({insert,Tab,KVs})    -> multi_inserter(tab(Tab),KVs);
ets({counter,Tab,Key})   -> update_counter(tab(Tab),key_e2i(i,Key),1);
ets({reset,Tab,Key})     -> reset_counter(tab(Tab),key_e2i(i,Key),0);
ets({next,Tab,Key})      -> next(tab(Tab),key_e2i(i,Key));
ets({prev,Tab,Key})      -> prev(tab(Tab),key_e2i(i,Key));
ets({multi,Tab,Key})     -> getter(multi,tab(Tab),key_e2i(l,Key));
ets({single,Tab,Key})    -> getter(single,tab(Tab),key_e2i(l,Key));
ets({delete,Tab,Key})    -> ets:delete(tab(Tab),key_e2i(i,Key)).

multi_inserter(Tab,{KVs}) ->
  Ops = [{mk_key(K),V} || {K,V} <- KVs],
  ets:insert_new(Tab,Ops).

mk_key(K) ->
  key_e2i(i,string:tokens(binary_to_list(K),"/")).

inserter(Tab,{Ks,V}) ->
  K = key_e2i(i,Ks),
  case ets:insert_new(Tab,{K,V}) of
    false-> throw({409,{exists,Tab,K}});
    true -> true
  end.

size_getter([])   -> [];
size_getter(Tabs) -> {[{T,ets:info(T,size)} || T <- Tabs]}.

key_getter(Tab) ->
  ets:foldr(fun({K,_},A) -> [key_i2e(K)|A] end,[],Tab).

next(Tab,Key) -> nextprev(next,Tab,Key).
prev(Tab,Key) -> nextprev(prev,Tab,Key).

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

update_counter(Tab,Key,Incr) ->
  try ets:update_counter(Tab,Key,Incr)
  catch _:_ -> reset_counter(Tab,Key,Incr)
  end.

reset_counter(Tab,Key,Beg) ->
  ets:insert(Tab,{Key,Beg}),
  Beg.

%% key handling
%% the external form of a key is the path part of an url, basically
%% any number of slash-separated elements, each of which is
%% string() | number(), like;
%%  "a/ddd/b/a_b_/1/3.14/x"
%% an element can not be a single underscore, "_". the single underscore
%% is used as a wildcard in lookups.
%% the internal representation is a tuple of string binaries;
%%  {<<"a">>,<<"ddd">>,<<"b">>,<<"a_b_">>,<<"1">>,<<"3.14">>,<<"x">>}

%% transform key, external to internal. there are two styles;
%% "i", for inserts/deletes; "_" is forbidden
%% "l", for lookups; "_" is a wildcard
key_e2i(Style,L) -> list_to_tuple([elem(Style,E) || E <- L]).

elem(i,"_") -> throw({404,key_has_underscore});
elem(l,"_") -> '_';
elem(_,E)   -> l2b(E).

%% transform key, internal to external.
key_i2e(T) ->
  l2b(join(tuple_to_list(T),<<"/">>)).

join([E],_) -> [E];
join([E|R],D) -> [E,D|join(R,D)].

%% table names
tab(L) ->
  try
    T = list_to_existing_atom(L),
    true = is_integer(ets:info(T,size)),
    T
  catch
    _:_ -> throw({404,no_such_table})
  end.

gcall(What) ->
  gen_server:call(rets_tables,What).

flat(Term) ->
  lists:flatten(io_lib:fwrite("~p",[Term])).

l2b(L) ->
  list_to_binary(L).

%% a nif that throws? insanity.
jd(Term) ->
  try jiffy:decode(Term)
  catch {error,R} -> error({R,Term})
  end.

%% a nif that throws? insanity.
je(Term) ->
  try jiffy:encode(Term)
  catch {error,R} -> error({R,Term})
  end.

%%%%%%%%%%
%% eunit
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

t00_test() ->
  restart_rets(),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:post(localhost,tibbe,
                                [{'aaa/1/x',"AAA"},
                                 {bbb,bBbB},
                                 {ccc,123.1},
                                 {ddd,[{a,"A"},{b,b},{c,123.3}]}])),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,"aaa/_/x")),
  ?assertEqual({200,["bbb","ccc","ddd","aaa/1/x"]},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'_/1/_')),
  ?assertEqual({200,"bBbB"},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,123.1},
               rets_client:get(localhost,tibbe,ccc)),
  ?assertEqual({200,[{"a","A"},{"b","b"},{"c",123.3}]},
               rets_client:get(localhost,tibbe,"ddd")).

t01_test() ->
  restart_rets(),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,'aaa/1/x',"AAA")),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,bbb,bBbB)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,ccc,123.1)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,ddd,[{a,"A"},{b,b},{c,123.3}])),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'aaa/_/x')),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'_/1/_')),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'aaa/1/x')),
  ?assertEqual({200,"bBbB"},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,123.1},
               rets_client:get(localhost,tibbe,ccc)),
  ?assertEqual({200,[{"a","A"},{"b","b"},{"c",123.3}]},
               rets_client:get(localhost,tibbe,ddd)).

t02_test() ->
  restart_rets(),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,bbb,bBbB)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,ddd,[{a,"A"},{b,b},{c,123.3}])),
  ?assertEqual({200,"bBbB"},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,[{"a","A"},{"b","b"},{"c",123.3}]},
               rets_client:get(localhost,tibbe,ddd)).

t03_test() ->
  restart_rets(),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,1},
               rets_client:put(localhost,tibbe,bbb,counter)),
  ?assertEqual({200,2},
               rets_client:put(localhost,tibbe,bbb,counter)),
  ?assertEqual({200,2},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,0},
               rets_client:put(localhost,tibbe,bbb,reset)),
  ?assertEqual({200,1},
               rets_client:put(localhost,tibbe,bbb,counter)).

t04_test() ->
  restart_rets(),
  ?assertEqual({200,[]},
               rets_client:get(localhost)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,[{tibbe,0}]},
               rets_client:get(localhost)).

t05_test() ->
  restart_rets(),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,17,foo)),
  ?assertEqual({200,[{tibbe,1}]},
               rets_client:get(localhost)),
  ?assertMatch({409,_},
               rets_client:put(localhost,tibbe,17,a)).

t06_test() ->
  restart_rets(),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,17,foo)),
  ?assertEqual({200,true},
               rets_client:delete(localhost,tibbe,17)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({200,true},
               rets_client:delete(localhost,tibbe)),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe)).

t07_test() ->
  restart_rets(),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,[{tibbe,0}]},
               rets_client:get(localhost)),
  ?assertEqual({200,[]},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,'a/2/x',segundo)),
  ?assertEqual({200,"segundo"},
               rets_client:get(localhost,tibbe,'a/_/_')),
  ?assertEqual({404,"key_has_underscore"},
               rets_client:put(localhost,tibbe,'a/_/_',s)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,'a/1/x',primo)),
  ?assertEqual({404,"no_such_key"},
                rets_client:get(localhost,tibbe,'a')),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,'a',1)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tibbe,'a')),
  ?assertEqual({404,"multiple_hits"},
               rets_client:get(localhost,tibbe,'a/_/_')),
  ?assertEqual({200,[{"a/1/x","primo"},{"a/2/x","segundo"}]},
               rets_client:get(localhost,tibbe,'a/_/_',multi)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tibbe,'b/_/_',multi)).

t08_test() ->
  T = [{"a","a"},{"b",[{"bb","bb"}]}], %% nested proplist
  restart_rets(),
  ?assertEqual({200,true},
               rets_client:put(localhost,tybbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tybbe,"abc",T)),
  ?assertMatch({200,T},
               rets_client:get(localhost,tybbe,"abc")).

t09_test() ->
  restart_rets(),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,a,1)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tebbe,a)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,a,next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,a,prev)),
  ?assertEqual({200,[{"a",1}]},
               rets_client:get(localhost,tebbe,0,next)).

restart_rets() ->
  application:stop(inets),
  application:stop(rets),
  application:stop(cowboy),
  application:stop(ranch),
  start_and_wait().

start_and_wait() ->
  receive after 200 -> ok end,
  case start() of
    {ok,_} ->
      wait_for_start();
    _ ->
      erlang:display(waiting_for_shutdown),
      receive after 200 -> ok end,
      start_and_wait()
  end.

wait_for_start() ->
  case supervisor:which_children(ranch_sup) of
    [{{_,rets_listener},_,_,[_]}|_] -> ok;
    R ->
      erlang:display({waiting_for_startup,R}),
      receive after 200 -> ok end,
      wait_for_start()
  end.
-endif. % TEST
