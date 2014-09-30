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
  cowboy_req:reply(Status,
                   [{<<"content-type">>, ContentType}],
                   Body,
                   Req).

reply(Req) ->
  case {method(Req),uri(Req),true_headers(Req)} of
    {"PUT",   [Tab]    ,[]}          -> je(gcall({create,Tab}));
    {"PUT",   [Tab|Key],[]}          -> je(ets({insert,Tab,Key,body(Req)}));
    {"PUT",   [Tab|Key],["counter"]} -> ets({counter,Tab,Key});
    {"PUT",   [Tab|Key],["reset"]}   -> ets({reset,Tab,Key});
    {"GET",   []       ,[]}          -> je(ets({sizes,gcall({all,[]})}));
    {"GET",   [Tab]    ,[]}          -> je(ets({keys,Tab}));
    {"GET",   [Tab|Key],[]}          -> ets({get,Tab,Key});
    {"GET",   [Tab|Key],["next"]}    -> je(ets({next,Tab,Key}));
    {"GET",   [Tab|Key],["prev"]}    -> je(ets({prev,Tab,Key}));
    {"GET",   [Tab|Key],["multi"]}   -> je(ets({multi_get,Tab,Key}));
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
  Body.

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
ets({counter,Tab,Key})   -> update_counter(tab(Tab),ikey(Key));
ets({reset,Tab,Key})     -> ets:insert(tab(Tab),{ikey(Key),0}),"0";
ets({next,Tab,Key})      -> next(tab(Tab),ikey(Key));
ets({prev,Tab,Key})      -> prev(tab(Tab),ikey(Key));
ets({get,Tab,Key})       -> getter(tab(Tab),lkey(Key));
ets({multi_get,Tab,Key}) -> multi_getter(tab(Tab),lkey(Key));
ets({delete,Tab,Key})    -> ets:delete(tab(Tab),ikey(Key)).

multi_inserter(Tab,KVs) ->
  {PL} = jd(KVs),
  ets:insert_new(Tab,[{ikey(binary_to_elems(K)),je(V)} || {K,V} <- PL]).

inserter(Tab,{Ks,V}) ->
  K = ikey(Ks),
  case ets:insert_new(Tab,{K,V}) of
    false-> throw({409,{exists,Tab,K}});
    true -> true
  end.

size_getter([])   -> [];
size_getter(Tabs) -> {[{T,ets:info(T,size)} || T <- Tabs]}.

key_getter(Tab) ->
  ets:foldr(fun({K,_},A) -> [elems_to_binary(K)|A] end,[],Tab).

next(Tab,Key) ->
  ets:lookup(Tab,ets:next(Tab,Key)).

prev(Tab,Key) ->
  ets:prev(Tab,Key).

getter(Tab,Key) ->
  case ets:select(Tab,[{{Key,'_'},[],['$_']}]) of
    [{_,Res}] -> maybe_counter(Res);
    []        -> throw({404,no_such_key});
    _         -> throw({404,multiple_hits})
  end.

maybe_counter(E) when is_integer(E) -> integer_to_list(E);
maybe_counter(E) -> E.

multi_getter(Tab,Key) ->
  case ets:select(Tab,[{{Key,'_'},[],['$_']}]) of
    []   -> throw({404,no_such_key});
    Hits -> lists:foldl(fun({K,_},A) -> [elems_to_binary(K)|A] end,[],Hits)
  end.

update_counter(Tab,Key) ->
  try integer_to_list(ets:update_counter(Tab,Key,1))
  catch _:_ -> ets:insert(Tab,{Key,1}),"1"
  end.

%% key for inserts/deletes
ikey(L) ->
  list_to_tuple([ielem(E) || E <- L]).

ielem("_") -> throw({404,key_has_underscore});
ielem(E)   -> l2b(E).

%% key for lookups
lkey(L) ->
  list_to_tuple([lelem(E) || E <- L]).

lelem("_") -> '_';
lelem(E)   -> l2b(E).

%% exporting keys
elems_to_binary(T) ->
  l2b(join(tuple_to_list(T),<<"/">>)).

join([E],_) -> [E];
join([E|R],D) -> [E,D|join(R,D)].

%% importing keys
binary_to_elems(B) ->
  string:tokens(binary_to_list(B),"/").

l2b(L) ->
  list_to_binary(L).

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

%% a nif that throws? insanity.
jd(Term) ->
  try jiffy:decode(Term)
  catch {error,R} -> error({R,Term})
  end.

je(Term) ->
  try jiffy:encode(Term)
  catch {error,R} -> error({R,Term})
  end.

flat(Term) ->
  lists:flatten(io_lib:fwrite("~p",[Term])).

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
  ?assertEqual({200,["a/2/x","a/1/x"]},
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
