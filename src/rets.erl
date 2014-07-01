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
    {"PUT",   [Tab]     ,[]}          -> je(gcall({create,Tab}));
    {"PUT",   [Tab|KeyL],[]}          -> je(ets({insert,Tab,KeyL,body(Req)}));
    {"PUT",   [Tab|KeyL],["counter"]} -> ets({counter,Tab,KeyL});
    {"PUT",   [Tab|KeyL],["reset"]}   -> ets({reset,Tab,KeyL});
    {"GET",   [[]]      ,[]}          -> je([l2b(T)||T<-gcall({all,[]})]);
    {"GET",   [Tab]     ,[]}          -> je(ets({keys,Tab}));
    {"GET",   [Tab|KeyL],[]}          -> ets({get,Tab,KeyL});
    {"POST",  [Tab]     ,[]}          -> je(ets({insert,Tab,body(Req)}));
    {"DELETE",[Tab]     ,[]}          -> je(gcall({delete,Tab}));
    {"DELETE",[Tab|KeyL],[]}          -> je(ets({delete,Tab,KeyL}));
    X                                 -> throw({404,X})
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

ets({keys,Tab})       -> key_getter(tab(Tab));
ets({insert,Tab,K,V}) -> inserter(tab(Tab),{K,V});
ets({insert,Tab,KVs}) -> multi_inserter(tab(Tab),KVs);
ets({counter,Tab,Key})-> update_counter(tab(Tab),ikey(Key));
ets({reset,Tab,Key})  -> ets:insert(tab(Tab),{ikey(Key),0}),"0";
ets({get,Tab,Key})    -> getter(tab(Tab),lkey(Key));
ets({delete,Tab,Key}) -> ets:delete(tab(Tab),ikey(Key)).

multi_inserter(Tab,KVs) ->
  {PL} = jd(KVs),
  ets:insert_new(Tab,[{ikey(binary_to_elems(K)),je(V)} || {K,V} <- PL]).

inserter(Tab,{Ks,V}) ->
  K = ikey(Ks),
  case ets:insert_new(Tab,{K,V}) of
    false-> throw({409,{exists,Tab,K}});
    true -> true
  end.

key_getter(Tab) ->
  ets:foldr(fun({K,_},A) -> [elems_to_binary(K)|A] end,[],Tab).

getter(Tab,Key) ->
  case ets:select(Tab,[{{Key,'_'},[],['$_']}]) of
    [{_,Res}] ->
      case is_integer(Res) of
        true -> integer_to_list(Res);
        false-> Res
      end;
    [] ->
      throw({404,no_such_key})
  end.

update_counter(Tab,Key) ->
  try integer_to_list(ets:update_counter(Tab,Key,1))
  catch _:_ -> ets:insert(Tab,{Key,1}),"1"
  end.

jd(Term) ->
  jiffy:decode(Term).

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
  list_to_binary(join(tuple_to_list(T),<<"/">>)).

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

je(Term) ->
  jiffy:encode(Term).

flat(Term) ->
  lists:flatten(io_lib:fwrite("~p",[Term])).

%%%%%%%%%%
%% eunit
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

t00_test() ->
  restart_rets(),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,"true"},
               rets_client:post(localhost,tibbe,
                                [{'aaa/1/x',"AAA"},
                                 {bbb,bBbB},
                                 {ccc,123.1},
                                 {ddd,[{a,"A"},{b,b},{c,123.3}]}])),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'aaa/_/x')),
  ?assertEqual({200,[bbb,ccc,ddd,'aaa/1/x']},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'_/1/_')),
  ?assertEqual({200,bBbB},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,123.1},
               rets_client:get(localhost,tibbe,ccc)),
  ?assertEqual({200,[{a,"A"},{b,b},{c,123.3}]},
               rets_client:get(localhost,tibbe,ddd)).

t01_test() ->
  restart_rets(),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe,'aaa/1/x',"AAA")),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe,bbb,bBbB)),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe,ccc,123.1)),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe,ddd,[{a,"A"},{b,b},{c,123.3}])),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'aaa/_/x')),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'_/1/_')),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'aaa/1/x')),
  ?assertEqual({200,bBbB},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,123.1},
               rets_client:get(localhost,tibbe,ccc)),
  ?assertEqual({200,[{a,"A"},{b,b},{c,123.3}]},
               rets_client:get(localhost,tibbe,ddd)).

t02_test() ->
  restart_rets(),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe,bbb,bBbB)),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe,ddd,[{a,"A"},{b,b},{c,123.3}])),
  ?assertEqual({200,<<"bBbB">>},
               rets_client:get(localhost,tibbe,bbb,[no_atoms])),
  ?assertEqual({200,[{<<"a">>,"A"},{<<"b">>,<<"b">>},{<<"c">>,123.3}]},
               rets_client:get(localhost,tibbe,ddd,[no_atoms])).

t03_test() ->
  restart_rets(),
  ?assertEqual({200,"true"},
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
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,[tibbe]},
               rets_client:get(localhost)).

t05_test() ->
  restart_rets(),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe,17,foo)),
  ?assertMatch({409,_},
               rets_client:put(localhost,tibbe,17,a)).

t06_test() ->
  restart_rets(),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe,17,foo)),
  ?assertEqual({200,"true"},
               rets_client:delete(localhost,tibbe,17)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({200,"true"},
               rets_client:delete(localhost,tibbe)),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe)).

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
