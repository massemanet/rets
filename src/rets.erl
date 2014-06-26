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
    try          cow_reply(200,<<"application/json">>,reply(Req),Req)
    catch _:R -> cow_reply(404,<<"text/plain">>,reply404(R),Req)
    end,
  {ok,Req2,State}.

reply404(R) ->
  "fourohfour - "++flat(R).

cow_reply(Status,ContentType,Body,Req) ->
  cowboy_req:reply(Status,
                   [{<<"content-type">>, ContentType}],
                   Body,
                   Req).

reply(Req) ->
  case {method(Req),uri(Req),true_headers(Req)} of
    {"PUT",   [Tab,Key],["counter"]} -> ets({counter,Tab,Key});
    {"PUT",   [Tab,Key],["reset"]}   -> ets({reset,Tab,Key});
    {"PUT",   [Tab,Key],[]}          -> je(ets({insert,Tab,Key,body(Req)}));
    {"PUT",   [Tab]    ,[]}          -> je(gcall({create,Tab}));
    {"GET",   [[]]     ,[]}          -> je([l2b(T)||T<-gcall({all,[]})]);
    {"GET",   [Tab]    ,[]}          -> je(ets({keys,Tab}));
    {"GET",   [Tab,Key],[]}          -> ets({get,Tab,Key});
    {"POST",  [Tab]    ,[]}          -> je(ets({insert,Tab,body(Req)}));
    {"DELETE",[Tab]    ,[]}          -> je(gcall({delete,Tab}));
    {"DELETE",[Tab,Key],[]}          -> je(ets({delete,Tab,Key}));
    _ -> throw({method(Req),uri(Req),headers(Req)})
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

ets({keys,Tab})       -> ets:foldr(fun({K,_},A)->[K|A]end,[],tab(Tab));
ets({insert,Tab,K,V}) -> ets:insert(tab(Tab),{l2b(K),V});
ets({insert,Tab,KVs}) -> ets:insert(tab(Tab),unpack(KVs));
ets({counter,Tab,Key})-> update_counter(tab(Tab),l2b(Key));
ets({reset,Tab,Key})  -> ets:insert(tab(Tab),{l2b(Key),0}),"0";
ets({get,Tab,Key})    -> getter(tab(Tab),l2b(Key));
ets({delete,Tab,Key}) -> ets:delete(tab(Tab),l2b(Key)).

getter(Tab,Key) ->
  case ets:lookup(Tab,Key) of
    [{Key,Res}] ->
      case is_integer(Res) of
        true -> integer_to_list(Res);
        false-> Res
      end;
    [] ->
      throw(no_such_key)
  end.

update_counter(Tab,Key) ->
  try integer_to_list(ets:update_counter(Tab,Key,1))
  catch _:_ -> ets:insert(Tab,{Key,1}),"1"
  end.

unpack(KVs) ->
  {PL} = jd(KVs),
  [{K,je(V)} || {K,V} <- PL].

jd(Term) ->
  jiffy:decode(Term).

l2b(L) ->
  list_to_binary(L).

tab(L) ->
  T = list_to_existing_atom(L),
  case ets:info(T,size) of
    undefined -> throw(no_such_table);
    _ -> T
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
                                [{aaa,"AAA"},{bbb,bBbB},{ccc,123.1},
                                 {ddd,[{a,"A"},{b,b},{c,123.3}]}])),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,aaa)),
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
               rets_client:put(localhost,tibbe,aaa,"AAA")),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe,bbb,bBbB)),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe,ccc,123.1)),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe,ddd,[{a,"A"},{b,b},{c,123.3}])),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,aaa)),
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
  ?assertEqual({200,"true"}, rets_client:put(localhost,tibbe)),
  ?assertEqual({200,1}, rets_client:put(localhost,tibbe,bbb,counter)),
  ?assertEqual({200,2}, rets_client:put(localhost,tibbe,bbb,counter)),
  ?assertEqual({200,2}, rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,0}, rets_client:put(localhost,tibbe,bbb,reset)),
  ?assertEqual({200,1}, rets_client:put(localhost,tibbe,bbb,counter)).

t04_test() ->
  restart_rets(),
  ?assertEqual({200,[]},
               rets_client:get(localhost)),
  ?assertEqual({200,"true"},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,[tibbe]},
               rets_client:get(localhost)).

restart_rets() ->
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
