%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 27 Jun 2012 by mats cronqvist <masse@klarna.com>

%% @doc
%% a rest wrapper around ets/leveldb
%% @end

-module(rets).
-author('mats cronqvist').

-export([start/0]).       % start the application
-export([start/1]).       % interactive start, with backend choice
-export([state/0]).

%% should say "-behavior(cowboy_http_handler)." here, but rebar freaks out

-export([handle/2,
         init/3,
         terminate/3]).

-export([cowboy_opts/0]).

%% main application starter
start() ->
  application:ensure_all_started(rets).

start(Backend) ->
  case lists:member(Backend,[leveldb,ets]) of
    true -> application:set_env(rets,backend,Backend);
    false-> exit({bad_backend,Backend})
  end,
  start().

state() ->
  rets_handler:state().

%% called from rets_app
cowboy_opts() ->
  [{name,rets_listener},
   {acceptors,100},
   {opts,[{port, 7890}]},
   {routes,[{'_', [{"/[...]", rets, []}]}]}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% cowboy callbacks
init({tcp, http}, Req, []) ->
  {ok, Req, []}.

terminate(_Reason, _Req, _State) ->
  ok.

%% called from cowboy. Req is the cowboy opaque state object.
handle(Req,State) ->
  {ok,mk_reply(Req),State}.

mk_reply(Req) ->
  try
    cow_reply(200,<<"application/json">>,je(reply(Req)),Req)
  catch
    throw:{Status,R} ->
      cow_reply(Status,<<"text/plain">>,reply(Status,R),Req)
  end.

cow_reply(Status,ContentType,Body,Req) ->
  Headers = [{<<"content-type">>, ContentType}],
  {ok,Rq} = cowboy_req:reply(Status,Headers,Body,Req),
  Rq.

reply(400,R) -> flat(R);
reply(404,R) -> flat(R);
reply(405,R) -> flat(R);
reply(409,R) -> flat(R);
reply(500,R) -> flat({R,erlang:get_stacktrace()}).

reply(Req) ->
  x(method(Req),uri(Req),rets_headers(Req),Req).

%% map http -> operations
x("GET",[]    ,[]        ,_)   -> g([{<<"info">>}]);
x("GET",[T]   ,[]        ,_)   -> g([{<<"keys">>  ,T}]);
x("GET",[T]   ,["info"]  ,_)   -> g([{<<"info">>  ,T}]);
x("GET",[T|Es],[]        ,_)   -> g([{<<"single">>,T,Es}]);
x("GET",[T|Es],["single"],_)   -> g([{<<"single">>,T,Es}]);
x("GET",[T|Es],["multi"] ,_)   -> g([{<<"multi">> ,T,Es}]);
x("GET",[T|Es],["keys"]  ,_)   -> g([{<<"keys">>  ,T,Es}]);
x("GET",[T|Es],["next"]  ,_)   -> g([{<<"next">>  ,T,Es}]);
x("GET",[T|Es],["prev"]  ,_)   -> g([{<<"prev">>  ,T,Es}]);

x("PUT",[T]   ,[]       ,_)    -> g([{<<"create">>,T}]);
x("PUT",[T|Es],[]       ,R)    -> g([{<<"insert">>,T,Es,body(R)}]);
x("PUT",[T|Es],["force"],R)    -> g([{<<"insert">>,T,Es,body(R),force}]);
x("PUT",[T|Es],["bump"] ,_)    -> g([{<<"bump">>  ,T,Es}]);
x("PUT",[T|Es],["reset"],_)    -> g([{<<"reset">> ,T,Es}]);

x("DELETE",[T]   ,[]       ,_) -> g([{<<"destroy">>,T}]);
x("DELETE",[T]   ,["force"],_) -> g([{<<"destroy">>,T,force}]);
x("DELETE",[T|Es],[]       ,R) -> g([{<<"delete">> ,T,Es,body(R)}]);
x("DELETE",[T|Es],["force"],_) -> g([{<<"delete">> ,T,Es,force}]);

x("POST",_,[],R)               -> g(chk_body(body(R)));

x("TRACE",_,_,_)               -> throw({405,"method not allowed"});
x(Method,URI,Headers,_)        -> throw({404,{Method,URI,Headers}}).

chk_body(Body) ->
  try [list_to_tuple(E) || E <- Body]
  catch _:R -> throw({400,{malformed_body,R,Body}})
  end.

body(Req) ->
  case cowboy_req:has_body(Req) of
    true ->
      {ok,Body,_} = cowboy_req:body(Req),
      case jd(Body) of
        <<>> -> [];
        B    -> B
      end;
    false-> []
  end.

method(Req) ->
  {Method,_} = cowboy_req:method(Req),
  string:to_upper(binary_to_list(Method)).

uri(Req) ->
  {Elems,_} = cowboy_req:path_info(Req),
  Elems.

g(Ops) ->
  {F,ValidatedOps} = chk_ops(Ops),
  case gen_server:call(rets_handler,{F,ValidatedOps}) of
    {ok,{[]}}  -> [];
    {ok,Reply} -> Reply;
    {Status,R} -> throw({Status,R})
  end.

%% validate operations
%% State is {r|w,[Tab|Elements],[ops()]}
chk_ops(Ops) ->
  {F,_,Rops} = lists:foldl(fun chk_op/2,{'',<<>>,[]},Ops),
  {F,lists:reverse(Rops)}.

%% these handle both ops from the normal verbs and oplists from POST
chk_op({<<"bump">>   ,T,K}      ,S) -> emit_op(w,bump    ,{T,K,1},S);
chk_op({<<"create">> ,T}        ,S) -> emit_op(t,create  ,{T,""},S);
chk_op({<<"delete">> ,T,K,V}    ,S) -> emit_op(w,delete  ,{T,K,V},S);
chk_op({<<"destroy">>,T,force}  ,S) -> emit_op(t,destroy ,{T,force},S);
chk_op({<<"destroy">>,T}        ,S) -> emit_op(t,destroy ,{T,""},S);
chk_op({<<"info">>}             ,S) -> emit_op(r,info    ,{"",""},S);
chk_op({<<"info">>   ,T}        ,S) -> emit_op(r,info    ,{T,""},S);
chk_op({<<"insert">> ,T,K,V,OV} ,S) -> emit_op(w,insert  ,{T,K,{V,OV}},S);
chk_op({<<"insert">> ,T,K,V}    ,S) -> emit_op(w,insert  ,{T,K,V},S);
chk_op({<<"keys">>   ,T,K}      ,S) -> emit_op(r,keys    ,{T,K},S);
chk_op({<<"keys">>   ,T}        ,S) -> emit_op(r,keys    ,{T,""},S);
chk_op({<<"multi">>  ,T,K}      ,S) -> emit_op(r,multi   ,{T,K},S);
chk_op({<<"next">>   ,T,K}      ,S) -> emit_op(r,next    ,{T,K},S);
chk_op({<<"prev">>   ,T,K}      ,S) -> emit_op(r,prev    ,{T,K},S);
chk_op({<<"reset">>  ,T,K}      ,S) -> emit_op(w,reset   ,{T,K,0},S);
chk_op({<<"single">> ,T,K}      ,S) -> emit_op(r,single  ,{T,K},S);
chk_op(What,_S)                     -> throw({400,{bad_op,What}}).

emit_op(X,Op,TO,{T,_}) when T=/=[] andalso T=/=X ->
  throw({400,{mixed_op_types,Op,TO}});
emit_op(t,Op,{T,O},{t,A}) ->
  chk_key(w,[T]),
  {t,[{Op,T,O}|A]};
emit_op(r,Op,{T,K},{_,A}) ->
  chk_key(r,[T|K]),
  {r,[{Op,T,K}|A]};
emit_op(w,Op,{T,K,V},{_,A}) ->
  chk_key(w,[T|K]),
  {w,[{Op,T,K,V}|A]}.

chk_key(RorW,Key) ->
  mk_bkey(lists:map(fun(E) -> chkk_el(RorW,E) end,mk_ekey(Key))).
chkk_el(w,".") -> throw({400,key_element_is_period});
chkk_el(r,".") -> ".";
chkk_el(_,El)  -> lists:map(fun good_char/1,El).

mk_ekey(Key) when is_binary(Key)  -> string:tokens(binary_to_list(Key),"/");
mk_ekey(Key) -> throw({400,{key_has_bad_type,Key}}).

mk_bkey(EKey) -> list_to_binary(string:join(EKey,"/")).

%% rfc 3986
%% unreserved = ALPHA / DIGIT / "-" / "." / "_" / "~"
-define(is_good(C),
        (($A=<C andalso C=<$Z)
         orelse ($a=<C andalso C=<$z)
         orelse ($0=<C andalso C=<$9)
         orelse (C=:=$-)
         orelse (C=:=$.)
         orelse (C=:=$_)
         orelse (C=:=$~))).
good_char(C) when ?is_good(C) -> C;
good_char(C) -> throw({400,{invalid_character_in_uri,C}}).

rets_headers(Req) ->
  {Headers,_}= cowboy_req:headers(Req),
  case proplists:get_value(<<"rets">>,Headers) of
    undefined  -> [];
    RetsHeader -> string:tokens(binary_to_list(RetsHeader),",")
  end.

flat(Term) ->
  lists:flatten(io_lib:fwrite("~p",[Term])).

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% eunit
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%t01_ets_test()     -> t01(ets).
t01_leveldb_test() -> t01(leveldb).
t01(Backend) ->
  restart_rets(Backend),
  ?assertEqual({200,[{"aaa/1/x",null},
                     {"bbb",null},
                     {"ccc",null},
                     {"ddd",null}]},
               rets_client:post(localhost,
                                [[insert,'aaa/1/x',"AAA"++[223]],
                                 [insert,bbb,bBbB],
                                 [insert,ccc,123.1],
                                 [insert,ddd,[{a,"A"},{b,b},{c,123.3}]]])),
  ?assertEqual({200,[{"aaa/1/x",[$A,$A,$A,223]},
                     {"aaa/1/x",[$A,$A,$A,223]},
                     {"bbb","bBbB"},
                     {"ccc",123.1}]},
               rets_client:post(localhost,
                               [[multi,'aaa/./x'],
                                [single,'aaa/1/x'],
                                [single,bbb],
                                [multi,ccc]])).


%t02_ets_test()     -> t02(ets).
t02_leveldb_test() -> t02(leveldb).
t02(Backend) ->
  restart_rets(Backend),
  ?assertMatch({409,_},
              rets_client:get(localhost,foo,prev)),
  ?assertMatch({409,_},
              rets_client:get(localhost,foo,next)),
  ?assertEqual({200,[{"aaa/1/x",null},
                     {"bbb",null},
                     {"ccc",null},
                     {"ddd",null}]},
               rets_client:post(localhost,
                                [[insert,'aaa/1/x',"AAA"++[223]],
                                 [insert,bbb,bBbB],
                                 [insert,ccc,123.1],
                                 [insert,ddd,[{a,"A"},{b,b},{c,123.3}]]])),
  ?assertEqual({200,[{"aaa/1/x","AAA"++[223]},
                     {"aaa/1/x","AAA"++[223]},
                     {"bbb","bBbB"},
                     {"ccc",123.1},
                     {"bbb","bBbB"},
                     {"ccc",123.1},
                     {"ddd",[{"a","A"},{"b","b"},{"c",123.3}]}]},
               rets_client:post(localhost,
                                [[next,''],
                                 [next,'aaa/./x'],
                                 [next,'aaa/1/x'],
                                 [next,bbb],
                                 [prev,ccc],
                                 [prev,ddd],
                                 [prev,xxx]])),

  ?assertMatch({409,_},
               rets_client:post(localhost,
                                [[next,ddd]])),

  ?assertMatch({409,_},
               rets_client:post(localhost,
                                [[prev,'aaa/1/x']])).

%t03_ets_test()     -> t03(ets).
t03_leveldb_test() -> t03(leveldb).
t03(Backend) ->
  restart_rets(Backend),
  ?assertEqual({200,[{"foo",null}]},
               rets_client:post(localhost,[[bump,"foo"]])),
  ?assertEqual({200,[{"foo",1}]},
               rets_client:post(localhost,[[bump,"foo"]])),
  ?assertEqual({200,[{"foo",2}]},
               rets_client:post(localhost,[[reset,"foo"]])),
  ?assertEqual({200,[{"foo",0}]},
               rets_client:post(localhost,[[bump,"foo"]])).

%t04_ets_test()     -> t04(ets).
t04_leveldb_test() -> t04(leveldb).
t04(Backend) ->
  restart_rets(Backend),
  ?assertEqual({200,[]},
               rets_client:post(localhost,[[keys,"bla"]])),
  ?assertEqual({200,[{"bla",null}]},
               rets_client:post(localhost,[[bump,"bla"]])),
  ?assertEqual({200,[{"bla",null}]},
               rets_client:post(localhost,[[keys,"bla"]])),
  ?assertEqual({200,[{"bla",1}]},
               rets_client:post(localhost,[[delete,"bla"]])),
  ?assertEqual({200,[]},
               rets_client:post(localhost,[[keys,"bla"]])),
  ?assertEqual({200,[{"bla/1",null}]},
               rets_client:post(localhost,[[bump,"bla/1"]])),
  ?assertEqual({200,[{"bla/1",null}]},
               rets_client:post(localhost,[[keys,"bla/."]])),
  ?assertEqual({200,[{"bla/2",null}]},
               rets_client:post(localhost,[[bump,"bla/2"]])),
  ?assertEqual({200,[{"bla/1",null},{"bla/2",null}]},
               rets_client:post(localhost,[[keys,"bla/."]])),
  ?assertEqual({200,[{"foo",null}]},
               rets_client:post(localhost,[[bump,"foo"]])),
  ?assertEqual({200,[{"bla/1",null},{"bla/2",null}]},
               rets_client:post(localhost,[[keys,"bla/."]])).

%t05_ets_test()     -> t05(ets).
t05_leveldb_test() -> t05(leveldb).
t05(Backend) ->
  restart_rets(Backend),
  ?assertEqual({200,[{"bla",null}]},
               rets_client:post(localhost,[[bump,"bla"]])),
  ?assertMatch({400,_},
               rets_client:post(localhost,[[delete,"bla",2]])),
  ?assertEqual({200,[{"bla",null}]},
               rets_client:post(localhost,[[keys,"bla"]])),
  ?assertEqual({200,[{"bla",1}]},
               rets_client:post(localhost,[[delete,"bla",1]])),
  ?assertEqual({200,[]},
               rets_client:post(localhost,[[keys,"bla"]])).

%t06_ets_test()     -> t06(ets).
t06_leveldb_test() -> t06(leveldb).
t06(Backend) ->
  restart_rets(Backend),
  ?assertEqual({200,[{"baz",null}]},
               rets_client:post(localhost,[[bump,"baz"]])),
  ?assertMatch({400,_},
               rets_client:post(localhost,[[insert,"baz",2,3]])),
  ?assertMatch({200,[{"baz",1}]},
               rets_client:post(localhost,[[insert,"baz",2,1]])),
  ?assertEqual({200,[{"baz",2}]},
               rets_client:get(localhost,"baz")).

%t07_ets_test()     -> t07(ets).
t07_leveldb_test() -> t07(leveldb).
t07(Backend) ->
  restart_rets(Backend),
  ?assertMatch({404,_},
               rets_client:get(localhost,"quux")),
  ?assertEqual({200,[{"quux/1",null}]},
               rets_client:put(localhost,"quux/1",1)),
  ?assertEqual({200,[{"quux/2",null}]},
               rets_client:put(localhost,"quux/2",2)),
  ?assertMatch({404,_},
               rets_client:get(localhost,"quux/.")).

restart_rets(Backend) ->
  application:stop(rets),
  {ok,_} = start(Backend).

-endif. % TEST
