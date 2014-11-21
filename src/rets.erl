%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 27 Jun 2012 by mats cronqvist <masse@klarna.com>

%% @doc
%% a rest wrapper around ets
%% @end

-module(rets).
-author('mats cronqvist').

-export([start/0]).       % start the application

-export([start/1]).       % interactive start, with backend choice

-behavior(cowboy_http_handler).
-export([handle/2,
         init/3,
         terminate/3]).

-export([cowboy_opts/0]).

%% main application starter
start() ->
  application:ensure_all_started(rets).

start(Backend) ->
  case Backend of
    leveldb -> application:set_env(rets,backend,leveldb);
    ets     -> application:unset_env(rets,backend)
  end,
  start().

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

reply(404,R) -> flat(R);
reply(405,R) -> flat(R);
reply(409,R) -> flat(R);
reply(500,R) -> flat({R,erlang:get_stacktrace()}).

reply(Req) ->
  x(method(Req),uri(Req),rets_headers(Req),Req).

x("PUT",   [Tab]    ,[],_)          -> g({create,[Tab]});
x("PUT",   [Tab|Key],[],R)          -> g({insert,[Tab,chkk(Key),body(R)]});
x("PUT",   [Tab|Key],["counter"],_) -> g({bump,  [Tab,chkk(Key),1]});
x("PUT",   [Tab|Key],["reset"],_)   -> g({reset, [Tab,chkk(Key),0]});
x("GET",   []       ,[],_)          -> g({sizes, []});
x("GET",   [Tab]    ,[],_)          -> g({keys,  [Tab]});
x("GET",   [Tab|Key],[],_)          -> g({single,[Tab,Key]});
x("GET",   [Tab|Key],["multi"],_)   -> g({multi, [Tab,Key]});
x("GET",   [Tab|Key],["next"],_)    -> g({next,  [Tab,Key]});
x("GET",   [Tab|Key],["prev"],_)    -> g({prev,  [Tab,Key]});
x("POST",  [Tab]    ,[],R)          -> g({insert,[Tab,chkb(body(R))]});
x("DELETE",[Tab]    ,[],_)          -> g({delete,[Tab]});
x("DELETE",[Tab|Key],[],_)          -> g({delete,[Tab,Key]});
x("TRACE",_,_,_)                    -> throw({405,"method not allowed"});
x(Meth,URI,Headers,Bdy)             -> throw({404,{Meth,URI,Headers,Bdy}}).

g(FArgs) ->
  case gen_server:call(rets_handler,FArgs) of
    {ok,Reply} -> Reply;
    Bad        -> throw(Bad)
  end.

chkb({Body}) -> lists:map(fun chk_bp/1,Body).
chk_bp({Key,Val}) ->
  {chkk(string:tokens(binary_to_list(Key),"/")),Val}.

chkk(Key) -> lists:map(fun chk_el/1,Key).
chk_el(".") -> throw({404,key_element_is_period});
chk_el(El)  -> El.

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
  {URI,_} = cowboy_req:path_info(Req),
  chk_uri(URI).

chk_uri([<<>>]) ->
  [];
chk_uri(URI) ->
  try
    [lists:map(fun good_char/1,binary_to_list(E)) || E <- URI]
  catch
    throw:{bad_char,C} -> {bad_char,{[C]}}
  end.

%%ALPHA / DIGIT / "-" / "." / "_" / "~"
-define(is_good(C),
        (($A=<C andalso C=<$Z)
         orelse ($a=<C andalso C=<$z)
         orelse ($0=<C andalso C=<$9)
         orelse (C=:=$-)
         orelse (C=:=$.)
         orelse (C=:=$_)
         orelse (C=:=$~))).
good_char(C) when ?is_good(C) -> C;
good_char(C) -> throw({bad_char,C}).

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

t00_ets_test()     -> t00(ets).
t00_leveldb_test() -> t00(leveldb).
t00(Backend) ->
  restart_rets(Backend),
  ?assertEqual({200,false},
               rets_client:delete(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,false},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:post(localhost,tibbe,
                                [{'aaa/1/x',"AAA"++[223]},
                                 {bbb,bBbB},
                                 {ccc,123.1},
                                 {ddd,[{a,"A"},{b,b},{c,123.3}]}])),
  ?assertEqual({200,[$A,$A,$A,223]},
               rets_client:get(localhost,tibbe,"aaa/./x")),
  ?assertEqual({200,["aaa/1/x","bbb","ccc","ddd"]},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tabbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tabbe,a,b)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tobbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tobbe,a,b)),
  ?assertEqual({200,["aaa/1/x","bbb","ccc","ddd"]},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,[$A,$A,$A,223]},
               rets_client:get(localhost,tibbe,'./1/.')),
  ?assertEqual({200,"bBbB"},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,123.1},
               rets_client:get(localhost,tibbe,ccc)),
  ?assertEqual({200,[{"a","A"},{"b","b"},{"c",123.3}]},
               rets_client:get(localhost,tibbe,"ddd")).

t01_ets_test()     -> t01(ets).
t01_leveldb_test() -> t01(leveldb).
t01(Backend) ->
  restart_rets(Backend),
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
               rets_client:get(localhost,tibbe,'aaa/./x')),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'./1/.')),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'aaa/1/x')),
  ?assertEqual({200,"bBbB"},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,123.1},
               rets_client:get(localhost,tibbe,ccc)),
  ?assertEqual({200,[{"a","A"},{"b","b"},{"c",123.3}]},
               rets_client:get(localhost,tibbe,ddd)).

t02_ets_test()     -> t02(ets).
t02_leveldb_test() -> t02(leveldb).
t02(Backend) ->
  restart_rets(Backend),
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

t03_ets_test()     -> t03(ets).
t03_leveldb_test() -> t03(leveldb).
t03(Backend) ->
  restart_rets(Backend),
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

t04_ets_test()     -> t04(ets).
t04_leveldb_test() -> t04(leveldb).
t04(Backend) ->
  restart_rets(Backend),
  ?assertEqual({200,[]},
               rets_client:get(localhost)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,[{tibbe,0}]},
               rets_client:get(localhost)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tabbe)),
  ?assertEqual({200,[{tabbe,0},{tibbe,0}]},
               rets_client:get(localhost)),
  ?assertEqual({200,true},
               rets_client:post(localhost,tibbe,[{foo,17}])),
  ?assertEqual({200,[{tabbe,0},{tibbe,1}]},
               rets_client:get(localhost)),
  ?assertEqual({200,17},
               rets_client:delete(localhost,tibbe,foo)),
  ?assertEqual({200,null},
               rets_client:delete(localhost,tibbe,foo)),
  ?assertEqual({200,[{tabbe,0},{tibbe,0}]},
               rets_client:get(localhost)).

t05_ets_test()     -> t05(ets).
t05_leveldb_test() -> t05(leveldb).
t05(Backend) ->
  restart_rets(Backend),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,foo,17)),
  ?assertEqual({200,[{tibbe,1}]},
               rets_client:get(localhost)),
  ?assertMatch({409,_},
               rets_client:put(localhost,tibbe,foo,a)).

t06_ets_test()     -> t06(ets).
t06_leveldb_test() -> t06(leveldb).
t06(Backend) ->
  restart_rets(Backend),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,17,foo)),
  ?assertEqual({200,"foo"},
               rets_client:delete(localhost,tibbe,17)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({200,true},
               rets_client:delete(localhost,tibbe)),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe)).

t07_ets_test()     -> t07(ets).
t07_leveldb_test() -> t07(leveldb).
t07(Backend) ->
  restart_rets(Backend),
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
               rets_client:get(localhost,tibbe,'a/./.')),
  ?assertEqual({404,"key_element_is_period"},
               rets_client:put(localhost,tibbe,'a/./.',s)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,'a/1/x',primo)),
  ?assertEqual({404,"no_such_key"},
                rets_client:get(localhost,tibbe,'a')),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,'a',1)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tibbe,'a')),
  ?assertEqual({404,"multiple_hits"},
               rets_client:get(localhost,tibbe,'a/./.')),
  ?assertEqual({200,[{"a/1/x","primo"},{"a/2/x","segundo"}]},
               rets_client:get(localhost,tibbe,'a/./.',multi)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tibbe,'b/./.',multi)).

t08_ets_test()     -> t08(ets).
t08_leveldb_test() -> t08(leveldb).
t08(Backend) ->
  restart_rets(Backend),
  T = [{"a","a"},{"b",[{"bb","bb"}]}], %% nested proplist
  ?assertEqual({200,true},
               rets_client:put(localhost,tybbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tybbe,"abc",T)),
  ?assertMatch({200,T},
               rets_client:get(localhost,tybbe,"abc")).

t09_ets_test()     -> t09(ets).
t09_leveldb_test() -> t09(leveldb).
t09(Backend) ->
  restart_rets(Backend),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,0,next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,0,prev)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,a,1)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tebbe,a)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,a,next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,a,prev)),
  ?assertEqual({200,[{"a",1}]},
               rets_client:get(localhost,tebbe,b,prev)),
  ?assertEqual({200,[{"a",1}]},
               rets_client:get(localhost,tebbe,0,next)).

restart_rets(Backend) ->
  application:stop(rets),
  start_and_wait(Backend).

start_and_wait(Backend) ->
  receive after 200 -> ok end,
  case start(Backend) of
    {ok,_} ->
      wait_for_start();
    R ->
      erlang:display({waiting_for_shutdown,R}),
      receive after 200 -> ok end,
      start_and_wait(Backend)
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
