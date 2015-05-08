%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 27 Jun 2012 by mats cronqvist <masse@klarna.com>

%% @doc
%% a rest wrapper around ets
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

reply(404,R) -> flat(R);
reply(405,R) -> flat(R);
reply(409,R) -> flat(R);
reply(500,R) -> flat({R,erlang:get_stacktrace()}).

reply(Req) ->
  x(method(Req),uri(Req),rets_headers(Req),Req).

x("PUT",   [Tab]    ,[],_)              -> g({create,[Tab]});
x("PUT",   [Tab|Key],["indirect",T],_)  -> g({via,   [T,Tab,chkk(Key)]});
x("PUT",   [Tab|Key],[],R)              -> g({insert,[Tab,chkk(Key),body(R)]});
x("PUT",   [Tab|Key],["counter"],_)     -> g({bump,  [Tab,chkk(Key),1]});
x("PUT",   [Tab|Key],["counter",L,H],_) -> g({bump,  [Tab,chkk(Key),i(L),i(H)
                                                     ]});
x("PUT",   [Tab|Key],["reset"],_)       -> g({reset, [Tab,chkk(Key),0]});
x("GET",   []       ,[],_)              -> g({sizes, []});
x("GET",   [Tab]    ,[],_)              -> g({keys,  [Tab]});
x("GET",   [Tab|Key],[],_)              -> g({single,[Tab,Key]});
x("GET",   [Tab|Key],["multi"],_)       -> g({multi, [Tab,Key]});
x("GET",   [Tab|Key],["next"],_)        -> g({next,  [Tab,Key]});
x("GET",   [Tab|Key],["prev"],_)        -> g({prev,  [Tab,Key]});
x("POST",  [Tab]    ,[],R)              -> g({insert,[Tab,chkb(body(R))]});
x("DELETE",[Tab]    ,[],_)              -> g({delete,[Tab]});
x("DELETE",[Tab|Key],[],_)              -> g({delete,[Tab,Key]});
x("TRACE",_,_,_)                        -> throw({405,"method not allowed"});
x(Meth,URI,Headers,Bdy)                 -> throw({404,{Meth,URI,Headers,Bdy}}).

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

i(Str) ->
  try list_to_integer(Str)
  catch error:badarg -> throw({400,not_an_integer})
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

t10_ets_test()     -> t10(ets).
t10_leveldb_test() -> t10(leveldb).
t10(Backend) ->
  %% This test verifies that if we have multiple tables performing a
  %% next on the first table's last element won't return the second
  %% table's first element.
  restart_rets(Backend),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),

  %% Verify empty tables
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,0,next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tibbe,0,next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,0,prev)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tibbe,0,prev)),

  %% Add some records
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,a,1)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,b,2)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,c,3)),
  ?assertEqual({200,[{tebbe,2},{tibbe,1}]},
               rets_client:get(localhost)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tebbe,a)),
  ?assertEqual({200,2},
               rets_client:get(localhost,tebbe,b)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tebbe,c)),
  ?assertEqual({200,3},
               rets_client:get(localhost,tibbe,c)),

  %% Verify next
  ?assertEqual({200,[{"a",1}]},
               rets_client:get(localhost,tebbe,0,next)),
  ?assertEqual({200,[{"b",2}]},
               rets_client:get(localhost,tebbe,"a",next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,"b",next)),
  ?assertEqual({200,[{"c",3}]},
               rets_client:get(localhost,tibbe,0,next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tibbe,"c",next)),

  %% Verify prev
  ?assertEqual({200,[{"b",2}]},
               rets_client:get(localhost,tebbe,"z",prev)),
  ?assertEqual({200,[{"a",1}]},
               rets_client:get(localhost,tebbe,"b",prev)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,"a",prev)),
  ?assertEqual({200,[{"c",3}]},
               rets_client:get(localhost,tibbe,"z",prev)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tibbe,"c",prev)).

t11_ets_test()     -> t11(ets).
t11_leveldb_test() -> t11(leveldb).
t11(Backend) ->
  restart_rets(Backend),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({409,"end_of_table"},
               rets_client:put(localhost,tibbe,a,{indirect,tebbe})),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,'a/b/c',foo)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,'x/y/z',bar)),
  ?assertEqual({200,[{"a/b/c","foo"}]},
               rets_client:put(localhost,tibbe,a,{indirect,tebbe})),
  ?assertEqual({200,"a/b/c"},
               rets_client:get(localhost,tibbe,a)),
  ?assertEqual({200,[{"x/y/z","bar"}]},
               rets_client:put(localhost,tibbe,a,{indirect,tebbe})),
  ?assertEqual({200,"x/y/z"},
               rets_client:get(localhost,tibbe,a)),
  ?assertEqual({200,[{"a/b/c","foo"}]},
               rets_client:put(localhost,tibbe,a,{indirect,tebbe})),
  ?assertEqual({200,"a/b/c"},
               rets_client:get(localhost,tibbe,a)).

t12_ets_test_()     -> t12_(ets).
t12_leveldb_test_() -> t12_(leveldb).
t12_(Backend) ->
  {setup,
   %% SETUP
   fun () ->
       application:set_env(rets, keep_db, true),
       restart_rets(Backend)
   end,
   %% CLEANUP
   fun (_) ->
       application:set_env(rets, keep_db, false),
       restart_rets(Backend)
   end,
   [{atom_to_list(Backend), fun () -> t12(Backend) end}
   ]}.
t12(Backend) ->
  %% Initially the DB is empty
  ?assertEqual({200,[]},
               rets_client:get(localhost)),

  %% Add some data
  ?assertEqual({200, true},
               rets_client:put(localhost,tebbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,a,1)),
  ?assertEqual({200,[{tebbe,1}]},
               rets_client:get(localhost)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tebbe,a)),

  %% Restart rets when keep_db is set
  restart_rets(Backend),
  
  %% Verify the data stayed
  ?assertEqual({200,[{tebbe,1}]},
               rets_client:get(localhost)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tebbe,a)),
  
  %% Restart rets when keep_db is not set
  application:set_env(rets, keep_db, false),
  restart_rets(Backend),
  
  %% Verify the data vanished
  ?assertEqual({200,[]},
               rets_client:get(localhost)).

t13_ets_test_() ->
  {setup,
   %% SETUP
   fun () ->
       application:set_env(rets, keep_db, true),
       restart_rets(ets)
   end,
   %% CLEANUP
   fun (_) ->
       application:set_env(rets, keep_db, false),
       restart_rets(ets)
   end,
   [fun t13/0
   ]}.
t13() ->
  %% Create two tables
  ?assertEqual({200, true},
               rets_client:put(localhost,tebbe)),
  ?assertEqual({200, true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,[{tebbe,0},{tibbe,0}]},
               rets_client:get(localhost)),
  
  %% After restart: both tables & the index should be saved to disk
  restart_rets(ets),
  Files1 = file:list_dir("/tmp/rets/db"),
  ?assertMatch({ok, _}, Files1),
  ?assertEqual(["idx.term", "tebbe.tab","tibbe.tab"],
               lists:sort(element(2, Files1))),
  
  %% Delete one of the tables
  ?assertEqual({200,true},
               rets_client:delete(localhost,tibbe)),
  ?assertEqual({200,[{tebbe,0}]},
               rets_client:get(localhost)),
  
  %% After restart: only one table & the index should be saved to disk
  restart_rets(ets),
  Files2 = file:list_dir("/tmp/rets/db"),
  ?assertMatch({ok, _}, Files2),
  ?assertEqual(["idx.term", "tebbe.tab"],
               lists:sort(element(2, Files2))).

restart_rets(Backend) ->
  application:stop(rets),
  %% Ranch opens the server socket in a supervisor and never
  %% explicitly closes it. So the socket is closed "shortly after" the
  %% supervisor process terminates. But if we are not lucky and try to
  %% restart very quickly we may get an `eaddrinuse'. So let's just
  %% wait a little bit here.
  timer:sleep(5),
  {ok,_} = start(Backend).

-endif. % TEST
