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
x("GET"   ,Key,["gauge"],_)  -> g([{gauge,Key}]);
x("GET"   ,Key,["keys"] ,_)  -> g([{keys,Key}]);
x("GET"   ,Key,["next"] ,_)  -> g([{next,Key}]);
x("GET"   ,Key,["prev"] ,_)  -> g([{prev,Key}]);
x("GET"   ,Key,["multi"],_)  -> g([{multi,Key}]);
x("GET"   ,Key,["single"],_) -> g([{single,Key}]);
x("GET"   ,Key,[]        ,_) -> g([{single,Key}]);

x("PUT"   ,Key,[]       ,R)  -> g([{insert,Key,body(R)}]);
x("PUT"   ,Key,["force"],R)  -> g([{insert,Key,{body(R),force}}]);
x("PUT"   ,Key,["gauge"],_)  -> g([{mk_gauge,Key}]);
x("PUT"   ,Key,["bump"] ,_)  -> g([{bump,Key}]);
x("PUT"   ,Key,["reset"],_)  -> g([{reset,Key}]);

x("DELETE",Key,[]       ,R)  -> g([{delete,Key,body(R)}]);
x("DELETE",Key,["gauge"],_)  -> g([{del_gauge,Key}]);
x("DELETE",Key,["force"],_)  -> g([{delete,Key,force}]);

x("POST"  ,_  ,[] ,R)        -> g([list_to_tuple(E) || E <- body(R)]);

x("TRACE" ,_    ,_        ,_)  -> throw({405,"method not allowed"});
x(Meth    ,URI  ,Headers  ,_)  -> throw({404,{Meth,URI,Headers}}).

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
  {URI,_} = cowboy_req:path(Req),
  URI.

g(Ops) ->
  {F,ValidatedOps} = chk_ops(Ops),
  case gen_server:call(rets_handler,{F,ValidatedOps}) of
    {ok,Reply} -> Reply;
    {Status,R} -> throw({Status,R})
  end.

%% validate operations
%% State is {r|w,Key,[ops()]}
chk_ops(Ops) ->
  {F,_,Rops} = lists:foldl(fun chk_op/2,{'',<<>>,[]},lists:keysort(2,Ops)),
  {F,lists:reverse(Rops)}.

chk_op({<<"insert">>,K,{V,OV}},S) -> chk_op({<<"insert">>,K,V,OV},S);
chk_op({<<"insert">>,K,V},S)      -> chk_op({<<"insert">>,K,V,force},S);
chk_op({<<"delete">>,K},S)        -> chk_op({<<"delete">>,K,force},S);
chk_op({<<"single">>,K},S)        -> emit_r_op(single,K,S);
chk_op({<<"multi">>,K},S)         -> emit_r_op(multi,K,S);
chk_op({<<"next">>,K},S)          -> emit_r_op(next,K,S);
chk_op({<<"prev">>,K},S)          -> emit_r_op(prev,K,S);
chk_op({<<"gauge">>,K},S)         -> emit_r_op(gauge,K,S);
chk_op({<<"keys">>,K},S)          -> emit_r_op(keys,K,S);
chk_op({<<"insert">>,K,V,OV},S)   -> emit_w_op(insert,K,{V,OV},S);
chk_op({<<"bump">>,K},S)          -> emit_w_op(bump,K,force,S);
chk_op({<<"reset">>,K},S)         -> emit_w_op(reset,K,force,S);
chk_op({<<"mk_gauge">>,K},S)      -> emit_w_op(mk_gauge,K,force,S);
chk_op({<<"del_gauge">>,K},S)     -> emit_w_op(del_gauge,K,force,S);
chk_op({<<"delete">>,K,V},S)      -> emit_w_op(delete,K,V,S);
chk_op(What,_S)                   -> throw({400,{bad_op,What}}).

emit_r_op(Op,K,{w,_,_}) -> throw({400,{mixed_read_write_ops,Op,K}});
emit_r_op(Op,K,{_,_,A}) -> chk_key(r,K), {r,K,[{Op,K}|A]}.

emit_w_op(Op,K,_,{r,_,_}) -> throw({400,{mixed_read_write_ops,Op,K}});
emit_w_op(Op,K,_,{w,K,_}) -> throw({400,{key_appears_twice,Op,K}});
emit_w_op(Op,K,V,{_,_,A}) -> chk_key(w,K), {w,K,[{Op,K,V}|A]}.

chk_key(RorW,Key) -> lists:foreach(fun(E) -> chkk_el(RorW,E) end,mk_ekey(Key)).
chkk_el(w,".") -> throw({400,key_element_is_period});
chkk_el(r,".") -> ok;
chkk_el(_,El)  -> lists:foreach(fun good_char/1,El).

mk_ekey(Key) -> string:tokens(binary_to_list(Key),"/").

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

restart_rets(Backend) ->
  application:stop(rets),
  {ok,_} = start(Backend).

-endif. % TEST
