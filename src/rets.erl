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

reply(400,R) -> flat(R);
reply(404,R) -> flat(R);
reply(405,R) -> flat(R);
reply(409,R) -> flat(R);
reply(500,R) -> flat({R,erlang:get_stacktrace()}).

reply(Req) ->
  x(method(Req),mk_ekey(uri(Req)),rets_headers(Req),Req).

x("GET"   ,Key,["gauge"],_)  -> g([chk_bp(r,["gauge",Key])]);
x("GET"   ,Key,["keys"] ,_)  -> g([chk_bp(r,["keys",Key])]);
x("GET"   ,Key,["next"] ,_)  -> g([chk_bp(r,["next",Key])]);
x("GET"   ,Key,["prev"] ,_)  -> g([chk_bp(r,["prev",Key])]);
x("GET"   ,Key,["multi"],_)  -> g([chk_bp(r,["multi",Key])]);
x("GET"   ,Key,["single"],_) -> g([chk_bp(r,["single",Key])]);
x("GET"   ,Key,[]        ,_) -> g([chk_bp(r,["single",Key])]);

x("PUT"   ,Key,[]       ,R)  -> g([chk_bp(w,["insert",Key,dbl(body(R))])]);
x("PUT"   ,Key,["force"],R)  -> g([chk_bp(w,["insert",Key,body(R)])]);
x("PUT"   ,Key,["gauge"],_)  -> g([chk_bp(w,["mk_gauge",Key])]);
x("PUT"   ,Key,["bump"] ,_)  -> g([chk_bp(w,["bump",Key])]);
x("PUT"   ,Key,["reset"],_)  -> g([chk_bp(w,["reset",Key])]);

x("DELETE",Key,[]       ,R)  -> g([chk_bp(w,["delete",Key,body(R)])]);
x("DELETE",Key,["gauge"],_)  -> g([chk_bp(w,["del_gauge",Key])]);
x("DELETE",Key,["force"],_)  -> g([chk_bp(w,["delete",Key])]);

x("POST"  ,[] ,["write"],R)  -> g(chk_body(w,body(R)));
x("POST"  ,[] ,["read"] ,R)  -> g(chk_body(r,body(R)));

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

dbl([V,OV]) -> {V,OV}.

method(Req) ->
  {Method,_} = cowboy_req:method(Req),
  string:to_upper(binary_to_list(Method)).

uri(Req) ->
  {URI,_} = cowboy_req:path(Req),
  URI.

g(FArgs) ->
  case gen_server:call(rets_handler,FArgs) of
    {ok,Reply} -> Reply;
    {Status,R} -> throw({Status,R})
  end.

chk_body(RorW,Body) ->
  lists:sort(lists:map(fun(E) -> chk_bp(RorW,E) end,Body)).

chk_bp(r,[<<"single">>,K])        -> emit_bp(r,single,K);
chk_bp(r,[<<"multi">>,K])         -> emit_bp(r,multi,K);
chk_bp(r,[<<"next">>,K])          -> emit_bp(r,next,K);
chk_bp(r,[<<"prev">>,K])          -> emit_bp(r,prev,K);
chk_bp(r,[<<"gauge">>,K])         -> emit_bp(r,gauge,K);
chk_bp(r,[<<"keys">>,K])          -> emit_bp(r,keys,K);
chk_bp(w,[<<"insert">>,K,{V,OV}]) -> chk_bp(w,[<<"insert">>,K,V,OV]);
chk_bp(w,[<<"insert">>,K,V])      -> chk_bp(w,[<<"insert">>,K,V,force]);
chk_bp(w,[<<"delete">>,K])        -> chk_bp(w,[<<"delete">>,K,force]);
chk_bp(w,[<<"insert">>,K,V,OV])   -> emit_bp(w,insert,K,{V,OV});
chk_bp(w,[<<"bump">>,K])          -> emit_bp(w,bump,K,force);
chk_bp(w,[<<"reset">>,K])         -> emit_bp(w,reset,K,force);
chk_bp(w,[<<"mk_gauge">>,K])      -> emit_bp(w,mk_gauge,K,force);
chk_bp(w,[<<"del_gauge">>,K])     -> emit_bp(w,del_gauge,K,force);
chk_bp(w,[<<"delete">>,K,V])      -> emit_bp(w,delete,K,V);
chk_bp(RorW,What)                 -> throw({400,{bad_request,{RorW,What}}}).

emit_bp(r,Op,K)   -> {chk_key(r,mk_ekey(K)),Op}.
emit_bp(w,Op,K,V) -> {chk_key(w,mk_ekey(K)),Op,V}.

mk_ekey(Key) -> string:tokens(binary_to_list(Key),"/").

chk_key(RorW,Key) -> lists:map(fun(E) -> chkk_el(RorW,E) end,Key).
chkk_el(w,".") -> throw({400,key_element_is_period});
chkk_el(r,".") -> ".";
chkk_el(_,El)  -> lists:map(fun good_char/1,El).

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
  ?assertEqual({200,null},
               rets_client:post(localhost,
                                [[insert,'aaa/1/x',"AAA"++[223]],
                                 [insert,bbb,bBbB],
                                 [insert,ccc,123.1],
                                 [insert,ddd,[{a,"A"},{b,b},{c,123.3}]]],
                                write)),
  ?assertEqual({200,[{"aaa/1/x",[$A,$A,$A,223]},
                     [$A,$A,$A,223],
                     bBbB,
                     [{ccc,123.1}]]},
               rets_client:post(localhost,"",
                               [[multi,'aaa/./x'],
                                [single,'aaa/1/x'],
                                [single,bbb],
                                [multi,ccc]],
                               read)).


restart_rets(Backend) ->
  application:stop(rets),
  {ok,_} = start(Backend).

-endif. % TEST
