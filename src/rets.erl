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

x("GET"   ,[KeyW],["gauge"],_) -> g({gauge,    [KeyW]});
x("GET"   ,[KeyW],["keys"] ,_) -> g({keys,     [KeyW]});
x("GET"   ,[KeyW],["next"] ,_) -> g({next,     [KeyW]});
x("GET"   ,[KeyW],["prev"] ,_) -> g({prev,     [KeyW]});
x("GET"   ,[KeyW],["multi"],_) -> g({multi,    [KeyW]});
x("GET"   ,[KeyW],_        ,_) -> g({single,   [KeyW]});

x("PUT"   ,[Key] ,["gauge"],_) -> g({mk_gauge, [Key]});
x("PUT"   ,[Key] ,["force"],R) -> g({force_ins,[chkk(Key),body(R)]});
x("PUT"   ,[Key] ,[]       ,R) -> g({insert,   [chkk(Key),body(R)]});
x("PUT"   ,[Key] ,["bump"] ,_) -> g({bump,     [chkk(Key),1]});
x("PUT"   ,[Key] ,["reset"],_) -> g({reset,    [chkk(Key),0]});

x("POST"  ,[]    ,["write"],R) -> g({write_ops,[chkb(body(R))]});
x("POST"  ,[]    ,["read"] ,R) -> g({read_ops, [chkb(body(R))]});

x("DELETE",[Key] ,["gauge"],_) -> g({del_gauge,[Key]});
x("DELETE",[Key] ,["force"],_) -> g({force_del,[Key]});
x("DELETE",[Key] ,[]       ,R) -> g({delete,   [Key,body(R)]});

x("TRACE" ,_     ,_        ,_) -> throw({405,"method not allowed"});
x(Meth    ,URI   ,Headers  ,_) -> throw({404,{Meth,URI,Headers}}).

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

%t00_ets_test()     -> t00(ets).
t00_leveldb_test() -> t00(leveldb).
t00(Backend) ->
  restart_rets(Backend),
  ?assertEqual({200,null},
               rets_client:post(localhost,
                                [{'aaa/1/x',"AAA"++[223]},
                                 {bbb,bBbB},
                                 {ccc,123.1},
                                 {ddd,[{a,"A"},{b,b},{c,123.3}]}])),
  ?assertEqual({200,[{"aaa/1/x",[$A,$A,$A,223]},
                     [$A,$A,$A,223],
                     bBbB,
                     [{ccc,123.1}]]},
               rets_client:post(localhost,"",[],
                               [{multi,'aaa/./x'},
                                {single,'aaa/1/x'},
                                {single,bbb},
                                {multi,ccc}])).


restart_rets(Backend) ->
  application:stop(rets),
  {ok,_} = start(Backend).

-endif. % TEST
