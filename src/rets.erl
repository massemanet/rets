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
   {opts,[{port, cowboy_port()}]},
   {routes,[{'_', [{"/[...]", rets, []}]}]}].

cowboy_port() ->
  case application:get_env(rets,port_number) of
    {ok,Port} -> Port;
    undefined -> 7890
  end.

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

x("PUT",   [Tab]    ,[],_)             -> g({create,[Tab]});
x("PUT",   [Tab|Key],["indirect",T],_) -> g({via,   [Tab,chkk(Key),T]});
x("PUT",   [Tab|Key],[],R)             -> g({insert,[Tab,chkk(Key),body(R)]});
x("PUT",   [Tab|Key],["counter"],_)    -> g({bump,  [Tab,chkk(Key),1]});
x("PUT",   [Tab|Key],["counter",L,H],_)-> g({bump,  [Tab,chkk(Key),i(L),i(H)]});
x("PUT",   [Tab|Key],["reset"],_)      -> g({reset, [Tab,chkk(Key),0]});
x("PUT",   _        ,Headers,_)        -> throw({405,{"bad header",Headers}});
x("GET",   []       ,[],_)             -> g({sizes, []});
x("GET",   [Tab]    ,[],_)             -> g({keys,  [Tab]});
x("GET",   [Tab|Key],[],_)             -> g({single,[Tab,Key]});
x("GET",   [Tab|Key],["multi"],_)      -> g({multi, [Tab,Key]});
x("GET",   [Tab|Key],["next"],_)       -> g({next,  [Tab,Key]});
x("GET",   [Tab|Key],["prev"],_)       -> g({prev,  [Tab,Key]});
x("GET",   _        ,Headers,_)        -> throw({405,{"bad header",Headers}});
x("POST",  [Tab]    ,[],R)             -> g({insert,[Tab,chkb(body(R))]});
x("DELETE",[Tab]    ,[],_)             -> g({delete,[Tab]});
x("DELETE",[Tab|Key],[],_)             -> g({delete,[Tab,Key]});
x("TRACE",_,_,_)                       -> throw({405,"method not allowed"});
x(Meth,URI,Headers,Bdy)                -> throw({404,{Meth,URI,Headers,Bdy}}).

g(FArgs) ->
  case gen_server:call(rets_handler,FArgs) of
    {ok,Reply} -> Reply;
    Bad        -> throw(Bad)
  end.

chkb({Body}) -> lists:map(fun chk_bp/1,Body).
chk_bp({Key,Val}) ->
  {chkk(string:tokens(binary_to_list(Key),"/")),Val}.

chkk(Key) -> lists:map(fun chk_el/1,Key).
chk_el("_") -> throw({404,key_element_is_single_underscore});
chk_el(El)  -> El.

i(Str) ->
  try list_to_integer(Str)
  catch error:badarg -> throw({404,not_an_integer,Str})
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
