%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 27 Jun 2012 by mats cronqvist <masse@klarna.com>

%% @doc
%% a rest wrapper around ets
%% @end

-module(rets).
-author('mats cronqvist').
-export([ start/0,       % start the application
          start_link/0   % supervisor callback
         ,do/2]).        % inets mod_fun callback

-include_lib("eunit/include/eunit.hrl").

%% main application starter
start() ->
  application:start(rets).

%% supervisor callback
start_link() ->
  [inets:start() || not is_started(inets)],
  inets:start(httpd,conf(),stand_alone),
  rets_tables:start_link().

conf() ->
  Root = filename:join("/tmp",?MODULE),
  [{port, 8765},
   {server_name,atom_to_list(?MODULE)},
   {server_root,ensure(Root)},
   {document_root,ensure(Root)},
   {modules, [mod_alias,mod_fun,mod_log]},
   {error_log,filename:join(ensure(Root),"errors.log")},
   {handler_function,{?MODULE,do}},
   {mime_types,[{"html","text/html"},
                {"css","text/css"},
                {"ico","image/x-icon"},
                {"js","application/javascript"}]}].

is_started(A) ->
  lists:member(A,[X || {X,_,_} <- application:which_applications()]).

ensure(X) ->
  filelib:ensure_dir(X++"/"),
  X.

%% called from mod_fun. runs in a fresh process.
%% Req is a dict with the request data from inets. It is implemented
%% as a fun/1, with the arg being the key in the dict.
%% we can deliver the content in chunks by calling Act(Chunk).
%% the first chunk can be headers; [{Key,Val}]
%% if we don't want to handle the request, we do Act(defer)
%% if we crash, there will be a 404.
do(Act,Req) ->
  URI = string:tokens(Req(request_uri),"/"),
  HeaderTrue = [K || {K,"true"} <- Req(parsed_header)],
  case {Req(method),URI,HeaderTrue} of
    {"GET",   []       ,[]} -> Act(je([l2b(T)||T<-gcall({all,[]})]));
    {"GET",   [Tab]    ,[]} -> Act(je(ets({keys,Tab})));
    {"GET",   [Tab,Key],[]} -> Act(ets({get,Tab,Key}));
    {"PUT",   [Tab]    ,[]} -> Act(je(gcall({create,Tab})));
    {"PUT",   [Tab,Key],["counter"]} -> Act(ets({counter,Tab,Key}));
    {"PUT",   [Tab,Key],["reset"]}   -> Act(ets({reset,Tab,Key}));
    {"PUT",   [Tab,Key],[]} -> Act(je(ets({insert,Tab,Key,Req(entity_body)})));
    {"POST",  [Tab]    ,[]} -> Act(je(ets({insert,Tab,Req(entity_body)})));
    {"DELETE",[Tab]    ,[]} -> Act(je(gcall({delete,Tab})));
    {"DELETE",[Tab,Key],[]} -> Act(je(ets({delete,Tab,Key})));
    _                       -> Act(io_lib:format("~p",[Req(all)]))
  end.

ets({keys,Tab})       -> ets:foldr(fun({K,_},A)->[K|A]end,[],l2ea(Tab));
ets({insert,Tab,K,V}) -> ets:insert(l2ea(Tab),{l2b(K),l2b(V)});
ets({insert,Tab,KVs}) -> ets:insert(l2ea(Tab),unpack(KVs));
ets({counter,Tab,Key})-> update_counter(l2ea(Tab),l2b(Key));
ets({reset,Tab,Key})  -> ets:insert(l2ea(Tab),{l2b(Key),0}),"0";
ets({get,Tab,Key})    -> getter(l2ea(Tab),l2b(Key));
ets({delete,Tab,Key}) -> ets:delete(l2ea(Tab),l2b(Key)).

getter(Tab,Key) ->
  [{Key,Res}] = ets:lookup(Tab,Key),
  case is_integer(Res) of
    true -> integer_to_list(Res);
    false-> Res
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

l2ea(L) ->
  list_to_existing_atom(L).

gcall(What) ->
  gen_server:call(rets_tables,What).

je(Term) ->
  jiffy:encode(Term).

%%%%%%%%%%
%% eunit
t00_test() ->
  application:stop(rets),
  application:start(rets),
  rets_client:put(localhost,tibbe),
  rets_client:post(localhost,tibbe,[{aaa,"AAA"},{bbb,bBbB},{ccc,123.1},
                                    {ddd,[{a,"A"},{b,b},{c,123.3}]}]),
  ?assertEqual(rets_client:get(localhost,tibbe,aaa),
               {200,"AAA"}),
  ?assertEqual(rets_client:get(localhost,tibbe,bbb),
               {200,bBbB}),
  ?assertEqual(rets_client:get(localhost,tibbe,ccc),
               {200,123.1}),
  ?assertEqual(rets_client:get(localhost,tibbe,ddd),
               {200,[{a,"A"},{b,b},{c,123.3}]}).

t01_test() ->
  application:stop(rets),
  application:start(rets),
  rets_client:put(localhost,tibbe),
  rets_client:put(localhost,tibbe,aaa,"AAA"),
  rets_client:put(localhost,tibbe,bbb,bBbB),
  rets_client:put(localhost,tibbe,ccc,123.1),
  rets_client:put(localhost,tibbe,ddd,[{a,"A"},{b,b},{c,123.3}]),
  ?assertEqual(rets_client:get(localhost,tibbe,aaa),
               {200,"AAA"}),
  ?assertEqual(rets_client:get(localhost,tibbe,bbb),
               {200,bBbB}),
  ?assertEqual(rets_client:get(localhost,tibbe,ccc),
               {200,123.1}),
  ?assertEqual(rets_client:get(localhost,tibbe,ddd),
               {200,[{a,"A"},{b,b},{c,123.3}]}).

t02_test() ->
  application:stop(rets),
  application:start(rets),
  rets_client:put(localhost,tibbe),
  rets_client:put(localhost,tibbe,bbb,bBbB),
  rets_client:put(localhost,tibbe,ddd,[{a,"A"},{b,b},{c,123.3}]),
  ?assertEqual(rets_client:get(localhost,tibbe,bbb,[no_atoms]),
               {200,<<"bBbB">>}),
  ?assertEqual(rets_client:get(localhost,tibbe,ddd,[no_atoms]),
               {200,[{<<"a">>,"A"},{<<"b">>,<<"b">>},{<<"c">>,123.3}]}).

t03_test() ->
  application:stop(rets),
  application:start(rets),
  rets_client:put(localhost,tibbe),
  ?assertEqual({200,1}, rets_client:put(localhost,tibbe,bbb,counter)),
  ?assertEqual({200,2}, rets_client:put(localhost,tibbe,bbb,counter)),
  ?assertEqual({200,2}, rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,0}, rets_client:put(localhost,tibbe,bbb,reset)),
  ?assertEqual({200,1}, rets_client:put(localhost,tibbe,bbb,counter)).
