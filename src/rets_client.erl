%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 29 Jan 2014 by mats cronqvist <masse@klarna.com>

%% @doc
%% @end

-module('rets_client').
-author('mats cronqvist').
-export([get/1,get/2,get/3,get/4,
         delete/2,delete/3,
         put/2,put/4,
         post/3,
         trace/1,trace/2]).

-define(is_string(S), S=="";is_integer(hd(S))).
-define(http_timeout, infinity).
-define(http_opts,    [{max_connections, 10000}]).

get(Host) ->
  atomize(get(Host,"")).
get(Host,Tab) ->
  get(Host,Tab,"").
get(Host,Tab,Key) ->
  get(Host,Tab,Key,[],[]).
get(Host,Tab,Key,next) ->
  get(Host,Tab,Key,[{"rets","next"}],[]);
get(Host,Tab,Key,prev) ->
  get(Host,Tab,Key,[{"rets","prev"}],[]);
get(Host,Tab,Key,multi) ->
  get(Host,Tab,Key,[{"rets","multi"}],[]).

%% internal
get(Host,Tab,Key,Headers,[]) ->
  httpc_request(get,Host,Tab,Key,Headers).

delete(Host,Tab) ->
  delete(Host,Tab,"").
delete(Host,Tab,Key) ->
  httpc_request(delete,Host,Tab,Key,[]).

put(Host,Tab) ->
  put(Host,Tab,"",[]).
put(Host,Tab,Key,{counter,L,H}) ->
  put(Host,Tab,Key,[{"rets",string:join(["counter",s(L),s(H)],",")}],[],[]);
put(Host,Tab,Key,counter) ->
  put(Host,Tab,Key,[{"rets","counter"}],[],[]);
put(Host,Tab,Key,reset) ->
  put(Host,Tab,Key,[{"rets","reset"}],[],[]);
put(Host,Tab,Key,PL) ->
  put(Host,Tab,Key,[],PL,[]).

%% internal
put(Host,Tab,Key,Headers,PL,[]) ->
  httpc_request(put,Host,Tab,Key,Headers,PL).

post(Host,Tab,PL) ->
  httpc_request(post,Host,Tab,"",[],PL).

trace(Host) ->
  trace(Host,[]).
trace(Host,Headers) ->
  httpc_request(trace,Host,"","",Headers).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

httpc_request(M,Host,Tab,Key,Headers) ->
  httpc_request(M,Host,Tab,Key,Headers,[]).
httpc_request(M,Host,Tab,Key,Headers,PL) ->
  start_app(lhttpc),
  case httpc_request(M,url(Host,Tab,Key),Headers,enc(prep(PL))) of
    {ok,{{Status,_StatusText},_Headers,Reply}} ->
      case Status of
        200 -> {200,unprep(dec(Reply))};
        _   -> {Status,binary_to_list(Reply)}
      end;
    Error->
      Error
  end.

httpc_request(M,URL,Headers,<<"\"\"">>) when M==trace; M==get; M==delete ->
  lhttpc:request(URL, M, Headers, [], ?http_timeout, ?http_opts);
httpc_request(M,URL,Headers,PL) when M==post; M==put ->
  lhttpc:request(URL, M, Headers, PL, ?http_timeout, ?http_opts).

start_app(M) ->
  [M:start() || false=:=lists:keysearch(M,1,application:which_applications())].

url(Host,Tab,Key) ->
  Prot = "http",
  Port = "7890",
  Prot++"://"++to_list(Host)++":"++Port++"/"++to_list(Tab)++"/"++to_list(Key).

to_list(X) when ?is_string(X) -> X;
to_list(X) when is_binary(X)  -> binary_to_list(X);
to_list(X) when is_integer(X) -> integer_to_list(X);
to_list(X) when is_atom(X)    -> atom_to_list(X).

%% convert from jiffy -> normal erlang
%% binary() -> string() and {proplist()} -> proplist()
unprep({PL} = {[{_,_}|_]})  -> [{unprep(K),unprep(V)}||{K,V}<-PL];
unprep(L) when is_list(L)   -> [unprep(E)||E<-L];
unprep(T) when is_tuple(T)  -> list_to_tuple([unprep(E)||E<-tuple_to_list(T)]);
unprep(X) when is_binary(X) -> unicode:characters_to_list(X,utf8);
unprep(X)                   -> X.

%% convert from normal erlang -> jiffy
%% string()|atom() -> binary(), proplist() -> {proplist()}
%% true, false, null and number() are left as is
prep(true)                 -> true;
prep(false)                -> false;
prep(null)                 -> null;
prep(X) when is_binary(X)  -> X;
prep(X) when is_number(X)  -> X;
prep(X) when is_atom(X)    -> list_to_binary(atom_to_list(X));
prep(X) when ?is_string(X) -> unicode:characters_to_binary(X,unicode,utf8);
prep(PL = [{_,_}|_])       -> {[{prep(K),prep(V)}||{K,V}<-PL]};
prep(L) when is_list(L)    -> [prep(E)||E<-L];
prep(T) when is_tuple(T)   -> list_to_tuple([prep(E)||E<-tuple_to_list(T)]).

atomize(X) ->
  ize(X,fun(Y) -> list_to_existing_atom(Y) end).

ize([],_) -> [];
ize(S,IZE) when ?is_string(S) ->
  try IZE(S)
  catch _:_ -> S
  end;
ize(L,IZE) when is_list(L)        -> [ize(E,IZE) || E <- L];
ize({I,R},IZE) when is_integer(I) -> {I,ize(R,IZE)};
ize(T,IZE) when is_tuple(T)       -> list_to_tuple(ize(tuple_to_list(T),IZE));
ize(X,_) -> X.

enc(Term) ->
  jiffy:encode(Term).

dec(Term) ->
  jiffy:decode(Term).

s(Integer) ->
  integer_to_list(Integer).
