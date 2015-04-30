%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 29 Jan 2014 by mats cronqvist <masse@klarna.com>

%% @doc
%% @end

-module('rets_client').
-author('mats cronqvist').
-export([get/1,get/2,get/3,
         delete/2,
         put/3,
         post/2,
         trace/1,trace/2]).

-define(is_string(S), S=="";is_integer(hd(S))).

get(Host) ->
  get(Host,"").
get(Host,Key) ->
  get(Host,Key,single).
get(Host,Key,Header) ->
  case lists:member(Header,[single,multi,next,prev,keys]) of
    true -> httpc_request(get,Host,Key,[{"rets",atom_to_list(Header)}],[]);
    false-> exit({unrecognized_header,Header})
  end.

delete(Host,Key) ->
  httpc_request(delete,Host,Key,[]).

put(Host,Key,counter) ->
  put(Host,Key,[{"rets","counter"}],[]);
put(Host,Key,reset) ->
  put(Host,Key,[{"rets","reset"}],[]);
put(Host,Key,PL) ->
  put(Host,Key,[],PL).

%% internal
put(Host,Key,Headers,PL) ->
  httpc_request(put,Host,Key,Headers,PL).

post(Host,PL) ->
  post(Host,PL,[]).

%% internal
post(Host,PL,Headers) ->
  httpc_request(post,Host,"",Headers,PL).

trace(Host) ->
  trace(Host,[]).
trace(Host,Headers) ->
  httpc_request(trace,Host,"","",Headers).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

httpc_request(M,Host,Key,Headers) ->
  httpc_request(M,Host,Key,Headers,[]).
httpc_request(M,Host,Key,Headers,PL) ->
  start_app(inets),
  case httpc_req(M,url(Host,Key),Headers,enc(prep(PL))) of
    {ok,{{_HttpVersion,Status,_StatusText},_Headers,Reply}} ->
      case Status of
        200 -> {200,unprep(dec(Reply))};
        _   -> {Status,Reply}
      end;
    Error->
      Error
  end.

httpc_req(M,URL,Headers,<<"\"\"">>) when M==trace; M==get; M==delete ->
  httpc:request(M,{URL,Headers},[],[]);
httpc_req(M,URL,Headers,PL) when M==post; M==put ->
  httpc:request(M,{URL,Headers,[],PL},[],[]).

start_app(M) ->
  [M:start() || false=:=lists:keysearch(M,1,application:which_applications())].

url(Host,Key) ->
  Prot = "http",
  Port = "7890",
  Prot++"://"++to_list(Host)++":"++Port++"/"++to_list(Key).

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

enc(Term) ->
  jiffy:encode(Term).

dec(Term) ->
  jiffy:decode(Term).
