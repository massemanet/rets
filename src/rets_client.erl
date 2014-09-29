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

get(Host) ->
  get(Host,"").
get(Host,Tab) ->
  get(Host,Tab,"").
get(Host,Tab,Key) ->
  get(Host,Tab,Key,[],[]).
get(Host,Tab,Key,next) ->
  get(Host,Tab,Key,[{"next","true"}],[]);
get(Host,Tab,Key,prev) ->
  get(Host,Tab,Key,[{"prev","true"}],[]);
get(Host,Tab,Key,multi) ->
  get(Host,Tab,Key,[{"multi","true"}],[]).

%% internal
get(Host,Tab,Key,Headers,Opts) ->
  case httpc_request(get,Host,Tab,Key,Headers) of
    {200,Reply} -> {200,maybe_atomize(unprep(dec(Reply)),[no_atoms|Opts])};
    Error       -> Error
  end.

delete(Host,Tab) ->
  delete(Host,Tab,"").
delete(Host,Tab,Key) ->
  httpc_request(delete,Host,Tab,Key,[]).

put(Host,Tab) ->
  put(Host,Tab,"",[]).
put(Host,Tab,Key,counter) ->
  put(Host,Tab,Key,[{"counter","true"}],[],[integerize]);
put(Host,Tab,Key,reset) ->
  put(Host,Tab,Key,[{"reset","true"}],[],[integerize]);
put(Host,Tab,Key,PL) ->
  put(Host,Tab,Key,[],PL,[]).

%% internal
put(Host,Tab,Key,Headers,PL,Opts) ->
  case httpc_request(put,Host,Tab,Key,Headers,enc(prep(PL))) of
    {200,Reply} -> {200,maybe_integerize(Reply,Opts)};
    Error       -> Error
  end.

post(Host,Tab,PL) ->
  httpc_request(post,Host,Tab,"",[],enc(prep(PL))).

trace(Host) ->
  trace(Host,[]).
trace(Host,Headers) ->
  httpc_request(trace,Host,"","",Headers).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

httpc_request(M,Host,Tab,Key,Headers) ->
  httpc_request(M,Host,Tab,Key,Headers,[]).
httpc_request(M,Host,Tab,Key,Headers,PL) ->
  start_app(inets),
  case httpc_request(M,url(Host,Tab,Key),Headers,PL) of
    {ok,{{_HttpVersion,Status,_StatusText},_Headers,Reply}} ->
      {Status,Reply};
    Error->
      Error
  end.

httpc_request(M,URL,Headers,[]) when M==trace; M==get; M==delete ->
  httpc:request(M,{URL,Headers},[],[]);
httpc_request(M,URL,Headers,PL) when M==post; M==put ->
  httpc:request(M,{URL,Headers,[],PL},[],[]).

start_app(M) ->
  [M:start() || false=:=lists:keysearch(M,1,application:which_applications())].

url(Host,Tab,Key) ->
  "http://"++to_list(Host)++":7890/"++to_list(Tab)++"/"++to_list(Key).

%% unwrap proplists from {} from jiffy
unprep({PL} = {[{_,_}|_]}) -> [{unprep(K),unprep(V)}||{K,V}<-PL];
unprep(L) when is_list(L) -> [unprep(E)||E<-L];
unprep(T) when is_tuple(T) -> list_to_tuple([unprep(E)||E<-tuple_to_list(T)]);
unprep(X) -> X.

%% wrap proplists in {} for jiffy
-define(is_string(S), S=="";is_integer(hd(S))).
prep(true)                 -> true;
prep(false)                -> false;
prep(null)                 -> null;
prep(X) when is_binary(X)  -> X;
prep(X) when is_number(X)  -> X;
prep(X) when is_atom(X)    -> list_to_binary(atom_to_list(X));
prep(X) when ?is_string(X) -> list_to_binary(X);
prep(PL = [{_,_}|_])       -> {[{prep(K),prep(V)}||{K,V}<-PL]};
prep(L) when is_list(L)    -> [prep(E)||E<-L];
prep(T) when is_tuple(T)   -> list_to_tuple([prep(E)||E<-tuple_to_list(T)]).


to_list(X) when is_binary(X) -> binary_to_list(X);
to_list(X) when is_list(X)   -> X;
to_list(X) when is_integer(X)-> integer_to_list(X);
to_list(X) when is_atom(X)   -> atom_to_list(X).

maybe_integerize(Term,Opts) ->
  case lists:member(integerize,Opts) of
    true -> list_to_integer(Term);
    false-> Term
  end.

maybe_atomize(Term,Opts) ->
  case lists:member(no_atoms,Opts) of
    true -> Term;
    false-> atomize(Term)
  end.

atomize(L) when is_list(L)   -> [atomize(E) || E <- L];
atomize(T) when is_tuple(T)  -> list_to_tuple(atomize(tuple_to_list(T)));
atomize(B) when is_binary(B) ->
  try list_to_atom(assert_printable(binary_to_list(B)))
  catch _:_ -> B
  end;
atomize(X) -> X.

assert_printable(L) ->
  lists:map(fun(C) when $ =< C,C =< $~ -> C end,L).

enc(Term) ->
  jiffy:encode(Term).

dec(Term) ->
  jiffy:decode(Term).
