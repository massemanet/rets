%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 29 Jan 2014 by mats cronqvist <masse@klarna.com>

%% @doc
%% @end

-module('rets_client').
-author('mats cronqvist').
-export([get/2,get/3,get/4,delete/2,delete/3,put/2,put/4,post/3]).

get(Host,Tab) ->
  get(Host,Tab,"").
get(Host,Tab,Key) ->
  get(Host,Tab,Key,[]).
get(Host,Tab,Key,Opts) ->
  case httpc:request(get,{url(Host,Tab,Key),[]},[],[]) of
    {ok,{{_HttpVersion,200,_StatusText},_Headers,Reply}} ->
      case dec(Reply) of
        {PL} -> {200,maybe_atomize(PL,Opts)};
        X    -> {200,maybe_atomize(X,Opts)}
      end;
    {ok,{{_HttpVersion,Status,StatusText},_Headers,Reply}} ->
      {error, {Status,StatusText,Reply}};
    Error ->
      Error
  end.

delete(Host,Tab) ->
  delete(Host,Tab,"").
delete(Host,Tab,Key) ->
  {ok,{{_HttpVersion,Status,_StatusText},_Headers,Reply}} =
    httpc:request(delete,{url(Host,Tab,Key),[]},[],[]),
  {Status,Reply}.

put(Host,Tab) ->
  put(Host,Tab,"",[]).
put(Host,Tab,Key,PL) ->
  {ok,{{_HttpVersion,Status,_StatusText},_Headers,Reply}} =
    httpc:request(put,{url(Host,Tab,Key),[],[],enc(prep(PL))},[],[]),
  {Status,Reply}.

post(Host,Tab,PL) ->
  {ok,{{_HttpVersion,Status,_StatusText},_Headers,Reply}} =
    httpc:request(post,{url(Host,Tab,""),[],[],enc(prep(PL))},[],[]),
  {Status,Reply}.

url(Host,Tab,Key) ->
  "http://"++to_list(Host)++":8765/"++to_list(Tab)++"/"++to_list(Key).

prep(PL = [{_,_}|_]) -> {[{prep(K),prep(V)}||{K,V}<-PL]};
prep(L) when is_list(L) -> [prep(E)||E<-L];
prep(T) when is_tuple(T) -> list_to_tuple([prep(E)||E<-tuple_to_list(T)]);
prep(X) -> X.

to_list(X) when is_binary(X) -> binary_to_list(X);
to_list(X) when is_list(X)   -> X;
to_list(X) when is_integer(X)-> integer_to_list(X);
to_list(X) when is_atom(X)   -> atom_to_list(X).

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
