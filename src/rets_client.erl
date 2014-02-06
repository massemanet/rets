%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 29 Jan 2014 by mats cronqvist <masse@klarna.com>

%% @doc
%% @end

-module('rets_client').
-author('mats cronqvist').
-export([get/2,get/3,delete/2,delete/3,put/2,put/4]).

get(Host,Tab) ->
  get(Host,Tab,"").
get(Host,Tab,Key) ->
  case httpc:request(get,{url(Host,Tab,Key),[]},[],[]) of
    {ok,{{_HttpVersion,200,_StatusText},_Headers,Reply}} ->
      case dec(Reply) of
        {PL} -> {200,PL};
        X    -> {200,X}
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

url(Host,Tab,Key) ->
  "http://"++to_list(Host)++":8765/"++to_list(Tab)++"/"++to_list(Key).

prep(P) ->
  case is_proplist(P) of
    true -> {P};
    false-> P
  end.

is_proplist([{_,_}|T]) -> is_proplist(T);
is_proplist([])        -> true;
is_proplist(_)         -> false.

to_list(X) when is_binary(X) -> binary_to_list(X);
to_list(X) when is_list(X)   -> X;
to_list(X) when is_integer(X)-> integer_to_list(X);
to_list(X) when is_atom(X)   -> atom_to_list(X).

enc(Term) ->
  jiffy:encode(Term).

dec(Term) ->
  jiffy:decode(Term).
