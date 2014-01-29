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
  {ok,{{_HttpVersion,Status,_StatusText},_Headers,Reply}} =
    httpc:request(get,{"http://"++Host++":8765/"++Tab++"/"++Key,[]},[],[]),
  {Status,Reply}.

delete(Host,Tab) ->
  delete(Host,Tab,"").
delete(Host,Tab,Key) ->
  {ok,{{_HttpVersion,Status,_StatusText},_Headers,Reply}} =
    httpc:request(delete,{"http://"++Host++":8765/"++Tab++"/"++Key,[]},[],[]),
  {Status,Reply}.

put(Host,Tab) ->
  put(Host,Tab,"",[]).
put(Host,Tab,Key,PL) ->
  {ok,{{_HttpVersion,Status,_StatusText},_Headers,Reply}} =
    httpc:request(put,{url(Host,Tab,Key),[],[],PL},[],[]),
  {Status,Reply}.

url(Host,Tab,Key) ->
  "http://"++Host++":8765/"++Tab++"/"++Key.
