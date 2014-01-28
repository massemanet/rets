%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 27 Jun 2012 by mats cronqvist <masse@klarna.com>

%% @doc
%% a pretty much minimal example of how to usu mod_fun
%% @end

-module(rets).
-author('mats cronqvist').
-export([ start/0,       % start the application
          start_link/0   % supervisor callback
         ,do/2]).        % inets mod_fun callback

%% main application starter
start() ->
  application:start(rets).

%% supervisor callback
start_link() ->
  [inets:start() || not is_started(inets)],
  inets:start(httpd,conf()),
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
  case {Req(method), string:tokens(Req(request_uri),"/")} of
    {"GET",   []}        -> Act(flat(gcall({all,[]})));
    {"GET",   [Tab]}     -> Act(flat(ets({list,Tab})));
    {"GET",   [Tab,Key]} -> Act(flat(ets({get,Tab,Key})));
    {"PUT",   [Tab]}     -> Act(flat(gcall({create,Tab})));
    {"PUT",   [Tab,Key]} -> Act(flat(ets({insert,Tab,Key,Req(entity_body)})));
    {"POST",  [Tab]}     -> Act(Tab++": "++Req(entity_body));
    {"POST",  [Tab,Key]} -> Act(Tab++": "++Key++": "++Req(entity_body));
    {"DELETE",[Tab]}     -> Act(flat(gcall({delete,Tab})));
    {"DELETE",[Tab,Key]} -> Act(flat(ets({delete,Tab,Key})));
    _                    -> Act(flat(Req(all)))
  end.

ets({list,Tab})       -> ets:tab2list(list_to_existing_atom(Tab));
ets({insert,Tab,K,V}) -> ets:insert(list_to_existing_atom(Tab),{K,V});
ets({get,Tab,Key})    -> ets:lookup(list_to_existing_atom(Tab),Key);
ets({delete,Tab,Key}) -> ets:delete(list_to_existing_atom(Tab),Key).

gcall(What) ->
  gen_server:call(rets_tables,What).

flat(Term) when not is_list(Term)->
  flat([Term]);
flat(Term) when is_list(Term)->
  lists:flatten([io_lib:fwrite("~p~n",[T])||T<-Term]).
