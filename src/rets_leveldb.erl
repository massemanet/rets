%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 21 Oct 2014 by Mats Cronqvist <masse@klarna.com>

%% @doc
%% @end

-module('rets_leveldb').
-author('Mats Cronqvist').
-export([init/1,
         terminate/1,
         %% GET
         next/2,
         prev/2,
         multi/2,
         single/2,
         %% PUT
         mk_gauge/2,
         force_ins/2,
         insert/2,
         bump/2,
         reset/2,
         %% POST
         write_ops/2,
         read_ops/2,
         %% DELETE
         del_gauge/2,
         force_del/2,
         delete/2
         ]).

-record(state,
        {handle,
         dir}).

init(Env) ->
  ets:new(leveldb_gauges,[ordered_set,named_table,public]),
  Dir = proplists:get_value(table_dir,Env,"/tmp/rets/leveldb"),
  filelib:ensure_dir(Dir),
  #state{handle = lvl_open(Dir),
         dir = Dir}.

terminate(S) ->
  lvl_close(S),
  delete_recursively(S#state.dir).

delete_recursively(File) ->
  case filelib:is_dir(File) of
    true ->
      {ok,Fs} = file:list_dir(File),
      Del = fun(F) -> delete_recursively(filename:join(File,F)) end,
      lists:foreach(Del,Fs),
      delete_file(del_dir,File);
    false->
      delete_file(delete,File)
  end.

delete_file(Op,File) ->
  case file:Op(File) of
    ok -> ok;
    {error,Err} -> throw({500,{file_delete_error,{Err,File}}})
  end.

%% ::(#state{},list(term(Args)) -> {jiffyable(Reply),#state{}}
%% GET
next     (S,[Key])     -> nextprev(S,next,Key).
prev     (S,[Key])     -> nextprev(S,prev,Key).
multi    (S,[Key])     -> getter(S,multi ,Key).
single   (S,[Key])     -> getter(S,single,Key).
%% PUT
mk_gauge (_,[Key])     -> Key.
force_ins(_,[Key,Val]) -> {[Key,Val]}.
insert   (S,[Key,V])   -> ins(S,[{K,V}]).
bump     (S,[Key,I])   -> update_counter(S,Key,I).
reset    (S,[Key,I])   -> reset_counter(S,Key,I).
%% POST
write_ops(S,[Wops])    -> wops(S,Wops).
read_ops (S,[Rops])    -> rops(S,Rops).
%% DELETE
del_gauge(_,Key)       -> Key.
force_del(S,Key)       -> delete(S,[Key,null]).
delete   (S,[Key,Val]) -> deleter(S,Key,OldVal).

%% get data from leveldb.
nextprev(S,OP,Key) ->
  case nextprev(S,{OP,Key}) of
    end_of_table -> throw({409,end_of_table});
    {K,V}        -> {[{K,unpack_val(V)}]}
  end.

nextprev(S,{OP,Key}) ->
  Iter = lvl_iter(S,key_vals),
  check_np(OP,lvl_mv_iter(Iter,Key),Iter,Key).

check_np(prev,invalid_iterator,Iter,Key) ->
  case lvl_mv_iter(Iter,last) of
    invalid_iterator -> throw({409,end_of_table});
    {LastKey,Val} ->
      case LastKey < Key of
        true -> check_np(prev,{LastKey,Val},Iter,Key);
        false-> check_np(next,invalid_iterator,Iter,Key)
      end
  end;
check_np(_,invalid_iterator,_,_) ->
  end_of_table;
check_np(OP,{Key,_},Iter,Key) ->
  check_np(OP,lvl_mv_iter(Iter,OP),Iter,Key);
check_np(_,{Key,V},_,_) ->
  {Key,V}.

%% allow wildcards (".") in keys
getter(S,single,Ekey) ->
  case getter(S,multi,Ekey) of
    {[{_,V}]} -> V;
    _         -> throw({404,multiple_hits})
  end;
getter(S,multi,Ekey) ->
  case lvl_get(S,Ekey) of
    not_found ->
      case next(S,Ekey,[]) of
        [] -> throw({404,no_such_key});
        As -> {As}
      end;
    Val ->
      {[{list_to_binary(string:join(Ekey,"/")),Val}]}
  end.

next(S,TK,WKey,Acc) ->
  case nextprev(S,next,TK) of
    end_of_table -> lists:reverse(Acc);
    {NewTK,V} ->
      Key = tk_key(NewTK),
      case key_match(WKey,mk_ekey(Key)) of
        true -> next(S,NewTK,WKey,[{Key,unpack_val(V)}|Acc]);
        false-> Acc
      end
  end.

key_match([],[])               -> true;
key_match(["."|Wkey],[_|Ekey]) -> key_match(Wkey,Ekey);
key_match([E|Wkey],[E|Ekey])   -> key_match(Wkey,Ekey);
key_match(_,_)                 -> false.

deleter(S,Tab,Key) ->
  TK = tk(Tab,Key),
  case lvl_get(S,TK) of
    not_found ->
      null;
    Val ->
      lvl_delete(S,tk(Tab,Key)),
      ets:update_counter(leveldb_tabs,Tab,-1),
      Val
  end.

key_getter(S,Tab) ->
  Fun = fun(TK,Acc) -> [tk_key(TK)|Acc] end,
  Iter = lvl_iter(S,keys_only),
  R = fold_loop(lvl_mv_iter(Iter,Tab),Tab,Iter,Fun,[]),
  lists:reverse(R).

fold_loop(invalid_iterator,_,_,_,Acc) ->
  Acc;
fold_loop(TK,Tab,Iter,Fun,Acc) ->
  case tk_tab(TK) of
    Tab -> fold_loop(lvl_mv_iter(Iter,prefetch),Tab,Iter,Fun,Fun(TK,Acc));
    _   -> Acc
  end.

update_counter(S,Tab,Key,Incr) ->
  case lvl_get(S,tk(Tab,Key)) of
    not_found -> ins_overwrite(S,Tab,Key,Incr);
    Val       -> ins_overwrite(S,Tab,Key,Val+Incr)
  end.

reset_counter(S,Tab,Key,Val) ->
  ins_overwrite(S,Tab,Key,Val).

ins(S,Tab,KVs) ->
  lists:foreach(fun({Key,Val}) -> ins_ifempty(S,Tab,Key,Val) end,KVs),
  true.

%%            key exists  doesn't exist
%% ifempty       N           W
%% overwrite     W           W

ins_ifempty(S,Tab,Key,Val) ->
  TK = tk(Tab,Key),
  case lvl_get(S,TK) of
    not_found ->
      ets:update_counter(leveldb_tabs,Tab,1),
      do_ins(S,TK,Val);
    Vl ->
      throw({409,{key_exists,{Tab,Key,Vl}}})
  end.

ins_overwrite(S,Tab,Key,Val) ->
  TK = tk(Tab,Key),
  case lvl_get(S,TK) of
    not_found -> ets:update_counter(leveldb_tabs,Tab,1);
    _         -> ok
  end,
  do_ins(S,TK,Val).

do_ins(S,TK,Val) ->
  lvl_put(S,TK,pack_val(Val)),
  Val.

%% data packing
pack_val(Val) ->
  term_to_binary(Val).
unpack_val(Val) ->
  binary_to_term(Val).

%% table names
tab(Tab) ->
  case ets:lookup(leveldb_tabs,tab_name(Tab)) of
    [{TabName,_}] -> TabName;
    [] -> throw({404,no_such_table})
  end.

tab_name(Tab) ->
  list_to_binary([$/|Tab]).

tab_unname(Tab) ->
  tl(binary_to_list(Tab)).

%% key mangling
mk_ekey(Bin) ->
  string:tokens(binary_to_list(Bin),"/").

tk(Tab,ElList) ->
  Key = list_to_binary(string:join(ElList,"/")),
  <<Tab/binary,$/,Key/binary>>.

tk_key(TK) ->
  tl(re:replace(TK,"^/[a-zA-Z0-9-_]+/","")).

tk_tab(TK) ->
  [<<>>,Tab|_] = re:split(TK,"/"),
  <<$/,Tab/binary>>.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% leveldb API

lvl_open(Dir) ->
  case eleveldb:open(Dir,[{create_if_missing,true}]) of
    {ok,Handle} -> Handle;
    {error,Err} -> throw({500,{open_error,Err}})
  end.

lvl_close(S) ->
  case eleveldb:close(S#state.handle) of
    ok          -> ok;
    {error,Err} -> throw({500,{close_error,Err}})
  end.

lvl_iter(S,What) ->
  {ok,Iter} =
    case What of
      keys_only -> eleveldb:iterator(S#state.handle,[],keys_only);
      key_vals  -> eleveldb:iterator(S#state.handle,[])
    end,
  Iter.

lvl_mv_iter(Iter,Where) ->
  case eleveldb:iterator_move(Iter,Where) of
    {ok,Key,Val}             -> {Key,Val};
    {ok,Key}                 -> Key;
    {error,invalid_iterator} -> invalid_iterator;
    {error,Err}              -> throw({500,{iter_error,Err}})
  end.

lvl_get(S,Key) ->
  case eleveldb:get(S#state.handle,Key,[]) of
    {ok,V}      -> unpack_val(V);
    not_found   -> not_found;
    {error,Err} -> throw({500,{get_error,Err}})
  end.

lvl_put(S,Key,Val) ->
  case eleveldb:put(S#state.handle,Key,Val,[]) of
    ok          -> Val;
    {error,Err} -> throw({500,{put_error,Err}})
  end.

lvl_delete(S,Key) ->
  case eleveldb:delete(S#state.handle,Key,[]) of
    ok          -> true;
    {error,Err} -> throw({500,{delete_error,Err}})
  end.
