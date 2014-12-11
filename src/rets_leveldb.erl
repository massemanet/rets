%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 21 Oct 2014 by Mats Cronqvist <masse@klarna.com>

%% @doc
%% @end

-compile(export_all).

-module('rets_leveldb').
-author('Mats Cronqvist').
-export([init/1,
         terminate/1,
         ops/2]).

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
ops(S,Ops) ->
  {{lists:map(mk_committer(S),lists:map(mk_validator(S),Ops))},
   S}.

mk_validator(S) ->
  fun
    ({K,insert,{V,force}}) -> {insert,K,V};
    ({K,insert,{V,OV}})    -> assert(S,K,OV),{insert,K,V};
    ({K,delete,force})     -> {delete,K};
    ({K,delete,OV})        -> assert(S,K,OV),{delete,K}
  end.

assert(S,K,OV) ->
  case {lvl_get(S,K),OV} of
    {not_found,null} -> ok;
    {not_found,OV}   -> throw({400,{key_exists,K,OV}});
    {OV,OV}          -> ok;
    {V,OV}           -> throw({400,{inconsistence,K,V,OV}})
  end.

mk_committer(S) ->
  fun
    ({insert,Key,Val}) -> inserter(S,Key,Val);
    ({delete,Key})     -> deleter(S,Key)
  end.

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
getter(S,multi,Key) ->
  case lvl_get(S,Key) of
    not_found ->
      case next(S,Key,Key,[]) of
        [] -> throw({404,no_such_key});
        As -> {As}
      end;
    Val ->
      {[{Key,Val}]}
  end.

next(S,Key,WKey,Acc) ->
  case nextprev(S,{next,Key}) of
    end_of_table -> lists:reverse(Acc);
    {Key,V} ->
      case key_match(WKey,Key) of
        true -> next(S,Key,WKey,[{Key,unpack_val(V)}|Acc]);
        false-> Acc
      end
  end.

key_match([],[])               -> true;
key_match(["."|Wkey],[_|Ekey]) -> key_match(Wkey,Ekey);
key_match([E|Wkey],[E|Ekey])   -> key_match(Wkey,Ekey);
key_match(_,_)                 -> false.

key_getter(S,Key) ->
  Iter = lvl_iter(S,keys_only),
  R = fold_loop(lvl_mv_iter(Iter,Key),Key,Iter,[]),
  lists:reverse(R).

fold_loop(invalid_iterator,_,_,Acc) ->
  Acc;
fold_loop(K,Key,Iter,Acc) ->
  case key_match(K,Key) of
    true -> fold_loop(lvl_mv_iter(Iter,prefetch),Key,Iter,[K|Acc]);
    false-> Acc
  end.

update_counter(S,Key,Incr) ->
  case lvl_get(S,Key) of
    not_found -> inserter(S,Key,Incr);
    Val       -> inserter(S,Key,Val+Incr)
  end.

reset_counter(S,Key,Val) ->
  inserter(S,Key,Val).

inserter(S,Key,Val) ->
  Bkey = mk_bkey(Key),
  lvl_put(S,Bkey,pack_val(Val)),
  {Bkey,Val}.

deleter(S,Key) ->
  Bkey = mk_bkey(Key),
  case lvl_get(S,Bkey) of
    not_found ->
      {Bkey,null};
    Val ->
      lvl_delete(S,Bkey),
      {Bkey,Val}
  end.

%% data packing
pack_val(Val) ->
  term_to_binary(Val).
unpack_val(Val) ->
  binary_to_term(Val).

%% key mangling
mk_ekey(Bin) ->
  string:tokens(binary_to_list(Bin),"/").
mk_bkey(Els) ->
  list_to_binary(string:join(Els,"/")).

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
