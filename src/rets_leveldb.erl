%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 21 Oct 2014 by Mats Cronqvist <masse@klarna.com>

%% @doc
%% @end

%%-compile(export_all).

-module('rets_leveldb').
-author('Mats Cronqvist').
-export([init/1,
         terminate/1,
         r/2,
         w/2]).

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
r(S,Ops) ->
  {{lists:flatmap(mk_reader(S),Ops)},
   S}.

w(S,Ops) ->
  {{lists:map(mk_committer(S),lists:map(mk_validator(S),Ops))},
   S}.

mk_reader(S) ->
  fun
    ({single,K}) -> wgetter(S,single,K);
    ({multi,K})  -> wgetter(S,multi,K);
    ({next,K})   -> nextprev(S,next,K);
    ({prev,K})   -> nextprev(S,prev,K);
    ({keys,K})   -> key_getter(S,K)
  end.

mk_validator(S) ->
  fun
    ({insert,K,{V,force}}) -> {insert,K,V,getter(S,K)};
    ({insert,K,{V,OV}})    -> {insert,K,V,assert(S,K,OV)};
    ({delete,K,force})     -> {delete,K,getter(S,K)};
    ({delete,K,OV})        -> {delete,K,assert(S,K,OV)};
    ({bump,K,force})       -> {bump,K,1};
    ({reset,K,force})      -> {reset,K,0}
  end.

assert(S,K,OV) ->
  case {getter(S,K),OV} of
    {{Bkey,OV},OV} -> {Bkey,OV};
    {V,OV}  -> throw({400,{inconsistence,K,V,OV}})
  end.

mk_committer(S) ->
  fun
    ({insert,Key,Val,{_,OV}}) -> inserter(S,Key,Val),{Key,OV};
    ({delete,Key,{_,OV}})     -> deleter(S,Key),{Key,OV};
    ({bump,K,I})              -> update_counter(S,K,I);
    ({reset,K,Z})             -> reset_counter(S,K,Z)
  end.

%% get data from leveldb.
inserter(S,Key,Val) ->
  lvl_put(S,Key,pack_val(Val)).

deleter(S,Key) ->
  lvl_delete(S,Key).

%% keys without wildcards
getter(S,Key) ->
  {Key,lvl_get(S,Key)}.

%% allow wildcards (".") in keys
wgetter(S,single,Key) ->
  case wgetter(S,multi,Key) of
    [{Key,V}] -> [{Key,V}];
    _         -> throw({404,{multiple_hits,Key}})
  end;
wgetter(S,multi,Key) ->
  case getter(S,Key) of
    {_,null} ->
      case next(S,Key,Key,[]) of
        [] -> throw({404,{no_such_key,Key}});
        As -> As
      end;
    {Key,Val} ->
      [{Key,Val}]
  end.

next(S,Key,WKey,Acc) ->
  case nextprev(S,{next,Key}) of
    end_of_table -> lists:reverse(Acc);
    {NKey,V} ->
      case key_match(WKey,NKey) of
        true -> next(S,NKey,WKey,[{NKey,unpack_val(V)}|Acc]);
        false-> Acc
      end
  end.

-define(binp(B1,B2), (is_binary(B1) andalso is_binary(B2))).
key_match(B1,B2) when ?binp(B1,B2) -> key_match(mk_ekey(B1),mk_ekey(B2));
key_match([],[])               -> true;
key_match(["."|Wkey],[_|Ekey]) -> key_match(Wkey,Ekey);
key_match([E|Wkey],[E|Ekey])   -> key_match(Wkey,Ekey);
key_match(_,_)                 -> false.

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

key_getter(S,Key) ->
  Iter = lvl_iter(S,keys_only),
  R = fold_loop(lvl_mv_iter(Iter,Key),Key,Iter,[]),
  lists:reverse(R).

fold_loop(invalid_iterator,_,_,Acc) ->
  Acc;
fold_loop(K,KeyW,Iter,Acc) ->
  case key_match(KeyW,K) of
    true -> fold_loop(lvl_mv_iter(Iter,prefetch),KeyW,Iter,[K|Acc]);
    false-> Acc
  end.

update_counter(S,Key,Incr) ->
  case lvl_get(S,Key) of
    null -> inserter(S,Key,Incr);
    Val  -> inserter(S,Key,Val+Incr)
  end.

reset_counter(S,Key,Val) ->
  inserter(S,Key,Val).

%% data packing
pack_val(Val) ->
  term_to_binary(Val).
unpack_val(Val) ->
  binary_to_term(Val).

%% key mangling
mk_ekey(Bin) ->
  string:tokens(binary_to_list(Bin),"/").

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
    not_found   -> null;
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
