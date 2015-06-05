%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created : 21 Oct 2014 by Mats Cronqvist <masse@klarna.com>

%% @doc
%% @end

-module('rets_leveldb').
-author('Mats Cronqvist').
-export([init/1,
         terminate/1,
         create/2,
         delete/2,
         sizes/2,
         keys/2,
         insert/2,
         bump/2,
         reset/2,
         next/2,
         prev/2,
         multi/2,
         single/2,
         via/2
        ]).

-record(state,
        {tabs,
         keep_db,
         dir}).

-record(lvl,
        {table,
         file,
         handle}).

init(Env) ->
  BaseDir = proplists:get_value(table_dir,Env),
  KeepDB = proplists:get_value(keep_db,Env),
  Dir  = filename:join(BaseDir,leveldb),
  case KeepDB of
    true -> ok;
    false-> rets_file:delete_recursively(Dir)
  end,
  filelib:ensure_dir(filename:join(Dir,dummy)),
  open_all(#state{dir = Dir,tabs = create_lvl(),keep_db = KeepDB}).

terminate(S) ->
  fold_lvl(S,fun(T,_) -> delete_tab(T,S#state.keep_db) end).

%% ::(#state{},list(term(Args)) -> {jiffyable(Reply),#state{}}
create(S ,[Tab])          -> {create_tab(S,Tab)}.
delete(S ,[Tab])          -> {delete_tab(S,tab(Tab),S};
delete(S ,[Tab,Key])      -> {deleter(S,tab(S,Tab),Key),S}.
sizes(S  ,[])             -> {siz(S),S}.
keys(S   ,[Tab])          -> {key_getter(S,tab(S,Tab)),S}.
insert(S ,[Tab,KVs])      -> {ins(S,tab(S,Tab),KVs),S};
insert(S ,[Tab,K,V])      -> {ins(S,tab(S,Tab),[{K,V}]),S}.
bump(S   ,[Tab,Key,I])    -> {update_counter(S,tab(S,Tab),Key,I),S};
bump(S   ,[Tab,Key,L,H])  -> {update_counter(S,tab(S,Tab),Key,L,H),S}.
reset(S  ,[Tab,Key,I])    -> {reset_counter(S,tab(S,Tab),Key,I),S}.
next(S   ,[Tab,Key])      -> {nextprev(S,next,tab(S,Tab),Key),S}.
prev(S   ,[Tab,Key])      -> {nextprev(S,prev,tab(S,Tab),Key),S}.
multi(S  ,[Tab,Key])      -> {getter(S,multi,tab(S,Tab),Key),S}.
single(S ,[Tab,Key])      -> {getter(S,single,tab(S,Tab),Key),S}.
via(S    ,[Tab,Key,TabI]) -> {via(S,tab(S,TabI),tab(S,Tab),Key),S}.

create_tab(S,Tab) ->
  case get_lvl(S,Tab) of
    undefined -> put_lvl(S,lvl_open(S#state.dir,Tab)),true;
    #lvl{}    -> false
  end.

delete_tab(S,Tab) ->
  case get_lvl(S,Tab) of
    undefined -> false;
    Lvl       -> get_rid_off(Lvl,S#state.keep_db),true
  end.

get_rid_off(Lvl,false) -> lvl_destroy(Lvl);
get_rid_off(Lvl,true)  -> lvl_close(Lvl).

open_all(S) ->
  Dir = S#state.dir,
  {ok,Files} = file:list_dir(Dir),
  lists:foreach(fun(F) -> put_lvl(S,lvl_open(Dir,F)) end,Files).

siz(S) ->
  fold_lvl(S,fun(Lvl,O) -> [{Lvl#lvl.table,undefined}|O] end).

key_getter(S,Tab) ->
  Iter = lvl_iter(S,keys_only),
  try
    lists:reverse(fold_loop(lvl_mv_iter(Iter,Tab),Tab,Iter,[]))
  after
    lvl_close_iter(Iter)
  end.

fold_loop(invalid_iterator,_,_,Acc) ->
  Acc;
fold_loop(K,Tab,Iter,Acc) ->
  fold_loop(lvl_mv_iter(Iter,prefetch),Tab,Iter,[K|Acc]).

update_counter(S,Tab,Key,Incr) ->
  case lvl_get(S,Tab,Key) of
    not_found -> ins_overwrite(S,Tab,Key,Incr);
    Val       -> ins_overwrite(S,Tab,Key,Val+Incr)
  end.

update_counter(S,Tab,Key,Low,High) ->
  case lvl_get(S,Tab,Key) of
    not_found           -> ins_overwrite(S,Tab,Key,Low);
    Val when Val < High -> ins_overwrite(S,Tab,Key,Val+1);
    _                   -> ins_overwrite(S,Tab,Key,Low)
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
  case lvl_get(S,Tab,Key) of
    not_found -> do_ins(S,Tab,Key,Val);
    Vl        -> throw({409,{key_exists,{Tab,Key,Vl}}})
  end.

ins_overwrite(S,Tab,Key,Val) ->
  do_ins(S,Tab,Key,Val).

do_ins(S,Tab,Key,Val) ->
  lvl_put(S,Tab,Key,pack_val(Val)),
  Val.

%% get data from leveldb.
%% allow wildcards (".") in keys
getter(S,single,Tab,Ekey) ->
  case getter(S,multi,Tab,Ekey) of
    {[{_,V}]} -> V;
    _         -> throw({404,multiple_hits})
  end;
getter(S,multi,Tab,Ekey) ->
  case lvl_get(S,Tab,Ekey) of
    not_found ->
      case next(S,Tab,Ekey,Ekey,[]) of
        [] -> throw({404,no_such_key});
        As -> {As}
      end;
    Val ->
      {[{unmk_ekey(Ekey),Val}]}
  end.

next(S,Tab,Key,WKey,Acc) ->
  case nextprev(S,next,Tab,Key) of
    end_of_table -> lists:reverse(Acc);
    {Key,V} ->
      case key_match(WKey,mk_ekey(Key)) of
        true -> next(S,Tab,Key,WKey,[{Key,unpack_val(V)}|Acc]);
        false-> Acc
      end
  end.

key_match([],[])               -> true;
key_match(["."|Wkey],[_|Ekey]) -> key_match(Wkey,Ekey);
key_match([E|Wkey],[E|Ekey])   -> key_match(Wkey,Ekey);
key_match(_,_)                 -> false.

nextprev(S,OP,Tab,Key) ->
  case nextprev(S,OP,{Tab,Key}) of
    end_of_table -> throw({409,end_of_table});
    {NewKey,V}   -> {[{NewKey,unpack_val(V)}]}
  end.

nextprev(S,OP,{Tab,Key}) ->
  Iter = lvl_iter(S,key_vals),
  try
    check_np(OP,lvl_mv_iter(Iter,Tab,Key),Iter,TK)
  after
    lvl_close_iter(Iter)
  end.

check_np(prev,invalid_iterator,Iter,TK) ->
  %% lvl_mv_iter/2 moved the iterator to the first record after TK: in
  %% case of TK being the last record, it will return an
  %% invalid_iterator and we have to fix the situation here
  check_np(prev,lvl_mv_iter(Iter,last),TK);
check_np(prev,{TKNext,_},Iter,TK) when TKNext >= TK ->
  %% lvl_mv_iter/2 moved the iterator to the first record not before
  %% TK: we are interested in the previous record, so an additional
  %% iterator step is necessary
  check_np(prev,lvl_mv_iter(Iter,prev),TK);
check_np(next,{TK,_},Iter,TK) ->
  %% the requested key was in the table: an additional iterator step
  %% is necessary
  check_np(next,lvl_mv_iter(Iter,next),TK);
check_np(next,TKV,_Iter,TK) ->
  check_np(next,TKV,TK).

check_np(_,invalid_iterator,_) ->
  end_of_table;
check_np(OP,{TK2,V},TK1) when OP =:= next andalso TK2 > TK1;
                              OP =:= prev andalso TK2 < TK1 ->
  %% The tab of the two TK:s must match, otherwise we've reached the
  %% end of this particular table
  case tk_tab(TK1) =:= tk_tab(TK2) of
    true  -> {TK2,V};
    false -> end_of_table
  end.

deleter(S,Tab,Key) ->
  TK = tk(Tab,Key),
  case lvl_get(S,TK) of
    not_found ->
      null;
    Val ->
      lvl_delete(S,tk(Tab,Key)),
      Val
  end.

via(S,Tab1,Tab2,Key2) ->
  %% Key2 in Tab2 holds the last visited key of Tab1. This operation
  %% gets the next key and value from Tab1, updating the pointer under
  %% Key2.
  %%
  %% It is important to pay attention to internal and external
  %% representation of keys:
  %% - Key2 does not need any transformation; it is already in the
  %%   format gettert and setter functions expect it to be.
  %% - Tab2 holds Key1 in external format (otherwise a simple get from
  %%   Tab2 @ Key2 would crash)
  %% - Key1 therefore needs to be converted to internal format before
  %%   using it for a lookup in Tab1
  %% - Once the next Key1 is found, it needs to be converted to
  %%   external format before saving in Tab2
  case lvl_get(S,tk(Tab2,Key2)) of
    not_found -> via(S,Tab1,[],Tab2,Key2,false);
    Key1      -> via(S,Tab1,mk_ekey(Key1),Tab2,Key2,true)
  end.

via(S,Tab1,Key1,Tab2,Key2,Retry) ->
  case nextprev(S,next,tk(Tab1,Key1)) of
    end_of_table when Retry ->
      via(S,Tab1,[],Tab2,Key2,false);
    end_of_table ->
      throw({409,end_of_table});
    {TK,V} ->
      NextKey = tk_key(TK),
      NextVal = unpack_val(V),
      ins_overwrite(S,Tab2,Key2,NextKey),
      {[{NextKey,NextVal}]}
  end.

%% data packing
pack_val(Val) ->
  term_to_binary(Val).
unpack_val(Val) ->
  binary_to_term(Val).

%% table names
tab(S,Tab) ->
  case get_lvl(S,Tab) of
    #lvl{}    -> Tab;
    undefined -> throw({404,no_such_table})
  end.

%% key mangling
mk_ekey(Bin) ->
  string:tokens(binary_to_list(Bin),"/").

unmk_ekey(EList) ->
  list_to_binary(string:join(EList,"/")).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% handle the table of tables

create_lvl() ->
  ets:new(rets_leveldb_tabs,[named_table,ordered_set,{keypos,2}]).

put_lvl(S,Lvl) ->
  ets:insert(S#state.tabs,Lvl).

get_lvl(S,Tab) ->
  ets:lookup(S#state.tabs,Tab).

fold_lvl(S,Fun) ->
  ets:foldl(Fun,[],S#state.tabs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% leveldb API

lvl_open(Dir,Tab) ->
  File = filename:join(Dir,Tab),
  case eleveldb:open(File,[{create_if_missing,true}]) of
    {ok,Handle} -> #lvl{table = Tab,handle = Handle,file = File};
    {error,Err} -> throw({500,{open_error,Err}})
  end.

lvl_close(Lvl) ->
  case eleveldb:close(Lvl#lvl.handle) of
    ok          -> ok;
    {error,Err} -> throw({500,{close_error,Err}})
  end.

lvl_iter(Lvl,What) ->
  {ok,Iter} =
    case What of
      keys_only -> eleveldb:iterator(Lvl#lvl.handle,[],keys_only);
      key_vals  -> eleveldb:iterator(Lvl#lvl.handle,[])
    end,
  Iter.

lvl_close_iter(Iter) ->
  eleveldb:iterator_close(Iter).

lvl_mv_iter(Iter,Where) ->
  case eleveldb:iterator_move(Iter,Where) of
    {ok,Key,Val}             -> {Key,Val};
    {ok,Key}                 -> Key;
    {error,invalid_iterator} -> invalid_iterator;
    {error,Err}              -> throw({500,{iter_error,Err}})
  end.

lvl_get(Lvl,Key) ->
  case eleveldb:get(Lvl#lvl.handle,Key,[]) of
    {ok,V}      -> unpack_val(V);
    not_found   -> not_found;
    {error,Err} -> throw({500,{get_error,Err}})
  end.

lvl_put(Lvl,Key,Val) ->
  case eleveldb:put(Lvl#lvl.handle,Key,Val,[]) of
    ok          -> Val;
    {error,Err} -> throw({500,{put_error,Err}})
  end.

lvl_delete(Lvl,Key) ->
  case eleveldb:delete(Lvl#lvl.handle,Key,[]) of
    ok          -> true;
    {error,Err} -> throw({500,{delete_error,Err}})
  end.

lvl_destroy(Lvl) ->
  lvl_close(Lvl),
  case eleveldb:destroy(Lvl#lvl.file,[]) of
    ok          -> true;
    {error,Err} -> throw({500,{delete_error,Err}})
  end.
