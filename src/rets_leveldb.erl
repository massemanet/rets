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
        {name,
         file,
         handle}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% the API

init(Env) ->
  BaseDir = proplists:get_value(table_dir,Env),
  KeepDB = proplists:get_value(keep_db,Env),
  Dir  = filename:join(BaseDir,leveldb),
  case KeepDB of
    true -> ok;
    false-> rets_file:delete_recursively(Dir)
  end,
  filelib:ensure_dir(filename:join(Dir,dummy)),
  {ok,Files} = file:list_dir(Dir),
  S = #state{dir = Dir,tabs = create_lvl(),keep_db = KeepDB},
  lists:foreach(fun(F) -> put_lvl(S,lvl_open(Dir,F)) end,Files),
  S.

terminate(S) ->
  fold_lvl(S,fun(T,_) -> delete_tab(T,S#state.keep_db) end).

%% ::(#state{},list(term(Args)) -> {jiffyable(Reply),#state{}}
create(S ,[Tab])          -> {create_tab(S,Tab)}.
delete(S ,[Tab])          -> {delete_tab(S,get_lvl(S,Tab)),S};
delete(S ,[Tab,Key])      -> {deleter(get_lvl(S,Tab),Key),S}.
sizes(S  ,[])             -> {siz(S),S}.
keys(S   ,[Tab])          -> {key_getter(get_lvl(S,Tab)),S}.
insert(S ,[Tab,KVs])      -> {ins(get_lvl(S,Tab),KVs),S};
insert(S ,[Tab,K,V])      -> {ins(get_lvl(S,Tab),[{K,V}]),S}.
bump(S   ,[Tab,Key,I])    -> {update_counter(get_lvl(S,Tab),Key,I),S};
bump(S   ,[Tab,Key,L,H])  -> {update_counter(get_lvl(S,Tab),Key,L,H),S}.
reset(S  ,[Tab,Key,I])    -> {reset_counter(get_lvl(S,Tab),Key,I),S}.
next(S   ,[Tab,Key])      -> {next_prev(next,get_lvl(S,Tab),Key),S}.
prev(S   ,[Tab,Key])      -> {next_prev(prev,get_lvl(S,Tab),Key),S}.
multi(S  ,[Tab,Key])      -> {getter(get_lvl(S,Tab),multi,Key),S}.
single(S ,[Tab,Key])      -> {getter(get_lvl(S,Tab),single,Key),S}.
via(S    ,[Tab,Key,TabI]) -> {via(get_lvl(S,TabI),get_lvl(S,Tab),Key),S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% the implementetion

create_tab(S,Tab) ->
  case get_lvl(S,Tab) of
    undefined -> put_lvl(S,lvl_open(S#state.dir,Tab)),true;
    #lvl{}    -> false
  end.

delete_tab(S,Lvl) ->
  case Lvl of
    undefined -> false;
    Lvl       -> get_rid_off(Lvl,S#state.keep_db),true
  end.

get_rid_off(Lvl,false) -> lvl_destroy(Lvl);
get_rid_off(Lvl,true)  -> lvl_close(Lvl).

siz(S) ->
  fold_lvl(S,fun(Lvl,O) -> [{Lvl#lvl.name,undefined}|O] end).

key_getter(Lvl) ->
  Iter = lvl_iter(Lvl,keys_only),
  try
    lists:reverse(key_getter_loop(lvl_mv_iter(Iter,first),Iter,[]))
  after
    lvl_close_iter(Iter)
  end.

key_getter_loop(invalid_iterator,_,Acc) ->
  Acc;
key_getter_loop(K,Iter,Acc) ->
  key_getter_loop(lvl_mv_iter(Iter,prefetch),Iter,[K|Acc]).

update_counter(Lvl,Key,Incr) ->
  case lvl_get(Lvl,Key) of
    not_found -> ins_overwrite(Lvl,Key,Incr);
    Val       -> ins_overwrite(Lvl,Key,Val+Incr)
  end.

update_counter(Lvl,Key,Low,High) ->
  case lvl_get(Lvl,Key) of
    not_found           -> ins_overwrite(Lvl,Key,Low);
    Val when Val < High -> ins_overwrite(Lvl,Key,Val+1);
    _                   -> ins_overwrite(Lvl,Key,Low)
  end.

reset_counter(Lvl,Key,Val) ->
  ins_overwrite(Lvl,Key,Val).

ins(Lvl,KVs) ->
  lists:foreach(fun({Key,Val}) -> ins_ifempty(Lvl,Key,Val) end,KVs),
  true.

%%            key exists  doesn't exist
%% ifempty       N           W
%% overwrite     W           W

ins_ifempty(Lvl,Key,Val) ->
  case lvl_get(Lvl,Key) of
    not_found -> do_ins(Lvl,Key,Val);
    Vl        -> throw({409,{key_exists,{Lvl#lvl.name,Key,Vl}}})
  end.

ins_overwrite(Lvl,Key,Val) ->
  do_ins(Lvl,Key,Val).

do_ins(Lvl,Key,Val) ->
  lvl_put(Lvl,Key,pack_val(Val)),
  Val.

%% get data from leveldb.
%% allow wildcards (".") in keys
getter(Lvl,single,Ekey) ->
  case getter(Lvl,multi,Ekey) of
    {[{_,V}]} -> V;
    _         -> throw({404,multiple_hits})
  end;
getter(Lvl,multi,Ekey) ->
  case lvl_get(Lvl,Ekey) of
    not_found ->
      case next(Lvl,Ekey,Ekey,[]) of
        [] -> throw({404,no_such_key});
        As -> {As}
      end;
    Val ->
      {[{unmk_ekey(Ekey),Val}]}
  end.

next(Lvl,Key,WKey,Acc) ->
  case next_prev(Lvl,next,Key) of
    end_of_table -> lists:reverse(Acc);
    {Key,V} ->
      case key_match(WKey,mk_ekey(Key)) of
        true -> next(Lvl,Key,WKey,[{Key,unpack_val(V)}|Acc]);
        false-> Acc
      end
  end.

key_match([],[])               -> true;
key_match(["."|Wkey],[_|Ekey]) -> key_match(Wkey,Ekey);
key_match([E|Wkey],[E|Ekey])   -> key_match(Wkey,Ekey);
key_match(_,_)                 -> false.

next_prev(Lvl,OP,Key) ->
  case nextprev(Lvl,OP,Key) of
    end_of_table -> throw({409,end_of_table});
    {NewKey,V}   -> {[{NewKey,unpack_val(V)}]}
  end.

nextprev(Lvl,OP,Key) ->
  Iter = lvl_iter(Lvl,key_vals),
  try
    check_np(OP,lvl_mv_iter(Iter,Key),Iter,Key)
  after
    lvl_close_iter(Iter)
  end.

check_np(prev,invalid_iterator,Iter,Key) ->
  %% lvl_mv_iter/2 moved the iterator to the first record after TK: in
  %% case of TK being the last record, it will return an
  %% invalid_iterator and we have to fix the situation here
  check_np(prev,lvl_mv_iter(Iter,last),Key);
check_np(prev,{KeyNext,_},Iter,Key) when KeyNext >= Key ->
  %% lvl_mv_iter/2 moved the iterator to the first record not before
  %% TK: we are interested in the previous record, so an additional
  %% iterator step is necessary
  check_np(prev,lvl_mv_iter(Iter,prev),Key);
check_np(next,{Key,_},Iter,Key) ->
  %% the requested key was in the table: an additional iterator step
  %% is necessary
  check_np(next,lvl_mv_iter(Iter,next),Key);
check_np(next,KV,_Iter,Key) ->
  check_np(next,KV,Key).

check_np(_,invalid_iterator,_) ->
  end_of_table;
check_np(OP,{K2,V},K1) when OP =:= next andalso K1 < K2;
                            OP =:= prev andalso K2 < K1 ->
  {K2,V}.


deleter(Lvl,Key) ->
  case lvl_get(Lvl,Key) of
    not_found ->
      null;
    Val ->
      lvl_delete(Lvl,Key),
      Val
  end.

via(Lvl1,Lvl2,Key2) ->
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
  case lvl_get(Lvl2,Key2) of
    not_found -> via(Lvl1,[],Lvl2,Key2,false);
    Key1      -> via(Lvl1,mk_ekey(Key1),Lvl2,Key2,true)
  end.

via(Lvl1,Key1,Lvl2,Key2,Retry) ->
  case nextprev(Lvl1,next,Key1) of
    end_of_table when Retry ->
      via(Lvl1,[],Lvl2,Key2,false);
    end_of_table ->
      throw({409,end_of_table});
    {NextKey,V} ->
      NextVal = unpack_val(V),
      ins_overwrite(Lvl2,Key2,NextKey),
      {[{NextKey,NextVal}]}
  end.

%% data packing
pack_val(Val) ->
  term_to_binary(Val).
unpack_val(Val) ->
  binary_to_term(Val).

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
  case ets:lookup(S#state.tabs,Tab) of
    [Lvl=#lvl{}]  -> Lvl;
    undefined     -> throw({404,no_such_table})
  end.

fold_lvl(S,Fun) ->
  ets:foldl(Fun,[],S#state.tabs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% leveldb API

lvl_open(Dir,Tab) ->
  File = filename:join(Dir,Tab),
  case eleveldb:open(File,[{create_if_missing,true}]) of
    {ok,Handle} -> #lvl{name = Tab,handle = Handle,file = File};
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
