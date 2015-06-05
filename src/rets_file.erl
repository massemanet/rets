%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created :  5 Jun 2015 by masse <masse@klarna.com>

%% @doc
%% @end

-module('rets_file').
-export([delete_recursively/1]).

-include_lib("kernel/include/file.hrl").
-define(filetype(Type), #file_info{type=Type}).

delete_recursively(File) ->
  case file:read_file_info(File) of
    {error,enoent} ->
      ok;
    {ok,?filetype(directory)} ->
      {ok,Fs} = file:list_dir(File),
      Del = fun(F) -> delete_recursively(filename:join(File,F)) end,
      lists:foreach(Del,Fs),
      delete_file(del_dir,File);
    {ok,?filetype(regular)} ->
      delete_file(delete,File)
  end.

delete_file(Op,File) ->
  case file:Op(File) of
    ok -> ok;
    {error,Err} -> throw({500,{file_delete_error,{Err,File}}})
  end.

