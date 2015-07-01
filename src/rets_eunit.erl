%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created :  1 Jul 2015 by masse <masse@klarna.com>

%% @doc
%% @end

-module('rets_eunit').

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(dont_keep(SETUP),
        [fun() -> t00(SETUP) end,
         fun() -> t01(SETUP) end,
         fun() -> t02(SETUP) end,
         fun() -> t03(SETUP) end,
         fun() -> t04(SETUP) end,
         fun() -> t05(SETUP) end,
         fun() -> t06(SETUP) end,
         fun() -> t07(SETUP) end,
         fun() -> t08(SETUP) end,
         fun() -> t09(SETUP) end,
         fun() -> t10(SETUP) end,
         fun() -> t11(SETUP) end,
         fun() -> t12(SETUP) end,
         fun() -> t13(SETUP) end
        ]).

-define(keep_db(Backend),
        [fun() -> t14(Backend) end,
         fun() -> t15(Backend) end]).

leveldb_dont_keep_test_() ->
  ?dont_keep(mk_setup(leveldb)).

ets_dont_keep_test_() ->
  ?dont_keep(mk_setup(ets)).

ets_keep_test_() ->
  ?keep_db(mk_setup(ets)).

leveldb_keep_test_() ->
  ?keep_db(mk_setup(leveldb)).

mk_setup(Backend) ->
  fun(reset) ->
      application:set_env(rets,keep_db,false),    % delete old database files
      restart_rets(Backend);
     (keep) ->
      application:set_env(rets,keep_db,true),     % keep old database files
      restart_rets(Backend);
     (backend) ->
      Backend
  end.

restart_rets(Backend) ->
  application:stop(rets),
  %% Ranch opens the server socket in a supervisor and never
  %% explicitly closes it. So the socket is closed "shortly after" the
  %% supervisor process terminates. But if we are not lucky and try to
  %% restart very quickly we may get an `eaddrinuse'. So let's just
  %% wait a little bit here.
  timer:sleep(50),
  {ok,_} = rets:start(Backend).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
t00(SETUP) ->
  SETUP(reset),
  ?assertEqual({200,false},
               rets_client:delete(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,false},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:post(localhost,tibbe,
                                [{aa,1},
                                 {'aaa/1/x',"AAA"++[223]},
                                 {bbb,bBbB},
                                 {ccc,123.1},
                                 {ddd,[{a,"A"},{b,b},{c,123.3}]}])),
  ?assertEqual({200,[$A,$A,$A,223]},
               rets_client:get(localhost,tibbe,"aaa/_/x")),
  ?assertEqual({200,["aa","aaa/1/x","bbb","ccc","ddd"]},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tabbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tabbe,a,b)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tobbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tobbe,a,b)),
  ?assertEqual({200,["aa","aaa/1/x","bbb","ccc","ddd"]},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,[$A,$A,$A,223]},
               rets_client:get(localhost,tibbe,'_/1/_')),
  ?assertEqual({200,"bBbB"},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,123.1},
               rets_client:get(localhost,tibbe,ccc)),
  ?assertEqual({200,[{"a","A"},{"b","b"},{"c",123.3}]},
               rets_client:get(localhost,tibbe,"ddd")).

t01(SETUP) ->
  SETUP(reset),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,"eoo","000")),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,"foo/bla","aaa")),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,"goo/bla","bbb")),
  ?assertEqual({200,[{"foo/bla","aaa"},{"goo/bla","bbb"}]},
               rets_client:get(localhost,tibbe,"_/bla",multi)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,'aaa/1/x',"AAA")),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,bbb,bBbB)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,ccc,123.1)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,ddd,[{a,"A"},{b,b},{c,123.3}])),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'aaa/_/x')),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'_/1/_')),
  ?assertEqual({200,"AAA"},
               rets_client:get(localhost,tibbe,'aaa/1/x')),
  ?assertEqual({200,"bBbB"},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,123.1},
               rets_client:get(localhost,tibbe,ccc)),
  ?assertEqual({200,[{"a","A"},{"b","b"},{"c",123.3}]},
               rets_client:get(localhost,tibbe,ddd)).

t02(SETUP) ->
  SETUP(reset),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,bbb,bBbB)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,ddd,[{a,"A"},{b,b},{c,123.3}])),
  ?assertEqual({200,"bBbB"},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,[{"a","A"},{"b","b"},{"c",123.3}]},
               rets_client:get(localhost,tibbe,ddd)).

t03(SETUP) ->
  SETUP(reset),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,1},
               rets_client:put(localhost,tibbe,bbb,counter)),
  ?assertEqual({200,2},
               rets_client:put(localhost,tibbe,bbb,counter)),
  ?assertEqual({200,2},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,0},
               rets_client:put(localhost,tibbe,bbb,reset)),
  ?assertEqual({200,1},
               rets_client:put(localhost,tibbe,bbb,counter)).

t04(SETUP) ->
  SETUP(reset),
  ?assertEqual({200,[]},
               rets_client:get(localhost)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,[{tibbe,0}]},
               rets_client:get(localhost)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tabbe)),
  ?assertEqual({200,[{tabbe,0},{tibbe,0}]},
               rets_client:get(localhost)),
  ?assertEqual({200,true},
               rets_client:post(localhost,tibbe,[{foo,17}])),
  ?assertEqual({200,[{tabbe,0},{tibbe,1}]},
               rets_client:get(localhost)),
  ?assertEqual({200,17},
               rets_client:delete(localhost,tibbe,foo)),
  ?assertEqual({200,null},
               rets_client:delete(localhost,tibbe,foo)),
  ?assertEqual({200,[{tabbe,0},{tibbe,0}]},
               rets_client:get(localhost)).

t05(SETUP) ->
  SETUP(reset),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,foo,17)),
  ?assertEqual({200,[{tibbe,1}]},
               rets_client:get(localhost)),
  ?assertMatch({409,_},
               rets_client:put(localhost,tibbe,foo,a)).

t06(SETUP) ->
  SETUP(reset),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,false},
               rets_client:put(localhost,tibbe)),  % double create
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,17,foo)),
  ?assertEqual({200,"foo"},
               rets_client:delete(localhost,tibbe,17)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({200,true},
               rets_client:delete(localhost,tibbe)),
  ?assertEqual({200,false},
               rets_client:delete(localhost,tibbe)), % double delete
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe,17)),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe)).

t07(SETUP) ->
  SETUP(reset),
  ?assertEqual({404,"no_such_table"},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,[{tibbe,0}]},
               rets_client:get(localhost)),
  ?assertEqual({200,[]},
               rets_client:get(localhost,tibbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,'a/2/x',segundo)),
  ?assertEqual({200,"segundo"},
               rets_client:get(localhost,tibbe,'a/_/_')),
  ?assertEqual({404,"key_element_is_single_underscore"},
               rets_client:put(localhost,tibbe,'a/_/_',s)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,'a/1/x',primo)),
  ?assertEqual({404,"no_such_key"},
                rets_client:get(localhost,tibbe,'a')),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,'a',1)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tibbe,'a')),
  ?assertEqual({404,"multiple_hits"},
               rets_client:get(localhost,tibbe,'a/_/_')),
  ?assertEqual({200,[{"a/1/x","primo"},{"a/2/x","segundo"}]},
               rets_client:get(localhost,tibbe,'a/_/_',multi)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tibbe,'b/_/_',multi)).

t08(SETUP) ->
  SETUP(reset),
  T = [{"a","a"},{"b",[{"bb","bb"}]}], %% nested proplist
  ?assertEqual({200,true},
               rets_client:put(localhost,tybbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tybbe,"abc",T)),
  ?assertMatch({200,T},
               rets_client:get(localhost,tybbe,"abc")).

t09(SETUP) ->
  SETUP(reset),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,0,next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,0,prev)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,a,1)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tebbe,a)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,a,next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,a,prev)),
  ?assertEqual({200,[{"a",1}]},
               rets_client:get(localhost,tebbe,b,prev)),
  ?assertEqual({200,[{"a",1}]},
               rets_client:get(localhost,tebbe,0,next)).

t10(SETUP) ->
  SETUP(reset),
  %% This test verifies that if we have multiple tables performing a
  %% next on the first table's last element won't return the second
  %% table's first element.
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),

  %% Verify empty tables
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,0,next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tibbe,0,next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,0,prev)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tibbe,0,prev)),

  %% Add some records
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,a,1)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,b,2)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe,c,3)),
  ?assertEqual({200,[{tebbe,2},{tibbe,1}]},
               rets_client:get(localhost)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tebbe,a)),
  ?assertEqual({200,2},
               rets_client:get(localhost,tebbe,b)),
  ?assertEqual({404,"no_such_key"},
               rets_client:get(localhost,tebbe,c)),
  ?assertEqual({200,3},
               rets_client:get(localhost,tibbe,c)),

  %% Verify next
  ?assertEqual({200,[{"a",1}]},
               rets_client:get(localhost,tebbe,0,next)),
  ?assertEqual({200,[{"b",2}]},
               rets_client:get(localhost,tebbe,"a",next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,"b",next)),
  ?assertEqual({200,[{"c",3}]},
               rets_client:get(localhost,tibbe,0,next)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tibbe,"c",next)),

  %% Verify prev
  ?assertEqual({200,[{"b",2}]},
               rets_client:get(localhost,tebbe,"z",prev)),
  ?assertEqual({200,[{"a",1}]},
               rets_client:get(localhost,tebbe,"b",prev)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tebbe,"a",prev)),
  ?assertEqual({200,[{"c",3}]},
               rets_client:get(localhost,tibbe,"z",prev)),
  ?assertEqual({409,"end_of_table"},
               rets_client:get(localhost,tibbe,"c",prev)).

t11(SETUP) ->
  SETUP(reset),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({409,"end_of_table"},
               rets_client:put(localhost,tibbe,a,{indirect,tebbe})),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,'a/b/c',foo)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,'x/y/z',bar)),
  ?assertEqual({200,[{"a/b/c","foo"}]},
               rets_client:put(localhost,tibbe,a,{indirect,tebbe})),
  ?assertEqual({200,"a/b/c"},
               rets_client:get(localhost,tibbe,a)),
  ?assertEqual({200,[{"x/y/z","bar"}]},
               rets_client:put(localhost,tibbe,a,{indirect,tebbe})),
  ?assertEqual({200,"x/y/z"},
               rets_client:get(localhost,tibbe,a)),
  ?assertEqual({200,[{"a/b/c","foo"}]},
               rets_client:put(localhost,tibbe,a,{indirect,tebbe})),
  ?assertEqual({200,"a/b/c"},
               rets_client:get(localhost,tibbe,a)).

t12(SETUP) ->
  SETUP(reset),
  ?assertEqual({200,true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,2},
               rets_client:put(localhost,tibbe,bbb,{counter,2,3})),
  ?assertEqual({200,3},
               rets_client:put(localhost,tibbe,bbb,{counter,2,3})),
  ?assertEqual({200,3},
               rets_client:get(localhost,tibbe,bbb)),
  ?assertEqual({200,2},
               rets_client:put(localhost,tibbe,bbb,{counter,2,3})),
  ?assertEqual({200,3},
               rets_client:put(localhost,tibbe,bbb,{counter,2,3})),
  ?assertEqual({200,0},
               rets_client:put(localhost,tibbe,bbb,reset)),
  ?assertEqual({200,1},
               rets_client:put(localhost,tibbe,bbb,{counter,1,1})).

t13(SETUP) ->
  SETUP(reset),
  BE = SETUP(backend),
  ?assertMatch(A when A==BE,
               proplists:get_value(backend,rets:state())).

t14(SETUP) ->
  SETUP(reset),

  %% restart with keep_db == true
  SETUP(keep),

  %% Here the DB is empty
  ?assertEqual({200,[]},
               rets_client:get(localhost)),

  %% Add some data
  ?assertEqual({200, true},
               rets_client:put(localhost,tebbe)),
  ?assertEqual({200,true},
               rets_client:put(localhost,tebbe,a,1)),
  ?assertEqual({200,[{tebbe,1}]},
               rets_client:get(localhost)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tebbe,a)),

  %% Restart rets when keep_db is set
  SETUP(keep),

  %% Verify the data stayed
  ?assertEqual({200,[{tebbe,1}]},
               rets_client:get(localhost)),
  ?assertEqual({200,1},
               rets_client:get(localhost,tebbe,a)),

  %% Restart rets when keep_db is not set
  SETUP(reset),

  %% Verify the data vanished
  ?assertEqual({200,[]},
               rets_client:get(localhost)).

t15(SETUP) ->
  SETUP(reset),

  SETUP(keep),

  %% Create two tables
  ?assertEqual({200, true},
               rets_client:put(localhost,tebbe)),
  ?assertEqual({200, true},
               rets_client:put(localhost,tibbe)),
  ?assertEqual({200,[{tebbe,0},{tibbe,0}]},
               rets_client:get(localhost)),

  %% After restart: both tables should be saved to disk
  SETUP(keep),
  ?assertEqual({ok,["tebbe","tibbe"]},
               file:list_dir(filename:join("/tmp/rets/db",SETUP(backend)))),

  %% Delete one of the tables
  ?assertEqual({200,true},
               rets_client:delete(localhost,tibbe)),
  ?assertEqual({200,[{tebbe,0}]},
               rets_client:get(localhost)),

  %% After restart: only one table should be saved to disk
  SETUP(keep),
  ?assertEqual({ok,["tebbe"]},
               file:list_dir(filename:join("/tmp/rets/db",SETUP(backend)))).

-endif. % TEST
