%%%-------------------------------------------------------------------
%%% File    : mio_skip_graph_tests.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 16 Apr 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_skip_graph_tests).
-include("../include/mio.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MEMCACHED_PORT, 11211).
-define(MEMCACHED_HOST, "127.0.0.1").

setup_mio() ->
    ok = application:start(mio),
    ok = mio_app:wait_startup(?MEMCACHED_HOST, ?MEMCACHED_PORT).

teardown_mio(_) ->
    ok = application:stop(mio).

sg_test_() ->
    {foreach, fun setup_mio/0, fun teardown_mio/1,
     [
      [?_test(c_o_same_mv())],
      [?_test(c_o_different_mv())],
      [?_test(search_c_o_1_same())],
      [?_test(search_c_o_1_different())],
      [?_test(search_c_o_2_same())],
      [?_test(search_c_o_2_different())],
      [?_test(search_c_o_3_same())],
      [?_test(search_c_o_3_different())],
      [?_test(search_c_o_4_same())],
      [?_test(search_c_o_4_different())],
      [?_test(search_c_o_c_1_same())],
      [?_test(search_c_o_c_1_different())],
      [?_test(search_c_o_c_2_same())],
      [?_test(search_c_o_c_2_different())],
      [?_test(search_c_o_c_3_same())],
      [?_test(search_c_o_c_3_different())],
      [?_test(search_c_o_c_4_same())],
      [?_test(search_c_o_c_4_different())],
      [?_test(search_c_o_c_5_same())],
      [?_test(search_c_o_c_5_different())],
      [?_test(search_c_o_c_6_same())],
      [?_test(search_c_o_c_6_different())],
      [?_test(insert_o_1())],
      [?_test(insert_o_2())],
      [?_test(insert_o_3())],
      [?_test(insert_many())]
      ]
     }.

c_o_same_mv() ->
    {Bucket, RightBucket} = make_c_o_same_mv(),

    %% check on level 0
    ?assertMatch(X when X =/= [], RightBucket),
    ?assertEqual([], mio_bucket:get_left_op(Bucket)),
    ?assertEqual(Bucket, mio_bucket:get_left_op(RightBucket)),

    %% check on level 1
    ?assertEqual(RightBucket, mio_bucket:get_right_op(Bucket, 1)),
    ?assertEqual([], mio_bucket:get_left_op(Bucket, 1)),
    ?assertEqual(Bucket, mio_bucket:get_left_op(RightBucket, 1)),

    %% search
    ?assertEqual({error, not_found}, mio_skip_graph:search_op(Bucket, "key0")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Bucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Bucket, "key3")),
    ?assertEqual({error, not_found}, mio_skip_graph:search_op(RightBucket, "key0")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(RightBucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(RightBucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(RightBucket, "key3")).

c_o_different_mv() ->
    {Bucket, RightBucket} = make_c_o_different_mv(),

    %% check on level 0
    ?assertMatch(X when X =/= [], RightBucket),
    ?assertEqual([], mio_bucket:get_left_op(Bucket)),
    ?assertEqual(RightBucket, mio_bucket:get_right_op(Bucket, 0)),
    ?assertEqual({?MAX_KEY, false}, mio_skip_graph:get_key_op(RightBucket)),
    ?assertEqual([], mio_bucket:get_left_op(Bucket, 0)),
    ?assertEqual([], mio_bucket:get_right_op(RightBucket, 0)),
    ?assertEqual(Bucket, mio_bucket:get_left_op(RightBucket, 0)),
    ?assertEqual({"key3", true}, mio_skip_graph:get_key_op(Bucket)),

    ?assertEqual(Bucket, mio_bucket:get_left_op(RightBucket)),

    %% check on level 1
    ?assertEqual([], mio_bucket:get_right_op(Bucket, 1)),
    ?assertEqual([], mio_bucket:get_left_op(Bucket, 1)),
    ?assertEqual([], mio_bucket:get_right_op(RightBucket, 1)),
    ?assertEqual([], mio_bucket:get_left_op(RightBucket, 1)),

    %% search
%%    ?assertEqual({error, not_found}, mio_skip_graph:search_op(Bucket, "key0")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Bucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Bucket, "key3")),
    ?assertEqual({error, not_found}, mio_skip_graph:search_op(RightBucket, "key0")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(RightBucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(RightBucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(RightBucket, "key3")),

    {Bucket, RightBucket}.

search_c_o_1_same() ->
    search_c_o_1(fun make_c_o_same_mv/0).

search_c_o_1_different() ->
    search_c_o_1(fun make_c_o_different_mv/0).

search_c_o_1(MakeC_O_C_Fun) ->
    {Bucket, RightBucket} = MakeC_O_C_Fun(),
    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key0", value0)),
    %% search
    ?assertEqual({ok, value0}, mio_skip_graph:search_op(Bucket, "key0")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Bucket, "key2")),
    ?assertMatch({{?MIN_KEY, false}, {"key2", true}}, mio_bucket:get_range_op(Bucket)),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Bucket, "key3")),
    ?assertEqual({ok, value0}, mio_skip_graph:search_op(RightBucket, "key0")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(RightBucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(RightBucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(RightBucket, "key3")).

search_c_o_2_same() ->
    search_c_o_2(fun make_c_o_same_mv/0).

search_c_o_2_different() ->
    search_c_o_2(fun make_c_o_different_mv/0).

search_c_o_2(MakeC_O_C_Fun) ->
    {Bucket, RightBucket} = MakeC_O_C_Fun(),
    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key00", value00)),
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key01", value01)),

    %% search
    ?assertEqual({ok, value00}, mio_skip_graph:search_op(Bucket, "key00")),
    ?assertEqual({ok, value01}, mio_skip_graph:search_op(Bucket, "key01")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Bucket, "key2")),
    ?assertMatch({{?MIN_KEY, false}, {"key1", true}}, mio_bucket:get_range_op(Bucket)),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Bucket, "key3")),
    ?assertEqual({ok, value00}, mio_skip_graph:search_op(RightBucket, "key00")),
    ?assertEqual({ok, value01}, mio_skip_graph:search_op(RightBucket, "key01")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(RightBucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(RightBucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(RightBucket, "key3")).

search_c_o_3_same() ->
    search_c_o_3(fun make_c_o_same_mv/0).

search_c_o_3_different() ->
    search_c_o_3(fun make_c_o_different_mv/0).

search_c_o_3(MakeC_O_C_Fun) ->
    {Bucket, RightBucket} = MakeC_O_C_Fun(),
    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key00", value00)),
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key01", value01)),
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key02", value02)),

    %% Right becomes c_o_c_r
    ?assertEqual(c_o_c_r, mio_bucket:get_type_op(RightBucket)),

    Middle = mio_bucket:get_right_op(Bucket),
    ?assertEqual(c_o_c_m, mio_bucket:get_type_op(Middle)),

    %% search
    ?assertEqual({ok, value00}, mio_skip_graph:search_op(Bucket, "key00")),
    ?assertEqual({ok, value01}, mio_skip_graph:search_op(Bucket, "key01")),
    ?assertEqual({ok, value02}, mio_skip_graph:search_op(Bucket, "key02")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Bucket, "key2")),
    ?assertMatch({{?MIN_KEY, false}, {"key02", true}}, mio_bucket:get_range_op(Bucket)),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Bucket, "key3")),
    ?assertEqual({ok, value00}, mio_skip_graph:search_op(RightBucket, "key00")),
    ?assertEqual({ok, value01}, mio_skip_graph:search_op(RightBucket, "key01")),
    ?assertEqual({ok, value02}, mio_skip_graph:search_op(RightBucket, "key02")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(RightBucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(RightBucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(RightBucket, "key3")),
    ?assertEqual({ok, value00}, mio_skip_graph:search_op(Middle, "key00")),
    ?assertEqual({ok, value01}, mio_skip_graph:search_op(Middle, "key01")),
    ?assertEqual({ok, value02}, mio_skip_graph:search_op(Middle, "key02")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Middle, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Middle, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Middle, "key3")).

search_c_o_4_same() ->
    search_c_o_4(fun make_c_o_same_mv/0).

search_c_o_4_different() ->
    search_c_o_4(fun make_c_o_different_mv/0).

search_c_o_4(MakeC_O_C_Fun) ->
    {Bucket, RightBucket} = MakeC_O_C_Fun(),
    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key4", value4)),
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key5", value5)),
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key6", value6)),

    %% Right becomes c_o_c_r
    ?assertEqual(c_o_c_r, mio_bucket:get_type_op(RightBucket)),

    Middle = mio_bucket:get_right_op(Bucket),
    ?assertEqual(c_o_c_m, mio_bucket:get_type_op(Middle)),

    %% search
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Bucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Bucket, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Bucket, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Bucket, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Bucket, "key6")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Middle, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Middle, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Middle, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Middle, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Middle, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Middle, "key6")),

    ?assertMatch({{?MIN_KEY, false}, {"key3", true}}, mio_bucket:get_range_op(Bucket)),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(RightBucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(RightBucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(RightBucket, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(RightBucket, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(RightBucket, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(RightBucket, "key6")),
    {Bucket, Middle, RightBucket}.

%% C1-O2-C3
%%   C1 [key1, key2, key3]
%%   O2 []
%%   C3 [key4, key5, key6]
%%
%%   Insertion to C1 : C1'-O2'-C3
search_c_o_c_1_same() ->
    search_c_o_c_1(fun make_c_o_same_mv/0).
search_c_o_c_1_different() ->
    search_c_o_c_1(fun make_c_o_different_mv/0).

search_c_o_c_1(MakeC_O_C_Fun) ->
    {Left, Middle, Right} = make_c_o_c(MakeC_O_C_Fun),

    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Left, "key0", value0)),

    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    ?assertEqual({{?MIN_KEY, false}, {"key2", true}}, mio_bucket:get_range_op(Left)),
    ?assertEqual({ok, value0}, mio_skip_graph:search_op(Left, "key0")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Left, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Left, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Left, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Left, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Left, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Left, "key6")),

    ?assertEqual({ok, value0}, mio_skip_graph:search_op(Middle, "key0")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Middle, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Middle, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Middle, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Middle, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Middle, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Middle, "key6")),

    ?assertEqual({ok, value0}, mio_skip_graph:search_op(Right, "key0")),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Right, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Right, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Right, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Right, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Right, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Right, "key6")).

%% C1-O2-C3
%%   C1 [key1, key2, key3]
%%   O2 []
%%   C3 [key4, key5, key6]
%%
%%   Insertion to C3 : C1-O2 | C3'-O4
search_c_o_c_2_same() ->
    search_c_o_c_2(fun make_c_o_same_mv/0).
search_c_o_c_2_different() ->
    search_c_o_c_2(fun make_c_o_different_mv/0).

search_c_o_c_2(MakeC_O_C_Fun) ->
    {Left, Middle, Right} = make_c_o_c(MakeC_O_C_Fun),

    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Right, "key7", value7)),

    %% check types
    NewBucket = mio_bucket:get_right_op(Right),
    ?assertEqual(c_o_l, mio_bucket:get_type_op(Left)),
    ?assertEqual(c_o_r, mio_bucket:get_type_op(Middle)),
    ?assertEqual(c_o_l, mio_bucket:get_type_op(Right)),
    ?assertEqual(c_o_r, mio_bucket:get_type_op(NewBucket)),

    %% search
    ?assertEqual({ok, value7}, mio_bucket:get_op(NewBucket, "key7")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(NewBucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(NewBucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(NewBucket, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(NewBucket, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(NewBucket, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(NewBucket, "key6")),
    ?assertEqual({ok, value7}, mio_skip_graph:search_op(NewBucket, "key7")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Left, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Left, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Left, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Left, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Left, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Left, "key6")),
    ?assertEqual({ok, value7}, mio_skip_graph:search_op(Left, "key7")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Middle, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Middle, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Middle, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Middle, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Middle, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Middle, "key6")),
    ?assertEqual({ok, value7}, mio_skip_graph:search_op(Middle, "key7")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Right, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Right, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Right, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Right, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Right, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Right, "key6")),
    ?assertEqual({ok, value7}, mio_skip_graph:search_op(Right, "key7")).

%% C1-O2-C3
%%   C1 [key1, key2, key3]
%%   O2 []
%%   C3 [key4, key5, key6]
%%
%%   Insertion to O2 : C1-O2'-C3
search_c_o_c_3_same() ->
    search_c_o_c_3(fun make_c_o_same_mv/0).
search_c_o_c_3_different() ->
    search_c_o_c_3(fun make_c_o_different_mv/0).

search_c_o_c_3(MakeC_O_C_Fun) ->
    {Left, Middle, Right} = make_c_o_c(MakeC_O_C_Fun),

    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Middle, "key30", value30)),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Left, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Left, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Left, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Left, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Left, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Left, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Left, "key30")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Middle, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Middle, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Middle, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Middle, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Middle, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Middle, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Middle, "key30")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Right, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Right, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Right, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Right, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Right, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Right, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Right, "key30")).

%% C1-O2$-C3
%%   C1 [key1, key2, key3]
%%   O2$ [key30, key31]
%%   C3 [key4, key5, key6]
%%
%%   Insertion to O2$ : C1-C2-C3 -> C1-O2' | C3'-O4
search_c_o_c_4_same() ->
    search_c_o_c_4(fun make_c_o_same_mv/0).
search_c_o_c_4_different() ->
    search_c_o_c_4(fun make_c_o_different_mv/0).

search_c_o_c_4(MakeC_O_C_Fun) ->
    {Left, Middle, Right} = make_c_O_c(MakeC_O_C_Fun),

    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Middle, "key32", value32)),

    %% check types
    NewBucket = mio_bucket:get_right_op(Right),
    ?assertEqual(c_o_l, mio_bucket:get_type_op(Left)),
    ?assertEqual(c_o_r, mio_bucket:get_type_op(Middle)),
    ?assertEqual(c_o_l, mio_bucket:get_type_op(Right)),
    ?assertEqual(c_o_r, mio_bucket:get_type_op(NewBucket)),

    %% search
    ?assertEqual({ok, value30}, mio_bucket:get_op(Middle, "key30")),
    ?assertEqual({ok, value31}, mio_bucket:get_op(Middle, "key31")),
    ?assertEqual({ok, value32}, mio_bucket:get_op(Right, "key32")),
    ?assertEqual({ok, value4}, mio_bucket:get_op(Right, "key4")),
    ?assertEqual({ok, value5}, mio_bucket:get_op(Right, "key5")),
    ?assertEqual({ok, value6}, mio_bucket:get_op(NewBucket, "key6")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(NewBucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(NewBucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(NewBucket, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(NewBucket, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(NewBucket, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(NewBucket, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(NewBucket, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(NewBucket, "key31")),
    ?assertEqual({ok, value32}, mio_skip_graph:search_op(NewBucket, "key32")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Left, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Left, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Left, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Left, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Left, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Left, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Left, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(Left, "key31")),
    ?assertEqual({ok, value32}, mio_skip_graph:search_op(Left, "key32")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Middle, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Middle, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Middle, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Middle, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Middle, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Middle, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Middle, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(Middle, "key31")),
    ?assertEqual({ok, value32}, mio_skip_graph:search_op(Middle, "key32")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Right, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Right, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Right, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Right, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Right, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Right, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Right, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(Right, "key31")),
    ?assertEqual({ok, value32}, mio_skip_graph:search_op(Right, "key32")).

%% C1-O2$-C3
%%   C1 [key1, key2, key3]
%%   O2$ [key30, key31]
%%   C3 [key4, key5, key6]
%%
%%     becomes
%%
%%   C1 [key1, key2, key22]
%%   O2 [key3, key30]
%%   C3 [key31, key4, key5]
%%   O4 [key6]
%%
%%   Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
search_c_o_c_5_same() ->
    search_c_o_c_5(fun make_c_o_same_mv/0).
search_c_o_c_5_different() ->
    search_c_o_c_5(fun make_c_o_different_mv/0).

search_c_o_c_5(MakeC_O_C_Fun) ->
    {Left, Middle, Right} = make_c_O_c(MakeC_O_C_Fun),

    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Left, "key22", value22)),

    %% check types
    NewBucket = mio_bucket:get_right_op(Right),
    ?assertEqual(c_o_l, mio_bucket:get_type_op(Left)),
    ?assertEqual(c_o_r, mio_bucket:get_type_op(Middle)),
    ?assertEqual(c_o_l, mio_bucket:get_type_op(Right)),
    ?assertEqual(c_o_r, mio_bucket:get_type_op(NewBucket)),

    %% search
    ?assertEqual({ok, value1}, mio_bucket:get_op(Left, "key1")),
    ?assertEqual({ok, value2}, mio_bucket:get_op(Left, "key2")),
    ?assertEqual({ok, value22}, mio_bucket:get_op(Left, "key22")),

    ?assertEqual({ok, value3}, mio_bucket:get_op(Middle, "key3")),
    ?assertEqual({ok, value30}, mio_bucket:get_op(Middle, "key30")),

    ?assertEqual({ok, value31}, mio_bucket:get_op(Right, "key31")),
    ?assertEqual({ok, value4}, mio_bucket:get_op(Right, "key4")),
    ?assertEqual({ok, value5}, mio_bucket:get_op(Right, "key5")),

    ?assertEqual({ok, value6}, mio_bucket:get_op(NewBucket, "key6")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(NewBucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(NewBucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(NewBucket, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(NewBucket, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(NewBucket, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(NewBucket, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(NewBucket, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(NewBucket, "key31")),
    ?assertEqual({ok, value22}, mio_skip_graph:search_op(NewBucket, "key22")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Left, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Left, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Left, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Left, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Left, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Left, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Left, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(Left, "key31")),
    ?assertEqual({ok, value22}, mio_skip_graph:search_op(Left, "key22")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Middle, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Middle, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Middle, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Middle, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Middle, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Middle, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Middle, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(Middle, "key31")),
    ?assertEqual({ok, value22}, mio_skip_graph:search_op(Middle, "key22")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Right, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Right, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Right, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Right, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Right, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Right, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Right, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(Right, "key31")),
    ?assertEqual({ok, value22}, mio_skip_graph:search_op(Right, "key22")).


%% C1-O2$-C3
%%   C1 [key1, key2, key3]
%%   O2$ [key30, key31]
%%   C3 [key4, key5, key6]
%%
%%     becomes
%%
%%   C1 [key1, key2, key3]
%%   O2 [key30, key30]
%%   C3 [key4, key5, key55]
%%   O4 [key6]
%%
%%   Insertion to C3  : C1-O2$ | C3'-O4
search_c_o_c_6_same() ->
    search_c_o_c_6(fun make_c_o_same_mv/0).
search_c_o_c_6_different() ->
    search_c_o_c_6(fun make_c_o_different_mv/0).

search_c_o_c_6(MakeC_O_C_Fun) ->
    {Left, Middle, Right} = make_c_O_c(MakeC_O_C_Fun),

    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Middle, "key55", value55)),

    %% check types
    NewBucket = mio_bucket:get_right_op(Right),
    ?assertEqual(c_o_l, mio_bucket:get_type_op(Left)),
    ?assertEqual(c_o_r, mio_bucket:get_type_op(Middle)),
    ?assertEqual(c_o_l, mio_bucket:get_type_op(Right)),
    ?assertEqual(c_o_r, mio_bucket:get_type_op(NewBucket)),

    %% search
    ?assertEqual({ok, value1}, mio_bucket:get_op(Left, "key1")),
    ?assertEqual({ok, value2}, mio_bucket:get_op(Left, "key2")),
    ?assertEqual({ok, value3}, mio_bucket:get_op(Left, "key3")),

    ?assertEqual({ok, value30}, mio_bucket:get_op(Middle, "key30")),
    ?assertEqual({ok, value31}, mio_bucket:get_op(Middle, "key31")),

    ?assertEqual({ok, value4}, mio_bucket:get_op(Right, "key4")),
    ?assertEqual({ok, value5}, mio_bucket:get_op(Right, "key5")),
    ?assertEqual({ok, value55}, mio_bucket:get_op(Right, "key55")),

    ?assertEqual({ok, value6}, mio_bucket:get_op(NewBucket, "key6")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(NewBucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(NewBucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(NewBucket, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(NewBucket, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(NewBucket, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(NewBucket, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(NewBucket, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(NewBucket, "key31")),
    ?assertEqual({ok, value55}, mio_skip_graph:search_op(NewBucket, "key55")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Left, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Left, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Left, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Left, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Left, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Left, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Left, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(Left, "key31")),
    ?assertEqual({ok, value55}, mio_skip_graph:search_op(Left, "key55")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Middle, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Middle, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Middle, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Middle, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Middle, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Middle, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Middle, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(Middle, "key31")),
    ?assertEqual({ok, value55}, mio_skip_graph:search_op(Middle, "key55")),

    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Right, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Right, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Right, "key3")),
    ?assertEqual({ok, value4}, mio_skip_graph:search_op(Right, "key4")),
    ?assertEqual({ok, value5}, mio_skip_graph:search_op(Right, "key5")),
    ?assertEqual({ok, value6}, mio_skip_graph:search_op(Right, "key6")),
    ?assertEqual({ok, value30}, mio_skip_graph:search_op(Right, "key30")),
    ?assertEqual({ok, value31}, mio_skip_graph:search_op(Right, "key31")),
    ?assertEqual({ok, value55}, mio_skip_graph:search_op(Right, "key55")).

insert_o_1() ->
    {ok, Bucket} = make_bucket(alone),
    ok = mio_skip_graph:insert_op(Bucket, "key1", value1),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")).

insert_o_2() ->
    {ok, Bucket} = make_bucket(alone),
    ok = mio_skip_graph:insert_op(Bucket, "key2", value2),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Bucket, "key2")),

    ok = mio_skip_graph:insert_op(Bucket, "key1", value1),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Bucket, "key2")).

insert_o_3() ->
    {ok, Bucket} = make_bucket(alone),
    ok = mio_skip_graph:insert_op(Bucket, "key2", value2),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Bucket, "key2")),

    ok = mio_skip_graph:insert_op(Bucket, "key1", value1),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Bucket, "key2")),

    ok = mio_skip_graph:insert_op(Bucket, "key3", value3),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")),
    ?assertEqual({ok, value2}, mio_skip_graph:search_op(Bucket, "key2")),
    ?assertEqual({ok, value3}, mio_skip_graph:search_op(Bucket, "key3")).

insert_many() ->
    mio_util:do_times(10, fun do_insert_many/0).
do_insert_many() ->
    ?debugHere,
    {ok, Bucket} = make_bucket(alone),
    ok = mio_skip_graph:insert_op(Bucket, "d4bd50c13cb2c368", "d4bd50c13cb2c368"),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "7b077f782f5239c6", "7b077f782f5239c6"),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "dc3df49638adfd2b", "dc3df49638adfd2b"),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "3a18f2c88d4ef922", "3a18f2c88d4ef922"),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "0177ebdfb54aba4c", "0177ebdfb54aba4c"),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "fc88c7d803f02fce", "fc88c7d803f02fce"),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "ae0aaa8b1c701ea7", "ae0aaa8b1c701ea7"),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "5580da7ca4202565", "5580da7ca4202565"),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "d2043d9b37d14ebf", "d2043d9b37d14ebf"),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "aeaf94f572e71d9c", "aeaf94f572e71d9c"),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "5e4f257214882b66", "5e4f257214882b66"),
%%    mio_skip_graph:dump_op(Bucket),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "f8d5e4182acde4df", "f8d5e4182acde4df"),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "f3a18fe8b425450e", "f3a18fe8b425450e"),
%%    mio_skip_graph:dump_op(Bucket),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "4a0672495168237d", "4a0672495168237d"),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "1923dbe8af615049", "1923dbe8af615049"),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "c12368669cccc56e", "c12368669cccc56e"),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "97d283ca38bd0e1c", "97d283ca38bd0e1c"),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "f2369ca008446bda", "f2369ca008446bda"),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "325af468d6b6b460", "325af468d6b6b460"),
%%    mio_skip_graph:dump_op(Bucket),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "1506b21ba57e6241", "1506b21ba57e6241"),
    ?assertEqual({ok, "1506b21ba57e6241"}, mio_skip_graph:search_op(Bucket, "1506b21ba57e6241")),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "694ff99f8b29aab0", "694ff99f8b29aab0"),
    ?assertEqual({ok, "694ff99f8b29aab0"}, mio_skip_graph:search_op(Bucket, "694ff99f8b29aab0")),
    ?assertEqual({ok, "1506b21ba57e6241"}, mio_skip_graph:search_op(Bucket, "1506b21ba57e6241")),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "f270646a9b74dde0", "f270646a9b74dde0"),
%%    mio_skip_graph:dump_op(Bucket),
    ?assertEqual({ok, "f270646a9b74dde0"}, mio_skip_graph:search_op(Bucket, "f270646a9b74dde0")),
    ?assertEqual({ok, "694ff99f8b29aab0"}, mio_skip_graph:search_op(Bucket, "694ff99f8b29aab0")),
    ?assertEqual({ok, "1506b21ba57e6241"}, mio_skip_graph:search_op(Bucket, "1506b21ba57e6241")),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "3929e7412fa542aa", "3929e7412fa542aa"),
    ?assertEqual({ok, "3929e7412fa542aa"}, mio_skip_graph:search_op(Bucket, "3929e7412fa542aa")),
    ?assertEqual({ok, "f270646a9b74dde0"}, mio_skip_graph:search_op(Bucket, "f270646a9b74dde0")),
    ?assertEqual({ok, "694ff99f8b29aab0"}, mio_skip_graph:search_op(Bucket, "694ff99f8b29aab0")),
    ?assertEqual({ok, "1506b21ba57e6241"}, mio_skip_graph:search_op(Bucket, "1506b21ba57e6241")),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "5022491d338a4ac2", "5022491d338a4ac2"),
%%    mio_skip_graph:dump_op(Bucket),
    ?assertEqual({ok, "5022491d338a4ac2"}, mio_skip_graph:search_op(Bucket, "5022491d338a4ac2")),
    ?assertEqual({ok, "3929e7412fa542aa"}, mio_skip_graph:search_op(Bucket, "3929e7412fa542aa")),
    ?assertEqual({ok, "f270646a9b74dde0"}, mio_skip_graph:search_op(Bucket, "f270646a9b74dde0")),
    ?assertEqual({ok, "694ff99f8b29aab0"}, mio_skip_graph:search_op(Bucket, "694ff99f8b29aab0")),
    ?assertEqual({ok, "1506b21ba57e6241"}, mio_skip_graph:search_op(Bucket, "1506b21ba57e6241")),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "87c3061cf0ed9dae", "87c3061cf0ed9dae"),
%%    mio_skip_graph:dump_op(Bucket),
    ?assertEqual({ok, "87c3061cf0ed9dae"}, mio_skip_graph:search_op(Bucket, "87c3061cf0ed9dae")),
    ?assertEqual({ok, "5022491d338a4ac2"}, mio_skip_graph:search_op(Bucket, "5022491d338a4ac2")),
    ?assertEqual({ok, "3929e7412fa542aa"}, mio_skip_graph:search_op(Bucket, "3929e7412fa542aa")),
    ?assertEqual({ok, "f270646a9b74dde0"}, mio_skip_graph:search_op(Bucket, "f270646a9b74dde0")),
    ?assertEqual({ok, "694ff99f8b29aab0"}, mio_skip_graph:search_op(Bucket, "694ff99f8b29aab0")),
    ?assertEqual({ok, "1506b21ba57e6241"}, mio_skip_graph:search_op(Bucket, "1506b21ba57e6241")),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "72725872d73c8881", "72725872d73c8881"),
    ?assertEqual({ok, "72725872d73c8881"}, mio_skip_graph:search_op(Bucket, "72725872d73c8881")),
    ?assertEqual({ok, "87c3061cf0ed9dae"}, mio_skip_graph:search_op(Bucket, "87c3061cf0ed9dae")),
    ?assertEqual({ok, "5022491d338a4ac2"}, mio_skip_graph:search_op(Bucket, "5022491d338a4ac2")),
    ?assertEqual({ok, "3929e7412fa542aa"}, mio_skip_graph:search_op(Bucket, "3929e7412fa542aa")),
    ?assertEqual({ok, "f270646a9b74dde0"}, mio_skip_graph:search_op(Bucket, "f270646a9b74dde0")),
    ?assertEqual({ok, "694ff99f8b29aab0"}, mio_skip_graph:search_op(Bucket, "694ff99f8b29aab0")),
    ?assertEqual({ok, "1506b21ba57e6241"}, mio_skip_graph:search_op(Bucket, "1506b21ba57e6241")),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "378e1ad721639aa5", "378e1ad721639aa5"),
    ?assertEqual({ok, "378e1ad721639aa5"}, mio_skip_graph:search_op(Bucket, "378e1ad721639aa5")),
    ?assertEqual({ok, "72725872d73c8881"}, mio_skip_graph:search_op(Bucket, "72725872d73c8881")),
    ?assertEqual({ok, "87c3061cf0ed9dae"}, mio_skip_graph:search_op(Bucket, "87c3061cf0ed9dae")),
    ?assertEqual({ok, "5022491d338a4ac2"}, mio_skip_graph:search_op(Bucket, "5022491d338a4ac2")),
    ?assertEqual({ok, "3929e7412fa542aa"}, mio_skip_graph:search_op(Bucket, "3929e7412fa542aa")),
    ?assertEqual({ok, "f270646a9b74dde0"}, mio_skip_graph:search_op(Bucket, "f270646a9b74dde0")),
    ?assertEqual({ok, "694ff99f8b29aab0"}, mio_skip_graph:search_op(Bucket, "694ff99f8b29aab0")),
    ?assertEqual({ok, "1506b21ba57e6241"}, mio_skip_graph:search_op(Bucket, "1506b21ba57e6241")),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "e56afcd4a7841e2c", "e56afcd4a7841e2c"),
    ?assertEqual({ok, "e56afcd4a7841e2c"}, mio_skip_graph:search_op(Bucket, "e56afcd4a7841e2c")),
    ?assertEqual({ok, "378e1ad721639aa5"}, mio_skip_graph:search_op(Bucket, "378e1ad721639aa5")),
    ?assertEqual({ok, "72725872d73c8881"}, mio_skip_graph:search_op(Bucket, "72725872d73c8881")),
    ?assertEqual({ok, "87c3061cf0ed9dae"}, mio_skip_graph:search_op(Bucket, "87c3061cf0ed9dae")),
    ?assertEqual({ok, "5022491d338a4ac2"}, mio_skip_graph:search_op(Bucket, "5022491d338a4ac2")),
    ?assertEqual({ok, "3929e7412fa542aa"}, mio_skip_graph:search_op(Bucket, "3929e7412fa542aa")),
    ?assertEqual({ok, "f270646a9b74dde0"}, mio_skip_graph:search_op(Bucket, "f270646a9b74dde0")),
    ?assertEqual({ok, "694ff99f8b29aab0"}, mio_skip_graph:search_op(Bucket, "694ff99f8b29aab0")),
    ?assertEqual({ok, "1506b21ba57e6241"}, mio_skip_graph:search_op(Bucket, "1506b21ba57e6241")),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "9ad21cf2116c8f12", "9ad21cf2116c8f12"),
    ?assertEqual({ok, "9ad21cf2116c8f12"}, mio_skip_graph:search_op(Bucket, "9ad21cf2116c8f12")),
    ?assertEqual({ok, "e56afcd4a7841e2c"}, mio_skip_graph:search_op(Bucket, "e56afcd4a7841e2c")),
    ?assertEqual({ok, "378e1ad721639aa5"}, mio_skip_graph:search_op(Bucket, "378e1ad721639aa5")),
    ?assertEqual({ok, "72725872d73c8881"}, mio_skip_graph:search_op(Bucket, "72725872d73c8881")),
    ?assertEqual({ok, "87c3061cf0ed9dae"}, mio_skip_graph:search_op(Bucket, "87c3061cf0ed9dae")),
    ?assertEqual({ok, "5022491d338a4ac2"}, mio_skip_graph:search_op(Bucket, "5022491d338a4ac2")),
    ?assertEqual({ok, "3929e7412fa542aa"}, mio_skip_graph:search_op(Bucket, "3929e7412fa542aa")),
    ?assertEqual({ok, "f270646a9b74dde0"}, mio_skip_graph:search_op(Bucket, "f270646a9b74dde0")),
    ?assertEqual({ok, "694ff99f8b29aab0"}, mio_skip_graph:search_op(Bucket, "694ff99f8b29aab0")),
    ?assertEqual({ok, "1506b21ba57e6241"}, mio_skip_graph:search_op(Bucket, "1506b21ba57e6241")),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")),
    ok = mio_skip_graph:insert_op(Bucket, "3538dbd9389a630a", "3538dbd9389a630a"),
    ?assertEqual({ok, "3538dbd9389a630a"}, mio_skip_graph:search_op(Bucket, "3538dbd9389a630a")),
    ?assertEqual({ok, "9ad21cf2116c8f12"}, mio_skip_graph:search_op(Bucket, "9ad21cf2116c8f12")),
    ?assertEqual({ok, "e56afcd4a7841e2c"}, mio_skip_graph:search_op(Bucket, "e56afcd4a7841e2c")),
    ?assertEqual({ok, "378e1ad721639aa5"}, mio_skip_graph:search_op(Bucket, "378e1ad721639aa5")),
    ?assertEqual({ok, "72725872d73c8881"}, mio_skip_graph:search_op(Bucket, "72725872d73c8881")),
    ?assertEqual({ok, "87c3061cf0ed9dae"}, mio_skip_graph:search_op(Bucket, "87c3061cf0ed9dae")),
    ?assertEqual({ok, "5022491d338a4ac2"}, mio_skip_graph:search_op(Bucket, "5022491d338a4ac2")),
    ?assertEqual({ok, "3929e7412fa542aa"}, mio_skip_graph:search_op(Bucket, "3929e7412fa542aa")),
    ?assertEqual({ok, "f270646a9b74dde0"}, mio_skip_graph:search_op(Bucket, "f270646a9b74dde0")),
    ?assertEqual({ok, "694ff99f8b29aab0"}, mio_skip_graph:search_op(Bucket, "694ff99f8b29aab0")),
    ?assertEqual({ok, "1506b21ba57e6241"}, mio_skip_graph:search_op(Bucket, "1506b21ba57e6241")),
    ?assertEqual({ok, "325af468d6b6b460"}, mio_skip_graph:search_op(Bucket, "325af468d6b6b460")),
    ?assertEqual({ok, "f2369ca008446bda"}, mio_skip_graph:search_op(Bucket, "f2369ca008446bda")),
    ?assertEqual({ok, "97d283ca38bd0e1c"}, mio_skip_graph:search_op(Bucket, "97d283ca38bd0e1c")),
    ?assertEqual({ok, "c12368669cccc56e"}, mio_skip_graph:search_op(Bucket, "c12368669cccc56e")),
    ?assertEqual({ok, "1923dbe8af615049"}, mio_skip_graph:search_op(Bucket, "1923dbe8af615049")),
    ?assertEqual({ok, "4a0672495168237d"}, mio_skip_graph:search_op(Bucket, "4a0672495168237d")),
    ?assertEqual({ok, "f3a18fe8b425450e"}, mio_skip_graph:search_op(Bucket, "f3a18fe8b425450e")),
    ?assertEqual({ok, "f8d5e4182acde4df"}, mio_skip_graph:search_op(Bucket, "f8d5e4182acde4df")),
    ?assertEqual({ok, "5e4f257214882b66"}, mio_skip_graph:search_op(Bucket, "5e4f257214882b66")),
    ?assertEqual({ok, "aeaf94f572e71d9c"}, mio_skip_graph:search_op(Bucket, "aeaf94f572e71d9c")),
    ?assertEqual({ok, "d2043d9b37d14ebf"}, mio_skip_graph:search_op(Bucket, "d2043d9b37d14ebf")),
    ?assertEqual({ok, "5580da7ca4202565"}, mio_skip_graph:search_op(Bucket, "5580da7ca4202565")),
    ?assertEqual({ok, "ae0aaa8b1c701ea7"}, mio_skip_graph:search_op(Bucket, "ae0aaa8b1c701ea7")),
    ?assertEqual({ok, "fc88c7d803f02fce"}, mio_skip_graph:search_op(Bucket, "fc88c7d803f02fce")),
    ?assertEqual({ok, "0177ebdfb54aba4c"}, mio_skip_graph:search_op(Bucket, "0177ebdfb54aba4c")),
    ?assertEqual({ok, "3a18f2c88d4ef922"}, mio_skip_graph:search_op(Bucket, "3a18f2c88d4ef922")),
    ?assertEqual({ok, "dc3df49638adfd2b"}, mio_skip_graph:search_op(Bucket, "dc3df49638adfd2b")),
    ?assertEqual({ok, "7b077f782f5239c6"}, mio_skip_graph:search_op(Bucket, "7b077f782f5239c6")),
    ?assertEqual({ok, "d4bd50c13cb2c368"}, mio_skip_graph:search_op(Bucket, "d4bd50c13cb2c368")).


%% Helper


%% C [key1, key2, key3]
%% O []
%% C [key4, key5, key6]
make_c_o_c(MakeC_O_C_Fun) ->
    search_c_o_4(MakeC_O_C_Fun).


%% C [key1, key2, key3]
%% O [key30, key31]
%% C [key4, key5, key6]
make_c_O_c(MakeC_O_C_Fun) ->
    {Left, Middle, Right} = make_c_o_c(MakeC_O_C_Fun),
    ?assertEqual(ok, mio_bucket:insert_op(Middle, "key30", value30)),
    ?assertEqual(ok, mio_bucket:insert_op(Middle, "key31", value31)),
    {Left, Middle, Right}.


%% C [key1, key2, key3]
%% O []
make_c_o_different_mv() ->
    {ok, Bucket} = mio_sup:make_bucket([], 3, alone, [1, 1]),
    mio_bucket:set_gen_mvector_op(Bucket, fun(_Level) -> [0, 1] end),
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key1", value1)),
    ok = mio_bucket:insert_op(Bucket, "key2", value2),
    ok = mio_bucket:insert_op(Bucket, "key3", value3),
    RightBucket = mio_bucket:get_right_op(Bucket),
    {Bucket, RightBucket}.

make_c_o_same_mv() ->
    {ok, Bucket} = mio_sup:make_bucket([], 3, alone, [1, 1]),
    mio_bucket:set_gen_mvector_op(Bucket, fun(_Level) -> [1, 1] end),
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key1", value1)),
    ok = mio_bucket:insert_op(Bucket, "key2", value2),
    ok = mio_bucket:insert_op(Bucket, "key3", value3),

    %% check on level 0
    RightBucket = mio_bucket:get_right_op(Bucket),
    {Bucket, RightBucket}.

make_bucket(Type) ->
    Capacity = 3,
    MaxLevel = 3,
    Allocator = [],
    mio_sup:make_bucket(Allocator, Capacity, Type, MaxLevel).
