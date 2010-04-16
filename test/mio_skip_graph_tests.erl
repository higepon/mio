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
    ok = mio_app:wait_startup(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    {ok, NodePid} = mio_sup:start_node(myKey, myValue, mio_mvector:make([1, 0])),
    true = register(mio_node, NodePid).

teardown_mio(_) ->
    ok = application:stop(mio).

sg_test_() ->
    {foreach, fun setup_mio/0, fun teardown_mio/1,
     [
      [?_test(search_o())],
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
      [?_test(insert_1())]

      ]
     }.

search_o() ->
    Capacity = 3,
    {ok, Bucket} = mio_sup:make_bucket(Capacity, alone),
    {error, not_found} = mio_skip_graph:search_op(Bucket, "key").

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
    ?assertEqual({error, not_found}, mio_skip_graph:search_op(Bucket, "key0")),
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

insert_1() ->
    Capacity = 3,
    {ok, Bucket} = mio_sup:make_bucket(Capacity, alone),
    ok = mio_skip_graph:insert_op(Bucket, "key1", value1),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")).

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
    {ok, Bucket} = mio_sup:make_bucket(3, alone, [1, 1]),
    mio_bucket:set_gen_mvector_op(Bucket, fun(_Level) -> [0, 1] end),
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key1", value1)),
    ok = mio_bucket:insert_op(Bucket, "key2", value2),
    ok = mio_bucket:insert_op(Bucket, "key3", value3),
    RightBucket = mio_bucket:get_right_op(Bucket),
    {Bucket, RightBucket}.

make_c_o_same_mv() ->
    {ok, Bucket} = mio_sup:make_bucket(3, alone, [1, 1]),
    mio_bucket:set_gen_mvector_op(Bucket, fun(_Level) -> [1, 1] end),
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key1", value1)),
    ok = mio_bucket:insert_op(Bucket, "key2", value2),
    ok = mio_bucket:insert_op(Bucket, "key3", value3),

    %% check on level 0
    RightBucket = mio_bucket:get_right_op(Bucket),
    {Bucket, RightBucket}.
