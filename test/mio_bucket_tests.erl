%%%-------------------------------------------------------------------
%%% File    : mio_bucket_tests.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 1 Apr 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_bucket_tests).
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
      [?_test(insert())],
      [?_test(insert_c_o_1())],
      [?_test(insert_c_o_2())],
      [?_test(insert_c_o_3())],
      [?_test(insert_c_o_4())],
      [?_test(insert_c_o_5())],
      [?_test(insert_c_o_6())],
      [?_test(insert_c_o_c_1())],
      [?_test(insert_c_o_c_2())],
      [?_test(insert_c_o_c_3())],
      [?_test(insert_c_o_c_4())],
      [?_test(insert_c_o_c_5())],
      [?_test(insert_c_o_c_6())],
      [?_test(insert_c_o_c_7())],
      [?_test(insert_c_o_c_8())],
      [?_test(insert_c_o_c_9())],
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
      [?_test(search_c_o_c_1())],
      [?_test(search_c_o_c_2())]
     ]
    }.

%% 0$ -> C-O*
insert() ->
    %% set up initial bucket
    Bucket = setup_full_bucket(3),
    ok = case mio_bucket:get_right_op(Bucket) of
             [] -> ?assert(false);
             RightBucket ->
                 ?assert(mio_bucket:is_empty_op(RightBucket)),
                 ?assertEqual(left, get_left_type(Bucket)),
                 ?assertEqual(right, get_right_type(RightBucket)),
                 ?assertEqual(Bucket, mio_bucket:get_left_op(RightBucket)),
                 check_range(Bucket, ?MIN_KEY, "key3"),
                 check_range(RightBucket, "key3", ?MAX_KEY),
                 ok
         end.


%% C1-O2 -> C1'-O2'
insert_c_o_1() ->
    %% set up initial bucket
    Bucket = setup_full_bucket(3),

    %% insert to most left of C1
    ok = mio_bucket:insert_op(Bucket, "key0", value0),
    {ok, value0} = mio_bucket:get_op(Bucket, "key0"),
    {ok, value1} = mio_bucket:get_op(Bucket, "key1"),
    {ok, value2} = mio_bucket:get_op(Bucket, "key2"),
    c_o_l = mio_bucket:get_type_op(Bucket),
    {error, not_found} = mio_bucket:get_op(Bucket, "key3"),

    Right = mio_bucket:get_right_op(Bucket),
    c_o_r = mio_bucket:get_type_op(Right),

    left = get_left_type(Bucket),
    right = get_right_type(mio_bucket:get_right_op(Bucket)),

    {ok, value3} = mio_bucket:get_op(Right, "key3").

%% C1-O2 -> C1'-O2'
insert_c_o_2() ->
    %% set up initial bucket
    Bucket = setup_full_bucket(3),
    {MinKey, MaxKey} = mio_bucket:get_range_op(Bucket),

    %% insert to most left of C1
    ok = mio_bucket:insert_op(Bucket, "key4", value4),
    {ok, value1} = mio_bucket:get_op(Bucket, "key1"),
    {ok, value2} = mio_bucket:get_op(Bucket, "key2"),
    {ok, value3} = mio_bucket:get_op(Bucket, "key3"),
    c_o_l = mio_bucket:get_type_op(Bucket),
    {error, not_found} = mio_bucket:get_op(Bucket, "key4"),
    check_range(Bucket, MinKey, MaxKey),

    Right = mio_bucket:get_right_op(Bucket),
    c_o_r = mio_bucket:get_type_op(Right),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    left = get_left_type(Bucket),
    right = get_right_type(mio_bucket:get_right_op(Bucket)).

%% C1-O2 -> C1-O2'
insert_c_o_3() ->
    %% set up initial bucket
    Bucket = setup_full_bucket(3),
    Right = mio_bucket:get_right_op(Bucket),
    ok = mio_bucket:insert_op(Right, "key4", value4),

    c_o_l = mio_bucket:get_type_op(Bucket),
    {ok, value1} = mio_bucket:get_op(Bucket, "key1"),
    {ok, value2} = mio_bucket:get_op(Bucket, "key2"),
    {ok, value3} = mio_bucket:get_op(Bucket, "key3"),
    c_o_r = mio_bucket:get_type_op(Right),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    left = get_left_type(Bucket),
    right = get_right_type(mio_bucket:get_right_op(Bucket)).

%% C1-O2$ -> C1-O*-C2
insert_c_o_4() ->
    %% set up initial bucket
    Bucket = setup_full_bucket(3),
    Right = mio_bucket:get_right_op(Bucket),

    %% right is nearly full
    ok = mio_bucket:insert_op(Right, "key4", value4),
    ok = mio_bucket:insert_op(Right, "key5", value5),
    c_o_r = mio_bucket:get_type_op(Right),

    %% insert!
    ok = mio_bucket:insert_op(Right, "key6", value6),

    c_o_c_l = mio_bucket:get_type_op(Bucket),
    c_o_c_r = mio_bucket:get_type_op(Right),

    NewRight = mio_bucket:get_right_op(Bucket),
    true = mio_bucket:is_empty_op(NewRight),
    c_o_c_m = mio_bucket:get_type_op(NewRight),

    Bucket = mio_bucket:get_left_op(NewRight),
    Right = mio_bucket:get_right_op(NewRight),
    NewRight = mio_bucket:get_left_op(Right),

    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    {ok, value5} = mio_bucket:get_op(Right, "key5"),
    {ok, value6} = mio_bucket:get_op(Right, "key6"),

    %%  C1(C1_min, C1_stored_max)
    %%  O*(C1_stored_max, O2_min)
    %%  C2(O2_min, O2_max)
    {?MIN_KEY, "key3"} = mio_bucket:get_range_op(Bucket),
    {"key3", "key4"} = mio_bucket:get_range_op(NewRight),
    {"key4", ?MAX_KEY} =  mio_bucket:get_range_op(Right),

    left = get_left_type(Bucket),
    right = get_right_type(Right).

%% C1-O2$ -> C1'-O*-C2
%% Insertion to C1
insert_c_o_5() ->
    %% set up initial bucket
    Bucket = setup_full_bucket(3),
    Right = mio_bucket:get_right_op(Bucket),

    %% right is nearly full
    ok = mio_bucket:insert_op(Right, "key4", value4),
    ok = mio_bucket:insert_op(Right, "key5", value5),

    %% insert!
    ok = mio_bucket:insert_op(Bucket, "key0", value0),

    true = mio_bucket:is_full_op(Bucket),
    true = mio_bucket:is_full_op(Right),

    NewRight = mio_bucket:get_right_op(Bucket),
    true = mio_bucket:is_empty_op(NewRight),

    Bucket = mio_bucket:get_left_op(NewRight),
    c_o_c_l = mio_bucket:get_type_op(Bucket),
    Right = mio_bucket:get_right_op(NewRight),
    c_o_c_r = mio_bucket:get_type_op(Right),
    NewRight = mio_bucket:get_left_op(Right),
    c_o_c_m = mio_bucket:get_type_op(NewRight),

    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    {ok, value5} = mio_bucket:get_op(Right, "key5"),
    {ok, value0} = mio_bucket:get_op(Bucket, "key0"),
    {ok, value1} = mio_bucket:get_op(Bucket, "key1"),
    {ok, value2} = mio_bucket:get_op(Bucket, "key2"),

    %%  C1(C1_min, C1_stored_max)
    %%  O*(C1_stored_max, O2_min)
    %%  C2(O2_min, O2_max)
    {?MIN_KEY, "key2"} = mio_bucket:get_range_op(Bucket),
    {"key2", "key3"} = mio_bucket:get_range_op(NewRight),
    {"key3", ?MAX_KEY} =  mio_bucket:get_range_op(Right),

    left = get_left_type(Bucket),
    right = get_right_type(Right),

    {Bucket, NewRight, Right}.

%% C1-O2$ -> C1'-O*-C2
%% Insertion to C1
insert_c_o_6() ->
    %% set up initial bucket
    Bucket = setup_full_bucket(3),
    Right = mio_bucket:get_right_op(Bucket),

    %% right is nearly full
    ok = mio_bucket:insert_op(Right, "key4", value4),
    ok = mio_bucket:insert_op(Right, "key5", value5),

    %% insert!
    ok = mio_bucket:insert_op(Bucket, "key31", value31),

    true = mio_bucket:is_full_op(Bucket),
    true = mio_bucket:is_full_op(Right),

    NewRight = mio_bucket:get_right_op(Bucket),
    true = mio_bucket:is_empty_op(NewRight),

    Bucket = mio_bucket:get_left_op(NewRight),
    Right = mio_bucket:get_right_op(NewRight),
    NewRight = mio_bucket:get_left_op(Right),

    {ok, value31} = mio_bucket:get_op(Right, "key31"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    {ok, value5} = mio_bucket:get_op(Right, "key5"),
    {ok, value1} = mio_bucket:get_op(Bucket, "key1"),
    {ok, value2} = mio_bucket:get_op(Bucket, "key2"),
    {ok, value3} = mio_bucket:get_op(Bucket, "key3"),

    left = get_left_type(Bucket),
    right = get_right_type(Right),

    {Bucket, NewRight, Right}.


%%  C1-O2-C3
%%    Insertion to C1 : C1'-O2'-C3
insert_c_o_c_1() ->
    %% setup C1-O2-C3
    {Left, Middle, Right} = insert_c_o_5(),

    {LMin, LMax} = mio_bucket:get_range_op(Left),
    {MMin, MMax} = mio_bucket:get_range_op(Middle),
    {RMin, RMax} = mio_bucket:get_range_op(Right),

    %% insert!
    ok = mio_bucket:insert_op(Left, "key10", value10),

    true = mio_bucket:is_full_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value10} = mio_bucket:get_op(Left, "key10"),

    {ok, value2} = mio_bucket:get_op(Middle, "key2"),

    true = mio_bucket:is_full_op(Right),
    c_o_c_l = mio_bucket:get_type_op(Left),
    c_o_c_m = mio_bucket:get_type_op(Middle),
    c_o_c_r = mio_bucket:get_type_op(Right),

    {LMin, LMax} = mio_bucket:get_range_op(Left),
    {MMin, MMax} = mio_bucket:get_range_op(Middle),
    {RMin, RMax} = mio_bucket:get_range_op(Right),
    ok.

%%  C1-O2-C3
%%    Insertion to C1 : C1'-O2'-C3
insert_c_o_c_2() ->
    %% setup C1-O2-C3
    {Left, Middle, Right} = insert_c_o_5(),

    {LMin, LMax} = mio_bucket:get_range_op(Left),
    {MMin, MMax} = mio_bucket:get_range_op(Middle),
    {RMin, RMax} = mio_bucket:get_range_op(Right),

    %% insert!
    ok = mio_bucket:insert_op(Left, "key3", value3),

    true = mio_bucket:is_full_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),

    {ok, value3} = mio_bucket:get_op(Middle, "key3"),

    true = mio_bucket:is_full_op(Right),
    c_o_c_l = mio_bucket:get_type_op(Left),
    c_o_c_m = mio_bucket:get_type_op(Middle),
    c_o_c_r = mio_bucket:get_type_op(Right),

    left = get_left_type(Left),
    right = get_right_type(Right),

    {LMin, LMax} = mio_bucket:get_range_op(Left),
    {MMin, MMax} = mio_bucket:get_range_op(Middle),
    {RMin, RMax} = mio_bucket:get_range_op(Right),
    ok.


%%  C1-O2-C3
%%    Insertion to C3 : C1-O2 | C3'-O4
insert_c_o_c_3() ->
    %% setup C1-O2-C3
    {Left, Middle, Right} = insert_c_o_5(),

    %% insert!
    ok = mio_bucket:insert_op(Right, "key7", value7),

    %% Check C1
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),
    Middle = mio_bucket:get_right_op(Left),
    {?MIN_KEY, "key2"} = mio_bucket:get_range_op(Left),

    %% Check O2
    true = mio_bucket:is_empty_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    {"key2", "key3"} = mio_bucket:get_range_op(Middle),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    {ok, value5} = mio_bucket:get_op(Right, "key5"),
    Middle = mio_bucket:get_left_op(Right),
    {"key3", "key5"} = mio_bucket:get_range_op(Right),

    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value7} = mio_bucket:get_op(MostRight, "key7"),
    Right = mio_bucket:get_left_op(MostRight),
    {"key5", ?MAX_KEY} = mio_bucket:get_range_op(MostRight),

    left = get_left_type(Left),
    right = get_right_type(MostRight),
    MostRight = mio_bucket:get_left_op(mio_bucket:get_right_op(MostRight)),
    ok.

%%  C1-O2-C3
%%    Insertion to C3 : C1-O2'-C3'
insert_c_o_c_4() ->
    %% setup C1-O2-C3
    {Left, Middle, Right} = insert_c_o_5(),

    %% insert!
    ok = mio_bucket:insert_op(Right, "key22", value22),

    %% Check C1
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),
    Middle = mio_bucket:get_right_op(Left),
    {?MIN_KEY, "key2"} = mio_bucket:get_range_op(Left),

    %% Check O2
    true = mio_bucket:is_empty_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    {"key2", "key3"} = mio_bucket:get_range_op(Middle),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value22} = mio_bucket:get_op(Right, "key22"),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    Middle = mio_bucket:get_left_op(Right),
    {"key3", "key4"} = mio_bucket:get_range_op(Right),

    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value5} = mio_bucket:get_op(MostRight, "key5"),
    Right = mio_bucket:get_left_op(MostRight),
    {"key4", ?MAX_KEY} = mio_bucket:get_range_op(MostRight),

    left = get_left_type(Left),
    right = get_right_type(MostRight),
    MostRight = mio_bucket:get_left_op(mio_bucket:get_right_op(MostRight)),
    ok.


%%  C1-O2-C3
%%    Insertion to O2 : C1-O2'-C3
insert_c_o_c_5() ->
    %% setup C1-O2-C3
    {Left, Middle, Right} = insert_c_o_5(),

    {LMin, LMax} = mio_bucket:get_range_op(Left),
    {MMin, MMax} = mio_bucket:get_range_op(Middle),
    {RMin, RMax} = mio_bucket:get_range_op(Right),

    %% insert!
    ok = mio_bucket:insert_op(Middle, "key22", value22),

    {LMin, LMax} = mio_bucket:get_range_op(Left),
    {MMin, MMax} = mio_bucket:get_range_op(Middle),
    {RMin, RMax} = mio_bucket:get_range_op(Right),

    true = mio_bucket:is_full_op(Right),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    {ok, value5} = mio_bucket:get_op(Right, "key5"),

    {ok, value22} = mio_bucket:get_op(Middle, "key22"),

    true = mio_bucket:is_full_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),
    c_o_c_l = mio_bucket:get_type_op(Left),
    c_o_c_m = mio_bucket:get_type_op(Middle),
    c_o_c_r = mio_bucket:get_type_op(Right),

    left = get_left_type(Left),
    right = get_right_type(Right),
    ok.

%%  C1-O2$-C3
%%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
insert_c_o_c_6() ->
    %% setup C1-O2$-C3
    {Left, Middle, Right} = insert_c_o_5(),
    ok = mio_bucket:insert_op(Middle, "key21", value21),
    ok = mio_bucket:insert_op(Middle, "key22", value22),

    %% insert!
    ok = mio_bucket:insert_op(Left, "key00", value00),

    %% Check C1'
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value00} = mio_bucket:get_op(Left, "key00"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    Middle = mio_bucket:get_right_op(Left),
    {?MIN_KEY, "key1"} = mio_bucket:get_range_op(Left),

    %% Check O2
    false = mio_bucket:is_full_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    {ok, value2} = mio_bucket:get_op(Middle, "key2"),
    {ok, value21} = mio_bucket:get_op(Middle, "key21"),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    {"key1", "key21"} = mio_bucket:get_range_op(Middle),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value22} = mio_bucket:get_op(Right, "key22"),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    Middle = mio_bucket:get_left_op(Right),
    {"key21", "key4"} = mio_bucket:get_range_op(Right),
    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value5} = mio_bucket:get_op(MostRight, "key5"),
    Right = mio_bucket:get_left_op(MostRight),
    {"key4", ?MAX_KEY} = mio_bucket:get_range_op(MostRight),

    left = get_left_type(Left),
    right = get_right_type(MostRight),
    MostRight = mio_bucket:get_left_op(mio_bucket:get_right_op(MostRight)),
    ok.

%%  C1-O2$-C3
%%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
insert_c_o_c_7() ->
    %% setup C1-O2$-C3
    {Left, Middle, Right} = insert_c_o_5(),
    ok = mio_bucket:insert_op(Middle, "key21", value21),
    ok = mio_bucket:insert_op(Middle, "key22", value22),

    %% insert!
    ok = mio_bucket:insert_op(Left, "key20", value20),

    %% Check C1'
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),
    Middle = mio_bucket:get_right_op(Left),
    {?MIN_KEY, "key2"} = mio_bucket:get_range_op(Left),

    %% Check O2
    false = mio_bucket:is_full_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    {ok, value20} = mio_bucket:get_op(Middle, "key20"),
    {ok, value21} = mio_bucket:get_op(Middle, "key21"),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    {"key2", "key21"} = mio_bucket:get_range_op(Middle),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value22} = mio_bucket:get_op(Right, "key22"),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    Middle = mio_bucket:get_left_op(Right),
    {"key21", "key4"} = mio_bucket:get_range_op(Right),

    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value5} = mio_bucket:get_op(MostRight, "key5"),
    Right = mio_bucket:get_left_op(MostRight),
    {"key4", ?MAX_KEY} = mio_bucket:get_range_op(MostRight),

    left = get_left_type(Left),
    right = get_right_type(MostRight),
    MostRight = mio_bucket:get_left_op(mio_bucket:get_right_op(MostRight)),
    ok.

%%  C1-O2$-C3
%%    Insertion to O2$ : C1-C2-C3 -> C1-O2' | C3'-O4
insert_c_o_c_8() ->
    %% setup C1-O2$-C3
    {Left, Middle, Right} = insert_c_o_5(),
    ok = mio_bucket:insert_op(Middle, "key21", value21),
    ok = mio_bucket:insert_op(Middle, "key22", value22),

    c_o_c_m = mio_bucket:get_type_op(Middle),

    %% insert!
    ok = mio_bucket:insert_op(Middle, "key23", value23),

    %% Check C1'
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),
    Middle = mio_bucket:get_right_op(Left),
    {?MIN_KEY, "key2"} = mio_bucket:get_range_op(Left),

    %% Check O2
    false = mio_bucket:is_full_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    {ok, value21} = mio_bucket:get_op(Middle, "key21"),
    {ok, value22} = mio_bucket:get_op(Middle, "key22"),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    {"key2", "key22"} = mio_bucket:get_range_op(Middle),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value23} = mio_bucket:get_op(Right, "key23"),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    Middle = mio_bucket:get_left_op(Right),
    {"key22", "key4"} = mio_bucket:get_range_op(Right),

    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value5} = mio_bucket:get_op(MostRight, "key5"),
    Right = mio_bucket:get_left_op(MostRight),
    {"key4", ?MAX_KEY} = mio_bucket:get_range_op(MostRight),

    left = get_left_type(Left),
    right = get_right_type(MostRight),
    MostRight = mio_bucket:get_left_op(mio_bucket:get_right_op(MostRight)),
    ok.



%%  C1-O2$-C3
%%      Insertion to C3  : C1-O2$ | C3'-O4
insert_c_o_c_9() ->
    %% setup C1-O2$-C3
    {Left, Middle, Right} = insert_c_o_5(),
    ok = mio_bucket:insert_op(Middle, "key21", value21),
    ok = mio_bucket:insert_op(Middle, "key22", value22),

    c_o_c_m = mio_bucket:get_type_op(Middle),

    %% insert!
    ok = mio_bucket:insert_op(Right, "key31", value31),

    %% Check C1'
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),
    Middle = mio_bucket:get_right_op(Left),
    {?MIN_KEY, "key2"} = mio_bucket:get_range_op(Left),

    %% Check O2
    false = mio_bucket:is_full_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    {ok, value21} = mio_bucket:get_op(Middle, "key21"),
    {ok, value22} = mio_bucket:get_op(Middle, "key22"),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    {"key2", "key3"} = mio_bucket:get_range_op(Middle),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value31} = mio_bucket:get_op(Right, "key31"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    Middle = mio_bucket:get_left_op(Right),
    {"key3", "key4"} = mio_bucket:get_range_op(Right),

    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value5} = mio_bucket:get_op(MostRight, "key5"),
    Right = mio_bucket:get_left_op(MostRight),
    {"key4", ?MAX_KEY} = mio_bucket:get_range_op(MostRight),

    left = get_left_type(Left),
    right = get_right_type(MostRight),
    MostRight = mio_bucket:get_left_op(mio_bucket:get_right_op(MostRight)),
    ok.

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
    ?assertEqual(?MAX_KEY, mio_skip_graph:get_key_op(RightBucket)),
    ?assertEqual([], mio_bucket:get_left_op(Bucket, 0)),
    ?assertEqual([], mio_bucket:get_right_op(RightBucket, 0)),
    ?assertEqual(Bucket, mio_bucket:get_left_op(RightBucket, 0)),
    ?assertEqual("key3", mio_skip_graph:get_key_op(Bucket)),

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
    ?assertEqual({?MIN_KEY, "key2"}, mio_bucket:get_range_op(Bucket)),
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
    ?assertEqual({?MIN_KEY, "key1"}, mio_bucket:get_range_op(Bucket)),
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
    ?assertEqual({?MIN_KEY, "key02"}, mio_bucket:get_range_op(Bucket)),
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

    ?assertEqual({?MIN_KEY, "key3"}, mio_bucket:get_range_op(Bucket)),
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
search_c_o_c_1() ->
    {Left, Middle, Right} = make_c_o_c(),

    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Left, "key0", value0)),

    {ok, value0} = mio_bucket:get_op(Left, "key0"),
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
%%   Insertion to C3 : C1-O2'-C3'
search_c_o_c_2() ->
    {Left, Middle, Right} = make_c_o_c(),

    %% insert
    ?assertEqual(ok, mio_bucket:insert_op(Right, "key7", value7)),

    ?assertEqual({ok, value7}, mio_bucket:get_op(Right, "key7")),
    ?assertEqual({ok, value4}, mio_bucket:get_op(Middle, "key4")),

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
%%   Insertion to O2 : C1-O2 | C3'-O4

%% C1-O2$-C3
%%   Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4

%% C1-O2$-C3
%%   Insertion to O2$ : C1-C2-C3 -> C1-O2' | C3'-O4

%% C1-O2$-C3
%%   Insertion to C3  : C1-O2$ | C3'-O4


%% Helper

%% C [key1, key2, key3]
%% O []
%% C [key4, key5, key6]
make_c_o_c() ->
    search_c_o_4(fun make_c_o_different_mv/0).

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

setup_full_bucket(Capacity) ->
    {ok, Bucket} = mio_sup:make_bucket(Capacity, alone),
    [] = mio_bucket:get_left_op(Bucket),
    [] = mio_bucket:get_right_op(Bucket),
    ok = mio_bucket:insert_op(Bucket, "key1", value1),
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key2", value2)),
    ?assertEqual([], mio_bucket:get_left_op(Bucket)),
    ?assertEqual([], mio_bucket:get_right_op(Bucket)),
    ?assertEqual(ok, mio_bucket:insert_op(Bucket, "key3", value3)),

    %% set dummy type.
    {ok, Right } = mio_sup:make_bucket(Capacity, right),
    {ok, Left } = mio_sup:make_bucket(Capacity, left),

    ok = mio_bucket:link_left_op(Bucket, 0, Left),
    ok = mio_bucket:link_right_op(Left, 0, Bucket),
    ok = mio_bucket:link_right_op(mio_bucket:get_right_op(Bucket), 0, Right),
    ok = mio_bucket:link_left_op(Right, 0, mio_bucket:get_right_op(Bucket)),
    Bucket.

get_left_type(Bucket) ->
    mio_bucket:get_type_op(mio_bucket:get_left_op(Bucket)).

get_right_type(Bucket) ->
    mio_bucket:get_type_op(mio_bucket:get_right_op(Bucket)).

check_range(Bucket, ExpectedMin, ExpectedMax) ->
    {ExpectedMin, ExpectedMax} = mio_bucket:get_range_op(Bucket).
