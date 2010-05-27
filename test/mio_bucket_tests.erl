%%%-------------------------------------------------------------------
%%% File    : mio_bucket_tests.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 1 Apr 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_bucket_tests).
-include("../src/mio.hrl").
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
      [?_test(delete_c_o())]
     ]
    }.

%% 0$ -> C-O*
insert() ->
    %% set up initial bucket
    Bucket = setup_full_bucket(),
    ok = case mio_bucket:get_right_op(Bucket) of
             [] -> exit({?MODULE, ?LINE, "should not come here"}) ;
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
    Bucket = setup_full_bucket(),

    %% insert to most left of C1
    {ok, _} = mio_bucket:insert_op(Bucket, "key0", value0),
    {ok, value0} = mio_bucket:get_op(Bucket, "key0"),
    {ok, value1} = mio_bucket:get_op(Bucket, "key1"),
    {ok, value2} = mio_bucket:get_op(Bucket, "key2"),
    c_o_l = mio_bucket:get_type_op(Bucket),
    {error, not_found} = mio_bucket:get_op(Bucket, "key3"),

    Right = mio_bucket:get_right_op(Bucket),
    c_o_r = mio_bucket:get_type_op(Right),

    left = get_left_type(Bucket),
    right = get_right_type(mio_bucket:get_right_op(Bucket)),

    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    Bucket.

%% C1-O2 -> C1'-O2'
insert_c_o_2() ->
    %% set up initial bucket
    Bucket = setup_full_bucket(),
    {{MinKey, false}, {MaxKey, true}} = mio_bucket:get_range_op(Bucket),

    %% insert to most left of C1
    {ok, _} = mio_bucket:insert_op(Bucket, "key4", value4),
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
    Bucket = setup_full_bucket(),
    Right = mio_bucket:get_right_op(Bucket),
    {ok, _} = mio_bucket:insert_op(Right, "key4", value4),

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
    Bucket = setup_full_bucket(),
    Right = mio_bucket:get_right_op(Bucket),

    %% right is nearly full
    {ok, _} = mio_bucket:insert_op(Right, "key4", value4),
    {ok, _} = mio_bucket:insert_op(Right, "key5", value5),
    c_o_r = mio_bucket:get_type_op(Right),
    %% insert!
    {ok, _} = mio_bucket:insert_op(Right, "key6", value6),
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
    ?assertEqual({{?MIN_KEY, false}, {"key3", true}}, mio_bucket:get_range_op(Bucket)),
    ?assertEqual({{"key3",false}, {"key4",false}}, mio_bucket:get_range_op(NewRight)),
    ?assertEqual({{"key4", true}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Right)),

    left = get_left_type(Bucket),
    right = get_right_type(Right).

%% C1-O2$ -> C1'-O*-C2
%% Insertion to C1
insert_c_o_5() ->
    %% set up initial bucket
    Bucket = setup_full_bucket(),
    Right = mio_bucket:get_right_op(Bucket),

    %% right is nearly full
    {ok, _} = mio_bucket:insert_op(Right, "key4", value4),
    {ok, _} = mio_bucket:insert_op(Right, "key5", value5),

    %% insert!
    {ok, _} = mio_bucket:insert_op(Bucket, "key0", value0),

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
    ?assertEqual({{?MIN_KEY, false}, {"key2", true}}, mio_bucket:get_range_op(Bucket)),
    ?assertEqual({{"key2", false}, {"key3", false}}, mio_bucket:get_range_op(NewRight)),
    ?assertEqual({{"key3", true}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Right)),

    left = get_left_type(Bucket),
    right = get_right_type(Right),

    {Bucket, NewRight, Right}.

%% C1-O2$ -> C1'-O*-C2
%% Insertion to C1
insert_c_o_6() ->
    %% set up initial bucket
    Bucket = setup_full_bucket(),
    Right = mio_bucket:get_right_op(Bucket),

    %% right is nearly full
    {ok, _} = mio_bucket:insert_op(Right, "key4", value4),
    {ok, _} = mio_bucket:insert_op(Right, "key5", value5),

    %% insert!
    {ok, _} = mio_bucket:insert_op(Bucket, "key31", value31),

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


    %% insert!
    {ok, _} = mio_bucket:insert_op(Left, "key10", value10),

    true = mio_bucket:is_full_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value10} = mio_bucket:get_op(Left, "key10"),

    {ok, value2} = mio_bucket:get_op(Middle, "key2"),

    true = mio_bucket:is_full_op(Right),
    c_o_c_l = mio_bucket:get_type_op(Left),
    c_o_c_m = mio_bucket:get_type_op(Middle),
    c_o_c_r = mio_bucket:get_type_op(Right),

    ok.

%%  C1-O2-C3
%%    Insertion to C1 : C1'-O2'-C3
insert_c_o_c_2() ->
    %% setup C1-O2-C3
    {Left, Middle, Right} = insert_c_o_5(),

    {{LMin, _}, {LMax, _}} = mio_bucket:get_range_op(Left),
    {{MMin, _}, {MMax, _}} = mio_bucket:get_range_op(Middle),
    {{RMin, _}, {RMax, _}} = mio_bucket:get_range_op(Right),

    %% insert!
    {ok, _} = mio_bucket:insert_op(Left, "key3", value3),

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

    {{LMin, _}, {LMax, _}} = mio_bucket:get_range_op(Left),
    {{MMin, _}, {MMax, _}} = mio_bucket:get_range_op(Middle),
    {{RMin, _}, {RMax, _}} = mio_bucket:get_range_op(Right),
    ok.


%%  C1-O2-C3
%%    Insertion to C3 : C1-O2 | C3'-O4
insert_c_o_c_3() ->
    %% setup C1-O2-C3
    {Left, Middle, Right} = insert_c_o_5(),

    %% insert!
    {ok, _} = mio_bucket:insert_op(Right, "key7", value7),

    %% Check C1
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),
    Middle = mio_bucket:get_right_op(Left),
    ?assertEqual({{?MIN_KEY, false}, {"key2", true}}, mio_bucket:get_range_op(Left)),

    %% Check O2
    true = mio_bucket:is_empty_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    ?assertEqual({{"key2", false}, {"key3", false}}, mio_bucket:get_range_op(Middle)),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    {ok, value5} = mio_bucket:get_op(Right, "key5"),
    Middle = mio_bucket:get_left_op(Right),
    ?assertEqual({{"key3", true}, {"key5", true}}, mio_bucket:get_range_op(Right)),

    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value7} = mio_bucket:get_op(MostRight, "key7"),
    Right = mio_bucket:get_left_op(MostRight),
    ?assertEqual({{"key5", false}, {?MAX_KEY, false}},mio_bucket:get_range_op(MostRight)),

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
    {ok, _} = mio_bucket:insert_op(Right, "key22", value22),

    %% Check C1
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),
    Middle = mio_bucket:get_right_op(Left),
    ?assertEqual({{?MIN_KEY, false}, {"key2", true}}, mio_bucket:get_range_op(Left)),

    %% Check O2
    true = mio_bucket:is_empty_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    ?assertEqual({{"key2", false}, {"key22", false}}, mio_bucket:get_range_op(Middle)),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value22} = mio_bucket:get_op(Right, "key22"),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    Middle = mio_bucket:get_left_op(Right),
    ?assertEqual({{"key22", true}, {"key4", true}}, mio_bucket:get_range_op(Right)),

    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value5} = mio_bucket:get_op(MostRight, "key5"),
    Right = mio_bucket:get_left_op(MostRight),
    ?assertEqual({{"key4", false}, {?MAX_KEY, false}}, mio_bucket:get_range_op(MostRight)),

    left = get_left_type(Left),
    right = get_right_type(MostRight),
    MostRight = mio_bucket:get_left_op(mio_bucket:get_right_op(MostRight)),
    ok.


%%  C1-O2-C3
%%    Insertion to O2 : C1-O2'-C3
insert_c_o_c_5() ->
    %% setup C1-O2-C3
    {Left, Middle, Right} = insert_c_o_5(),

    {{LMin, _}, {LMax, _}} = mio_bucket:get_range_op(Left),
    {{MMin, _}, {MMax, _}} = mio_bucket:get_range_op(Middle),
    {{RMin, _}, {RMax, _}} = mio_bucket:get_range_op(Right),

    %% insert!
    {ok, _} = mio_bucket:insert_op(Middle, "key22", value22),

    {{LMin, _}, {LMax, _}} = mio_bucket:get_range_op(Left),
    {{MMin, _}, {MMax, _}} = mio_bucket:get_range_op(Middle),
    {{RMin, _}, {RMax, _}} = mio_bucket:get_range_op(Right),

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
    {ok, _} = mio_bucket:insert_op(Middle, "key21", value21),
    {ok, _} = mio_bucket:insert_op(Middle, "key22", value22),

    %% insert!
    {ok, _} = mio_bucket:insert_op(Left, "key00", value00),

    %% Check C1'
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value00} = mio_bucket:get_op(Left, "key00"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    Middle = mio_bucket:get_right_op(Left),
    ?assertEqual({{?MIN_KEY, false}, {"key1", true}}, mio_bucket:get_range_op(Left)),

    %% Check O2
    false = mio_bucket:is_full_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    {ok, value2} = mio_bucket:get_op(Middle, "key2"),
    {ok, value21} = mio_bucket:get_op(Middle, "key21"),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    ?assertEqual({{"key1", false}, {"key21", true}}, mio_bucket:get_range_op(Middle)),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value22} = mio_bucket:get_op(Right, "key22"),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    Middle = mio_bucket:get_left_op(Right),
    ?assertEqual({{"key21", false}, {"key4", true}}, mio_bucket:get_range_op(Right)),
    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value5} = mio_bucket:get_op(MostRight, "key5"),
    Right = mio_bucket:get_left_op(MostRight),
    ?assertEqual({{"key4", false}, {?MAX_KEY, false}}, mio_bucket:get_range_op(MostRight)),

    left = get_left_type(Left),
    right = get_right_type(MostRight),
    MostRight = mio_bucket:get_left_op(mio_bucket:get_right_op(MostRight)),
    ok.

%%  C1-O2$-C3
%%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
insert_c_o_c_7() ->
    %% setup C1-O2$-C3
    {Left, Middle, Right} = insert_c_o_5(),
    {ok, _} = mio_bucket:insert_op(Middle, "key21", value21),
    {ok, _} = mio_bucket:insert_op(Middle, "key22", value22),

    %% insert!
    {ok, _} = mio_bucket:insert_op(Left, "key20", value20),

    %% Check C1'
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),
    Middle = mio_bucket:get_right_op(Left),
    {{?MIN_KEY, false}, {"key2", true}} = mio_bucket:get_range_op(Left),

    %% Check O2
    false = mio_bucket:is_full_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    {ok, value20} = mio_bucket:get_op(Middle, "key20"),
    {ok, value21} = mio_bucket:get_op(Middle, "key21"),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    {{"key2", false}, {"key21", true}} = mio_bucket:get_range_op(Middle),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value22} = mio_bucket:get_op(Right, "key22"),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    Middle = mio_bucket:get_left_op(Right),
    {{"key21", false}, {"key4", true}} = mio_bucket:get_range_op(Right),

    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value5} = mio_bucket:get_op(MostRight, "key5"),
    Right = mio_bucket:get_left_op(MostRight),
    {{"key4", false}, {?MAX_KEY, false}} = mio_bucket:get_range_op(MostRight),

    left = get_left_type(Left),
    right = get_right_type(MostRight),
    MostRight = mio_bucket:get_left_op(mio_bucket:get_right_op(MostRight)),
    ok.

%%  C1-O2$-C3
%%    Insertion to O2$ : C1-C2-C3 -> C1-O2' | C3'-O4
insert_c_o_c_8() ->
    %% setup C1-O2$-C3
    {Left, Middle, Right} = insert_c_o_5(),
    {ok, _} = mio_bucket:insert_op(Middle, "key21", value21),
    {ok, _} = mio_bucket:insert_op(Middle, "key22", value22),

    c_o_c_m = mio_bucket:get_type_op(Middle),

    %% insert!
    {ok, _} = mio_bucket:insert_op(Middle, "key23", value23),

    %% Check C1'
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),
    Middle = mio_bucket:get_right_op(Left),
    {{?MIN_KEY, false}, {"key2", true}} = mio_bucket:get_range_op(Left),

    %% Check O2
    false = mio_bucket:is_full_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    {ok, value21} = mio_bucket:get_op(Middle, "key21"),
    {ok, value22} = mio_bucket:get_op(Middle, "key22"),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    {{"key2", false}, {"key22", true}} = mio_bucket:get_range_op(Middle),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value23} = mio_bucket:get_op(Right, "key23"),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    Middle = mio_bucket:get_left_op(Right),
    {{"key22", false}, {"key4", true}} = mio_bucket:get_range_op(Right),

    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value5} = mio_bucket:get_op(MostRight, "key5"),
    Right = mio_bucket:get_left_op(MostRight),
    {{"key4", false}, {?MAX_KEY, false}} = mio_bucket:get_range_op(MostRight),

    left = get_left_type(Left),
    right = get_right_type(MostRight),
    MostRight = mio_bucket:get_left_op(mio_bucket:get_right_op(MostRight)),
    ok.



%%  C1-O2$-C3
%%      Insertion to C3  : C1-O2$ | C3'-O4
insert_c_o_c_9() ->
    %% setup C1-O2$-C3
    {Left, Middle, Right} = insert_c_o_5(),
    {ok, _} = mio_bucket:insert_op(Middle, "key21", value21),
    {ok, _} = mio_bucket:insert_op(Middle, "key22", value22),

    c_o_c_m = mio_bucket:get_type_op(Middle),

    %% insert!
    {ok, _} = mio_bucket:insert_op(Right, "key31", value31),

    %% Check C1'
    true = mio_bucket:is_full_op(Left),
    c_o_l = mio_bucket:get_type_op(Left),
    {ok, value0} = mio_bucket:get_op(Left, "key0"),
    {ok, value1} = mio_bucket:get_op(Left, "key1"),
    {ok, value2} = mio_bucket:get_op(Left, "key2"),
    Middle = mio_bucket:get_right_op(Left),
    {{?MIN_KEY, false}, {"key2", true}} = mio_bucket:get_range_op(Left),

    %% Check O2
    false = mio_bucket:is_full_op(Middle),
    c_o_r = mio_bucket:get_type_op(Middle),
    {ok, value21} = mio_bucket:get_op(Middle, "key21"),
    {ok, value22} = mio_bucket:get_op(Middle, "key22"),
    Right = mio_bucket:get_right_op(Middle),
    Left = mio_bucket:get_left_op(Middle),
    {{"key2", false}, {"key3", false}} = mio_bucket:get_range_op(Middle),

    %% Check C3'
    true = mio_bucket:is_full_op(Right),
    c_o_l = mio_bucket:get_type_op(Right),
    {ok, value3} = mio_bucket:get_op(Right, "key3"),
    {ok, value31} = mio_bucket:get_op(Right, "key31"),
    {ok, value4} = mio_bucket:get_op(Right, "key4"),
    Middle = mio_bucket:get_left_op(Right),
    {{"key3", true}, {"key4", true}} = mio_bucket:get_range_op(Right),

    %% Check O4
    MostRight = mio_bucket:get_right_op(Right),
    false = mio_bucket:is_full_op(MostRight),
    c_o_r = mio_bucket:get_type_op(MostRight),
    {ok, value5} = mio_bucket:get_op(MostRight, "key5"),
    Right = mio_bucket:get_left_op(MostRight),
    {{"key4", false}, {?MAX_KEY, false}} = mio_bucket:get_range_op(MostRight),

    left = get_left_type(Left),
    right = get_right_type(MostRight),
    MostRight = mio_bucket:get_left_op(mio_bucket:get_right_op(MostRight)),
    ok.

delete_c_o() ->
    Bucket = insert_c_o_1(),
    ?assertMatch({ok, false},  mio_bucket:delete_op(Bucket, "key1")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(Bucket, "key0")),
    ?assertMatch({error, not_found}, mio_bucket:get_op(Bucket, "key1")),
    ?assertMatch({ok, value2}, mio_bucket:get_op(Bucket, "key2")),
    ?assertMatch({ok, value3}, mio_bucket:get_op(Bucket, "key3")).


setup_full_bucket() ->
    {ok, Bucket} = make_bucket(alone),
    [] = mio_bucket:get_left_op(Bucket),
    [] = mio_bucket:get_right_op(Bucket),
    {ok, _} = mio_bucket:insert_op(Bucket, "key1", value1),
    ?assertMatch({ok, _}, mio_bucket:insert_op(Bucket, "key2", value2)),
    ?assertEqual([], mio_bucket:get_left_op(Bucket)),
    ?assertEqual([], mio_bucket:get_right_op(Bucket)),
    ?assertMatch({ok, _}, mio_bucket:insert_op(Bucket, "key3", value3)),

    %% set dummy type.
    {ok, Right } = make_bucket(right),
    {ok, Left } = make_bucket(left),

    ok = mio_skip_graph:link_left_op(Bucket, 0, Left),
    ok = mio_skip_graph:link_right_op(Left, 0, Bucket),
    ok = mio_skip_graph:link_right_op(mio_bucket:get_right_op(Bucket), 0, Right),
    ok = mio_skip_graph:link_left_op(Right, 0, mio_bucket:get_right_op(Bucket)),
    Bucket.

get_left_type(Bucket) ->
    mio_bucket:get_type_op(mio_bucket:get_left_op(Bucket)).

get_right_type(Bucket) ->
    mio_bucket:get_type_op(mio_bucket:get_right_op(Bucket)).

check_range(Bucket, ExpectedMin, ExpectedMax) ->
    {{ExpectedMin, _}, {ExpectedMax, _}} = mio_bucket:get_range_op(Bucket).

make_bucket(Type) ->
    Capacity = 3,
    MaxLevel = 3,
    Allocator = [],
    mio_sup:make_bucket(Allocator, Capacity, Type, MaxLevel).
