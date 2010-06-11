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
      [?_test(more_coverage())], %% should be first, this test does re-covercompile.
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
      [?_test(delete_o())],
      [?_test(delete_o_empty())],
      [?_test(delete_c_o_1())],
      [?_test(delete_c_o_2())],
      [?_test(delete_c_o_c_1())],
      [?_test(delete_c_o_c_2())],
      [?_test(delete_c_o_c_3())],
      [?_test(delete_c_O_c_1())],
      [?_test(delete_c_O_c_2())],
      [?_test(delete_c_O_1())],
      [?_test(delete_c_O_2())],
      [?_test(delete_c_O_3())],
      [?_test(delete_c_O_4())],
      [?_test(delete_c_O_5())],
      [?_test(delete_c_O_6())],
      [?_test(delete_c_O_7())],
      [?_test(delete_c_O_8())],
      [?_test(delete_c_O_9())]
     ]
    }.

more_coverage() ->
    {ok, Bucket} = mio_bucket:start_link([1, 1, 1, []]),
    ?assertEqual(ok, mio_util:for_better_coverage(mio_bucket, Bucket)),
    process_flag(trap_exit, true),
    exit(Bucket, normal).

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
    {Bucket, Right}.

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
    {Left, Middle, Right}.

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

%%  O1*
%%    O1* -> O1*
delete_o_empty() ->

    %% []
    {ok, Bucket} = make_bucket(alone),

    %% []
    ?assertMatch({error, not_found}, mio_bucket:delete_op(Bucket, "key1")),

    ?assertMatch({error, not_found}, mio_bucket:get_op(Bucket, "key1")),

    %% type
    ?assertMatch(alone, mio_bucket:get_type_op(Bucket)),

    %% range
    ?assertMatch({{?MIN_KEY, false}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Bucket)).


%%  O1
%%    O1 -> O2 or O2*
delete_o() ->

    %% [key1]
    {ok, Bucket} = make_bucket(alone),
    {ok, _} = mio_bucket:insert_op(Bucket, "key1", value1),

    ?assertMatch({ok, value1}, mio_bucket:get_op(Bucket, "key1")),

    %% []
    ?assertMatch({ok, []}, mio_bucket:delete_op(Bucket, "key1")),

    ?assertMatch({error, not_found}, mio_bucket:get_op(Bucket, "key1")),

    %% type
    ?assertMatch(alone, mio_bucket:get_type_op(Bucket)),

    %% range
    ?assertMatch({{?MIN_KEY, false}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Bucket)).

%%  C1-O2
%%    Deletion from C1: C1'-O2'
delete_c_o_1() ->
    %% [0 1 2] [3]
    {Bucket, Right} = insert_c_o_1(),

    %% [0 2 3] []
    ?assertMatch({ok, []}, mio_bucket:delete_op(Bucket, "key1")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(Bucket, "key0")),
    ?assertMatch({error, not_found}, mio_bucket:get_op(Bucket, "key1")),
    ?assertMatch({ok, value2}, mio_bucket:get_op(Bucket, "key2")),
    ?assertMatch({ok, value3}, mio_bucket:get_op(Bucket, "key3")),

    ?assertMatch({error, not_found}, mio_bucket:get_op(Right, "key3")),
    ?assertMatch(true, mio_bucket:is_empty_op(Right)),

    %% type
    ?assertMatch(c_o_l, mio_bucket:get_type_op(Bucket)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(Right)),

    %% range
    ?assertMatch({{?MIN_KEY, false}, {"key3", true}}, mio_bucket:get_range_op(Bucket)),
    ?assertMatch({{"key3", false}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Right)).

%%  C1-O2
%%    Deletion from O2: C1-O2'
delete_c_o_2() ->
    %% [0 1 2] [3]
    {Bucket, Right} = insert_c_o_1(),

    %% [0 1 2] []
    ?assertMatch({ok, []}, mio_bucket:delete_op(Right, "key3")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(Bucket, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(Bucket, "key1")),
    ?assertMatch({ok, value2}, mio_bucket:get_op(Bucket, "key2")),

    ?assertMatch({error, not_found}, mio_bucket:get_op(Right, "key3")),
    ?assertMatch(true, mio_bucket:is_empty_op(Right)),

    %% type
    ?assertMatch(c_o_l, mio_bucket:get_type_op(Bucket)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(Right)),

    %% range
    ?assertMatch({{?MIN_KEY, false}, {"key2", true}}, mio_bucket:get_range_op(Bucket)),
    ?assertMatch({{"key2", false}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Right)).

%%  C1-O2-C3
%%    Deletion from C1: C1'-O2'-C3
delete_c_o_c_1() ->
    %% [0, 1, 10] [2] [3, 4, 5]
    {Left, Middle, Right} = insert_c_o_c_1(),
    ?assertMatch({ok, value10}, mio_bucket:get_op(Left, "key10")),

    %% [0, 1, 2] [] [3, 4, 5]
    ?assertMatch({ok, []}, mio_bucket:delete_op(Left, "key10")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(Left, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(Left, "key1")),
    ?assertMatch({ok, value2}, mio_bucket:get_op(Left, "key2")),
    ?assertMatch({error, not_found}, mio_bucket:get_op(Left, "key10")),

    ?assertMatch(true, mio_bucket:is_empty_op(Middle)),

    ?assertMatch({ok, value3}, mio_bucket:get_op(Right, "key3")),
    ?assertMatch({ok, value4}, mio_bucket:get_op(Right, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(Right, "key5")),

    %% type
    ?assertMatch(c_o_c_l, mio_bucket:get_type_op(Left)),
    ?assertMatch(c_o_c_m, mio_bucket:get_type_op(Middle)),
    ?assertMatch(c_o_c_r, mio_bucket:get_type_op(Right)),

    %% range
    ?assertMatch({{?MIN_KEY, false}, {"key2", true}}, mio_bucket:get_range_op(Left)),
    ?assertMatch({{"key2", false}, {"key3", false}}, mio_bucket:get_range_op(Middle)),
    ?assertMatch({{"key3", true}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Right)).

%%  C1-O2-C3
%%    Deletion from O2: C1-O2'-C3
delete_c_o_c_2() ->
    %% [0, 1, 10] [2] [3, 4, 5]
    {Left, Middle, Right} = insert_c_o_c_1(),

    %% [0, 1, 10] [] [3, 4, 5]
    ?assertMatch({ok, []}, mio_bucket:delete_op(Middle, "key2")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(Left, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(Left, "key1")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(Left, "key10")),
    ?assertMatch({error, not_found}, mio_bucket:get_op(Middle, "key2")),

    ?assertMatch(true, mio_bucket:is_empty_op(Middle)),

    ?assertMatch({ok, value3}, mio_bucket:get_op(Right, "key3")),
    ?assertMatch({ok, value4}, mio_bucket:get_op(Right, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(Right, "key5")),

    %% type
    ?assertMatch(c_o_c_l, mio_bucket:get_type_op(Left)),
    ?assertMatch(c_o_c_m, mio_bucket:get_type_op(Middle)),
    ?assertMatch(c_o_c_r, mio_bucket:get_type_op(Right)),

    %% range
    ?assertMatch({{?MIN_KEY, false}, {"key10", true}}, mio_bucket:get_range_op(Left)),
    ?assertMatch({{"key10", false}, {"key3", false}}, mio_bucket:get_range_op(Middle)),
    ?assertMatch({{"key3", true}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Right)).

%%  C1-O2-C3
%%    Deletion from C3: C1-O2'-C3'
delete_c_o_c_3() ->
    %% [0, 1, 10] [2] [3, 4, 5]
    {Left, Middle, Right} = insert_c_o_c_1(),

    %% [0, 1, 10] [] [2, 3, 5]
    ?assertMatch({ok, []}, mio_bucket:delete_op(Right, "key4")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(Left, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(Left, "key1")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(Left, "key10")),
    ?assertMatch({error, not_found}, mio_bucket:get_op(Middle, "key2")),

    ?assertMatch({ok, value2}, mio_bucket:get_op(Right, "key2")),
    ?assertMatch({ok, value3}, mio_bucket:get_op(Right, "key3")),
    ?assertMatch({error, not_found}, mio_bucket:get_op(Right, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(Right, "key5")),

    %% type
    ?assertMatch(c_o_c_l, mio_bucket:get_type_op(Left)),
    ?assertMatch(c_o_c_m, mio_bucket:get_type_op(Middle)),
    ?assertMatch(c_o_c_r, mio_bucket:get_type_op(Right)),

    %% range
    ?assertMatch({{?MIN_KEY, false}, {"key10", true}}, mio_bucket:get_range_op(Left)),
    ?assertMatch({{"key10", false}, {"key2", false}}, mio_bucket:get_range_op(Middle)),
    ?assertMatch({{"key2", true}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Right)).

make_c_O_c() ->
    %% [0, 1, 10] [2] [3, 4, 5]
    {Left, Middle, Right} = insert_c_o_c_1(),
    %% [0, 1, 10] [] [2, 3, 5]
    ?assertMatch({ok, []}, mio_bucket:delete_op(Middle, "key2")),
    {Left, Middle, Right}.


%%  C1-O2*-C3
%%    Deletion from C1: C1'-O3'
delete_c_O_c_1() ->
    %% [0, 1, 10] [] [3, 4, 5]
    {Left, Middle, Right} = make_c_O_c(),

    %% [0, 1, 3] [] [4, 5]
    %% the Middle bucket is removed from Skip Graph
    ?assertMatch({ok, [Middle]}, mio_bucket:delete_op(Left, "key10")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(Left, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(Left, "key1")),
    ?assertMatch({ok, value3}, mio_bucket:get_op(Left, "key3")),

    ?assertMatch(true, mio_bucket:is_empty_op(Middle)),

    ?assertMatch({error, not_found}, mio_bucket:get_op(Right, "key3")),
    ?assertMatch({ok, value4}, mio_bucket:get_op(Right, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(Right, "key5")),

    %% type
    ?assertMatch(c_o_l, mio_bucket:get_type_op(Left)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(Right)),

    %% link
    ?assertEqual(Right, mio_bucket:get_right_op(Left)),
    ?assertEqual(Left, mio_bucket:get_left_op(Right)),

    %% range
    ?assertMatch({{?MIN_KEY, false}, {"key3", true}}, mio_bucket:get_range_op(Left)),
    ?assertMatch({{"key3", false}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Right)).

%%  C1-O2*-C3
%%    Deletion from C3: C1-O3'
delete_c_O_c_2() ->
    %% [0, 1, 10] [] [3, 4, 5]
    {Left, Middle, Right} = make_c_O_c(),

    %% [0, 1, 10] [] [3, 5]
    %% the Middle bucket is removed from Skip Graph
    ?assertMatch({ok, [Middle]}, mio_bucket:delete_op(Right, "key4")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(Left, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(Left, "key1")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(Left, "key10")),

    ?assertMatch(true, mio_bucket:is_empty_op(Middle)),

    ?assertMatch({ok, value3}, mio_bucket:get_op(Right, "key3")),
    ?assertMatch({error, not_found}, mio_bucket:get_op(Right, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(Right, "key5")),

    %% type
    ?assertMatch(c_o_l, mio_bucket:get_type_op(Left)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(Right)),

    %% link
    ?assertEqual(Right, mio_bucket:get_right_op(Left)),
    ?assertEqual(Left, mio_bucket:get_left_op(Right)),

    %% range
    ?assertMatch({{?MIN_KEY, false}, {"key3", false}}, mio_bucket:get_range_op(Left)),
    ?assertMatch({{"key3", true}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Right)).

%% [1 2 3] []
make_alone_c_O() ->
    {ok, Bucket} = make_bucket(alone),
    ?assertMatch({ok, _}, mio_bucket:insert_op(Bucket, "key1", value1)),
    ?assertMatch({ok, _}, mio_bucket:insert_op(Bucket, "key2", value2)),
    ?assertEqual([], mio_bucket:get_left_op(Bucket)),
    ?assertEqual([], mio_bucket:get_right_op(Bucket)),
    ?assertMatch({ok, _}, mio_bucket:insert_op(Bucket, "key3", value3)),
    {Bucket, mio_bucket:get_right_op(Bucket)}.


%% C1-O2*
%%   Both left and right not exist: O1
delete_c_O_1() ->
    %% [1 2 3] []
    {Bucket, Right} = make_alone_c_O(),

    ?assertMatch(c_o_l, mio_bucket:get_type_op(Bucket)),

    %% [2 3]
    ?assertMatch({ok, [Right]}, mio_bucket:delete_op(Bucket, "key1")),

    ?assertMatch({error, not_found}, mio_bucket:get_op(Bucket, "key1")),
    ?assertMatch({ok, value2}, mio_bucket:get_op(Bucket, "key2")),
    ?assertMatch({ok, value3}, mio_bucket:get_op(Bucket, "key3")),

    %% type
    ?assertMatch(alone, mio_bucket:get_type_op(Bucket)),

    %% range
    ?assertMatch({{?MIN_KEY, false}, {?MAX_KEY, false}}, mio_bucket:get_range_op(Bucket)).

%% C1-O2 | C3-O4*
%%   [0, 1, 10] [2] | [3, 4, 5] []
make_c_o__c_O() ->
    %% [0, 1, 10] [2] [3, 4, 5]
    {C1, O2, C3} = insert_c_o_c_1(),
    ?assertMatch({ok, _}, mio_bucket:insert_op(C3, "key6", value6)),
    O4 = mio_bucket:get_right_op(C3),
    ?assertMatch({ok, []}, mio_bucket:delete_op(O4, "key6")),

    ?assertMatch(c_o_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O2)),
    ?assertMatch(c_o_l, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O4)),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(C1, "key1")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),

    ?assertMatch({ok, value2}, mio_bucket:get_op(O2, "key2")),

    ?assertMatch({ok, value3}, mio_bucket:get_op(C3, "key3")),
    ?assertMatch({ok, value4}, mio_bucket:get_op(C3, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C3, "key5")),

    {C1, O2, C3, O4}.

%% Returns [0, 1, 10] [] | [3, 4, 5] | [55] | [6 7 8]
make_c_O__c_o_c() ->
    %% [0, 1, 10] [2] | [3, 4, 5] []
    {C1, O2, C3, O4} = make_c_o__c_O(),

    ?assertMatch({ok, []}, mio_bucket:delete_op(O2, "key2")),
    ?assertMatch({ok, _}, mio_bucket:insert_op(O4, "key6", value6)),
    ?assertMatch({ok, _}, mio_bucket:insert_op(O4, "key7", value7)),
    {ok, NewBucket} = mio_bucket:insert_op(O4, "key8", value8),
    ?assertMatch({ok, _}, mio_bucket:insert_op(NewBucket, "key55", value55)),

    ?assertMatch(c_o_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O2)),
    ?assertMatch(c_o_c_l, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_c_m, mio_bucket:get_type_op(NewBucket)),
    ?assertMatch(c_o_c_r, mio_bucket:get_type_op(O4)),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(C1, "key1")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),

    ?assertEqual(true, mio_bucket:is_empty_op(O2)),

    ?assertMatch({ok, value3}, mio_bucket:get_op(C3, "key3")),
    ?assertMatch({ok, value4}, mio_bucket:get_op(C3, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C3, "key5")),

    ?assertMatch({ok, value55}, mio_bucket:get_op(NewBucket, "key55")),

    ?assertMatch({ok, value6}, mio_bucket:get_op(O4, "key6")),
    ?assertMatch({ok, value7}, mio_bucket:get_op(O4, "key7")),
    ?assertMatch({ok, value8}, mio_bucket:get_op(O4, "key8")),

    {C1, O2, C3, NewBucket, O4}.

%% Returns [0, 1, 10] [] | [3, 4, 5] []
make_c_O__c_O() ->
    %% [0, 1, 10] [2] | [3, 4, 5] []
    {C1, O2, C3, O4} = make_c_o__c_O(),
    ?assertMatch({ok, []}, mio_bucket:delete_op(O2, "key2")),

    ?assertMatch(c_o_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O2)),
    ?assertMatch(c_o_l, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O4)),

    {C1, O2, C3, O4}.

%% C1-O2-C3 | C4-O5*
%%   [0, 1, 10] [19] [2, 21, 22] | [3, 4, 5] []
make_c_o_c__c_O() ->
    %%   [0, 1, 10] [2] | [3, 4, 5] []
    {C1, O2, C3, O4} = make_c_o__c_O(),
    ?assertMatch({ok, _}, mio_bucket:insert_op(O2, "key21", value21)),
    {ok, NewBucket} = mio_bucket:insert_op(O2, "key22", value22),
    ?assertMatch({ok, _}, mio_bucket:insert_op(NewBucket, "key19", value19)),

    ?assertMatch(c_o_c_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_c_m, mio_bucket:get_type_op(NewBucket)),
    ?assertMatch(c_o_c_r, mio_bucket:get_type_op(O2)),
    ?assertMatch(c_o_l, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O4)),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(C1, "key1")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),

    ?assertMatch({ok, value19}, mio_bucket:get_op(NewBucket, "key19")),

    ?assertMatch({ok, value2}, mio_bucket:get_op(O2, "key2")),
    ?assertMatch({ok, value21}, mio_bucket:get_op(O2, "key21")),
    ?assertMatch({ok, value22}, mio_bucket:get_op(O2, "key22")),


    ?assertMatch({ok, value3}, mio_bucket:get_op(C3, "key3")),
    ?assertMatch({ok, value4}, mio_bucket:get_op(C3, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C3, "key5")),

    {C1, NewBucket, O2, C3, O4}.

%% C1-O2*-C3 | C4-O5*
%%   [0, 1, 10] [] [2, 21, 22] | [3, 4, 5] []
make_c_O_c__c_O() ->
    %% [0, 1, 10] [19] [2, 21, 22] | [3, 4, 5] []
    {C1, O2, C3, C4, O5} = make_c_o_c__c_O(),
    ?assertMatch({ok, []}, mio_bucket:delete_op(O2, "key19")),
    {C1, O2, C3, C4, O5}.

%% C1-O2 | C3-O4*
%%   Returns [0, 1, 10] [] | [3, 4, 5] [6]
make_c_O__c_o() ->
    %% [0, 1, 10] [2] [3, 4, 5]
    {C1, O2, C3} = insert_c_o_c_1(),
    ?assertMatch({ok, _}, mio_bucket:insert_op(C3, "key6", value6)),
    O4 = mio_bucket:get_right_op(C3),
    ?assertMatch({ok, []}, mio_bucket:delete_op(O2, "key2")),

    ?assertMatch(c_o_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O2)),
    ?assertMatch(c_o_l, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O4)),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(C1, "key1")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),

    ?assertMatch({error, not_found}, mio_bucket:get_op(O2, "key2")),

    ?assertMatch({ok, value3}, mio_bucket:get_op(C3, "key3")),
    ?assertMatch({ok, value4}, mio_bucket:get_op(C3, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C3, "key5")),

    ?assertMatch({ok, value6}, mio_bucket:get_op(O4, "key6")),

    {C1, O2, C3, O4}.

%% returns [0, 1, 10] [] | [3, 4, 5] [] [6, 7 8]
make_c_O__c_O_c() ->
    %%  [0, 1, 10] [] | [3, 4, 5] [6]
    {C1, O2, C3, O4} = make_c_O__c_o(),
    ?assertMatch({ok, _}, mio_bucket:insert_op(O4, "key7", value7)),
    {ok, NewBucket} = mio_bucket:insert_op(O4, "key8", value8),

    ?assertMatch(c_o_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O2)),
    ?assertMatch(c_o_c_l, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_c_r, mio_bucket:get_type_op(O4)),
    ?assertMatch(c_o_c_m, mio_bucket:get_type_op(NewBucket)),
    {C1, O2, C3, NewBucket, O4}.

%% C1-O2*
%%   C-O exists on left
delete_c_O_2() ->
    %%   [0, 1, 10] [2] | [3, 4, 5] []
    {C1, O2, C3, O4} = make_c_o__c_O(),

    %%   [0, 1, 10] [] | [2, 4, 5] []
    ?assertMatch({ok, []}, mio_bucket:delete_op(C3, "key3")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(C1, "key1")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),

    ?assertMatch({ok, value2}, mio_bucket:get_op(C3, "key2")),
    ?assertMatch({ok, value4}, mio_bucket:get_op(C3, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C3, "key5")),

    ?assertMatch(c_o_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O2)),
    ?assertMatch(c_o_l, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O4)),

    ?assertMatch({{"key10", false}, {"key2", false}}, mio_bucket:get_range_op(O2)),
    ?assertMatch({{"key2", true}, _}, mio_bucket:get_range_op(C3)),
    ok.

%% C1-O2*
%%   C-O exists on right
delete_c_O_3() ->
    %%   [0, 1, 10] [] | [3, 4, 5] [6]
    {C1, O2, C3, O4} = make_c_O__c_o(),

    %%   [0, 10, 3] [] | [4, 5, 6] []
    ?assertMatch({ok, []}, mio_bucket:delete_op(C1, "key1")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),
    ?assertMatch({ok, value3}, mio_bucket:get_op(C1, "key3")),

    ?assertMatch({ok, value4}, mio_bucket:get_op(C3, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C3, "key5")),
    ?assertMatch({ok, value6}, mio_bucket:get_op(C3, "key6")),

    ?assertMatch(c_o_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O2)),
    ?assertMatch(c_o_l, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O4)),

    ?assertMatch({_, {"key3", true}}, mio_bucket:get_range_op(C1)),
    ?assertMatch({{"key3", false}, {"key4", false}}, mio_bucket:get_range_op(O2)),
    ?assertMatch({{"key4", true}, {"key6", true}}, mio_bucket:get_range_op(C3)),
    ?assertMatch({{"key6", false}, _}, mio_bucket:get_range_op(O4)),
    ok.

%% C1-O2*
%%   C-O-C exists on left
delete_c_O_4() ->
    %% [0, 1, 10] [19] [2, 21, 22] | [3, 4, 5] []
    {C1, O2, C3, C4, O5} = make_c_o_c__c_O(),

    %% [0, 1, 10] [] [19, 2, 21] | [22, 3, 5] []
    ?assertMatch({ok, []}, mio_bucket:delete_op(C4, "key4")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(C1, "key1")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),

    ?assertEqual(true, mio_bucket:is_empty_op(O2)),

    ?assertMatch({ok, value19}, mio_bucket:get_op(C3, "key19")),
    ?assertMatch({ok, value2}, mio_bucket:get_op(C3, "key2")),
    ?assertMatch({ok, value21}, mio_bucket:get_op(C3, "key21")),

    ?assertMatch({ok, value22}, mio_bucket:get_op(C4, "key22")),
    ?assertMatch({ok, value3}, mio_bucket:get_op(C4, "key3")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C4, "key5")),

    ?assertEqual(true, mio_bucket:is_empty_op(O5)),

    ?assertMatch(c_o_c_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_c_m, mio_bucket:get_type_op(O2)),
    ?assertMatch(c_o_c_r, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_l, mio_bucket:get_type_op(C4)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O5)),

    ?assertMatch({{"key10", false}, {"key19", false}}, mio_bucket:get_range_op(O2)),
    ?assertMatch({{"key19", true}, {"key22", false}}, mio_bucket:get_range_op(C3)),
    ?assertMatch({{"key22", true}, _}, mio_bucket:get_range_op(C4)),
    ok.

%% C1-O2*
%%   C-O-C exists on right
delete_c_O_5() ->
    %% [0, 1, 10] [] | [3, 4, 5] [55] [6 7 8]
    {C1, O2, C3, O4, C5} = make_c_O__c_o_c(),

    %% [0, 10, 3] [] | [4, 5, 55] [] [6 7 8]
    ?assertMatch({ok, []}, mio_bucket:delete_op(C1, "key1")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),
    ?assertMatch({ok, value3}, mio_bucket:get_op(C1, "key3")),

    ?assertEqual(true, mio_bucket:is_empty_op(O2)),

    ?assertMatch({ok, value4}, mio_bucket:get_op(C3, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C3, "key5")),
    ?assertMatch({ok, value55}, mio_bucket:get_op(C3, "key55")),

    ?assertEqual(true, mio_bucket:is_empty_op(O4)),

    ?assertMatch({ok, value6}, mio_bucket:get_op(C5, "key6")),
    ?assertMatch({ok, value7}, mio_bucket:get_op(C5, "key7")),
    ?assertMatch({ok, value8}, mio_bucket:get_op(C5, "key8")),

    ?assertMatch(c_o_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(O2)),

    ?assertMatch(c_o_c_l, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_c_m, mio_bucket:get_type_op(O4)),
    ?assertMatch(c_o_c_r, mio_bucket:get_type_op(C5)),

    ?assertMatch({_, {"key3", true}}, mio_bucket:get_range_op(C1)),
    ?assertMatch({{"key3", false}, {"key4", false}}, mio_bucket:get_range_op(O2)),
    ?assertMatch({{"key4", true}, {"key55", true}}, mio_bucket:get_range_op(C3)),
    ?assertMatch({{"key55", false}, _}, mio_bucket:get_range_op(O4)),
    ok.

%% C1-O2*
%%   C-O* exists on left
delete_c_O_6() ->
    %% [0, 1, 10] [] | [3, 4, 5] []
    {C1, O2, C3, O4} = make_c_O__c_O(),

    O4Right = mio_bucket:get_right_op(O4),
    {_, {O4MaxKey, O4MaxEncompass}} = mio_bucket:get_range_op(O4),

    %% [0, 1, 10] [3, 5]
    ?assertMatch({ok, [O2, O4]}, mio_bucket:delete_op(C3, "key4")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(C1, "key1")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),

    ?assertMatch({ok, value3}, mio_bucket:get_op(C3, "key3")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C3, "key5")),

    ?assertMatch(c_o_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(C3)),

    ?assertEqual(C1, mio_bucket:get_left_op(C3)),
    ?assertEqual(C3, mio_bucket:get_right_op(C1)),
    ?assertEqual(O4Right, mio_bucket:get_right_op(C3)),

    ?assertMatch({_, {"key3", false}}, mio_bucket:get_range_op(C1)),
    ?assertMatch({{"key3", true}, {O4MaxKey, O4MaxEncompass}}, mio_bucket:get_range_op(C3)),
    ok.

%% C1-O2*
%%   C-O* exists on right
delete_c_O_7() ->
    %% [0, 1, 10] [] | [3, 4, 5] []
    {C1, O2, C3, O4} = make_c_O__c_O(),

    O4Right = mio_bucket:get_right_op(O4),
    {_, {O4MaxKey, O4MaxEncompass}} = mio_bucket:get_range_op(O4),

    %% [0, 10, 3] [4, 5]
    ?assertMatch({ok, [O2, O4]}, mio_bucket:delete_op(C1, "key1")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),
    ?assertMatch({ok, value3}, mio_bucket:get_op(C1, "key3")),

    ?assertMatch({ok, value4}, mio_bucket:get_op(C3, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C3, "key5")),

    ?assertMatch(c_o_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_r, mio_bucket:get_type_op(C3)),

    ?assertEqual(C1, mio_bucket:get_left_op(C3)),
    ?assertEqual(C3, mio_bucket:get_right_op(C1)),
    ?assertEqual(O4Right, mio_bucket:get_right_op(C3)),

    ?assertMatch({_, {"key3", true}}, mio_bucket:get_range_op(C1)),
    ?assertMatch({{"key3", false}, {O4MaxKey, O4MaxEncompass}}, mio_bucket:get_range_op(C3)),
    ok.

%% C1-O2*
%%   C-O*-C exists on left
delete_c_O_8() ->

    %%   [0, 1, 10] [] [2, 21, 22] | [3, 4, 5] []
    {C1, O2, C3, C4, O5} = make_c_O_c__c_O(),

    O5Right = mio_bucket:get_right_op(O5),
    {_, {O5MaxKey, O5MaxEncompass}} = mio_bucket:get_range_op(O5),
    {_, {O2MaxKey, O2MaxEncompass}} = mio_bucket:get_range_op(O2),

    %% [0, 1, 10] [2, 21] [22, 3, 5]
    ?assertMatch({ok, [O2, O5]}, mio_bucket:delete_op(C4, "key4")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value1}, mio_bucket:get_op(C1, "key1")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),

    ?assertMatch({ok, value2}, mio_bucket:get_op(C3, "key2")),
    ?assertMatch({ok, value21}, mio_bucket:get_op(C3, "key21")),

    ?assertMatch({ok, value22}, mio_bucket:get_op(C4, "key22")),
    ?assertMatch({ok, value3}, mio_bucket:get_op(C4, "key3")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C4, "key5")),


    ?assertMatch(c_o_c_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_c_m, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_c_r, mio_bucket:get_type_op(C4)),

    ?assertEqual(C1, mio_bucket:get_left_op(C3)),
    ?assertEqual(C4, mio_bucket:get_right_op(C3)),
    ?assertEqual(C3, mio_bucket:get_right_op(C1)),

    ?assertEqual(C3, mio_bucket:get_left_op(C4)),
    ?assertEqual(O5Right, mio_bucket:get_right_op(C4)),

    ?assertMatch({_, {O2MaxKey, O2MaxEncompass}}, mio_bucket:get_range_op(C1)),
    NewMaxEncompass = not O2MaxEncompass,
    ?assertMatch({{O2MaxKey, NewMaxEncompass}, {"key22", false}}, mio_bucket:get_range_op(C3)),
    ?assertMatch({{"key22", true}, {O5MaxKey, O5MaxEncompass}}, mio_bucket:get_range_op(C4)),
    ok.

%% C1-O2*
%%   C-O*-C exists on right
delete_c_O_9() ->

    %% [0, 1, 10] [] | [3, 4, 5] [] [6, 7 8]
    {C1, O2, C3, O4, C5} = make_c_O__c_O_c(),

    %% [0, 10, 3] [4, 5] [6, 7 8]
    ?assertMatch({ok, [O2, O4]}, mio_bucket:delete_op(C1, "key1")),

    ?assertMatch({ok, value0}, mio_bucket:get_op(C1, "key0")),
    ?assertMatch({ok, value10}, mio_bucket:get_op(C1, "key10")),
    ?assertMatch({ok, value3}, mio_bucket:get_op(C1, "key3")),

    ?assertMatch({ok, value4}, mio_bucket:get_op(C3, "key4")),
    ?assertMatch({ok, value5}, mio_bucket:get_op(C3, "key5")),

    ?assertMatch({ok, value6}, mio_bucket:get_op(C5, "key6")),
    ?assertMatch({ok, value7}, mio_bucket:get_op(C5, "key7")),
    ?assertMatch({ok, value8}, mio_bucket:get_op(C5, "key8")),

    ?assertMatch(c_o_c_l, mio_bucket:get_type_op(C1)),
    ?assertMatch(c_o_c_m, mio_bucket:get_type_op(C3)),
    ?assertMatch(c_o_c_r, mio_bucket:get_type_op(C5)),

    ?assertEqual(C3, mio_bucket:get_right_op(C1)),
    ?assertEqual(C1, mio_bucket:get_left_op(C3)),
    ?assertEqual(C5, mio_bucket:get_right_op(C3)),
    ?assertEqual(C3, mio_bucket:get_left_op(C5)),


    ?assertMatch({_, {"key3", true}}, mio_bucket:get_range_op(C1)),
    ?assertMatch({{"key3", false}, _}, mio_bucket:get_range_op(C3)),
    ok.


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
