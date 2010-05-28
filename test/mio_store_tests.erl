%%%-------------------------------------------------------------------
%%% File    : mio_store_SUITE.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 5 Mar 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_store_tests).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    {full, B3} = mio_store:set(key_c, value_c, B2),
    {ok, value_a} = mio_store:get(key_a, B3),
    {ok, value_b} = mio_store:get(key_b, B3),
    {ok, value_c} = mio_store:get(key_c, B3).

not_found_test() ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    {full, B3} = mio_store:set(key_c, value_c, B2),
    {error, not_found} = mio_store:get(key_d, B3).

remove_test() ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    {full, B3} = mio_store:set(key_c, value_c, B2),
    {ok, value_a} = mio_store:get(key_a, B3),

    B4 = mio_store:remove(key_a, B3),
    B5 = mio_store:remove(key_a, B4),
    {error, not_found} = mio_store:get(key_a, B5).

overflow_test() ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    {full, B3} = mio_store:set(key_c, value_c, B2),

    %% overflow accept key, value
    {overflow, B4} = mio_store:set(key_d, value_d, B3),

    {ok, value_d} = mio_store:get(key_d, B4).

full_test() ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    false = mio_store:is_full(B1),
    {full, B3} = mio_store:set(key_c, value_c, B2),
    true = mio_store:is_full(B3).

full2_test() ->
    B = mio_store:new(1),
    {full, B1} = mio_store:set(key_a, value_a, B),
    {full, _} = mio_store:set(key_a, value_a, B1).


take_smallest_test() ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    {full, B3} = mio_store:set(key_c, value_c, B2),
    {key_a, value_a, B4} = mio_store:take_smallest(B3),

    {error, not_found} = mio_store:get(key_a, B4),

    B5 = mio_store:new(3),
    none = mio_store:take_smallest(B5).

take_largest_test() ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    {full, B3} = mio_store:set(key_c, value_c, B2),
    {key_c, value_c, B4} = mio_store:take_largest(B3),

    {error, not_found} = mio_store:get(key_c, B4),

    B5 = mio_store:new(3),
    none = mio_store:take_largest(B5).

empty_test() ->
    B = mio_store:new(3),
    none = mio_store:largest(B),
    none = mio_store:smallest(B).

range_test() ->
    B = mio_store:new(5),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_d, value_c, B2),
    B4 = mio_store:set(key_d, value_d, B3),

    ?assertMatch([{key_b, value_b}], mio_store:get_range(key_aa, key_bb, 10, B4)).

range2_test() ->
    B = mio_store:new(5),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),
    B4 = mio_store:set(key_d, value_d, B3),

    ?assertMatch([{key_b, value_b}], mio_store:get_range(key_aa, key_bb, 1, B4)).

range3_test() ->
    B = mio_store:new(5),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),
    B4 = mio_store:set(key_d, value_d, B3),

    ?assertMatch([{key_b, value_b}, {key_c, value_c}, {key_d, value_d}], mio_store:get_range(key_aa, key_z, 10, B4)).

range4_test() ->
    B = mio_store:new(5),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),
    B4 = mio_store:set(key_d, value_d, B3),

    ?assertMatch([{key_b, value_b}, {key_c, value_c}, {key_d, value_d}], mio_store:get_range(key_b, key_z, 3, B4)).

range5_test() ->
    B = mio_store:new(5),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_d, value_c, B2),
    B4 = mio_store:set(key_d, value_d, B3),

    ?assertMatch([{key_b, value_b}], mio_store:get_range(key_bb, key_aa, 10, B4)).

range6_test() ->
    B = mio_store:new(5),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),
    B4 = mio_store:set(key_d, value_d, B3),

    ?assertMatch([{key_b, value_b}], mio_store:get_range(key_bb, key_aa, 1, B4)).

range7_test() ->
    B = mio_store:new(5),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),
    B4 = mio_store:set(key_d, value_d, B3),

    ?assertMatch([{key_d, value_d}, {key_c, value_c}, {key_b, value_b}, {key_a, value_a}], mio_store:get_range(key_z, key_a, 10, B4)).

range8_test() ->
    B = mio_store:new(5),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),
    B4 = mio_store:set(key_d, value_d, B3),

    ?assertMatch([{key_d, value_d}, {key_c, value_c}], mio_store:get_range(key_d, key_b, 2, B4)).

range9_test() ->
    B = mio_store:new(5),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_c, value_c, B1),
    B3 = mio_store:set(key_d, value_d, B2),

    ?assertMatch([{key_d, value_d} , {key_c, value_c}, {key_b, value_b} ], mio_store:get_range(key_d, key_a, 10, B3)).
