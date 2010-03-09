%%%-------------------------------------------------------------------
%%% File    : mio_store_SUITE.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 5 Mar 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_store_SUITE).

-compile(export_all).

basic(_Config) ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),

    value_a = mio_store:get(key_a, B3),
    value_b = mio_store:get(key_b, B3),
    value_c = mio_store:get(key_c, B3).

not_found(_Config) ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),
    none = mio_store:get(key_d, B3).

remove(_Config) ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),
    value_a = mio_store:get(key_a, B3),

    B4 = mio_store:remove(key_a, B3),
    none = mio_store:get(key_a, B4).

overflow(_Config) ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),
    overflow = mio_store:set(key_d, value_d, B3),

    none = mio_store:get(key_d, B3),
    B4 = mio_store:remove(key_a, B3),

    B5 = mio_store:set(key_d, value_d, B4),
    value_d = mio_store:get(key_d, B5).

full(_Config) ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    false = mio_store:is_full(B1),
    B3 = mio_store:set(key_c, value_c, B2),
    true = mio_store:is_full(B3).

take_smallest(_Config) ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),
    {key_a, value_a, B4} = mio_store:take_smallest(B3),

    none = mio_store:get(key_a, B4),

    B5 = mio_store:new(3),
    none = mio_store:take_smallest(B5).

take_largest(_Config) ->
    B = mio_store:new(3),
    B1 = mio_store:set(key_b, value_b, B),
    B2 = mio_store:set(key_a, value_a, B1),
    B3 = mio_store:set(key_c, value_c, B2),
    {key_c, value_c, B4} = mio_store:take_largest(B3),

    none = mio_store:get(key_c, B4),

    B5 = mio_store:new(3),
    none = mio_store:take_largest(B5).

all() ->
    [
     basic,
     not_found,
     remove,
     overflow,
     full,
     take_smallest,
     take_largest
    ].
