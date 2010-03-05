%%%-------------------------------------------------------------------
%%% File    : mio_bucket_SUITE.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 5 Mar 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_bucket_SUITE).

-compile(export_all).

basic(_Config) ->
    B = mio_bucket:new(3),
    B1 = mio_bucket:set(key_b, value_b, B),
    B2 = mio_bucket:set(key_a, value_a, B1),
    B3 = mio_bucket:set(key_c, value_c, B2),

    value_a = mio_bucket:get(key_a, B3),
    value_b = mio_bucket:get(key_b, B3),
    value_c = mio_bucket:get(key_c, B3).

not_found(_Config) ->
    B = mio_bucket:new(3),
    B1 = mio_bucket:set(key_b, value_b, B),
    B2 = mio_bucket:set(key_a, value_a, B1),
    B3 = mio_bucket:set(key_c, value_c, B2),
    none = mio_bucket:get(key_d, B3).

remove(_Config) ->
    B = mio_bucket:new(3),
    B1 = mio_bucket:set(key_b, value_b, B),
    B2 = mio_bucket:set(key_a, value_a, B1),
    B3 = mio_bucket:set(key_c, value_c, B2),
    value_a = mio_bucket:get(key_a, B3),

    B4 = mio_bucket:remove(key_a, B3),
    none = mio_bucket:get(key_a, B4).

overflow(_Config) ->
    B = mio_bucket:new(3),
    B1 = mio_bucket:set(key_b, value_b, B),
    B2 = mio_bucket:set(key_a, value_a, B1),
    B3 = mio_bucket:set(key_c, value_c, B2),
    overflow = mio_bucket:set(key_d, value_d, B3),

    none = mio_bucket:get(key_d, B3),
    B4 = mio_bucket:remove(key_a, B3),

    B5 = mio_bucket:set(key_d, value_d, B4),
    value_d = mio_bucket:get(key_d, B5).

full(_Config) ->
    B = mio_bucket:new(3),
    B1 = mio_bucket:set(key_b, value_b, B),
    B2 = mio_bucket:set(key_a, value_a, B1),
    false = mio_bucket:is_full(B1),
    B3 = mio_bucket:set(key_c, value_c, B2),
    true = mio_bucket:is_full(B3).

all() ->
    [
     basic,
     not_found,
     remove,
     overflow,
     full
    ].
