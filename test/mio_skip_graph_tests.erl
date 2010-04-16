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
      [?_test(insert_1())]

      ]
     }.

insert_1() ->
    Capacity = 3,
    {ok, Bucket} = mio_sup:make_bucket(Capacity, alone),
    ok = mio_skip_graph:insert_op(Bucket, "key1", value1),
    ?assertEqual({ok, value1}, mio_skip_graph:search_op(Bucket, "key1")).
