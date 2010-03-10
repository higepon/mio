%%%-------------------------------------------------------------------
%%% File    : mio_bucket_SUITE.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 10 Mar 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_bucket_SUITE).

-compile(export_all).
-include("../include/mio.hrl").

-define(MEMCACHED_PORT, 11211).
-define(MEMCACHED_HOST, "127.0.0.1").

init_per_suite(Config) ->
    ok = application:start(mio),
    ok = mio_app:wait_startup(?MEMCACHED_HOST, ?MEMCACHED_PORT),

    {ok, NodePid} = mio_sup:start_node(myKey, myValue, mio_mvector:make([1, 0])),
    true = register(mio_node, NodePid),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(mio),
    ok.

insert(_Config) ->
    %% set up initial bucket
    Capacity = 3,
    {ok, Bucket} = mio_sup:make_bucket(Capacity),
    [] = mio_bucket:get_left_op(Bucket).
%%    [] = mio_bucket:get_right_op(Bucket),

%%     ok = mio_bucket:insert(Bucket, key1, value1),
%%     ok = mio_bucket:insert(Bucket, key2, value2),
%%     [] = mio_bucket:get_left_op(Bucket),
%%     [] = mio_bucket:get_right_op(Bucket),
%%     ok = mio_bucket:insert(Bucket, key3, value3),
%%     ok = case mio_bucket:get_right_op(Bucket) of
%%              [] -> ng;
%%              RightBucket ->
%%                  true = mio_bucket:is_empty_op(RightBucket),
%%                  ok
%%          end.

all() ->
    [
     insert
    ].
