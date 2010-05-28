%%%-------------------------------------------------------------------
%%% File    : mio_coverage_tests.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 5 Mar 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_coverage_tests).
-include_lib("eunit/include/eunit.hrl").

for_better_coverage_test() ->
    {ok, Serializer} = mio_serializer:start_link(),
    ?assertEqual(ok, mio_util:for_better_coverage(mio_serializer, Serializer)),
    process_flag(trap_exit, true),

    {ok, Allocator} = mio_allocator:start_link(),
    ?assertEqual(ok, mio_util:for_better_coverage(mio_bootstrap, Allocator)),

    exit(Serializer, normal),
    exit(Allocator, normal).
