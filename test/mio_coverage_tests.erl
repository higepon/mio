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
    ?assertMatch(ok, mio_util:for_better_coverage(mio_serializer, Serializer)),
    process_flag(trap_exit, true),


    {ok, BootStrap} = mio_bootstrap:start_link(1, 2, 3),
    ?assertMatch(ok, mio_util:for_better_coverage(mio_bootstrap, BootStrap,
                                         fun() ->
                                                 ?assertMatch({ok, 1, 2, 3}, mio_bootstrap:get_boot_info(node())) end)),

    {ok, Allocator} = mio_allocator:start_link(),
    ?assertMatch(ok, mio_util:for_better_coverage(mio_bootstrap, Allocator)),

    exit(Serializer, normal),
    exit(BootStrap, normal),
    exit(Allocator, normal).
