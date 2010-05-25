%%%-------------------------------------------------------------------
%%% File    : mio_coverage_tests.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 5 Mar 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_coverage_tests).
-include_lib("eunit/include/eunit.hrl").

for_better_coverage(Mod, Pid) ->
for_better_coverage(Mod, Pid, false).
for_better_coverage(Mod, Pid, Fun) ->
    process_flag(trap_exit, true),
    spawn_link(fun() ->
                       c:l(Mod),          %% Moduel:code_change/3
                       cover:compile(
                         Mod,
                         [{i, "include"}
                         ]),
                       sys:suspend(Pid),
                       sys:change_code(Pid, Mod, foo, foo),
                       sys:resume(Pid),
                       code:purge(Mod),
                       gen_server:cast(Pid, hoge),    %% Module:handle_cast/2
                       Pid ! hoge,                    %% Module:handle_info/2
                       case Fun of
                           false -> ok;
                           _ ->
                               Fun()
                       end,
                       gen_server:call(Pid, stop_op)  %% Module:terminate/2
               end),
    receive
        {'EXIT', _, normal} -> ok;
        {'EXIT',_ ,{normal,{gen_server,call,[_ ,stop_op]}}} -> ok;
        Other ->
            ?debugFmt("Other=~p", [Other]),
            {error, Other}
    end.


for_better_coverage_test() ->
    {ok, Serializer} = mio_serializer:start_link(),
    ?assertMatch(ok, for_better_coverage(mio_serializer, Serializer)),
    process_flag(trap_exit, true),


    {ok, Bucket} = mio_bucket:start_link([1, 1, 1, []]),
    ?assertMatch(ok, for_better_coverage(mio_bucket, Bucket)),

    {ok, BootStrap} = mio_bootstrap:start_link(1, 2, 3),
    ?assertMatch(ok, for_better_coverage(mio_bootstrap, BootStrap,
                                         fun() ->
                                                 ?assertMatch({ok, 1, 2, 3}, mio_bootstrap:get_boot_info(node())) end)),

    {ok, Allocator} = mio_allocator:start_link(),
    ?assertMatch(ok, for_better_coverage(mio_bootstrap, Allocator)),

    exit(Serializer, normal),
    exit(Bucket, normal),
    exit(BootStrap, normal),
    exit(Allocator, normal).
