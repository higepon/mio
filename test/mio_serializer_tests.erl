%%%-------------------------------------------------------------------
%%% File    : mio_serializer_tests.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 5 Mar 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_serializer_tests).
-include_lib("eunit/include/eunit.hrl").

for_better_coverage_test() ->
    process_flag(trap_exit, true),
    spawn_link(fun() ->
                       {ok, Pid} = mio_serializer:start_link(),
                       c:l(mio_serializer),
                       cover:compile(
                         mio_serializer,
                         [{i, "include"}
                         ]),
                       sys:suspend(Pid),
                       sys:change_code(Pid, mio_serializer, foo, foo),
                       sys:resume(Pid),
                       code:purge(mio_serializer),
                       gen_server:cast(Pid, hoge),    %% Module:cast
                       Pid ! hoge,                    %% Module:handle_info
                       gen_server:call(Pid, stop_op)  %% Module:terminate
               end).
