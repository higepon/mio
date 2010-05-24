%%%-------------------------------------------------------------------
%%% File    : mio_multiple_nodes_tests.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 19 May 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_multiple_nodes_tests).
-include_lib("eunit/include/eunit.hrl").

start_and_kill_test() ->
    Capacity = 1000,
    %% start first mio server
    {ok, Mio1} = mio_sup:start_first_mio(mio_sup, 11211, 3, Capacity, ".", false),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11211)),

    %% start second mio server
    %% On this test code, Introducer node is the same node as first mio server.
    {ok, Mio2} = mio_sup:start_second_mio(mio_sup2, node(), 11311, 3, Capacity, false),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11311)),

    %% Kill them.
    process_flag(trap_exit, true),
    exit(Mio1, normal),
    exit(Mio2, normal),

    %% Can we reuse the port?
    {ok, Mio3} = mio_sup:start_first_mio(mio_sup3, 11211, 3, Capacity, ".", false),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11211)),
    {ok, Mio4} = mio_sup:start_second_mio(mio_sup4, node(), 11311, 3, Capacity, false),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11311)),
    exit(Mio3, normal),
    exit(Mio4, normal).

introducer_not_exist_test() ->
    ?assertEqual({error, shutdown}, mio_sup:start_second_mio(mio_sup1, "hig", 11311, 3, 1000, false)).
