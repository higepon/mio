%%%-------------------------------------------------------------------
%%% File    : mio_multiple_nodes_tests.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 19 May 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_multiple_nodes_tests).
-include_lib("eunit/include/eunit.hrl").

start_test() ->
    %% start first mio server
    {ok, Mio1} = mio_sup:start_first_mio(mio_sup, 11211, 3, ".", false),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11211)),

    %% start second mio server
    %% On this test code, Introducer node is the same node as first mio server.
    ?assertMatch({ok, _}, mio_sup:start_second_mio(mio_sup2, node(), 11311, 3, false)),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11311)),
process_flag(trap_exit, true),
    exit(Mio1, normal),
    {ok, Mio3} = mio_sup:start_first_mio(mio_sup3, 11211, 3, ".", false),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11211)),

    ?debugHere.
