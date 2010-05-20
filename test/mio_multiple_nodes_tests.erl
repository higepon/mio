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
    ?assertMatch({ok, _} , supervisor:start_link({local, mio_sup}, mio_sup, [11211, 3, false, ".", false])),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11211)),
    ?assertMatch({ok, _}, supervisor:start_link({local, mio_sup2}, mio_sup, [second, 11311, 3, node(), ".", false])),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11311)),
    ?debugHere.
