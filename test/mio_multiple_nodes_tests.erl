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

insert_many_test() ->
    Capacity = 3,
    %% start first mio server
    {ok, Mio1} = mio_sup:start_first_mio(mio_sup, 11211, 3, Capacity, ".", false),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11211)),

    %% start second mio server
    %% On this test code, Introducer node is the same node as first mio server.
    {ok, Mio2} = mio_sup:start_second_mio(mio_sup2, node(), 11311, 3, Capacity, false),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11311)),

    {ok, Conn} = memcached:connect("127.0.0.1", 11211),

    ok = memcached:set(Conn, "1", "1"),
    {ok, "1"} = memcached:get(Conn, "1"),
    ok = memcached:set(Conn, "2", "2"),
    {ok, "2"} = memcached:get(Conn, "2"),
    ok = memcached:set(Conn, "3", "3"),
    {ok, "3"} = memcached:get(Conn, "3"),
    ok = memcached:set(Conn, "4", "4"),
    {ok, "4"} = memcached:get(Conn, "4"),
    ok = memcached:set(Conn, "5", "5"),
    {ok, "5"} = memcached:get(Conn, "5"),
    ok = memcached:set(Conn, "6", "6"),
    {ok, "6"} = memcached:get(Conn, "6"),
    ok = memcached:set(Conn, "7", "7"),
    {ok, "7"} = memcached:get(Conn, "7"),

    ok = memcached:disconnect(Conn),

    %% Kill them.
    process_flag(trap_exit, true),
    exit(Mio1, normal),
    exit(Mio2, normal).

introducer_not_exist_test() ->
    ?assertEqual({error, shutdown}, mio_sup:start_second_mio(mio_sup1, "hig", 11311, 3, 1000, false)).
