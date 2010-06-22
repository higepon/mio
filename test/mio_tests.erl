%%%-------------------------------------------------------------------
%%% File    : mio_SUITE.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 2 Aug 2009 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_tests).
-include("../src/mio.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MEMCACHED_PORT, 11211).
-define(MEMCACHED_HOST, "127.0.0.1").

setup_mio() ->
    application:set_env(mio, capacity, 3),
    ok = application:start(mio),
    ok = mio_app:wait_startup(?MEMCACHED_HOST, ?MEMCACHED_PORT).

teardown_mio(_) ->
    ok = application:stop(mio).

coverage_test() ->
    {ok, BootStrap} = mio_bootstrap:start_link(1, 2, 3),
    ?assertEqual(ok, mio_util:for_better_coverage(mio_bootstrap, BootStrap,
                                         fun() ->
                                                 ?assertMatch({ok, 1, 2, 3}, mio_bootstrap:get_boot_info(node())) end)),
    process_flag(trap_exit, true),
    exit(BootStrap, normal).

mio_test_() ->
    {foreach, fun setup_mio/0, fun teardown_mio/1,
     [
      [?_test(set_and_get())],
      [?_test(set_and_get_alphabet_key())],
      [?_test(delete())],
      [?_test(empty())],
      [?_test(expiration())],
      [?_test(range_search())],
      [?_test(range_search_alphabet())],
      [?_test(range_search_expiration())],
      [?_test(stats())]
     ]
    }.


set_and_get() ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1234", "myvalue"),
    {ok, "myvalue"} = memcached:get(Conn, "1234"),
    ok = memcached:disconnect(Conn).

set_and_get_alphabet_key() ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "mykey", "myvalue"),
    {ok, "myvalue"} = memcached:get(Conn, "mykey"),
    ok = memcached:disconnect(Conn).

delete() ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1234", "myvalue"),
    {ok, "myvalue"} = memcached:get(Conn, "1234"),
    ok = memcached:delete(Conn, "1234"),
    {error, not_found} = memcached:get(Conn, "1234"),
    {error, not_found} = memcached:delete(Conn, "1234"),
    ok = memcached:disconnect(Conn).

get_start_bucket(Host, Port) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, raw}, {nodelay, true}, {reuseaddr, true}, {active, false},
                      {sndbuf,16384},{recbuf,4096}]),
    gen_tcp:send(Socket, <<"get_start_bucket\r\n">>),
    {ok, Packet}  = gen_tcp:recv(Socket, 0, 10000),
    ok = gen_tcp:close(Socket),
    binary_to_term(Packet).

empty() ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    C = get_start_bucket(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "key1", "value1"),
    ok = memcached:set(Conn, "key2", "value2"),
    ok = memcached:set(Conn, "key3", "value3"),
    O = get_start_bucket(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ?assert(C =/= O),

    ok = memcached:delete(Conn, "key1"),
    ?assertEqual(C, get_start_bucket(?MEMCACHED_HOST, ?MEMCACHED_PORT)),
    ok = memcached:delete(Conn, "key2"),
    ?assertEqual(C, get_start_bucket(?MEMCACHED_HOST, ?MEMCACHED_PORT)),
    ok = memcached:delete(Conn, "key3"),
    ?assertEqual(C, get_start_bucket(?MEMCACHED_HOST, ?MEMCACHED_PORT)),
    ok = memcached:disconnect(Conn).

expiration() ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1234", "myvalue", 0, 1),
    ?assertEqual({ok, "myvalue"}, memcached:get(Conn, "1234")),
    timer:sleep(1000),
    ?assertEqual({error, not_found}, memcached:get(Conn, "1234")),
    ok = memcached:disconnect(Conn).

range_search() ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1001", "Hello"),
    ok = memcached:set(Conn, "2001", "Japan"),
    ok = memcached:set(Conn, "3001", "World"),
    ?assertMatch({ok, [{"1001","Hello"}, {"2001","Japan"}]}, memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "asc"])),
    ?assertMatch({ok, [{"2001","Japan"}, {"1001","Hello"}]}, memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "desc"])),
    ?assertMatch({ok, [{"1001","Hello"}]}, memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "1", "asc"])),
    ?assertMatch({ok, [{"2001","Japan"}]}, memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "1", "desc"])),
    ?assertMatch({ok, []}, memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "0", "asc"])),
    ok = memcached:disconnect(Conn).

range_search_many_buckets_test() ->
    Capacity = 3,
    %% start first mio server
    {ok, Mio1} = mio_sup:start_first_mio(mio_sup, 11211, 3, Capacity, ".", false),
    ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11211)),

    {ok, Conn} = memcached:connect("127.0.0.1", 11211),

    ok = memcached:set(Conn, "1", "1"),
    ok = memcached:set(Conn, "2", "2"),
    ok = memcached:set(Conn, "3", "3"),
    ok = memcached:set(Conn, "4", "4"),
    ok = memcached:set(Conn, "5", "5"),
    ok = memcached:set(Conn, "6", "6"),
    ok = memcached:set(Conn, "7", "7"),
    ok = memcached:set(Conn, "8", "8"),
    ok = memcached:set(Conn, "9", "9"),

    ?assertMatch({ok, [{"2","2"}, {"3","3"}, {"4","4"}, {"5","5"}, {"6","6"}]}, memcached:get_multi(Conn, ["mio:range-search", "11", "6", "10", "asc"])),
    ?assertMatch({ok, []}, memcached:get_multi(Conn, ["mio:range-search", "6", "11", "10", "asc"])),


    ?assertMatch({ok, [{"6","6"}, {"5","5"}, {"4","4"}, {"3","3"}, {"2","2"}]}, memcached:get_multi(Conn, ["mio:range-search", "11", "6", "10", "desc"])),
    ?assertMatch({ok, [{"9","9"}, {"8","8"}, {"7","7"}, {"6","6"}]}, memcached:get_multi(Conn, ["mio:range-search", "11", "9", "4", "desc"])),
    ?assertMatch({ok, [{"9","9"}, {"8","8"}, {"7","7"}, {"6","6"}]}, memcached:get_multi(Conn, ["mio:range-search", "6", "9", "8", "desc"])),
    ?assertMatch({ok, []}, memcached:get_multi(Conn, ["mio:range-search", "6", "9", "0", "desc"])),
    ?assertMatch({ok, []}, memcached:get_multi(Conn, ["mio:range-search", "6", "11", "10", "desc"])),

    ok = memcached:disconnect(Conn),

    %% Kill them.
    process_flag(trap_exit, true),
    exit(Mio1, normal).


range_search_alphabet() ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "Higepon", "Hello"),
    ok = memcached:set(Conn, "John", "Japan"),
    ok = memcached:set(Conn, "Paul", "World"),
    {ok, [{"Higepon","Hello"}, {"John","Japan"}]} = memcached:get_multi(Conn, ["mio:range-search", "C", "K", "10", "asc"]),
    {ok, [{"John","Japan"}, {"Higepon","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "C", "K", "10", "desc"]),
    {ok, [{"Higepon","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "C", "K", "1", "asc"]),
    {ok, [{"John","Japan"}]} = memcached:get_multi(Conn, ["mio:range-search", "C", "K", "1", "desc"]),
    {ok, []} = memcached:get_multi(Conn, ["mio:range-search", "C", "K", "0", "asc"]),
    ok = memcached:disconnect(Conn).


range_search_expiration() ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1001", "Hello"),
    ok = memcached:set(Conn, "2001", "Japan", 0, 1),
    ok = memcached:set(Conn, "3001", "World"),
    {ok, [{"1001","Hello"}, {"2001","Japan"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "asc"]),
    {ok, [{"2001","Japan"}, {"1001","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "desc"]),
    timer:sleep(1000),
    {ok, [{"1001","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "asc"]),
    {ok, [{"1001","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "desc"]),
    ok = memcached:disconnect(Conn).

string2integer(S) ->
    {ok,[N],[]} =  io_lib:fread("~d", S),
    N.

stats() ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1001", "Hello"),
    {ok, [{"uptime", UptimeStr}, {"total_items", "1"}, {"cmd_get", "0"}]} = memcached:stats(Conn),
    Uptime = string2integer(UptimeStr),
    ?assert(Uptime >= 0 andalso Uptime < 3),
    ok = memcached:disconnect(Conn).
