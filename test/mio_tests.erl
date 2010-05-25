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
    ok = application:start(mio),
    ok = mio_app:wait_startup(?MEMCACHED_HOST, ?MEMCACHED_PORT).

teardown_mio(_) ->
    ok = application:stop(mio).

mio_test_() ->
    {foreach, fun setup_mio/0, fun teardown_mio/1,
     [
      [?_test(set_and_get())],
      [?_test(set_and_get_alphabet_key())],
%%       [?_test(delete())],
%%       [?_test(expiration())],
      [?_test(range_search())]
%%       [?_test(range_search_alphabet())],
%%       [?_test(range_search_expiration())]
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

%% TODO.
%% delete() ->
%%     {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
%%     ok = memcached:set(Conn, "1234", "myvalue"),
%%     {ok, "myvalue"} = memcached:get(Conn, "1234"),
%%     ok = memcached:delete(Conn, "1234"),
%%     {error, not_found} = memcached:get(Conn, "1234"),
%%     ok = memcached:disconnect(Conn).

%% expiration() ->
%%     {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
%%     ok = memcached:set(Conn, "1234", "myvalue", 0, 1),
%%     {ok, "myvalue"} = memcached:get(Conn, "1234"),
%%     timer:sleep(1000),
%%     {error, not_found} = memcached:get(Conn, "1234"),
%%     ok = memcached:disconnect(Conn).

range_search() ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1001", "Hello"),
    ok = memcached:set(Conn, "2001", "Japan"),
    ok = memcached:set(Conn, "3001", "World"),
    ?assertMatch({ok, [{"1001","Hello"}, {"2001","Japan"}]}, memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "asc"])),
%%     {ok, [{"2001","Japan"}, {"1001","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "desc"]),
    {ok, [{"1001","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "1", "asc"]),
%%     {ok, [{"2001","Japan"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "1", "desc"]),
    {ok, []} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "0", "asc"]),
    ok = memcached:disconnect(Conn).

%% range_search_alphabet() ->
%%     {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
%%     ok = memcached:set(Conn, "Higepon", "Hello"),
%%     ok = memcached:set(Conn, "John", "Japan"),
%%     ok = memcached:set(Conn, "Paul", "World"),
%%     {ok, [{"Higepon","Hello"}, {"John","Japan"}]} = memcached:get_multi(Conn, ["mio:range-search", "C", "K", "10", "asc"]),
%%     {ok, [{"John","Japan"}, {"Higepon","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "C", "K", "10", "desc"]),
%%     {ok, [{"Higepon","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "C", "K", "1", "asc"]),
%%     {ok, [{"John","Japan"}]} = memcached:get_multi(Conn, ["mio:range-search", "C", "K", "1", "desc"]),
%%     {ok, []} = memcached:get_multi(Conn, ["mio:range-search", "C", "K", "0", "asc"]),
%%     ok = memcached:disconnect(Conn).


%% range_search_expiration() ->
%%     {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
%%     ok = memcached:set(Conn, "1001", "Hello"),
%%     ok = memcached:set(Conn, "2001", "Japan", 0, 1),
%%     ok = memcached:set(Conn, "3001", "World"),
%%     {ok, [{"1001","Hello"}, {"2001","Japan"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "asc"]),
%%     {ok, [{"2001","Japan"}, {"1001","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "desc"]),
%%     timer:sleep(1000),
%%     {ok, [{"1001","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "asc"]),
%%     {ok, [{"1001","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "desc"]),
%%     ok = memcached:disconnect(Conn).

