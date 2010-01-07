%%%-------------------------------------------------------------------
%%% File    : mio_SUITE.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description :
%%%
%%% Created : 2 Aug 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_SUITE).

-compile(export_all).
-include("../include/mio.hrl").
-define(MEMCACHED_PORT, 11211).
-define(MEMCACHED_HOST, "127.0.0.1").

init_per_testcase(_Name, Config) ->
    application:set_env(mio, port, ?MEMCACHED_PORT),
    ok = application:start(mio),
    ok = mio_app:wait_startup(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    Config.

end_per_testcase(_Name, _Config) ->
    ok = application:stop(mio).

set_and_get(_Config) ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1234", "myvalue"),
    {ok, "myvalue"} = memcached:get(Conn, "1234"),
    ok = memcached:disconnect(Conn).

set_and_get_alphabet_key(_Config) ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "mykey", "myvalue"),
    {ok, "myvalue"} = memcached:get(Conn, "mykey"),
    ok = memcached:disconnect(Conn).


delete(_Config) ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1234", "myvalue"),
    {ok, "myvalue"} = memcached:get(Conn, "1234"),
    ok = memcached:delete(Conn, "1234"),
    {error, not_found} = memcached:get(Conn, "1234"),
    ok = memcached:disconnect(Conn).

expiration(_Config) ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1234", "myvalue", 0, 1),
    {ok, "myvalue"} = memcached:get(Conn, "1234"),
    timer:sleep(1000),
    {error, not_found} = memcached:get(Conn, "1234"),
    ok = memcached:disconnect(Conn).

range_search(_Config) ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1001", "Hello"),
    ok = memcached:set(Conn, "2001", "Japan"),
    ok = memcached:set(Conn, "3001", "World"),
    {ok, [{"1001","Hello"}, {"2001","Japan"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "asc"]),
    {ok, [{"2001","Japan"}, {"1001","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "10", "desc"]),
    {ok, [{"1001","Hello"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "1", "asc"]),
    {ok, [{"2001","Japan"}]} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "1", "desc"]),
    {ok, []} = memcached:get_multi(Conn, ["mio:range-search", "1000", "3000", "0", "asc"]),
    ok = memcached:disconnect(Conn).

range_search_alphabet(_Config) ->
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


range_search_expiration(_Config) ->
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

all() ->
    [
     set_and_get,
     set_and_get_alphabet_key,
     delete,
     expiration,
     range_search,
     range_search_alphabet,
     range_search_expiration
    ].
