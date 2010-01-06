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

init_per_suite(Config) ->
    ok = application:start(mio),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(mio),
    ok.

set_and_get(_Config) ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1234", "myvalue"),
    {ok, "myvalue"} = memcached:get(Conn, "1234"),
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

all() ->
    [
     set_and_get,
     delete,
     expiration
    ].
