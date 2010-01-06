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
    application:set_env(mio, port, ?MEMCACHED_PORT),
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

range_search(_Config) ->
    ok.
%%         (set m "hello" "world")
%%         (set m "hi" "japan")
%%         (set m "ipod" "mp3")
%%         (test* 'get '((hello . "world")) (get m "hello"))
%%         (test* 'get '((hello . "world")) (get m "mio:range-search" "he" "hi" "10" "asc"))
%%         (test* 'get '((hello . "world") (hi . "japan")) (get m "mio:range-search" "he" "ipod" "10" "asc"))
%%         (test* 'get '((hi . "japan") (hello . "world")) (get m "mio:range-search" "he" "ipod" "10" "desc"))
%%         (test* 'get '((hi . "japan")) (get m "mio:range-search" "he" "ipod" "1" "desc"))
%%         (delete m "hello")
%%         (test* 'get '() (get m "hello"))
%%         ;; expiry tests
%%         (set m "abc" "1")
%%         (set m "abd" "2" :exptime 1)
%%         (set m "abe" "3")
%%         (test* 'get '((abd . "2") (abc . "1")) (get m "mio:range-search" "ab" "abe" "2" "desc"))
%%         (sys-sleep 1)
%%         (test* 'get '((abc . "1")) (get m "mio:range-search" "ab" "abe" "2" "desc"))


all() ->
    [
     set_and_get,
     delete,
     expiration,
     range_search
    ].
