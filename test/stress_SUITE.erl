%%%-------------------------------------------------------------------
%%% File    : strss_test_SUITE.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description :
%%%
%%% Created : 6 Aug 2010 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(stress_test_SUITE).

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

%% Tests start.
test_simple(_Config) ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "mykey", "myvalue"),
    {ok, "myvalue"} = memcached:get(Conn, "mykey"),
    ok = memcached:disconnect(Conn).

%% Tests end.
all() ->
    [
     test_simple
    ].
