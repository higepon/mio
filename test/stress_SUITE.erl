%%%-------------------------------------------------------------------
%%% File    : strss_test_SUITE.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description :
%%%
%%% Created : 6 Aug 2010 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(stress_SUITE).

-compile(export_all).
-include("../include/mio.hrl").

-define(MEMCACHED_PORT, 11411).
-define(MEMCACHED_HOST, "127.0.0.1").

init_per_suite(Config) ->
    application:set_env(mio, port, ?MEMCACHED_PORT),
    ok = application:start(mio),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(mio),
    ok.

%% Tests start.
test_simple(_Config) ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1235", "myvalue"),
    {ok, "myvalue"} = memcached:get(Conn, "1235"),
    ok = memcached:disconnect(Conn).

%% Tests end.
all() ->
    [
     test_simple
    ].
