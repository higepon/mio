%%%-------------------------------------------------------------------
%%% File    : strss_SUITE.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : Stress test.
%%%
%%% Created : 6 Aug 2010 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(stress_SUITE).

-compile(export_all).
-include("../include/mio.hrl").

-define(MEMCACHED_PORT, 11411).
-define(MEMCACHED_HOST, "127.0.0.1").

suite() ->
    [{timetrap,{seconds,3}}].

init_per_testcase(_Name, Config) ->
    application:set_env(mio, port, ?MEMCACHED_PORT),
    ok = application:start(mio),
    ok = wait_mio_startup(10),
    Config.

end_per_testcase(_Name, _Config) ->
    ok = application:stop(mio).

init_per_group(_Name, Config) ->
    Config.

end_per_group(_Name, _Config) ->
    ok.

%% Tests start.
test_simple(_Config) ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1235", "myvalue"),
    {ok, "myvalue"} = memcached:get(Conn, "1235"),
    ok = memcached:disconnect(Conn).

test_die(_Config) ->
    memcached_n_procs_m_times(
      fun(Conn) ->
              ok = memcached:set(Conn, "10", "hoge"),
              {ok, "hoge"} = memcached:get(Conn, "10"),
              ok
      end,
      30,
      2).

%% Tests end.
all() ->
    [
     test_simple,
     {group, set_one_key_parallel}
    ].

groups() ->
    [{set_one_key_parallel, [{repeat, 3}], [test_die]}].

%%====================================================================
%% Internal functions
%%====================================================================
memcached_n_procs_m_times(Fun, N, M) ->
    mio_util:do_workers(N, fun(_Index) ->
              io:format("hige<0>~p", [self()]),
                                   {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
              io:format("hige<3>~p", [self()]),
                                   mio_util:do_times(M, Fun, [Conn]),
              io:format("hige<4>~p", [self()]),
                                   ok = memcached:disconnect(Conn)
                end).
wait_mio_startup(0) ->
    {error, mio_not_started};
wait_mio_startup(N) ->
    case gen_tcp:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT, []) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            ok;
        {error, econnrefused} ->
            timer:sleep(100),
            wait_mio_startup(N - 1);
        Other ->
            Other
    end.
