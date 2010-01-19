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
-define(REPEAT_COUNT, 2).
-define(NUMBER_OF_PROCESSES, 30).
-define(NUMBER_OF_COMMANDS, 10).

%% suite() ->
%%     [{timetrap,{seconds,3}}].

init_per_testcase(_Name, Config) ->
    application:set_env(mio, port, ?MEMCACHED_PORT),
    ok = application:start(mio),
    ok = mio_app:wait_startup(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    Config.

end_per_testcase(_Name, _Config) ->
    ok = application:stop(mio).

init_per_group(_Name, Config) ->
    Config.

end_per_group(_Name, _Config) ->
    ok.

check_stats(Conn) ->
    {ok, [{"mio_status","OK"}]} = memcached:stats(Conn).

%% Tests start.
test_simple(_Config) ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1235", "myvalue"),
    check_stats(Conn),
    {ok, "myvalue"} = memcached:get(Conn, "1235"),
    check_stats(Conn),
    ok = memcached:disconnect(Conn).

test_parallel_one(_Config) ->
    memcached_n_procs_m_times(
      fun(Conn) ->
              ok = memcached:set(Conn, "10", "hoge"),
              check_stats(Conn),
              {ok, "hoge"} = memcached:get(Conn, "10"),
              check_stats(Conn),
              ok
      end,
      ?NUMBER_OF_PROCESSES,
      ?NUMBER_OF_COMMANDS).

test_parallel_one_delete(_Config) ->
    memcached_n_procs_m_times(
      fun(Conn) ->
              case memcached:delete(Conn, "10") of
                  ok ->
                      check_stats(Conn),
                      ok;
                  {error, not_found} ->
                      check_stats(Conn),
                      ok;
                  Other -> exit(Other)
              end,
              ok = memcached:set(Conn, "10", "hoge"),
              check_stats(Conn),
              ok
      end,
      ?NUMBER_OF_PROCESSES,
      ?NUMBER_OF_COMMANDS).

test_parallel_two(_Config) ->
    memcached_n_procs_m_times(
      fun(Conn) ->
              ok = memcached:set(Conn, "10", "hoge"),
              check_stats(Conn),
              ok = memcached:set(Conn, "11", "hige"),
              check_stats(Conn),

              {ok, "hoge"} = memcached:get(Conn, "10"),
              check_stats(Conn),
              {ok, "hige"} = memcached:get(Conn, "11"),
              check_stats(Conn),
              ok
      end,
      ?NUMBER_OF_PROCESSES,
      ?NUMBER_OF_COMMANDS).

test_parallel_three(_Config) ->
    memcached_n_procs_m_times(
      fun(Conn) ->
              ok = memcached:set(Conn, "10", "hoge"),
              check_stats(Conn),
              ok = memcached:set(Conn, "11", "hige"),
              check_stats(Conn),
              ok = memcached:set(Conn, "12", "hage"),
              check_stats(Conn),
              {ok, "hoge"} = memcached:get(Conn, "10"),
              check_stats(Conn),
              {ok, "hige"} = memcached:get(Conn, "11"),
              check_stats(Conn),
              {ok, "hage"} = memcached:get(Conn, "12"),
              check_stats(Conn),
              ok
      end,
      ?NUMBER_OF_PROCESSES,
      ?NUMBER_OF_COMMANDS).

test_parallel_four(_Config) ->
    memcached_n_procs_m_times(
      fun(Conn) ->
              ok = memcached:set(Conn, "10", "hoge"),
              check_stats(Conn),
              ok = memcached:set(Conn, "11", "hige"),
              check_stats(Conn),
              ok = memcached:set(Conn, "12", "hage"),
              check_stats(Conn),
              ok = memcached:set(Conn, "13", "hege"),
              check_stats(Conn),
              {ok, "hoge"} = memcached:get(Conn, "10"),
              check_stats(Conn),
              {ok, "hige"} = memcached:get(Conn, "11"),
              check_stats(Conn),
              {ok, "hage"} = memcached:get(Conn, "12"),
              check_stats(Conn),
              {ok, "hege"} = memcached:get(Conn, "13"),
              check_stats(Conn),
              ok
      end,
      ?NUMBER_OF_PROCESSES,
      ?NUMBER_OF_COMMANDS).


%% Tests end.
all() ->
    [
%     test_simple,
     {group, set_one_key_parallel}
    ].

groups() ->
    [{set_one_key_parallel,
      [{repeat, ?REPEAT_COUNT}],
%%      [{repeat_until_any_fail, forever}],
      [%% test_parallel_one,
%%        test_parallel_two,
%%        test_parallel_three,
%%        test_parallel_four,
       test_parallel_one_delete
      ]}].

%%====================================================================
%% Internal functions
%%====================================================================
memcached_n_procs_m_times(Fun, N, M) ->
    mio_util:do_workers(N, fun(_Index) ->
                             {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
                                   mio_util:do_times(M, Fun, [Conn]),
                                   ok = memcached:disconnect(Conn)
                           end).
