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
-define(NUMBER_OF_COMMANDS, 100).

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

%% Tests start.
test_simple(_Config) ->
    {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
    ok = memcached:set(Conn, "1235", "myvalue"),
    {ok, "myvalue"} = memcached:get(Conn, "1235"),
    ok = memcached:disconnect(Conn).

test_parallel_one(_Config) ->
    memcached_n_procs_m_times(
      fun(Conn) ->
              ok = memcached:set(Conn, "10", "hoge"),
              {ok, "hoge"} = memcached:get(Conn, "10"),
              ok
      end,
      ?NUMBER_OF_PROCESSES,
      ?NUMBER_OF_COMMANDS).

test_parallel_one_delete(_Config) ->
    memcached_n_procs_m_times(
      fun(Conn) ->
              case memcached:delete(Conn, "10") of
                  ok -> ok;
                  {error, not_found} -> ok;
                  Other -> exit(Other)
              end,
              ok = memcached:set(Conn, "10", "hoge"),
              ok
      end,
      ?NUMBER_OF_PROCESSES,
      ?NUMBER_OF_COMMANDS).

test_parallel_two(_Config) ->
    memcached_n_procs_m_times(
      fun(Conn) ->

              ok = memcached:set(Conn, "10", "hoge"),
              ok = memcached:set(Conn, "11", "hige"),

              {ok, "hoge"} = memcached:get(Conn, "10"),
              {ok, "hige"} = memcached:get(Conn, "11"),
              ok
      end,
      ?NUMBER_OF_PROCESSES,
      ?NUMBER_OF_COMMANDS).

test_parallel_three(_Config) ->
    memcached_n_procs_m_times(
      fun(Conn) ->
              ok = memcached:set(Conn, "10", "hoge"),
              ok = memcached:set(Conn, "11", "hige"),
              ok = memcached:set(Conn, "12", "hage"),
              {ok, "hoge"} = memcached:get(Conn, "10"),
              {ok, "hige"} = memcached:get(Conn, "11"),
              {ok, "hage"} = memcached:get(Conn, "12"),
              ok
      end,
      ?NUMBER_OF_PROCESSES,
      ?NUMBER_OF_COMMANDS).

test_parallel_four(_Config) ->
    memcached_n_procs_m_times(
      fun(Conn) ->
              ok = memcached:set(Conn, "10", "hoge"),
              ok = memcached:set(Conn, "11", "hige"),
              ok = memcached:set(Conn, "12", "hage"),
              ok = memcached:set(Conn, "13", "hege"),
              {ok, "hoge"} = memcached:get(Conn, "10"),
              {ok, "hige"} = memcached:get(Conn, "11"),
              {ok, "hage"} = memcached:get(Conn, "12"),
              {ok, "hege"} = memcached:get(Conn, "13"),
              ok
      end,
      ?NUMBER_OF_PROCESSES,
      ?NUMBER_OF_COMMANDS).


%% Tests end.
all() ->
    [
%     test_simple
     {group, set_one_key_parallel}
    ].

groups() ->
    [{set_one_key_parallel,
%%      [{repeat, ?REPEAT_COUNT}],
      [{repeat_until_any_fail, forever}],
      [test_parallel_one
%%        test_parallel_two,
%%        test_parallel_three,
%%        test_parallel_four
      ]}].

%%====================================================================
%% Internal functions
%%====================================================================
memcached_n_procs_m_times(Fun, N, M) ->
    mio_util:do_workers(N, fun(_Index) ->
                             {ok, Conn} = memcached:connect(?MEMCACHED_HOST, ?MEMCACHED_PORT),
                                   io:format("connect ~p~n", [Conn]),
                                   mio_util:do_times(M, Fun, [Conn]),
                                   io:format("close: ~p", [Conn]),
                                   ok = memcached:disconnect(Conn)
                           end).
