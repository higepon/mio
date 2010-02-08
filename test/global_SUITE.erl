%%%-------------------------------------------------------------------
%%% File    : global_SUITE.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 21 Nov 2009 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(global_SUITE).

-compile(export_all).

init_per_suite(Config) ->
    ok = application:start(mio),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(mio),
    ok.

global_lock(_Config) ->
    spawn(fun() ->
                  true = global:set_lock({x, self()}, [node() | nodes()], infinity),
                  timer:sleep(10000)
          end),
    timer:sleep(100),
    false = global:set_lock({x, self()}, [node() | nodes()], 0),
    ok.

all() ->
    [global_lock].
