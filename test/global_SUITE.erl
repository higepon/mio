%%%-------------------------------------------------------------------
%%% File    : global_SUITE.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description :
%%%
%%% Created : 21 Nov 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(global_SUITE).

-compile(export_all).

init_per_suite(Config) ->
    application:start(mio),
    Config.

end_per_suite(_Config) ->
    application:stop(mio),
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
