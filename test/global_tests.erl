%%%-------------------------------------------------------------------
%%% File    : global_tests.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 21 Nov 2009 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(global_tests).
-include_lib("eunit/include/eunit.hrl").

global_lock_test() ->
    spawn(fun() ->
                  true = global:set_lock({x, self()}, [node() | nodes()], infinity),
                  timer:sleep(10000)
          end),
    timer:sleep(100),
    false = global:set_lock({x, self()}, [node() | nodes()], 0),
    ok.

