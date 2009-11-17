%%%-------------------------------------------------------------------
%%% File    : mio_util.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : Some utilities for mio.
%%%
%%% Created : 17 Nov 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_util).

%% API
-export([random_sleep/1]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: 
%% Description:
%%--------------------------------------------------------------------
random_sleep(Times) ->
    case (Times rem 10) of
        0 -> erase(random_seed);
        _ -> ok
    end,
    case get(random_seed) of
        undefined ->
            {A1, A2, A3} = now(),
            random:seed(A1, A2, A3 + erlang:phash(node(), 100000));
        _ -> ok
    end,
    T = random:uniform(1000) rem 20 + 1,
    io:format("HERE sleep ~p msec ~n", [T]),
    receive after T -> ok end.

%%====================================================================
%% Internal functions
%%====================================================================
