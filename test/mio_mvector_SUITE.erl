%%%-------------------------------------------------------------------
%%% File    : mio_mvector_SUITE.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description :
%%%
%%% Created : 30 Jun 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_mvector_SUITE).

-compile(export_all).

make(_Config) ->
    [1, 0] = mio_mvector:make([1, 0]).

equal(_Config) ->
    A = mio_mvector:make([1, 0]),
    B = mio_mvector:make([1, 0]),
    true = mio_mvector:eq(A, B).

not_equal(_Config) ->
    A = mio_mvector:make([1, 0]),
    B = mio_mvector:make([1, 1]),
    false = mio_mvector:eq(A, B).

get(_Config) ->
    A = mio_mvector:make([3, 2, 1, 0]),
    [3, 2, 1, 0] = mio_mvector:get(A, 0),
    [3, 2, 1] = mio_mvector:get(A, 1),
    [3, 2] = mio_mvector:get(A, 2),
    [3] = mio_mvector:get(A, 3),
    ok.

all() ->
    [make, equal, not_equal, get].
