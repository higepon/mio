%% Membership Vectyor
-module(mio_mvector).

-export([make/1, eq/2]).

make(Args) ->
    Args.

eq(A, B) ->
    A =:= B.

