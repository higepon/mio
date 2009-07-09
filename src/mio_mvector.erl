%% Membership Vectyor
-module(mio_mvector).

-export([make/1, eq/2, get/2]).

make(Args) ->
    Args.

eq(A, B) ->
    A =:= B.

get(MVector, Level) ->
    case Level of
        0 ->
            MVector;
        _ ->
            lists:nthtail(Level, MVector)
    end.
