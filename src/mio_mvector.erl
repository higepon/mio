%% Membership Vectyor
-module(mio_mvector).

-export([make/1, eq/2, eq/3, gt/2, gt/3, get/2]).

make(Args) ->
    Args.

eq(A, B) ->
    A =:= B.

eq(Level, A, B) ->
    eq(get(A, Level), get(B, Level)).


gt(A, B) ->
    A >= B.

gt(Level, A, B) ->
    gt(get(A, Level), get(B, Level)).

get(MVector, Level) ->
    case Level of
        0 ->
            MVector;
        _ ->
            lists:sublist(MVector, length(MVector) - Level)
    end.
