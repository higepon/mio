%% Membership Vectyor
-module(mio_mvector).

-export([make/1, eq/2, eq/3, gt/2, gt/3, get/2, generate/1]).

make(Args) ->
    Args.

eq(A, B) ->
    A =:= B.

eq(Level, A, B) ->
    eq(get(A, Level), get(B, Level)).

generate(MaxLevel) ->
    %% each process requires this once.
    {A1, A2, A3} = now(),
    random:seed(A1, A2, A3),
    [random:uniform(2) - 1 || _ <- lists:duplicate(MaxLevel, 0)].

gt(A, B) ->
    A >= B.

gt(Level, A, B) ->
    gt(get(A, Level), get(B, Level)).

get(MVector, Level) ->
    case Level of
        0 ->
            [];
        _ ->
            lists:sublist(MVector, Level)
    end.
