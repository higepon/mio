%%%-------------------------------------------------------------------
%%% File    : mio_lock_SUITE.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 21 Nov 2009 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_lock_SUITE).

-compile(export_all).

simple(_Config) ->
    Target = [a, b, c],
    true = mio_lock:lock(Target),
    mio_lock:unlock(Target),
    ok.

lock_twice(_Config) ->
    Target = [a, b, c],
    true = mio_lock:lock(Target),
    true = mio_lock:lock(Target),
    mio_lock:unlock(Target),
    ok.

locked_by_other(_Config) ->
    Target = [d, e, f],
    Locker = spawn(fun() ->
                           true = mio_lock:lock(Target),
                           receive
                               unlock ->
                                   mio_lock:unlock(Target)
                           end
                   end),
    timer:sleep(100),
    false = mio_lock:lock(Target),
    Locker ! unlock,
    timer:sleep(100),
    true = mio_lock:lock(Target),
    mio_lock:unlock(Target).

unlock_two(_Config) ->
    mio_lock:lock([g, h, i]),
    Locker = spawn(fun() ->
                           false = mio_lock:lock([g, h, i]),
                           receive
                               retry ->
                                   false = mio_lock:lock([g, h, i]),
                                   true = mio_lock:lock([h, g]),
                                   mio_lock:unlock([h, g])
                           end
                   end),
    false = mio_lock:unlock([g, h]),
    Locker ! retry,
    timer:sleep(100),
    true = mio_lock:lock([i]).


all() ->
    [simple, lock_twice, locked_by_other, unlock_two].
