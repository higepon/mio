%%%-------------------------------------------------------------------
%%% File    : mio_local_store_tests.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description :
%%%
%%% Created : 19 May 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_local_store_tests).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
    {ok, Store} = mio_local_store:new(),
    ?assertEqual({error, not_found}, mio_local_store:get(Store, hige)),
    ?assertEqual(ok, mio_local_store:set(Store, hige, pon)),
    ?assertEqual({ok, pon}, mio_local_store:get(Store, hige)).
