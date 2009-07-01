%%%-------------------------------------------------------------------
%%% File    : mio_node_SUITE.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : 
%%%
%%% Created : 30 Jun 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_node_SUITE).

-compile(export_all).

init_per_suite(Config) ->
    {ok, Pid} = mio_sup:start_link(),
    unlink(Pid),
    Config.

end_per_suite(Config) ->
    ok.

all() -> 
    [get_call, atom_compare].

get_call() ->
    [].

get_call(_Config) ->
    {myKey, myValue} = gen_server:call(mio_node, get),
    {myKey, myValue2} = gen_server:call(mio_node, get),
    ok.


atom_compare(_Config) ->
    false = abc > def,
    true = mio_node:key_gt(def, abc).
