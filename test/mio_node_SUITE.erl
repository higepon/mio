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
    {ok, NodePid} = mio_sup:start_node(myKey, myValue),
    register(mio_node, NodePid),
    Config.

end_per_suite(Config) ->
    ok.

all() -> 
    [get_call, atom_compare, left_right_call, dump_nodes_call, search_call].

get_call() ->
    [].

get_call(_Config) ->
    {myKey, myValue} = gen_server:call(mio_node, get),
    {myKey, myValue2} = gen_server:call(mio_node, get),
    ok.

left_right_call(_Config) ->
    [] = gen_server:call(mio_node, left),
    [] = gen_server:call(mio_node, right).

atom_compare(_Config) ->
    false = abc > def,
    true = mio_node:key_gt(def, abc).

%% add_right_call(_Config) ->
%%     true = gen_server:call(mio_node, add_right).


dump_to_right_call(_Config) ->
%%     [{myKey, myValue2}] =  gen_server:call(mio_node, dump_to_right),
%%     {ok, Pid} = gen_server:call(mio_node, {insert, myKey1, myValue1}),
%%     [{myKey, myValue2}, {myKey1, myValue1}] =  gen_server:call(mio_node, dump_to_right),
    ok.

dump_to_left_call(_Config) ->
%%     [{myKey, myValue2}] =  gen_server:call(mio_node, dump_to_left),
%%     {ok, Pid} = gen_server:call(mio_node, {insert, myKex, myValue1}),
%%     [{myKex, myValue1}, {myKey, myValue2}] =  gen_server:call(mio_node, dump_to_left),
    ok.

dump_nodes_call(_Config) ->
    %% first conditoin
    [{myKey, myValue2}] =  gen_server:call(mio_node, dump_to_right),

    %% insert to right
    {ok, Pid} = gen_server:call(mio_node, {insert, myKey1, myValue1}),
    [{myKey, myValue2}, {myKey1, myValue1}] =  gen_server:call(mio_node, dump_to_right),
    [{myKey, myValue2}] =  gen_server:call(mio_node, dump_to_left),

    %% insert to left
    {ok, Pid2} = gen_server:call(mio_node, {insert, myKex, myValue1}),
    [{myKex, myValue1}, {myKey, myValue2}] = gen_server:call(mio_node, dump_to_left),
    [{myKex, myValue1}, {myKey, myValue2}, {myKey1, myValue1}] =  gen_server:call(mio_node, dump_nodes),

    %% ask to another node
% todo, after search-op is implemented
%    [{myKex, myValue1}, {myKey, myValue2}, {myKey1, myValue1}] =  gen_server:call(Pid, dump_nodes),
    ok.

search_call(_Config) ->
    {ok, myValue1} = gen_server:call(mio_node, {search, mio_node, mykey1}).
