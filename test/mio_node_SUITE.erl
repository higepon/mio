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
    [get_call, left_right_call, dump_nodes_call, search_call, search_level2].

get_call() ->
    [].

get_call(_Config) ->
    {myKey, myValue} = gen_server:call(mio_node, get),
    {myKey, myValue2} = gen_server:call(mio_node, get),
    ok.

left_right_call(_Config) ->
    [] = gen_server:call(mio_node, left),
    [] = gen_server:call(mio_node, right).



dump_nodes_call(_Config) ->
    %% insert to right
    {ok, Pid} = gen_server:call(mio_node, {insert, myKey1, myValue1}),

    %% insert to left
    {ok, Pid2} = gen_server:call(mio_node, {insert, myKex, myKexValue}),
    [{myKex, myKexValue}, {myKey, myValue2}, {myKey1, myValue1}] =  mio_node:dump_nodes(mio_node, 0), %% dump on Level 0

    ok.

search_call(_Config) ->
    %% I have the value
    {ok, myValue2} = mio_node:search(mio_node, myKey),
    %% search to right
    {ok, myValue1} = mio_node:search(mio_node, myKey1),
    {ok, myValue1} = mio_node:search(mio_node, myKey1),
    %% search to left
    {ok, myKexValue} = mio_node:search(mio_node, myKex),

    %% not found
    %% returns closest node
    {ok, myKey1, myValue1} = gen_server:call(mio_node, {search, mio_node, myKey2}),
    %% returns ng
    ng = mio_node:search(mio_node, myKey2),
    ok.

%% very simple case: there is only one node.
search_level2(_Config) ->
    {ok, Node} = mio_sup:start_node(myKey, myValue),
    {ok, myKey, myValue} = gen_server:call(Node, {search, Node, myKey}),
    [{myKey, myValue}] = mio_node:dump_nodes(Node, 0), %% dump on Level 0
    ok.
