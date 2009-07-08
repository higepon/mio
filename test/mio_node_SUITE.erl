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
    [get_call, left_right_call, dump_nodes_call, search_call, search_level2_simple, search_level2, test_set_nth].

get_call() ->
    [].

get_call(_Config) ->
    {myKey, myValue} = gen_server:call(mio_node, get),
    {myKey, myValue2} = gen_server:call(mio_node, get),
    ok.

left_right_call(_Config) ->
    [[], []] = gen_server:call(mio_node, left),
    [[], []] = gen_server:call(mio_node, right).



dump_nodes_call(_Config) ->
    %% insert to right
    {ok, Pid} = gen_server:call(mio_node, {insert, myKey1, myValue1}),

    %% insert to left
    {ok, Pid2} = gen_server:call(mio_node, {insert, myKex, myKexValue}),
    [{myKex, myKexValue}, {myKey, myValue2}, {myKey1, myValue1}] =  mio_node:dump_nodes(mio_node, 0), %% dump on Level 0

    ok.

search_call(_Config) ->
%%     %% I have the value
%%     {ok, myValue2} = mio_node:search(mio_node, myKey),
%%     %% search to right
%%     {ok, myValue1} = mio_node:search(mio_node, myKey1),
%%     {ok, myValue1} = mio_node:search(mio_node, myKey1),
%%     %% search to left
%%     {ok, myKexValue} = mio_node:search(mio_node, myKex),

%%     %% not found
%%     %% returns closest node
%%     {ok, myKey1, myValue1} = gen_server:call(mio_node, {search, mio_node, [], myKey2}),
%%     %% returns ng
%%     ng = mio_node:search(mio_node, myKey2),
    ok.

%% very simple case: there is only one node.
search_level2_simple(_Config) ->
    {ok, Node} = mio_sup:start_node(myKey, myValue),
    {ok, myKey, myValue} = gen_server:call(Node, {search, Node, [], myKey}),

    %% dump nodes on Level 0 and 1
    [{myKey, myValue}] = mio_node:dump_nodes(Node, 0),
    [{myKey, myValue}] = mio_node:dump_nodes(Node, 1),
    ok.

search_level2(_Config) ->
    %% We want to test search-op without insert op.
    %%   setup predefined nodes.
    %%     level1 [3] [5]
    %%     level0 [3 <-> 5]
    {ok, Node3} = mio_sup:start_node(key3, value3),
    {ok, Node5} = mio_sup:start_node(key5, value5),
    ok = mio_node:set_right(Node3, 0, Node5),
    ok = mio_node:set_left(Node5, 0, Node3),

    %% dump nodes on Level 0 and 1
    [{key3, value3}, {key5, value5}] = mio_node:dump_nodes(Node3, 0),

    %% search!
    {ok, value3} = mio_node:search(Node3, key3),
    {ok, value5} = mio_node:search(Node5, key5),

    ok.

test_set_nth(_Config) ->
    [1, 3] = mio_node:set_nth(2, 3, [1, 2]),
    [0, 2] = mio_node:set_nth(1, 0, [1, 2]),
    ok.
