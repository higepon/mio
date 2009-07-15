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
    error_logger:tty(false),
    ok = error_logger:logfile({open, "./error.log"}),
    {ok, Pid} = mio_sup:start_link(),
    unlink(Pid),
    {ok, NodePid} = mio_sup:start_node(myKey, myValue, mio_mvector:make([1, 0])),
    register(mio_node, NodePid),
    Config.

end_per_suite(Config) ->
    ok.


get_call() ->
    [].

get_call(_Config) ->
    {myKey, myValue} = gen_server:call(mio_node, get),
    {myKey, myValue} = gen_server:call(mio_node, get),
    ok.

left_right_call(_Config) ->
    [[], []] = gen_server:call(mio_node, left),
    [[], []] = gen_server:call(mio_node, right).



dump_nodes_call(_Config) ->
    %% insert to right
    {ok, Pid} = gen_server:call(mio_node, {insert, myKey1, myValue1}),

    %% insert to left
    {ok, Pid2} = gen_server:call(mio_node, {insert, myKex, myKexValue}),
    [{myKex, myKexValue, [1, 0]}, {myKey, myValue, [1, 0]}, {myKey1, myValue1, [1, 0]}] =  mio_node:dump_nodes(mio_node, 0), %% dump on Level 0

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
    {ok, Node} = mio_sup:start_node(myKey, myValue, mio_mvector:make([1, 0])),
    {ok, _, myKey, myValue} = gen_server:call(Node, {search, Node, [], myKey}),

    %% dump nodes on Level 0 and 1
    [{myKey, myValue, [1, 0]}] = mio_node:dump_nodes(Node, 0),
    [{Pid, myKey, myValue, [1, 0]}] = mio_node:new_dump(Node),
    [[{myKey, myValue, [1, 0]}]] = mio_node:dump_nodes(Node, 1),
    ok.

search_level2_1(_Config) ->
    %% We want to test search-op without insert op.
    %%   setup predefined nodes as follows.
    %%     level1 [3] [5]
    %%     level0 [3 <-> 5]
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),

    ok = link_nodes(0, [Node3, Node5]),

    %% dump nodes on Level 0 and 1
    [{key3, value3, [0, 0]}, {key5, value5, [1, 1]}] = mio_node:dump_nodes(Node3, 0),
    [[{key3, value3, [0, 0]}], [{key5, value5, [1, 1]}]] = mio_node:dump_nodes(Node3, 1),

    %% search!
    {ok, value3} = mio_node:search(Node3, key3),
    {ok, value3} = mio_node:search(Node5, key3),
    {ok, value5} = mio_node:search(Node3, key5),
    {ok, value5} = mio_node:search(Node5, key5),
    ok.

search_level2_2(_Config) ->
    %% We want to test search-op without insert op.
    %%   setup predefined nodes as follows.
    %%     level1 [3 <-> 9] [5]
    %%     level0 [3 <-> 5 <-> 9]
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([1, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([0, 1])),
    {ok, Node9} = mio_sup:start_node(key9, value9, mio_mvector:make([1, 1])),

    ok = link_nodes(0, [Node3, Node5, Node9]),
    ok = link_nodes(1, [Node3, Node9]),

    %% dump nodes on Level 0 and 1
    [{key3, value3, [1, 0]}, {key5, value5, [0, 1]}, {key9, value9, [1, 1]}] = mio_node:dump_nodes(Node3, 0),

    Level1Nodes = [[{key5, value5, [0, 1]}], [{key3, value3, [1, 0]}, {key9, value9, [1, 1]}]],
    Level1Nodes = mio_node:dump_nodes(Node3, 1),
    Level1Nodes = mio_node:dump_nodes(Node5, 1),
    Level1Nodes = mio_node:dump_nodes(Node9, 1),

    %% search!
    {ok, value3} = mio_node:search(Node3, key3),
    {ok, value3} = mio_node:search(Node5, key3),
    {ok, value3} = mio_node:search(Node9, key3),

    {ok, value5} = mio_node:search(Node3, key5),
    {ok, value5} = mio_node:search(Node5, key5),
    {ok, value5} = mio_node:search(Node9, key5),

    {ok, value9} = mio_node:search(Node3, key9),
    {ok, value9} = mio_node:search(Node5, key9),

    ng = mio_node:search(Node5, key10),
    %% closest node should be returned
    {ok, _, key5, value5} = gen_server:call(Node5, {search, Node5, [], key8}),
    ok.

search_level2_3(_Config) ->
    %% We want to test search-op without insert op.
    %%   setup predefined nodes as follows.
    %%     level1 [3 <-> 7 <-> 9] [5 <-> 8]
    %%     level0 [3 <-> 5 <-> 7 <-> 8 <-> 9]
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    {ok, Node7} = mio_sup:start_node(key7, value7, mio_mvector:make([0, 1])),
    {ok, Node8} = mio_sup:start_node(key8, value8, mio_mvector:make([1, 0])),
    {ok, Node9} = mio_sup:start_node(key9, value9, mio_mvector:make([0, 0])),

    %% level 0
    ok = link_nodes(0, [Node3, Node5, Node7, Node8, Node9]),

    %% level 1
    ok = link_nodes(1, [Node3, Node7, Node9]),
    ok = link_nodes(1, [Node5, Node8]),

    %% dump nodes on Level 0 and 1
    [{key3, value3, [0, 0]}, {key5, value5, [1, 1]}, {key7, value7, [0, 1]}, {key8, value8, [1, 0]}, {key9, value9, [0, 0]}] = mio_node:dump_nodes(Node3, 0),
    Level1Nodes = [[{key3, value3, [0, 0]}, {key7, value7, [0, 1]}, {key9, value9, [0, 0]}], [{key5, value5, [1, 1]}, {key8, value8, [1, 0]}]],

    Level1Nodes= mio_node:dump_nodes(Node3, 1),
    Level1Nodes= mio_node:dump_nodes(Node5, 1),
    Level1Nodes= mio_node:dump_nodes(Node7, 1),
    Level1Nodes= mio_node:dump_nodes(Node8, 1),
    Level1Nodes= mio_node:dump_nodes(Node9, 1),

    %% search!
    %%  level1: 3->7 level0: 7->8
    {ok, value8} = mio_node:search(Node3, key8),
    {ok, value3} = mio_node:search(Node3, key3),
    {ok, value5} = mio_node:search(Node3, key5),
    {ok, value7} = mio_node:search(Node3, key7),
    {ok, value9} = mio_node:search(Node3, key9),

    {ok, value8} = mio_node:search(Node5, key8),
    {ok, value3} = mio_node:search(Node5, key3),
    {ok, value5} = mio_node:search(Node5, key5),
    {ok, value7} = mio_node:search(Node5, key7),
    {ok, value9} = mio_node:search(Node5, key9),

    {ok, value8} = mio_node:search(Node7, key8),
    {ok, value3} = mio_node:search(Node7, key3),
    {ok, value5} = mio_node:search(Node7, key5),
    {ok, value7} = mio_node:search(Node7, key7),
    {ok, value9} = mio_node:search(Node7, key9),

    {ok, value8} = mio_node:search(Node8, key8),
    {ok, value3} = mio_node:search(Node8, key3),
    {ok, value5} = mio_node:search(Node8, key5),
    {ok, value7} = mio_node:search(Node8, key7),
    {ok, value9} = mio_node:search(Node8, key9),

    {ok, value8} = mio_node:search(Node9, key8),
    {ok, value3} = mio_node:search(Node9, key3),
    {ok, value5} = mio_node:search(Node9, key5),
    {ok, value7} = mio_node:search(Node9, key7),
    {ok, value9} = mio_node:search(Node9, key9),


    ng = mio_node:search(Node5, key10),
    ng = mio_node:search(Node5, key6),
    %% closest node should be returned
    %% Is this ok?
    %%  The definition of closest node will change depends on whether search direction is right or left.
    {ok, _, key9, value9} = gen_server:call(Node5, {search, Node5, [], key9_9}),
    {ok, _, key7, value7} = gen_server:call(Node9, {search, Node9, [], key6}),
    ok.

test_set_nth(_Config) ->
    [1, 3] = mio_node:set_nth(2, 3, [1, 2]),
    [0, 2] = mio_node:set_nth(1, 0, [1, 2]),

    Level = 1,
    Level0Nodes = [{key3, value3, [0, 1]}, {key5, value5, [1, 1]}, {key7, value7, [1, 0]}, {key8, value8, [0, 0]}, {key9, value9, [1, 0]}],
    MVectors= lists:usort(fun(A, B) ->
                                  mio_mvector:gt(Level, A, B)
                          end,
                          lists:map(fun({_, _, MVector}) ->
                                            MVector
                                    end,
                                    Level0Nodes)),
   error_logger:info_msg("~p", [lists:map(fun(X) ->
                           lists:filter(
                             fun({_, _, MV}) ->
                                        mio_mvector:eq(Level, MV, X)
                             end,
                             Level0Nodes
                             )
                  end,
                  MVectors)]),



%%     [{myKex, myKexValue, [1, 0]}] = lists:usort(fun(A, B) ->
%%                                                         {_, _, AMVector} = A,
%%                                                         {_, _, BMVector} = B,
%%                                                         mio_mvector:gt(AMVector, BMVector)
%%                                                 end,
%%                                                 mio_node:dump_nodes(mio_node, 0)),




    ok.

link_op(_Config) ->
    {ok, Node2} = mio_sup:start_node(key2, value2, mio_mvector:make([0, 0])),
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),

    %% link on level 0
    Level = 0,
    ok = link_node(Level, Node3, Node5),
    ok = link_node(Level, Node2, Node3),

    %% check
    [{key2, value2, [0, 0]}, {key3, value3, [0, 0]}, {key5, value5, [1, 1]}] = mio_node:dump_nodes(Node3, 0),
    ok.

link_op_propagation(_Config) ->
%% TODO
%%     {ok, Node1} = mio_sup:start_node(key1, value1, mio_mvector:make([0, 0])),
%%     {ok, Node2} = mio_sup:start_node(key2, value2, mio_mvector:make([0, 0])),
%%     {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
%%     {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
%%     {ok, Node6} = mio_sup:start_node(key6, value6, mio_mvector:make([1, 1])),

%%     %% link on level 0
%%     %% 2 <-> 3
%%     Level = 0,
%%     ok = mio_node:link_op(Node2, Node3, right, Level),

%%     %% propagation case to right
%%     %% 2 <-> 3 <-> 5
%%     ok = mio_node:link_op(Node2, Node5, right, Level),

%%     %% propagation case to left
%%     %% 1 <-> 2 <-> 3 <-> 5
%%     ok = mio_node:link_op(Node3, Node1, left, Level),

%%     %% propagation case to right
%%     %% 2 <-> 3 <-> 5 <-> 6
%%     ok = mio_node:link_op(Node3, Node6, right, Level),

%%     %% check
%%     [{key1, value1, [0, 0]}, {key2, value2, [0, 0]}, {key3, value3, [0, 0]}, {key5, value5, [1, 1]}, {key6, value6, [1, 1]}] = mio_node:dump_nodes(Node3, 0),
    ok.

buddy_op(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    {ok, Node7} = mio_sup:start_node(key7, value7, mio_mvector:make([0, 1])),
    {ok, Node8} = mio_sup:start_node(key8, value8, mio_mvector:make([1, 0])),
    {ok, Node9} = mio_sup:start_node(key9, value9, mio_mvector:make([0, 0])),

    %% level 0
    ok = link_nodes(0, [Node3, Node5, Node7, Node8, Node9]),
    {ok, Buddy} = mio_node:buddy_op(Node5, [0, 0], right, 0),
    {key9, value9} = gen_server:call(Buddy, get),

    {ok, Buddy2} = mio_node:buddy_op(Node3, [0, 0], right, 0),
    {key3, value3} = gen_server:call(Buddy2, get),

    {ok, Buddy3} = mio_node:buddy_op(Node8, [0, 1], left, 0),
    {key7, value7} = gen_server:call(Buddy3, get),


    ok.

insert_op_self(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    ok = mio_node:insert_op(Node3, Node3).

insert_op_two_nodes(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    ok = mio_node:insert_op(Node3, Node3),
    ok = mio_node:insert_op(Node5, Node3),

    %% check on level 0
    [{key3, value3, [0, 0]}, {key5, value5, [1, 1]}] = mio_node:dump_nodes(Node3, 0),
    [{key3, value3, [0, 0]}, {key5, value5, [1, 1]}] = mio_node:dump_nodes(Node5, 0),

    %% check on level 1
    [[{key3,value3,[0,0]}], [{key5,value5,[1,1]}]] = mio_node:dump_nodes(Node3, 1),
    [[{key3,value3,[0,0]}], [{key5,value5,[1,1]}]] = mio_node:dump_nodes(Node5, 1),
    ok.

insert_op_three_nodes(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    {ok, Node7} = mio_sup:start_node(key7, value7, mio_mvector:make([1, 0])),
    ok = mio_node:insert_op(Node3, Node3),
    ok = mio_node:insert_op(Node5, Node3),
    ok = mio_node:insert_op(Node7, Node3),

    %% check on level 0
    [{key3, value3, [0, 0]}, {key5, value5, [1, 1]}, {key7, value7, [1, 0]}] = mio_node:dump_nodes(Node3, 0),
    [{key3, value3, [0, 0]}, {key5, value5, [1, 1]}, {key7, value7, [1, 0]}] = mio_node:dump_nodes(Node5, 0),
    [{key3, value3, [0, 0]}, {key5, value5, [1, 1]}, {key7, value7, [1, 0]}] = mio_node:dump_nodes(Node7, 0),

    %% check on level 1
    [[{key3,value3,[0, 0]}], [{key5,value5,[1, 1]}, {key7,value7,[1, 0]}]] = mio_node:dump_nodes(Node3, 1),
%%     [[{key3,value3,[0,0]}], [{key5,value5,[1,1]}]] = mio_node:dump_nodes(Node5, 1),
    ok.



all() ->
    [test_set_nth, get_call, left_right_call, dump_nodes_call, search_call, search_level2_simple, search_level2_1
     %,search_level2_2%, search_level2_3
     %link_op, link_op_propagation, buddy_op, insert_op_self, insert_op_two_nodes, insert_op_three_nodes].
     ].

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
link_node(Level, NodeA, NodeB) ->
    mio_node:link_right_op(NodeA, Level, NodeB),
    mio_node:link_left_op(NodeB, Level, NodeA).

%%    ok = mio_node:link_op(NodeA, NodeB, right, Level).

link_nodes(Level, [NodeA | [NodeB | More]]) ->
    link_node(Level, NodeA, NodeB),
    link_nodes(Level, [NodeB | More]);
link_nodes(Level, []) -> ok;
link_nodes(Level, [Node | []]) -> ok.
