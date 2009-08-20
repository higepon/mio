%%%-------------------------------------------------------------------
%%% File    : mio_node_SUITE.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description :
%%%
%%% Created : 30 Jun 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_node_SUITE).

-compile(export_all).
-include("mio.hrl").

init_per_suite(Config) ->
    %% config file is specified on runtest's command line option
    IsVerbose = ct:get_config(isVerbose),
    if IsVerbose ->
            error_logger:tty(true);
       true ->
            error_logger:tty(false)
    end,
    ok = error_logger:logfile({open, "./error.log"}),
%%     {ok, Pid} = mio_sup:start_link(),
%%     unlink(Pid),
    application:start(mio),
    {ok, NodePid} = mio_sup:start_node(myKey, myValue, mio_mvector:make([1, 0])),
    register(mio_node, NodePid),
    Config.

end_per_suite(_Config) ->
    application:stop(mio),
    ok.

get_call(_Config) ->
    {myKey, myValue, _, _, _} = gen_server:call(mio_node, get_op),
    {myKey, myValue, _, _, _} = gen_server:call(mio_node, get_op),
    ok.

%% very simple case: there is only one node.
search_level2_simple(_Config) ->
    {ok, Node} = mio_sup:start_node(myKey, myValue, mio_mvector:make([1, 0])),
    {ok, _, myKey, myValue} = gen_server:call(Node, {search_op, [], myKey}),

    %% dump nodes on Level 0 and 1
    [{_, myKey, myValue, [1, 0]}] = mio_node:dump_op(Node, 0),
    [[{_, myKey, myValue, [1, 0]}]] = mio_node:dump_op(Node, 1),
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
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}]= mio_node:dump_op(Node3, 0),
    [[{_, key3, value3, [0, 0]}], [{_, key5, value5, [1, 1]}]] = mio_node:dump_op(Node3, 1),

    %% search!
    {ok, value3} = mio_node:search_op(Node3, key3),
    {ok, value3} = mio_node:search_op(Node5, key3),
    {ok, value5} = mio_node:search_op(Node3, key5),
    {ok, value5} = mio_node:search_op(Node5, key5),
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
    [{_, key3, value3, [1, 0]}, {_, key5, value5, [0, 1]}, {_, key9, value9, [1, 1]}] = mio_node:dump_op(Node3, 0),

    [[{_, key5, value5, [0, 1]}], [{_, key3, value3, [1, 0]}, {_, key9, value9, [1, 1]}]] = mio_node:dump_op(Node3, 1),
    [[{_, key5, value5, [0, 1]}], [{_, key3, value3, [1, 0]}, {_, key9, value9, [1, 1]}]] = mio_node:dump_op(Node5, 1),
    [[{_, key5, value5, [0, 1]}], [{_, key3, value3, [1, 0]}, {_, key9, value9, [1, 1]}]] = mio_node:dump_op(Node9, 1),

    %% search!
    {ok, value3} = mio_node:search_op(Node3, key3),
    {ok, value3} = mio_node:search_op(Node5, key3),
    {ok, value3} = mio_node:search_op(Node9, key3),

    {ok, value5} = mio_node:search_op(Node3, key5),
    {ok, value5} = mio_node:search_op(Node5, key5),
    {ok, value5} = mio_node:search_op(Node9, key5),

    {ok, value9} = mio_node:search_op(Node3, key9),
    {ok, value9} = mio_node:search_op(Node5, key9),

    ng = mio_node:search_op(Node5, key10),
    %% closest node should be returned
    {ok, _, key5, value5} = gen_server:call(Node5, {search_op, [], key8}),
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
    [[{_, key3, value3, [0, 0]}, {_, key7, value7, [0, 1]}, {_, key9, value9, [0, 0]}], [{_, key5, value5, [1, 1]}, {_, key8, value8, [1, 0]}]] = mio_node:dump_op(Node3, 1),
    [[{_, key3, value3, [0, 0]}, {_, key7, value7, [0, 1]}, {_, key9, value9, [0, 0]}], [{_, key5, value5, [1, 1]}, {_, key8, value8, [1, 0]}]] = mio_node:dump_op(Node5, 1),
    [[{_, key3, value3, [0, 0]}, {_, key7, value7, [0, 1]}, {_, key9, value9, [0, 0]}], [{_, key5, value5, [1, 1]}, {_, key8, value8, [1, 0]}]] = mio_node:dump_op(Node7, 1),
    [[{_, key3, value3, [0, 0]}, {_, key7, value7, [0, 1]}, {_, key9, value9, [0, 0]}], [{_, key5, value5, [1, 1]}, {_, key8, value8, [1, 0]}]] = mio_node:dump_op(Node8, 1),
    [[{_, key3, value3, [0, 0]}, {_, key7, value7, [0, 1]}, {_, key9, value9, [0, 0]}], [{_, key5, value5, [1, 1]}, {_, key8, value8, [1, 0]}]] = mio_node:dump_op(Node9, 1),

    %% search!
    %%  level1: 3->7 level0: 7->8
    {ok, value8} = mio_node:search_op(Node3, key8),
    {ok, value3} = mio_node:search_op(Node3, key3),
    {ok, value5} = mio_node:search_op(Node3, key5),
    {ok, value7} = mio_node:search_op(Node3, key7),
    {ok, value9} = mio_node:search_op(Node3, key9),

    {ok, value8} = mio_node:search_op(Node5, key8),
    {ok, value3} = mio_node:search_op(Node5, key3),
    {ok, value5} = mio_node:search_op(Node5, key5),
    {ok, value7} = mio_node:search_op(Node5, key7),
    {ok, value9} = mio_node:search_op(Node5, key9),

    {ok, value8} = mio_node:search_op(Node7, key8),
    {ok, value3} = mio_node:search_op(Node7, key3),
    {ok, value5} = mio_node:search_op(Node7, key5),
    {ok, value7} = mio_node:search_op(Node7, key7),
    {ok, value9} = mio_node:search_op(Node7, key9),

    {ok, value8} = mio_node:search_op(Node8, key8),
    {ok, value3} = mio_node:search_op(Node8, key3),
    {ok, value5} = mio_node:search_op(Node8, key5),
    {ok, value7} = mio_node:search_op(Node8, key7),
    {ok, value9} = mio_node:search_op(Node8, key9),

    {ok, value8} = mio_node:search_op(Node9, key8),
    {ok, value3} = mio_node:search_op(Node9, key3),
    {ok, value5} = mio_node:search_op(Node9, key5),
    {ok, value7} = mio_node:search_op(Node9, key7),
    {ok, value9} = mio_node:search_op(Node9, key9),


    ng = mio_node:search_op(Node5, key10),
    ng = mio_node:search_op(Node5, key6),
    %% closest node should be returned
    %% Is this ok?
    %%  The definition of closest node will change depends on whether search direction is right or left.
    {ok, _, key9, value9} = gen_server:call(Node5, {search_op, [], key9_9}),
    {ok, _, key7, value7} = gen_server:call(Node9, {search_op, [], key6}),
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
    [{_, key2, value2, [0, 0]}, {_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}] = mio_node:dump_op(Node3, 0),
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
    {key9, value9, _, _, _} = gen_server:call(Buddy, get_op),

    {ok, Buddy2} = mio_node:buddy_op(Node3, [0, 0], right, 0),
    {key3, value3, _, _, _} = gen_server:call(Buddy2, get_op),

    {ok, Buddy3} = mio_node:buddy_op(Node8, [0, 1], left, 0),
    {key7, value7, _, _, _} = gen_server:call(Buddy3, get_op),
    ok.

delete_op(_Config) ->
    [Node3, _, Node7, _] = setup_nodes_for_range_search_op(),
    [{_, key3, _, _}, {_, key5, _, _}, {_, key7, _, _}, {_, key9, _, _}] = mio_node:dump_op(Node3, 0),
    [[{_, key3, _, _}, {_, key9, _, _}], [{_, key5, _, _}, {_, key7, _, _}]] = mio_node:dump_op(Node3, 1),

    %% delete key5!
    ok = mio_node:delete_op(Node3, key5),
    [{_, key3, _, _}, {_, key7, _, _}, {_, key9, _, _}] = mio_node:dump_op(Node3, 0),
    [[{_, key3, _, _}, {_, key9, _, _}], [{_, key7, _, _}]] = mio_node:dump_op(Node3, 1),

    %% key11 not exist
    ng = mio_node:delete_op(Node3, key11),
    [{_, key3, _, _}, {_, key7, _, _}, {_, key9, _, _}] = mio_node:dump_op(Node3, 0),
    [[{_, key3, _, _}, {_, key9, _, _}], [{_, key7, _, _}]] = mio_node:dump_op(Node3, 1),

    %% delete key9!
    ok = mio_node:delete_op(Node3, key9),
    [{_, key3, _, _}, {_, key7, _, _}] = mio_node:dump_op(Node3, 0),
    [[{_, key3, _, _}], [{_, key7, _, _}]] = mio_node:dump_op(Node3, 1),

    %% delete key3!, introducer == self
    Ref = erlang:monitor(process, Node3),
    ok = mio_node:delete_op(Node3, key3),
    [{_, key7, _, _}] = mio_node:dump_op(Node7, 0),
    [[{_, key7, _, _}]] = mio_node:dump_op(Node7, 1),

    %% Node3 should be teminated
    receive
        {'DOWN', Ref, process, Node3, Reason} ->
            io:format("process is down, reason: ~p.~n",
                      [Reason]);
        Any ->
            throw("ANY")
    after 1000 ->
            throw("timeout.~n")
    end,
    ok.

insert_op_self(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    ok = mio_node:insert_op(Node3, Node3).

insert_op_two_nodes(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    ok = mio_node:insert_op(Node3, Node3),
    ok = mio_node:insert_op(Node3, Node5),

    %% check on level 0
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}] = mio_node:dump_op(Node3, 0),
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}] = mio_node:dump_op(Node5, 0),

    %% check on level 1
    [[{_, key3,value3,[0,0]}], [{_, key5,value5,[1,1]}]] = mio_node:dump_op(Node3, 1),
    [[{_, key3,value3,[0,0]}], [{_, key5,value5,[1,1]}]] = mio_node:dump_op(Node5, 1),
    ok.

insert_op_two_nodes_2(_Config) ->
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    {ok, Node7} = mio_sup:start_node(key7, value7, mio_mvector:make([1, 0])),
    ok = mio_node:insert_op(Node7, Node7),
    ok = mio_node:insert_op(Node7, Node5),

    [{_, key5, value5, [1, 1]}, {_, key7, value7, [1, 0]}] = mio_node:dump_op(Node5, 0),
    [[{_, key5, value5, [1, 1]}, {_, key7, value7, [1, 0]}]] = mio_node:dump_op(Node5, 1),
    ok.

insert_op_two_nodes_3(_Config) ->
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    {ok, Node6} = mio_sup:start_node(key6, value6, mio_mvector:make([1, 1])),
    {ok, Node7} = mio_sup:start_node(key7, value7, mio_mvector:make([1, 1])),

    % insert and check on level 1
    ok = mio_node:insert_op(Node5, Node5),
    [[{_, key5, value5, [1, 1]}]] = mio_node:dump_op(Node5, 1),

    % insert and check on level 1
    ok = mio_node:insert_op(Node5, Node7),
    [[{_, key5, value5, [1, 1]}, {_, key7, value7, [1, 1]}]] = mio_node:dump_op(Node5, 1),

    ok = mio_node:insert_op(Node5, Node6),
    ok.

insert_op_three_nodes(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    {ok, Node7} = mio_sup:start_node(key7, value7, mio_mvector:make([1, 0])),
    ok = mio_node:insert_op(Node3, Node3),
    ok = mio_node:insert_op(Node3, Node5),

    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}] = mio_node:dump_op(Node3, 0),
    [[{_, key3, value3, [0, 0]}], [{_, key5, value5, [1, 1]}]] = mio_node:dump_op(Node3, 1),

    ok = mio_node:insert_op(Node3, Node7),

    %% check on level 0
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}, {_, key7, value7, [1, 0]}] = mio_node:dump_op(Node3, 0),
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}, {_, key7, value7, [1, 0]}] = mio_node:dump_op(Node5, 0),
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}, {_, key7, value7, [1, 0]}] = mio_node:dump_op(Node7, 0),

    %% check next node is correct?
    {_, _, _, LeftNodes7, RightNode7} = gen_server:call(Node7, get_op),
    [] = mio_node:node_on_level(RightNode7, 0),
    Node5 = mio_node:node_on_level(LeftNodes7, 0),

    {_, _, _, LeftNodes, RightNode} = gen_server:call(Node5, get_op),
    Node7 = mio_node:node_on_level(RightNode, 0),
    Node3 = mio_node:node_on_level(LeftNodes, 0),

    %% check on level 1
    [[{_, key3,value3,[0, 0]}], [{_, key5,value5,[1, 1]}, {_, key7,value7,[1, 0]}]] = mio_node:dump_op(Node3, 1),
    [[{_, key3,value3,[0, 0]}], [{_, key5,value5,[1, 1]}, {_, key7,value7,[1, 0]}]] = mio_node:dump_op(Node5, 1),
    ok.

%% for buddy-op coverage
insert_op_three_nodes_2(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    {ok, Node7} = mio_sup:start_node(key7, value7, mio_mvector:make([1, 1])),
    ok = mio_node:insert_op(Node5, Node5),
    ok = mio_node:insert_op(Node5, Node7),

    [{_, key5, value5, [1, 1]}, {_, key7, value7, [1, 1]}] = mio_node:dump_op(Node5, 0),
    [[{_, key5, value5, [1, 1]}, {_, key7, value7, [1, 1]}]] = mio_node:dump_op(Node7, 1),

    ok = mio_node:insert_op(Node5, Node3),

    %% check on level 0
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}, {_, key7, value7, [1, 1]}] = mio_node:dump_op(Node3, 0),
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}, {_, key7, value7, [1, 1]}] = mio_node:dump_op(Node5, 0),
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}, {_, key7, value7, [1, 1]}] = mio_node:dump_op(Node7, 0),

    %% check next node is correct?
    {_, _, _, LeftNodes7, RightNode7} = gen_server:call(Node7, get_op),
    [] = mio_node:node_on_level(RightNode7, 0),
    Node5 = mio_node:node_on_level(LeftNodes7, 0),

    {_, _, _, LeftNodes, RightNode} = gen_server:call(Node5, get_op),
    Node7 = mio_node:node_on_level(RightNode, 0),
    Node3 = mio_node:node_on_level(LeftNodes, 0),

    %% check on level 1
    [[{_, key3,value3,[0, 0]}], [{_, key5,value5,[1, 1]}, {_, key7,value7,[1, 1]}]] = mio_node:dump_op(Node3, 1),
    [[{_, key3,value3,[0, 0]}], [{_, key5,value5,[1, 1]}, {_, key7,value7,[1, 1]}]] = mio_node:dump_op(Node5, 1),
    ok.

insert_op_three_nodes_3(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    {ok, Node7} = mio_sup:start_node(key7, value7, mio_mvector:make([1, 1])),
    ok = mio_node:insert_op(Node5, Node5),
    ok = mio_node:insert_op(Node5, Node7),

    [{_, key5, value5, [1, 1]}, {_, key7, value7, [1, 1]}] = mio_node:dump_op(Node5, 0),
    [[{_, key5, value5, [1, 1]}, {_, key7, value7, [1, 1]}]] = mio_node:dump_op(Node7, 1),

    ok = mio_node:insert_op(Node5, Node3),

    %% check on level 0
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}, {_, key7, value7, [1, 1]}] = mio_node:dump_op(Node3, 0),
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}, {_, key7, value7, [1, 1]}] = mio_node:dump_op(Node5, 0),
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}, {_, key7, value7, [1, 1]}] = mio_node:dump_op(Node7, 0),

    %% check next node is correct?
    {_, _, _, LeftNodes7, RightNode7} = gen_server:call(Node7, get_op),
    [] = mio_node:node_on_level(RightNode7, 0),
    Node5 = mio_node:node_on_level(LeftNodes7, 0),

    {_, _, _, LeftNodes, RightNode} = gen_server:call(Node5, get_op),
    Node7 = mio_node:node_on_level(RightNode, 0),
    Node3 = mio_node:node_on_level(LeftNodes, 0),

    %% check on level 1
    [[{_, key3,value3,[0, 0]}], [{_, key5,value5,[1, 1]}, {_, key7,value7,[1, 1]}]] = mio_node:dump_op(Node3, 1),
    [[{_, key3,value3,[0, 0]}], [{_, key5,value5,[1, 1]}, {_, key7,value7,[1, 1]}]] = mio_node:dump_op(Node5, 1),
    ok.


insert_op_many_nodes(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    {ok, Node7} = mio_sup:start_node(key7, value7, mio_mvector:make([1, 0])),
    {ok, Node9} = mio_sup:start_node(key9, value9, mio_mvector:make([0, 1])),

    %% insert and check
    ok = mio_node:insert_op(Node3, Node3),
    [{_, key3, value3, [0, 0]}] = mio_node:dump_op(Node3, 0),
    [[{_, key3, value3, [0, 0]}]] = mio_node:dump_op(Node3, 1),

    %% insert and check
    ok = mio_node:insert_op(Node3, Node5),
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}] = mio_node:dump_op(Node3, 0),
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}] = mio_node:dump_op(Node5, 0),
    [[{_, key3, value3, [0, 0]}], [{_, key5, value5, [1, 1]}]] = mio_node:dump_op(Node3, 1),
    [[{_, key3, value3, [0, 0]}], [{_, key5, value5, [1, 1]}]] = mio_node:dump_op(Node5, 1),

    %% insert and check
    ok = mio_node:insert_op(Node5, Node9),
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}, {_, key9, value9, [0, 1]}] = mio_node:dump_op(Node3, 0),
    [[{_, key3, value3, [0, 0]}, {_, key9, value9, [0, 1]}], [{_, key5, value5, [1, 1]}]] = mio_node:dump_op(Node3, 1),

    %% insert and check
    ok = mio_node:insert_op(Node9, Node7),
    [{_, key3, value3, [0, 0]}, {_, key5, value5, [1, 1]}, {_, key7, value7, [1, 0]}, {_, key9, value9, [0, 1]}] = mio_node:dump_op(Node5, 0),
    [[{_, key3, value3, [0, 0]}, {_, key9, value9, [0, 1]}], [{_, key5, value5, [1, 1]}, {_, key7, value7, [1, 0]}]] = mio_node:dump_op(Node3, 1),
    ok.

setup_nodes_for_range_search_op() ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    {ok, Node7} = mio_sup:start_node(key7, value7, mio_mvector:make([1, 0])),
    {ok, Node9} = mio_sup:start_node(key9, value9, mio_mvector:make([0, 1])),
    ok = mio_node:insert_op(Node3, Node3),
    ok = mio_node:insert_op(Node3, Node5),
    ok = mio_node:insert_op(Node5, Node9),
    ok = mio_node:insert_op(Node9, Node7),
    [Node3, Node5, Node7, Node9].

range_search_asc_op(_Config) ->
    [Node3, _, _, _] = setup_nodes_for_range_search_op(),

    [{_, key5, value5}, {_, key7, value7}] = mio_node:range_search_asc_op(Node3, key3, key9, 10),
    [{_, key5, value5}, {_, key7, value7}, {_, key9, value9}] = mio_node:range_search_asc_op(Node3, key3, key999, 10),
    [{_, key5, value5}, {_, key7, value7}] = mio_node:range_search_asc_op(Node3, key3, key999, 2),
    [{_, key5, value5}] = mio_node:range_search_asc_op(Node3, key3, key999, 1),
    [] = mio_node:range_search_asc_op(Node3, key3, key999, 0),
    [{_, key3, value3}] = mio_node:range_search_asc_op(Node3, key1, key4, 1),
    [] = mio_node:range_search_asc_op(Node3, key99, key999, 1),
    ok.

range_search_desc_op(_Config) ->
    [Node3, _, _, _] = setup_nodes_for_range_search_op(),

    [{_, key7, value7}] = mio_node:range_search_desc_op(Node3, key3, key9, 1),
    [{_, key9, value9}, {_, key7, value7}, {_, key5, value5}] = mio_node:range_search_desc_op(Node3, key3, key99, 10),
    [{_, key9, value9}, {_, key7, value7}] = mio_node:range_search_desc_op(Node3, key3, key99, 2),
    [] = mio_node:range_search_desc_op(Node3, key3, key5, 2),
    [{_, key3, value3}] = mio_node:range_search_desc_op(Node3, key2, key5, 1),
    [] = mio_node:range_search_desc_op(Node3, key1, key2, 1),
    ok.

search_closest(_Config) ->
    [Node3, Node5, Node7, Node9] = setup_nodes_for_range_search_op(),

    %% search always returns left closest key
    {ok, _, key7, value7} = gen_server:call(Node3, {search_op, [], key8}),
    {ok, _, key7, value7} = gen_server:call(Node5, {search_op, [], key8}),
    {ok, _, key7, value7} = gen_server:call(Node7, {search_op, [], key8}),
    {ok, _, key7, value7} = gen_server:call(Node9, {search_op, [], key8}),
    ok.

overwrite_value(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    {ok, Node7} = mio_sup:start_node(key7, value7, mio_mvector:make([1, 0])),
    {ok, Node9} = mio_sup:start_node(key9, value9, mio_mvector:make([0, 1])),

    {ok, NewNode3} = mio_sup:start_node(key3, new_value3, mio_mvector:make([0, 0])),
    ok = mio_node:insert_op(Node3, Node3),
    ok = mio_node:insert_op(Node3, Node5),
    ok = mio_node:insert_op(Node5, Node9),
    ok = mio_node:insert_op(Node9, Node7),
    ok = mio_node:insert_op(Node9, NewNode3),
    {ok, new_value3} = mio_node:search_op(Node9, key3),
    ok.

search_not_found(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    {ok, Node5} = mio_sup:start_node(key5, value5, mio_mvector:make([1, 1])),
    ok = mio_node:insert_op(Node3, Node3),
    ok = mio_node:insert_op(Node3, Node5),
    ng =  mio_node:search_op(Node5, key4),
    ok.

handle_info(_Config) ->
    {ok, Node3} = mio_sup:start_node(key3, value3, mio_mvector:make([0, 0])),
    Node3 ! "hello".

terminate_node(_Config) ->
    [Node3, _, _, _] = setup_nodes_for_range_search_op(),
    %% this causes error, mio_node:terminate will be called
    try gen_server:call(Node3, {buddy_op, xxx, xxxx, xxx}) of
        _ -> []
    catch
        throw:_ -> [];
        exit:_ -> [];
        error:_ -> []
    end.

node_on_level(_Config) ->
    mio_node:node_on_level([], 0).

all() ->
    [
     test_set_nth,
     get_call,
     search_not_found,
     search_level2_simple,
    search_level2_1,
     search_level2_2,
     search_level2_3,
     link_op,
     buddy_op,
     delete_op,
     insert_op_self,
     insert_op_two_nodes,
     insert_op_two_nodes_2,
     insert_op_two_nodes_3,
     insert_op_three_nodes,
     insert_op_three_nodes_2,
     insert_op_three_nodes_3,
     insert_op_many_nodes,
     range_search_asc_op,
     range_search_desc_op,
     overwrite_value,
     handle_info,
     terminate_node,
     node_on_level
     ].

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
link_node(Level, NodeA, NodeB) ->
    mio_node:link_right_op(NodeA, Level, NodeB),
    mio_node:link_left_op(NodeB, Level, NodeA).

link_nodes(Level, [NodeA | [NodeB | More]]) ->
    link_node(Level, NodeA, NodeB),
    link_nodes(Level, [NodeB | More]);
link_nodes(_Level, []) -> ok;
link_nodes(_Level, [_Node | []]) -> ok.
