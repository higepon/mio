%%    Copyright (c) 2009-2010  Taro Minowa(Higepon) <higepon@users.sourceforge.jp>
%%
%%    Redistribution and use in source and binary forms, with or without
%%    modification, are permitted provided that the following conditions
%%    are met:
%%
%%    1. Redistributions of source code must retain the above copyright
%%       notice, this list of conditions and the following disclaimer.
%%
%%    2. Redistributions in binary form must reproduce the above copyright
%%       notice, this list of conditions and the following disclaimer in the
%%       documentation and/or other materials provided with the distribution.
%%
%%    3. Neither the name of the authors nor the names of its contributors
%%       may be used to endorse or promote products derived from this
%%       software without specific prior written permission.
%%
%%    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
%%    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
%%    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
%%    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
%%    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
%%    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
%%    TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
%%    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
%%    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
%%    NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
%%    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

%%%-------------------------------------------------------------------
%%% File    : mio_dump.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : Dumper
%%%
%%% Created : 28 Jan 2010 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_dump).

%% API
-export([dump_op/3]).
-include("mio.hrl").

dump_op(_StartNode, Current, MaxLevel) when Current > MaxLevel ->
    [];
dump_op(StartNode, Current, MaxLevel) ->
    CurrentDump = dump_op(StartNode, Current),
    ?INFOF("current =~p", [Current]),
    D = lists:map(fun(Nodes) ->
                      [{_Pid, _Key, _Value, MV} | _More] = Nodes,
                      {mio_mvector:get(MV, Current), length(Nodes)}
                  end,
                  CurrentDump),
    ?INFOF("D=~p", [D]),
    dump_op(StartNode, Current + 1, MaxLevel).

dump_op(StartNode, Level) ->
    Level0Nodes = enum_nodes_(StartNode, 0),
    case Level of
        0 ->
            [Level0Nodes];
        _ ->
            StartNodes= lists:map(fun({Node, _}) -> Node end, lists:usort(fun({_, A}, {_, B}) -> mio_mvector:gt(Level, B, A) end,
                                                                          lists:map(fun({Node, _, _, MV}) -> {Node, MV} end,
                                                                                    Level0Nodes))),
            lists:map(fun(Node) ->
                              lists:map(fun({Pid, Key, Value, MV}) -> {Pid, Key, Value, MV} end,
                                        enum_nodes_(Node, Level))
                      end,
                      StartNodes)
    end.

dump_side_([], _Side, _Level) ->
    [];
dump_side_(StartNode, Side, Level) ->
    gen_server:cast(StartNode, {dump_side_cast, Side, Level, self(), []}),
    receive
        {dump_side_accumed, Accumed} ->
            Accumed
    end.

enum_nodes_(StartNode, Level) ->
    {Key, Value, MembershipVector, LeftNodes, RightNodes} = gen_server:call(StartNode, get_op),
    RightNode = mio_node:node_on_level(RightNodes, Level),
    LeftNode = mio_node:node_on_level(LeftNodes, Level),
    lists:append([dump_side_(LeftNode, left, Level),
                  [{StartNode, Key, Value, MembershipVector}],
                  dump_side_(RightNode, right, Level)]).
