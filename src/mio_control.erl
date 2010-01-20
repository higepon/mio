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
%%% File    : mio_control.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : Control mio.
%%%
%%% Created : 25 Nov 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_control).
-include("mio.hrl").

%% API
-export([start/0, usage/0]).

%%====================================================================
%% API
%%====================================================================
start() ->
    case init:get_argument(command) of
        {ok, [["stop"|_]|_]} ->
            io:format("stopping"),
            command_stop(),
            halt();
        _ ->
            usage(),
            halt(1)
    end.


usage() ->
    io:format("~nUsage:mioctl [-n <node_name>] [-c <cookie>] <command>

Available commands:

  stop      - stops the mio application on <node_name>

"),
    ok.


%%====================================================================
%% Internal functions
%%====================================================================
command_stop() ->
    case init:get_argument(nodename) of
        {ok, [[NodeName|_]|_]} ->
            Node = list_to_atom(NodeName),
            case call(Node, application, stop, [mio]) of
                ok -> ok;
                error ->
                    halt(1)
            end,
            case call(Node, init, stop, []) of
                ok -> ok;
                error ->
                    halt(1)
            end,
            io:format("...done~n");
        _ ->
            ?ERROR("nodename is not specified~n"),
            usage()
    end.


call(Node, Module, Func, Args) ->
    case rpc:call(Node, Module, Func, Args) of
        ok ->
            ok;
        {error, Reason} ->
            ?ERRORF("Error: ~p", [Reason]),
            error;
        {badrpc, Reason} ->
            ?ERRORF("unable to connect mio node ~w: ~w", [Node, Reason]),
            error;
        Other ->
            ?ERRORF("Error: ~p", [Other]),
            error
    end.
