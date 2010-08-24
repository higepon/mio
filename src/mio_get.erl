%%    Copyright (C) 2010 Cybozu Labs, Inc., written by Taro Minowa(Higepon) <higepon@labs.cybozu.co.jp>
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
%%% File    : mio_get.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : mio stats
%%%
%%% Created : 23 Aug 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_get).

-export([start/0]).

get_arg(Key, DefaultValue) ->
    case init:get_argument(Key) of
        {ok, [[Value]]} ->
            Value;
        _ -> 
            DefaultValue
    end.

do_get(Conn, Key, Ntimes) ->
    do_get(Conn, Key, 0, Ntimes).

do_get(_Conn, _Key, Counter, Ntimes) when Counter =:= Ntimes ->
    ok;
do_get(Conn, Key, Counter, Ntimes) ->
    memcached:get(Conn, Key),
    do_get(Conn, Key, Counter + 1, Ntimes).

start() ->
    Port = list_to_integer(get_arg(port, "11211")),
    Key = get_arg(key, "1"),
    Ntimes = list_to_integer(get_arg(ntimes, "1")),
    io:format("~nget ~p times: ", [Ntimes]),
    {ok, Conn} = memcached:connect("127.0.0.1", Port),
    statistics(wall_clock),
    do_get(Conn, Key, Ntimes),
    {_, Msec} = statistics(wall_clock),
    io:format("~p msec~n~n", [Msec]),
    memcached:disconnect(Conn),
    halt(0).
