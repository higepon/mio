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
%%% File    : mio_node_info.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : Skip Graph Node information.
%%%
%%% Created : 30 Jan 2010 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_node_info).

-include("mio.hrl").

%% API
-export([start/0, stop/0,
         is_deleted/1, set_deleted/1
        ]).

start() ->
    try do_start() of
        ok -> ok
    catch
        throw:X -> {error, X}
    end.

do_start() ->
    case mnesia:create_schema([node()]) of
        ok -> ok;
        {error, {_, {already_exists, _}}} -> ok;
        {error, Reason} -> throw({mnesia_create_scheme, Reason})
    end,
    case mnesia:start() of
        ok -> ok;
        {error, Reason2} -> throw({mnesia_start, Reason2})
    end,
    case mnesia:create_table(is_deleted, [{attributes, [pid, is_deleted]}]) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, _}} -> ok;
        {error, Reason3} -> throw({mnesia_create_table, Reason3})
    end,
    ok = mnesia:wait_for_tables([is_deleted], 10000),
    ok.

stop() ->
    stopped = mnesia:stop(),
    ok.

is_deleted(Pid) ->
    case mnesia:dirty_read({is_deleted, Pid}) of
        [] -> false;
        [{_, _Pid, IsDeleted}] -> IsDeleted
    end.

set_deleted(Pid) ->
    ok = mnesia:dirty_write({is_deleted, Pid, true}).
%%     F = fun() -> mnesia:write({is_deleted, Pid, true}) end,
%%     {atomic, ok} = mnesia:transaction(F).
