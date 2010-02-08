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
%%% File    : mio_node_info.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : Skip Graph Node information.
%%%
%%% Created : 30 Jan 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_node_info).

-include("mio.hrl").
-record(node_info, {pid, is_deleted, array_of_inserted}).

%% API
-export([start/0, stop/0,
         make_empty_info/2,
         delete/1,
         is_deleted/1, set_deleted/1,
         is_inserted/1, is_inserted/2, set_inserted/1, set_inserted/2
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
    %% for developer mode
    mnesia:delete_table(node_info),
    case mnesia:create_table(node_info, [{attributes, record_info(fields, node_info)}]) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, _}} -> ok;
        {error, Reason3} -> throw({mnesia_create_table, Reason3})
    end,
    ok = mnesia:wait_for_tables([node_info], 10000),
    ok.

stop() ->
    stopped = mnesia:stop(),
    ok.

make_empty_info(Pid, MaxLevel) ->
    ok = mnesia:dirty_write({node_info, Pid, false, lists:duplicate(MaxLevel, false)}).

is_deleted(Pid) ->
    [Info] = mnesia:dirty_read({node_info, Pid}),
    Info#node_info.is_deleted.

set_deleted(Pid) ->
    [Info] = mnesia:dirty_read({node_info, Pid}),
    ok = mnesia:dirty_write(Info#node_info{is_deleted=true}).

is_inserted(Pid) ->
    [Info] = mnesia:dirty_read({node_info, Pid}),
    lists:all(fun(X) -> X end, Info#node_info.array_of_inserted).

is_inserted(Pid, Level) ->
    [Info] = mnesia:dirty_read({node_info, Pid}),
    mio_node:node_on_level(Info#node_info.array_of_inserted, Level).

set_inserted(Pid) ->
    [Info] = mnesia:dirty_read({node_info, Pid}),
    ok = mnesia:dirty_write(Info#node_info{array_of_inserted=lists:duplicate(length(Info#node_info.array_of_inserted) + 1, true)}).

set_inserted(Pid, Level) ->
    [Info] = mnesia:dirty_read({node_info, Pid}),
    ok = mnesia:dirty_write(Info#node_info{array_of_inserted=mio_util:lists_set_nth(Level + 1, true, Info#node_info.array_of_inserted)}).

delete(Pid) ->
    ok = mnesia:dirty_delete(node_info, Pid).
