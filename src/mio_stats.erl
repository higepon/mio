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
%%% File    : mio_stats.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : mio stats
%%%
%%% Created : 22 June 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_stats).
-export([init/1,
         uptime/1,
         bytes/0,
         sweeped_items/1, inc_sweeped_items/2,
         total_items/1, inc_total_items/1, dec_total_items/1,
         cmd_get_multi/1, inc_cmd_get_multi/1,
         cmd_get_multi_avg_time/1,
         cmd_get_multi_total_time/1, inc_cmd_get_multi_total_time/2,
         cmd_get_multi_worst_time/1, set_cmd_get_multi_worst_time/2,
         cmd_get_worst_time/1, cmd_get_avg_time/1, cmd_get_total_time/1,
         cmd_get/1, inc_cmd_get/1,  inc_cmd_get_total_time/2, set_cmd_get_worst_time/2]).
-include_lib("eunit/include/eunit.hrl").
-include("mio.hrl").

init(LocalSetting) ->
    init_uptime(LocalSetting),
    init_total_items(LocalSetting),
    init_cmd_get(LocalSetting),
    init_cmd_get_total_time(LocalSetting),
    init_cmd_get_worst_time(LocalSetting),
    init_cmd_get_multi(LocalSetting),
    init_cmd_get_multi_total_time(LocalSetting),
    init_cmd_get_multi_worst_time(LocalSetting),
    init_sweeped_items(LocalSetting).

bytes() ->
    erlang:memory(total).

init_uptime(LocalSetting) ->
    {_, NowSec, _} = now(),
    ok = mio_local_store:set(LocalSetting, stats_uptime, NowSec).

uptime(LocalSetting) ->
    {ok, StartSec} = mio_local_store:get(LocalSetting, stats_uptime),
    {_, NowSec, _} = now(),
    NowSec - StartSec.

get(LocalSetting, Key) ->
    {ok, Item} = mio_local_store:get(LocalSetting, Key),
    Item.

inc(LocalSetting, Key) ->
    ok = mio_local_store:set(LocalSetting, Key, get(LocalSetting, Key) + 1).

dec(LocalSetting, Key) ->
    ok = mio_local_store:set(LocalSetting, Key, get(LocalSetting, Key) - 1).

init_total_items(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_total_items, 0).

init_sweeped_items(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_sweeped_items, 0).

total_items(LocalSetting) ->
    get(LocalSetting, stats_total_items).

sweeped_items(LocalSetting) ->
    get(LocalSetting, stats_sweeped_items).

inc_sweeped_items(LocalSetting, N) ->
    Prev = sweeped_items(LocalSetting),
    ok = mio_local_store:set(LocalSetting, stats_sweeped_items, N + Prev).

inc_total_items(LocalSetting) ->
    inc(LocalSetting, stats_total_items).

dec_total_items(LocalSetting) ->
    dec(LocalSetting, stats_total_items).

init_cmd_get(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_cmd_get, 0).

cmd_get(LocalSetting) ->
    get(LocalSetting, stats_cmd_get).

inc_cmd_get(LocalSetting) ->
    inc(LocalSetting, stats_cmd_get).

init_cmd_get_multi(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_cmd_get_multi, 0).

cmd_get_multi(LocalSetting) ->
    get(LocalSetting, stats_cmd_get_multi).

inc_cmd_get_multi(LocalSetting) ->
    inc(LocalSetting, stats_cmd_get_multi).

init_cmd_get_multi_total_time(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_cmd_get_multi_total_time, 0).

cmd_get_multi_total_time(LocalSetting) ->
    get(LocalSetting, stats_cmd_get_multi_total_time).

inc_cmd_get_multi_total_time(LocalSetting, D) ->
    ok = mio_local_store:set(LocalSetting, stats_cmd_get_multi_total_time, get(LocalSetting, stats_cmd_get_multi_total_time) + D).

cmd_get_multi_avg_time(LocalSetting) ->
    case mio_stats:cmd_get_multi(LocalSetting) of
        0 ->
            0;
        N ->
            mio_stats:cmd_get_multi_total_time(LocalSetting) / N
    end.

init_cmd_get_multi_worst_time(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_cmd_get_multi_worst_time, 0).

cmd_get_multi_worst_time(LocalSetting) ->
    get(LocalSetting, stats_cmd_get_multi_worst_time).

set_cmd_get_multi_worst_time(LocalSetting, MSec) ->
    case cmd_get_multi_worst_time(LocalSetting) < MSec of
        true ->
            ok = mio_local_store:set(LocalSetting, stats_cmd_get_multi_worst_time, MSec);
        _ ->
            ok
    end.

init_cmd_get_total_time(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_cmd_get_total_time, 0).

cmd_get_total_time(LocalSetting) ->
    get(LocalSetting, stats_cmd_get_total_time).

inc_cmd_get_total_time(LocalSetting, D) ->
    ok = mio_local_store:set(LocalSetting, stats_cmd_get_total_time, get(LocalSetting, stats_cmd_get_total_time) + D).

cmd_get_avg_time(LocalSetting) ->
    case mio_stats:cmd_get(LocalSetting) of
        0 ->
            0;
        N ->
            mio_stats:cmd_get_total_time(LocalSetting) / N
    end.

init_cmd_get_worst_time(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_cmd_get_worst_time, 0).

cmd_get_worst_time(LocalSetting) ->
    get(LocalSetting, stats_cmd_get_worst_time).

set_cmd_get_worst_time(LocalSetting, MSec) ->
    case cmd_get_worst_time(LocalSetting) < MSec of
        true ->
            ok = mio_local_store:set(LocalSetting, stats_cmd_get_worst_time, MSec);
        _ ->
            ok
    end.
