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
-export([init_uptime/1, uptime/1,
         init_total_items/1, total_items/1, inc_total_items/1,
         init_cmd_get/1, cmd_get/1, inc_cmd_get/1]).
-include_lib("eunit/include/eunit.hrl").
-include("mio.hrl").

init_uptime(LocalSetting) ->
    {_, NowSec, _} = now(),
    ok = mio_local_store:set(LocalSetting, stats_uptime, NowSec).

uptime(LocalSetting) ->
    {ok, StartSec} = mio_local_store:get(LocalSetting, stats_uptime),
    {_, NowSec, _} = now(),
    NowSec - StartSec.

init_total_items(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_total_items, 0).

total_items(LocalSetting) ->
    {ok, Items} = mio_local_store:get(LocalSetting, stats_total_items),
    Items.

inc_total_items(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_total_items, total_items(LocalSetting) + 1).

init_cmd_get(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_cmd_get, 0).

cmd_get(LocalSetting) ->
    {ok, Items} = mio_local_store:get(LocalSetting, stats_cmd_get),
    Items.

inc_cmd_get(LocalSetting) ->
    ok = mio_local_store:set(LocalSetting, stats_cmd_get, cmd_get(LocalSetting) + 1).
