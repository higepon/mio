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
%%% File    : mio_util.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : Some utilities for mio.
%%%
%%% Created : 17 Nov 2009 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_util).

%% API
-export([%% random_sleep/1, 
         lists_set_nth/3,%%  do_times_with_index/3,
         for_better_coverage/2, for_better_coverage/3,
%%          do_workers/2, do_workers/3,
         do_times/2, do_times/3, is_local_process/1]).
-include_lib("eunit/include/eunit.hrl").
-include("mio.hrl").


%%====================================================================
%% API
%%====================================================================
for_better_coverage(Mod, Pid) ->
    for_better_coverage(Mod, Pid, false).
for_better_coverage(Mod, Pid, Fun) ->
    process_flag(trap_exit, true),
    spawn_link(fun() ->
                       c:l(Mod),          %% Moduel:code_change/3
                       cover:compile(
                         Mod,
                         [{i, "include"}
                         ]),
                       sys:suspend(Pid),
                       sys:change_code(Pid, Mod, foo, foo),
                       sys:resume(Pid),
                       code:purge(Mod),
                       gen_server:cast(Pid, hoge),    %% Module:handle_cast/2
                       Pid ! hoge,                    %% Module:handle_info/2
                       case Fun of
                           false -> ok;
                           _ ->
                               Fun()
                       end,
                       gen_server:call(Pid, stop_op)  %% Module:terminate/2
               end),
    receive
        {'EXIT', _, normal} -> ok;
        {'EXIT',_ ,{normal,{gen_server,call,[_ ,stop_op]}}} -> ok
    end.


is_local_process([]) ->
    false;
is_local_process(Pid) ->
    node() =:= node(Pid).

%% random_sleep(Times) ->
%%     case (Times rem 10) of
%%         0 -> erase(random_seed);
%%         _ -> ok
%%     end,
%%     case get(random_seed) of
%%         undefined ->
%%             {A1, A2, A3} = now(),
%%             random:seed(A1, A2, A3 + erlang:phash(node(), 100000));
%%         _ -> ok
%%     end,
%%     T = random:uniform(1000) rem 20 + 1,
%%     ?INFOF("HERE sleep ~p msec ~n", [T]),
%%     receive after T -> ok end.

lists_set_nth(Index, Value, List) ->
    lists:append([lists:sublist(List, 1, Index - 1),
                  [Value],
                  lists:sublist(List, Index + 1, length(List))]).

%% do_times_with_index(Start, End, _Fun) when Start > End ->
%%     ok;
%% do_times_with_index(Start, End, Fun) ->
%%     case apply(Fun, [Start]) of
%%         ok ->
%%             do_times_with_index(Start + 1, End, Fun);
%%         Other ->
%%             Other
%%     end.


do_times(N, Fun) ->
    do_times(N, Fun, []).
do_times(0, _Fun, _Args) ->
    ok;
do_times(N, Fun, Args) ->
    ok = apply(Fun, Args),
    do_times(N - 1, Fun, Args).


%% do_workers(N, Fun) ->
%%     do_workers(N, Fun, []).
%% do_workers(N, Fun, Args) ->
%%     do_workers(N, N, Fun, Args).
%% do_workers(Max, 0, _Fun, _Args) ->
%%     wait_workers(Max, done),
%%     ok;
%% do_workers(Max, N, Fun, Args) ->
%%     Self = self(),
%%     spawn_link(fun() ->
%%                   ok = apply(Fun, [Max - N | Args]),
%%                   Self ! done
%%           end),
%%     do_workers(Max, N - 1, Fun, Args).


%% wait_workers(0, _Msg) ->
%%     ok;
%% wait_workers(Concurrency, Msg) ->
%%     receive
%%         Msg -> []
%%     after 1000 * 60 * 5->
%%           io:format("timeout~n")
%%     end,
%%     wait_workers(Concurrency - 1, Msg).
