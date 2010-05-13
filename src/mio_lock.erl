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
%%% File    : mio_lock.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : wrapper of global:set_lock for Mio
%%%
%%% Created : 21 Oct 2009 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_lock).

%% API
-export([lock/1, lock/2, unlock/1]).

%%====================================================================
%% API
%%====================================================================

%%--------------------------------------------------------------------
%% Function: lock/1
%% Description: lock nodes
%%--------------------------------------------------------------------
lock(Nodes) ->
    set_lock(Nodes, []).

lock(Nodes, Retries) ->
    set_lock(Nodes, [], Retries).

%%--------------------------------------------------------------------
%% Function: unlock/1
%% Description: unlock nodes
%%--------------------------------------------------------------------
unlock(Nodes) ->
    del_lock(Nodes).

%%====================================================================
%% Internal functions
%%====================================================================
set_lock(NodesToLock, LockedNodes) ->
    set_lock(NodesToLock, LockedNodes, 0).
set_lock([], _LockedNodes, _Retries) ->
    true;
set_lock([[] | More], LockedNodes, Retries) ->
    set_lock(More, LockedNodes, Retries);
set_lock([NodeToLock | More], LockedNodes, Retries) ->
    dynomite_prof:start_prof(global_lock),
    case global:set_lock({NodeToLock, self()}, [node() | nodes()], Retries) of
        false ->
            dynomite_prof:stop_prof(global_lock),
            del_lock(LockedNodes);
        true ->
            dynomite_prof:stop_prof(global_lock),
            set_lock(More, [NodeToLock | LockedNodes], Retries)
    end.

del_lock([]) ->
    false;
del_lock([Node | More]) ->
    global:del_lock({Node, self()}),
    del_lock(More).
