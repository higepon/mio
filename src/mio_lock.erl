%%%-------------------------------------------------------------------
%%% File    : mio_lock.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : wrapper of global:set_lock for Mio
%%%
%%% Created : 21 Oct 2009 by higepon <higepon@users.sourceforge.jp>
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
    case global:set_lock({NodeToLock, self()}, [node() | nodes()], Retries) of
        false ->
            del_lock(LockedNodes);
        true ->
            set_lock(More, [NodeToLock | LockedNodes], Retries)
    end.

del_lock([]) ->
    false;
del_lock([Node | More]) ->
    global:del_lock({Node, self()}),
    del_lock(More).
