%%%-------------------------------------------------------------------
%%% File    : mio_lock.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : wrapper of global:set_lock for Mio
%%%
%%% Created : 21 Oct 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_lock).

%% API
-export([lock/1, unlock/1]).

%%====================================================================
%% API
%%====================================================================

%%--------------------------------------------------------------------
%% Function: lock/1
%% Description: lock nodes
%%--------------------------------------------------------------------
lock(Nodes) ->
    set_lock(Nodes, []).

%%--------------------------------------------------------------------
%% Function: unlock/1
%% Description: unlock nodes
%%--------------------------------------------------------------------
unlock(Nodes) ->
    del_lock(Nodes).

%%====================================================================
%% Internal functions
%%====================================================================
set_lock([], _LockedNodes) ->
    true;
set_lock([NodeToLock | More], LockedNodes) ->
    case global:set_lock({NodeToLock, self()}, [node() | nodes()], 0) of
        false ->
            del_lock(LockedNodes);
        true ->
            set_lock(More, [NodeToLock | LockedNodes])
    end.

del_lock([]) ->
    false;
del_lock([Node | More]) ->
    global:del_lock({Node, self()}),
    del_lock(More).
