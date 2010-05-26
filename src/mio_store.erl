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
%%% File    : mio_store.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : store
%%%
%%% Created : 4 Mar 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_store).

-record(store, {capacity, ets}).
-include_lib("eunit/include/eunit.hrl").

%% API
-export([new/1, set/3, get/2, get_range/4, remove/2, is_full/1, take_smallest/1, take_largest/1,
         capacity/1, is_empty/1, largest/1, smallest/1, keys/1]).

%%====================================================================
%% API
%%====================================================================

%%--------------------------------------------------------------------
%% Function: new/1
%% Description: make a store
%%--------------------------------------------------------------------
new(Capacity) ->
    Ets = ets:new(store, [private, ordered_set]),
    #store{capacity=Capacity, ets=Ets}.

%%--------------------------------------------------------------------
%% Function: keys/1
%% Description: returns list of keys.
%%--------------------------------------------------------------------
keys(Store) ->
    ets:foldl(fun(E, Accum) ->
                      [E | Accum]
              end,
              [],
              Store#store.ets).

%%--------------------------------------------------------------------
%% Function: set/3
%% Description: set (key, value)
%%--------------------------------------------------------------------
set(Key, Value, Store) ->
    case is_full(Store) of
        true ->
            case ets:insert_new(Store#store.ets, {Key, Value}) of
                false ->
                    ets:insert(Store#store.ets, {Key, Value}),
                    {full, Store};
                true ->
                    {overflow, Store}
            end;
        _ ->
            ets:insert(Store#store.ets, {Key, Value}),
            case is_full(Store) of
                true ->
                    {full, Store};
                _ ->
                    Store
            end
    end.

%%--------------------------------------------------------------------
%% Function: get/2
%% Description: get value by key
%%--------------------------------------------------------------------
get(Key, Store) ->
    case ets:lookup(Store#store.ets, Key) of
        [] ->
            {error, not_found};
        [{Key, Value}] ->
            {ok, Value}
    end.

get_range(KeyA, KeyB, Limit, Store) when KeyA =< KeyB ->
    case ets:lookup(Store#store.ets, KeyA) of
        [] ->
            lists:reverse(get_range_rec(KeyA, KeyB, 0, Limit, [], Store));
        [{KeyA, Value}] ->
            lists:reverse(get_range_rec(KeyA, KeyB, 1, Limit, [{KeyA, Value}], Store))
    end;
get_range(KeyA, KeyB, Limit, Store) when KeyA > KeyB ->
    case ets:lookup(Store#store.ets, KeyA) of
        [] ->
            lists:reverse(get_range_rec(KeyA, KeyB, 0, Limit, [], Store));
        [{KeyA, Value}] ->
            lists:reverse(get_range_rec(KeyA, KeyB, 1, Limit, [{KeyA, Value}], Store))
    end.

get_range_rec(_StartKey, _EndKey, Count, Limit, AccumValues, _Store) when Count =:= Limit ->
    AccumValues;
get_range_rec(StartKey, EndKey, Count, Limit, AccumValues, Store) when StartKey =< EndKey ->
    case ets:next(Store#store.ets, StartKey) of
        '$end_of_table' ->
            AccumValues;
        NextKey ->
            case NextKey > EndKey of
                true ->
                    AccumValues;
                _ ->
                    {ok, Value} = get(NextKey, Store),
                    get_range_rec(NextKey, EndKey, Count + 1, Limit, [{NextKey, Value} | AccumValues], Store)
            end
    end;
get_range_rec(StartKey, EndKey, Count, Limit, AccumValues, Store) when StartKey > EndKey ->
    case ets:prev(Store#store.ets, StartKey) of
        '$end_of_table' ->
            AccumValues;
        NextKey ->
            case NextKey < EndKey of
                true ->
                    AccumValues;
                _ ->
                    {ok, Value} = get(NextKey, Store),
                    get_range_rec(NextKey, EndKey, Count + 1, Limit, [{NextKey, Value} | AccumValues], Store)
            end
    end.

%%--------------------------------------------------------------------
%% Function: take_smallest/1
%% Description: get value by smallest key and remove it.
%%--------------------------------------------------------------------
take_smallest(Store) ->
    case ets:first(Store#store.ets) of
        '$end_of_table' ->
            none;
        Key ->
            case ets:lookup(Store#store.ets, Key) of
                [{Key, Value}] ->
                    true = ets:delete(Store#store.ets, Key),
                    {Key, Value, Store}
            end
    end.

%%--------------------------------------------------------------------
%% Function: take_largest/1
%% Description: get value by largest key and remove it.
%%--------------------------------------------------------------------
take_largest(Store) ->
    case ets:last(Store#store.ets) of
        '$end_of_table' ->
            none;
        Key ->
            case ets:lookup(Store#store.ets, Key) of
                [{Key, Value}] ->
                    true = ets:delete(Store#store.ets, Key),
                    {Key, Value, Store}
            end
    end.

%%--------------------------------------------------------------------
%% Function: largest/1
%% Description: get value by largest key.
%%--------------------------------------------------------------------
largest(Store) ->
    case ets:last(Store#store.ets) of
        '$end_of_table' ->
            none;
        Key ->
            case ets:lookup(Store#store.ets, Key) of
                [{Key, Value}] ->
                    {Key, Value}
            end
    end.

%%--------------------------------------------------------------------
%% Function: smallest/1
%% Description: get value by smallest key.
%%--------------------------------------------------------------------
smallest(Store) ->
    case ets:first(Store#store.ets) of
        '$end_of_table' ->
            none;
        Key ->
            case ets:lookup(Store#store.ets, Key) of
                [{Key, Value}] ->
                    {Key, Value}
            end
    end.

%%--------------------------------------------------------------------
%% Function: remove/2
%% Description: remove value by key
%%--------------------------------------------------------------------
remove(Key, Store) ->
    true = ets:delete(Store#store.ets, Key),
    Store.

%%--------------------------------------------------------------------
%% Function: is_full/1
%% Description: returns is store full?
%%--------------------------------------------------------------------
is_full(Store) ->
    Store#store.capacity =:= ets:info(Store#store.ets, size).

%%--------------------------------------------------------------------
%% Function: capacity
%% Description: returns capacity
%%--------------------------------------------------------------------
capacity(Store) ->
    Store#store.capacity.

%%--------------------------------------------------------------------
%% Function: is_empty
%% Description: returns store is empty
%%--------------------------------------------------------------------
is_empty(Store) ->
    0 =:= ets:info(Store#store.ets, size).

%%====================================================================
%% Internal functions
%%====================================================================
