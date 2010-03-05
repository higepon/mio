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
%%% File    : mio_bucket.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : bucket
%%%
%%% Created : 4 Mar 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_bucket).

-record(bucket, {capacity, tree}).

%% API
-export([new/1, set/3, get/2, remove/2]).

%%====================================================================
%% API
%%====================================================================

%%--------------------------------------------------------------------
%% Function: new/1
%% Description: make a bucket
%%--------------------------------------------------------------------
new(Capacity) ->
    #bucket{capacity=Capacity, tree=gb_trees:empty()}.

%%--------------------------------------------------------------------
%% Function: set/3
%% Description: set (key, value)
%%--------------------------------------------------------------------
set(Key, Value, Bucket) ->
    Bucket#bucket{tree=gb_trees:enter(Key, Value, Bucket#bucket.tree)}.

%%--------------------------------------------------------------------
%% Function: get/2
%% Description: get value by key
%%--------------------------------------------------------------------
get(Key, Bucket) ->
    case gb_trees:lookup(Key, Bucket#bucket.tree) of
        none ->
            none;
        {value, Value} ->
            Value
    end.

%%--------------------------------------------------------------------
%% Function: remove/2
%% Description: remove value by key
%%--------------------------------------------------------------------
remove(Key, Bucket) ->
    Bucket#bucket{tree=gb_trees:delete_any(Key, Bucket#bucket.tree)}.

%%====================================================================
%% Internal functions
%%====================================================================
