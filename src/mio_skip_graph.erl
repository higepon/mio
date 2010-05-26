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
%%% File    : mio_skip_graph.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : bucket
%%%
%%% Created : 12 Apr 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_skip_graph).
-include("mio.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([search_op/2, search_op/3,
         range_search_asc_op/4,
         range_search_desc_op/4,
         get_key_op/1,
         insert_op/3,
         dump_op/1


        ]).

%% Exported for handle_call
-export([search_op_call/5,
         insert_op_call/4,
         dump_op_call/1,
         get_key/1

        ]).
%%--------------------------------------------------------------------
%%  accessors
%%--------------------------------------------------------------------
get_key_op(Bucket) ->
    gen_server:call(Bucket, skip_graph_get_key_op).

%%--------------------------------------------------------------------
%%  Dump operation for Debug.
%%--------------------------------------------------------------------
dump_op(StartBucket) ->
    gen_server:call(StartBucket, skip_graph_dump_op).

%%--------------------------------------------------------------------
%%  Insertion operation
%%--------------------------------------------------------------------
insert_op(Introducer, Key, Value) ->
    gen_server:call(Introducer, {skip_graph_insert_op, Key, Value}).



insert_op_call(From, Self, Key, Value) ->
    StartLevel = [],
    Bucket = gen_server:call(Self, {skip_graph_search_op, Key, StartLevel}, infinity),
%%    ?debugFmt("<Bucket=~p: SearchKey=~p ~p~n>Self=~p~n~n", [Bucket, Key, get_key_op(Bucket), get_key_op(Self)]),
    Ret = mio_bucket:insert_op(Bucket, Key, Value),
    gen_server:reply(From, Ret).

%%--------------------------------------------------------------------
%%  Search operation
%%
%%  Condition : SearchKey > NodeKey && right exists
%%    Level ge 1 : to the right on the same level
%%    Level 0    : to the right on the same level
%%
%%  Condition : SearchKey > NodeKey && right not exists
%%    Level ge 1 : to the lower level
%%    Level 0    : not found (On mio this case can't be happen, since it handles ?MAX_KEY)
%%
%%  Condition : SearchKey = NodeKey
%%    Level ge 1 : Search on the bucket and return
%%    Level 0    : Search on the bucket and return
%%
%%  Condition : SearchKey < NodeKey && left exist && SearchKey <= LeftKey
%%    Level ge 1 : to the left on the same level
%%    Level 0    : to the left on the same level
%%
%%  Condition : SearchKey < NodeKey && left exist && SearchKey > LeftKey
%%    Level ge 1 : to the lower level
%%    Level 0    : Search on the bucket and return
%%
%%  Condition : SearchKey < NodeKey && left not exist
%%    Level ge 1 : to the lower level
%%    Level 0    : not_found  (On mio this case can't be happen, since it handles ?MIX_KEY)

%%--------------------------------------------------------------------
search_op(StartBucket, SearchKey) ->
    search_op(StartBucket, SearchKey, []).
search_op(StartBucket, SearchKey, StartLevel) ->
    dynomite_prof:start_prof(search_get),
    Bucket = search_bucket_op(StartBucket, SearchKey, StartLevel),
    dynomite_prof:stop_prof(search_get),
    dynomite_prof:start_prof(store_get),
    Ret = mio_bucket:get_op(Bucket, SearchKey),
    dynomite_prof:stop_prof(store_get),
    Ret.

search_bucket_op(StartBucket, SearchKey) ->
    search_bucket_op(StartBucket, SearchKey, []).
search_bucket_op(StartBucket, SearchKey, StartLevel) ->
    gen_server:call(StartBucket, {skip_graph_search_op, SearchKey, StartLevel}, infinity).

range_search_asc_op(StartBucket, Key1, Key2, Limit) when Key1 =< Key2 ->
    Bucket = search_bucket_op(StartBucket, Key1),
    mio_bucket:get_range_values_op(Bucket, Key1, Key2, Limit);
range_search_asc_op(_StartBucket, _Key1, _Key2, _Limit) ->
    [].

range_search_desc_op(StartBucket, Key1, Key2, Limit) when Key1 =< Key2 ->
    Bucket = search_bucket_op(StartBucket, Key2),
    mio_bucket:get_range_values_op(Bucket, Key2, Key1, Limit);
range_search_desc_op(_StartBucket, _Key1, _Key2, _Limit) ->
    [].

get_range(State) ->
    {{State#node.min_key, State#node.encompass_min}, {State#node.max_key, State#node.encompass_max}}.

in_range(Key, Min, MinEncompass, Max, MaxEncompass) ->
    ((MinEncompass andalso Min =< Key) orelse (not MinEncompass andalso Min < Key))
      andalso
    ((MaxEncompass andalso Key =< Max) orelse (not MaxEncompass andalso Key < Max)).

search_to_right(_From, _State, Self, _SearchKey, Level) when Level < 0 ->
    Self;
search_to_right(From, State, Self, SearchKey, Level) ->
    %%   ?debugFmt("Right State=~p SearchKey=~p Level=~p~n", [State, SearchKey, Level]),
    case neighbor_node(State, right, Level) of
        [] ->
            search_to_right(From, State, Self, SearchKey, Level - 1);
        Right ->
            {{RMin, RMinEncompass}, {RMax, RMaxEncompass}} = mio_bucket:get_range_op(Right),
            case RMax =< SearchKey orelse in_range(SearchKey, RMin, RMinEncompass, RMax, RMaxEncompass) of
                true ->
                    search_bucket_op(Right, SearchKey, Level);
                _ ->
                    search_to_right(From, State, Self, SearchKey, Level - 1)
            end
    end.

search_to_left(_From, _State, Self, _SearchKey, Level) when Level < 0 ->
    Self;
search_to_left(From, State, Self, SearchKey, Level) ->
    %%   ?debugFmt("Left State=~p SearchKey=~p Level=~p~n", [State, SearchKey, Level]),
    case neighbor_node(State, left, Level) of
        [] ->
            search_to_left(From, State, Self, SearchKey, Level - 1);
        Left ->
            {{LMin, LMinEncompass}, {LMax, LMaxEncompass}} = mio_bucket:get_range_op(Left),
            case LMax >= SearchKey orelse in_range(SearchKey, LMin, LMinEncompass, LMax, LMaxEncompass) of
                true ->
                    search_bucket_op(Left, SearchKey, Level);
                _ ->
                    search_to_left(From, State, Self, SearchKey, Level - 1)
            end
    end.

search_op_call(From, State, Self, SearchKey, Level) ->
    dynomite_prof:start_prof(in_range),
    dynomite_prof:start_prof(in_range2),
    {{Min, MinEncompass}, {Max, MaxEncompass}} = get_range(State),
    case in_range(SearchKey, Min, MinEncompass, Max, MaxEncompass) of
        %% Key may be found in Self.
        true ->
            dynomite_prof:stop_prof(in_range),
            gen_server:reply(From, Self);
        _ ->
            dynomite_prof:stop_prof(in_range2),
            StartLevel = start_level(State, Level),
            case (MaxEncompass andalso Max < SearchKey) orelse (not MaxEncompass andalso Max =< SearchKey) of
                true ->
                    gen_server:reply(From, search_to_right(From, State, Self, SearchKey, StartLevel));
                _ ->
                    gen_server:reply(From, search_to_left(From, State, Self, SearchKey, StartLevel))
            end
    end.

dump_op_call(State) ->
    Key = get_key(State),
    ?INFOF("===========================================~nBucket: ~p<~p>~n", [Key, State#node.type]),
    lists:foreach(fun(K) ->
                          ?INFOF("    ~p~n", [K])
                  end, mio_store:keys(State#node.store)),
    case neighbor_node(State, right, 0) of
        [] -> [];
        RightBucket ->
            dump_op(RightBucket)
    end.


%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% My key is max_range.
get_key(State) ->
    {State#node.max_key, State#node.encompass_max}.

start_level(State, []) ->
    length(State#node.right) - 1; %% Level is 0 origin
start_level(_State, Level) ->
    Level.

node_on_level(Nodes, Level) ->
    case Nodes of
        [] -> [];
        _ ->  lists:nth(Level + 1, Nodes) %% Erlang array is 1 origin.
    end.

neighbor_node(State, Direction, Level) ->
    case Direction of
        right ->
            node_on_level(State#node.right, Level);
        left ->
            node_on_level(State#node.left, Level)
    end.
