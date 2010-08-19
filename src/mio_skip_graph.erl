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
-include("mio_path_stats.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([search_op/2, search_op/3, delete_op/2,
         range_search_asc_op/4,
         range_search_desc_op/4,
         get_key_op/1,
         insert_op/3, insert_op/4,
         buddy_op/4,
         dump_op/1,
         link_right_op/3, link_left_op/3,
         link_two_nodes/3,
         link_three_nodes/4,
         link_on_level_ge1/2,
         get_local_buckets/1
        ]).

%% Exported for handle_call
-export([search_op_call/6,
         insert_op_call/5,
         buddy_op_call/6,
         dump_op_call/1,
         delete_op_call/4,
         get_key/1
        ]).

get_local_buckets_left([]) ->
    [];
get_local_buckets_left(Bucket) ->
    case mio_util:is_local_process(Bucket) of
        true ->
            [Bucket | get_local_buckets_left(mio_bucket:get_left_op(Bucket))];
        false ->
            get_local_buckets_left(mio_bucket:get_left_op(Bucket))
    end.

get_local_buckets_right([]) ->
    [];
get_local_buckets_right(Bucket) ->
    case mio_util:is_local_process(Bucket) of
        true ->
            [Bucket | get_local_buckets_right(mio_bucket:get_right_op(Bucket))];
        false ->
            get_local_buckets_right(mio_bucket:get_right_op(Bucket))
    end.

get_local_buckets(Bucket) ->
    Left = lists:reverse(get_local_buckets_left(Bucket)),
    Right = get_local_buckets_right(mio_bucket:get_right_op(Bucket)),
    Left ++ Right.

%%--------------------------------------------------------------------
%%  accessors
%%--------------------------------------------------------------------
get_key_op(Bucket) ->
    gen_server:call(Bucket, skip_graph_get_key_op).

%%--------------------------------------------------------------------
%%  Dump operation for Debug.
%%--------------------------------------------------------------------
get_most_left(Bucket) ->
    case mio_bucket:get_left_op(Bucket) of
        [] ->
             Bucket;
        Left ->
            get_most_left(Left)
    end.

dump_op(StartBucket) ->
    MostLeft = get_most_left(StartBucket),
    gen_server:call(MostLeft, skip_graph_dump_op).

dump_op1(StartBucket) ->
    gen_server:call(StartBucket, skip_graph_dump_op).


dump_op_call(State) ->
    Key = get_key(State),
    ?INFOF("===========================================~nBucket: ~p<~p>:~p ~p~n", [Key, State#node.type, mio_bucket:get_range(State), State#node.membership_vector]),
    %% lists:foreach(fun(K) ->
    %%                       ?INFOF("    ~p~n", [K])
    %%               end, mio_store:keys(State#node.store)),
    case neighbor_node(State, right, 0) of
        [] -> [];
        RightBucket ->
            dump_op1(RightBucket)
    end.

%%--------------------------------------------------------------------
%%  Insertion operation
%%--------------------------------------------------------------------
insert_op(Introducer, Key, Value) ->
    gen_server:call(Introducer, {skip_graph_insert_op, Key, Value, ?NEVER_EXPIRE}).

insert_op(Introducer, Key, Value, ExpirationTime) ->
    gen_server:call(Introducer, {skip_graph_insert_op, Key, Value, ExpirationTime}).

insert_op_call(From, Self, Key, Value, ExpirationTime) ->
    StartLevel = [],
    Bucket = search_bucket_op(Self, Key, StartLevel),
    Ret = mio_bucket:insert_op(Bucket, Key, Value, ExpirationTime),
    gen_server:reply(From, Ret).

%%--------------------------------------------------------------------
%%  Delete operation
%%--------------------------------------------------------------------
delete_op(Introducer, Key) ->
    gen_server:call(Introducer, {skip_graph_delete_op, Key}).

delete_op_call(From, State, Self, Key) ->
    Bucket = search_bucket_op(Self, Key),
    case mio_bucket:delete_op(Bucket, Key) of
        {error, Reason} ->
            gen_server:reply(From, {error, Reason});
        {ok, BucketsToDelete} ->
            lists:foreach(fun (B) -> delete_loop(B, length(State#node.membership_vector)) end, BucketsToDelete),
            gen_server:reply(From, {ok, BucketsToDelete})
    end.


delete_loop(_Self, Level) when Level < 0 ->
    [];
delete_loop(Self, Level) ->
    RightBucket = mio_bucket:get_right_op(Self, Level),
    LeftBucket = mio_bucket:get_left_op(Self, Level),
    link_two_nodes(LeftBucket, RightBucket, Level),
    delete_loop(Self, Level - 1).

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
%%
%%--------------------------------------------------------------------
search_op(StartBucket, SearchKey) ->
    search_op(StartBucket, SearchKey, []).
search_op(StartBucket, SearchKey, StartLevel) ->
    ?MIO_PATH_STATS_PUSH2(SearchKey, start),
    dynomite_prof:start_prof(search_bucket_op),
    Ret = search_direct_op(StartBucket, SearchKey, StartLevel),
    dynomite_prof:stop_prof(search_bucket_op),
    ?MIO_PATH_STATS_PUSH2(SearchKey, {result, Ret}),
    ?MIO_PATH_STATS_SHOW(SearchKey),
    Ret.

%% search_bucket_op returns a bucket which may have the SearchKey.
search_bucket_op(StartBucket, SearchKey) ->
    search_bucket_op(StartBucket, SearchKey, []).
search_bucket_op(StartBucket, SearchKey, StartLevel) ->
    gen_server:call(StartBucket, {skip_graph_search_op, SearchKey, StartLevel}, infinity).

%% search_direct_op finds a bucket which may have the SearchKey, and returns search result.
%% This function exists for performance reason.
%% On multiple nodes, we should reduce gen_server:call to remote node.
search_direct_op(StartBucket, SearchKey, StartLevel) ->
    gen_server:call(StartBucket, {skip_graph_search_direct_op, SearchKey, StartLevel}, infinity).

search_op_call(From, State, Self, SearchKey, Level, IsDirectReturn) ->
    {{Min, MinEncompass}, {Max, MaxEncompass}} = mio_bucket:get_range(State),
    case in_range(SearchKey, Min, MinEncompass, Max, MaxEncompass) of
        %% Key may be found in Self.
        true ->
            ?MIO_PATH_STATS_PUSH2(SearchKey, {Self, mio_bucket:get_range(State), bucket_found}),
            if IsDirectReturn ->
                    gen_server:reply(From, search_on_bucket(SearchKey, State));
               true ->
                    gen_server:reply(From, Self)
            end;
        _ ->
            StartLevel = start_level(State, Level),
            case (MaxEncompass andalso Max < SearchKey) orelse (not MaxEncompass andalso Max =< SearchKey) of
                true ->
                    ?MIO_PATH_STATS_PUSH2(SearchKey, "    ===> right "),
                    Ret = search_to_right(From, State, Self, SearchKey, StartLevel, IsDirectReturn),
                    gen_server:reply(From, Ret);
                _ ->
                    ?MIO_PATH_STATS_PUSH2(SearchKey, "    <=== left "),
                    Ret = search_to_left(From, State, Self, SearchKey, StartLevel, IsDirectReturn),
                    gen_server:reply(From, Ret)
            end
    end.

search_on_bucket(Key, State) ->
    case mio_store:get(Key, State#node.store) of
        {ok, {Value, ExpirationTime}} ->
            {ok, Value, ExpirationTime};
        Other -> Other
    end.

search_to_right(_From, State, Self, SearchKey, Level, IsDirectReturn) when Level < 0 ->
    if IsDirectReturn ->
            search_on_bucket(SearchKey, State);
       true ->
            Self
    end;
search_to_right(From, State, Self, SearchKey, Level, IsDirectReturn) ->
    ?MIO_PATH_STATS_PUSH3({Self, mio_bucket:get_left_op(Self, Level), mio_bucket:get_right_op(Self, Level), mio_bucket:get_range(State)}, SearchKey, Level),
    case neighbor_node(State, right, Level) of
        [] ->
            search_to_right(From, State, Self, SearchKey, Level - 1, IsDirectReturn);
        Right ->
            {{RMin, RMinEncompass}, {RMax, RMaxEncompass}} = mio_bucket:get_range_op(Right),
            case RMax =< SearchKey orelse in_range(SearchKey, RMin, RMinEncompass, RMax, RMaxEncompass) of
                true ->
                    if IsDirectReturn ->
                            search_direct_op(Right, SearchKey, Level);
                       true ->
                            search_bucket_op(Right, SearchKey, Level)
                    end;
                _ ->
                    search_to_right(From, State, Self, SearchKey, Level - 1, IsDirectReturn)
            end
    end.
search_to_left(_From, State, Self, SearchKey, Level, IsDirectReturn) when Level < 0 ->
    if IsDirectReturn ->
            search_on_bucket(SearchKey, State);
       true ->
            Self
    end;
search_to_left(From, State, Self, SearchKey, Level, IsDirectReturn) ->
    ?MIO_PATH_STATS_PUSH3({Self, mio_bucket:get_left_op(Self, Level), mio_bucket:get_right_op(Self, Level), mio_bucket:get_range(State)}, SearchKey, Level),
    case neighbor_node(State, left, Level) of
        [] ->
            search_to_left(From, State, Self, SearchKey, Level - 1, IsDirectReturn);
        Left ->
            {{LMin, LMinEncompass}, {LMax, LMaxEncompass}} = mio_bucket:get_range_op(Left),
            case LMax >= SearchKey orelse in_range(SearchKey, LMin, LMinEncompass, LMax, LMaxEncompass) of
                true ->
                    if IsDirectReturn ->
                            search_direct_op(Left, SearchKey, Level);
                       true ->
                            search_bucket_op(Left, SearchKey, Level)
                    end;
                _ ->
                    search_to_left(From, State, Self, SearchKey, Level - 1, IsDirectReturn)
            end
    end.

%%--------------------------------------------------------------------
%%  Range search operation
%%--------------------------------------------------------------------
range_search_asc_op(StartBucket, Key1, Key2, Limit) when Key1 =< Key2 ->
    Bucket = search_bucket_op(StartBucket, Key1),
    range_search_asc_rec(Bucket, Key1, Key2, [], 0, Limit);
range_search_asc_op(_StartBucket, _Key1, _Key2, _Limit) ->
    [].

range_search_asc_rec([], _Key1, _Key2, Accum, _Count, _Limit) ->
    Accum;
range_search_asc_rec(_Bucket, _Key1, _Key2, Accum, Count, Limit) when Count =:= Limit ->
    Accum;
range_search_asc_rec(Bucket, Key1, Key2, Accum, Count, Limit) ->
    {{Min, _MinEncompass}, _} = mio_bucket:get_range_op(Bucket),
    case Key2 >= Min of
        true ->
            KeyValues = mio_bucket:get_range_values_op(Bucket, Key1, Key2, Limit - Count),
            RightBucket = mio_bucket:get_right_op(Bucket),
            range_search_asc_rec(RightBucket, Key1, Key2, Accum ++ KeyValues, Count + length(KeyValues), Limit);
        _ ->
            Accum
    end.

range_search_desc_op(StartBucket, Key1, Key2, Limit) when Key1 =< Key2 ->
    Bucket = search_bucket_op(StartBucket, Key2),
    range_search_desc_rec(Bucket, Key1, Key2, [], 0, Limit);
range_search_desc_op(_StartBucket, _Key1, _Key2, _Limit) ->
    [].

range_search_desc_rec([], _Key1, _Key2, Accum, _Count, _Limit) ->
    Accum;
range_search_desc_rec(_Bucket, _Key1, _Key2, Accum, Count, Limit) when Count =:= Limit ->
    Accum;
range_search_desc_rec(Bucket, Key1, Key2, Accum, Count, Limit) ->
    {_, {Max, _MaxEncompass}} = mio_bucket:get_range_op(Bucket),
    case Key1 =< Max of
        true ->
            KeyValues = mio_bucket:get_range_values_op(Bucket, Key2, Key1, Limit - Count),
            LeftBucket = mio_bucket:get_left_op(Bucket),
            range_search_desc_rec(LeftBucket, Key1, Key2, Accum ++ KeyValues, Count + length(KeyValues), Limit);
        _ ->
            Accum
    end.

%%--------------------------------------------------------------------
%%  link operation
%%--------------------------------------------------------------------
is_valid_order(_Left, []) ->
    true;
is_valid_order([], _Right)->
    true;
is_valid_order(Left, Right) ->
    {_, {LMax, _}} = mio_bucket:get_range_op(Left),
    {{RMin, _}, _} = mio_bucket:get_range_op(Right),
    case {LMax, RMin} of
        {[], _} -> true;
        {_, []} -> true;
        _ ->
            LMax =< RMin
    end.

link_right_op([], _Level, _Right) ->
    ok;
link_right_op(Node, Level, Right) ->
    gen_server:call(Node, {link_right_op, Level, Right}).

link_left_op([], _Level, _Left) ->
    ok;
link_left_op(Node, Level, Left) ->
    gen_server:call(Node, {link_left_op, Level, Left}).

%%--------------------------------------------------------------------
%%  Buddy operation
%%--------------------------------------------------------------------
%%--------------------------------------------------------------------
%%  buddy operation
%%--------------------------------------------------------------------
buddy_op(Node, MembershipVector, Direction, Level) ->
    gen_server:call(Node, {skip_graph_buddy_op, MembershipVector, Direction, Level}).

buddy_op_call(From, State, Self, MembershipVector, Direction, Level) ->
    IsSameMV = mio_mvector:eq(Level, MembershipVector, State#node.membership_vector),
%    ?debugFmt("buddy_op to ~p ~p MV=~p ", [Direction, {Self, mio_bucket:get_range_op(Self)}, {MembershipVector, State#node.membership_vector}]),
    if
        IsSameMV ->
            MyKey = mio_bucket:my_key(State),
            ReverseDirection = reverse_direction(Direction),
            MyNeighbor = neighbor_node(State, ReverseDirection, Level),
            gen_server:reply(From, {ok, Self, MyKey, MyNeighbor});
        true ->
            case neighbor_node(State, Direction, Level - 1) of %% N.B. should be on LowerLevel
                [] ->
                    gen_server:reply(From, not_found);
                NeighborNode ->
                    gen_server:reply(From, buddy_op(NeighborNode, MembershipVector, Direction, Level))
            end
    end.

%% buddy_op_proxy([], [], _MyMV, _Level) ->
%%     not_found;
buddy_op_proxy(LeftOnLower, [], MyMV, Level) ->
    case buddy_op(LeftOnLower, MyMV, left, Level) of
        not_found ->
            not_found;
        {ok, Buddy, BuddyKey, BuddyRight} ->
            {ok, left, Buddy, BuddyKey, BuddyRight}
    end;
buddy_op_proxy([], RightOnLower, MyMV, Level) ->
    case buddy_op(RightOnLower, MyMV, right, Level) of
        not_found ->
            not_found;
        {ok, Buddy, BuddyKey, BuddyLeft} ->
            {ok, right, Buddy, BuddyKey, BuddyLeft}
    end;
buddy_op_proxy(LeftOnLower, RightOnLower, MyMV, Level) ->
    case buddy_op_proxy(LeftOnLower, [], MyMV, Level) of
        not_found ->
            buddy_op_proxy([], RightOnLower, MyMV, Level);
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
%% link on Level >= 1
link_on_level_ge1(Self, State) ->
    MaxLevel = length(State#node.membership_vector),
    link_on_level_ge1(Self, 1, MaxLevel).

%% Link on all levels done.
link_on_level_ge1(_Self, Level, MaxLevel) when Level > MaxLevel ->
    [];
%% buddy node has same membership_vector on this level.
%% Insert Sample
%%
%%   Node = [NodeName:MemberShip on Level]
%%
%%   Start State
%%     <Level - 1>: [A:m] <-> [B:n] <-> [NodeToInsert:m] <-> [C:n] <-> [D:m] <-> [E:n] <-> [F:m]
%%     <Level>    : [A:m] <-> [D:m] <-> [F:m]
%%
%%   Insert
%%     1. Search node to the right side that has membership_vector m start from NodeToInsert.
%%     2. [D:m] found.
%%     3. Link and insert [NodeToInsert:m]
%%     4. Go up to next Level = Level + 1
%%
%%   End State
%%     <Level - 1>: [A:m] <-> [B:n] <-> [NodeToInsert:m] <-> [C:n] <-> [D:m] <-> [E:n] <-> [F:m]
%%     <Level>    : [A:m] <-> [NodeToInsert:m] <-> [D:m] <-> [F:m]
%%
link_on_level_ge1(Self, Level, MaxLevel) ->
    {_MyKey, _MyValue, MyMV, MyLeft, MyRight} = gen_server:call(Self, get_op),
    LeftOnLower = node_on_level(MyLeft, Level - 1),
    RightOnLower = node_on_level(MyRight, Level - 1),
%    ?debugFmt("LeftOnLower=~p Self=~p RightOnLower=~p", [{LeftOnLower, mio_bucket:get_range_op(LeftOnLower)}, {Self, mio_bucket:get_range_op(Self)}, {RightOnLower, mio_bucket:get_range_op(RightOnLower)}]),
    case buddy_op_proxy(LeftOnLower, RightOnLower, MyMV, Level) of
        not_found ->
            %% We have no buddy on this level.
            %% On higher Level, we have no buddy also.
            %% So we've done.
            [];
        %% [Buddy] <-> [NodeToInsert] <-> [BuddyRight]
        {ok, left, Buddy, _BuddyKey, BuddyRight} ->
            case is_valid_order(Buddy, Self) andalso is_valid_order(Self, BuddyRight) of
                true ->
                    do_link_level_ge1(Self, Buddy, BuddyRight, Level, MaxLevel, left);
                _ ->
                    throw({"skip graph is broken ~p ~p ~p ~p on Level ~p", [{is_valid_order(Buddy, Self), is_valid_order(Self, BuddyRight)}, mio_bucket:get_range_op(Buddy), mio_bucket:get_range_op(Self), mio_bucket:get_range_op(BuddyRight), Level]})
            end;
        %% [BuddyLeft] <-> [NodeToInsert] <-> [Buddy]
        {ok, right, Buddy, _BuddyKey, BuddyLeft} ->
            case is_valid_order(BuddyLeft, Self) andalso is_valid_order(Self, Buddy) of
                true ->
                    do_link_level_ge1(Self, Buddy, BuddyLeft, Level, MaxLevel, right);
                _ ->
                    throw({"skip graph is broken ~p ~p ~p ~p on Level ~p", [{is_valid_order(Buddy, Self), is_valid_order(Self, BuddyLeft)}, mio_bucket:get_range_op(Buddy), mio_bucket:get_range_op(Self), mio_bucket:get_range_op(BuddyLeft), Level]})
            end
    end.

do_link_level_ge1(Self, Buddy, BuddyNeighbor, Level, MaxLevel, Direction) ->
    case Direction of
        right ->
            link_three_nodes(BuddyNeighbor, Self, Buddy, Level);
        left ->
            link_three_nodes(Buddy, Self, BuddyNeighbor, Level)
    end,
    %% Go up to next Level.
    link_on_level_ge1(Self, Level + 1, MaxLevel).

in_range(Key, Min, MinEncompass, Max, MaxEncompass) ->
    ((MinEncompass andalso Min =< Key) orelse (not MinEncompass andalso Min < Key))
      andalso
    ((MaxEncompass andalso Key =< Max) orelse (not MaxEncompass andalso Key < Max)).

%% My key is max_range.
get_key(State) ->
    {State#node.max_key, State#node.encompass_max}.

start_level(State, []) ->
    length(State#node.right) - 1; %% Level is 0 origin
start_level(_State, Level) ->
    Level.

%% coverage says this is not necessary
%% node_on_level(Nodes, Level) ->
%%     case Nodes of
%%         [] -> [];
%%         _ ->  lists:nth(Level + 1, Nodes) %% Erlang array is 1 origin.
%%     end.
node_on_level(Nodes, Level) ->
    lists:nth(Level + 1, Nodes). %% Erlang array is 1 origin.

neighbor_node(State, Direction, Level) ->
    case Direction of
        right ->
            node_on_level(State#node.right, Level);
        left ->
            node_on_level(State#node.left, Level)
    end.

reverse_direction(Direction) ->
    case Direction of
        right ->
             left;
        left ->
            right
    end.

link_two_nodes(LeftNode, RightNode, Level) ->
    link_right_op(LeftNode, Level, RightNode),
    link_left_op(RightNode, Level, LeftNode).

link_three_nodes(LeftNode, CenterNode, RightNode, Level) ->
    %% [Left] -> [Center]  [Right]
    link_right_op(LeftNode, Level, CenterNode),

    %% [Left]    [Center] <- [Right]
    link_left_op(RightNode, Level, CenterNode),

    %% [Left] <- [Center]    [Right]
    link_left_op(CenterNode, Level, LeftNode),

    %% [Left]    [Center] -> [Right]
    link_right_op(CenterNode, Level, RightNode).
