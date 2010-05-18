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
%%% Created : 9 Mar 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_bucket).
-include("mio.hrl").
-include_lib("eunit/include/eunit.hrl").
-behaviour(gen_server).

%% API
-export([start_link/1,
         get_left_op/1, get_right_op/1,
         get_left_op/2, get_right_op/2,
         insert_op/3, just_insert_op/3,
         get_type_op/1, set_type_op/2,
         is_empty_op/1, is_full_op/1,
         take_largest_op/1,
         get_largest_op/1, get_smallest_op/1,
         insert_op_call/5,
         get_range_op/1, set_range_op/3,
         set_max_key_op/3, set_min_key_op/3,
         %% Skip Graph layer
%%stats_op/2,
get_op/2,
          buddy_op_call/6, get_op_call/2, get_neighbor_op_call/4,
          link_right_op/3, link_left_op/3,
         set_expire_time_op/2, buddy_op/4,
         %% delete_op/2, delete_op/1,
range_search_asc_op/4, range_search_desc_op/4,
         node_on_level/2,
         %% For testability
         set_gen_mvector_op/2
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


%%====================================================================
%%  Skip Graph layer
%%====================================================================
%%
%%  We use max_range value of bucket as representative key.
%%

%%====================================================================
%%  Bucket layer
%%====================================================================
%%
%% Known types
%%   O     : <alone>
%%   C-O   : <c_o_l>-<c_o_r>
%%   C-O-C : <c_o_c_l>-<c_o_c_m>-<c_o_c_r>
%%
%%  C: Closed bucket
%%  O: Open bucket
%%  O*: Open and empty bucket
%%  O$: Nearly closed bucket
%%
%%  Three invariants on bucket group.
%%
%%    (1) O
%%      The system has one open bucket.
%%
%%    (2) C-O
%%      Every closed buket is adjacent to an open bucket.
%%
%%    (3) C-O-C
%%      Every open bucket has aclosed bucket to its left.
%%
%%
%%  Insertion patterns.
%%
%%    (a) O* -> O'
%%
%%    (c) O$ -> [C] -> C-O*
%%        Range partition:
%%          C(system_key_min, C_stored_key_max)
%%          O*(C_stored_key_max, system_key_max)
%%
%%    (c) C1-O2
%%      Insertion to C1 : C1'-O2'.
%%      Insertion to O2 : C1-O2'.
%%
%%    (d) C1-O2$
%%      Insertion to C1 : C1'-C2 -> C1'-O*-C2.
%%      Insertion to O2$ : C1-C2 -> C1-O*-C2
%%        Range partition:
%%          C1(C1_min, C1_stored_max)
%%          O*(C1_stored_max, O2_min)
%%          C2(O2_min, O2_max)
%%
%%    (e) C1-O2-C3
%%      Insertion to C1 : C1'-O2'-C3
%%      Insertion to C3 : C1-O2 | C3'-O4
%%      Insertion to O2 : C1-O2'-C3
%%        Range partition:
%%          C1(C1_min, C1_max)
%%          O2(O2_min, O2_max)
%%          C3(O2_max, C3_stored_max)
%%          O4(C3_stored_max, C3_max)
%%
%%    (f) C1-O2$-C3
%%      Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
%%      Insertion to O2$ : C1-C2-C3 -> C1-O2' | C3'-O4
%%      Insertion to C3  : C1-O2$ | C3'-O4
%%
%%  Deletion patterns
%%
%%    (a) C1-O2 | C3-O4
%%      Deletion from C1 causes O2 -> C1.
%%        C1'-O2' | C3-O4.
%%
%%      Deletion from C3. Same as above.
%%
%%    (b) C1-O2 | C3-O4*
%%      Deletion from C1. Same as (a).
%%
%%      Deletion from C3 causes O2 -> C3.
%%        C1-O2'-C3'
%%
%%    (c) C1-O2 | C3-O4-C5
%%      Deletion from C1. Same as (a).
%%
%%      Deletion from C3 causes O4-> C3
%%        C1-O2 | C3'-O4'-C5
%%
%%      Deletion from C5 causes O4-> C5
%%        C1-O2 | C3-O4'-C5'
%%
%%    (d) C1-O2 | C3-O4*-C5
%%      Deletion from C1. Same as (a).
%%
%%      Deletion from C3 causes C5 to C3.
%%        C1-O2 | C3'-O5
%%
%%      Deletion from C5.
%%        C1-O2 | C3-O5
%%
%%    (e) C1-02* | C3-O4
%%      Deletion from C1.
%%        -> O1-O2* | C3-O4 -> C1'-O2* | O3-O4 -> C1'-O2* | C3'-O4'
%%
%%      Deletion from C3. Same as (a).
%%
%%    (f) C1-O2* | C3-O4*
%%      This should be never appears. C1-O2*C3.
%%
%%    (g) C1-O2* | C3-O4-C5
%%      Deletion from C1.
%%        O1-O2* | C3-O4-C5 -> C1'-O2* | O3-O4-C5 -> C1'-O2* | C3'-O4'-C5
%%
%%      Deletion from C3 or C5 same as (c).
%%
%%    (h) C1-O2* | C3-O4*-C5
%%      Deletion from C1.
%%        O1-O2* | C3-O4*-C5 -> C1'-O2* | O3-O4*-C5 -> C1'-O2* | C3'-O5'
%%
%%    (i) C1-O2-C3 | C4-O5
%%       Same as (a) and (c)
%%
%%    (j) C1-O2-C3 | C4-O5*
%%      Deletion from C1 or C3. Same as (c).
%%
%%      Deletion from C4
%%        C1-O2-C3 | O4-O5* -> C1-O2 | C3-O4
%%
%%    (k) C-O2-C3 | C4-O5-C6
%%      Same as (c)
%%
%%    (l) C1-O2-C3 | C4-O5*-C6
%%      Deletion from C1 or C3. Same as (c).
%%
%%      Deletion from C4.
%%        C1-O2-C3 | O4-O5*-C6 -> C1-O2-C3 | C4'-O6
%%
%%      Deletion from C6.
%%        C1-O2-C3 | C4-O5*-O6 -> C1-O2-C3 | C4-O6
%%
%%    (m) C1-O2*-C3 | C4-O5
%%      Deletion from C1.
%%        O1-O2*-C3 | C4-O5 -> C1'-O3' | C4-O5
%%
%%      Deletion from C3.
%%        C1-O2*-O3 | C4-O5 -> C1-O3 | C4-O5
%%
%%    (n) C1-O2*-C3 | C4-O5*
%%      Deletion from C1 or C3. Same as (m).
%%
%%      Deletion from C4.
%%        C1-O2*-C3 | O4-O5* -> C1-O2* | C3-O4
%%
%%    (o) C1-O2*-C3 | C4-O5-C6
%%      Deletion from C1 or C3. Same as (m)
%%      Deletion from C4 or C6. Same as (c)
%%
%%    (p) C1-O2*-C3 | C4-O5*-C6
%%      Same as (m)

%%====================================================================
%% API
%%====================================================================
set_gen_mvector_op(Bucket, Fun) ->
    gen_server:call(Bucket, {set_gen_mvector_op, Fun}).


get_left_op(Bucket) ->
    get_left_op(Bucket, 0).

get_right_op(Bucket) ->
    get_right_op(Bucket, 0).

get_left_op(Bucket, Level) ->
    gen_server:call(Bucket, {get_left_op, Level}).

get_right_op(Bucket, Level) ->
    gen_server:call(Bucket, {get_right_op, Level}).

get_range_op(Bucket) ->
    gen_server:call(Bucket, get_range_op).

get_type_op(Bucket) ->
    gen_server:call(Bucket, get_type_op).

set_type_op(Bucket, Type) ->
    gen_server:call(Bucket, {set_type_op, Type}).

set_range_op(Bucket, {MinKey, EncompassMin}, {MaxKey, EncompassMax}) ->
    gen_server:call(Bucket, {set_range_op, {MinKey, EncompassMin}, {MaxKey, EncompassMax}}).

set_max_key_op(Bucket, MaxKey, EncompassMax) ->
    gen_server:call(Bucket, {set_max_key_op, MaxKey, EncompassMax}).

set_min_key_op(Bucket, MinKey, EncompassMin) ->
    gen_server:call(Bucket, {set_min_key_op, MinKey, EncompassMin}).

insert_op(Bucket, Key, Value) ->
    gen_server:call(Bucket, {insert_op, Key, Value}).

just_insert_op(Bucket, Key, Value) ->
    gen_server:call(Bucket, {just_insert_op, Key, Value}).

is_empty_op(Bucket) ->
    gen_server:call(Bucket, is_empty_op).

is_full_op(Bucket) ->
    gen_server:call(Bucket, is_full_op).

get_largest_op(Bucket) ->
    gen_server:call(Bucket, get_largest_op).

get_smallest_op(Bucket) ->
    gen_server:call(Bucket, get_smallest_op).

take_largest_op(Bucket) ->
    gen_server:call(Bucket, take_largest_op).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Args) ->
%%    io:format("mio_bucket allocated on ~p~n", [node()]),
    gen_server:start_link(?MODULE, Args, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Allocator, Capacity, Type, MembershipVector]) ->
    Length = length(MembershipVector),
    EmptyNeighbors = lists:duplicate(Length + 1, []), % Level 3, require 0, 1, 2, 3
    Insereted = case Type of
                    alone ->
                        %% set as inserted state
                        lists:duplicate(Length + 1, true);
                    _ ->
                        lists:duplicate(Length + 1, false)
                end,

    {ok, #node{store=mio_store:new(Capacity),
                type=Type,
                min_key=?MIN_KEY,
                encompass_min=false,
                max_key=?MAX_KEY,
                encompass_max=false,
                left=EmptyNeighbors,
                right=EmptyNeighbors,
                membership_vector=MembershipVector,
                expire_time=0,
                inserted=Insereted,
                deleted=false,
                gen_mvector=fun mio_mvector:generate/1,
                allocator=Allocator
               }}.



%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Function:
%% Description:
%%--------------------------------------------------------------------

%%====================================================================
%% Internal functions
%%====================================================================
insert_op_call(From, State, Self, Key, Value)  ->
    InsertState = just_insert_op(Self, Key, Value),
    NewlyAllocatedBucket =
    case {State#node.type, InsertState} of
        {c_o_l, overflow} ->
            insert_c_o_l_overflow(State, Self, neighbor_node(State, right, 0));
        {c_o_c_l, overflow} ->
            insert_c_o_c_l_overflow(State, Self, neighbor_node(State, right, 0));
        {c_o_c_r, overflow} ->
            %% C1-O2-C3 -> C1-O2 | C3'-O4
            %%   or
            %% C1-O2$-C3 -> C1-O2$ | C3'-O4
            split_c_o_c_by_r(State, get_left_op(neighbor_node(State, left, 0)), neighbor_node(State, left, 0), Self);
        {alone, full} ->
            %% O$ -> [C] -> C-O*
            insert_alone_full(State, Self);
        {c_o_r, full} ->
            %% C1-O2$ -> C1-O*-C2
            make_c_o_c(State, neighbor_node(State, left, 0), Self);
        %% Insertion to left o
        {c_o_c_m, full} ->
            %%  C1-O2$-C3
            %%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
            split_c_o_c_by_m(State, neighbor_node(State, left, 0), Self, neighbor_node(State, right, 0));
        _ -> []
    end,

    %% It's preferable that start buckt is local.
    case mio_util:is_local_process(NewlyAllocatedBucket) of
        true ->
%%            io:format("registered ~p ~p ~p~n", [NewlyAllocatedBucket, node(), node(NewlyAllocatedBucket)]),
            ok = mio_local_store:set(start_bucket, NewlyAllocatedBucket);
        _ ->
            []
    end,
    gen_server:reply(From, ok).

make_empty_bucket(State, Type) ->
    MaxLevel = length(State#node.membership_vector),
    case State#node.allocator of
        [] ->
            mio_sup:make_bucket([], mio_store:capacity(State#node.store), Type, apply(State#node.gen_mvector, [MaxLevel]));
        Allocator ->
            mio_allocator:allocate_bucket(Allocator, mio_store:capacity(State#node.store), Type, apply(State#node.gen_mvector, [MaxLevel]))
    end.

make_c_o_c(State, Left, Right) ->
    {EmptyBucketMinKey, _} = get_largest_op(Left),
    {EmptyBucketMaxKey, _} = get_smallest_op(Right),
    {NewLeftMaxKey, _} = get_largest_op(Left),

    %%  Range partition:
    %%    C1(C1_min, C1_stored_max)
    %%    O*(C1_stored_max, O2_min)
    %%    C2(O2_min, O2_max)
    {ok, EmptyBucket} = make_empty_bucket(State, c_o_c_m),
    set_range_op(EmptyBucket, {EmptyBucketMinKey, false}, {EmptyBucketMaxKey, false}),
    set_min_key_op(Right, EmptyBucketMaxKey, true),
    set_max_key_op(Left, NewLeftMaxKey, true),

    link_three_nodes(Left, EmptyBucket, Right, 0),
    ok = set_type_op(Right, c_o_c_r),
    ok = set_type_op(Left, c_o_c_l),
    %% link on Level >= 1
    link_on_level_ge1(EmptyBucket, State),
    EmptyBucket.

insert_c_o_l_overflow(State, Left, Right) ->
    {LargeKey, LargeValue} = take_largest_op(Left),
    case just_insert_op(Right, LargeKey, LargeValue) of
        full ->
            %% C1-O2$ -> C1'-O*-C2
            NewlyAllocatedBucket = make_c_o_c(State, Left, Right),
            NewlyAllocatedBucket;
        _ ->
            %% C1-O -> C1'-O'
            {NewMaxKey, _} = get_largest_op(Left),
            set_max_key_op(Left, NewMaxKey, true),
            set_min_key_op(Right, NewMaxKey, false),
            []
    end.


insert_c_o_c_l_overflow(State, Left, Middle) ->
    {LargeKey, LargeValue} = take_largest_op(Left),
    NewlyAllocatedBucket =
    case just_insert_op(Middle, LargeKey, LargeValue) of
        full ->
            %%  C1-O2$-C3
            %%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
            split_c_o_c_by_l(State, Left, Middle, get_right_op(Middle));
        _ ->
            %% C1-O2-C3 -> C1'-O2'-C3
            {LeftMax, _} = get_largest_op(Left),
            set_max_key_op(Left, LeftMax, true),
            set_min_key_op(Middle, LeftMax, false),
            []
    end,
    NewlyAllocatedBucket.

insert_alone_full(State, Self) ->
    {ok, EmptyBucket} = make_empty_bucket(State, c_o_r),
    {LargestKey, _} = get_largest_op(Self),
    SelfMaxKey = LargestKey,
    {EmptyMinKey, EmptyMaxKey} = {LargestKey, State#node.max_key},

    %% Change type
    set_type_op(Self, c_o_l),

    %% link on Level 0
    link_right_op(Self, 0, EmptyBucket),
    link_left_op(EmptyBucket, 0, Self),

    %% range partition
    set_max_key_op(Self, SelfMaxKey, true),
    set_range_op(EmptyBucket, {EmptyMinKey, false}, {EmptyMaxKey, State#node.encompass_max}),

    %% link on Level >= 1
    link_on_level_ge1(EmptyBucket, State),
    EmptyBucket.

prepare_split_c_o_c(State, Left, Middle, Right) ->
    %%  C1-O2$-C3
    %%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
    {ok, EmptyBucket} = make_empty_bucket(State, c_o_r),
    ok = set_type_op(Right, c_o_l),
    ok = set_type_op(Middle, c_o_r),
    ok = set_type_op(Left, c_o_l),
    PrevRight = get_right_op(Right),
    {PrevRight, EmptyBucket}.

adjust_range_link_c_o_c(Left, Middle, Right, PrevRight, EmptyBucket) ->
    {LeftMaxKey, _} = get_largest_op(Left),
    {MiddleMaxKey, _} = get_largest_op(Middle),
    {_, {OldRightMaxKey, OldEncompassMax}} = get_range_op(Right),
    {RightMaxKey, _} = get_largest_op(Right),
    EmptyMinKey = RightMaxKey,
    MiddleMinKey = LeftMaxKey,
    set_max_key_op(Left, LeftMaxKey, true),
    set_range_op(Middle, {MiddleMinKey, false}, {MiddleMaxKey, true}),
    set_range_op(Right, {MiddleMaxKey, false}, {RightMaxKey, true}),
    set_range_op(EmptyBucket, {EmptyMinKey, false}, {OldRightMaxKey, OldEncompassMax}),
    %% C3'-O4 | C ...
    link_three_nodes(Right, EmptyBucket, PrevRight, 0).


%% Insertion to the Left causes overflow
split_c_o_c_by_l(State, Left, Middle, Right) ->
    {PrevRight, EmptyBucket} = prepare_split_c_o_c(State, Left, Middle, Right),

    {LargeRKey, LargeRValue} = take_largest_op(Right),
    ok = just_insert_op(EmptyBucket, LargeRKey, LargeRValue),
    {LargeMKey, LargeMValue} = take_largest_op(Middle),
    full = just_insert_op(Right, LargeMKey, LargeMValue),

    adjust_range_link_c_o_c(Left, Middle, Right, PrevRight, EmptyBucket),

    %% link on Level >= 1
    link_on_level_ge1(EmptyBucket, State),
    EmptyBucket.

%% Insertion to the Middle causes overflow
split_c_o_c_by_m(State, Left, Middle, Right) ->
    {PrevRight, EmptyBucket} = prepare_split_c_o_c(State, Left, Middle, Right),

    {LargeKey, LargeValue} = take_largest_op(Middle),
    overflow = just_insert_op(Right, LargeKey, LargeValue),
    {LargeRKey, LargeRValue} = take_largest_op(Right),
    ok = just_insert_op(EmptyBucket, LargeRKey, LargeRValue),

    adjust_range_link_c_o_c(Left, Middle, Right, PrevRight, EmptyBucket),

    %% link on Level >= 1
    link_on_level_ge1(EmptyBucket, State),
    EmptyBucket.


%% Insertion to the Right causes overflow
split_c_o_c_by_r(State, Left, Middle, Right) ->
    {PrevRight, EmptyBucket} = prepare_split_c_o_c(State, Left, Middle, Right),
    {LargeKey, LargeValue} = take_largest_op(Right),
    ok = just_insert_op(EmptyBucket, LargeKey, LargeValue),

    %% range partition
    {_, {OldMaxKey, OldEncompassMax}} = get_range_op(Right),
    {NewRightMaxKey, _} = get_largest_op(Right),
    {NewMiddleMaxKey, _} = get_smallest_op(Right),
    set_max_key_op(Middle, NewMiddleMaxKey, false),
    set_range_op(Right, {NewMiddleMaxKey, true}, {NewRightMaxKey, true}),
    set_range_op(EmptyBucket, {NewRightMaxKey, false}, {OldMaxKey, OldEncompassMax}),

    %% C3'-O4 | C ...
    link_three_nodes(Right, EmptyBucket, PrevRight, 0),

    %% link on Level >= 1
    link_on_level_ge1(EmptyBucket, State),

    EmptyBucket.

%% Skip graphs

%%--------------------------------------------------------------------
%%  set expire_time operation
%%--------------------------------------------------------------------
set_expire_time_op(Node, ExpireTime) ->
    gen_server:call(Node, {set_expire_time_op, ExpireTime}).

%%--------------------------------------------------------------------
%%  insert operation
%%--------------------------------------------------------------------
%% insert_op(Introducer, NodeToInsert) ->
%%     %% Insertion timeout should be infinity since they are serialized and waiting.
%%     gen_server:call(NodeToInsert, {insert_op, Introducer}, 5000).

%%--------------------------------------------------------------------
%%  delete operation
%%--------------------------------------------------------------------
%% delete_op(Introducer, Key) ->
%%     {FoundNode, FoundKey, _, _} = mio_skip_graph:search_op(Introducer, Key),
%%     case string:equal(FoundKey, Key) of
%%         true ->
%%             delete_op(FoundNode),
%%             ok;
%%         false -> ng
%%     end.

%% delete_op(Node) ->
%%     gen_server:call(Node, delete_op, 30000), %% todo proper timeout value

%%     %% Since the node to delete may be still referenced,
%%     %% We wait 1 minitue.
%%     OneMinute = 60000,
%%     terminate_node(Node, OneMinute),
%%     ok.


%% stats_op(Node, MaxLevel) ->
%%     stats_status(Node, MaxLevel).

%% stats_curr_items(Node) ->
%%     {"curr_items", integer_to_list(length(dump_op(Node, 0)))}.

%% stats_status(Node, MaxLevel) ->
%%     case mio_util:do_times_with_index(0, MaxLevel,
%%                              fun(Level) ->
%%                                      mio_debug:check_sanity(Node, Level, stats, 0)
%%                              end) of
%%         ok -> {"mio_status", "OK"};
%%         Other ->
%%             {"mio_status", io_lib:format("STAT check_sanity NG Broken : ~p", [Other])}
%%     end.

%% terminate_node(Node, After) ->
%%     spawn_link(fun() ->
%%                   receive after After -> ok end
%%           end).

%%--------------------------------------------------------------------
%%  range search operation
%%--------------------------------------------------------------------
%% Key1 and Key2 are not in the search result.
range_search_asc_op(StartNode, Key1, Key2, Limit) ->
    range_search_order_op_(StartNode, Key1, Key2, Limit, asc).

range_search_desc_op(StartNode, Key1, Key2, Limit) ->
    range_search_order_op_(StartNode, Key1, Key2, Limit, desc).

%% Since StartNodes may be in between Key1 and Key2, we have to avoid being gen_server:call blocked.
%% For this purpose, we do range search in this process, which is not node process.
range_search_order_op_(StartNode, Key1, Key2, Limit, Order) ->
    {StartKey, CastOp} = case Order of
                               asc -> {Key1, range_search_asc_op_cast};
                               _ -> {Key2, range_search_desc_op_cast}
                         end,
    {ClosestNode, _, _, _} = mio_skip_graph:search_op(StartNode, StartKey),
    ReturnToMe = self(),
    gen_server:cast(ClosestNode, {CastOp, ReturnToMe, Key1, Key2, [], Limit}),
    receive
        {range_search_accumed, Accumed} ->
            Accumed
    after 100000 ->
            range_search_timeout %% should never happen
    end.

%%--------------------------------------------------------------------
%%  Search operation
%%
%%--------------------------------------------------------------------

%%     %% If Level is not specified, the start node checkes his max level and use it

%%     ?LOGF(""),
%%     {FoundNode, FoundKey, Value, ExpireTime} = gen_server:call(StartNode, {search_op, Key, StartLevel}, infinity),
%%     ?LOGF(""),
%%     case Key =< FoundKey of
%%         true ->
%%             ?LOGF(""),
%%             get_op(FoundNode, Key);
%%         _ ->
%% %%             ?LOGF(""),
%% %%             io:format("Key=~p FoundKey=~p", [Key, FoundKey]),
%% %%             ?assert(false),
%% %%             {error, not_found}
%%             get_op(FoundNode, Key)
%%     end.

%%--------------------------------------------------------------------
%%  buddy operation
%%--------------------------------------------------------------------
buddy_op(Node, MembershipVector, Direction, Level) ->
    gen_server:call(Node, {buddy_op, MembershipVector, Direction, Level}).

%%--------------------------------------------------------------------
%%  link operation
%%--------------------------------------------------------------------
link_right_op([], _Level, _Right) ->
    ok;
link_right_op(Node, Level, Right) ->
    gen_server:call(Node, {link_right_op, Level, Right}).

link_left_op([], _Level, _Left) ->
    ok;
link_left_op(Node, Level, Left) ->
    gen_server:call(Node, {link_left_op, Level, Left}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(is_empty_op, _From, State) ->
    {reply, mio_store:is_empty(State#node.store), State};

handle_call(is_full_op, _From, State) ->
    {reply, mio_store:is_full(State#node.store), State};

handle_call({get_op, Key}, _From, State) ->
    {reply, mio_store:get(Key, State#node.store), State};

handle_call(skip_graph_get_key_op, _From, State) ->
    {reply, mio_skip_graph:get_key(State), State};

handle_call(skip_graph_dump_op, _From, State) ->
    {reply, mio_skip_graph:dump_op_call(State), State};

handle_call({get_left_op, Level}, _From, State) ->
    {reply, neighbor_node(State, left, Level), State};

handle_call({get_right_op, Level}, _From, State) ->
    {reply, neighbor_node(State, right, Level), State};

handle_call({set_type_op, Type}, _From, State) ->
    {reply, ok, State#node{type=Type}};

handle_call({set_range_op, {MinKey, EncompassMin}, {MaxKey, EncompassMax}}, _From, State) ->
    {reply, ok, State#node{min_key=MinKey, encompass_min=EncompassMin, max_key=MaxKey, encompass_max=EncompassMax}};

handle_call({set_max_key_op, MaxKey, EncompassMax}, _From, State) ->
    {reply, ok, State#node{max_key=MaxKey, encompass_max=EncompassMax}};

handle_call({set_min_key_op, MinKey, EncompassMin}, _From, State) ->
    {reply, ok, State#node{min_key=MinKey, encompass_min=EncompassMin}};

handle_call({set_gen_mvector_op, Fun}, _From, State) ->
    {reply, ok, State#node{gen_mvector=Fun}};

handle_call(get_type_op, _From, State) ->
    {reply, State#node.type, State};

handle_call(take_largest_op, _From, State) ->
    {Key, Value, NewStore} = mio_store:take_largest(State#node.store),
    {reply, {Key, Value}, State#node{store=NewStore}};

handle_call(get_largest_op, _From, State) ->
    {reply, mio_store:largest(State#node.store), State};

handle_call(get_smallest_op, _From, State) ->
    {reply, mio_store:smallest(State#node.store), State};

handle_call({just_insert_op, Key, Value}, _From, State) ->
    case mio_store:set(Key, Value, State#node.store) of
        {overflow, NewStore} ->
            {reply, overflow, State#node{store=NewStore}};
        {full, NewStore} ->
            {reply, full, State#node{store=NewStore}};
        NewStore ->
            {reply, ok, State#node{store=NewStore}}
    end;

handle_call({insert_op, Key, Value}, From, State) ->
    Self = self(),
    spawn_link(?MODULE, insert_op_call, [From, State, Self, Key, Value]),
    {noreply, State};

handle_call({skip_graph_insert_op, Key, Value}, From, State) ->
    Self = self(),
    spawn_link(mio_skip_graph, insert_op_call, [From, Self, Key, Value]),
    {noreply, State};

handle_call(get_range_op, _From, State) ->
    {reply, {{State#node.min_key, State#node.encompass_min}, {State#node.max_key, State#node.encompass_max}}, State};


%% Read Only Operations start
handle_call({skip_graph_search_op, SearchKey, Level}, From, State) ->
    Self = self(),
    spawn_link(mio_skip_graph, search_op_call, [From, State, Self, SearchKey, Level]),
    {noreply, State};

handle_call(get_op, From, State) ->
    spawn_link(?MODULE, get_op_call, [From, State]),
    {noreply, State};

handle_call({buddy_op, MembershipVector, Direction, Level}, From, State) ->
    Self = self(),
    spawn_link(?MODULE, buddy_op_call, [From, State, Self, MembershipVector, Direction, Level]),
    {noreply, State};

%% Write Operations start
handle_call({insert_op, Introducer}, From, State) ->

    Self = self(),
    spawn_link(?MODULE, insert_op_call, [From, State, Self, Introducer]),
    {noreply, State};

handle_call(delete_op, From, State) ->
    Self = self(),
    spawn_link(?MODULE, delete_op_call, [From, Self, State]),
    {noreply, State};

%% handle_call({set_op, NewValue}, _From, State) ->
%%     set_op_call(State, NewValue);

handle_call({link_right_op, Level, RightNode}, _From, State) ->
    {reply, ok, set_right(State, Level, RightNode)};

handle_call({link_left_op, Level, LeftNode}, _From, State) ->
    {reply, ok, set_left(State, Level, LeftNode)};

handle_call(Args, _From, State) ->
    io:format("Unknown handle call on mio_bucket : ~p:State=~p", [Args, State]).


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%%  dump operation
%%--------------------------------------------------------------------
%% handle_cast({dump_side_cast, Direction, Level, ReturnToMe, Accum}, State) ->
%%     MyKey = my_key(State),
%%     MyValue = State#node.value,
%%     MyMVector = State#node.membership_vector,
%%     case neighbor_node(State, Direction, Level) of
%%         [] ->
%%             Ret = case Direction of
%%                       right ->
%%                           lists:reverse([{self(), MyKey, MyValue, MyMVector} | Accum]);
%%                       left ->
%%                           [{self(), MyKey, MyValue, MyMVector} | Accum]
%%                   end,
%%             ReturnToMe ! {dump_side_accumed, Ret};
%%         NeighborNode ->
%%             gen_server:cast(NeighborNode, {dump_side_cast, Direction, Level, ReturnToMe, [{self(), MyKey, MyValue, MyMVector} | Accum]})
%%     end,
%%     {noreply, State};

%%--------------------------------------------------------------------
%%  range search operation
%%--------------------------------------------------------------------
%% handle_cast({range_search_asc_op_cast, ReturnToMe, Key1, Key2, Accum, Limit}, State) ->
%%     range_search_(ReturnToMe, Key1, Key2, Accum, Limit, State,
%%                   range_search_asc_op_cast, right, my_key(State) =< Key1),
%%     {noreply, State};

%% handle_cast({range_search_desc_op_cast, ReturnToMe, Key1, Key2, Accum, Limit}, State) ->
%%     range_search_(ReturnToMe, Key1, Key2, Accum, Limit, State,
%%                   range_search_desc_op_cast, left, my_key(State) >= Key2),
%%     {noreply, State}.

%% range_search_(ReturnToMe, _Key1, _Key2, Accum, Limit, _State, _Op, _Direction, _IsOutOfRange) when Limit =:= 0 ->
%%     ReturnToMe ! {range_search_accumed, lists:reverse(Accum)};
%% range_search_(ReturnToMe, Key1, Key2, Accum, Limit, State, Op, Direction, IsOutOfRange) when IsOutOfRange ->
%%     case neighbor_node(State, Direction, 0) of
%%         [] ->
%%             ReturnToMe ! {range_search_accumed, lists:reverse(Accum)};
%%         NextNode ->
%%             gen_server:cast(NextNode,
%%                             {Op, ReturnToMe, Key1, Key2, Accum, Limit})
%%     end;
%% range_search_(ReturnToMe, Key1, Key2, Accum, Limit, State, Op, Direction, _IsOutOfRange) ->
%%     MyKey = my_key(State),
%%     MyValue = State#node.value,
%%     MyExpireTime = State#node.expire_time,
%%     if
%%        Key1 < MyKey andalso MyKey < Key2 ->
%%             case neighbor_node(State, Direction, 0) of
%%                 [] ->
%%                     ReturnToMe ! {range_search_accumed, lists:reverse([{self(), MyKey, MyValue, MyExpireTime} | Accum])};
%%                 NextNode ->
%%                     gen_server:cast(NextNode,
%%                                     {Op, ReturnToMe, Key1, Key2, [{self(), MyKey, MyValue, MyExpireTime} | Accum], Limit - 1})
%%             end;
%%        true ->
%%             ReturnToMe ! {range_search_accumed, lists:reverse(Accum)}
%%     end.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%% %%--------------------------------------------------------------------
%% handle_info(_Info, State) ->
%%     {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
%% terminate(_Reason, _State) ->
%%     ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
%% code_change(_OldVsn, State, _Extra) ->
%%     {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------


%%--------------------------------------------------------------------
%%% Implementation of genserver:call
%%--------------------------------------------------------------------
get_op_call(From, State) ->
    gen_server:reply(From, {my_key(State), dummy_value_todo, State#node.membership_vector, State#node.left, State#node.right}).
%% get_neighbor_op_call(From, State, Direction, Level) ->
%%     NeighborNode = neighbor_node(State, Direction, Level),
%%     NeighborKey = neighbor_key(State, Direction, Level),
%%     gen_server:reply(From, {NeighborNode, NeighborKey}).

%% set_op_call(State, NewValue) ->
%%     {reply, dummy_todo, State}.

buddy_op_call(From, State, Self, MembershipVector, Direction, Level) ->
    IsSameMV = mio_mvector:eq(Level, MembershipVector, State#node.membership_vector),
    %% N.B.
    %%   We have to check whether this node is inserted on this Level, if not this node can't be buddy.
    IsInserted = node_on_level(State#node.inserted, Level),
    if
        IsSameMV andalso IsInserted ->
            MyKey = my_key(State),
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



%%--------------------------------------------------------------------
%%  delete operation
%%--------------------------------------------------------------------
%% delete_op_call(From, Self, State) ->
%%     LockedNodes = lock_or_exit([Self], ?LINE, [my_key(State)]),
%%     IsDeleted = gen_server:call(Self, get_deleted_op),
%%     if IsDeleted ->
%%             %% already deleted.
%%             unlock(LockedNodes, ?LINE),
%%             gen_server:reply(From, ok);
%%        true ->
%%             case gen_server:call(Self, get_inserted_op) of
%%                 true ->
%%                     MaxLevel = length(State#node.membership_vector),
%%                     %% My State will not be changed, since I will be killed soon.
%%                     gen_server:call(Self, set_deleted_op),
%%                     %% N.B.
%%                     %% To prevent deadlock, we unlock the Self after deleted mark is set.
%%                     %% In delete_loop, Self will be locked/unlocked with left/right nodes on each level for the same reason.
%%                     unlock(LockedNodes, ?LINE),
%%                     delete_loop_(Self, MaxLevel),
%%                     gen_server:reply(From, ok);
%%                 _ ->
%%                     unlock(LockedNodes, ?LINE),
%%                     %% Not inserted yet, wait.
%%                     mio_util:random_sleep(0),
%%                     ?INFO("not inserted yet. waiting ..."),
%%                     delete_op_call(From, Self, State)
%%             end
%%     end.

%% delete_loop_(_Self, Level) when Level < 0 ->
%%     [];
%% delete_loop_(Self, Level) ->
%%     RightNode = get_right_op(Self, Level),
%%     LeftNode = get_left_op(Self, Level),
%%     LockedNodes = lock_or_exit([RightNode, Self, LeftNode], ?LINE, []),

%%     ?CHECK_SANITY(Self, Level),

%%     ok = link_left_op(RightNode, Level, LeftNode),
%%     ok = link_right_op(LeftNode, Level, RightNode),

%%     ?CHECK_SANITY(Self, Level),
%%     unlock(LockedNodes, ?LINE),
%%     %% N.B.
%%     %% We keep the right/left node of Self, since it may be still located on search path.
%%     delete_loop_(Self, Level - 1).


get_neighbor_op_call(From, State, Direction, Level) ->
    NeighborNode = neighbor_node(State, Direction, Level),
    {NeighborKey, _} = mio_skip_graph:get_key_op(NeighborNode),
    gen_server:reply(From, {NeighborNode, NeighborKey}).

link_three_nodes(LeftNode, CenterNode, RightNode, Level) ->
    %% [Left] -> [Center]  [Right]
    link_right_op(LeftNode, Level, CenterNode),

    %% [Left]    [Center] <- [Right]
    link_left_op(RightNode, Level, CenterNode),

    %% [Left] <- [Center]    [Right]
    link_left_op(CenterNode, Level, LeftNode),

    %% [Left]    [Center] -> [Right]
    link_right_op(CenterNode, Level, RightNode).

%%--------------------------------------------------------------------
%%  Insert operation
%%--------------------------------------------------------------------



buddy_op_proxy([], [], _MyMV, _Level) ->
    not_found;
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
    case buddy_op_proxy(LeftOnLower, RightOnLower, MyMV, Level) of
        not_found ->
            %% We have no buddy on this level.
            %% On higher Level, we have no buddy also.
            %% So we've done.
            ?CHECK_SANITY(Self, Level),
            dynomite_prof:stop_prof(link_on_level_ge1),
            [];
        %% [Buddy] <-> [NodeToInsert] <-> [BuddyRight]
        {ok, left, Buddy, _BuddyKey, BuddyRight} ->
            dynomite_prof:start_prof(link_on_level_ge1),
            do_link_level_ge1(Self, Buddy, BuddyRight, Level, MaxLevel, left),
            ?CHECK_SANITY(Self, Level),
            dynomite_prof:stop_prof(link_on_level_ge1);
        %% [BuddyLeft] <-> [NodeToInsert] <-> [Buddy]
        {ok, right, Buddy, _BuddyKey, BuddyLeft} ->
            dynomite_prof:start_prof(link_on_level_ge1),
            do_link_level_ge1(Self, Buddy, BuddyLeft, Level, MaxLevel, right),
            ?CHECK_SANITY(Self, Level),
            dynomite_prof:stop_prof(link_on_level_ge1)
    end.

do_link_level_ge1(Self, Buddy, BuddyNeighbor, Level, MaxLevel, Direction) ->
    case Direction of
        right ->
            link_three_nodes(BuddyNeighbor, Self, Buddy, Level);
        left ->
            link_three_nodes(Buddy, Self, BuddyNeighbor, Level)
    end,
    ?CHECK_SANITY(Self, Level),
    %% Go up to next Level.
    link_on_level_ge1(Self, Level + 1, MaxLevel).

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

reverse_direction(Direction) ->
    case Direction of
        right ->
             left;
        left ->
            right
    end.

set_right(State, Level, Node) ->
    State#node{right=mio_util:lists_set_nth(Level + 1, Node, State#node.right)}.

set_left(State, Level, Node) ->
    State#node{left=mio_util:lists_set_nth(Level + 1, Node, State#node.left)}.

my_key(State) ->
    State#node.max_key.

get_op(Bucket, Key) ->
    gen_server:call(Bucket, {get_op, Key}).
