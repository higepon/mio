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
         get_op/2,
         get_left_op/1, get_right_op/1,
         get_left_op/2, get_right_op/2,
         set_left_op/2, set_right_op/2,
         set_left_op/3, set_right_op/3,
         insert_op/3, just_insert_op/3,
         get_type_op/1, set_type_op/2,
         is_empty_op/1, is_full_op/1,
         take_largest_op/1,
         get_largest_op/1, get_smallest_op/1,
         insert_op_call/5,
         get_range_op/1, set_range_op/3,
         set_max_key_op/2, set_min_key_op/2,
         %% Skip Graph layer
stats_op/2,
         search_op_call/5, buddy_op_call/6, get_op_call/2, get_neighbor_op_call/4,
         insert_op_call/4, delete_op_call/3,
         search_op/2, link_right_op/4, link_left_op/4,
         set_expire_time_op/2, buddy_op/4, insert_op/2,
         delete_op/2, delete_op/1,range_search_asc_op/4, range_search_desc_op/4,
         check_invariant_level_0_left/5,
         check_invariant_level_0_right/5,
         check_invariant_level_ge1_left/5,
         check_invariant_level_ge1_right/5,
         node_on_level/2,
         %% For testability
         set_gen_mvector_op/2
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {store, type, min_key, max_key, left, right, membership_vector, gen_mvector,
                left_keys, right_keys,
                expire_time, inserted, deleted
               }).

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
%%      Insertion to C3 : C1-O2'-C3'
%%      Insertion to O2 : C1-O2 | C3'-O4
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

get_op(Bucket, Key) ->
    gen_server:call(Bucket, {get_op, Key}).

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

set_left_op(Bucket, Left, Level) ->
    gen_server:call(Bucket, {set_left_op, Left, Level}).

set_right_op(Bucket, Right, Level) ->
    gen_server:call(Bucket, {set_right_op, Right, Level}).

set_left_op(Bucket, Left) ->
    set_left_op(Bucket, Left, 0).

set_right_op(Bucket, Right) ->
    set_right_op(Bucket, Right, 0).

get_type_op(Bucket) ->
    gen_server:call(Bucket, get_type_op).

set_type_op(Bucket, Type) ->
    gen_server:call(Bucket, {set_type_op, Type}).

set_range_op(Bucket, MinKey, MaxKey) ->
    gen_server:call(Bucket, {set_range_op, MinKey, MaxKey}).

set_max_key_op(Bucket, MaxKey) ->
    gen_server:call(Bucket, {set_max_key_op, MaxKey}).

set_min_key_op(Bucket, MaxKey) ->
    gen_server:call(Bucket, {set_min_key_op, MaxKey}).

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
init([Capacity, Type, MembershipVector]) ->
    Length = length(MembershipVector),
    EmptyNeighbors = lists:duplicate(Length + 1, []), % Level 3, require 0, 1, 2, 3
    Insereted = case Type of
                    alone ->
                        %% set as inserted state
                        lists:duplicate(Length + 1, true);
                    _ ->
                        lists:duplicate(Length + 1, false)
                end,

    {ok, #state{store=mio_store:new(Capacity),
                type=Type,
                min_key=?MIN_KEY,
                max_key=?MAX_KEY,
                left=EmptyNeighbors,
                right=EmptyNeighbors,
                membership_vector=MembershipVector,
                left_keys=EmptyNeighbors,
                right_keys=EmptyNeighbors,
                expire_time=0,
                inserted=Insereted,
                deleted=false,
                gen_mvector=fun mio_mvector:generate/1
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
link_on_level0(Left, Right) ->
    ok = set_right_op(Left, Right),
    ok = set_left_op(Right, Left).

link3_op(Left, Middle, Right) ->
    link_on_level0(Left, Middle),
    link_on_level0(Middle, Right).

insert_op_call(From, State, Self, Key, Value)  ->
    InsertState = just_insert_op(Self, Key, Value),
    case {State#state.type, InsertState} of
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
    gen_server:reply(From, ok).

make_empty_bucket(State, Type) ->
    MaxLevel = length(State#state.membership_vector),
    mio_sup:make_bucket(mio_store:capacity(State#state.store), Type, apply(State#state.gen_mvector, [MaxLevel])).

make_c_o_c(State, Left, Right) ->
    {EmptyBucketMinKey, _} = get_largest_op(Left),
    {EmptyBucketMaxKey, _} = get_smallest_op(Right),
    {NewLeftMaxKey, _} = get_largest_op(Left),

    %%  Range partition:
    %%    C1(C1_min, C1_stored_max)
    %%    O*(C1_stored_max, O2_min)
    %%    C2(O2_min, O2_max)
    {ok, EmptyBucket} = make_empty_bucket(State, c_o_c_m),
    set_range_op(EmptyBucket, EmptyBucketMinKey, EmptyBucketMaxKey),
    set_min_key_op(Right, EmptyBucketMaxKey),
    set_max_key_op(Left, NewLeftMaxKey),

    link3_op(Left, EmptyBucket, Right),
    ok = set_type_op(Right, c_o_c_r),
    ok = set_type_op(Left, c_o_c_l).

insert_c_o_l_overflow(State, Left, Right) ->
    {LargeKey, LargeValue} = take_largest_op(Left),
    case just_insert_op(Right, LargeKey, LargeValue) of
        full ->
            %% C1-O2$ -> C1'-O*-C2
            make_c_o_c(State, Left, Right);
        _ ->
            %% C1-O -> C1'-O'
            []
    end.

insert_c_o_c_l_overflow(State, Left, Middle) ->
    {LargeKey, LargeValue} = take_largest_op(Left),
    case just_insert_op(Middle, LargeKey, LargeValue) of
        full ->
            %%  C1-O2$-C3
            %%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
            split_c_o_c_by_l(State, Left, Middle, get_right_op(Middle));
        _ ->
            %% C1-O2-C3 -> C1'-O2'-C3
            []
    end.

insert_alone_full(State, Self) ->
    {ok, EmptyBucket} = make_empty_bucket(State, c_o_r),
    {LargestKey, _} = get_largest_op(Self),
    link_right_op(Self, 0, EmptyBucket, State#state.max_key),
    link_left_op(EmptyBucket, 0, Self, LargestKey),
    gen_server:call(EmptyBucket, {set_inserted_op, 0}),
%%    link_on_level0(Self, EmptyBucket),

    set_type_op(Self, c_o_l),

    MaxLevel = length(State#state.membership_vector),

    link_on_level_ge1(EmptyBucket, MaxLevel),

    %% range partition

    set_max_key_op(Self, LargestKey),

    set_range_op(EmptyBucket, LargestKey, State#state.max_key).


prepare_for_split_c_o_c(State, Left, Middle, Right) ->
    ?ASSERT_NOT_NIL(Left),
    ?ASSERT_NOT_NIL(Middle),
    ?ASSERT_NOT_NIL(Right),
    {ok, EmptyBucket} = make_empty_bucket(State, c_o_r),
    ok = set_type_op(Right, c_o_l),
    ok = set_type_op(Middle, c_o_r),
    ok = set_type_op(Left, c_o_l),
    PrevRight = get_right_op(Right),

    %% C3'-O4 | C ...
    link3_op(Right, EmptyBucket, PrevRight),
    EmptyBucket.

adjust_range_c_o_c_o(Left, Middle, Right, MostRight) ->
    {LeftMax, _} = get_largest_op(Left),
    set_max_key_op(Left, LeftMax),

    {MiddleMax, _} = get_largest_op(Middle),
    set_range_op(Middle, LeftMax, MiddleMax),

    {_, OldRightMax} = get_range_op(Right),
    {RightMax, _} = get_largest_op(Right),
    set_range_op(Right, MiddleMax, RightMax),
    set_range_op(MostRight, RightMax, OldRightMax).

%% Insertion to the Left causes overflow
split_c_o_c_by_l(State, Left, Middle, Right) ->
    %%  C1-O2$-C3
    %%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
    EmptyBucket = prepare_for_split_c_o_c(State, Left, Middle, Right),
    {LargeRKey, LargeRValue} = take_largest_op(Right),
    ok = just_insert_op(EmptyBucket, LargeRKey, LargeRValue),
    {LargeMKey, LargeMValue} = take_largest_op(Middle),
    full = just_insert_op(Right, LargeMKey, LargeMValue),

    %% range partition
    adjust_range_c_o_c_o(Left, Middle, Right, EmptyBucket).


%% Insertion to the Middle causes overflow
split_c_o_c_by_m(State, Left, Middle, Right) ->
    %%  C1-O2$-C3
    %%    Insertion to O2$  : C1-C2-C3 -> C1-O2' | C3'-O4
    EmptyBucket = prepare_for_split_c_o_c(State, Left, Middle, Right),
    {LargeKey, LargeValue} = take_largest_op(Middle),
    overflow = just_insert_op(Right, LargeKey, LargeValue),
    {LargeRKey, LargeRValue} = take_largest_op(Right),
    ok = just_insert_op(EmptyBucket, LargeRKey, LargeRValue),

    %% range partition
    adjust_range_c_o_c_o(Left, Middle, Right, EmptyBucket).

%% Insertion to the Right causes overflow
split_c_o_c_by_r(State, Left, Middle, Right) ->
    EmptyBucket = prepare_for_split_c_o_c(State, Left, Middle, Right),
    {LargeKey, LargeValue} = take_largest_op(Right),
    ok = just_insert_op(EmptyBucket, LargeKey, LargeValue),

    %% range partition
    {_, OldMaxKey} = get_range_op(Right),
    {NewRightMaxKey, _} = get_largest_op(Right),
    set_max_key_op(Right, NewRightMaxKey),
    set_range_op(EmptyBucket, NewRightMaxKey, OldMaxKey).

my_key(State) ->
    State#state.max_key.

%% Skip graphs

%%--------------------------------------------------------------------
%%  set expire_time operation
%%--------------------------------------------------------------------
set_expire_time_op(Node, ExpireTime) ->
    gen_server:call(Node, {set_expire_time_op, ExpireTime}).

%%--------------------------------------------------------------------
%%  insert operation
%%--------------------------------------------------------------------
insert_op(Introducer, NodeToInsert) ->
    %% Insertion timeout should be infinity since they are serialized and waiting.
    gen_server:call(NodeToInsert, {insert_op, Introducer}, 5000).

%%--------------------------------------------------------------------
%%  delete operation
%%--------------------------------------------------------------------
delete_op(Introducer, Key) ->
    {FoundNode, FoundKey, _, _} = search_op(Introducer, Key),
    case string:equal(FoundKey, Key) of
        true ->
            delete_op(FoundNode),
            ok;
        false -> ng
    end.

delete_op(Node) ->
    gen_server:call(Node, delete_op, 30000), %% todo proper timeout value

    %% Since the node to delete may be still referenced,
    %% We wait 1 minitue.
    OneMinute = 60000,
    terminate_node(Node, OneMinute),
    ok.


stats_op(Node, MaxLevel) ->
    stats_status(Node, MaxLevel).

%% stats_curr_items(Node) ->
%%     {"curr_items", integer_to_list(length(dump_op(Node, 0)))}.

stats_status(Node, MaxLevel) ->
    case mio_util:do_times_with_index(0, MaxLevel,
                             fun(Level) ->
                                     mio_debug:check_sanity(Node, Level, stats, 0)
                             end) of
        ok -> {"mio_status", "OK"};
        Other ->
            {"mio_status", io_lib:format("STAT check_sanity NG Broken : ~p", [Other])}
    end.

terminate_node(Node, After) ->
    spawn_link(fun() ->
                  receive after After -> ok end,
                  mio_sup:terminate_node(Node)
          end).

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
    {ClosestNode, _, _, _} = search_op(StartNode, StartKey),
    ReturnToMe = self(),
    gen_server:cast(ClosestNode, {CastOp, ReturnToMe, Key1, Key2, [], Limit}),
    receive
        {range_search_accumed, Accumed} ->
            Accumed
    after 100000 ->
            range_search_timeout %% should never happen
    end.

%%--------------------------------------------------------------------
%%  search operation
%%--------------------------------------------------------------------
search_op(StartNode, Key) ->
    %% If Level is not specified, the start node checkes his max level and use it
    StartLevel = [],
    {FoundNode, FoundKey, Value, ExpireTime} = gen_server:call(StartNode, {search_op, Key, StartLevel}, infinity),
    case Key =< FoundKey of
        true ->
            get_op(FoundNode, Key);
        _ ->
            io:format("Key=~p FoundKey=~p", [Key, FoundKey]),
            ?assert(false),
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%%  buddy operation
%%--------------------------------------------------------------------
buddy_op(Node, MembershipVector, Direction, Level) ->
    gen_server:call(Node, {buddy_op, MembershipVector, Direction, Level}).

%%--------------------------------------------------------------------
%%  link operation
%%--------------------------------------------------------------------
link_right_op([], _Level, _Right, _RightKey) ->
    ok;
link_right_op(Node, Level, Right, RightKey) ->
    gen_server:call(Node, {link_right_op, Level, Right, RightKey}).

link_left_op([], _Level, _Left, _LeftKey) ->
    ok;
link_left_op(Node, Level, Left, LeftKey) ->
    gen_server:call(Node, {link_left_op, Level, Left, LeftKey}).

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
    {reply, mio_store:is_empty(State#state.store), State};

handle_call(is_full_op, _From, State) ->
    {reply, mio_store:is_full(State#state.store), State};

handle_call({get_op, Key}, _From, State) ->
    {reply, mio_store:get(Key, State#state.store), State};

handle_call({get_left_op, Level}, _From, State) ->
    {reply, neighbor_node(State, left, Level), State};

handle_call({get_right_op, Level}, _From, State) ->
    {reply, neighbor_node(State, right, Level), State};

handle_call({set_left_op, Left, Level}, _From, State) ->
    {reply, ok, set_left(State, Level, Left, dummy_left_key)};

handle_call({set_right_op, Right, Level}, _From, State) ->
    {reply, ok, set_right(State, Level, Right, dummy_right_key)};

handle_call({set_type_op, Type}, _From, State) ->
    {reply, ok, State#state{type=Type}};

handle_call({set_range_op, MinKey, MaxKey}, _From, State) ->
    {reply, ok, State#state{min_key=MinKey, max_key=MaxKey}};

handle_call({set_max_key_op, MaxKey}, _From, State) ->
    {reply, ok, State#state{max_key=MaxKey}};

handle_call({set_min_key_op, MinKey}, _From, State) ->
    {reply, ok, State#state{min_key=MinKey}};

handle_call({set_gen_mvector_op, Fun}, _From, State) ->
    {reply, ok, State#state{gen_mvector=Fun}};

handle_call(get_type_op, _From, State) ->
    {reply, State#state.type, State};

handle_call(take_largest_op, _From, State) ->
    {Key, Value, NewStore} = mio_store:take_largest(State#state.store),
    {reply, {Key, Value}, State#state{store=NewStore}};

handle_call(get_largest_op, _From, State) ->
    {reply, mio_store:largest(State#state.store), State};

handle_call(get_smallest_op, _From, State) ->
    {reply, mio_store:smallest(State#state.store), State};

handle_call({just_insert_op, Key, Value}, _From, State) ->
    case mio_store:set(Key, Value, State#state.store) of
        {overflow, NewStore} ->
            {reply, overflow, State#state{store=NewStore}};
        {full, NewStore} ->
            {reply, full, State#state{store=NewStore}};
        NewStore ->
            {reply, ok, State#state{store=NewStore}}
    end;

handle_call({insert_op, Key, Value}, From, State) ->
    Self = self(),
    spawn_link(?MODULE, insert_op_call, [From, State, Self, Key, Value]),
    {noreply, State};

handle_call(get_range_op, _From, State) ->
    {reply, {State#state.min_key, State#state.max_key}, State};


%% Read Only Operations start
handle_call({search_op, Key, Level}, From, State) ->
    Self = self(),
    spawn_link(?MODULE, search_op_call, [From, State, Self, Key, Level]),
    {noreply, State};

handle_call(get_op, From, State) ->
    spawn_link(?MODULE, get_op_call, [From, State]),
    {noreply, State};

%% Returns insert is done?
handle_call(get_inserted_op, _From, State) ->
    {reply, lists:all(fun(X) -> X end, State#state.inserted), State};

handle_call(get_deleted_op, _From, State) ->
    {reply, State#state.deleted, State};

handle_call({sg_get_right_op, Level}, From, State) ->
    spawn_link(?MODULE, get_neighbor_op_call, [From, State, right, Level]),
    {noreply, State};

handle_call({sg_get_left_op, Level}, From, State) ->
    spawn_link(?MODULE, get_neighbor_op_call, [From, State, left, Level]),
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

handle_call({set_op, NewValue}, _From, State) ->
    set_op_call(State, NewValue);

handle_call({link_right_op, Level, RightNode, RightKey}, _From, State) ->
    {reply, ok, set_right(State, Level, RightNode, RightKey)};

handle_call({link_left_op, Level, LeftNode, LeftKey}, _From, State) ->
    {reply, ok, set_left(State, Level, LeftNode, LeftKey)};

handle_call({set_expire_time_op, ExpireTime}, _From, State) ->
    {reply, ok, State#state{expire_time=ExpireTime}};

handle_call(set_deleted_op, _From, State) ->
    {reply, ok, State#state{deleted=true}};


handle_call({set_inserted_op, Level}, _From, State) ->
    {reply, ok, State#state{inserted=mio_util:lists_set_nth(Level + 1, true, State#state.inserted)}};

handle_call(set_inserted_op, _From, State) ->
    {reply, ok, State#state{inserted=lists:duplicate(length(State#state.inserted) + 1, true)}}.

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
%%     MyValue = State#state.value,
%%     MyMVector = State#state.membership_vector,
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
%%     MyValue = State#state.value,
%%     MyExpireTime = State#state.expire_time,
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

node_on_level(Nodes, Level) ->
    case Nodes of
        [] -> [];
        _ ->  lists:nth(Level + 1, Nodes) %% Erlang array is 1 origin.
    end.

neighbor_node(State, Direction, Level) ->
    case Direction of
        right ->
            node_on_level(State#state.right, Level);
        left ->
            node_on_level(State#state.left, Level)
    end.

reverse_direction(Direction) ->
    case Direction of
        right ->
             left;
        left ->
            right
    end.

neighbor_key(State, Direction, Level) ->
    case Direction of
        right ->
            node_on_level(State#state.right_keys, Level);
        left ->
            node_on_level(State#state.left_keys, Level)
    end.

set_right(State, Level, Node, Key) ->
    NewState = State#state{right_keys=mio_util:lists_set_nth(Level + 1, Key, State#state.right_keys)},
    NewState#state{right=mio_util:lists_set_nth(Level + 1, Node, NewState#state.right)}.

set_left(State, Level, Node, Key) ->
    NewState = State#state{left_keys=mio_util:lists_set_nth(Level + 1, Key, State#state.left_keys)},
    NewState#state{left=mio_util:lists_set_nth(Level + 1, Node, NewState#state.left)}.

%%--------------------------------------------------------------------
%%% Implementation of genserver:call
%%--------------------------------------------------------------------
get_op_call(From, State) ->
    gen_server:reply(From, {my_key(State), dummy_value_todo, State#state.membership_vector, State#state.left, State#state.right}).
get_neighbor_op_call(From, State, Direction, Level) ->
    NeighborNode = neighbor_node(State, Direction, Level),
    NeighborKey = neighbor_key(State, Direction, Level),
    gen_server:reply(From, {NeighborNode, NeighborKey}).

set_op_call(State, NewValue) ->
    {reply, dummy_todo, State}.

buddy_op_call(From, State, Self, MembershipVector, Direction, Level) ->
    IsSameMV = mio_mvector:eq(Level, MembershipVector, State#state.membership_vector),
    %% N.B.
    %%   We have to check whether this node is inserted on this Level, if not this node can't be buddy.
    IsInserted = node_on_level(State#state.inserted, Level),
    if
        IsSameMV andalso IsInserted ->
            MyKey = my_key(State),
            ReverseDirection = reverse_direction(Direction),
            MyNeighborKey = neighbor_key(State, ReverseDirection, Level),
            MyNeighbor = neighbor_node(State, ReverseDirection, Level),
            gen_server:reply(From, {ok, Self, MyKey, MyNeighbor, MyNeighborKey});
        true ->
            case neighbor_node(State, Direction, Level - 1) of %% N.B. should be on LowerLevel
                [] ->
                    gen_server:reply(From, not_found);
                NeighborNode ->
                    gen_server:reply(From, buddy_op(NeighborNode, MembershipVector, Direction, Level))
            end
    end.

%%--------------------------------------------------------------------
%%  Search operation
%%    Search operation never change the State
%%--------------------------------------------------------------------
search_op_call(From, State, Self, Key, Level) ->
    SearchLevel = case Level of
                      [] ->
                          length(State#state.right) - 1; %% Level is 0 origin
                      _ -> Level
                  end,
    MyKey = my_key(State),
    Found = string:equal(MyKey, Key),
    if
        %% Found!
        Found ->
            MyValue = dummy_todo,
            MyExpireTime = State#state.expire_time,
            gen_server:reply(From, {Self, MyKey, MyValue, MyExpireTime});
        MyKey < Key ->
            io:format("~p~n", [?LINE]),
            do_search(From, Self, State, right, fun(X, Y) -> X =< Y end, SearchLevel, Key);
        true ->
            io:format("~p~n", [?LINE]),
            do_search(From, Self, State, left, fun(X, Y) -> X >= Y end, SearchLevel, Key)
    end.

%% Not Found.
do_search(From, Self, State, _Direction, _CompareFun, Level, _Key) when Level < 0 ->
            io:format("~p~n", [?LINE]),
    MyKey = my_key(State),
    MyValue = dummy_todo,
    MyExpireTime = State#state.expire_time,
    gen_server:reply(From, {Self, MyKey, MyValue, MyExpireTime});
do_search(From, Self, State, Direction, CompareFun, Level, Key) ->
    case neighbor_node(State, Direction, Level) of
        [] ->
            io:format("~p Level=~p~n", [?LINE, Level]),
            do_search(From, Self, State, Direction, CompareFun, Level - 1, Key);
        NextNode ->
            NextKey = neighbor_key(State, Direction, Level),
            io:format("~p ~p ~p~n", [?LINE, NextKey, Key]),
            case CompareFun(NextKey, Key) of
                true ->
            io:format("~p~n", [?LINE]),
                    gen_server:reply(From, gen_server:call(NextNode, {search_op, Key, Level}, infinity));
                _ ->
            io:format("~p~n", [?LINE]),
                    do_search(From, Self, State, Direction, CompareFun, Level - 1, Key)
            end
    end.

%%--------------------------------------------------------------------
%%  delete operation
%%--------------------------------------------------------------------
delete_op_call(From, Self, State) ->
    LockedNodes = lock_or_exit([Self], ?LINE, [my_key(State)]),
    IsDeleted = gen_server:call(Self, get_deleted_op),
    if IsDeleted ->
            %% already deleted.
            unlock(LockedNodes, ?LINE),
            gen_server:reply(From, ok);
       true ->
            case gen_server:call(Self, get_inserted_op) of
                true ->
                    MaxLevel = length(State#state.membership_vector),
                    %% My State will not be changed, since I will be killed soon.
                    gen_server:call(Self, set_deleted_op),
                    %% N.B.
                    %% To prevent deadlock, we unlock the Self after deleted mark is set.
                    %% In delete_loop, Self will be locked/unlocked with left/right nodes on each level for the same reason.
                    unlock(LockedNodes, ?LINE),
                    delete_loop_(Self, MaxLevel),
                    gen_server:reply(From, ok);
                _ ->
                    unlock(LockedNodes, ?LINE),
                    %% Not inserted yet, wait.
                    mio_util:random_sleep(0),
                    ?INFO("not inserted yet. waiting ..."),
                    delete_op_call(From, Self, State)
            end
    end.

delete_loop_(_Self, Level) when Level < 0 ->
    [];
delete_loop_(Self, Level) ->
    {RightNode, RightKey} = gen_server:call(Self, {sg_get_right_op, Level}),
    {LeftNode, LeftKey}  = gen_server:call(Self, {sg_get_left_op, Level}),
    LockedNodes = lock_or_exit([RightNode, Self, LeftNode], ?LINE, [LeftKey, RightKey]),

    ?CHECK_SANITY(Self, Level),

    ok = link_left_op(RightNode, Level, LeftNode, LeftKey),
    ok = link_right_op(LeftNode, Level, RightNode, RightKey),

    ?CHECK_SANITY(Self, Level),
    unlock(LockedNodes, ?LINE),
    %% N.B.
    %% We keep the right/left node of Self, since it may be still located on search path.
    delete_loop_(Self, Level - 1).


lock(Nodes, infinity, _Line) ->
    mio_lock:lock(Nodes, infinity);
lock(Nodes, 100, Line) ->
    ?FATALF("mio_node:lock dead lock Nodes=~p at mio_node:~p", [Nodes, Line]),
    false;
lock(Nodes, Times, Line) ->
    case mio_lock:lock(Nodes) of
        true ->
            true;
        false ->
            ?INFOF("Lock NG Sleeping ~p~n", [Nodes]),
            dynomite_prof:start_prof(lock_sleep),
            mio_util:random_sleep(Times),
            dynomite_prof:stop_prof(lock_sleep),
            lock(Nodes, Times + 1, Line)
    end.

lock(Nodes, Line) ->
    lock(Nodes, 0, Line).

unlock(Nodes, _Line) ->
%%    ?INFOF("unlocked ~p at ~p:~p", [Nodes, Line, self()]),
    mio_lock:unlock(Nodes).

lock_or_exit(Nodes, Line, Info) ->
    IsLocked = lock(Nodes, Line),
    if not IsLocked ->
            ?FATALF("~p:~p <~p> lock failed~n", [?MODULE, Line, Info]),
            exit(lock_failed);
       true ->
            Nodes
    end.

link_three_nodes({LeftNode, LeftKey}, {CenterNode, CenterKey}, {RightNode, RightKey}, Level) ->
    %% [Left] -> [Center]  [Right]
    link_right_op(LeftNode, Level, CenterNode, CenterKey),

    %% [Left]    [Center] <- [Right]
    link_left_op(RightNode, Level, CenterNode, CenterKey),

    %% [Left] <- [Center]    [Right]
    link_left_op(CenterNode, Level, LeftNode, LeftKey),

    %% [Left]    [Center] -> [Right]
    link_right_op(CenterNode, Level, RightNode, RightKey).

%%--------------------------------------------------------------------
%%  Insert operation
%%--------------------------------------------------------------------
insert_op_call(From, _State, Self, Introducer) when Introducer =:= Self->
    %% I am alone.
    gen_server:call(Self, set_inserted_op),
    gen_server:reply(From, ok);
insert_op_call(From, State, Self, Introducer) ->
    dynomite_prof:start_prof(insert_op_call),
    %% At first, we lock the Self not to be deleted.
    %% => This causes dead lock, since Self will never be released to others.
    %%    We use inserted_op instead in order to prevent deletion.
    %% LockedNodes = lock_or_exit([Self], ?LINE, MyKey),
    dynomite_prof:start_prof(link_on_level_0),
    case link_on_level_0(From, State, Self, Introducer) of
        no_more ->
            dynomite_prof:stop_prof(link_on_level_0),
            gen_server:call(Self, set_inserted_op),
            ?CHECK_SANITY(Self, 0);
        _ ->
            gen_server:call(Self, {set_inserted_op, 0}),

            ?CHECK_SANITY(Self, 0),
            %% link on level > 0
            MaxLevel = length(State#state.membership_vector),
            dynomite_prof:stop_prof(link_on_level_0),
            link_on_level_ge1(Self, MaxLevel)
    end,
    dynomite_prof:stop_prof(insert_op_call),
    gen_server:reply(From, ok).


link_on_level_0(From, State, Self, Introducer) ->
    MyKey = my_key(State),
    {Neighbor, NeighborKey, _, _} = search_op(Introducer, MyKey),

    IsSameKey = string:equal(NeighborKey, MyKey),
    if
        %% MyKey is already exists
        IsSameKey ->
            try_overwrite_value(From, State, Self, Neighbor, Introducer);
        %% insert!
        true ->
            do_link_on_level_0(From, State, Self, Neighbor, NeighborKey, Introducer)
    end.

try_overwrite_value(From, State, Self, Neighbor, Introducer) ->
    Value = dummy_todo,
    %% Since this process doesn't have any other lock, dead lock will never happen.
    %% Just wait infinity.
    lock([Neighbor], infinity, ?LINE),

    IsDeleted = gen_server:call(Neighbor, get_deleted_op),
    if IsDeleted ->
            %% Retry
            ?INFO("link_on_level_0: Neighbor deleted "),
            unlock([Neighbor], ?LINE),
            mio_util:random_sleep(0),
            link_on_level_0(From, State, Self, Introducer);
       true ->
            %% overwrite the value
            ok = gen_server:call(Neighbor, {set_op, Value}),
            unlock([Neighbor], ?LINE),
            %% tell the callee, link_on_level_ge1 is not necessary
            no_more
    end.

%% [Neighbor] <-> [NodeToInsert] <-> [NeigborRight]
do_link_on_level_0(From, State, Self, Neighbor, NeighborKey, Introducer) ->
    case NeighborKey < my_key(State) of
        true ->
            do_link_on_level_0(From, State, Self, Neighbor, NeighborKey, Introducer, sg_get_right_op, check_invariant_level_0_left);
        _ ->
            %% [NeighborLeft] <-> [NodeToInsert] <-> [Neigbor]
            do_link_on_level_0(From, State, Self, Neighbor, NeighborKey, Introducer, sg_get_left_op, check_invariant_level_0_right)
    end.

do_link_on_level_0(From, State, Self, Neighbor, NeighborKey, Introducer, GetNeighborOp, CheckInvariantFun) ->
    dynomite_prof:start_prof(do_link_on_level_0),
    MyKey = my_key(State),
    dynomite_prof:start_prof(do_link_on_level_0_hoge),
    %% Lock 3 nodes [Neighbor], [NodeToInsert] and [NeighborRightOrLeft]
    {NeighborRightOrLeft, _} = gen_server:call(Neighbor, {GetNeighborOp, 0}),
    dynomite_prof:stop_prof(do_link_on_level_0_hoge),
    dynomite_prof:start_prof(do_link_on_level_0_hoge1),

    LockedNodes = lock_or_exit([Neighbor, Self, NeighborRightOrLeft], ?LINE, MyKey),
    dynomite_prof:stop_prof(do_link_on_level_0_hoge1),

    dynomite_prof:start_prof(do_link_on_level_0_hoge2),

    %% After locked 3 nodes, check invariants.
    %% invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    {RealNeighborRightOrLeft, RealNeighborRightOrLeftKey} = gen_server:call(Neighbor, {GetNeighborOp, 0}),
    dynomite_prof:stop_prof(do_link_on_level_0_hoge2),
    dynomite_prof:start_prof(do_link_on_level_0_invari),
    case apply(?MODULE, CheckInvariantFun, [MyKey, Neighbor, NeighborRightOrLeft, RealNeighborRightOrLeft, RealNeighborRightOrLeftKey]) of
        retry ->
            dynomite_prof:stop_prof(do_link_on_level_0_invari),
            unlock(LockedNodes, ?LINE),
            dynomite_prof:stop_prof(do_link_on_level_0),
            link_on_level_0(From, State, Self, Introducer);
        ok ->
            dynomite_prof:stop_prof(do_link_on_level_0_invari),
            case GetNeighborOp of
                get_right_op ->
                    link_three_nodes({Neighbor, NeighborKey}, {Self, MyKey}, {RealNeighborRightOrLeft, RealNeighborRightOrLeftKey}, 0);
                _ ->
                    link_three_nodes({RealNeighborRightOrLeft, RealNeighborRightOrLeftKey}, {Self, MyKey}, {Neighbor, NeighborKey}, 0)
            end,
            unlock(LockedNodes, ?LINE),
            dynomite_prof:stop_prof(do_link_on_level_0),
            need_link_on_level_ge1
    end.

%% [Neighbor] <=> [Self] <=> [NeighborRight]
check_invariant_level_0_left(MyKey, Neighbor, NeighborRight, RealNeighborRight, RealNeighborRightKey) ->
    check_invariant_level_0(MyKey, Neighbor, NeighborRight, RealNeighborRight, RealNeighborRightKey, fun(X, Y) -> X >= Y end).

%% [NeighborLeft] <=> [Self] <=> [Neighbor]
check_invariant_level_0_right(MyKey, Neighbor, NeighborLeft, RealNeighborLeft, RealNeighborLeftKey) ->
    check_invariant_level_0(MyKey, Neighbor, NeighborLeft, RealNeighborLeft, RealNeighborLeftKey, fun(X, Y) -> X =< Y end).

%% N.B.
%%   callee shoudl lock all nodes
%%
%% Returns
%%   retry: You should retry link_on_level_ge1 on same level.
%%   ok: invariant is satified, you can link safely on this level.
check_invariant_level_0(MyKey, Neighbor, NeighborOfNeighbor, RealNeighborOfNeighbor, RealNeighborOfNeighborKey, CompareFun) ->
    %% After locked 3 nodes, check invariants.
    %% invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    %%   NeighborOfNeighbor == RealNeighborOfNeighbor
    %%   Neighbor->rightKey < MyKey (sanity check)
    IsInvalidOrder = (RealNeighborOfNeighborKey =/= [] andalso CompareFun(MyKey, RealNeighborOfNeighborKey)),
    IsNeighborChanged = (NeighborOfNeighbor =/= RealNeighborOfNeighbor),

    if IsInvalidOrder orelse IsNeighborChanged
       ->
            %% Retry: another key is inserted
            ?INFOF("RETRY: check_invariant_level_0 MyKey=~p RealNeighborOfNeighborKey=~p", [MyKey, RealNeighborOfNeighborKey]),
            retry;
       true ->
            IsDeleted =
                (Neighbor =/= [] andalso gen_server:call(Neighbor, get_deleted_op))
                orelse
                (NeighborOfNeighbor =/= [] andalso gen_server:call(NeighborOfNeighbor, get_deleted_op)),
            if IsDeleted ->
                    ?INFO("RETRY: check_invariant_level_0 neighbor deleted"),
                    retry;
               true ->
                    ok
            end
    end.

buddy_op_proxy([], [], _MyMV, _Level) ->
    not_found;
buddy_op_proxy(LeftOnLower, [], MyMV, Level) ->
    case buddy_op(LeftOnLower, MyMV, left, Level) of
        not_found ->
            not_found;
        {ok, Buddy, BuddyKey, BuddyRight, BuddyRightKey} ->
            {ok, left, Buddy, BuddyKey, BuddyRight, BuddyRightKey}
    end;
buddy_op_proxy([], RightOnLower, MyMV, Level) ->
    case buddy_op(RightOnLower, MyMV, right, Level) of
        not_found ->
            not_found;
        {ok, Buddy, BuddyKey, BuddyLeft, BuddyLeftKey} ->
            {ok, right, Buddy, BuddyKey, BuddyLeft, BuddyLeftKey}
    end;
buddy_op_proxy(LeftOnLower, RightOnLower, MyMV, Level) ->
    case buddy_op_proxy(LeftOnLower, [], MyMV, Level) of
        not_found ->
            buddy_op_proxy([], RightOnLower, MyMV, Level);
        Other ->
            Other
    end.

%% link on Level >= 1
link_on_level_ge1(Self, MaxLevel) ->
    dynomite_prof:start_prof(link_on_level_ge1_mino),
    link_on_level_ge1(Self, 1, MaxLevel),
    dynomite_prof:stop_prof(link_on_level_ge1_mino).

%% Link on all levels done.
link_on_level_ge1(_Self, Level, MaxLevel) when Level > MaxLevel ->
    [];
%% Find buddy node and link it.
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
    {MyKey, _MyValue, MyMV, MyLeft, MyRight} = gen_server:call(Self, get_op),
    LeftOnLower = node_on_level(MyLeft, Level - 1),
    RightOnLower = node_on_level(MyRight, Level - 1),
    case buddy_op_proxy(LeftOnLower, RightOnLower, MyMV, Level) of
        not_found ->
            %% We have no buddy on this level.
            %% On higher Level, we have no buddy also.
            %% So we've done.
            gen_server:call(Self, set_inserted_op),
            ?CHECK_SANITY(Self, Level),
            dynomite_prof:stop_prof(link_on_level_ge1),
            [];
        %% [Buddy] <-> [NodeToInsert] <-> [BuddyRight]
        {ok, left, Buddy, BuddyKey, BuddyRight, BuddyRightKey} ->
            dynomite_prof:start_prof(link_on_level_ge1),
            do_link_level_ge1(Self, MyKey, Buddy, BuddyKey, BuddyRight, BuddyRightKey, Level, MaxLevel, left, check_invariant_level_ge1_left),
            ?CHECK_SANITY(Self, Level),
            dynomite_prof:stop_prof(link_on_level_ge1);
        %% [BuddyLeft] <-> [NodeToInsert] <-> [Buddy]
        {ok, right, Buddy, BuddyKey, BuddyLeft, BuddyLeftKey} ->
            dynomite_prof:start_prof(link_on_level_ge1),
            do_link_level_ge1(Self, MyKey, Buddy, BuddyKey, BuddyLeft, BuddyLeftKey, Level, MaxLevel, right, check_invariant_level_ge1_right),
            ?CHECK_SANITY(Self, Level),
            dynomite_prof:stop_prof(link_on_level_ge1)
    end.

do_link_level_ge1(Self, MyKey, Buddy, BuddyKey, BuddyNeighbor, BuddyNeighborKey, Level, MaxLevel, Direction, CheckInvariantFun) ->
    dynomite_prof:start_prof(do_link_level_ge1_lock),
    LockedNodes = lock_or_exit([Self, BuddyNeighbor, Buddy], ?LINE, MyKey),
    dynomite_prof:stop_prof(do_link_level_ge1_lock),

    case apply(?MODULE, CheckInvariantFun, [Level, MyKey, Buddy, BuddyKey, BuddyNeighbor]) of
        retry ->
            dynomite_prof:start_prof(do_link_level_ge1_retry),
            unlock(LockedNodes, ?LINE),
            mio_util:random_sleep(0),
            dynomite_prof:stop_prof(do_link_level_ge1_retry),
            link_on_level_ge1(Self, Level, MaxLevel);
        done ->
            gen_server:call(Self, set_inserted_op),
            unlock(LockedNodes, ?LINE),
            ?CHECK_SANITY(Self, Level);
        ok ->

            case Direction of
                right ->
                    link_three_nodes({BuddyNeighbor, BuddyNeighborKey}, {Self, MyKey}, {Buddy, BuddyKey}, Level);
                left ->
                    link_three_nodes({Buddy, BuddyKey}, {Self, MyKey}, {BuddyNeighbor, BuddyNeighborKey}, Level)
            end,
            gen_server:call(Self, {set_inserted_op, Level}),
            unlock(LockedNodes, ?LINE),
            ?CHECK_SANITY(Self, Level),
            %% Go up to next Level.
            link_on_level_ge1(Self, Level + 1, MaxLevel)
    end.

%% callee should lock all nodes
%% Returns
%%   retry: You should retry link_on_level_ge1 on same level.
%%   done: link process is done. link on higher level is not necessary.
%%   ok: invariant is satified, you can link safely on this level.
check_invariant_level_ge1_left(Level, MyKey, Buddy, BuddyKey, BuddyRight) ->
    check_invariant_ge1(Level, MyKey, Buddy, BuddyKey, BuddyRight, sg_get_right_op, fun(X, Y) -> X > Y end).
check_invariant_level_ge1_right(Level, MyKey, Buddy, BuddyKey, BuddyLeft) ->
    check_invariant_ge1(Level, MyKey, Buddy, BuddyKey, BuddyLeft, sg_get_left_op, fun(X, Y) -> X < Y end).

check_invariant_ge1(Level, MyKey, Buddy, BuddyKey, BuddyNeighbor, GetNeighborOp, CompareFun) ->
    {RealBuddyNeighbor, RealBuddyNeighborKey} = gen_server:call(Buddy, {GetNeighborOp, Level}),
    IsSameKey = RealBuddyNeighborKey =/= [] andalso string:equal(MyKey, RealBuddyNeighborKey),
    IsInvalidOrder = (RealBuddyNeighborKey =/= [] andalso CompareFun(MyKey, RealBuddyNeighborKey)),
    IsNeighborChanged = (RealBuddyNeighbor =/= BuddyNeighbor),
    %% Invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    if
        %% done: other process insert on higher level, so we have nothing to do.
        IsSameKey ->
            done;
        %% retry: another key is inserted
        IsInvalidOrder orelse IsNeighborChanged ->
            ?INFOF("RETRY: check_invariant_ge1 Level=~p ~p ~p", [Level, [RealBuddyNeighbor, BuddyNeighbor], [MyKey, BuddyKey, RealBuddyNeighborKey]]),
            retry;
        true->
            IsDeleted =
                (Buddy =/= [] andalso gen_server:call(Buddy, get_deleted_op))
                orelse
                  (BuddyNeighbor =/= [] andalso gen_server:call(BuddyNeighbor, get_deleted_op)),
            if IsDeleted ->
                    ?INFO("RETRY: check_invariant_ge1 Neighbor deleted"),
                    retry;
               true ->
                    ok
            end
    end.
