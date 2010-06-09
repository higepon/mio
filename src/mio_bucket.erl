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
         delete_op/2,
         get_range/1, my_key/1,
         get_range_values_op/4,
         get_left_op/1, get_right_op/1,
         get_left_op/2, get_right_op/2,
         insert_op/3, just_insert_op/3,
         get_type_op/1, set_type_op/2,
         is_empty_op/1, is_full_op/1,
         take_largest_op/1, take_smallest_op/1,
         get_largest_op/1, get_smallest_op/1,
         insert_op_call/5,
         delete_op_call/4,
         get_range_op/1, set_range_op/3,
         set_max_key_op/3, set_min_key_op/3,
         %% Skip Graph layer
         get_op/2,
         get_op_call/2,
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
%%    following bucket group appears on deletion.
%%
%%      O, O*, C-O, C-O-C, C-O* and C-O*-C.
%%
%%    (a) O1
%%      O1 -> O2 or O2*
%%        Range never change.
%%
%%    (b) O*
%%      O*
%%        Range never change.
%%
%%    (c) C1-O2
%%      Deletion from C1: C1'-O2'
%%        C1'->max = {moved key, true}.
%%        O2'->min = {moved key, false}.
%%      Deletion from O2: C1-O2'
%%        Range never change.
%%
%%    (d) C1-O2-C3
%%      Deletion from C1: C1'-O2'-C3
%%        C1'->max = {moved key, true}.
%%        O2'->min = {moved key, false}.
%%      Deletion from O2: C1-O2'-C3
%%        Range never change.
%%      Deletion from C3: C1-O2'-C3'
%%        O2'->max = {moved key, false}.
%%        C3'->min = {moved key, true}.
%%
%%    (e) C1-O2*-C3
%%      Deletion from C1: C1'-O3'
%%        C1'->max = {moved key, true}.
%%        O3'->min = {moved key, false}.
%%      Deletion from C3: C1-O3'
%%        C1->max = not(C3'->min)
%%
%%    (f) C1-O2*
%%      Both left and right not exist: O1
%%        O1->max = O2*->max
%%      C-O exists on left or right: C3-O4 | C1'-O2* or C1'-O2* | C5-O6
%%        C1'->min = {moved_key, true}
%%        O4->max = {moved_key, false}
%%                or
%%        C1'->max = {moved_key, true}
%%        C5->min = {moved_key, false}
%%        C5->max = {moved_key2, true}
%%        O6->minx = {moved_key2, false}
%%      C-O-C exists on left or right: C3-O4-C5 | C1'-O2* or C1'-O2* | C6-O7-C8
%%        C1'->min = {moved_key, true}
%%        C5->max = {moved_key, false}
%%        C5->min = {moved_key, true}
%%        O4->max = {moved_key, false}
%%                or
%%        C1'->max = {moved_key, true}
%%        C6->min = {moved_key, false}
%%        C6->max = {moved_key2, true}
%%        O7->min = {moved_key2, false}
%%      C4-O5* exists on left or right: C1'-O3
%%        C1'->max = O2*->max
%%        C3->max = C1->min
%%                or
%%        C3->max = O4*->max
%%        C3->min = {moved_key, false}
%%        C1->max = {moved_key, true}
%%      C-O*-C exists on left or right: C1'-O3-C4
%%        C1->max = {moved_key, true}
%%        O3->min = {moved_key, false}
%%        C3->max = not(C5)
%%                or
%%        C1->max = O2*->max
%%        C1->min = {moved_key, true}
%%        C5->max = {moved_key, false}
%%        C5->min = not(C3->max)
%%
%%====================================================================
%% API
%%====================================================================
set_gen_mvector_op(Bucket, Fun) ->
    gen_server:call(Bucket, {set_gen_mvector_op, Fun}).

get_range_values_op(Bucket, Key1, Key2, Limit) ->
    gen_server:call(Bucket, {get_range_values_op, Key1, Key2, Limit}).

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

delete_op(Bucket, Key) ->
    gen_server:call(Bucket, {delete_op, Key}).

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

take_smallest_op(Bucket) ->
    gen_server:call(Bucket, take_smallest_op).

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
delete_from_alone(State, Key) ->
    mio_store:remove(Key, State#node.store),
    {ok, false}.

delete_from_c_O_l_no_neibor(Self, RightBucket) ->
    %% unlink
    mio_skip_graph:link_right_op(Self, 0, []),
    set_type_op(Self, alone),
    {_, {MaxKey, EncompassMax}} = get_range_op(RightBucket),
    set_max_key_op(Self, MaxKey, EncompassMax),
    {ok, RightBucket}.


delete_from_c_O_l(Self, RightBucket) ->
    case {get_left_op(Self), get_right_op(RightBucket)} of
        %% C1-O2*
        %%   Both left and right not exist: O1
        {[], []}->
            delete_from_c_O_l_no_neibor(Self, RightBucket);
        {Left, Right} ->
            case get_type_op(Left) of
                c_o_r ->
                    case take_largest_op(Left) of
                        %% C-O* exists on left
                        none ->
                            {C1, O2, C3, O4, O4Right} = {get_left_op(Left), Left, Self, RightBucket, get_right_op(RightBucket)},
                            {_, {O4RightMaxKey, O4RightMaxKeyEncompass}} = get_range_op(O4Right),
                            mio_skip_graph:link_right_op(C3, 0, O4Right),
                            mio_skip_graph:link_left_op(C3, 0, C1),
                            mio_skip_graph:link_right_op(C1, 0, C3),

                            {C3MinKey, _} = get_smallest_op(C3),
                            set_max_key_op(C1, C3MinKey, false),
                            set_range_op(C3, {C3MinKey, true}, {O4RightMaxKey, O4RightMaxKeyEncompass}),
                            set_type_op(C3, c_o_r),
                            {ok, [O2, O4]};
                        %% C-O exists on left
                        {MaxKey, MaxValue} ->
                            just_insert_op(Self, MaxKey, MaxValue),
                            set_min_key_op(Self, MaxKey, true),
                            set_max_key_op(Left, MaxKey, false),
                            {ok, false}
                    end;
                %% C-O-C exists on left
                c_o_c_r ->
                    %% C1-O2-C3 | C4-O5*
                    {O2, C3, C4} = {get_left_op(Left), Left, Self},
                    case is_empty_op(O2) of
                        true ->
                            C1 = get_left_op(O2),
                            O5 = RightBucket,
                            O5Right = Right,
                            {C3MaxKey, C3MaxValue} = take_largest_op(C3),
                            just_insert_op(C4, C3MaxKey, C3MaxValue),

                            {_, {O5MaxKey, O5MaxEncompass}} = get_range_op(O5),
                            {_, {O2MaxKey, O2MaxEncompass}} = get_range_op(O2),
                            set_max_key_op(C1, O2MaxKey, O2MaxEncompass),
                            set_range_op(C3, {O2MaxKey, not O2MaxEncompass}, {C3MaxKey, false}),
                            set_range_op(C4, {C3MaxKey, true}, {O5MaxKey, O5MaxEncompass}),
                            mio_skip_graph:link_right_op(C1, 0, C3),
                            mio_skip_graph:link_left_op(C3, 0, C1),
                            mio_skip_graph:link_right_op(C3, 0, C4),
                            mio_skip_graph:link_left_op(C4, 0, C3),
                            mio_skip_graph:link_right_op(C4, 0, O5Right),
                            set_type_op(C3, c_o_c_m),
                            set_type_op(C4, c_o_c_r),
                            {ok, [O2, O5]};
                        false ->
                            {C3MaxKey, C3MaxValue} = take_largest_op(C3),
                            just_insert_op(C4, C3MaxKey, C3MaxValue),
                            set_min_key_op(C4, C3MaxKey, true),

                            {O2MaxKey, O2MaxValue} = take_largest_op(O2),
                            just_insert_op(C3, O2MaxKey, O2MaxValue),
                            set_range_op(C3, {O2MaxKey, true}, {C3MaxKey, false}),

                            set_max_key_op(O2, O2MaxKey, false),
                            {ok, false}
                    end;
                _ ->
                    %% C-O exists on right
                    case get_type_op(Right) of
                        c_o_l ->
                            {C1, O2, C3, O4} = {Self, RightBucket, Right, get_right_op(Right)},
                            case is_empty_op(O4) of
                                %% C1-O2 | C3-O4*
                                true ->
                                    O4Right = get_right_op(O4),
                                    {_, {O4RightMaxKey, O4RightMaxKeyEncompass}} = get_range_op(O4Right),
                                    mio_skip_graph:link_right_op(C3, 0, O4Right),
                                    mio_skip_graph:link_left_op(C3, 0, C1),
                                    mio_skip_graph:link_right_op(C1, 0, C3),

                                    {C3MinKey, C3MinValue} = get_smallest_op(C3),
                                    just_insert_op(C1, C3MinKey, C3MinValue),
                                    set_max_key_op(C1, C3MinKey, true),
                                    set_range_op(C3, {C3MinKey, false}, {O4RightMaxKey, O4RightMaxKeyEncompass}),
                                    set_type_op(C3, c_o_r),
                                    {ok, [O2, O4]};
                                %% C1-O2* | C3-O4
                                false ->
                                    {C3MinKey, C3MinValue} = take_smallest_op(C3),
                                    just_insert_op(C1, C3MinKey, C3MinValue),
                                    set_max_key_op(C1, C3MinKey, true),

                                    {NewC3MinKey, _} = get_smallest_op(C3),
                                    set_range_op(O2, {C3MinKey, false}, {NewC3MinKey, false}),

                                    {C3MaxKey, C3MaxValue} = take_smallest_op(O4),
                                    just_insert_op(C3, C3MaxKey, C3MaxValue),
                                    set_range_op(C3, {NewC3MinKey, true}, {C3MaxKey, true}),

                                    set_min_key_op(O4, C3MaxKey, false),
                                    {ok, false}
                            end;
                        %% C-O-C exists on right
                        c_o_c_l ->
                            {C1, O2, C3, O4} = {Self, RightBucket, Right, get_right_op(Right)},
                            case is_empty_op(O4) of
                                %% C1-O2* | C3 O4* C5
                                true ->
                                    C5 = get_right_op(O4),
                                    {C3MinKey, C3MinValue} = take_smallest_op(C3),
                                    just_insert_op(C1, C3MinKey, C3MinValue),

                                    set_max_key_op(C1, C3MinKey, true),

                                    {_, {O4MaxKey, O4MaxEncompass}} = get_range_op(O4),
                                    set_range_op(C3, {C3MinKey, false}, {O4MaxKey, O4MaxEncompass}),
                                    mio_skip_graph:link_right_op(C1, 0, C3),
                                    mio_skip_graph:link_left_op(C3, 0, C1),
                                    mio_skip_graph:link_right_op(C3, 0, C5),
                                    mio_skip_graph:link_left_op(C5, 0, C3),
                                    set_type_op(C1, c_o_c_l),
                                    set_type_op(C3, c_o_c_m),
                                    {ok, [O2, O4]};
                                %% C1-O2* | C3 O4 C5
                                false ->
                                    {C3MinKey, C3MinValue} = take_smallest_op(C3),
                                    just_insert_op(C1, C3MinKey, C3MinValue),
                                    set_max_key_op(C1, C3MinKey, true),

                                    {NewC3MinKey, _} = get_smallest_op(C3),
                                    set_range_op(O2, {C3MinKey, false}, {NewC3MinKey, false}),

                                    {O4MinKey, O4MinValue} = take_smallest_op(O4),
                                    set_min_key_op(O4, O4MinKey, false),

                                    just_insert_op(C3, O4MinKey, O4MinValue),
                                    set_range_op(C3, {NewC3MinKey, true}, {O4MinKey, true}),
                                    {ok, false}
                                end;
                        _ ->
                            {ok, todo}
                    end
            end
    end.

delete_from_c_o_l(State, Self, Key) ->
    mio_store:remove(Key, State#node.store),
    RightBucket = get_right_op(Self),
    case take_smallest_op(RightBucket) of
        %% C1-O2*
        none ->
            delete_from_c_O_l(Self, RightBucket);
        %% C1-O2
        %%   Deletion from C1: C1'-O2'
        {MinKey, MinValue} ->
            just_insert_op(Self, MinKey, MinValue),
            set_max_key_op(Self, MinKey, true),
            set_min_key_op(RightBucket, MinKey, false),
            {ok, false}
    end.

delete_from_c_o_r(State, Key) ->
    mio_store:remove(Key, State#node.store),
    {ok, false}.

delete_from_c_o_c_l(State, Self, Key) ->
    mio_store:remove(Key, State#node.store),
    RightBucket = get_right_op(Self),
    case is_empty_op(RightBucket) of
        %% C1-O2*-C3
        %%   Deletion from C1: C1'-O3'
        true ->
            MostRightBucket = get_right_op(RightBucket),
            {MinKey, MinValue} = take_smallest_op(MostRightBucket),
            just_insert_op(Self, MinKey, MinValue),

            %% link
            mio_skip_graph:link_right_op(Self, 0, MostRightBucket),
            mio_skip_graph:link_left_op(MostRightBucket, 0, Self),

            %% type
            set_type_op(Self, c_o_l),
            set_type_op(MostRightBucket, c_o_r),

            set_max_key_op(Self, MinKey, true),
            set_min_key_op(MostRightBucket, MinKey, false),
            {ok, RightBucket};
        %% C1-O2-C3
        %%   Deletion from C1: C1'-O2'-C3
        false ->
            {MinKey, MinValue} = take_smallest_op(RightBucket),
            just_insert_op(Self, MinKey, MinValue),

            %% the inserted key becomes largest on C1
            set_max_key_op(Self, MinKey, true),
            set_min_key_op(RightBucket, MinKey, false),
            {ok, false}
    end.

delete_from_c_o_c_m(State, Key) ->
    mio_store:remove(Key, State#node.store),
    {ok, false}.

delete_from_c_o_c_r(State, Self, Key) ->
    mio_store:remove(Key, State#node.store),
    LeftBucket = get_left_op(Self),
    case is_empty_op(LeftBucket) of
        %% C1-O2*-C3
        %%   Deletion from C3: C1-O3'
        true ->
            MostLeftBucket = get_left_op(LeftBucket),
            {{MinKey, EncompassMin}, _} = get_range(State),
            set_max_key_op(MostLeftBucket, MinKey, not EncompassMin),

            %% link
            mio_skip_graph:link_right_op(MostLeftBucket, 0, Self),
            mio_skip_graph:link_left_op(Self, 0, MostLeftBucket),

            %% type
            set_type_op(MostLeftBucket, c_o_l),
            set_type_op(Self, c_o_r),

            {ok, LeftBucket};
        %% C1-O2-C3
        %%   Deletion from C3: C1-O2'-C3'
        false ->
            {MaxKey, MaxValue} = take_largest_op(LeftBucket),
            just_insert_op(Self, MaxKey, MaxValue),
            set_max_key_op(LeftBucket, MaxKey, false),
            set_min_key_op(Self, MaxKey, true),
            {ok, false}
    end.

%% pre-condition: Store has the key
delete(State, Self, Key) ->
    case State#node.type of
        %% O1
        %%   O1 -> O2 or O2*
        alone ->
            delete_from_alone(State, Key);
        c_o_l ->
            delete_from_c_o_l(State, Self, Key);
        %% C1-O2
        %%   Deletion from O2: C1-O2'
        c_o_r ->
            delete_from_c_o_r(State, Key);
        c_o_c_l ->
            delete_from_c_o_c_l(State, Self, Key);
        %% C1-O2-C3
        %%   Deletion from O2: C1-O2'-C3
        c_o_c_m ->
            delete_from_c_o_c_m(State, Key);
        c_o_c_r ->
            delete_from_c_o_c_r(State, Self, Key);
        _ ->
            {ok, todo}
    end.

delete_op_call(From, State, Self, Key) ->
    Ret = case mio_store:get(Key, State#node.store) of
              {ok, _Value} ->
                  delete(State, Self, Key);
              _ ->
                  {ok, false}
          end,
    gen_server:reply(From, Ret).

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
    gen_server:reply(From, {ok, NewlyAllocatedBucket}).

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

    mio_skip_graph:link_three_nodes(Left, EmptyBucket, Right, 0),
    ok = set_type_op(Right, c_o_c_r),
    ok = set_type_op(Left, c_o_c_l),
    %% link on Level >= 1
    mio_skip_graph:link_on_level_ge1(EmptyBucket, State),
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
    mio_skip_graph:link_right_op(Self, 0, EmptyBucket),
    mio_skip_graph:link_left_op(EmptyBucket, 0, Self),

    %% range partition
    set_max_key_op(Self, SelfMaxKey, true),
    set_range_op(EmptyBucket, {EmptyMinKey, false}, {EmptyMaxKey, State#node.encompass_max}),

    %% link on Level >= 1
    mio_skip_graph:link_on_level_ge1(EmptyBucket, State),
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
    mio_skip_graph:link_three_nodes(Right, EmptyBucket, PrevRight, 0).


%% Insertion to the Left causes overflow
split_c_o_c_by_l(State, Left, Middle, Right) ->
    {PrevRight, EmptyBucket} = prepare_split_c_o_c(State, Left, Middle, Right),

    {LargeRKey, LargeRValue} = take_largest_op(Right),
    ok = just_insert_op(EmptyBucket, LargeRKey, LargeRValue),
    {LargeMKey, LargeMValue} = take_largest_op(Middle),
    full = just_insert_op(Right, LargeMKey, LargeMValue),

    adjust_range_link_c_o_c(Left, Middle, Right, PrevRight, EmptyBucket),

    %% link on Level >= 1
    mio_skip_graph:link_on_level_ge1(EmptyBucket, State),
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
    mio_skip_graph:link_on_level_ge1(EmptyBucket, State),
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
    mio_skip_graph:link_three_nodes(Right, EmptyBucket, PrevRight, 0),

    %% link on Level >= 1
    mio_skip_graph:link_on_level_ge1(EmptyBucket, State),

    EmptyBucket.

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
handle_call({delete_op, Key}, From, State) ->
    Self = self(),
    spawn_link(?MODULE, delete_op_call, [From, State, Self, Key]),
    {noreply, State};

handle_call(stop_op, _From, State) ->
    {stop, normal, State};
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
    case mio_store:take_largest(State#node.store) of
        none ->
            {reply, none, State};
        {Key, Value, NewStore} ->
            {reply, {Key, Value}, State#node{store=NewStore}}
    end;

handle_call(take_smallest_op, _From, State) ->
    case mio_store:take_smallest(State#node.store) of
        none ->
            {reply, none, State};
        {Key, Value, NewStore} ->
            {reply, {Key, Value}, State#node{store=NewStore}}
    end;

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
    {reply, get_range(State), State};

handle_call({get_range_values_op, Key1, Key2, Limit}, _From, State) ->
    {reply, mio_store:get_range(Key1, Key2, Limit, State#node.store), State};

handle_call({skip_graph_search_op, SearchKey, Level}, From, State) ->
    Self = self(),
    spawn_link(mio_skip_graph, search_op_call, [From, State, Self, SearchKey, Level]),
    {noreply, State};

handle_call(get_op, From, State) ->
    spawn_link(?MODULE, get_op_call, [From, State]),
    {noreply, State};

handle_call({skip_graph_buddy_op, MembershipVector, Direction, Level}, From, State) ->
    Self = self(),
    spawn_link(mio_skip_graph, buddy_op_call, [From, State, Self, MembershipVector, Direction, Level]),
    {noreply, State};

handle_call({link_right_op, Level, RightNode}, _From, State) ->
    {reply, ok, set_right(State, Level, RightNode)};

handle_call({link_left_op, Level, LeftNode}, _From, State) ->
    {reply, ok, set_left(State, Level, LeftNode)}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------

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

node_on_level(Nodes, Level) ->
    case Nodes of
%%         [] -> [];
        _ ->  lists:nth(Level + 1, Nodes) %% Erlang array is 1 origin.
    end.

neighbor_node(State, Direction, Level) ->
    case Direction of
        right ->
            node_on_level(State#node.right, Level);
        left ->
            node_on_level(State#node.left, Level)
    end.

set_right(State, Level, Node) ->
    State#node{right=mio_util:lists_set_nth(Level + 1, Node, State#node.right)}.

set_left(State, Level, Node) ->
    State#node{left=mio_util:lists_set_nth(Level + 1, Node, State#node.left)}.

my_key(State) ->
    State#node.max_key.

get_op(Bucket, Key) ->
    gen_server:call(Bucket, {get_op, Key}).

get_range(State) ->
    {{State#node.min_key, State#node.encompass_min}, {State#node.max_key, State#node.encompass_max}}.
