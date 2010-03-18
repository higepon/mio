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
-behaviour(gen_server).

%% API
-export([start_link/1,
         get_op/2,
         get_left_op/1, get_right_op/1,
         set_left_op/2, set_right_op/2,
         insert_op/3, just_insert_op/3,
         get_type_op/1, set_type_op/2,
         is_empty_op/1, is_full_op/1,
         take_largest_op/1,
         insert_op_call/5
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {store, left, right, type}).

%% Known types
%%   O     : <alone>
%%   C-O   : <c_o_l>-<c_o_r>
%%   C-O-C : <c_o_c_l>-<c_o_c_m>-<c_o_c_r>

%%====================================================================
%%  Bucket layer
%%====================================================================
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
%%
%%    (c) C1-O2
%%      Insertion to C1 : C1'-O2'.
%%      Insertion to O2 : C1-O2'.
%%
%%    (d) C1-O2$
%%      Insertion to C1 : C1'-C2 -> C1'-O*-C2.
%%      Insertion to O2$ : C1-C2 -> C1-O*-C2
%%
%%    (e) C1-O2-C3
%%      Insertion to C1 : C1'-O2'-C3
%%      Insertion to C3 : C1-O2'-C3'
%%      Insertion to O2 : C1-O2 | C3'-O4
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
get_op(Bucket, Key) ->
    gen_server:call(Bucket, {get_op, Key}).

get_left_op(Bucket) ->
    gen_server:call(Bucket, get_left_op).

get_right_op(Bucket) ->
    gen_server:call(Bucket, get_right_op).

set_left_op(Bucket, Left) ->
    gen_server:call(Bucket, {set_left_op, Left}).

set_right_op(Bucket, Right) ->
    gen_server:call(Bucket, {set_right_op, Right}).

get_type_op(Bucket) ->
    gen_server:call(Bucket, get_type_op).

set_type_op(Bucket, Type) ->
    gen_server:call(Bucket, {set_type_op, Type}).

insert_op(Bucket, Key, Value) ->
    gen_server:call(Bucket, {insert_op, Key, Value}).

just_insert_op(Bucket, Key, Value) ->
    gen_server:call(Bucket, {just_insert_op, Key, Value}).

is_empty_op(Bucket) ->
    gen_server:call(Bucket, is_empty_op).

is_full_op(Bucket) ->
    gen_server:call(Bucket, is_full_op).

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
init([Capacity, Type]) ->
    {ok, #state{store=mio_store:new(Capacity),
                left=[],
                right=[],
                type=Type
               }}.

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
    case mio_store:get(Key, State#state.store) of
        none ->
            {reply, {error, not_found}, State};
        Value ->
            {reply, {ok, Value}, State}
    end;

handle_call(get_left_op, _From, State) ->
    {reply, State#state.left, State};

handle_call(get_right_op, _From, State) ->
    {reply, State#state.right, State};

handle_call({set_left_op, Left}, _From, State) ->
    {reply, ok, State#state{left=Left}};

handle_call({set_right_op, Right}, _From, State) ->
    {reply, ok, State#state{right=Right}};

handle_call({set_type_op, Type}, _From, State) ->
    {reply, ok, State#state{type=Type}};

handle_call(get_type_op, _From, State) ->
    {reply, State#state.type, State};

handle_call(take_largest_op, _From, State) ->
    {Key, Value, NewStore} = mio_store:take_largest(State#state.store),
    {reply, {Key, Value}, State#state{store=NewStore}};

handle_call({just_insert_op, Key, Value}, _From, State) ->
    case mio_store:set(Key, Value, State#state.store) of
        {overflow, NewStore} ->
            {reply, overflow, State#state{store=NewStore}};
        NewStore ->
            {reply, ok, State#state{store=NewStore}}
    end;

handle_call({insert_op, Key, Value}, From, State) ->
    Self = self(),
    spawn_link(?MODULE, insert_op_call, [From, State, Self, Key, Value]),
    {noreply, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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
link_op(Left, Right) ->
    ok = set_right_op(Left, Right),
    ok = set_left_op(Right, Left).

link3_op(Left, Middle, Right) ->
    link_op(Left, Middle),
    link_op(Middle, Right).

insert_op_call(From, State, Self, Key, Value)  ->
    case just_insert_op(Self, Key, Value) of
        overflow ->
            case State#state.type of
                c_o_l ->
                    Left = Self,
                    Right = State#state.right,
                    ?ASSERT_NOT_NIL(Right),
                    {LargeKey, LargeValue} = take_largest_op(Left),
                    ok = just_insert_op(Right, LargeKey, LargeValue),
                    case is_full_op(Right) of
                        true ->
                            %% C1-O2$ -> C1'-O*-C2
                            make_c_o_c(State, Left, Right);
                        _ ->
                            %% C1-O -> C1'-O'
                            []
                    end;
                %% Insertion to left c
                c_o_c_l ->
                    Left = Self,
                    Middle = State#state.right,
                    ?ASSERT_NOT_NIL(Middle),
                    {LargeKey, LargeValue} = take_largest_op(Left),
                    ok = just_insert_op(Middle, LargeKey, LargeValue),

                    case is_full_op(Middle) of
                        true ->
                            %%  C1-O2$-C3
                            %%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
                            split_c_o_c(State, Left, Middle, get_right_op(Middle));
                        _ ->
                            %% C1-O2-C3 -> C1'-O2'-C3
                            []
                    end;
                c_o_c_r ->
                    %% C1-O2-C3 -> C1-O2 | C3'-O4
                    %%   or
                    %% C1-O2$-C3 -> C1-O2$ | C3'-O4
                    Right = Self,
                    Middle = State#state.left,
                    split_c_o_c3(State, Middle, Right)
            end;
        ok ->
            case is_full_op(Self) of
                true ->
                    case State#state.type of
                        alone ->
                            %% O$ -> [C] -> C-O*
                            {ok, EmptyBucket} = make_empty_bucket(State, c_o_r),
                            link_op(Self, EmptyBucket),
                            set_type_op(Self, c_o_l);
                        c_o_r ->
                            Right = Self,
                            Left = State#state.left,
                            %% C1-O2$ -> C1-O*-C2
                            make_c_o_c(State, Left, Right);
                        %% Insertion to left o
                        c_o_c_m ->
                            Left = State#state.left,
                            Middle = Self,
                            Right = State#state.right,
                            ?ASSERT_NOT_NIL(Right),
                            ?ASSERT_NOT_NIL(Left),

                            %%  C1-O2$-C3
                            %%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
                            split_c_o_c2(State, Left, Middle, Right)
                    end;
                _ ->
                    []
            end
    end,
    gen_server:reply(From, ok).

make_empty_bucket(State, Type) ->
    mio_sup:make_bucket(mio_store:capacity(State#state.store), Type).

make_c_o_c(State, Left, Right) ->
    {ok, EmptyBucket} = make_empty_bucket(State, c_o_c_m),
    link3_op(Left, EmptyBucket, Right),
    ok = set_type_op(Right, c_o_c_r),
    ok = set_type_op(Left, c_o_c_l).

prepare_for_split_c_o_c(State, Left, Middle, Right) ->
    {ok, EmptyBucket} = make_empty_bucket(State, c_o_r),
    ok = set_type_op(Right, c_o_l),
    ok = set_type_op(Middle, c_o_r),
    ok = set_type_op(Left, c_o_l),
    PrevRight = get_right_op(Right),

    %% C3'-O4 | C ...
    link3_op(Right, EmptyBucket, PrevRight),
    EmptyBucket.

split_c_o_c(State, Left, Middle, Right) ->
    %%  C1-O2$-C3
    %%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
    EmptyBucket = prepare_for_split_c_o_c(State, Left, Middle, Right),
    {LargeRKey, LargeRValue} = take_largest_op(Right),
    ok = just_insert_op(EmptyBucket, LargeRKey, LargeRValue),
    {LargeMKey, LargeMValue} = take_largest_op(Middle),
    ok = just_insert_op(Right, LargeMKey, LargeMValue).

split_c_o_c2(State, Left, Middle, Right) ->
    %%  C1-O2$-C3
    %%    Insertion to C1  : C1'-C2-C3 -> C1'-O2 | C3'-O4
    EmptyBucket = prepare_for_split_c_o_c(State, Left, Middle, Right),
    {LargeKey, LargeValue} = take_largest_op(Middle),
    overflow = just_insert_op(Right, LargeKey, LargeValue),
    {LargeRKey, LargeRValue} = take_largest_op(Right),
    ok = just_insert_op(EmptyBucket, LargeRKey, LargeRValue).

split_c_o_c3(State, Middle, Right) ->
    EmptyBucket = prepare_for_split_c_o_c(State, get_left_op(Middle), Middle, Right),
    {LargeKey, LargeValue} = take_largest_op(Right),
    ok = just_insert_op(EmptyBucket, LargeKey, LargeValue).
