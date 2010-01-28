%%    Copyright (c) 2009-2010  Taro Minowa(Higepon) <higepon@users.sourceforge.jp>
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
%%% File    : mio_node.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : Skip Graph Node implementation
%%%
%%% Created : 30 Jan 2010 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_node).
-include("mio.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1,
         stats_op/2,
         search_op_call/5, buddy_op_call/6, get_op_call/2, get_right_op_call/3,
         get_left_op_call/3, insert_op_call/4, delete_op_call/3,
         search_op/2, link_right_op/4, link_left_op/4,
         set_expire_time_op/2, buddy_op/4, insert_op/2,
         delete_op/2, delete_op/1,range_search_asc_op/4, range_search_desc_op/4,
         check_invariant_level_0_left/5,
         check_invariant_level_0_right/5,
         node_on_level/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {key, value, membership_vector,
                left, right, left_keys, right_keys,
                expire_time, inserted, deleted}).

%%====================================================================
%% API
%%====================================================================
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

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
%%                                     ?INFOF("check_sanity Level~p~n", [Level]),
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
    gen_server:call(StartNode, {search_op, Key, StartLevel}, infinity).

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
init(Args) ->
    [MyKey, MyValue, MyMembershipVector, ExpireTime] = Args,
    Length = length(MyMembershipVector),
    EmptyNeighbor = lists:duplicate(Length + 1, []), % Level 3, require 0, 1, 2, 3
    Insereted = lists:duplicate(Length + 1, false),
    {ok, #state{key=MyKey,
                value=MyValue,
                membership_vector=MyMembershipVector,
                left=EmptyNeighbor,
                right=EmptyNeighbor,
                left_keys=EmptyNeighbor,
                right_keys=EmptyNeighbor,
                expire_time=ExpireTime,
                inserted=Insereted,
                deleted=false
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

%% Read Only Operations start
handle_call({search_op, Key, Level}, From, State) ->
    Self = self(),
    spawn_link(?MODULE, search_op_call, [From, State, Self, Key, Level]),
    {noreply, State};

handle_call(get_op, From, State) ->
    spawn_link(?MODULE, get_op_call, [From, State]),
    {noreply, State};

handle_call({get_inserted_op, Level}, _From, State) ->
    {reply, node_on_level(State#state.inserted, Level), State};

%% Returns insert is done?
handle_call(get_inserted_op, _From, State) ->
    ?INFOF("inserted?=~p", [ State#state.inserted]),
    {reply, lists:all(fun(X) -> X end, State#state.inserted), State};

handle_call(get_deleted_op, _From, State) ->
    {reply, State#state.deleted, State};

handle_call({get_right_op, Level}, From, State) ->
    spawn_link(?MODULE, get_right_op_call, [From, State, Level]),
    {noreply, State};

handle_call({get_left_op, Level}, From, State) ->
    spawn_link(?MODULE, get_left_op_call, [From, State, Level]),
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

%% {noreply, State, hibernate};

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

%% handle_call({link_left_op, Level, LeftNode, LeftKey}, _From, State) ->
%%     Self = self(),
%%     spawn(fun () ->
%%                   Self ! {ok , set_left(State, Level, LeftNode, LeftKey)}
%%           end),
%%     receive {ok, NewState} ->
%%             {reply, ok, NewState}
%%     after 1000 ->
%%             io:format("timeout")
%%     end;


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
handle_cast({dump_side_cast, right, Level, ReturnToMe, Accum}, State) ->
    MyKey = State#state.key,
    MyValue = State#state.value,
    MyMVector = State#state.membership_vector,
    case right_node(State, Level) of
        [] ->
            ReturnToMe ! {dump_side_accumed, lists:reverse([{self(), MyKey, MyValue, MyMVector} | Accum])};
        RightPid ->
            gen_server:cast(RightPid, {dump_side_cast, right, Level, ReturnToMe, [{self(), MyKey, MyValue, MyMVector} | Accum]})
    end,
    {noreply, State};
handle_cast({dump_side_cast, left, Level, ReturnToMe, Accum}, State) ->
    MyKey = State#state.key,
    MyValue = State#state.value,
    MyMVector = State#state.membership_vector,
    case left_node(State, Level) of
        [] ->
            ReturnToMe ! {dump_side_accumed, [{self(), MyKey, MyValue, MyMVector} | Accum]};
        LeftPid -> gen_server:cast(LeftPid, {dump_side_cast, left, Level, ReturnToMe, [{self(), MyKey, MyValue, MyMVector} | Accum]})
    end,
    {noreply, State};

%%--------------------------------------------------------------------
%%  range search operation
%%--------------------------------------------------------------------
handle_cast({range_search_asc_op_cast, ReturnToMe, Key1, Key2, Accum, Limit}, State) ->
    range_search_(ReturnToMe, Key1, Key2, Accum, Limit, State,
                  range_search_asc_op_cast,
                  fun(MyState, Level) -> right_node(MyState, Level) end,
                  State#state.key =< Key1),
    {noreply, State};

handle_cast({range_search_desc_op_cast, ReturnToMe, Key1, Key2, Accum, Limit}, State) ->
    range_search_(ReturnToMe, Key1, Key2, Accum, Limit, State,
                  range_search_desc_op_cast,
                  fun(MyState, Level) -> left_node(MyState, Level) end,
                  State#state.key >= Key2),
    {noreply, State}.

range_search_(ReturnToMe, _Key1, _Key2, Accum, Limit, _State, _Op, _NextNodeFunc, _IsOutOfRange) when Limit =:= 0 ->
    ReturnToMe ! {range_search_accumed, lists:reverse(Accum)};
range_search_(ReturnToMe, Key1, Key2, Accum, Limit, State, Op, NextNodeFunc, IsOutOfRange) when IsOutOfRange ->
    case NextNodeFunc(State, 0) of
        [] ->
            ReturnToMe ! {range_search_accumed, lists:reverse(Accum)};
        NextNode ->
            gen_server:cast(NextNode,
                            {Op, ReturnToMe, Key1, Key2, Accum, Limit})
    end;
range_search_(ReturnToMe, Key1, Key2, Accum, Limit, State, Op, NextNodeFunc, _IsOutOfRange) ->
    MyKey = State#state.key,
    MyValue = State#state.value,
    MyExpireTime = State#state.expire_time,
    if
       Key1 < MyKey andalso MyKey < Key2 ->
            case NextNodeFunc(State, 0) of
                [] ->
                    ReturnToMe ! {range_search_accumed, lists:reverse([{self(), MyKey, MyValue, MyExpireTime} | Accum])};
                NextNode ->
                    gen_server:cast(NextNode,
                                    {Op, ReturnToMe, Key1, Key2, [{self(), MyKey, MyValue, MyExpireTime} | Accum], Limit - 1})
            end;
       true ->
            ReturnToMe ! {range_search_accumed, lists:reverse(Accum)}
    end.

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
%%% Internal functions
%%--------------------------------------------------------------------

node_on_level(Nodes, Level) ->
    case Nodes of
        [] -> [];
        _ ->  lists:nth(Level + 1, Nodes) %% Erlang array is 1 origin.
    end.

left_node(State, Level) ->
    node_on_level(State#state.left, Level).

right_node(State, Level) ->
    node_on_level(State#state.right, Level).

left_key(State, Level) ->
    node_on_level(State#state.left_keys, Level).

right_key(State, Level) ->
    node_on_level(State#state.right_keys, Level).

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
    gen_server:reply(From, {State#state.key, State#state.value, State#state.membership_vector, State#state.left, State#state.right}).
get_right_op_call(From, State, Level) ->
    RightNode = right_node(State, Level),
    RightKey = right_key(State, Level),
    gen_server:reply(From, {RightNode, RightKey}).

get_left_op_call(From, State, Level) ->
    LeftNode = left_node(State, Level),
    LeftKey = left_key(State, Level),
    gen_server:reply(From, {LeftNode, LeftKey}).

set_op_call(State, NewValue) ->
    {reply, ok, State#state{value=NewValue}}.

buddy_op_call(From, State, Self, MembershipVector, Direction, Level) ->
    Found = mio_mvector:eq(Level, MembershipVector, State#state.membership_vector),
    if
        Found ->
            MyKey = State#state.key,
            MyRightKey = right_key(State, Level),
            MyRight = right_node(State, Level),
            gen_server:reply(From, {ok, Self, MyKey, MyRight, MyRightKey});
        true ->
            case Direction of
                right ->
                    case right_node(State, Level - 1) of %% N.B. should be on LowerLevel
                        [] ->
                            gen_server:reply(From, not_found);
                        RightNode ->
                            gen_server:reply(From, buddy_op(RightNode, MembershipVector, Direction, Level))
                    end;
                _ ->
                    case left_node(State, Level - 1) of
                        [] ->
                            gen_server:reply(From, not_found);
                        LeftNode ->
                            gen_server:reply(From, buddy_op(LeftNode, MembershipVector, Direction, Level))
                    end
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
    MyKey = State#state.key,
    Found = string:equal(MyKey, Key),
    if
        %% Found!
        Found ->
            MyValue = State#state.value,
            MyExpireTime = State#state.expire_time,
            gen_server:reply(From, {Self, MyKey, MyValue, MyExpireTime});
        MyKey < Key ->
            search_to_right(From, Self, State, SearchLevel, Key);
        true ->
            search_to_left(From, Self, State, SearchLevel, Key)
    end.

%% Not found
search_to_right(From, Self, State, Level, _Key) when Level < 0 ->
    MyKey = State#state.key,
    MyValue = State#state.value,
    MyExpireTime = State#state.expire_time,
    gen_server:reply(From, {Self, MyKey, MyValue, MyExpireTime});

search_to_right(From, Self, State, Level, Key) ->
    case right_node(State, Level) of
        [] ->
            search_to_right(From, Self, State, Level - 1, Key);
        NextNode ->
            NextKey = right_key(State, Level),
            if
                NextKey =< Key ->
                    gen_server:reply(From, gen_server:call(NextNode, {search_op, Key, Level}, infinity));
                true ->
                    search_to_right(From, Self, State, Level - 1, Key)
            end
    end.

%% Not found
search_to_left(From, Self, State, Level, _Key) when Level < 0 ->
    MyKey = State#state.key,
    MyValue = State#state.value,
    MyExpireTime = State#state.expire_time,
    gen_server:reply(From, {Self, MyKey, MyValue, MyExpireTime});

search_to_left(From, Self, State, Level, Key) ->
    case left_node(State, Level) of
        [] ->
            search_to_left(From, Self, State, Level - 1, Key);
        NextNode ->
            NextKey = left_key(State, Level),
            if
                NextKey >= Key ->
                    gen_server:reply(From, gen_server:call(NextNode, {search_op, Key, Level}, infinity));
                true ->
                    search_to_left(From, Self, State, Level - 1, Key)
            end
    end.

%%--------------------------------------------------------------------
%%  delete operation
%%--------------------------------------------------------------------
delete_op_call(From, Self, State) ->
    LockedNodes = lock_or_exit([Self], ?LINE, [State#state.key]),
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
    {RightNode, RightKey} = gen_server:call(Self, {get_right_op, Level}),
    {LeftNode, LeftKey}  = gen_server:call(Self, {get_left_op, Level}),
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
    ?ERRORF("mio_node:lock dead lock ~p at ~p~n", [Nodes, Line]),
    false;
lock(Nodes, Times, Line) ->
    case mio_lock:lock(Nodes) of
        true ->
            true;
        false ->
            ?INFOF("Lock NG Sleeping ~p~n", [Nodes]),
            mio_util:random_sleep(Times),
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
            ?ERRORF("~p:~p <~p> lock failed~n", [?MODULE, Line, Info]),
            exit(lock_failed);
       true ->
%%            ?INFOF("~p:~p:~p <~p> ~plock ok~n", [?MODULE, Line, self(), Info, Nodes]),
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

    %% At first, we lock the Self not to be deleted.
    %% => This causes dead lock, since Self will never be released to others.
    %%    We use inserted_op instead in order to prevent deletion.
    %% LockedNodes = lock_or_exit([Self], ?LINE, MyKey),

    case link_on_level_0(From, State, Self, Introducer) of
        no_more ->
            gen_server:call(Self, set_inserted_op),
            ?CHECK_SANITY(Self, 0);
        _ ->
            gen_server:call(Self, {set_inserted_op, 0}),

            ?CHECK_SANITY(Self, 0),
            %% link on level > 0
            MaxLevel = length(State#state.membership_vector),
            link_on_level_ge1(Self, MaxLevel)
    end,
    gen_server:reply(From, ok).


link_on_level_0(From, State, Self, Introducer) ->
    MyKey = State#state.key,
    {Neighbor, NeighborKey, _, _} = search_op(Introducer, MyKey),

    IsSameKey = string:equal(NeighborKey, MyKey),
    if
        %% MyKey is already exists
        IsSameKey ->
            MyValue = State#state.value,
            % Since this process doesn't have any other lock, dead lock will never happen.
            % Just wait infinity.
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
                    ok = gen_server:call(Neighbor, {set_op, MyValue}),
                    unlock([Neighbor], ?LINE),
                    %% tell the callee, link_on_level_ge1 is not necessary
                    no_more
            end;
        %% insert!
        true ->
            do_link_on_level_0(From, State, Self, Neighbor, NeighborKey,
                              Introducer)
    end.


%% [Neighbor] <-> [NodeToInsert] <-> [NeigborRight]
do_link_on_level_0(From, State, Self, Neighbor, NeighborKey, Introducer) when NeighborKey < State#state.key ->
   do_link_on_level_0(From, State, Self, Neighbor, NeighborKey, Introducer, get_right_op, check_invariant_level_0_left);

%% [NeighborLeft] <-> [NodeToInsert] <-> [Neigbor]
do_link_on_level_0(From, State, Self, Neighbor, NeighborKey, Introducer) ->
    do_link_on_level_0(From, State, Self, Neighbor, NeighborKey, Introducer, get_left_op, check_invariant_level_0_right).

do_link_on_level_0(From, State, Self, Neighbor, NeighborKey, Introducer, DirectionOp, CheckInvariantFun) ->
    MyKey = State#state.key,

    %% Lock 3 nodes [Neighbor], [NodeToInsert] and [NeighborRightOrLeft]
    {NeighborRightOrLeft, _} = gen_server:call(Neighbor, {DirectionOp, 0}),

    LockedNodes = lock_or_exit([Neighbor, Self, NeighborRightOrLeft], ?LINE, MyKey),

    %% After locked 3 nodes, check invariants.
    %% invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    {RealNeighborRightOrLeft, RealNeighborRightOrLeftKey} = gen_server:call(Neighbor, {DirectionOp, 0}),

    case apply(?MODULE, CheckInvariantFun, [MyKey, Neighbor, NeighborRightOrLeft, RealNeighborRightOrLeft, RealNeighborRightOrLeftKey]) of
        retry ->
            unlock(LockedNodes, ?LINE),
            link_on_level_0(From, State, Self, Introducer);
        ok ->
            case DirectionOp of
                get_right_op ->
                    link_three_nodes({Neighbor, NeighborKey}, {Self, MyKey}, {RealNeighborRightOrLeft, RealNeighborRightOrLeftKey}, 0);
                _ ->
                    link_three_nodes({RealNeighborRightOrLeft, RealNeighborRightOrLeftKey}, {Self, MyKey}, {Neighbor, NeighborKey}, 0)
            end,
            unlock(LockedNodes, ?LINE),
            need_link_on_level_ge1;
        UnknownInvariant ->
            ?ERRORF("FATAL: unknown invariant ~p\n", [UnknownInvariant]),
            exit(unknown_invariant)
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
    IsInvalidOrder = (RealNeighborOfNeighborKey =/= [] andalso apply(CompareFun, [MyKey, RealNeighborOfNeighborKey])),
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
    link_on_level_ge1(Self, 1, MaxLevel).

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
            [];
        {ok, left, Buddy, BuddyKey, BuddyRight, BuddyRightKey} ->
            link_on_level_ge1_left_buddy(Self, MyKey, Buddy, BuddyKey, BuddyRight, BuddyRightKey, Level, MaxLevel),
            ?CHECK_SANITY(Self, Level);
        {ok, right, Buddy, BuddyKey, BuddyLeft, BuddyLeftKey} ->
            link_on_level_ge1_right_buddy(Self, MyKey, Buddy, BuddyKey, BuddyLeft, BuddyLeftKey, Level, MaxLevel),
            ?CHECK_SANITY(Self, Level)
    end.

%% callee should lock all nodes
%% Returns
%%   retry: You should retry link_on_level_ge1 on same level.
%%   done: link process is done. link on higher level is not necessary.
%%   ok: invariant is satified, you can link safely on this level.
check_invariant_ge1_left_buddy(Level, MyKey, Buddy, BuddyKey, BuddyRight) ->
    BuddyInserted = gen_server:call(Buddy, {get_inserted_op, Level}),

    {RealBuddyRight, RealBuddyRightKey} = gen_server:call(Buddy, {get_right_op, Level}),
    IsSameKey = RealBuddyRightKey =/= [] andalso string:equal(MyKey,RealBuddyRightKey),

    %% invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    %%   Buddy->rightKey < MyKey
    if
        %% retry: Buddy is exists only lower level, we have to wait Buddy will be inserted on this level
        not BuddyInserted ->
            ?INFOF("RETRY: check_invariant_ge1_left_buddy Level=~p ~p ~p", [Level, [RealBuddyRight, BuddyRight], [MyKey, BuddyKey, RealBuddyRightKey]]),
            retry;
        %% done: other process insert on higher level, so we have nothing to do.
        IsSameKey ->
            done;
        %% retry: another key is inserted
       (RealBuddyRightKey =/= [] andalso MyKey > RealBuddyRightKey)
       orelse
       (RealBuddyRight =/= BuddyRight) ->
            ?INFOF("RETRY: check_invariant_ge1_left_buddy Level=~p ~p ~p", [Level, [RealBuddyRight, BuddyRight], [MyKey, BuddyKey, RealBuddyRightKey]]),
            retry;
       true->
            IsDeleted =
                (Buddy =/= [] andalso gen_server:call(Buddy, get_deleted_op))
                orelse
                  (BuddyRight =/= [] andalso gen_server:call(BuddyRight, get_deleted_op)),
            if IsDeleted ->
                    ?INFO("RETRY: check_invariant_ge1_left_buddy Neighbor deleted"),
                    retry;
               true ->
                    ok
            end
    end.

check_invariant_ge1_right_buddy(MyKey, Buddy, Level) ->
    IsDeleted = gen_server:call(Buddy, get_deleted_op),
    if IsDeleted ->
            ?INFO("RETRY: check_invariant_ge1_right_buddy Neighbor deleted"),
            retry;
       true ->
            %% check invariants
            %%   Buddy's left is []
            {_, BuddyLeftKey} = gen_server:call(Buddy, {get_left_op, Level}),
            IsSameKey = string:equal(BuddyLeftKey, MyKey),
            if IsSameKey ->
                    ?INFOF("Done: check_invariant_ge1_right_buddy INSERT Nomore MyKey=~p Level~p", [MyKey, Level]),
                    done;
               BuddyLeftKey =/= [] ->
                    %% Retry: another key is inserted
                    ?INFOF("RETRY: check_invariant_ge1_right_buddy MyKey=~p BuddyLeftKey=~p", [MyKey, BuddyLeftKey]),
                    retry;
               true ->
                    ok
            end
    end.

%% [NodeToInsert] <-> [Buddy]
link_on_level_ge1_right_buddy(Self, MyKey, Buddy, BuddyKey, _BuddyLeft, _BuddyLeftKey, Level, MaxLevel) ->
    %% Lock 2 nodes [NodeToInsert] and [Buddy]
    LockedNodes = lock_or_exit([Self, Buddy], ?LINE, MyKey),

    case check_invariant_ge1_right_buddy(MyKey, Buddy, Level) of
        retry ->
            unlock(LockedNodes, ?LINE),
            mio_util:random_sleep(0),
            link_on_level_ge1(Self, Level, MaxLevel);
        done ->
            gen_server:call(Self, set_inserted_op),
            unlock(LockedNodes, ?LINE),
            ?CHECK_SANITY(Self, Level);
        ok ->

            link_three_nodes({[], []}, {Self, MyKey}, {Buddy, BuddyKey}, Level),
            gen_server:call(Self, {set_inserted_op, Level}),
            unlock(LockedNodes, ?LINE),
            %% Go up to next Level.
            link_on_level_ge1(Self, Level + 1, MaxLevel)
    end.

link_on_level_ge1_left_buddy(Self, MyKey, Buddy, BuddyKey, BuddyRight, BuddyRightKey, Level, MaxLevel) ->
    %% Lock 3 nodes [A:m=Buddy], [NodeToInsert] and [D:m]
    LockedNodes = lock_or_exit([Buddy, Self, BuddyRight], ?LINE, MyKey),
    case check_invariant_ge1_left_buddy(Level, MyKey, Buddy, BuddyKey, BuddyRight) of
        retry ->
            unlock(LockedNodes, ?LINE),
            mio_util:random_sleep(0),
            link_on_level_ge1(Self, Level, MaxLevel);
        done ->
            %% mark as inserted
            gen_server:call(Self, set_inserted_op),
            unlock(LockedNodes, ?LINE),
            ?CHECK_SANITY(Self, Level),
            [];
        ok ->
            %% [A:m] <=> [NodeToInsert:m] <=> [D:m]
            link_three_nodes({Buddy, BuddyKey}, {Self, MyKey}, {BuddyRight, BuddyRightKey}, Level),
            gen_server:call(Self, {set_inserted_op, Level}),
            unlock(LockedNodes, ?LINE),
            ?CHECK_SANITY(Self, Level),
            link_on_level_ge1(Self, Level + 1, MaxLevel)
    end.
