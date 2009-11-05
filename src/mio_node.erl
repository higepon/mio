%%% Description : Skip Graphde
%%%
%%% Created : 30 Jun 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_node).
-include("mio.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1, search_op_call/5, buddy_op_call/6, get_op_call/2, get_right_op_call/3,
         get_left_op_call/3,
         insert_op_call/4, delete_op_call/2,link_right_op_call/6, link_left_op_call/6,
         search_op/2, link_right_op/4, link_left_op/4, set_nth/3,
         set_expire_time_op/2, buddy_op/4, insert_op/2, dump_op/2, node_on_level/2,
         delete_op/2, delete_op/1,range_search_asc_op/4, range_search_desc_op/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {key, value, membership_vector, left, right, left_keys, right_keys, expire_time, inserted}).

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
    IsSameKey = string:equal(FoundKey,Key),
    if IsSameKey ->
            delete_op(FoundNode),
            ok;
       true -> ng
    end.

delete_op(Node) ->
    gen_server:call(Node, delete_op),
    mio_sup:terminate_node(Node),
    ok.

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

%% For concurrent node joins, link_right_op checks consistency of SkipGraph.
%% If found inconsistent state, link_right_op will be redirect to the next node.
%% But for now, since we serialize all insert/delete requests, we don't redirect.
link_right_op(Node, Level, Right, RightKey) ->
    gen_server:call(Node, {link_right_op, Level, Right, RightKey}).

link_left_op(Node, Level, Left, LeftKey) ->
    gen_server:call(Node, {link_left_op, Level, Left, LeftKey}).

%% For delete operation, no redirect is required.
link_right_no_redirect_op(Node, Level, Right, RightKey) ->
    gen_server:call(Node, {link_right_no_redirect_op, Level, Right, RightKey}).

link_left_no_redirect_op(Node, Level, Left, LeftKey) ->
    gen_server:call(Node, {link_left_no_redirect_op, Level, Left, LeftKey}).

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
                inserted=Insereted
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
    spawn(?MODULE, search_op_call, [From, State, Self, Key, Level]),
    {noreply, State};

handle_call(get_op, From, State) ->
    spawn(?MODULE, get_op_call, [From, State]),
    {noreply, State};

handle_call({get_inserted_op, Level}, _From, State) ->
    {reply, node_on_level(State#state.inserted, Level), State};

handle_call({get_right_op, Level}, From, State) ->
    spawn(?MODULE, get_right_op_call, [From, State, Level]),
    {noreply, State};

handle_call({get_left_op, Level}, From, State) ->
    spawn(?MODULE, get_left_op_call, [From, State, Level]),
    {noreply, State};


handle_call({buddy_op, MembershipVector, Direction, Level}, From, State) ->
    Self = self(),
    spawn(?MODULE, buddy_op_call, [From, State, Self, MembershipVector, Direction, Level]),
    {noreply, State};

handle_call({insert_op, Introducer}, From, State) ->
    Self = self(),
    spawn(?MODULE, insert_op_call, [From, State, Self, Introducer]),
    {noreply, State};

handle_call(delete_op, From, State) ->
    spawn(?MODULE, delete_op_call, [From, State]),
    {noreply, State};

handle_call({set_op, NewValue}, _From, State) ->
    set_op_call(State, NewValue);

handle_call({link_right_op, Level, RightNode, RightKey}, From, State) ->
    Self = self(),
    spawn(?MODULE, link_right_op_call, [From, State, Self, RightNode, RightKey, Level]),
    {noreply, State};

handle_call({set_expire_time_op, ExpireTime}, _From, State) ->
    {reply, ok, State#state{expire_time=ExpireTime}};

handle_call({link_left_op, Level, LeftNode, LeftKey}, From, State) ->
    Self = self(),
    spawn(?MODULE, link_left_op_call, [From, State, Self, LeftNode, LeftKey, Level]),
    {noreply, State};

handle_call({set_inserted_op, Level}, _From, State) ->
    {reply, ok, State#state{inserted=set_nth(Level + 1, true, State#state.inserted)}};

handle_call(set_inserted_op, _From, State) ->
    {reply, ok, State#state{inserted=lists:duplicate(length(State#state.inserted) + 1, true)}};


handle_call({link_right_no_redirect_op, Level, RightNode, RightKey}, _From, State) ->
    Prev = {right(State, Level), right_key(State, Level)},
    {reply, Prev, set_right(State, Level, RightNode, RightKey)};
handle_call({link_left_no_redirect_op, Level, LeftNode, LeftKey}, _From, State) ->
    Prev = {left(State, Level), left_key(State, Level)},
    {reply, Prev, set_left(State, Level, LeftNode, LeftKey)}.

link_right_op_call(From, _State, Self, RightNode, RightKey, Level) ->
    Prev = link_right_no_redirect_op(Self, Level, RightNode, RightKey),
    gen_server:reply(From, Prev).

link_left_op_call(From, _State, Self, LeftNode, LeftKey, Level) ->
    Prev = link_left_no_redirect_op(Self, Level, LeftNode, LeftKey),
    gen_server:reply(From, Prev).

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
    case right(State, Level) of
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
    case left(State, Level) of
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
                  fun(MyState, Level) -> right(MyState, Level) end,
                  State#state.key =< Key1),
    {noreply, State};

handle_cast({range_search_desc_op_cast, ReturnToMe, Key1, Key2, Accum, Limit}, State) ->
    range_search_(ReturnToMe, Key1, Key2, Accum, Limit, State,
                  range_search_desc_op_cast,
                  fun(MyState, Level) -> left(MyState, Level) end,
                  State#state.key >= Key2),
    {noreply, State}.

range_search_(ReturnToMe, Key1, Key2, Accum, Limit, State, Op, NextNodeFunc, IsOutOfRange) ->
    MyKey = State#state.key,
    MyValue = State#state.value,
    MyExpireTime = State#state.expire_time,
    if Limit =:= 0 ->
            ReturnToMe ! {range_search_accumed, lists:reverse(Accum)};
       IsOutOfRange ->
            case NextNodeFunc(State, 0) of
                [] ->
                    ReturnToMe ! {range_search_accumed, lists:reverse(Accum)};
                NextNode ->
                    gen_server:cast(NextNode,
                                    {Op, ReturnToMe, Key1, Key2, Accum, Limit})
            end;
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
set_nth(Index, Value, List) ->
    lists:append([lists:sublist(List, 1, Index - 1),
                 [Value],
                 lists:sublist(List, Index + 1, length(List))]).

node_on_level(Nodes, Level) ->
    case Nodes of
        [] -> [];
        _ ->  lists:nth(Level + 1, Nodes) %% Erlang array is 1 origin.
    end.

left(State, Level) ->
    node_on_level(State#state.left, Level).

right(State, Level) ->
    node_on_level(State#state.right, Level).

left_key(State, Level) ->
    node_on_level(State#state.left_keys, Level).

right_key(State, Level) ->
    node_on_level(State#state.right_keys, Level).

set_right(State, Level, Node, Key) ->
    NewState = State#state{right_keys=set_nth(Level + 1, Key, State#state.right_keys)},
    NewState#state{right=set_nth(Level + 1, Node, NewState#state.right)}.

set_left(State, Level, Node, Key) ->
    NewState = State#state{left_keys=set_nth(Level + 1, Key, State#state.left_keys)},
    NewState#state{left=set_nth(Level + 1, Node, NewState#state.left)}.

%%--------------------------------------------------------------------
%%% Implementation of call
%%--------------------------------------------------------------------
get_op_call(From, State) ->
    gen_server:reply(From, {State#state.key, State#state.value, State#state.membership_vector, State#state.left, State#state.right}).
get_right_op_call(From, State, Level) ->
    RightNode = right(State, Level),
    RightKey = right_key(State, Level),
    gen_server:reply(From, {RightNode, RightKey}).

get_left_op_call(From, State, Level) ->
    LeftNode = left(State, Level),
    LeftKey = left_key(State, Level),
    gen_server:reply(From, {LeftNode, LeftKey}).

set_op_call(State, NewValue) ->
    {reply, ok, State#state{value=NewValue}}.

buddy_op_call(From, State, Self, MembershipVector, Direction, Level) ->
    Found = mio_mvector:eq(Level, MembershipVector, State#state.membership_vector),
%    io:format("buddy ~p key=~p~n", [MembershipVector, State#state.key]),
    if
        Found ->
%            io:format("buddy ~p found~n", [State#state.key]),
            MyKey = State#state.key,
            MyRightKey = right_key(State, Level),
            MyRight = right(State, Level),
            gen_server:reply(From, {ok, Self, MyKey, MyRight, MyRightKey});
        true ->
%            io:format("buddy not found redirect ~n"),
            case Direction of
                right ->
                    case right(State, Level - 1) of %% N.B. should be on LowerLevel
                        [] ->
                            gen_server:reply(From, {ok, [], [], [], []});
                        RightNode ->
                            gen_server:reply(From, buddy_op(RightNode, MembershipVector, Direction, Level))
                    end;
                _ ->
                    case left(State, Level - 1) of
                        [] ->
                            gen_server:reply(From, {ok, [], [], [], []});
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
    case right(State, Level) of
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
    case left(State, Level) of
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
delete_op_call(From, State) ->
    MaxLevel = length(State#state.membership_vector),
    delete_loop_(State, MaxLevel),
    %% My State will not be changed, since I'm killed soon.
    gen_server:reply(From, ok).

delete_loop_(State, Level) when Level < 0 ->
    State;
delete_loop_(State, Level) ->
    RightNode = right(State, Level),
    LeftNode = left(State, Level),
    RightKey = right_key(State, Level),
    LeftKey = left_key(State, Level),

    case RightNode of
        [] -> [];
        _ ->
            link_left_no_redirect_op(RightNode, Level, LeftNode, LeftKey)
    end,
    case LeftNode of
        [] -> [];
        _ ->
            link_right_no_redirect_op(LeftNode, Level, RightNode, RightKey)
    end,
    delete_loop_(set_left(set_right(State, Level, [], []), Level, [], []), Level - 1).

%%--------------------------------------------------------------------
%%  Insert operation
%%--------------------------------------------------------------------
%%   N.B.
%%   insert_op may issue other xxx_op, for example link_right_op.
%%   These issued op should not be circular.
%%
insert_op_call(From, _State, Self, Introducer) when Introducer =:= Self->
    %% there's no buddy
    %% insertiion done
    gen_server:call(Self, set_inserted_op),
    gen_server:reply(From, ok);
insert_op_call(From, State, Self, Introducer) ->
    %% link on level = 0
    case link_on_level0(From, State, Self, Introducer) of
        no_more ->
            gen_server:call(Self, {set_inserted_op, 0}),
            ?CHECK_SANITY(Self, 0);
        _ ->
            gen_server:call(Self, {set_inserted_op, 0}),
            ?CHECK_SANITY(Self, 0),
            %% link on level > 0
            MaxLevel = length(State#state.membership_vector),
            link_on_level_ge1(Self, MaxLevel)
    end,
    gen_server:reply(From, ok).

%% borrowed from global.erl, todo replace
random_sleep(Times) ->
    case (Times rem 10) of
        0 -> erase(random_seed);
        _ -> ok
    end,
    case get(random_seed) of
        undefined ->
            {A1, A2, A3} = now(),
            random:seed(A1, A2, A3 + erlang:phash(node(), 100000));
        _ -> ok
    end,
    %% First time 1/4 seconds, then doubling each time up to 8 seconds max.
    Tmax = if Times > 5 -> 8000;
              true -> ((1 bsl Times) * 1000) div 8
           end,
    T = random:uniform(Tmax),
    io:format("sleep ~p msec~n", [T]),
    receive after T -> ok end.

lock(Nodes, infinity) ->
    mio_lock:lock(Nodes, infinity);
lock(Nodes, 10) ->
    io:format("mio_node:lock dead lock ~p~n", [Nodes]),
    false;
lock(Nodes, Times) ->
    case mio_lock:lock(Nodes) of
        true ->
            true;
        false ->
            io:format("mio_node:lock sleeping for ~p~n", [Nodes]),
            random_sleep(Times),
            lock(Nodes, Times + 1)
    end.

lock(Nodes) ->
    lock(Nodes, 0).

unlock(Nodes) ->
    mio_lock:unlock(Nodes).

link_on_level0(From, State, Self, Introducer) ->
    MyKey = State#state.key,
    {Neighbor, NeighborKey, _, _} = search_op(Introducer, MyKey),
%    io:format("link_on_level0 MyKey=~p NeighborKey=~p ~n", [MyKey, NeighborKey]),
    IsSameKey = string:equal(NeighborKey, MyKey),
    if
        %% MyKey is already exists
        IsSameKey ->
            MyValue = State#state.value,
            % Since this process doesn't have any other lock, dead lock will never happen.
            % Just wait infinity.
            lock([Neighbor], infinity), % TODO: check deleted

            %% overwrite the value
            ok = gen_server:call(Neighbor, {set_op, MyValue}),

            unlock([Neighbor]),
            %% tell the callee, link_on_level_ge1 is not necessary
            no_more;
        %% insert!
        true ->
            link_on_level0(From, State, Self, Neighbor, NeighborKey, Introducer)
    end.


%% [Neighbor] <-> [NodeToInsert] <-> [NeigborRight]
link_on_level0(From, State, Self, Neighbor, NeighborKey, Introducer) when NeighborKey < State#state.key ->
    MyKey = State#state.key,
%    io:format("start link_on_level0! Self=~p self=~p~n", [Self, self()]),
    %% Lock 3 nodes [Neighbor], [NodeToInsert] and [NeigborRight]
    {NeighborRight, _} = gen_server:call(Neighbor, {get_right_op, 0}),
    IsLocked = lock([Neighbor, Self, NeighborRight]),
    if not IsLocked ->
            ?ERRORF("link_on_level0: key = ~p lock failed~n", [MyKey]),
            exit(lock_failed);
       true -> []
    end,

    %% TODO: check deleted

    %% After locked 3 nodes, check invariants.
    %% invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    %%   NeighborRight == RealNeighborRight
    %%   Neighbor->rightKey < MyKey (sanity check)
    {RealNeighborRight, RealNeighborRightKey} = gen_server:call(Neighbor, {get_right_op, 0}),

    if (RealNeighborRightKey =/= [] andalso MyKey >= RealNeighborRightKey)
       orelse
       (NeighborRight =/= RealNeighborRight)
       ->
            %% Retry: another key is inserted
            io:format("** RETRY link_on_level0[3] **~p Self=~p self=~p~n", [State#state.key, Self, self()]),
            unlock([Neighbor, Self, NeighborRight]),
            link_on_level0(From, State, Self, Introducer);
       true ->
            %% [Neighbor] -> [NodeToInsert]  [NeigborRight]
            {PrevNeighborRight, PrevNeighborRightKey} = link_right_op(Neighbor, 0, Self, MyKey),
            case PrevNeighborRight of
                [] -> [];
                _ ->
                    %% [Neighbor]    [NodeToInsert] <- [NeigborRight]
                    link_left_op(PrevNeighborRight, 0, Self, MyKey)
            end,
            %% [Neighbor] <- [NodeToInsert]    [NeigborRight]
            link_left_op(Self, 0, Neighbor, NeighborKey),
            %% [Neighbor]    [NodeToInsert] -> [NeigborRight]
            link_right_op(Self, 0, PrevNeighborRight, PrevNeighborRightKey),

%            io:format("INSERTed A ~p level0 Self=~p ~n", [MyKey, Self]),
%            io:format("Level=~p : ~p ~n", [0, dump_op(Self, 0)]),
            unlock([Neighbor, Self, NeighborRight]),
            need_link_on_level_ge1
    end;


%% [NeighborLeft] <-> [NodeToInsert] <-> [Neigbor]
link_on_level0(From, State, Self, Neighbor, NeighborKey, Introducer) ->
    MyKey = State#state.key,
%    io:format("start link_on_level0 Self=~p self=~p~n", [Self, self()]),
    %% Lock 3 nodes [NeighborLeft], [NodeToInsert] and [Neigbor]
    {NeighborLeft, _} = gen_server:call(Neighbor, {get_left_op, 0}),
    IsLocked = lock([Neighbor, Self, NeighborLeft]),
    if not IsLocked ->
            ?ERRORF("link_on_level0: key = ~p lock failed~n", [MyKey]),
            exit(lock_failed);
       true -> []
    end,

    %% TODO: check deleted

    %% After locked 3 nodes, check invariants.
    %% invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    %%   Neighbor->leftKey < MyKey
    {RealNeighborLeft, RealNeighborLeftKey} = gen_server:call(Neighbor, {get_left_op, 0}),

    if (RealNeighborLeftKey =/= [] andalso MyKey =< RealNeighborLeftKey)
       orelse
       (RealNeighborLeft =/= NeighborLeft)
       ->
            %% Retry: another key is inserted
            io:format("** RETRY link_on_level0[1] Self=~p self=~p ~p **~n", [Self, self(), [MyKey, NeighborKey, RealNeighborLeftKey]]),
            unlock([Neighbor, Self, NeighborLeft]),
            link_on_level0(From, State, Self, Introducer);
       true ->
            %% [NeighborLeft]   [NodeToInsert] <-  [Neigbor]
            {PrevNeighborLeft, PrevNeighborLeftKey} = link_left_op(Neighbor, 0, Self, MyKey),
            case PrevNeighborLeft of
                [] -> [];
                _ ->
                    %% [NeighborLeft] -> [NodeToInsert]   [Neigbor]
                    link_right_op(PrevNeighborLeft, 0, Self, MyKey)
            end,
            %% [NeighborLeft]  [NodeToInsert] -> [Neigbor]
            link_right_op(Self, 0, Neighbor, NeighborKey),

            %% [NeighborLeft] <- [NodeToInsert]     [Neigbor]
            link_left_op(Self, 0, PrevNeighborLeft, PrevNeighborLeftKey),

%            io:format("INSERTed B ~p level0 Self=~p~n", [MyKey, Self]),
%            io:format("Level=~p : ~p ~n", [0, dump_op(Self, 0)]),
            unlock([Neighbor, Self, NeighborLeft]),
            need_link_on_level_ge1
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
    LowerLevel = Level - 1,
    case node_on_level(MyLeft, LowerLevel) of
        %%  <Level - 1>: [NodeToInsert:m] <-> [C:n] <-> [D:m] <-> [E:n] <-> [F:m]
        %%  <Level>    : [D:m] <-> [F:m]
        [] ->
            RightNodeOnLower = node_on_level(MyRight, LowerLevel),
            %% This should never happen.
            %% If leftNodeOnLower does not exist, RightNodeOnLower should exist,
            %% since insert to self is returned immediately on insert_op.
%            io:format("Before assert self=~p ~p ~p~n", [self(), MyKey, Level]),
            ?ASSERT_NOT_NIL(RightNodeOnLower),
            {ok, Buddy, BuddyKey, _, _} = buddy_op(RightNodeOnLower, MyMV, right, Level),
            case Buddy of
                %% [NodeToInsert]
                [] ->
                    %% We have no buddy on this level.
                    %% On higher Level, we have no buddy also.
                    %% So we've done.
                    gen_server:call(Self, set_inserted_op),
                    ?CHECK_SANITY(Self, Level),
%                    io:format("INSERT Nomore ~p level~p:~p~n", [MyKey, Level, ?LINE]),
                    [];
                %% [NodeToInsert] <-> [Buddy]
                _ ->
                    %% Lock 2 nodes [NodeToInsert] and [Buddy]
                    IsLocked = lock([Self, Buddy]),
                    if not IsLocked ->
                            %% todo retry
                            ?ERRORF("link_on_levelge[1]: key = ~p lock failed~n", [MyKey]),
                            exit(lock_failed);
                       true -> []
                    end,

                    %% check invariants
                    %%   Buddy's left is []
                    {_, BuddyLeftKey} = gen_server:call(Buddy, {get_left_op, Level}),
                    IsSameKey = string:equal(BuddyLeftKey, MyKey),
                    if IsSameKey ->
%                    if BuddyLeftKey =:= MyKey ->
%                            io:format("INSERT Nomore ~p level~p:~p~n", [MyKey, Level, ?LINE]),
                            gen_server:call(Self, set_inserted_op),
                            [];
                       BuddyLeftKey =/= [] ->
                            %% Retry: another key is inserted
                            io:format("** RETRY link_on_levelge1[2] ~p~p**~n", [MyKey, BuddyLeftKey]),
                            unlock([Buddy, Self]),
                            link_on_level_ge1(Self, Level, MaxLevel);
                       true ->
                            %% [NodeToInsert] <- [Buddy]
                            link_left_op(Buddy, Level, Self, MyKey),

                            %% [NodeToInsert] -> [Buddy]
                            link_right_op(Self, Level, Buddy, BuddyKey),
                            unlock([Buddy, Self]),
%                            io:format("INSERTed C ~p level~p:~p~n", [MyKey, Level, ?LINE]),
%                            io:format("Level=~p : ~p ~n", [Level, dump_op(Self, Level)]),
                            %% Go up to next Level.
                            gen_server:call(Self, {set_inserted_op, Level}),
                            link_on_level_ge1(Self, Level + 1, MaxLevel)
                    end,
                    ?CHECK_SANITY(Self, Level)
            end;
        %%     <Level - 1>: [A:m] <-> [B:n] <-> [NodeToInsert:m] <-> [C:n] <-> [D:m] <-> [E:n] <-> [F:m]
        %%     <Level>    : [A:m] <-> [D:m] <-> [F:m]
        LeftNodeOnLower ->
            {ok, Buddy, BuddyKey, BuddyRight, BuddyRightKey} = buddy_op(LeftNodeOnLower, MyMV, left, Level),
            case Buddy of
                [] ->
                    case node_on_level(MyRight, LowerLevel) of
                        %% We have no buddy on this level.
                        %% On higher Level, we have no buddy also.
                        %% So we've done.
                        %% <Level - 1>: [B:n] <-> [NodeToInsert:m]
                        [] ->
                            gen_server:call(Self, set_inserted_op),
                            ?CHECK_SANITY(Self, Level),
%                            io:format("INSERT Nomore ~p level~p:~p~n", [MyKey, Level, ?LINE]),
                            [];
                        %% <Level - 1>: [B:n] <-> [NodeToInsert:m] <-> [C:n] <-> [D:m] <-> [E:n] <-> [F:m]
                        RightNodeOnLower2 ->
                            {ok, Buddy2, Buddy2Key, _, _} = buddy_op(RightNodeOnLower2, MyMV, right, Level),
                            case Buddy2 of
                                %% <Level - 1>: [B:n] <-> [NodeToInsert:m] <-> [C:n]
                                [] ->
                                    %% we have no buddy on this level.
                                    %% So we've done.
                                    gen_server:call(Self, set_inserted_op),
                                    ?CHECK_SANITY(Self, Level),
%                                    io:format("INSERT Nomore ~p level~p:~p~n", [MyKey, Level, ?LINE]),
                                    [];
                                %% [NodeToInsert:m] <-> [C:n] <-> [D:m] <-> [E:n] <-> [F:m]
                                _ ->
                                    %% Lock 2 nodes [NodeToInsert] and [Buddy]
                                    IsLocked2 = lock([Self, Buddy2]),
                                    if not IsLocked2 ->
                                            %% todo retry
                                            ?ERRORF("link_on_levelge[3]: key = ~p lock failed~n", [MyKey]),
                                            exit(lock_failed);
                                       true -> []
                                    end,

                                    %% check invariants
                                    %%   Buddy's left is []
                                    {_, Buddy2LeftKey} = gen_server:call(Buddy2, {get_left_op, Level}),
                                    if Buddy2LeftKey =/= [] ->
                                            %% Retry: another key is inserted
                                            io:format("** RETRY link_on_levelge1[4] ~p**~n", [Buddy2LeftKey]),
                                            unlock([Buddy2, Self]),
%                                            io:format("INSERTed D~p level~p:~p~n", [MyKey, Level, ?LINE]),
%                                            io:format("Level=~p : ~p ~n", [Level, dump_op(Self, Level)]),
                                            link_on_level_ge1(Self, Level, MaxLevel);
                                       true ->
                                            %% [NodeToInsert:m] <- [D:m]
                                            link_left_op(Buddy2, Level, Self, MyKey),

                                            %% [NodeToInsert:m] -> [D:m]
                                            link_right_op(Self, Level, Buddy2, Buddy2Key),
                                            unlock([Buddy2, Self]),
%                                            io:format("INSERTed E~p level~p:~p~n", [MyKey, Level, ?LINE]),
%                                            io:format("Level=~p : ~p ~n", [Level, dump_op(Self, Level)]),
                                            gen_server:call(Self, {set_inserted_op, Level}),
                                            link_on_level_ge1(Self, Level + 1, MaxLevel)
                                    end,
                                    ?CHECK_SANITY(Self, Level)
                            end
                    end;
                %% <Level - 1>: [A:m] <-> [B:n] <-> [NodeToInsert:m] <-> [C:n] <-> [D:m] <-> [E:n] <-> [F:m]
                _ ->
                    %% Lock 3 nodes [A:m=Buddy], [NodeToInsert] and [D:m]
                    IsLocked3 = lock([Self, Buddy, BuddyRight]),
                    if not IsLocked3 ->
                            %% todo retry
                            ?ERRORF("link_on_levelge: key = ~p lock failed", [MyKey]),
                            exit(lock_failed);
                       true -> []
                    end,

                    %% TODO: Having "inserted lock" on each level can reduce "inserted lock" contention.
                    BuddyInserted = gen_server:call(Buddy, {get_inserted_op, Level}),

                    %% After locked 3 nodes, check invariants.
                    %% invariant
                    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
                    %%   Buddy->rightKey < MyKey
                    {RealBuddyRight, RealBuddyRightKey} = gen_server:call(Buddy, {get_right_op, Level}),
                    IsSameKey = string:equal(MyKey,RealBuddyRightKey),

                    if not BuddyInserted -> %% RealBuddyRight =:= [] andalso RealBuddyLeft =:= [] ->
                            %% Retry: Buddy is exists only lower level, we have to wait Buddy will be inserted on this level
                            io:format("** RETRY link_on_levelge[88] level=~p ~p ~p~n", [Level, [RealBuddyRight, BuddyRight], [MyKey, BuddyKey, RealBuddyRightKey]]),
                            unlock([Self, Buddy, BuddyRight]),
                            random_sleep(0),
                            io:format("wakup MyKey=~p Self=~p~n", [MyKey, Self]),
                            link_on_level_ge1(Self, Level, MaxLevel);
                       (RealBuddyRightKey =/= [] andalso IsSameKey)
                       ->
                            gen_server:call(Self, set_inserted_op),
                            %% other process insert on higher level, so we have nothing to do.
                            ?CHECK_SANITY(Self, Level),
%                            io:format("INSERTed F Nomore ~p level~p:~p~n", [MyKey, Level, ?LINE]),
                            [];
                       (RealBuddyRightKey =/= [] andalso MyKey > RealBuddyRightKey)
                       orelse
                       (RealBuddyRight =/= BuddyRight)
                       ->
                            %% Retry: another key is inserted
                            io:format("** RETRY link_on_levelge[9] level=~p ~p ~p~n", [Level, [RealBuddyRight, BuddyRight], [MyKey, BuddyKey, RealBuddyRightKey]]),
                            unlock([Self, Buddy, BuddyRight]),
                            random_sleep(0),
                            io:format("wakup MyKey=~p Self=~p~n", [MyKey, Self]),
                            link_on_level_ge1(Self, Level, MaxLevel);
                       true->
                            % [A:m] -> [NodeToInsert:m]
                            link_right_op(Buddy, Level, Self, MyKey),
                            case BuddyRight of
                                [] -> [];
                                X ->
                                    % [NodeToInsert:m] <- [D:m]
                                    link_left_op(X, Level, Self, MyKey)
                            end,
                            % [A:m] <- [NodeToInsert:m]
                            link_left_op(Self, Level, Buddy, BuddyKey),

                            % [NodeToInsert:m] -> [D:m]
                            link_right_op(Self, Level, BuddyRight, BuddyRightKey),
                            unlock([Self, Buddy, BuddyRight]),
                            ?CHECK_SANITY(Self, Level),
                            gen_server:call(Self, {set_inserted_op, Level}),
                            %% Debug info start
%                            io:format("INSERTed G ~p level~p:~p BuddyKey ~p BuddyRightKey =~p ~n", [MyKey, Level, ?LINE, BuddyKey, BuddyRightKey]),

                            %% Debug info end

%                            io:format("Level=~p : ~p ~n", [Level, dump_op(Self, Level)]),
                            link_on_level_ge1(Self, Level + 1, MaxLevel)
                    end
            end
    end.

%%--------------------------------------------------------------------
%%  check_sanity
%%--------------------------------------------------------------------
assert(Cond, Message, Module, Line) ->
    if not Cond ->
            io:format("ASSERTION failed ~p:{~p,~p}:~n", [Message, Module, Line]),
            exit(Message);
       true ->
            []
    end.

check_sanity_to_right(Node, Level, Module, Line) ->
    {Key, _, _, _, _} = gen_server:call(Node, get_op),
    {Right, RightKey} = gen_server:call(Node, {get_right_op, Level}),

    %% Key < RightKey (if Right exists)
    case Right of
        [] -> [];
        _ ->
            assert(Key < RightKey, "Key < RightKey", Module, Line),
%%             {Neighbor, NeigborKey} = gen_server:call(Right, {get_left_op, Level}),
%%             IsSameKey = string:equal(Key, NeigborKey),
%%             assert(Neighbor =:= Node andalso IsSameKey, "Right Node Key consitency", Module, Line),
            check_sanity_to_right(Right, Level, Module, Line)
    end.

check_sanity_to_left(Node, Level, Module, Line) ->
    {Key, _, _, _, _} = gen_server:call(Node, get_op),
    {Left, LeftKey} = gen_server:call(Node, {get_left_op, Level}),

    %% Key < LeftKey (if Left exists)
    case Left of
        [] -> [];
        _ ->
            assert(LeftKey < Key, "LeftKey < Key", Module, Line),
%%             {Neighbor, NeigborKey} = gen_server:call(Left, {get_right_op, Level}),
%%             IsSameKey = string:equal(Key, NeigborKey),
%%             assert(Neighbor =:= Node andalso IsSameKey, "Left Node Key consitency", Module, Line),
            check_sanity_to_left(Left, Level, Module, Line)
    end.



check_sanity(Node, Level, Module, Line) ->
    check_sanity_to_left(Node, Level, Module, Line),
    check_sanity_to_right(Node, Level, Module, Line).



%%     %% Key < RightKey (if Right exists)
%%     case Right of
%%         [] -> [];
%%         _ ->
%%             assert(Key < RightKey, "Key < RightKey", Module, Line),
%%             {Neighbor, NeigborKey} = gen_server:call(Right, {get_left_op, Level}),
%%             IsSameKey = string:equal(Key, NeigborKey),
%%             assert(Neighbor =:= Node andalso IsSameKey, "Right Node Key consitency", Module, Line)
%%     end,
%%     case Left of
%%         [] -> [];
%%         _ ->
%%             assert(LeftKey < Key, "LeftKey < Key", Module, Line),
%%             {Neighbor2, NeigborKey2} = gen_server:call(Left, {get_right_op, Level}),
%%             IsSameKey2 = string:equal(Key, NeigborKey2),
%%             assert(Neighbor2 =:= Node andalso IsSameKey2, "Left Node Key consitency", Module, Line),
%%     end.

%%--------------------------------------------------------------------
%%  dump operation
%%--------------------------------------------------------------------
dump_op(StartNode, Level) ->
    Level0Nodes = enum_nodes_(StartNode, 0),
    case Level of
        0 ->
            Level0Nodes;
        _ ->
            StartNodes= lists:map(fun({Node, _}) -> Node end, lists:usort(fun({_, A}, {_, B}) -> mio_mvector:gt(Level, B, A) end,
                                                                          lists:map(fun({Node, _, _, MV}) -> {Node, MV} end,
                                                                                    Level0Nodes))),
            lists:map(fun(Node) ->
                              lists:map(fun({Pid, Key, Value, MV}) -> {Pid, Key, Value, MV} end,
                                        enum_nodes_(Node, Level))
                      end,
                      StartNodes)
    end.

dump_side_([], _Side, _Level) ->
    [];
dump_side_(StartNode, Side, Level) ->
    gen_server:cast(StartNode, {dump_side_cast, Side, Level, self(), []}),
    receive
        {dump_side_accumed, Accumed} ->
            Accumed
    end.

enum_nodes_(StartNode, Level) ->
    {Key, Value, MembershipVector, LeftNodes, RightNodes} = gen_server:call(StartNode, get_op),
    RightNode = node_on_level(RightNodes, Level),
    LeftNode = node_on_level(LeftNodes, Level),
    lists:append([dump_side_(LeftNode, left, Level),
                  [{StartNode, Key, Value, MembershipVector}],
                  dump_side_(RightNode, right, Level)]).
