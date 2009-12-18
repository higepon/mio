
%%% Description : Skip Graphde
%%%
%%% Created : 30 Jun 2009 by higepon <higepon@users.sourceforge.jp>
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
         set_expire_time_op/2, buddy_op/4, insert_op/2, dump_op/2,
         delete_op/2, delete_op/1,range_search_asc_op/4, range_search_desc_op/4,
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
     %% stats_curr_items(Node),
    [
     stats_status(Node, MaxLevel)].

stats_curr_items(Node) ->
    {"curr_items", integer_to_list(length(dump_op(Node, 0)))}.

stats_status(Node, MaxLevel) ->
    case mio_util:do_times_with_index(0, MaxLevel,
                             fun(Level) ->
                                     ?INFOF("check_sanity Level~p~n", [Level]),
                                     check_sanity(Node, Level, stats, 0)
                             end) of
        ok -> {"mio_status", "OK"};
        Other ->
            {"mio_status", io_lib:format("STAT check_sanity NG Broken : ~p", [Other])}
    end.



terminate_node(Node, After) ->
    spawn(fun() ->
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
link_right_op(Node, Level, Right, RightKey) ->
    gen_server:call(Node, {link_right_op, Level, Right, RightKey}).

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
    spawn(?MODULE, search_op_call, [From, State, Self, Key, Level]),
    {noreply, State};

handle_call(get_op, From, State) ->
    spawn(?MODULE, get_op_call, [From, State]),
    {noreply, State};

handle_call({get_inserted_op, Level}, _From, State) ->
    {reply, node_on_level(State#state.inserted, Level), State};

handle_call(get_deleted_op, _From, State) ->
    {reply, State#state.deleted, State};

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

%% Write Operations start
handle_call({insert_op, Introducer}, From, State) ->
    Self = self(),
    spawn(?MODULE, insert_op_call, [From, State, Self, Introducer]),
    {noreply, State};

%% {noreply, State, hibernate};

handle_call(delete_op, From, State) ->
    Self = self(),
    spawn(?MODULE, delete_op_call, [From, Self, State]),
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

left(State, Level) ->
    node_on_level(State#state.left, Level).

right(State, Level) ->
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
    if
        Found ->
            MyKey = State#state.key,
            MyRightKey = right_key(State, Level),
            MyRight = right(State, Level),
            gen_server:reply(From, {ok, Self, MyKey, MyRight, MyRightKey});
        true ->
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
delete_op_call(From, Self, State) ->
    LockedNodes = lock_or_exit([Self], ?LINE, [State#state.key]),

    IsDeleted = gen_server:call(Self, get_deleted_op),
    if IsDeleted ->
            %% already deleted.
            ?INFO("delete_op_call: Already deleted"),
            unlock(LockedNodes),
            gen_server:reply(From, ok);
       true ->
            MaxLevel = length(State#state.membership_vector),
            delete_loop_(Self, MaxLevel),
            %% My State will not be changed, since I'm killed soon.
            gen_server:call(Self, set_deleted_op),
            unlock(LockedNodes),
            gen_server:reply(From, ok)
    end.

delete_loop_(_Self, Level) when Level < 0 ->
    [];
delete_loop_(Self, Level) ->
    {RightNode, RightKey} = gen_server:call(Self, {get_right_op, Level}),
    {LeftNode, LeftKey}  = gen_server:call(Self, {get_left_op, Level}),
    LockedNodes = lock_or_exit([RightNode, LeftNode], ?LINE, [LeftKey, RightKey]),

    ?CHECK_SANITY(Self, Level),
    case RightNode of
        [] -> [];
        _ ->
            link_left_op(RightNode, Level, LeftNode, LeftKey)
    end,
    case LeftNode of
        [] -> [];
        _ ->
            link_right_op(LeftNode, Level, RightNode, RightKey)
    end,
    ?CHECK_SANITY(Self, Level),
    unlock(LockedNodes),
    %% N.B.
    %% We keep the right/left node of Self, since it may be still located on search path.
    delete_loop_(Self, Level - 1).

%%--------------------------------------------------------------------
%%  Insert operation
%%--------------------------------------------------------------------
%%   N.B.
%%   insert_op may issue other xxx_op, for example link_right_op.
%%   These issued op should not be circular.
%%
insert_op_call(From, _State, Self, Introducer) when Introducer =:= Self->
    ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
    %% there's no buddy
    %% insertiion done
    gen_server:call(Self, set_inserted_op),
    gen_server:reply(From, ok);
insert_op_call(From, State, Self, Introducer) ->
    ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
    %% link on level = 0
    case link_on_level0(From, State, Self, Introducer) of
        no_more ->
            gen_server:call(Self, {set_inserted_op, 0}),
            ?CHECK_SANITY(Self, 0);
        _ ->
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            gen_server:call(Self, {set_inserted_op, 0}),
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            ?CHECK_SANITY(Self, 0),
            %% link on level > 0
            MaxLevel = length(State#state.membership_vector),
            link_on_level_ge1(Self, MaxLevel)
    end,
    gen_server:reply(From, ok).


lock(Nodes, infinity) ->
    mio_lock:lock(Nodes, infinity);
lock(Nodes, 50) ->
    ?ERRORF("mio_node:lock dead lock ~p~n", [Nodes]),
    false;
lock(Nodes, Times) ->
    case mio_lock:lock(Nodes) of
        true ->
            true;
        false ->
            ?INFOF("Lock NG Sleeping ~p~n", [Nodes]),
            mio_util:random_sleep(Times),
            lock(Nodes, Times + 1)
    end.

lock(Nodes) ->
    lock(Nodes, 0).

unlock(Nodes) ->
    mio_lock:unlock(Nodes).

lock_or_exit(Nodes, Line, Info) ->
    IsLocked = lock(Nodes),
    if not IsLocked ->
            ?ERRORF("~p:~p <~p> lock failed~n", [?MODULE, Line, Info]),
            exit(lock_failed);
       true -> Nodes
    end.

link_on_level0(From, State, Self, Introducer) ->
    MyKey = State#state.key,
    {Neighbor, NeighborKey, _, _} = search_op(Introducer, MyKey),
%%    ?INFOF("link_on_level0: MyKey=~p NeighborKey=~p", [MyKey, NeighborKey]),
    IsSameKey = string:equal(NeighborKey, MyKey),
    if
        %% MyKey is already exists
        IsSameKey ->
            MyValue = State#state.value,
            % Since this process doesn't have any other lock, dead lock will never happen.
            % Just wait infinity.
            lock([Neighbor], infinity),

            IsDeleted = gen_server:call(Neighbor, get_deleted_op),
            if IsDeleted ->
                    %% Retry
                    ?INFO("link_on_level0: Neighbor deleted"),
                    unlock([Neighbor]),
                    link_on_level0(From, State, Self, Introducer);
               true ->
                    %% overwrite the value
                    ok = gen_server:call(Neighbor, {set_op, MyValue}),

                    unlock([Neighbor]),
                    %% tell the callee, link_on_level_ge1 is not necessary
                    no_more
            end;
        %% insert!
        true ->
            link_on_level0(From, State, Self, Neighbor, NeighborKey, Introducer)
    end.

%% [Neighbor] <-> [NodeToInsert] <-> [NeigborRight]
link_on_level0(From, State, Self, Neighbor, NeighborKey, Introducer) when NeighborKey < State#state.key ->
    MyKey = State#state.key,
    ?INFOF("start link_on_level0: Self=~p self=~p~n", [Self, self()]),
    %% Lock 3 nodes [Neighbor], [NodeToInsert] and [NeigborRight]
    {NeighborRight, _} = gen_server:call(Neighbor, {get_right_op, 0}),

    LockedNodes = lock_or_exit([Neighbor, Self, NeighborRight], ?LINE, MyKey),

    %% After locked 3 nodes, check invariants.
    %% invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    %%   NeighborRight == RealNeighborRight
    %%   Neighbor->rightKey < MyKey (sanity check)
    {RealNeighborRight, RealNeighborRightKey} = gen_server:call(Neighbor, {get_right_op, 0}),

    case check_invariant_level0_left_buddy(Self, MyKey, Neighbor, NeighborRight, RealNeighborRight, RealNeighborRightKey) of
        retry ->
            unlock(LockedNodes),
            link_on_level0(From, State, Self, Introducer);
        ok ->
            %% [Neighbor] -> [NodeToInsert]  [NeigborRight]
            link_right_op(Neighbor, 0, Self, MyKey),
            case RealNeighborRight of
                [] -> [];
                _ ->
                    %% [Neighbor]    [NodeToInsert] <- [NeigborRight]
                    link_left_op(RealNeighborRight, 0, Self, MyKey)
            end,
            %% [Neighbor] <- [NodeToInsert]    [NeigborRight]
            link_left_op(Self, 0, Neighbor, NeighborKey),
            %% [Neighbor]    [NodeToInsert] -> [NeigborRight]
            link_right_op(Self, 0, RealNeighborRight, RealNeighborRightKey),

            unlock(LockedNodes),
            need_link_on_level_ge1;
        UnknownInvariant ->
            ?ERRORF("FATAL: unknown invariant ~p\n", [UnknownInvariant]),
            exit(unknown_invariant)
    end;

%% [NeighborLeft] <-> [NodeToInsert] <-> [Neigbor]
link_on_level0(From, State, Self, Neighbor, NeighborKey, Introducer) ->
    MyKey = State#state.key,
%    ?INFOF("link_on_level0: Self=~p self=~p~n", [Self, self()]),
    {NeighborLeft, _} = gen_server:call(Neighbor, {get_left_op, 0}),
    %% Lock 3 nodes [NeighborLeft], [NodeToInsert] and [Neigbor]
    LockedNodes = lock_or_exit([Neighbor, Self, NeighborLeft], ?LINE, MyKey),

    %% After locked 3 nodes, check invariants.
    %% invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    %%   Neighbor->leftKey < MyKey
    {RealNeighborLeft, RealNeighborLeftKey} = gen_server:call(Neighbor, {get_left_op, 0}),

    %% After locked 3 nodes, check invariants.
    %% invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    %%   Neighbor->leftKey < MyKey
    {RealNeighborLeft, RealNeighborLeftKey} = gen_server:call(Neighbor, {get_left_op, 0}),

    case check_invariant_level0_right_buddy(Self, MyKey, Neighbor, NeighborKey, NeighborLeft, RealNeighborLeft, RealNeighborLeftKey) of
        retry ->
            unlock(LockedNodes),
            link_on_level0(From, State, Self, Introducer);
        ok ->

            %% [NeighborLeft]   [NodeToInsert] <-  [Neigbor]
            link_left_op(Neighbor, 0, Self, MyKey),
            case RealNeighborLeft of
                [] -> [];
                _ ->
                    %% [NeighborLeft] -> [NodeToInsert]   [Neigbor]
                    link_right_op(RealNeighborLeft, 0, Self, MyKey)
            end,
            %% [NeighborLeft]  [NodeToInsert] -> [Neigbor]
            link_right_op(Self, 0, Neighbor, NeighborKey),

            %% [NeighborLeft] <- [NodeToInsert]     [Neigbor]
            link_left_op(Self, 0, RealNeighborLeft, RealNeighborLeftKey),

            unlock(LockedNodes),
            need_link_on_level_ge1;
        UnknownInvariant ->
            ?ERRORF("FATAL: unknown invariant ~p\n", [UnknownInvariant]),
            exit(unknown_invariant)
    end.



%% callee shoudl lock all nodes
%% Returns
%%   retry: You should retry link_on_level_ge1 on same level.
%%   ok: invariant is satified, you can link safely on this level.
check_invariant_level0_left_buddy(Self, MyKey, Neighbor, NeighborRight, RealNeighborRight, RealNeighborRightKey) ->
    %% After locked 3 nodes, check invariants.
    %% invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    %%   NeighborRight == RealNeighborRight
    %%   Neighbor->rightKey < MyKey (sanity check)
    if (RealNeighborRightKey =/= [] andalso MyKey >= RealNeighborRightKey)
       orelse
       (NeighborRight =/= RealNeighborRight)
       ->
            %% Retry: another key is inserted
            ?INFOF("RETRY: check_invariant_level0_left_buddy MyKey=~p Self=~p self=~p", [MyKey, Self, self()]),
            retry;
       true ->
            IsDeleted =
                (Neighbor =/= [] andalso gen_server:call(Neighbor, get_deleted_op))
                orelse
                (NeighborRight =/= [] andalso gen_server:call(NeighborRight, get_deleted_op)),
            if IsDeleted ->
                    ?INFO("RETRY: check_invariant_level0_left_buddy neighbor deleted"),
                    retry;
               true ->
                    ok
            end
    end.



%% callee shoudl lock all nodes
%% Returns
%%   retry: You should retry link_on_level_ge1 on same level.
%%   ok: invariant is satified, you can link safely on this level.
check_invariant_level0_right_buddy(Self, MyKey, Neighbor, NeighborKey, NeighborLeft, RealNeighborLeft, RealNeighborLeftKey) ->
    if (RealNeighborLeftKey =/= [] andalso MyKey =< RealNeighborLeftKey)
       orelse
       (RealNeighborLeft =/= NeighborLeft)
       ->
            %% Retry: another key is inserted
            ?INFOF("RETRY: check_invariant_level0_right_buddy Self=~p self=~p ~p ", [Self, self(), [MyKey, NeighborKey, RealNeighborLeftKey]]),
            retry;
       true ->
            IsDeleted =
                (Neighbor =/= [] andalso gen_server:call(Neighbor, get_deleted_op))
                orelse
                (NeighborLeft =/= [] andalso gen_server:call(NeighborLeft, get_deleted_op)),
            if IsDeleted ->
                    ?INFO("check_invariant_level0_right_buddy: Neighbor deleted"),
                    retry;
               true ->
                    ok
            end
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
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
    {MyKey, _MyValue, MyMV, MyLeft, MyRight} = gen_server:call(Self, get_op),
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
    LeftOnLower = node_on_level(MyLeft, Level - 1),
    RightOnLower = node_on_level(MyRight, Level - 1),
    case LeftOnLower of
        %%  <Level - 1>: [NodeToInsert:m] <-> [C:n] <-> [D:m] <-> [E:n] <-> [F:m]
        %%  <Level>    : [D:m] <-> [F:m]
        [] ->
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            link_on_level_ge1_to_right(Self, Level, MaxLevel, MyKey, MyMV, RightOnLower);
        %%     <Level - 1>: [A:m] <-> [B:n] <-> [NodeToInsert:m] <-> [C:n] <-> [D:m] <-> [E:n] <-> [F:m]
        %%     <Level>    : [A:m] <-> [D:m] <-> [F:m]
        _ ->
            {ok, Buddy, BuddyKey, BuddyRight, BuddyRightKey} = buddy_op(LeftOnLower, MyMV, left, Level),
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            case Buddy of
                [] ->
                    case RightOnLower of
                        %% We have no buddy on this level.
                        %% On higher Level, we have no buddy also.
                        %% So we've done.
                        %% <Level - 1>: [B:n] <-> [NodeToInsert:m]
                        [] ->
                            gen_server:call(Self, set_inserted_op),
                            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
                            ?CHECK_SANITY(Self, Level),
%%                            ?INFOF("link_on_level_ge1: INSERT Nomore MyKey=~p Level~p", [MyKey, Level]),
                            [];
                        %% <Level - 1>: [B:n] <-> [NodeToInsert:m] <-> [C:n] <-> [D:m] <-> [E:n] <-> [F:m]
                        _ ->
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
                            link_on_level_ge1_to_right(Self, Level, MaxLevel, MyKey, MyMV, RightOnLower)
                    end;
                %% <Level - 1>: [A:m] <-> [B:n] <-> [NodeToInsert:m] <-> [C:n] <-> [D:m] <-> [E:n] <-> [F:m]
                _ ->
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
                    link_on_level_ge1_left_buddy(Self, Level, MaxLevel, MyKey, Buddy, BuddyKey, BuddyRight, BuddyRightKey)
            end
    end.

%% callee shoudl lock all nodes
%% Returns
%%   retry: You should retry link_on_level_ge1 on same level.
%%   done: link process is done. link on higher level is not necessary.
%%   ok: invariant is satified, you can link safely on this level.
check_invariant_ge1_left_buddy(Level, MyKey, Buddy, BuddyKey, BuddyRight) ->
    %% TODO: Having "inserted lock" on each level can reduce "inserted lock" contention.
    BuddyInserted = gen_server:call(Buddy, {get_inserted_op, Level}),

    {RealBuddyRight, RealBuddyRightKey} = gen_server:call(Buddy, {get_right_op, Level}),
    IsSameKey = string:equal(MyKey,RealBuddyRightKey),

    %% invariant
    %%   http://docs.google.com/present/edit?id=0AWmP2yjXUnM5ZGY5cnN6NHBfMmM4OWJiZGZm&hl=ja
    %%   Buddy->rightKey < MyKey
    if
        %% retry: Buddy is exists only lower level, we have to wait Buddy will be inserted on this level
        not BuddyInserted ->
            ?INFOF("RETRY: check_invariant_ge1_left_buddy Level=~p ~p ~p", [Level, [RealBuddyRight, BuddyRight], [MyKey, BuddyKey, RealBuddyRightKey]]),
            retry;
        %% done: other process insert on higher level, so we have nothing to do.
       (RealBuddyRightKey =/= [] andalso IsSameKey) ->
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

link_on_level_ge1_to_right(Self, Level, MaxLevel, MyKey, MyMV, RightNodeOnLower) ->
    %% This should never happen.
    %% If leftNodeOnLower does not exist, RightNodeOnLower should exist,
    %% since insert to self is returned immediately on insert_op.
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
            ?INFOF("INSERT Nomore ~p level~p", [MyKey, Level]),
            [];
        %% [NodeToInsert] <-> [Buddy]
        _ ->
            link_on_level_ge1_right_buddy(Self, MyKey, Buddy, BuddyKey, Level, MaxLevel),
            ?CHECK_SANITY(Self, Level)
    end.

%% [NodeToInsert] <-> [Buddy]
link_on_level_ge1_right_buddy(Self, MyKey, Buddy, BuddyKey, Level, MaxLevel) ->
    %% Lock 2 nodes [NodeToInsert] and [Buddy]
    LockedNodes = lock_or_exit([Self, Buddy], ?LINE, MyKey),

    case check_invariant_ge1_right_buddy(MyKey, Buddy, Level) of
        retry ->
            unlock(LockedNodes),
            link_on_level_ge1(Self, Level, MaxLevel);
        done ->
            gen_server:call(Self, set_inserted_op),
            unlock(LockedNodes);
        ok ->
            %% [NodeToInsert] <- [Buddy]
            link_left_op(Buddy, Level, Self, MyKey),

            %% [NodeToInsert] -> [Buddy]
            link_right_op(Self, Level, Buddy, BuddyKey),
            gen_server:call(Self, {set_inserted_op, Level}),
            unlock(LockedNodes),
            %% Go up to next Level.
            link_on_level_ge1(Self, Level + 1, MaxLevel)
    end.

link_on_level_ge1_left_buddy(Self, Level, MaxLevel, MyKey, Buddy, BuddyKey, BuddyRight, BuddyRightKey) ->
    %% Lock 3 nodes [A:m=Buddy], [NodeToInsert] and [D:m]
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
    LockedNodes = lock_or_exit([Self, Buddy, BuddyRight], ?LINE, MyKey),
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
    case check_invariant_ge1_left_buddy(Level, MyKey, Buddy, BuddyKey, BuddyRight) of
        retry ->
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            unlock(LockedNodes),
            mio_util:random_sleep(0),
            link_on_level_ge1(Self, Level, MaxLevel);
        done ->
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            %% mark as inserted
            gen_server:call(Self, set_inserted_op),
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            unlock(LockedNodes),
            ?CHECK_SANITY(Self, Level),
            ?INFOF("Insert Nomore ~p level~p", [MyKey, Level]),
            [];
        ok ->
            % [A:m] -> [NodeToInsert:m]
            link_right_op(Buddy, Level, Self, MyKey),
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            case BuddyRight of
                [] -> [];
                X ->
                    %% [NodeToInsert:m] <- [D:m]
                    link_left_op(X, Level, Self, MyKey)
            end,
            %% [A:m] <- [NodeToInsert:m]
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            link_left_op(Self, Level, Buddy, BuddyKey),
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            erlang:garbage_collect(Self),
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            %% [NodeToInsert:m] -> [D:m]
            link_right_op(Self, Level, BuddyRight, BuddyRightKey),
            gen_server:call(Self, {set_inserted_op, Level}),
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            unlock(LockedNodes),
            ?INFOF("mem=~p stack=~p words heap=~p ~p", [process_info(Self, memory), process_info(Self, total_heap_size), process_info(Self, stack_size), ?LINE]),
            ?CHECK_SANITY(Self, Level),
            %% Debug info start
%%            ?INFOF("Insereted<~p> Level~p BuddyKey ~p BuddyRightKey=~p", [MyKey, Level, BuddyKey, BuddyRightKey]),
            link_on_level_ge1(Self, Level + 1, MaxLevel);
        UnknownInvariant ->
            ?ERRORF("FATAL: unknown invariant ~p\n", [UnknownInvariant]),
            exit(unknown_invariant)
    end.

%%--------------------------------------------------------------------
%%  check_sanity
%%--------------------------------------------------------------------
assert(Cond, Message, Module, Line) ->
    if not Cond ->
            ?ERRORF("ASSERTION failed ~p:{~p,~p}:~n", [Message, Module, Line]),
            exit(Message);
       true ->
            []
    end.

check_sanity_to_right(Node, Level, Module, Line) ->
    {Key, _, _, _, _} = gen_server:call(Node, get_op),
    {Right, RightKey} = gen_server:call(Node, {get_right_op, Level}),

    %% Should be Key < RightKey (if Right exists)
    case Right of
        [] -> ok;
        _ ->
            if
                not(Key < RightKey) ->
                    Reason = io_lib:format("check_sanity_to_right failed: Node=~p Key=~p RightKey=~p~n", [Node, Key, RightKey]),
                    ?ERROR(Reason),
                    {error, Reason};
                true ->
                    check_sanity_to_right(Right, Level, Module, Line)
            end
    end.

check_sanity_to_left(Node, Level, Module, Line) ->
    {Key, _, _, _, _} = gen_server:call(Node, get_op),
    {Left, LeftKey} = gen_server:call(Node, {get_left_op, Level}),
    ?INFOF("mem=~p stack=~p words heap=~p", [process_info(Node, memory), process_info(Node, total_heap_size), process_info(Node, stack_size)]),
    %% Key < LeftKey (if Left exists)
    case Left of
        [] -> ok;
        _ ->
            if
                not(LeftKey < Key) ->
                    Reason = io_lib:format("check_sanity_to_left failed: Node=~p Key=~p LeftKey=~p~n", [Node, Key, LeftKey]),
                    ?ERROR(Reason),
                    {error, Reason};
                true ->
                    check_sanity_to_left(Left, Level, Module, Line)
            end
    end.

check_sanity(Node, Level, Module, Line) ->
    case check_sanity_to_left(Node, Level, Module, Line) of
        ok ->
            case check_sanity_to_right(Node, Level, Module, Line) of
                ok ->
                    ok;
                Other -> Other
            end;
        Other2 ->
            Other2
    end.


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
