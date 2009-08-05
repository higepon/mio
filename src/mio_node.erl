%%% Description : Skip Graph Node
%%%
%%% Created : 30 Jun 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_node).

-behaviour(gen_server).

%% API
-export([start_link/1,
         search_op/2, link_right_op/3, link_left_op/3, set_nth/3,
         buddy_op/4, insert_op/2, dump_op/2, node_on_level/2,
         range_search_asc_op/4, range_search_desc_op/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("mio.hrl").

-record(state, {key, value, membership_vector, left, right}).

%%====================================================================
%% API
%%====================================================================
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

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

dump_side_(StartNode, Side, Level) ->
    case StartNode of
        [] ->
            [];
        _ ->
            gen_server:cast(StartNode, {dump_side_cast, Side, Level, self(), []}),
            receive
                {dump_side_accumed, Accumed} ->
                    Accumed
            end
    end.

enum_nodes_(StartNode, Level) ->
    {Key, Value, MembershipVector, LeftNodes, RightNodes} = gen_server:call(StartNode, get_op),
    RightNode = node_on_level(RightNodes, Level),
    LeftNode = node_on_level(LeftNodes, Level),
    lists:append([dump_side_(LeftNode, left, Level),
                  [{StartNode, Key, Value, MembershipVector}],
                  dump_side_(RightNode, right, Level)]).

%%--------------------------------------------------------------------
%%  insert operation
%%--------------------------------------------------------------------
insert_op(NodeToInsert, Introducer) ->
    gen_server:call(NodeToInsert, {insert_op, Introducer}).

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
    {ok, ClosestNode, _, _} = gen_server:call(StartNode, {search_op, [], StartKey}),
    ReturnToMe = self(),
    gen_server:cast(ClosestNode, {CastOp, ReturnToMe, Key1, Key2, [], Limit}),
    receive
        {range_search_accumed, Accumed} ->
            Accumed
    after 1000 ->
            [timeout]
    end.

%%--------------------------------------------------------------------
%%  search operation
%%--------------------------------------------------------------------
search_op(StartNode, Key) ->
    %% 2nd parameter [] of gen_server:call(search_op, ...) is Level.
    %% If Level is not specified, The start node checks his max level and use it.
    {ok, _, FoundKey, FoundValue} = gen_server:call(StartNode, {search_op, [], Key}),
    if
        FoundKey =:= Key ->
            {ok, FoundValue};
        true ->
            ng
    end.

%%--------------------------------------------------------------------
%%  buddy operation
%%--------------------------------------------------------------------
buddy_op(Node, MembershipVector, Direction, Level) ->
    gen_server:call(Node, {buddy_op, MembershipVector, Direction, Level}).

%%--------------------------------------------------------------------
%%  link operation
%%--------------------------------------------------------------------
link_right_op(Node, Level, Right) ->
    gen_server:call(Node, {link_right_op, Level, Right}).

link_left_op(Node, Level, Left) ->
    gen_server:call(Node, {link_left_op, Level, Left}).

set_nth(Index, Value, List) ->
    lists:append([lists:sublist(List, 1, Index - 1),
                 [Value],
                 lists:sublist(List, Index + 1, length(List))]).

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
    [MyKey, MyValue, MyMembershipVector] = Args,
    {ok, #state{key=MyKey, value=MyValue, membership_vector=MyMembershipVector, left=[[], []], right=[[], []]}}.

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

handle_call(get_op, _From, State) ->
    {reply, get_op_call(State), State};

handle_call({search_op, Level, Key}, _From, State) ->
    {reply, search_op_call(State, Level, Key), State};

handle_call({buddy_op, MembershipVector, Direction, Level}, _From, State) ->
    {reply, buddy_op_call(State, MembershipVector, Direction, Level), State};

%% Read Only Operations end

handle_call({insert_op, Introducer}, _From, State) ->
    insert_op_call(State, Introducer);

handle_call({set_op, NewValue}, _From, State) ->
    set_op_call(State, NewValue);

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
                    ReturnToMe ! {range_search_accumed, lists:reverse([{self(), MyKey, MyValue} | Accum])};
                NextNode ->
                    gen_server:cast(NextNode,
                                    {Op, ReturnToMe, Key1, Key2, [{self(), MyKey, MyValue} | Accum], Limit - 1})
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

search_right_(MyKey, MyValue, RightNodes, Level, SearchKey) ->
    search_(MyKey, MyValue, RightNodes, Level, SearchKey,
            fun(Key, SKey) -> Key =< SKey end).

search_left_(MyKey, MyValue, LeftNodes, Level, SearchKey) ->
    search_(MyKey, MyValue, LeftNodes, Level, SearchKey,
            fun(Key, SKey) -> Key >= SKey end).

search_(MyKey, MyValue, NextNodes, Level, SearchKey, CompareFun) ->
    if
        Level < 0 ->
            {ok, self(), MyKey, MyValue};
        true ->
            case node_on_level(NextNodes, Level) of
                [] ->
                    search_(MyKey, MyValue, NextNodes, Level - 1, SearchKey, CompareFun);
                NextNode ->
                    {NextKey, _, _, _, _} = gen_server:call(NextNode, get_op),
                    %% we can make short cut. when equal case todo
                    Compare = CompareFun(NextKey, SearchKey),
                    if
                        Compare ->
                            gen_server:call(NextNode, {search_op, Level, SearchKey});
                        true ->
                            search_(MyKey, MyValue, NextNodes, Level - 1, SearchKey, CompareFun)
                    end
            end
    end.

node_on_level(Nodes, Level) ->
    case Nodes of
        [] -> [];
        _ ->  lists:nth(Level + 1, Nodes) %% Erlang array is 1 origin.
    end.

left(State, Level) ->
    node_on_level(State#state.left, Level).

right(State, Level) ->
    node_on_level(State#state.right, Level).

set_right(State, Level, Node) ->
    State#state{right=set_nth(Level + 1, Node, State#state.right)}.

set_left(State, Level, Node) ->
    State#state{left=set_nth(Level + 1, Node, State#state.left)}.

get_op_call(State) ->
    {State#state.key, State#state.value, State#state.membership_vector, State#state.left, State#state.right}.

set_op_call(State, NewValue) ->
    {reply, ok, State#state{value=NewValue}}.

buddy_op_call(State, MembershipVector, Direction, Level) ->
    Found = mio_mvector:eq(Level, MembershipVector, State#state.membership_vector),
    if
        Found ->
            {ok, self()};
        true ->
            case Direction of
                right ->
                    case right(State, 0) of %% N.B. should be on Level 0
                        [] -> {ok, []};
                        RightNode ->
                            buddy_op(RightNode, MembershipVector, Direction, Level)
                    end;
                _ ->
                    case left(State, 0) of
                        [] -> {ok, []};
                        LeftNode ->
                            buddy_op(LeftNode, MembershipVector, Direction, Level)
                    end
            end
    end.

%%--------------------------------------------------------------------
%%  Search operation
%%    Search operation never change the State
%%--------------------------------------------------------------------
search_op_call(State, Level, Key) ->
    SearchLevel = case Level of
                      [] ->
                          length(State#state.right) - 1; %% Level is 0 origin
                      _ -> Level
                  end,
    MyKey = State#state.key,
    MyValue = State#state.value,
    if
        %% This is myKey, found!
        MyKey =:= Key ->
            {ok, self(), MyKey, MyValue};
        MyKey < Key ->
            search_right_(MyKey, MyValue, State#state.right, SearchLevel, Key);
        true ->
            search_left_(MyKey, MyValue, State#state.left, SearchLevel, Key)
    end.

%%--------------------------------------------------------------------
%%  Insert operation
%%--------------------------------------------------------------------
%%   N.B.
%%   insert_op may issue other xxx_op, for example link_right_op.
%%   These issued op should not be circular.
insert_op_call(State, Introducer) ->
    MyKey = State#state.key,
    MyValue = State#state.value,
    if
        %% there's no buddy
        Introducer =:= self() ->
            {reply, ok, State};
        true ->
            {ok, Neighbor, NeighBorKey, _} = gen_server:call(Introducer, {search_op, [], MyKey}),
            if NeighBorKey =:= MyKey ->
                    ok = gen_server:call(Neighbor, {set_op, MyValue}),
                    {reply, ok, State};
               true ->
                    LinkedState = if
                                      NeighBorKey < MyKey ->
                                          {_, _, _, _, NeighborRight} = gen_server:call(Neighbor, get_op),
                                          link_right_op(Neighbor, 0, self()),
                                          case node_on_level(NeighborRight, 0) of
                                              [] -> [];
                                              X -> link_left_op(X, 0, self())
                                          end,
                                          set_right(set_left(State, 0, Neighbor), 0, node_on_level(NeighborRight, 0));
                                      true ->
                                          {_, _, _, NeighborLeft, _} = gen_server:call(Neighbor, get_op),
                                          link_left_op(Neighbor, 0, self()),
                                          case node_on_level(NeighborLeft, 0) of
                                              [] -> [];
                                              X -> link_right_op(X, 0, self())
                                          end,
                                          set_left(set_right(State, 0, Neighbor), 0, node_on_level(NeighborLeft, 0))
                                  end,
                    MaxLevel = length(LinkedState#state.right) - 1,
                    %% link on level > 0
                    ReturnState = insert_loop(1, MaxLevel, LinkedState),
                    {reply, ok, ReturnState}
            end
    end.

%% link on Level > 0
insert_loop(Level, MaxLevel, LinkedState) ->
    %% Find buddy node and link it.
    %% buddy node has same membership_vector on this level.
    if
        Level > MaxLevel -> LinkedState;
        true ->
            case left(LinkedState, 0) of
                [] ->
                    case right(LinkedState, 0) of
                        %% This should never happen, insert to self is returned immediately on insert_op.
                        %%[] ->
                        %%    %% we have no buddy on this level.
                        %%    insert_loop(Level + 1, MaxLevel, LinkedState);
                        RightNodeOnLevel0 ->
                            {ok, Buddy} = buddy_op(RightNodeOnLevel0, LinkedState#state.membership_vector, right, Level),
                            case Buddy of
                                [] ->
                                    %% we have no buddy on this level.
                                    insert_loop(Level + 1, MaxLevel, LinkedState);
                                _ ->
                                    {_, _, _, BuddyLeft, _} = gen_server:call(Buddy, get_op),
                                    link_left_op(Buddy, Level, self()),
                                    %% Since left(Level:0) is empty, this should never happen.
                                    %% case node_on_level(BuddyLeft, Level) of
                                    %%    [] -> [];
                                    %%    X -> link_right_op(X, Level, self())
                                    %% end,
                                    NewLinkedState = set_left(set_right(LinkedState, Level, Buddy), Level, node_on_level(BuddyLeft, Level)),
                                    insert_loop(Level + 1, MaxLevel, NewLinkedState)
                            end
                    end;
                LeftNodeOnLevel0 ->
                    {ok, Buddy} = buddy_op(LeftNodeOnLevel0, LinkedState#state.membership_vector, left, Level),
                    case Buddy of
                        [] ->
                            insert_loop(Level + 1, MaxLevel, LinkedState);
                        _ ->
                            {_, _, _, _, BuddyRight} = gen_server:call(Buddy, get_op),
                            link_right_op(Buddy, Level, self()),
                            case node_on_level(BuddyRight, Level) of
                                [] -> [];
                                X ->
                                    link_left_op(X, Level, self())
                            end,
                            NewLinkedState = set_right(set_left(LinkedState, Level, Buddy), Level, node_on_level(BuddyRight, Level)),
                            insert_loop(Level + 1, MaxLevel, NewLinkedState)
                    end
            end
    end.
