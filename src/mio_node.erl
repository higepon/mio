%%% Description : Skip Graph Node
%%%
%%% Created : 30 Jun 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_node).

-behaviour(gen_server).

%% API
-export([start_link/1, call/2, buddy_op_call/6, get_op_call/2, insert_op_call/4, delete_op_call/2,link_right_op_call/5, link_left_op_call/5,
         search_op/2, search_detail_op/2, link_right_op/3, link_left_op/3, set_nth/3,
         buddy_op/4, insert_op/2, dump_op/2, node_on_level/2, delete_op/2,
         range_search_asc_op/4, range_search_desc_op/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("mio.hrl").

-record(state, {key, value, membership_vector, left, right}).

call(ServerRef, Request) ->
    call(ServerRef, Request, 5000, 3000).
call(ServerRef, Request, Timeout, Timeout1) ->
    if 0 >= Timeout ->
            timeout;
       true ->
            case ServerRef =:= self() of
                true ->
                    ?ERRORF("***** FATAL call myself **** : ~p ~p~n", [ServerRef, Request]);
                _ ->
                    case catch gen_server:call(ServerRef, Request, Timeout1) of
                        {'EXIT', Reason} ->
                            ?ERRORF("Timeout=~p/~p Target=~p Request=~p Request=~p~n", [Timeout1, Timeout, ServerRef, Request, Reason]),
                    {A1, A2, A3} = now(),
                    random:seed(A1, A2, A3),
                    T = random:uniform(2000),
                            timer:sleep(T),
                            call(ServerRef, Request, Timeout - Timeout1, Timeout1);
                        ReturnValue ->
                            ReturnValue
                    end
            end
    end.

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
    {Key, Value, MembershipVector, LeftNodes, RightNodes} = call(StartNode, get_op),
    RightNode = node_on_level(RightNodes, Level),
    LeftNode = node_on_level(LeftNodes, Level),
    lists:append([dump_side_(LeftNode, left, Level),
                  [{StartNode, Key, Value, MembershipVector}],
                  dump_side_(RightNode, right, Level)]).

%%--------------------------------------------------------------------
%%  insert operation
%%--------------------------------------------------------------------
insert_op(Introducer, NodeToInsert) ->
    ?TRACE(insert_op),
    %% Since insert_op may issue get_op and buddy_op,
    %% they should be called with timeout and insert_op without timeout.

    %% Insertion timeout should be infinity since they are serialized and waiting.
    gen_server:call(NodeToInsert, {insert_op, Introducer}, infinity).

%%--------------------------------------------------------------------
%%  delete operation
%%--------------------------------------------------------------------
delete_op(Introducer, Key) ->
    {FoundNode, FoundKey, _} = search_detail_op(Introducer, Key),
    if FoundKey =:= Key ->
            %% ToDo terminate child
            call(FoundNode, delete_op),
            mio_sup:terminate_node(FoundNode),
            ok;
       true -> ng
    end.

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
    {ClosestNode, _, _} = search_detail_op(StartNode, StartKey),
    ReturnToMe = self(),
    gen_server:cast(ClosestNode, {CastOp, ReturnToMe, Key1, Key2, [], Limit}),
    receive
        {range_search_accumed, Accumed} ->
            Accumed
    after 100000 ->
            [timeout]
    end.

%%--------------------------------------------------------------------
%%  search operation
%%--------------------------------------------------------------------
search_detail_timeout(StartNode, ReturnToMe, StartLevel, Key, Count) ->
    if Count < 0 ->
       timout;
       true ->
            gen_server:cast(StartNode, {search_op, ReturnToMe, StartLevel, Key}),
            receive
                {search_result, Result} -> Result
            after 1000 ->
                    {A1, A2, A3} = now(),
                    random:seed(A1, A2, A3),
                    T = random:uniform(2000),
                    timer:sleep(T),
                    io:format("~p~p SEARCH TIMEOUT ReturnToMe~p~n", [erlang:now(), self(), ReturnToMe]),
                    search_detail_timeout(StartNode, ReturnToMe, StartLevel, Key, Count -1)
            end
    end.




search_detail_op(StartNode, Key) ->
    ?TRACE(search_detail_op),
    StartLevel = [], %% If Level is not specified, the start node checkes his max level and use it
    ReturnToMe = self(),
    %% Since we don't want to lock any nodes on search path, we use gen_server:cast instead of gen_server:call
    search_detail_timeout(StartNode, ReturnToMe, StartLevel, Key, 10).

search_op(StartNode, Key) ->
    ?TRACE(search_op),
    %% Since we don't want to lock any nodes on search path, we use gen_server:cast instead of gen_server:cast
    case search_detail_op(StartNode, Key) of
        {_FoundNode, FoundKey, FoundValue} ->
            if FoundKey =:= Key ->
                    {ok, FoundValue};
               true -> ng
            end;
        timeout -> ng
    end.

%%--------------------------------------------------------------------
%%  buddy operation
%%--------------------------------------------------------------------
buddy_op(Node, MembershipVector, Direction, Level) ->
    ?TRACE(buddy_op),
    call(Node, {buddy_op, MembershipVector, Direction, Level}).

%%--------------------------------------------------------------------
%%  link operation
%%--------------------------------------------------------------------

%% For concurrent node joins, link_right_op checks consistency of SkipGraph.
%% If found inconsistent state, link_right_op will be redirect to the next node.
link_right_op(Node, Level, Right) ->
    ?TRACE(link_right_op),
    call(Node, {link_right_op, Level, Right}).

link_left_op(Node, Level, Left) ->
    ?TRACE(link_left_op),
    call(Node, {link_left_op, Level, Left}).

%% For delete operation, no redirect is required.
link_right_no_redirect_op(Node, Level, Right) ->
    ?TRACE(link_right_no_redirect_op),
    call(Node, {link_right_no_redirect_op, Level, Right}).

link_left_no_redirect_op(Node, Level, Left) ->
    ?TRACE(link_left_no_redirect_op),
    call(Node, {link_left_no_redirect_op, Level, Left}).

set_nth(Index, Value, List) ->
    ?TRACE(set_nth),
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
    Length = length(MyMembershipVector),
    EmptyNeighbor = lists:duplicate(Length + 1, []), % Level 3, require 0, 1, 2, 3
    {ok, #state{key=MyKey, value=MyValue, membership_vector=MyMembershipVector, left=EmptyNeighbor, right=EmptyNeighbor}}.

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

handle_call(get_op, From, State) ->
    ?TRACE(c_get_op),
    spawn(?MODULE, get_op_call, [From, State]),
    {noreply, State};


handle_call({buddy_op, MembershipVector, Direction, Level}, From, State) ->
    ?TRACE(c_buddy_op),
    Self = self(),
    spawn(?MODULE, buddy_op_call, [From, Self, State, MembershipVector, Direction, Level]),
    {noreply, State};

handle_call({set_state_op, NewState}, _From, _State) ->
    ?TRACE(c_set_state_op),
    {reply, ok, NewState};

%% Read Only Operations end

handle_call({insert_op, Introducer}, From, State) ->
    ?TRACE(c_insert_op),
    Self = self(),
    spawn(?MODULE, insert_op_call, [From, Self, State, Introducer]),
    {noreply, State};


handle_call(delete_op, From, State) ->
    ?TRACE(delete_op),
    spawn(?MODULE, delete_op_call, [From, State]),
    {noreply, State};

handle_call({set_op, NewValue}, _From, State) ->
    set_op_call(State, NewValue);

handle_call({link_right_op, Level, RightNode}, From, State) ->
    Self = self(),
    spawn(?MODULE, link_right_op_call, [From, Self, State, RightNode, Level]),
    {noreply, State};
%% N.B.
%% For concurrent join, we should check whether Skip Graph is not broken.
%% But for now, we serialize join/delete request.

%%     case right(State, Level) of
%%         [] ->
%%             {reply, ok, set_right(State, Level, RightNode)};
%%         MyRightNode ->
%%             {MyRightKey, _, _, _, _} = call(MyRightNode, get_op),
%%             {RightKey, _, _, _, _} = call(RightNode, get_op),
%%     MyKey = State#state.key,

%%             io:format("************MyKey=~p RightKey=~p MyRightKey=~p Level=~p~n", [MyKey, RightKey, MyRightKey, Level]),

%%             if RightKey > MyRightKey ->
%% %                    link_right_op(MyRightNode, Level, RightNode),
%%                     {reply, ok, set_right(State, Level, RightNode)};
%% %                    {reply, ok, State};
%%                true ->
%                    {reply, ok, set_right(State, Level, RightNode)};
%%             end
%%     end;

handle_call({link_left_op, Level, LeftNode}, From, State) ->
    Self = self(),
    spawn(?MODULE, link_left_op_call, [From, Self, State, LeftNode, Level]),
    {noreply, State};
%    {reply, ok, set_left(State, Level, LeftNode)};

handle_call({link_right_no_redirect_op, Level, RightNode}, _From, State) ->
    {reply, ok, set_right(State, Level, RightNode)};
handle_call({link_left_no_redirect_op, Level, LeftNode}, _From, State) ->
    {reply, ok, set_left(State, Level, LeftNode)}.

link_right_op_call(From, Self, State, RightNode, Level) ->
%%     case right(State, Level) of
%%         [] ->
            link_right_no_redirect_op(Self, Level, RightNode),
            gen_server:reply(From, ok).
%%         MyRightNode ->
%%             {MyRightKey, _, _, _, _} = call(MyRightNode, get_op),
%%             {RightKey, _, _, _, _} = call(RightNode, get_op),
%%             MyKey = State#state.key,

%%             if RightKey > MyRightKey ->
%%                     io:format("************MyKey=~p RightKey=~p MyRightKey=~p Level=~p~n", [MyKey, RightKey, MyRightKey, Level]),
%%                     %% redirec to next node
%%                     link_right_op(MyRightNode, Level, RightNode),
%%                     gen_server:reply(From, ok);
%%                true ->
%%                     link_right_no_redirect_op(Self, Level, RightNode),
%%                     gen_server:reply(From, ok)
%%             end
%    end.

link_left_op_call(From, Self, State, LeftNode, Level) ->
%%     case left(State, Level) of
%%         [] ->
            link_left_no_redirect_op(Self, Level, LeftNode),
            gen_server:reply(From, ok).
%%         MyLeftNode ->
%%             {MyLeftKey, _, _, _, _} = call(MyLeftNode, get_op),
%%             {LeftKey, _, _, _, _} = call(LeftNode, get_op),
%%             MyKey = State#state.key,

%%             if LeftKey < MyLeftKey ->
%%                     io:format("************MyKey=~p LeftKey=~p MyLeftKey=~p Level=~p~n", [MyKey, LeftKey, MyLeftKey, Level]),
%%                     %% redirec to next node
%%                     link_left_op(MyLeftNode, Level, LeftNode),
%%                     gen_server:reply(From, ok);
%%                true ->
%%                     link_left_no_redirect_op(Self, Level, LeftNode),
%%                     gen_server:reply(From, ok)
%%             end
%%     end.



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
%%  search operation
%%--------------------------------------------------------------------
handle_cast({search_op, ReturnToMe, Level, Key}, State) ->
    ?TRACE(ca_search_op),
    search_op_cast_(ReturnToMe, State, Level, Key),
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

search_op_right_cast_(ReturnToMe, State, Level, Key) ->
    ?TRACE(search_op_right_cast_),
    MyKey = State#state.key,
    MyValue = State#state.value,
    if
        Level < 0 ->
            ReturnToMe ! {search_result, {self(), MyKey, MyValue}};
        true ->
            case right(State, Level) of
                [] ->
                    search_op_right_cast_(ReturnToMe, State, Level - 1, Key);
                NextNode ->
                    {NextKey, _, _, _, _} = call(NextNode, get_op),
                    %% we can make short cut. when equal case todo
                    Compare = NextKey =< Key,
                    if
                        Compare ->
                            gen_server:cast(NextNode, {search_op, ReturnToMe, Level, Key});
                        true ->
                            search_op_right_cast_(ReturnToMe, State, Level - 1, Key)
                    end
            end
    end.

search_op_left_cast_(ReturnToMe, State, Level, Key) ->
    ?TRACE(search_op_left_cast_),
    MyKey = State#state.key,
    MyValue = State#state.value,
    if
        Level < 0 ->
            ReturnToMe ! {search_result, {self(), MyKey, MyValue}};
        true ->
            case left(State, Level) of
                [] ->
                    search_op_left_cast_(ReturnToMe, State, Level - 1, Key);
                NextNode ->
                    {NextKey, _, _, _, _} = call(NextNode, get_op),
                    %% we can make short cut. when equal case todo
                    Compare = NextKey >= Key,
                    if
                        Compare ->
                            gen_server:cast(NextNode, {search_op, ReturnToMe, Level, Key});
                        true ->
                            search_op_left_cast_(ReturnToMe, State, Level - 1, Key)
                    end
            end
    end.



search_op_cast_(ReturnToMe, State, Level, Key) ->
    ?TRACE(search_op_cast_),
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
            ReturnToMe ! {search_result, {self(), MyKey, MyValue}};
        MyKey < Key ->
            search_op_right_cast_(ReturnToMe, State, SearchLevel, Key);
        true ->
            search_op_left_cast_(ReturnToMe, State, SearchLevel, Key)
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

%% search_right_(MyKey, MyValue, RightNodes, Level, SearchKey) ->
%%     search_(MyKey, MyValue, RightNodes, Level, SearchKey,
%%             fun(Key, SKey) -> Key =< SKey end).

%% search_left_(MyKey, MyValue, LeftNodes, Level, SearchKey) ->
%%     search_(MyKey, MyValue, LeftNodes, Level, SearchKey,
%%             fun(Key, SKey) -> Key >= SKey end).

%% search_(MyKey, MyValue, NextNodes, Level, SearchKey, CompareFun) ->
%%     if
%%         Level < 0 ->
%%             {ok, self(), MyKey, MyValue};
%%         true ->
%%             case node_on_level(NextNodes, Level) of
%%                 [] ->
%%                     search_(MyKey, MyValue, NextNodes, Level - 1, SearchKey, CompareFun);
%%                 NextNode ->
%%                     {NextKey, _, _, _, _} = call(NextNode, get_op),
%%                     %% we can make short cut. when equal case todo
%%                     Compare = CompareFun(NextKey, SearchKey),
%%                     if
%%                         Compare ->
%%                             call(NextNode, {search_op, Level, SearchKey});
%%                         true ->
%%                             search_(MyKey, MyValue, NextNodes, Level - 1, SearchKey, CompareFun)
%%                     end
%%             end
%%     end.

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

get_op_call(From, State) ->
    ?TRACE(get_op_call),
    gen_server:reply(From, {State#state.key, State#state.value, State#state.membership_vector, State#state.left, State#state.right}).

set_op_call(State, NewValue) ->
    ?TRACE(set_op_call),
    {reply, ok, State#state{value=NewValue}}.

buddy_op_call(From, Self, State, MembershipVector, Direction, Level) ->
    ?TRACE(buddy_op_call),
    Found = mio_mvector:eq(Level, MembershipVector, State#state.membership_vector),
    if
        Found ->
            gen_server:reply(From, {ok, Self});
        true ->
            case Direction of
                right ->
                    case right(State, Level - 1) of %% N.B. should be on Level 0
                        [] -> gen_server:reply(From, {ok, []});
                        RightNode ->
                            gen_server:reply(From, buddy_op(RightNode, MembershipVector, Direction, Level))
                    end;
                _ ->
                    case left(State, Level - 1) of
                        [] -> gen_server:reply(From, {ok, []});
                        LeftNode ->
                            gen_server:reply(From, buddy_op(LeftNode, MembershipVector, Direction, Level))
                    end
            end
    end.

%%--------------------------------------------------------------------
%%  Search operation
%%    Search operation never change the State
%%--------------------------------------------------------------------
%% search_op_call(State, Level, Key) ->
%%     SearchLevel = case Level of
%%                       [] ->
%%                           length(State#state.right) - 1; %% Level is 0 origin
%%                       _ -> Level
%%                   end,
%%     MyKey = State#state.key,
%%     MyValue = State#state.value,
%%     if
%%         %% This is myKey, found!
%%         MyKey =:= Key ->
%%             {ok, self(), MyKey, MyValue};
%%         MyKey < Key ->
%%             search_right_(MyKey, MyValue, State#state.right, SearchLevel, Key);
%%         true ->
%%             search_left_(MyKey, MyValue, State#state.left, SearchLevel, Key)
%%     end.

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
    case RightNode of
        [] -> [];
        _ ->
            ok = link_left_no_redirect_op(RightNode, Level, LeftNode)
    end,
    case LeftNode of
        [] -> [];
        _ ->
            ?LOG(),
            ok = link_right_no_redirect_op(LeftNode, Level, RightNode)
    end,
    delete_loop_(set_left(set_right(State, Level, []), Level, []), Level - 1).

%%--------------------------------------------------------------------
%%  Insert operation
%%--------------------------------------------------------------------
%%   N.B.
%%   insert_op may issue other xxx_op, for example link_right_op.
%%   These issued op should not be circular.
insert_op_call(From, Self, State, Introducer) ->
    ?TRACE(insert_op_call),
    MyKey = State#state.key,
    MyValue = State#state.value,
    io:format("~p:~p:insert ~p START~n", [erlang:now(), Self, MyKey]),
    if
        %% there's no buddy
        Introducer =:= Self ->
            gen_server:reply(From, ok);
        true ->
            {Neighbor, NeighBorKey, _} = search_detail_op(Introducer, MyKey),
            if NeighBorKey =:= MyKey ->
                    ok = call(Neighbor, {set_op, MyValue}),
                    gen_server:reply(From, ok);
               true ->
                    LinkedState = if
                                      NeighBorKey < MyKey ->
                                          ?LOG(),
                                          {_, _, _, _, NeighborRight} = call(Neighbor, get_op),
                                          link_right_op(Neighbor, 0, Self),
                                          case node_on_level(NeighborRight, 0) of
                                              [] -> [];
                                              X -> link_left_op(X, 0, Self)
                                          end,
                                          set_right(set_left(State, 0, Neighbor), 0, node_on_level(NeighborRight, 0));
                                      true ->
                                          {_, _, _, NeighborLeft, _} = call(Neighbor, get_op),
                                          link_left_op(Neighbor, 0, Self),
                                          case node_on_level(NeighborLeft, 0) of
                                              [] -> [];
                                              X -> link_right_op(X, 0, Self)
                                          end,
                                          set_left(set_right(State, 0, Neighbor), 0, node_on_level(NeighborLeft, 0))
                                  end,
                    MaxLevel = length(LinkedState#state.membership_vector),
                    %% link on level > 0
                    gen_server:call(Self, {set_state_op, LinkedState}),
                    ReturnState = insert_loop(Self, 1, MaxLevel, LinkedState),
                    gen_server:call(Self, {set_state_op, ReturnState}),
                    io:format("~p:~p:insert ~p END~n", [erlang:now(), Self, MyKey]),
                    gen_server:reply(From, ok)
            end
    end.

%% link on Level > 0
insert_loop(Self, Level, MaxLevel, LinkedState) ->
    ?TRACE(insert_loop),
    %% Find buddy node and link it.
    %% buddy node has same membership_vector on this level.
    if
        Level > MaxLevel -> LinkedState;
        true ->
            case left(LinkedState, Level - 1) of
                [] ->
                    case right(LinkedState, Level - 1) of
                        %% This should never happen, insert to self is returned immediately on insert_op.
                        %%[] ->
                        %%    %% we have no buddy on this level.
                        %%    insert_loop(Level + 1, MaxLevel, LinkedState);
                        RightNodeOnLevel0 ->
                            {ok, Buddy} = buddy_op(RightNodeOnLevel0, LinkedState#state.membership_vector, right, Level),
                            case Buddy of
                                [] ->
                                    %% we have no buddy on this level.
                                    %% So we've done.
                                    LinkedState;
                                _ ->
                                    {_, _, _, BuddyLeft, _} = call(Buddy, get_op),
                                    link_left_op(Buddy, Level, Self),
                                    %% Since left(Level:0) is empty, this should never happen.
                                    %% case node_on_level(BuddyLeft, Level) of
                                    %%    [] -> [];
                                    %%    X -> link_right_op(X, Level, Self)
                                    %% end,
                                    NewLinkedState = set_left(set_right(LinkedState, Level, Buddy), Level, node_on_level(BuddyLeft, Level)),
                                    gen_server:call(Self, {set_state_op, NewLinkedState}),
                                    insert_loop(Self, Level + 1, MaxLevel, NewLinkedState)
                            end
                    end;
                LeftNodeOnLevel0 ->
                    {ok, Buddy} = buddy_op(LeftNodeOnLevel0, LinkedState#state.membership_vector, left, Level),
                    case Buddy of
                        [] ->
                            case right(LinkedState, Level - 1) of
                                [] ->
                                    LinkedState;
                                %% This should never happen, insert to self is returned immediately on insert_op.
                                %%[] ->
                                %%    %% we have no buddy on this level.
                                %%    insert_loop(Level + 1, MaxLevel, LinkedState);
                                RightNodeOnLevel02 ->
                                    {ok, Buddy2} = buddy_op(RightNodeOnLevel02, LinkedState#state.membership_vector, right, Level),
                                    case Buddy2 of
                                        [] ->
                                            %% we have no buddy on this level.
                                            %% So we've done.
                                            LinkedState;
                                        _ ->
                                            {_, _, _, BuddyLeft2, _} = call(Buddy2, get_op),
                                            link_left_op(Buddy2, Level, Self),
                                            %% Since left(Level:0) is empty, this should never happen.
                                            %% case node_on_level(BuddyLeft, Level) of
                                            %%    [] -> [];
                                            %%    X -> link_right_op(X, Level, Self)
                                            %% end,
                                            NewLinkedState2 = set_left(set_right(LinkedState, Level, Buddy2), Level, node_on_level(BuddyLeft2, Level)),
                                            gen_server:call(Self, {set_state_op, NewLinkedState2}),
                                            insert_loop(Self, Level + 1, MaxLevel, NewLinkedState2)
                                    end
                            end;
                        _ ->
                            {_, _, _, _, BuddyRight} = call(Buddy, get_op),
                            link_right_op(Buddy, Level, Self),
                            case node_on_level(BuddyRight, Level) of
                                [] -> [];
                                X ->
                                    link_left_op(X, Level, Self)
                            end,
                            NewLinkedState = set_right(set_left(LinkedState, Level, Buddy), Level, node_on_level(BuddyRight, Level)),
                            gen_server:call(Self, {set_state_op, LinkedState}),
                            insert_loop(Self, Level + 1, MaxLevel, NewLinkedState)
                    end
            end
    end.
