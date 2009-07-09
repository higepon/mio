%%%-------------------------------------------------------------------
%%% File    : mio_node.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : Skip Graph Node
%%%
%%% Created : 30 Jun 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_node).

-behaviour(gen_server).

%% API
-export([start_link/1, search/2, dump_nodes/2, set_right/3, set_left/3, set_nth/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(L(), error_logger:info_msg("{~p ~p,~p}:~n", [self(), ?MODULE,?LINE])).
-define(LOG(X), error_logger:info_msg("{~p ~p,~p}: ~s = ~p~n", [self(), ?MODULE,?LINE,??X,X])).
-define(LOGF(X, Data), error_logger:info_msg("{~p ~p,~p}: "++X++"~n" , [self(), ?MODULE,?LINE] ++ Data)).


-record(state, {key, value, membership_vector, left, right}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
%% init_log() ->
%%     %% TODO, where should I write this setting?
%%     error_logger:tty(false),
%%     error_logger:logfile("error.log"),
%%     %% TODO End
%%     ok.

start_link(Args) ->
    error_logger:info_msg("~p start_link\n", [?MODULE]),
    error_logger:info_msg("args = ~p start_link\n", [Args]),
    gen_server:start_link(?MODULE, Args, []).

search(StartNode, Key) ->
    %% 2nd parameter [] of gen_server:call(search, ...) is Level.
    %% If Level is not specified, The start node checks his max level and use it.
    {ok, FoundKey, FoundValue} = gen_server:call(StartNode, {search, StartNode, [], Key}),
    if
        FoundKey =:= Key ->
            {ok, FoundValue};
        true ->
            ng
    end.

dump_nodes(StartNode, Level) ->
    gen_server:call(StartNode, {dump_nodes, Level}).

set_right(Node, Level, Right) ->
    gen_server:call(Node, {set_right, Level, Right}).

set_left(Node, Level, Left) ->
    gen_server:call(Node, {set_left, Level, Left}).

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
    error_logger:info_msg("~p init\n", [?MODULE]),
    error_logger:info_msg("~p init\n", [Args]),
    [MyKey, MyValue, MyMembershipVector] = Args,
    {ok, #state{key=MyKey, value=MyValue, membership_vector=MyMembershipVector, left=[[], []], right=[[], []]}}.

getRandomId() ->
    integer_to_list(crypto:rand_uniform(1, 65536 * 65536)).


%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(get, _From, State) ->
    ?L(),
    {reply, {State#state.key, State#state.value}, State};

%% API for construct test nodes.
handle_call({set_right, Level, Right}, _From, State) ->
    ?L(),
    {reply, ok, State#state{right=set_nth(Level + 1, Right, State#state.right)}};
handle_call({set_left, Level, Left}, _From, State) ->
    ?L(),
    {reply, ok, State#state{left=set_nth(Level + 1, Left, State#state.left)}};

%% fetch list of {key, value} tuple.
handle_call({dump_nodes, Level}, _From, State) ->
    ?L(),
    enum_nodes(State, Level);
%%     HasRight = case right(State, Level) of
%%                    [] -> false;
%%                    _ -> true
%%                end,
%%     HasLeft = case left(State, Level) of %%case State#state.left of
%%                    [] -> false;
%%                    _ -> true
%%               end,
%%     if
%%         HasRight ->
%%             gen_server:cast(right(State, Level), {dump_to_right_cast, Level, self(), []});
%%         true -> []
%%     end,
%%     if
%%         HasLeft ->
%%             gen_server:cast(left(State, Level), {dump_to_left_cast, Level, self(), []});
%%         true -> []
%%     end,

%%     if
%%         HasRight ->
%%             if
%%                 HasLeft ->
%%                     receive
%%                         {dump_right_accumed, RightAccumed} ->
%%                             receive
%%                                 {dump_left_accumed, LeftAccumed} ->
%%                                     {reply, lists:append([LeftAccumed, [{MyKey, MyValue}], RightAccumed]), State}
%%                             end
%%                     end;
%%                 true ->
%%                     receive
%%                         {dump_right_accumed, RightAccumed} ->
%%                             {reply, [{MyKey, MyValue} | RightAccumed], State}
%%                     end
%%             end;
%%         true ->
%%             if
%%                 HasLeft ->
%%                     receive
%%                         {dump_left_accumed, LeftAccumed} ->
%%                             {reply, lists:append(LeftAccumed, [{MyKey, MyValue}]), State}
%%                     end;
%%                 true ->
%%                     {reply, [{MyKey, MyValue}], State}
%%             end
%%     end;
handle_call({search, ReturnToMe, Level, Key}, _From, State) ->

    SearchLevel = case Level of
                      [] ->
                          length(State#state.right) - 1; %% Level is 0 origin
                      _ -> Level
                  end,
    MyKey = State#state.key,
    MyValue = State#state.value,
    ?LOGF("search_call: MyKey=~p searchKey=~p SearchLevel=~p~n", [MyKey, Key, SearchLevel]),
    if
        %% This is myKey, found!
        MyKey =:= Key ->
            ?L(),
            {reply, {ok, MyKey, MyValue}, State};
        MyKey < Key ->
            ?L(),
            {reply, search_right(MyKey, MyValue, State#state.right, ReturnToMe, SearchLevel, Key), State};
%%             ?L(),
%%             case right(State, 0) of
%%                 [] ->
%%                     ?L(),
%%                     {reply, {ok, MyKey, MyValue}, State}; % todo
%%                 RightNode ->
%%                     ?L(),
%%                     ok = gen_server:cast(RightNode, {search, ReturnToMe, SearchLevel, Key}),
%%                     ?L(),
%%                     receive
%%                         {ok, FoundKey, FoundValue} ->
%%                             ?L(),
%%                             {reply, {ok, FoundKey, FoundValue}, State};
%%                         x -> ?LOG(x)
%%                     end
%%             end;
        true ->
            ?L(),
            {reply, search_left(MyKey, MyValue, State#state.left, ReturnToMe, SearchLevel, Key), State}
%%             case left(State, 0) of
%%                 [] ->
%%                     ?L(),
%%                     {reply, {ok, MyKey, MyValue}, State}; % todo
%%                 LeftNode ->
%%                     ?L(),
%%                     gen_server:cast(LeftNode, {search, ReturnToMe, SearchLevel, Key}),
%%                     receive
%%                         {ok, FoundKey, FoundValue} ->
%%                             ?L(),
%%                             {reply, {ok, FoundKey, FoundValue}, State}
%%                     end
%%             end
    end;

handle_call({insert, Key, Value}, _From, State) ->
    ?L(),
    {ok, Pid} = mio_sup:start_node(Key, Value, [1, 0]),
    MyKey = State#state.key,
    if
        Key > MyKey ->
            error_logger:info_msg("~p insert to right\n", [?MODULE]),
            {reply, {ok, Pid}, State#state{right=[Pid, Pid]}};
        true ->
            error_logger:info_msg("~p insert to left\n", [?MODULE]),
            {reply, {ok, Pid}, State#state{left=[Pid, Pid]}}
    end;

handle_call(left, _From, State) ->
    {reply, State#state.left, State};

handle_call(right, _From, State) ->
    {reply, State#state.right, State};

handle_call(add_right, _From, State) ->
    {ok, Pid} = mio_sup:start_node(myKeyRight, myValueRight, [1, 0]),
    error_logger:info_msg("~p Pid=~p\n", [?MODULE, Pid]),
    {reply, true, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({search, ReturnToMe, Level, Key}, State) ->
    MyKey = State#state.key,
    MyValue = State#state.value,
    ?LOGF("search_cast: MyKey=~p searchKey=~p~n", [MyKey, Key]),
    if
        %% This is myKey, found!
        MyKey =:= Key ->
            ?L(),
            ReturnToMe ! {ok, MyKey, MyValue},
            ?L();
        MyKey < Key ->
            ?L(),
            case right(State, Level) of
                [] ->
                    ?L(),
                    ?LOGF("ReturnToMe=~p", [whereis(ReturnToMe)]),
                    ReturnToMe ! {ok, MyKey, MyValue},
                    ?L();
                RightNode ->
                    ?L(),
                    gen_server:cast(RightNode, {search, ReturnToMe, Key})
            end;
        true ->
            ?L(),
            case left(State, 0) of
                [] ->
                    ?L(),
                    ReturnToMe ! {ok, MyKey, MyValue}; %% todo
                LeftNode ->
                    ?L(),
                    gen_server:cast(LeftNode, {search, ReturnToMe, Key})
            end
    end,
    {noreply, State};

handle_cast({dump_to_right_cast, Level, ReturnToMe, Accum}, State) ->
    ?L(),
    MyKey = State#state.key,
    MyValue = State#state.value,
    case right(State, Level) of
        [] -> ReturnToMe ! {dump_right_accumed, lists:reverse([{MyKey, MyValue} | Accum])};
        RightPid -> gen_server:cast(RightPid, {dump_to_right_cast, Level, ReturnToMe, [{MyKey, MyValue} | Accum]})
    end,
    {noreply, State};
handle_cast({dump_to_left_cast, Level, ReturnToMe, Accum}, State) ->
    ?L(),
    MyKey = State#state.key,
    MyValue = State#state.value,
    case left(State, Level) of
        [] -> ReturnToMe ! {dump_left_accumed, [{MyKey, MyValue} | Accum]};
        LeftPid -> gen_server:cast(LeftPid, {dump_to_left_cast, Level, ReturnToMe, [{MyKey, MyValue} | Accum]})
    end,
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
%%% Internal functions
%%--------------------------------------------------------------------


search_right(MyKey, MyValue, RightNodes, ReturnToMe, Level, SearchKey) ->
    ?LOGF("search_right: MyKey=~p MyValue=~p searchKey=~p SearchLevel=~p RightNodes=~p~n", [MyKey, MyValue, SearchKey, Level, RightNodes]),
    if
        Level < 0 ->
            ?L(),
            {ok, MyKey, MyValue};
        true ->
            ?L(),
            RightNode = lists:nth(Level + 1, RightNodes),
            ?LOG(RightNode),
            case RightNode of
                [] ->
                    ?L(),
                    search_right(MyKey, MyValue, RightNodes, ReturnToMe, Level - 1, SearchKey);
                RightNode ->
                    ?L(),
                    {RightKey, _} = gen_server:call(RightNode, get),
                    if
                        %% we can make short cut. when equal case todo
                        RightKey =< SearchKey ->
                            ?L(),
                            gen_server:call(RightNode, {search, ReturnToMe, Level, SearchKey});
                        true ->
                            ?L(),
                            search_right(MyKey, MyValue, RightNodes, ReturnToMe, Level - 1, SearchKey)
                    end
            end
    end.

search_left(MyKey, MyValue, LeftNodes, ReturnToMe, Level, SearchKey) ->
    ?LOGF("search_left: MyKey=~p MyValue=~p searchKey=~p SearchLevel=~p LeftNodes=~p~n", [MyKey, MyValue, SearchKey, Level, LeftNodes]),
    if
        Level < 0 ->
            ?L(),
            {ok, MyKey, MyValue};
        true ->
            ?L(),
            LeftNode = lists:nth(Level + 1, LeftNodes),
            ?LOG(LeftNode),
            case LeftNode of
                [] ->
                    ?L(),
                    search_left(MyKey, MyValue, LeftNodes, ReturnToMe, Level - 1, SearchKey);
                LeftNode ->
                    ?L(),
                    {LeftKey, _} = gen_server:call(LeftNode, get),
                    if
                        %% we can make short cut. todo
                        LeftKey >= SearchKey ->
                            ?L(),
                            gen_server:call(LeftNode, {search, ReturnToMe, Level, SearchKey});
                        true ->
                            ?L(),
                            search_left(MyKey, MyValue, LeftNodes, ReturnToMe, Level - 1, SearchKey)
                    end
            end
    end.


left(State, Level) ->
    case State#state.left of
        [] -> [];
        LeftNodes ->  lists:nth(Level + 1, LeftNodes) %% Erlang array is 1 origin.
    end.

right(State, Level) ->
    case State#state.right of
        [] -> [];
        RightNodes ->  lists:nth(Level + 1, RightNodes) %% Erlang array is 1 origin.
    end.

enum_nodes(State, Level) ->
    MyKey = State#state.key,
    MyValue = State#state.value,
    HasRight = case right(State, Level) of
                   [] -> false;
                   _ -> true
               end,
    HasLeft = case left(State, Level) of %%case State#state.left of
                   [] -> false;
                   _ -> true
              end,
    if
        HasRight ->
            gen_server:cast(right(State, Level), {dump_to_right_cast, Level, self(), []});
        true -> []
    end,
    if
        HasLeft ->
            gen_server:cast(left(State, Level), {dump_to_left_cast, Level, self(), []});
        true -> []
    end,

    if
        HasRight ->
            if
                HasLeft ->
                    receive
                        {dump_right_accumed, RightAccumed} ->
                            receive
                                {dump_left_accumed, LeftAccumed} ->
                                    {reply, lists:append([LeftAccumed, [{MyKey, MyValue}], RightAccumed]), State}
                            end
                    end;
                true ->
                    receive
                        {dump_right_accumed, RightAccumed} ->
                            {reply, [{MyKey, MyValue} | RightAccumed], State}
                    end
            end;
        true ->
            if
                HasLeft ->
                    receive
                        {dump_left_accumed, LeftAccumed} ->
                            {reply, lists:append(LeftAccumed, [{MyKey, MyValue}]), State}
                    end;
                true ->
                    {reply, [{MyKey, MyValue}], State}
            end
    end.
