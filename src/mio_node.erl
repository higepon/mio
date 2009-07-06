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
-export([start_link/1, key_gt/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {key, value, left, right}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Args) ->
    error_logger:info_msg("~p start_link\n", [?MODULE]),
    error_logger:info_msg("args = ~p start_link\n", [Args]),
    gen_server:start_link(?MODULE, Args, []).

key_gt(Key1, Key2) ->
    Key1 > Key2.


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
    [MyKey, MyValue] = Args,
    {ok, #state{key=MyKey, value=MyValue, left=[], right=[]}}.

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
handle_call(get, From, State) ->
    {reply, {State#state.key, State#state.value}, State#state{value=myValue2}};

%% fetch list of {key, value} tuple.
handle_call(dump_nodes, From, State) ->
    MyKey = State#state.key,
    MyValue = State#state.value,
    HasRight = case State#state.right of
                   [] -> false;
                   _ -> true
               end,
    HasLeft = case State#state.left of
                   [] -> false;
                   _ -> true
              end,
    if
        HasRight ->
            gen_server:cast(State#state.right, {dump_to_right_cast, self(), []});
        true -> []
    end,
    if
        HasLeft ->
            gen_server:cast(State#state.left, {dump_to_left_cast, self(), []});
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
                    {reply, [], State}
            end
    end;

handle_call(dump_to_right, From, State) ->
    io:write(State#state.right),
    case State#state.right of
        [] -> {reply, [{State#state.key,  State#state.value}], State};
        RightPid -> gen_server:cast(RightPid, {dump_to_right_cast, self(), [{State#state.key,  State#state.value}]}),
                    receive
                        {dump_right_accumed, Accumed} ->
                            {reply, Accumed, State}
                    end
    end;

handle_call(dump_to_left, From, State) ->
    io:write(State#state.left),
    case State#state.left of
        [] -> {reply, [{State#state.key,  State#state.value}], State};
        RightPid -> gen_server:cast(RightPid, {dump_to_left_cast, self(), [{State#state.key,  State#state.value}]}),
                    receive
                        {dump_left_accumed, Accumed} ->
                            {reply, Accumed, State}
                    end
    end;

handle_call({search, ReturnToMe, Key}, From, State) ->
    {reply, {ok, myValue1}, State};

handle_call({insert, Key, Value}, From, State) ->
    {ok, Pid} = mio_sup:start_node(Key, Value),
    MyKey = State#state.key,
    if
        Key > MyKey ->
            error_logger:info_msg("~p insert to right\n", [?MODULE]),
            {reply, {ok, Pid}, State#state{right=Pid}};
        true ->
            error_logger:info_msg("~p insert to left\n", [?MODULE]),
            {reply, {ok, Pid}, State#state{left=Pid}}
    end;

handle_call(left, From, State) ->
    {reply, State#state.left, State};

handle_call(right, From, State) ->
    {reply, State#state.right, State};

handle_call(add_right, From, State) ->
    {ok, Pid} = mio_sup:start_node(myKeyRight, myValueRight),
    error_logger:info_msg("~p Pid=~p\n", [?MODULE, Pid]),
    {reply, true, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({dump_to_right_cast, ReturnToMe, Accum}, State) ->
    error_logger:info_msg("~p dump_to_right_cast right=~p\n", [?MODULE, State#state.right]),
    MyKey = State#state.key,
    MyValue = State#state.value,
    case State#state.right of
        [] -> ReturnToMe ! {dump_right_accumed, lists:reverse([{MyKey, MyValue} | Accum])};
        RightPid  -> gen_server:cast(RightPid, {dump_to_right_cast, ReturnToMe, [{MyKey, MyValue} | Accum]})
    end,
    {noreply, State};
handle_cast({dump_to_left_cast, ReturnToMe, Accum}, State) ->
    error_logger:info_msg("~p dump_to_left_cast left=~p\n", [?MODULE, State#state.left]),
    MyKey = State#state.key,
    MyValue = State#state.value,
    case State#state.left of
        [] -> ReturnToMe ! {dump_left_accumed, [{MyKey, MyValue} | Accum]};
        LeftPid  -> gen_server:cast(LeftPid, {dump_to_left_cast, ReturnToMe, [{MyKey, MyValue} | Accum]})
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
