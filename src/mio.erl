%%%-------------------------------------------------------------------
%%% File    : mio.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : mio application
%%%
%%% Created : 29 Jun 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio).

-behaviour(application).
-export([start/0, mio/1, process_command/2]).
-import(mio).
%% Application callbacks
-export([start/2, stop/1]).
%% Utility
-define(SERVER, ?MODULE).
-define(L(), error_logger:info_msg("{~p ~p,~p}:~n", [self(), ?MODULE,?LINE])).
-define(LOG(X), error_logger:info_msg("{~p ~p,~p}: ~s = ~p~n", [self(), ?MODULE,?LINE,??X,X])).
-define(LOGF(X, Data), error_logger:info_msg("{~p ~p,~p}: "++X++"~n" , [self(), ?MODULE,?LINE] ++ Data)).


%% -export([start_link/1]).
%% -export([start_link/0]).


%%====================================================================
%% Application callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start(Type, StartArgs) -> {ok, Pid} |
%%                                     {ok, Pid, State} |
%%                                     {error, Reason}
%% Description: This function is called whenever an application
%% is started using application:start/1,2, and should start the processes
%% of the application. If the application is structured according to the
%% OTP design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%%--------------------------------------------------------------------
start(_Type, StartArgs) ->
    {ok, IsDebugMode} = application:get_env(mio, debug),
    ?LOG(IsDebugMode),
    if IsDebugMode =:= true ->
            ?L(),
            [];
       true ->
            ?L(),
            error_logger:tty(false)
    end,
    error_logger:info_msg("mio application start\n"),
    mio_sup:start_link(),
    Pid = spawn(?MODULE, mio, [11211]),
    register(mio, Pid),
    {ok , Pid}.
%%     {ok, register(mio, Pid)}.


%%    supervisor:start_child(mio_sup, []).
%%     Pid = spawn(fun() -> process() end),
%%     {ok, Pid}.
%%     case 'TopSupervisor':start_link(StartArgs) of
%%         {ok, Pid} ->
%%             {ok, Pid};
%%         Error ->
%%             Error
%%     end.

%%--------------------------------------------------------------------
%% Function: stop(State) -> void()
%% Description: This function is called whenever an application
%% has stopped. It is intended to be the opposite of Module:start/2 and
%% should do any necessary cleaning up. The return value is ignored.
%%--------------------------------------------------------------------
stop(_State) ->
    error_logger:info_msg("mio application stop\n"),
    ok.

%% start_link() ->
%%     io:format("hige"),
%%     process().


%% start_link(Args) ->
%%     io:format("hige"),
%%     process().


%%====================================================================
%% Internal functions
%%====================================================================
start() ->
    application:load(mio),
    application:start(mio),
    ok.

mio(Port) ->
    {ok, Listen} =
        gen_tcp:listen(
          Port, [binary, {packet, line}, {active, false}, {reuseaddr, true}]),
    io:fwrite("< server listening ~p\n", [Port]),
    {ok, BootPid} = mio_sup:start_node(dummy, dummy, [1, 0]), %% todo mvector
    mio_accept(Listen, BootPid).

mio_accept(Listen, StartNode) ->
    {ok, Sock} = gen_tcp:accept(Listen),
    io:fwrite("<~p new client connection\n", [Sock]),
    spawn(?MODULE, process_command, [Sock, StartNode]),
    mio_accept(Listen, StartNode).

process_command(Sock, StartNode) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Line} ->
            io:fwrite(">~p ~s", [Sock, Line]),
            Token = string:tokens(binary_to_list(Line), " \r\n"),
            case Token of
                ["get", Key] ->
                    process_get(Sock, StartNode, Key);
                ["get", "mio:range-search", Key1, Key2, Limit] ->
                    io:fwrite(">range search Key1 =~p Key2=~p Limit=~p\n", [Key1, Key2, Limit]),
                    process_get_s(Sock, StartNode, Key1, Key2, list_to_integer(Limit));
                ["set", Key, Flags, Expire, Bytes] ->
                    inet:setopts(Sock,[{packet, raw}]),
                    process_set(Sock, StartNode, Key, Flags, Expire, Bytes),
                    inet:setopts(Sock,[{packet, line}]);
%%                 ["set/s", Key, Index, Flags, Expire, Bytes] ->
%%                     inet:setopts(Sock,[{packet, raw}]),
%%                     process_set_s(Sock, Key, list_to_atom(Index), Flags, Expire, Bytes),
%%                     inet:setopts(Sock,[{packet, line}]);
%%                 ["delete", Key] ->
%%                     process_delete(Sock, Key);
                ["quit"] -> gen_tcp:close(Sock);
                _ -> gen_tcp:send(Sock, "ERROR\r\n")
            end,
            process_command(Sock, StartNode);
        {error, closed} ->
            io:fwrite("<~p connection closed.\n", [Sock]);
        Error ->
            io:fwrite("<~p error: ~p\n", [Sock, Error])
    end.

process_get(Sock, StartNode, Key) ->
    Value = case mio_node:search(StartNode, Key) of
                ng -> "";
                {ok, FoundValue} -> FoundValue
              end,
    ?LOG(Value),
    gen_tcp:send(Sock, io_lib:format(
                         "VALUE ~s 0 ~w\r\n~s\r\nEND\r\n",
                         [Key, size(Value), Value])).

process_values([{_, Key, Value} | More]) ->
    ?LOG(Key),
    ?LOG(Value),
    io_lib:format("VALUE ~s 0 ~w\r\n~s\r\n~s",
                  [Key, size(Value), Value, process_values(More)]);
process_values([]) ->
    "END\r\n".


process_get_s(Sock, StartNode, Key1, Key2, Limit) ->
    ?LOGF("Key1=~p, Key2=~p\n", [Key1, Key2]),
    Values = mio_node:range_search_op(StartNode, Key1, Key2, Limit),
    ?LOG(Values),
    P = process_values(Values),
    ?LOG(P),
    gen_tcp:send(Sock, P).

process_set(Sock, Introducer, Key, _Flags, _Expire, Bytes) ->
    case gen_tcp:recv(Sock, list_to_integer(Bytes)) of
        {ok, Value} ->
            ?LOG(Key),
            ?LOG(Value),
            {ok, NodeToInsert} = mio_sup:start_node(Key, Value, [1, 0]),
            mio_node:insert_op(NodeToInsert, Introducer),
            gen_tcp:send(Sock, "STORED\r\n");
        {error, closed} ->
            ok;
        Error ->
            io:fwrite("Error: ~p\n", [Error])
    end,
    gen_tcp:recv(Sock, 2).




%% process() ->
%%     receive
%%         _ ->
%%             io:format("hige"),
%%             process()
%%     end.
