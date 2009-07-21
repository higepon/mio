%%%-------------------------------------------------------------------
%%% File    : mio.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : mio application
%%%
%%% Created : 29 Jun 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio).

-behaviour(application).
-export([start/0, mio/1, process_command/1]).
-import(mio).
%% Application callbacks
-export([start/2, stop/1]).
%% Utility


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
    error_logger:info_msg("mio application start\n"),
    mio_sup:start_link(),
    Pid = spawn(?MODULE, mio, [11121]),
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
    mio_accept(Listen).

mio_accept(Listen) ->
    {ok, Sock} = gen_tcp:accept(Listen),
    io:fwrite("<~p new client connection\n", [Sock]),
    spawn(?MODULE, process_command, [Sock]),
    mio_accept(Listen).

process_command(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Line} ->
            io:fwrite(">~p ~s", [Sock, Line]),
            Token = string:tokens(binary_to_list(Line), " \r\n"),
            case Token of
                ["get", Key] ->
                    process_get(Sock, Key);
%%                 ["get/s", Key1, Key2, Index, Limit] ->
%%                     process_get_s(Sock, Key1, Key2, Index, Limit);
%%                 ["set", Key, Flags, Expire, Bytes] ->
%%                     inet:setopts(Sock,[{packet, raw}]),
%%                     process_set(Sock, Key, Flags, Expire, Bytes),
%%                     inet:setopts(Sock,[{packet, line}]);
%%                 ["set/s", Key, Index, Flags, Expire, Bytes] ->
%%                     inet:setopts(Sock,[{packet, raw}]),
%%                     process_set_s(Sock, Key, list_to_atom(Index), Flags, Expire, Bytes),
%%                     inet:setopts(Sock,[{packet, line}]);
%%                 ["delete", Key] ->
%%                     process_delete(Sock, Key);
                ["quit"] -> gen_tcp:close(Sock);
                _ -> gen_tcp:send(Sock, "ERROR\r\n")
            end,
            process_command(Sock);
        {error, closed} ->
            io:fwrite("<~p connection closed.\n", [Sock]);
        Error ->
            io:fwrite("<~p error: ~p\n", [Sock, Error])
    end.

process_get(Sock, Key) ->
    Value = list_to_binary("value1"),
    gen_tcp:send(Sock, io_lib:format(
                         "VALUE ~s 0 ~w\r\n~s\r\nEND\r\n",
                         [Key, size(Value), Value])).



%% process() ->
%%     receive
%%         _ ->
%%             io:format("hige"),
%%             process()
%%     end.
