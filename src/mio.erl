%%%-------------------------------------------------------------------
%%% File    : mio.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : mio application
%%%
%%% Created : 29 Jun 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).
%% Utility
-export([start/0]).

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
    mio_sup:start_link().
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

%% process() ->
%%     receive
%%         _ ->
%%             io:format("hige"),
%%             process()
%%     end.
