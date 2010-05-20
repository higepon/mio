%%    Copyright (C) 2010 Cybozu Labs, Inc.,
%%    written by Taro Minowa(Higepon) <higepon@labs.cybozu.co.jp>
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
%%% File    : mio_app.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : mio application
%%%
%%% Created : 3 Aug 2009 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_app).
-behaviour(application).
-include("mio.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start/0, stop/0, fatal/4, get_env/1, get_env/2, wait_startup/2]).

%% Application callbacks
-export([start/2, stop/1, prep_stop/1]).

%%====================================================================
%% API
%%====================================================================
start() ->
    case application:start(mio) of
        ok ->
            ok;
        {error, {shutdown, Reason}} ->
            ?FATALF("Can't start workers. See more information on ~p.", [Reason, log_file_path()]);
        {error, Reason} ->
            ?FATALF("Application start failed ~p. See more information on ~p.", [Reason, log_file_path()])
    end.


stop() ->
    case init:get_argument(target_node) of
        {ok,[[Node]]} ->
            ok = rpc:call(list_to_atom(Node), application, stop, [mio]),
            ok = rpc:call(list_to_atom(Node), init, stop, []);
        X ->
            ?FATALF("Application stop failed : ~p", [X])
    end.

fatal(Msg, Args, Module, Line) ->
    ?ERRORF("
Fatal Error:

     " ++ Msg ++ " ~p:~p
", Args ++ [Module, Line]),
    init:stop(1).

wait_startup(Host, Port) ->
    wait_startup(10, Host, Port).
wait_startup(0, _Host, _Port) ->
    {error, mio_not_started};
wait_startup(N, Host, Port) ->

    case gen_tcp:connect(Host, Port, []) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            ok;
        {error, econnrefused} ->
            timer:sleep(100),
            wait_startup(N - 1, Host, Port);
        Other ->
            Other
    end.


get_env(Key) ->
    get_env(Key, ng).
get_env(Key, DefaultValue) ->
    case application:get_env(mio, Key) of
        {ok, EnvValue} ->
            {ok, EnvValue};
        _ ->
            {ok, DefaultValue}
    end.


hige() ->
   io:format("start=~p", [net_kernel:start(["hige"])]),
    receive 
        _ ->
            supervisor:start_link({local, mio_sup2}, mio_sup, [second, 11311, 3, false, ".", false])
    end.


start(_Type, _StartArgs) ->
%%     ?assertMatch({ok, _} , supervisor:start_link({local, mio_sup}, mio_sup, [11211, 3, false, ".", false])),
%%     ?assertEqual(ok, mio_app:wait_startup("127.0.0.1", 11211)),
%%     ?assertMatch({ok, _}, supervisor:start_link({local, mio_sup2}, mio_sup, [second, 11311, 3, false, ".", false])).
    supervisor:start_link({local, mio_sup}, mio_sup, []).

stop(_State) ->
    ok.

prep_stop(_State) ->
%%   io:format("stats ~p", [dynomite_prof:stats()]),
    %% N.B.
    %% PROFILER_STOP should be placed here, tty may be closed on stop/1 function.
%%    ?PROFILER_STOP(),
    ok.

log_file_path() ->
    {ok, LogDir} = mio_app:get_env(log_dir, "."),
    LogDir ++ "/mio.log.n".
