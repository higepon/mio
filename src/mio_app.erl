%%%-------------------------------------------------------------------
%%% File    : mio.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : mio application
%%%
%%% Created : 3 Aug 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_app).

-behaviour(application).
-import(mio).
%% Application callbacks
-export([start/2, stop/1]).

%% For init script
-export([start/0, stop/0]).

%% Utility
-export([get_env/1, get_env/2]).

-include("mio.hrl").

start() ->
    application:start(mio).

stop() ->
    ?LOG(stopped),
    case init:get_argument(target_node) of
        {ok,[[Node]]} ->
    ?LOG(moge),
            ok = rpc:call(list_to_atom(Node), application, stop, [mio]),
            ok = rpc:call(list_to_atom(Node), init, stop, []);
        X -> ?LOG(X)
    end.


start(_Type, _StartArgs) ->
    case application:get_env(mio, debug) of
        {ok, IsDebugMode} ->
            if IsDebugMode =:= true ->
                    [];
               true ->
                    error_logger:tty(false)
            end;
        _ -> []
    end,
    supervisor:start_link({local, mio_sup}, mio_sup, []).

stop(_State) ->
    ?LOG(stopped),
    ok.

get_env(Key) ->
    case application:get_env(mio, Key) of
        {ok, EnvValue} ->
            {ok, EnvValue};
        _ ->
            ng
    end.
get_env(Key, DefaultValue) ->
    case application:get_env(mio, Key) of
        {ok, EnvValue} ->
            {ok, EnvValue};
        _ ->
            {ok, DefaultValue}
    end.
