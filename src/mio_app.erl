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
    case init:get_argument(target_node) of
        {ok,[[Node]]} ->
            ok = rpc:call(list_to_atom(Node), application, stop, [mio]),
            ok = rpc:call(list_to_atom(Node), init, stop, []);
        X ->
            ?ERROR(X)
    end.


start(_Type, _StartArgs) ->
    supervisor:start_link({local, mio_sup}, mio_sup, []).

stop(_State) ->
    ?INFO("mio application stopped"),
    ?PROFILER_STOP(),
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
