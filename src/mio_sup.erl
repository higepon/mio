%%    Copyright (C) 2010 Cybozu Labs, Inc., written by Taro Minowa(Higepon) <higepon@labs.cybozu.co.jp>
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
%%% File    : mio_sup.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : Supervisor
%%%
%%% Created : 30 Jan 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------

-module(mio_sup).
-behaviour(supervisor).
%% API
-export([start_first_mio/6, start_second_mio/6]).
-export([init/1, start_serializer/1, make_bucket/4, make_bucket/5, start_bootstrap/4, start_allocator/1]).
-include("mio.hrl").
-include_lib("eunit/include/eunit.hrl").


%% Start first mio server.
start_first_mio(SupName, Port, MaxLevel, Capacity, LogDir, Verbose) ->
    BootNode = false,
    supervisor:start_link({local, SupName}, mio_sup, [Port, MaxLevel, Capacity, BootNode, LogDir, Verbose]).

%% Start second mio server using introducer "Node"
start_second_mio(SupName, Node, Port, MaxLevel, Capacity, Verbose) ->
    supervisor:start_link({local, SupName}, mio_sup, [Port, MaxLevel, Capacity, Node, Verbose]).


%% supervisor:
%%   On start up, supervisor starts mio_memcached.
%%   mio_memcached starts a dummy node using mio_sup:start_node.
%%   Whenever new node is to create, mio_sup:start_nodes is used.

start_bootstrap(Sup, BootBucket, Allocator, Serializer) ->
    {ok, _} = supervisor:start_child(Sup, {mio_bootstrap,
                                           {mio_bootstrap, start_link, [BootBucket, Allocator, Serializer]},
                                           temporary, brutal_kill, worker, [mio_bootstrap]}).

start_allocator(Sup) ->
    {ok, _} = supervisor:start_child(Sup, {mio_allocator,
                                           {mio_allocator, start_link, []},
                                           temporary, brutal_kill, worker, [mio_allocator]}).


make_bucket(Allocator, Capacity, Type, MembershipVector) ->
    make_bucket(mio_sup, Allocator, Capacity, Type, MembershipVector).

make_bucket(Sup, Allocator, Capacity, Type, MaxLevel) when is_integer(MaxLevel) ->
    make_bucket(Sup, Allocator, Capacity, Type, mio_mvector:generate(MaxLevel));

make_bucket(Supervisor, Allocator, Capacity, Type, MembershipVector) ->
    {ok, _} = supervisor:start_child(Supervisor, {getRandomId(),
                                                  {mio_bucket, start_link, [[Allocator, Capacity, Type, MembershipVector]]},
                                                  temporary, brutal_kill, worker, [mio_bucket]}).


start_serializer(Sup) ->
    {ok, _} = supervisor:start_child(Sup,
                                     {getRandomId(),
                                      {mio_serializer, start_link, []},
                                      temporary, brutal_kill, worker, [mio_serializer]}).


%% Logging policy
%%
%%   1. tty output is OFF by default.
%%   2. Log is written in text asccii format not in binary.
%%   3. Not to use SASL.
%%
add_disk_logger(LogDir) ->
    %% N.B. disk_logger requires tty logger installed.
    error_logger:tty(true),
    Opts = [{name, logger},
            {file, LogDir ++ "/mio.log"},
            {type, wrap},
            {format, external},
            {force_size, true},
            {size, {10 * 1024*1024, 5}}], % 10MB, 5 files
    case gen_event:add_sup_handler(
           error_logger,
           {disk_log_h, logger},
           disk_log_h:init(fun logger:form_no_progress/1, Opts)) of
        ok ->
            ok;
        {error, Reason} ->
            io:format("Error on logger ~p~n", [Reason]),
            halt(1);
        Other ->
            io:format("Error on logger ~p~n", [Other]),
            halt(1)
   end.

%% Used for local second mio server
%% Just start up memcached.
init([Port, MaxLevel, Capacity, BootNode, Verbose]) ->
    Sup = self(),
    {ok, {{one_for_one, 10, 20},
          [{mio_memcached,
            {mio_memcached, start_link, [Sup, Port, MaxLevel, Capacity, BootNode, Verbose]},
            permanent, brutal_kill, worker, [mio_memcached]}]}};

init([Port, MaxLevel, Capacity, BootNode, LogDir, Verbose]) ->
    %% getRandomId uses crypto server
    crypto:start(),

    %% However we want to set log Verbose here, we have to wait logger starting up.
    %% So we set Verbose flag on mio_memcached:start_link
    add_disk_logger(LogDir),
    %% todo
    %% Make this simple_one_for_one
    ?PROFILER_START(self()),
    Sup = self(),
    {ok, {{one_for_one, 10, 20},
          %% logger should be the first.
          [
           {getRandomId(), {logger, start_link, []},
            permanent, brutal_kill, worker, [logger]},
           {dynomite_prof, {dynomite_prof, start_link, []},
            permanent, brutal_kill, worker, [dynomite_prof]},
           {mio_memcached, %% this is just id of specification, will not be registered by register/2.
            {mio_memcached, start_link, [Sup, Port, MaxLevel, Capacity, BootNode, Verbose]},
            permanent, brutal_kill, worker, [mio_memcached]}]}};

init([]) ->
    {ok, Port} = mio_app:get_env(port, 11211),
    {ok, MaxLevel} = mio_app:get_env(maxlevel, 10),
    {ok, BootNode} = mio_app:get_env(boot_node, false),
    {ok, Capacity} = mio_app:get_env(capacity, 1000),
    {ok, LogDir} = mio_app:get_env(log_dir, "."),
    {ok, Verbose} = mio_app:get_env(verbose, false),
    init([Port, MaxLevel, Capacity, BootNode, LogDir, Verbose]).


getRandomId() ->
    integer_to_list(crypto:rand_uniform(1, 65536 * 65536)).
