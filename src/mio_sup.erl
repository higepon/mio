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
-export([init/1, start_node/3, start_node/4, start_write_serializer/0, terminate_node/1, make_bucket/1, make_bucket/2]).
-include("mio.hrl").

%% supervisor:
%%   On start up, supervisor starts mio_memcached.
%%   mio_memcached starts a dummy node using mio_sup:start_node.
%%   Whenever new node is to create, mio_sup:start_nodes is used.

%% start normal mio_node
start_node(Key, Value, MembershipVector) ->
    start_node(Key, Value, MembershipVector, 0).
start_node(Key, Value, MembershipVector, Expire) ->
    %% node should be 'temporary', since a crash of node means fatal error.
    {ok, _} = supervisor:start_child(mio_sup, {getRandomId(),
                                               {mio_node, start_link, [[Key, Value, MembershipVector, Expire]]},
                                               temporary, brutal_kill, worker, [mio_node]}).

make_bucket(Capacity, Type) ->
    {ok, _} = supervisor:start_child(mio_sup, {getRandomId(),
                                               {mio_bucket, start_link, [[Capacity, Type]]},
                                               temporary, brutal_kill, worker, [mio_bucket]}).
make_bucket(Capacity) ->
    {ok, _} = supervisor:start_child(mio_sup, {getRandomId(),
                                               {mio_bucket, start_link, [[Capacity, alone]]},
                                               temporary, brutal_kill, worker, [mio_bucket]}).



start_write_serializer() ->
    {ok, _} = supervisor:start_child(mio_sup,
                                     {getRandomId(),
                                      {mio_write_serializer, start_link, []},
                                      temporary, brutal_kill, worker, [mio_write_serializer]}).

terminate_node(TargetPid) ->
    lists:any(fun({Id, Pid, _, _}) ->
                      if Pid =:= TargetPid ->
                              supervisor:terminate_child(mio_sup, Id),
                              true;
                         true -> false
                      end end, supervisor:which_children(mio_sup)).
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


init(_Args) ->
    %% getRandomId uses crypto server
    crypto:start(),

    {ok, Port} = mio_app:get_env(port, 11211),
    {ok, MaxLevel} = mio_app:get_env(maxlevel, 10),
    {ok, BootNode} = mio_app:get_env(boot_node, false),
    {ok, LogDir} = mio_app:get_env(log_dir, "."),
    {ok, Verbose} = mio_app:get_env(verbose, false),

    %% However we want to set log Verbose here, we have to wait logger starting up.
    %% So we set Verbose flag on mio_memcached:start_link
    add_disk_logger(LogDir),

    %% todo
    %% Make this simple_one_for_one
    ?PROFILER_START(self()),
    {ok, {{one_for_one, 10, 20},
          %% logger should be the first.
          [
           {logger, {logger, start_link, []},
            permanent, brutal_kill, worker, [logger]},
           {dynomite_prof, {dynomite_prof, start_link, []},
            permanent, brutal_kill, worker, [dynomite_prof]},
           {mio_logger, {mio_logger, start_link, []},
            permanent, brutal_kill, worker, [mio_logger]},
           {mio_memcached, %% this is just id of specification, will not be registered by register/2.
            {mio_memcached, start_link, [Port, MaxLevel, BootNode, Verbose]},
            permanent, brutal_kill, worker, [mio_memcached]}]}}.


getRandomId() ->
    integer_to_list(crypto:rand_uniform(1, 65536 * 65536)).
