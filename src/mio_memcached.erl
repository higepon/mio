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
%%% File    : mio_memcached.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : mio memcached I/F
%%%
%%% Created : 3 Aug 2009 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_memcached).
-export([start_link/6]).
-export([memcached/5, process_request/4]).
-include_lib("eunit/include/eunit.hrl").
-include("mio.hrl").

init_start_node(Sup, MaxLevel, Capacity, BootNode) ->
    {ok, LocalSetting} = mio_local_store:new(),

    case BootNode of
        %% Bootstrap
        false ->
            {ok, Serializer} = mio_sup:start_serializer(Sup),
            {ok, Allocator} = mio_sup:start_allocator(Sup),
            ok = mio_allocator:add_node(Allocator, Sup),
            {ok, Bucket} = mio_sup:make_bucket(Sup, Allocator, Capacity, alone, MaxLevel),
            ok = mio_local_store:set(LocalSetting, start_bucket, Bucket),
            %% N.B.
            %%   For now, bootstrap server is SPOF.
            %%   This should be replaced with Mnesia(ram_copy).
            {ok, _BootStrap} = mio_sup:start_bootstrap(Sup, Bucket, Allocator, Serializer),
            {Serializer, LocalSetting};
        %% Introducer bootnode exists
        _ ->
            {ok, BootBucket, Allocator, Serializer} = mio_bootstrap:get_boot_info(BootNode),
            Supervisor = whereis(mio_sup),
            ok = mio_allocator:add_node(Allocator, Supervisor),
            ok = mio_local_store:set(LocalSetting, start_bucket, BootBucket),
            {Serializer, LocalSetting}
    end.


%% we should check boot node is exist?
is_boot_node_exists(false) ->
    true;
is_boot_node_exists(BootNode) ->
    try mio_bootstrap:get_boot_info(BootNode) of
        {ok, _BootBucket, _Allocator, _Serializer} ->
            true
    catch
        exit:_ ->
            false
    end.

%% supervisor calls this to create new memcached.
start_link(Sup, Port, MaxLevel, Capacity, BootNode, Verbose) ->
    error_logger:tty(Verbose),
    case is_boot_node_exists(BootNode) of
        true ->
            Pid = spawn_link(?MODULE, memcached, [Sup, Port, MaxLevel, Capacity, BootNode]),
            {ok, Pid};
        _ ->
            {error, "introducer not found"}
    end.

%%====================================================================
%% Internal functions
%%====================================================================
memcached(Sup, Port, MaxLevel, Capacity, BootNode) ->
    try init_start_node(Sup, MaxLevel, Capacity, BootNode) of
        {Serializer, LocalSetting} ->
            %% backlog value is same as on memcached
            case gen_tcp:listen(Port, [binary, {packet, line}, {active, false}, {reuseaddr, true}, {backlog, 1024}]) of
                {ok, Listen} ->
                    mio_accept(Listen, MaxLevel, Serializer, LocalSetting);
                {error, eaddrinuse} ->
                    ?FATALF("Port ~p is in use", [Port]);
                {error, Reason} ->
                    ?FATALF("Can't start memcached compatible server : ~p", [Reason])
            end
    catch
         throw:Reason -> ?FATALF("~p", [Reason])
    end.

mio_accept(Listen, MaxLevel, Serializer, LocalSetting) ->
    case gen_tcp:accept(Listen) of
        {ok, Sock} ->
            spawn_link(?MODULE, process_request, [Sock, MaxLevel, Serializer, LocalSetting]),
            mio_accept(Listen, MaxLevel, Serializer, LocalSetting);
        Other ->
            ?FATALF("accept returned ~w",[Other])
    end.


process_request(Sock, MaxLevel, Serializer, LocalSetting) ->
    {ok, StartBucket} = mio_local_store:get(LocalSetting, start_bucket),
    case gen_tcp:recv(Sock, 0) of
        {ok, Line} ->
            Token = string:tokens(binary_to_list(Line), " \r\n"),
%%            ?INFO(Token),
            case Token of
                ["get", Key] ->
                    process_get(Sock, StartBucket, Key),
                    process_request(Sock, MaxLevel, Serializer, LocalSetting);
%%                 ["get", "mio:range-search", Key1, Key2, Limit, "asc"] ->
%%                     process_range_search_asc(Sock, StartBucket, Key1, Key2, list_to_integer(Limit)),
%%                     process_request(Sock, StartBucketEts, MaxLevel, Serializer);
%%                 ["get", "mio:range-search", Key1, Key2, Limit, "desc"] ->
%%                     process_range_search_desc(Sock, StartBucket, Key1, Key2, list_to_integer(Limit)),
%%                     process_request(Sock, StartBucketEts, MaxLevel, Serializer);
                ["set", Key, Flags, ExpireDate, Bytes] ->
                    inet:setopts(Sock,[{packet, raw}]),
%%                    ?INFOF("Start set ~p", [self()]),
                    process_set(Sock, StartBucket, LocalSetting, Key, Flags, list_to_integer(ExpireDate), Bytes, MaxLevel, Serializer),
%%                    ?INFOF("End set ~p", [self()]),
                    %% process_set increses process memory size and never shrink.
                    %% We have to collect them here.
%%                    erlang:garbage_collect(InsertedNode),

                    inet:setopts(Sock,[{packet, line}]),
                    process_request(Sock, MaxLevel, Serializer, LocalSetting);
%%                 ["delete", Key] ->
%%                     process_delete(Sock, StartBucket, Key),
%%                     process_request(Sock, StartBucketEts, MaxLevel, Serializer);
%%                 ["delete", Key, _Time] ->
%%                     process_delete(Sock, StartBucket, Key),
%%                     process_request(Sock, StartBucketEts, MaxLevel, Serializer);
%%                 ["delete", Key, _Time, _NoReply] ->
%%                     process_delete(Sock, StartBucket, Key),
%%                     process_request(Sock, StartBucketEts, MaxLevel, Serializer);
%%                 ["quit"] ->
%%                     ok = gen_tcp:close(Sock);
%%                 ["stats"] ->
%%                     process_stats(Sock, StartBucket, MaxLevel),
%%                     process_request(Sock, StartBucketEts, MaxLevel, Serializer);
                X ->
                    ?ERRORF("Unknown memcached command error: ~p\n", [X]),
                    ok = gen_tcp:send(Sock, "ERROR\r\n")
            end;
        {error, closed} ->
            ok;
        Error ->
            ?ERRORF("memcached socket error: ~p\n", [Error])
    end.


%% process_stats(Sock, Node, MaxLevel) ->
%%     mio_skip_graph:dump_op(Node),
%%     ok = gen_tcp:send(Sock, io_lib:format("STAT ~s ~s\r\nEND\r\n", [hoge, hoge])).


%% process_delete(Sock, StartNode, Key) ->
%%     exit({todo, ?LINE}).

%% %% Expiry format definition
%% %% Expire:
%% %%   0 -> never expire
%% %%   -1 -> expired and enqueued to delete queue
%% %%   greater than zero -> expiration date in Unix time format
%% %% Returns {Expired?, NeedEnqueue}
%% check_expired(0) ->
%%     exit({todo, ?LINE}),
%%     {false, false};
%% check_expired(-1) ->
%%     exit({todo, ?LINE}),
%%     {true, false};
%% check_expired(ExpireDate) ->
%%     exit({todo, ?LINE}),
%%     Expired = ExpireDate =< unixtime(),
%%     {Expired, Expired}.


%% todo expire
process_get(Sock, StartNode, Key) ->
    case mio_skip_graph:search_op(StartNode, Key) of
        {ok, Value} ->
            ok = gen_tcp:send(Sock, io_lib:format(
                                      "VALUE ~s 0 ~w\r\n~s\r\nEND\r\n",
                                      [Key, size(Value), Value]));
        {error, not_found} ->
            ok = gen_tcp:send(Sock, "END\r\n")
    end.
%% %%    erlang:garbage_collect(Node),

%% %%     %% enqueue to the delete queue
%% %%     if NeedEnqueue ->
%% %%             enqueue_to_delete(WriteSerializer, Node);
%% %%        true -> []
%% %%     end.

%% process_values([{_, Key, Value, _} | More]) ->
%%     io_lib:format("VALUE ~s 0 ~w\r\n~s\r\n~s",
%%                   [Key, size(Value), Value, process_values(More)]);
%% process_values([]) ->
%%     "END\r\n".

%% enqueue_to_delete(WriteSerializer, Node) ->
%%     mio_node:set_expire_time_op(Node, -1),
%%     spawn_link(fun() -> mio_write_serializer:delete_op(WriteSerializer, Node) end).

%% filter_expired(WriteSerializer, Values) ->
%%     lists:filter(fun({Node, _, _, ExpireDate}) ->
%%                          {Expired, NeedEnqueue} = check_expired(ExpireDate),
%%                          if NeedEnqueue ->
%%                                  enqueue_to_delete(WriteSerializer, Node);
%%                             true -> []
%%                          end,
%%                          not Expired
%%                  end, Values).

%% process_range_search_asc(Sock, StartNode, Key1, Key2, Limit) ->
%%     exit({todo, ?LINE}).
%% %%     Values = mio_node:range_search_asc_op(StartNode, Key1, Key2, Limit),
%% %%     ActiveValues = filter_expired(WriteSerializer, Values),
%% %%     P = process_values(ActiveValues),
%% %%     ok = gen_tcp:send(Sock, P).

%% process_range_search_desc(Sock, StartNode, Key1, Key2, Limit) ->
%%     exit({todo, ?LINE}).
%% %%     Values = mio_node:range_search_desc_op(StartNode, Key1, Key2, Limit),
%% %%     ActiveValues = filter_expired(WriteSerializer, Values),
%% %%     P = process_values(ActiveValues),
%% %%     ok = gen_tcp:send(Sock, P).

%% %% See expiry format definition on process_get
process_set(Sock, Introducer, LocalSetting, Key, _Flags, _ExpireDate, Bytes, _MaxLevel, Serializer) ->
    case gen_tcp:recv(Sock, list_to_integer(Bytes)) of
        {ok, Value} ->
            {ok, NewlyAllocatedBucket} = mio_serializer:insert_op(Serializer, Introducer, Key, Value),
            ok = gen_tcp:send(Sock, "STORED\r\n"),
            {ok, _Data} = gen_tcp:recv(Sock, 2),
            %% It's preferable that start buckt is local.
            case mio_util:is_local_process(NewlyAllocatedBucket) of
                true ->
                    %%            io:format("registered ~p ~p ~p~n", [NewlyAllocatedBucket, node(), node(NewlyAllocatedBucket)]),
                    ok = mio_local_store:set(LocalSetting, start_bucket, NewlyAllocatedBucket);
                _ ->
                    []
            end;
        {error, closed} ->
            ok;
        Error ->
           ?ERRORF("Error: ~p\n", [Error]),
            {ok, _Data} = gen_tcp:recv(Sock, 2),
            Introducer
    end.

%% unixtime() ->
%%     {Msec, Sec, _} = now(),
%%     Msec * 1000 + Sec.
