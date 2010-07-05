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
-export([memcached/5, process_request/3]).
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
            ok = mio_local_store:set(LocalSetting, start_buckets, [Bucket]),
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
            ok = mio_local_store:set(LocalSetting, start_buckets, [BootBucket]),
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

            mio_stats:init(LocalSetting),

            %% backlog value is same as on memcached
            case gen_tcp:listen(Port, [binary, {packet, line}, {active, false}, {reuseaddr, true}, {backlog, 1024}]) of
                {ok, Listen} ->
                    mio_accept(Listen, Serializer, LocalSetting);
                {error, eaddrinuse} ->
                    ?FATALF("Port ~p is in use", [Port]);
                {error, Reason} ->
                    ?FATALF("Can't start memcached compatible server : ~p", [Reason])
            end
    catch
         throw:Reason -> ?FATALF("~p", [Reason])
    end.

mio_accept(Listen, Serializer, LocalSetting) ->
    case gen_tcp:accept(Listen) of
        {ok, Sock} ->
            spawn_link(?MODULE, process_request, [Sock, Serializer, LocalSetting]),
            mio_accept(Listen, Serializer, LocalSetting);
        Other ->
            ?FATALF("accept returned ~w",[Other])
    end.

now_in_msec() ->
    {MegaSec, Sec, MicroSec} = now(),
    MegaSec * 1000 * 1000 * 1000 + Sec * 1000 + MicroSec / 1000.

process_request(Sock, Serializer, LocalSetting) ->
    {ok, [StartBucket | _]} = mio_local_store:get(LocalSetting, start_buckets),
    case gen_tcp:recv(Sock, 0) of
        {ok, Line} ->
            Token = string:tokens(binary_to_list(Line), " \r\n"),
%%            ?INFO(Token),
            case Token of
                ["get_start_bucket"] ->
                    ok = gen_tcp:send(Sock, term_to_binary(StartBucket)),
                    process_request(Sock, Serializer, LocalSetting);
                ["get", Key] ->
                    Start = now_in_msec(),
                    process_get(Sock, StartBucket, Serializer, Key),
                    End = now_in_msec(),
                    mio_stats:inc_cmd_get(LocalSetting),
                    mio_stats:inc_cmd_get_total_time(LocalSetting, End - Start),
                    mio_stats:set_cmd_get_worst_time(LocalSetting, End - Start),
                    process_request(Sock, Serializer, LocalSetting);
                ["get", "mio:range-search", Key1, Key2, Limit, "asc"] ->
                    Start = now_in_msec(),
                    process_range_search_asc(Sock, StartBucket, Serializer, LocalSetting, Key1, Key2, list_to_integer(Limit)),
                    End = now_in_msec(),
                    mio_stats:inc_cmd_get_multi(LocalSetting),
                    mio_stats:inc_cmd_get_multi_total_time(LocalSetting, End - Start),
                    mio_stats:set_cmd_get_multi_worst_time(LocalSetting, End - Start),
                    process_request(Sock, Serializer, LocalSetting);
                ["get", "mio:range-search", Key1, Key2, Limit, "desc"] ->
                    Start = now_in_msec(),
                    process_range_search_desc(Sock, StartBucket, Serializer, LocalSetting, Key1, Key2, list_to_integer(Limit)),
                    End = now_in_msec(),
                    mio_stats:inc_cmd_get_multi(LocalSetting),
                    mio_stats:inc_cmd_get_multi_total_time(LocalSetting, End - Start),
                    mio_stats:set_cmd_get_multi_worst_time(LocalSetting, End - Start),
                    process_request(Sock, Serializer, LocalSetting);
                ["set", "mio:sweep", _, _, _] ->
                    ok = gen_tcp:send(Sock, "STORED\r\n"),
                    process_request(Sock, Serializer, LocalSetting);
                ["set", Key, Flags, ExpirationTime, Bytes] ->
                    inet:setopts(Sock,[{packet, raw}]),
                    process_set(Sock, StartBucket, LocalSetting, Key, Flags, list_to_integer(ExpirationTime), Bytes, Serializer),
                    inet:setopts(Sock,[{packet, line}]),
                    process_request(Sock, Serializer, LocalSetting);
                ["delete", Key] ->
                    process_delete(Sock, StartBucket, LocalSetting, Serializer, Key),
                    process_request(Sock, Serializer, LocalSetting);
%%                 ["delete", Key, _Time] ->
%%                     process_delete(Sock, StartBucket, Key),
%%                     process_request(Sock, StartBucketEts, Serializer);
%%                 ["delete", Key, _Time, _NoReply] ->
%%                     process_delete(Sock, StartBucket, Key),
%%                     process_request(Sock, StartBucketEts, Serializer);
%%                 ["quit"] ->
%%                     ok = gen_tcp:close(Sock);
                ["stats"] ->
                    process_stats(Sock, LocalSetting),
                    process_request(Sock, Serializer, LocalSetting);
                X ->
                    ?ERRORF("Unknown memcached command error: ~p\n", [X]),
                    ok = gen_tcp:send(Sock, "ERROR\r\n")
            end;
        {error, closed} ->
            ok = gen_tcp:close(Sock);
        Error ->
            ?ERRORF("memcached socket error: ~p\n", [Error])
    end.

make_stats([]) ->
    "END\r\n";
make_stats([{StatKey, StatValue} | More]) ->
    io_lib:format("STAT ~s ~p\r\n~s", [StatKey, StatValue, make_stats(More)]).

process_stats(Sock, LocalSetting) ->
    Stats = [{uptime, mio_stats:uptime(LocalSetting)},
             {total_items, mio_stats:total_items(LocalSetting)},
             {cmd_get, mio_stats:cmd_get(LocalSetting)},
             {bytes, mio_stats:bytes()},
             {cmd_get_multi_avg_time, mio_stats:cmd_get_multi_avg_time(LocalSetting)},
             {cmd_get_multi_worst_time, mio_stats:cmd_get_multi_worst_time(LocalSetting)},
             {cmd_get_avg_time, mio_stats:cmd_get_avg_time(LocalSetting)},
             {cmd_get_worst_time, mio_stats:cmd_get_worst_time(LocalSetting)}
             ],
    ok = gen_tcp:send(Sock, make_stats(Stats)).

process_delete(Sock, StartBucket, LocalSetting, Serializer, Key) ->
    case mio_serializer:delete_op(Serializer, StartBucket, Key) of
        {ok, DeletedBuckets} ->
            ok = gen_tcp:send(Sock, "DELETED\r\n"),
            case mio_local_store:get(LocalSetting, start_buckets) of
                {ok, [_BootBucket]} ->
                    ok;
                {ok, [StartBucket, BootBucket]} ->
                    %% If start_bucket is deleted, we have to replace the start bucket.
                    %% N.B. We are sure BootBucket will never be deleted.
                    case lists:member(StartBucket, DeletedBuckets) of
                        true ->
                            ok = mio_local_store:set(LocalSetting, start_buckets, [BootBucket]);
                        _ ->
                            ok
                    end
            end;
        _ ->
            ok = gen_tcp:send(Sock, "NOT_FOUND\r\n")
    end.

enqueue_to_delete(Serializer, StartBucket, Key) ->
    spawn_link(fun () -> mio_serializer:delete_op(Serializer, StartBucket, Key) end).

process_get(Sock, StartBucket, Serializer, Key) ->
    case mio_skip_graph:search_op(StartBucket, Key) of
        {ok, Value, ExpirationTime} ->
            case ExpirationTime of
                ?NEVER_EXPIRE ->
                    ok = gen_tcp:send(Sock, io_lib:format(
                                              "VALUE ~s 0 ~w\r\n~s\r\nEND\r\n",
                                              [Key, size(Value), Value]));
                ?MARKED_EXPIRED ->
                    ok = gen_tcp:send(Sock, "END\r\n");
                _ ->
                    case ExpirationTime =< unixtime() of
                        true ->
                            enqueue_to_delete(Serializer, StartBucket, Key),
                            mio_serializer:insert_op(Serializer, StartBucket, Key, Value, ?MARKED_EXPIRED),
                            ok = gen_tcp:send(Sock, "END\r\n");
                        false ->
                            ok = gen_tcp:send(Sock, io_lib:format(
                                                      "VALUE ~s 0 ~w\r\n~s\r\nEND\r\n",
                                                      [Key, size(Value), Value]))
                    end
            end;
        {error, not_found} ->
            ok = gen_tcp:send(Sock, "END\r\n")
    end.

filter_expired(Serializer, StartBucket, Values) ->
    lists:map(fun({Key, {Value, _ExpirationTime}}) ->
                 {Key, Value}
              end,
              lists:filter(fun({Key, {Value, ExpirationTime}}) ->
                                   case ExpirationTime of
                                       ?NEVER_EXPIRE ->
                                           true;
                                       ?MARKED_EXPIRED ->
                                           false;
                                       _ ->
                                           case unixtime() < ExpirationTime of
                                               true ->
                                                   true;
                                               false ->
                                                   enqueue_to_delete(Serializer, StartBucket, Key),
                                                   mio_serializer:insert_op(Serializer, StartBucket, Key, Value, ?MARKED_EXPIRED),
                                                   false
                                           end
                                   end
                           end,
                           Values)).

process_range_search_asc(Sock, StartBucket, Serializer, LocalSetting, Key1, Key2, Limit) ->
    mio_stats:inc_cmd_get_multi(LocalSetting),
    Values = mio_skip_graph:range_search_asc_op(StartBucket, Key1, Key2, Limit),
    P = process_values(filter_expired(Serializer, StartBucket, Values)),
    ok = gen_tcp:send(Sock, P).

process_range_search_desc(Sock, StartNode, Serializer, LocalSetting, Key1, Key2, Limit) ->
    mio_stats:inc_cmd_get_multi(LocalSetting),
    Values = mio_skip_graph:range_search_desc_op(StartNode, Key1, Key2, Limit),
    P = process_values(filter_expired(Serializer, StartNode, Values)),
    ok = gen_tcp:send(Sock, P).


process_values([{Key, Value} | More]) ->
    io_lib:format("VALUE ~s 0 ~w\r\n~s\r\n~s",
                  [Key, size(Value), Value, process_values(More)]);
process_values([]) ->
    "END\r\n".

unixtime() ->
    {Msec, Sec, _} = now(),
    Msec * 1000 + Sec.

normalize_expiration_time(Time) when Time =:= ?NEVER_EXPIRE ->
    ?NEVER_EXPIRE;
normalize_expiration_time(Time) when Time > 60*60*24*30 ->
    Time;
normalize_expiration_time(Time) ->
    unixtime() + Time.

process_set(Sock, Introducer, LocalSetting, Key, _Flags, ExpirationTime, Bytes, Serializer) ->
    case gen_tcp:recv(Sock, list_to_integer(Bytes)) of
        {ok, Value} ->

            case mio_skip_graph:search_op(Introducer, Key) of
                {error, not_found} ->
                    mio_stats:inc_total_items(LocalSetting);
                _ -> ok
            end,

            {ok, NewlyAllocatedBucket} = mio_serializer:insert_op(Serializer, Introducer, Key, Value, normalize_expiration_time(ExpirationTime)),

            ok = gen_tcp:send(Sock, "STORED\r\n"),
            {ok, _Data} = gen_tcp:recv(Sock, 2),
            %% It's preferable that start buckt is local.
            case mio_util:is_local_process(NewlyAllocatedBucket) of
                true ->
                    case mio_local_store:get(LocalSetting, start_buckets) of
                        {ok, [BootBucket]} ->
                            ok = mio_local_store:set(LocalSetting, start_buckets, [NewlyAllocatedBucket, BootBucket]);
                        {ok, [_ ,BootBucket]} ->
                            ok = mio_local_store:set(LocalSetting, start_buckets, [NewlyAllocatedBucket, BootBucket])
                    end;
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
