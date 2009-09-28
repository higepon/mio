%%%-------------------------------------------------------------------
%%% File    : mio_memcached.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : mio application
%%%
%%% Created : 3 Aug 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_memcached).
-export([start_link/3]). %% supervisor needs this.
-export([memcached/3, process_command/4]). %% spawn needs these.
-export([get_boot_node/0]).

-include("mio.hrl").

boot_node_loop(BootNode, Serializer) ->
    receive
        {From, get} -> From ! {BootNode, Serializer};
        _ -> []
    end,
    boot_node_loop(BootNode, Serializer).

get_boot_node() ->
    boot_node_loop ! {self(), get},
    receive
        {BootNode, Serializer} -> {BootNode, Serializer}
    end.

init_start_node(From, MaxLevel, BootNode) ->
    {StartNode, Serializer} = case BootNode of
                    [] ->
                        MVector = mio_mvector:generate(MaxLevel),
                        {ok, Node} = mio_sup:start_node("dummy", list_to_binary("dummy"), MVector),
                        {ok, WriteSerializer} = mio_sup:start_write_serializer(),
                        register(boot_node_loop, spawn(fun() ->  boot_node_loop(Node, WriteSerializer) end)),
                        {Node, WriteSerializer};
                    _ ->
                        case rpc:call(BootNode, ?MODULE, get_boot_node, []) of
                            {badrpc, Reason} ->
                                throw({"Can't start, introducer node not found", {badrpc, Reason}});
                            {Introducer, MySerializer} ->
                                {ok, Node} = mio_sup:start_node("dummy"++BootNode, list_to_binary("dummy"), mio_mvector:generate(MaxLevel)),
                                mio_node:insert_op(Introducer, Node),
                                {Node, MySerializer}
                        end
                end,
    From ! {ok, StartNode, Serializer}.

%% supervisor calls this to create new memcached.
start_link(Port, MaxLevel, BootNode) ->
    Pid = spawn_link(?MODULE, memcached, [Port, MaxLevel, BootNode]),
    {ok, Pid}.

%% todo: on exit, cleanup socket

%%====================================================================
%% Internal functions
%%====================================================================

memcached(Port, MaxLevel, BootNode) ->
    Self = self(),
    spawn(fun() -> init_start_node(Self, MaxLevel, BootNode) end),
    receive
        {ok, StartNode, WriteSerializer} ->
            {ok, Listen} = gen_tcp:listen(Port, [binary, {packet, line}, {active, false}, {reuseaddr, true}]),
            mio_accept(Listen, WriteSerializer, StartNode, MaxLevel)
    end.

mio_accept(Listen, WriteSerializer, StartNode, MaxLevel) ->
    {ok, Sock} = gen_tcp:accept(Listen),
   io:format("<~p new client connection\n", [Sock]),
    spawn(?MODULE, process_command, [Sock, WriteSerializer, StartNode, MaxLevel]),
    mio_accept(Listen, WriteSerializer, StartNode, MaxLevel).

process_command(Sock, WriteSerializer, StartNode, MaxLevel) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Line} ->
%%            ?LOGF(">~p ~s", [Sock, Line]),
            Token = string:tokens(binary_to_list(Line), " \r\n"),
%%            ?LOGF("<Token:~p>", [Token]),
            NewStartNode =
            case Token of
                ["get", Key] ->
                    process_get(Sock, StartNode, Key),
                    StartNode;
                ["get", "mio:range-search", Key1, Key2, Limit, "asc"] ->
                    ?LOGF(">range search Key1 =~p Key2=~p Limit=~p\n", [Key1, Key2, Limit]),
                    process_range_search_asc(Sock, StartNode, Key1, Key2, list_to_integer(Limit)),
                    StartNode;
                ["get", "mio:range-search", Key1, Key2, Limit, "desc"] ->
                    ?LOGF(">range search Key1 =~p Key2=~p Limit=~p\n", [Key1, Key2, Limit]),
                    process_range_search_desc(Sock, StartNode, Key1, Key2, list_to_integer(Limit)),
                    StartNode;
                ["set", Key, Flags, Expire, Bytes] ->
                    inet:setopts(Sock,[{packet, raw}]),
                    InsertedNode = process_set(Sock, WriteSerializer, StartNode, Key, Flags, Expire, Bytes, MaxLevel),
                    inet:setopts(Sock,[{packet, line}]),
                    StartNode;
%%                    InsertedNode;
                ["delete", Key] ->
                    process_delete(Sock, WriteSerializer, StartNode, Key),
                    StartNode;
                ["delete", Key, _Time] ->
                    process_delete(Sock, WriteSerializer, StartNode, Key),
                    StartNode;
                ["delete", Key, _Time, _NoReply] ->
                    process_delete(Sock, WriteSerializer, StartNode, Key),
                    StartNode;
                ["quit"] -> gen_tcp:close(Sock);
                X ->
%%                    ?LOGF("<Error:~p>", [X]),
                    gen_tcp:send(Sock, "ERROR\r\n"),
                    StartNode
            end,
            process_command(Sock, WriteSerializer, NewStartNode, MaxLevel);
        {error, closed} ->
            ok;
%            ?LOGF("<~p connection closed.\n", [Sock]);
        Error ->
            ?ERRORF("<~p error: ~p\n", [Sock, Error])
    end.

process_delete(Sock, WriteSerializer, StartNode, Key) ->
    case mio_write_serializer:delete_op(WriteSerializer, StartNode, Key) of
        ng ->
            gen_tcp:send(Sock, "NOT_FOUND\r\n");
        _ ->
            gen_tcp:send(Sock, "DELETED\r\n")
    end.

process_get(Sock, StartNode, Key) ->
    case mio_node:search_op(StartNode, Key) of
        ng ->
            gen_tcp:send(Sock, "END\r\n");
        {ok, FoundValue} ->
            gen_tcp:send(Sock, io_lib:format(
                                 "VALUE ~s 0 ~w\r\n~s\r\nEND\r\n",
                                 [Key, size(FoundValue), FoundValue]))
    end.


process_values([{_, Key, Value} | More]) ->
    io_lib:format("VALUE ~s 0 ~w\r\n~s\r\n~s",
                  [Key, size(Value), Value, process_values(More)]);
process_values([]) ->
    "END\r\n".

process_range_search_asc(Sock, StartNode, Key1, Key2, Limit) ->
    Values = mio_node:range_search_asc_op(StartNode, Key1, Key2, Limit),
    P = process_values(Values),
    gen_tcp:send(Sock, P).

process_range_search_desc(Sock, StartNode, Key1, Key2, Limit) ->
    Values = mio_node:range_search_desc_op(StartNode, Key1, Key2, Limit),
    P = process_values(Values),
    gen_tcp:send(Sock, P).

process_set(Sock, WriteSerializer, Introducer, Key, _Flags, _Expire, Bytes, MaxLevel) ->
    case gen_tcp:recv(Sock, list_to_integer(Bytes)) of
        {ok, Value} ->
%            ?LOGF(">set Key =~p Value=~p\n", [Key, Value]),
            MVector = mio_mvector:generate(MaxLevel),
%            ?LOGF("search-hige:insert_=~p ~p\n", [Key, MVector]),
%%            ?LOG(MVector),
%            {ok, NodeToInsert} = mio_sup:start_node(Key, true, MVector),
            {ok, NodeToInsert} = mio_sup:start_node(Key, Value, MVector),
            ?LOGF("memcached~p:NodeToInsert=~p ~n", [self(), NodeToInsert]),
%%            mio_node:insert_op(Introducer, NodeToInsert),
            mio_write_serializer:insert_op(WriteSerializer, Introducer, NodeToInsert),
            gen_tcp:send(Sock, "STORED\r\n"),
            gen_tcp:recv(Sock, 2),
            NodeToInsert;
        {error, closed} ->
            ok;
        Error ->
           ?ERRORF("Error: ~p\n", [Error]),
            gen_tcp:recv(Sock, 2),
            Introducer
    end.
