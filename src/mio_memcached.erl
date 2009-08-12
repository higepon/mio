%%%-------------------------------------------------------------------
%%% File    : mio_memcached.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : mio application
%%%
%%% Created : 3 Aug 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_memcached).
-export([start_link/0]). %% supervisor needs this.
-export([memcached/1, process_command/2]). %% spawn needs these.
-export([get_boot_node/0]).

-include("mio.hrl").

boot_node_loop(BootNode) ->
    receive
        {From, get} -> From ! BootNode;
        _ -> []
    end,
    boot_node_loop(BootNode).

get_boot_node() ->
    boot_node_loop ! {self(), get},
    receive
        BootNode -> BootNode
    end.

init_start_node(From) ->
    StartNode = case mio_app:get_env(boot_node) of
                    {ok, BootNode} ->
                        rpc:call(BootNode, ?MODULE, get_boot_node, []);
                    _ ->
                        {ok, Node} = mio_sup:start_node("dummy", list_to_binary("dummy"), [1, 0]),
                        register(boot_node_loop, spawn(fun() ->  boot_node_loop(Node) end)),
                        Node
                end,
    case StartNode of
        {badrpc, Reason} ->
            throw({badrpc, Reason});
        _ ->
            From ! {ok, StartNode}
    end.  

%% supervisor calls this to create new memcached.
start_link() ->
    {ok, Port} = mio_app:get_env(port, 11211),
    ?LOG(Port),
    Pid = spawn_link(?MODULE, memcached, [Port]),
    {ok, Pid}.

%% todo: on exit, cleanup socket

%%====================================================================
%% Internal functions
%%====================================================================

memcached(Port) ->
    Self = self(),
    spawn(fun() -> init_start_node(Self) end),
    receive
        {ok, StartNode} ->
            {ok, Listen} =
                gen_tcp:listen(
                  Port, [binary, {packet, line}, {active, false}, {reuseaddr, true}]),
            ?LOGF("< server listening ~p\n", [Port]),
            %%    {ok, BootPid} = mio_sup:start_node("dummy", list_to_binary("dummy"), [1, 0]), %% todo mvector
            mio_accept(Listen, StartNode)
    end.

mio_accept(Listen, StartNode) ->
    {ok, Sock} = gen_tcp:accept(Listen),
    ?LOGF("<~p new client connection\n", [Sock]),
    spawn(?MODULE, process_command, [Sock, StartNode]),
    mio_accept(Listen, StartNode).

process_command(Sock, StartNode) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Line} ->
            ?LOGF(">~p ~s", [Sock, Line]),
            Token = string:tokens(binary_to_list(Line), " \r\n"),
            ?LOGF("<Token:~p>", [Token]),
            case Token of
                ["get", Key] ->
                    process_get(Sock, StartNode, Key);
                ["get", "mio:range-search", Key1, Key2, Limit, "asc"] ->
                    ?LOGF(">range search Key1 =~p Key2=~p Limit=~p\n", [Key1, Key2, Limit]),
                    process_range_search_asc(Sock, StartNode, Key1, Key2, list_to_integer(Limit));
                ["get", "mio:range-search", Key1, Key2, Limit, "desc"] ->
                    ?LOGF(">range search Key1 =~p Key2=~p Limit=~p\n", [Key1, Key2, Limit]),
                    process_range_search_desc(Sock, StartNode, Key1, Key2, list_to_integer(Limit));
                ["set", Key, Flags, Expire, Bytes] ->
                    inet:setopts(Sock,[{packet, raw}]),
                    process_set(Sock, StartNode, Key, Flags, Expire, Bytes),
                    inet:setopts(Sock,[{packet, line}]);
                ["quit"] -> gen_tcp:close(Sock);
                X ->
                    ?LOGF("<Error:~p>", [X]),
                    gen_tcp:send(Sock, "ERROR\r\n")
            end,
            process_command(Sock, StartNode);
        {error, closed} ->
            ?LOGF("<~p connection closed.\n", [Sock]);
        Error ->
            ?LOGF("<~p error: ~p\n", [Sock, Error])
    end.

process_get(Sock, StartNode, Key) ->
    Value = case mio_node:search_op(StartNode, Key) of
                ng -> list_to_binary([]);
                {ok, FoundValue} -> FoundValue
              end,
    gen_tcp:send(Sock, io_lib:format(
                         "VALUE ~s 0 ~w\r\n~s\r\nEND\r\n",
                         [Key, size(Value), Value])).

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

process_set(Sock, Introducer, Key, _Flags, _Expire, Bytes) ->
    case gen_tcp:recv(Sock, list_to_integer(Bytes)) of
        {ok, Value} ->
            {ok, NodeToInsert} = mio_sup:start_node(Key, Value, [1, 0]),
            mio_node:insert_op(Introducer, NodeToInsert),
            gen_tcp:send(Sock, "STORED\r\n");
        {error, closed} ->
            ok;
        Error ->
            ?LOGF("Error: ~p\n", [Error])
    end,
    gen_tcp:recv(Sock, 2).
