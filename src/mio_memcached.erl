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

-include("mio.hrl").

%% supervisor calls this to create new memcached.
start_link() ->
    Pid = spawn_link(?MODULE, memcached, [11211]),
    {ok, Pid}.

%% todo: on exit, cleanup socket

%%====================================================================
%% Internal functions
%%====================================================================

memcached(Port) ->
    {ok, Listen} =
        gen_tcp:listen(
          Port, [binary, {packet, line}, {active, false}, {reuseaddr, true}]),
    ?LOGF("< server listening ~p\n", [Port]),
    {ok, BootPid} = mio_sup:start_node("dummy", list_to_binary("dummy"), [1, 0]), %% todo mvector
    mio_accept(Listen, BootPid).

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
                ["get", "mio:range-search", Key1, Key2, Limit] ->
                    ?LOGF(">range search Key1 =~p Key2=~p Limit=~p\n", [Key1, Key2, Limit]),
                    process_gets(Sock, StartNode, Key1, Key2, list_to_integer(Limit));
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
%%                 ["set/s", Key, Index, Flags, Expire, Bytes] ->
%%                     inet:setopts(Sock,[{packet, raw}]),
%%                     process_set_s(Sock, Key, list_to_atom(Index), Flags, Expire, Bytes),
%%                     inet:setopts(Sock,[{packet, line}]);
%%                 ["delete", Key] ->
%%                     process_delete(Sock, Key);
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
    ?LOG(Value),
    gen_tcp:send(Sock, io_lib:format(
                         "VALUE ~s 0 ~w\r\n~s\r\nEND\r\n",
                         [Key, size(Value), Value])).

process_values([{_, Key, Value} | More]) ->
    ?LOG(Key),
    ?LOG(Value),
    io_lib:format("VALUE ~s 0 ~w\r\n~s\r\n~s",
                  [Key, size(Value), Value, process_values(More)]);
process_values([]) ->
    "END\r\n".


process_gets(Sock, StartNode, Key1, Key2, Limit) ->
    ?LOGF("Key1=~p, Key2=~p\n", [Key1, Key2]),
    Values = mio_node:range_search_op(StartNode, Key1, Key2, Limit),
    ?LOG(Values),
    P = process_values(Values),
    ?LOG(P),
    gen_tcp:send(Sock, P).

process_range_search_asc(Sock, StartNode, Key1, Key2, Limit) ->
    ?LOGF("Key1=~p, Key2=~p\n", [Key1, Key2]),
    Values = mio_node:range_search_asc_op(StartNode, Key1, Key2, Limit),
    ?LOG(Values),
    P = process_values(Values),
    ?LOG(P),
    gen_tcp:send(Sock, P).

process_range_search_desc(Sock, StartNode, Key1, Key2, Limit) ->
    ?LOGF("Key1=~p, Key2=~p\n", [Key1, Key2]),
    Values = mio_node:range_search_desc_op(StartNode, Key1, Key2, Limit),
    ?LOG(Values),
    P = process_values(Values),
    ?LOG(P),
    gen_tcp:send(Sock, P).


process_get_gt(Sock, StartNode, Key, Limit) ->
    Values = mio_node:range_search_gt_op(StartNode, Key, Limit),
    ?LOG(Values),
    P = process_values(Values),
    ?LOG(P),
    gen_tcp:send(Sock, P).

process_get_lt(Sock, StartNode, Key, Limit) ->
    Values = mio_node:range_search_lt_op(StartNode, Key, Limit),
    ?LOG(Values),
    P = process_values(Values),
    ?LOG(P),
    gen_tcp:send(Sock, P).


process_set(Sock, Introducer, Key, _Flags, _Expire, Bytes) ->
    case gen_tcp:recv(Sock, list_to_integer(Bytes)) of
        {ok, Value} ->
            ?LOG(Key),
            ?LOG(Value),
            {ok, NodeToInsert} = mio_sup:start_node(Key, Value, [1, 0]),
            mio_node:insert_op(NodeToInsert, Introducer),
            gen_tcp:send(Sock, "STORED\r\n");
        {error, closed} ->
            ok;
        Error ->
            ?LOGF("Error: ~p\n", [Error])
    end,
    gen_tcp:recv(Sock, 2).




%% process() ->
%%     receive
%%         _ ->
%%             io:format("hige"),
%%             process()
%%     end.
