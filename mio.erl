% Based on http://dsas.blog.klab.org/archives/51094713.html

% Memcached protocol
% http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt

-module(mio).
-export([start/0, mio/1, process_command/1]).
-import(mio).

start() ->
    register(mio, spawn(?MODULE, mio, [11211])).

mio(Port) ->
    miodb:new(item),
    {ok, Listen} =
        gen_tcp:listen(
          Port, [binary, {packet, line}, {active, false}, {reuseaddr, true}]),
    io:fwrite("< server listening ~p\n", [Port]),
    mio_accept(Listen).

mio_accept(Listen) ->
    {ok, Sock} = gen_tcp:accept(Listen),
    io:fwrite("<~p new client connection\n", [Sock]),
    spawn(?MODULE, process_command, [Sock]),
    mio_accept(Listen).

process_command(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Line} ->
            io:fwrite(">~p ~s", [Sock, Line]),
            Token = string:tokens(binary_to_list(Line), " \r\n"),
            case Token of
                ["get", Key] ->
                    process_get(Sock, Key);
                ["set", Key, Flags, Expire, Bytes] ->
                    inet:setopts(Sock,[{packet, raw}]),
                    process_set(Sock, Key, Flags, Expire, Bytes),
                    inet:setopts(Sock,[{packet, line}]);
                ["delete", Key] ->
                    process_delete(Sock, Key);
                ["quit"] -> gen_tcp:close(Sock);
                _ -> gen_tcp:send(Sock, "ERROR\r\n")
            end,
            process_command(Sock);
        {error, closed} ->
            io:fwrite("<~p connection closed.\n", [Sock]);
        Error ->
            io:fwrite("<~p error: ~p\n", [Sock, Error])
    end.

process_get(Sock, Key) ->
    case miodb:get_value(item, Key) of
        no_exists ->
            gen_tcp:send(Sock, "END\r\n");
        Value ->
            gen_tcp:send(Sock, io_lib:format(
                                 "VALUE ~s 0 ~w\r\n~s\r\nEND\r\n",
                                 [Key, size(Value), Value]))
    end.

process_set(Sock, Key, _Flags, _Expire, Bytes) ->
    case gen_tcp:recv(Sock, list_to_integer(Bytes)) of
        {ok, Value} ->
            miodb:set_value(item, Key, Value,
                                    case list_to_integer(_Expire) of
                                        0 -> 0;
                                        Expire -> Expire
                                    end),
            gen_tcp:send(Sock, "STORED\r\n");
        {error, closed} ->
            ok;
        Error ->
            io:fwrite("Error: ~p\n", [Error])
    end,
    gen_tcp:recv(Sock, 2).

process_delete(Sock, Key) ->
    case ets:lookup(item, Key) of
        [{_, _}] ->
            miodb:delete(item, Key),
            gen_tcp:send(Sock, "DELETED\r\n");
        _ ->
            gen_tcp:send(Sock, "NOT_FOUND\r\n")
    end.

