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
    {StartNode, Serializer}
        = case BootNode of
              [] ->
                  MVector = mio_mvector:generate(MaxLevel),
                  {ok, Node} = mio_sup:start_node("dummy", list_to_binary("dummy"), MVector),
                  mio_node:insert_op(Node, Node),
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
    case gen_tcp:accept(Listen) of
        {ok, Sock} ->
            spawn(?MODULE, process_command, [Sock, WriteSerializer, StartNode, MaxLevel]),
            mio_accept(Listen, WriteSerializer, StartNode, MaxLevel);
        Other ->
            io:format("accept returned ~w - goodbye!~n",[Other]),
            ok
    end.

process_command(Sock, WriteSerializer, StartNode, MaxLevel) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Line} ->
            Token = string:tokens(binary_to_list(Line), " \r\n"),
            NewStartNode =
            case Token of
                ["get", Key] ->
                    process_get(Sock, WriteSerializer, StartNode, Key),
                    StartNode;
                ["get", "mio:range-search", Key1, Key2, Limit, "asc"] ->
                    process_range_search_asc(Sock, WriteSerializer, StartNode, Key1, Key2, list_to_integer(Limit)),
                    StartNode;
                ["get", "mio:range-search", Key1, Key2, Limit, "desc"] ->
                    process_range_search_desc(Sock, WriteSerializer, StartNode, Key1, Key2, list_to_integer(Limit)),
                    StartNode;
                ["set", Key, Flags, ExpireDate, Bytes] ->
                    inet:setopts(Sock,[{packet, raw}]),
                    InsertedNode = process_set(Sock, WriteSerializer, StartNode, Key, Flags, list_to_integer(ExpireDate), Bytes, MaxLevel),
                    inet:setopts(Sock,[{packet, line}]),
                    StartNode;
                ["delete", Key] ->
                    process_delete(Sock, WriteSerializer, StartNode, Key),
                    StartNode;
                ["delete", Key, _Time] ->
                    process_delete(Sock, WriteSerializer, StartNode, Key),
                    StartNode;
                ["delete", Key, _Time, _NoReply] ->
                    process_delete(Sock, WriteSerializer, StartNode, Key),
                    StartNode;
                ["quit"] ->
                    ok = gen_tcp:close(Sock);
                X ->
                    ?ERRORF("<~p error: ~p\n", [Sock, X]),
                    ok = gen_tcp:send(Sock, "ERROR\r\n"),
                    StartNode
            end,
            process_command(Sock, WriteSerializer, NewStartNode, MaxLevel);
        {error, closed} ->
            ok;
        Error ->
            ?ERRORF("<~p error: ~p\n", [Sock, Error])
    end.

process_delete(Sock, WriteSerializer, StartNode, Key) ->
    case mio_write_serializer:delete_op(WriteSerializer, StartNode, Key) of
        ng ->
            ok = gen_tcp:send(Sock, "NOT_FOUND\r\n");
        _ ->
            ok = gen_tcp:send(Sock, "DELETED\r\n")
    end.

%% Expiry format definition
%% Expire:
%%   0 -> never expire
%%   -1 -> expired and enqueued to delete queue
%%   greater than zero -> expiration date in Unix time format

%% Returns {Expired?, NeedEnqueue}
check_expired(0) ->
    {false, false};
check_expired(-1) ->
    {true, false};
check_expired(ExpireDate) ->
    Expired = ExpireDate =< unixtime(),
    {Expired, Expired}.

process_get(Sock, WriteSerializer, StartNode, Key) ->
    {Node, FoundKey, Value, ExpireDate} = mio_node:search_op(StartNode, Key),
    {Expired, NeedEnqueue} = check_expired(ExpireDate),
    if Key =:= FoundKey andalso not Expired ->
            ok = gen_tcp:send(Sock, io_lib:format(
                                      "VALUE ~s 0 ~w\r\n~s\r\nEND\r\n",
                                      [Key, size(Value), Value]));
       true ->
            ok = gen_tcp:send(Sock, "END\r\n")
    end,
    %% enqueue to the delete queue
    if NeedEnqueue ->
            enqueue_to_delete(WriteSerializer, Node);
       true -> []
    end.

process_values([{_, Key, Value, _} | More]) ->
    io_lib:format("VALUE ~s 0 ~w\r\n~s\r\n~s",
                  [Key, size(Value), Value, process_values(More)]);
process_values([]) ->
    "END\r\n".

enqueue_to_delete(WriteSerializer, Node) ->
    mio_node:set_expire_time_op(Node, -1),
    spawn(fun() -> mio_write_serializer:delete_op(WriteSerializer, Node) end).

filter_expired(WriteSerializer, Values) ->
    lists:filter(fun({Node, _, _, ExpireDate}) ->
                         {Expired, NeedEnqueue} = check_expired(ExpireDate),
                         if NeedEnqueue ->
                                 enqueue_to_delete(WriteSerializer, Node);
                            true -> []
                         end,
                         not Expired
                 end, Values).

process_range_search_asc(Sock, WriteSerializer, StartNode, Key1, Key2, Limit) ->
    Values = mio_node:range_search_asc_op(StartNode, Key1, Key2, Limit),
    ActiveValues = filter_expired(WriteSerializer, Values),
    P = process_values(ActiveValues),
    ok = gen_tcp:send(Sock, P).

process_range_search_desc(Sock, WriteSerializer, StartNode, Key1, Key2, Limit) ->
    Values = mio_node:range_search_desc_op(StartNode, Key1, Key2, Limit),
    ActiveValues = filter_expired(WriteSerializer, Values),
    P = process_values(ActiveValues),
    ok = gen_tcp:send(Sock, P).

%% See expiry format definition on process_get
process_set(Sock, WriteSerializer, Introducer, Key, _Flags, ExpireDate, Bytes, MaxLevel) ->
    case gen_tcp:recv(Sock, list_to_integer(Bytes)) of
        {ok, Value} ->
            MVector = mio_mvector:generate(MaxLevel),
            UnixTime = unixtime(),
            ExpireDateUnixTime = if  ExpireDate =:= 0 ->
                                     0;
                                 ExpireDate > UnixTime ->
                                     ExpireDate;
                                 true ->
                                     ExpireDate + UnixTime
                             end,
            {ok, NodeToInsert} = mio_sup:start_node(Key, Value, MVector, ExpireDateUnixTime),
%% serialize or concurrent
%            io:format("MEM_INSERT ~p ~p~n", [Key, self()]),
            mio_node:insert_op(Introducer, NodeToInsert),
%            io:format("MEM_INSERT_DONE ~p ~p~n", [Key, self()]),
%%            mio_write_serializer:insert_op(WriteSerializer, Introducer, NodeToInsert),
            ok = gen_tcp:send(Sock, "STORED\r\n"),
            {ok, _Data} = gen_tcp:recv(Sock, 2),
            NodeToInsert;
        {error, closed} ->
            ok;
        Error ->
           ?ERRORF("Error: ~p\n", [Error]),
            {ok, _Data} = gen_tcp:recv(Sock, 2),
            Introducer
    end.

unixtime() ->
    {Msec, Sec, _} = now(),
    Msec * 1000 + Sec.
