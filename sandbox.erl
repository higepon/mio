-module(sandbox).
-export([start/0]).

-include_lib("stdlib/include/qlc.hrl").
-record(store, {key, value}).

start() ->
    create_table().

do(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Results} = mnesia:transaction(F),
    Results.


create_table() ->
    %% we need to delete schema
    mnesia:delete_schema([node()]),

    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(store, [{type, ordered_set}, {attributes, record_info(fields, store)}]),
    mnesia:transaction(fun() -> mnesia:write(#store{key=def, value=30}) end),
    mnesia:transaction(fun() -> mnesia:write(#store{key=abc, value=30}) end),
    mnesia:transaction(fun() -> mnesia:write(#store{key=axy, value=40}) end),

    %% show information
    mnesia:schema(),

    %% fetch all
    io:write(do(qlc:q([X || X <- mnesia:table(store)]))),

    %% fetch =:=
    io:write(do(qlc:q([X#store.key || X <- mnesia:table(store), X#store.key =:= abc]))),

    %% fetch range
    io:write(do(qlc:q([X#store.key || X <- mnesia:table(store), X#store.key < bcc,
                                                                X#store.key > abc
                                         ]))),

    mnesia:stop().
