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
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(store, [{type, ordered_set}, {attributes, record_info(fields, store)}]),
    mnesia:transaction(fun() -> mnesia:write(#store{key=def, value=30}) end),
    mnesia:transaction(fun() -> mnesia:write(#store{key=abc, value=30}) end),

    %% fetch all
    io:write(do(qlc:q([X || X <- mnesia:table(store)]))),

    %% fetch ==
    io:write(do(qlc:q([X#store.key || X <- mnesia:table(store), X#store.key =:= abc]))),
%    io:format("val=~p\n", Val),
%%     mnesia:transaction(fun() ->
%%                                Result = mnesia:read({my_keys, abc}),
%%                                io:write(Result) end),
    mnesia:stop().
