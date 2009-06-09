-module(sandbox).
-export([start/0]).
-import(lists, [map/2, reverse/1]).
-import(memcache).
-import(miodb).

-include_lib("stdlib/include/qlc.hrl").
-record(store, {key, value}).

start() ->
    create_table().

do(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Results} = mnesia:transaction(F),
    Results.

do(Q, Limit) ->
  {atomic, Res} = mnesia:transaction(fun() ->
    QC = qlc:cursor(Q),
    Res = qlc:next_answers(QC, Limit),
    qlc:delete_cursor(QC),
    Res
  end),
  Res.

table_exists(Table) ->
    try mnesia:table_info(Table, all) of
        _ -> exists
    catch
        exit:{aborted, {no_exists, Table, _}} -> no_exists
    end.

mio_set(Key, Value) ->
    memcache_set(Key, Value).

mio_set(Key, Value, Index) ->
    ok = memcache_set(Key, Value),
    case table_exists(Index) of
        exists ->
            mnesia:transaction(fun() -> mnesia:write(Index, #store{key=Key, value=dummy}, write) end),
            {ok, index_exists};
        %% index not exists, create it!
        no_exists ->
            {atomic, ok} = mnesia:create_table(Index, [{type, ordered_set}, {record_name, store},
                                                       {attributes, record_info(fields, store)}]),
            mnesia:transaction(fun() -> mnesia:write(Index, #store{key=Key, value=dummy}, write) end),
            {ok, index_no_exists}
    end.

mio_get(Key) ->
    memcache_get(Key).

mio_get_sorted(Index) ->
    map(fun(Key) -> memcache_get(Key) end, do(qlc:q([X#store.key || X <- mnesia:table(Index)]))).

mio_get_sorted(Index, {limit, N}) ->
    map(fun(Key) -> memcache_get(Key) end, do(qlc:q([X#store.key || X <- mnesia:table(Index)]), N)).

mio_get_sorted(Index, {limit, N}, reverse) ->
    reverse(map(fun(Key) -> memcache_get(Key) end, do(qlc:q([X#store.key || X <- mnesia:table(Index)]), N))).

mio_get_sorted(Key1, Key2, Index, {limit, N}) ->
    map(fun(Key) -> memcache_get(Key) end, do(qlc:q([X#store.key || X <- mnesia:table(Index),
                                                                    X#store.key < Key2,
                                                                    X#store.key > Key1]), N)).



memcache_set(Key, Value) ->
    ets:insert(memcache,
               {Key, {Value, expire}}),
    ok.

memcache_get(Key) ->
    case ets:lookup(memcache, Key) of
        [{_, {Value, Expire}}] ->
            Value;
        [] -> no_exists
    end.

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
    %%mnesia:schema(),

    %% If you share the record type between tables.
    %% Adde record_name option to create_table.
    {atomic, ok} = mnesia:create_table(higepon, [{type, ordered_set}, {record_name, store}, {attributes, record_info(fields, store)}]),
    {atomic, ok} = mnesia:transaction(fun() -> mnesia:write(higepon, #store{key=hage, value=dummy}, write) end),


    %% fetch all
    [{store,abc,30},{store,axy,40},{store,def,30}] = do(qlc:q([X || X <- mnesia:table(store)])),

    %% fetch =:=
    [abc] = do(qlc:q([X#store.key || X <- mnesia:table(store), X#store.key =:= abc])),

    %% fetch range
    [axy] = do(qlc:q([X#store.key || X <- mnesia:table(store), X#store.key < bcc,
                                                                X#store.key > abc
                                         ])),
    %% table exists?
    exists = table_exists(store),
    no_exists = table_exists(hoge),

    %% Okay, we start memcache
    ets:new(memcache, [public, named_table]),

    %% insert!
    ok = memcache_set("test-key1", "test-value1"),
    "test-value1" = memcache_get("test-key1"),
    "test-value1" = memcache_get("test-key1"),

    no_exists = memcache_get("test-key-not-exists"),

    %% hello_index exists? if not create it.
    {ok, index_no_exists} = mio_set("hello", "world", hello_index),
    {ok, index_exists} = mio_set("hello", "world", hello_index),
    {ok, index_exists} = mio_set("hello", "world", store),

    %% mio_set
    ok = mio_set("article1", "Hello 1"),
    "Hello 1" = mio_get("article1"),

    {ok, _} = mio_set("bbs2", "article2", bbs_index),
    {ok, _} = mio_set("bbs1", "article1", bbs_index),
    {ok, _} = mio_set("bbs3", "article3", bbs_index),
    {ok, _} = mio_set("bbs4", "article4", bbs_index),


    %% Try mio_get
    ["article1", "article2", "article3", "article4"] = mio_get_sorted(bbs_index),

    %% Try mio_get_sorted with limit
    ["article1", "article2"] = mio_get_sorted(bbs_index, {limit, 2}),

    %% Try mio_get_sorted with limit and reverse order
    ["article2", "article1"] = mio_get_sorted(bbs_index, {limit, 2}, reverse),

    %% range
    ["article2"] = mio_get_sorted("bbs1", "bbs2.3", bbs_index, {limit, 2}),

    %% memcache interface
    memcache:test(),
    miodb:test(),

    mnesia:stop().

