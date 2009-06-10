-module(miodb).
-export([test/0, new/1, set_value/3, set_value/4, get_value/2, set_value_with_index/4, get_values_with_index/5]).
-import(lists, [map/2, reverse/1]).
-include_lib("stdlib/include/qlc.hrl").
-record(store, {key, value}).

new(Name) ->
    memcache:start(Name),
    stopped = mnesia:stop(),
    ok = mnesia:delete_schema([node()]),
    ok = mnesia:create_schema([node()]),
    ok = mnesia:start(),
    done.

set_value(Name, Key, Value) ->
    memcache:set_value(Name, Key, Value).

set_value(Name, Key, Value , Expire) ->
    memcache:set_value(Name, Key, Value, Expire).

set_value_with_index(Name, Key, Value, Index) ->
    io:fwrite("[1]"),
    true = memcache:set_value(Name, Key, Value, Index),
    io:fwrite("[2]"),
    case table_exists(Index) of
        exists ->
    io:fwrite("[3.5]"),
            mnesia:transaction(fun() -> mnesia:write(Index, #store{key=Key, value=dummy}, write) end),
    io:fwrite("[4]"),
            {ok, index_exists};
        %% index not exists, create it!
        no_exists ->
    io:fwrite("[5]"),
            {atomic, ok} = mnesia:create_table(Index, [{type, ordered_set}, {record_name, store},
                                                       {attributes, record_info(fields, store)}]),
    io:fwrite("[6]"),
            mnesia:transaction(fun() -> mnesia:write(Index, #store{key=Key, value=dummy}, write) end),
    io:fwrite("[7]"),
            {ok, index_no_exists}
    end.

get_values_with_index(Name, Key1, Key2, Index, {limit, N}) ->
    map(fun(Key) -> [Key, memcache:get_value(Name, Key)] end, do(qlc:q([X#store.key || X <- mnesia:table(Index),
                                                                                X#store.key < Key2,
                                                                                X#store.key > Key1]), N)).
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

get_value(Name, Key) ->
    memcache:get_value(Name, Key).

delete(Name, Key) ->
    memcache:delete(Name, Key).

expire(Name) ->
    memcache:expire(Name).

expire(Name, Epoch) ->
    memcache:expire(Name, Epoch).

table_exists(Table) ->
    io:fwrite("[3.1]"),
    io:fwrite(Table),
%%    io:fwrite (mnesia:table_info(item, arity)),
    io:fwrite("[3.2]"),
    try mnesia:table_info(Table, type) of
        _ ->
    io:fwrite("[4]"),
            exists
    catch
        exit:{aborted, {no_exists, Table, _}} -> no_exists
    end.


test() ->
    DBName = mio_test,
    new(DBName),
    true = set_value(DBName, "Hi", "Hello"),
    true = set_value(DBName, "bye", "byebye"),
    "Hello" = get_value(DBName, "Hi"),
    "byebye" = get_value(DBName, "bye"),
    no_exists = get_value(DBName, "foo"),

    %% Delete
    true = delete(DBName, "Hi"),
    no_exists = get_value(DBName, "Hi"),

    %% With expire
    Now = memcache:epoch(),
    true = set_value(DBName, "Hi", "there", 1),
    timer:sleep(1100),
    no_exists = get_value(DBName, "Hi"),

    true = set_value(DBName, "Hi", "there", 1),
    expire(DBName, Now + 1000),
    no_exists = get_value(DBName, "Hi"),

    %% Range Search
    %% hello_index exists? if not create it.
    {ok, index_no_exists} = set_value_with_index(DBName, "hello", "world", mio_hello_index),
    {ok, index_exists} = set_value_with_index(DBName, "hello", "world2", mio_hello_index),

    true = set_value(DBName, "article1", "Hello 1"),
    "Hello 1" = get_value(DBName, "article1"),

    {ok, _} = set_value_with_index(DBName, "bbs2", "article2", bbs_index),
    {ok, _} = set_value_with_index(DBName, "bbs1", "article1", bbs_index),
    {ok, _} = set_value_with_index(DBName, "bbs3", "article3", bbs_index),
    {ok, _} = set_value_with_index(DBName, "bbs4", "article4", bbs_index),

    %% range
    ["article2"] = get_values_with_index(DBName, "bbs1", "bbs2.3", bbs_index, {limit, 2}),

    done.
