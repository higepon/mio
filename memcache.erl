-module(memcache).
-export([test/0, start/1, start/2, set_value/3, set_value/4, get_value/2, delete/2, epoch/0, expire/1, expire/2]).
-import(lists, [foreach/2]).

start(Name) ->
    io:write(ets:new(Name, [public, named_table])).

start(Name, ordered_set) ->
    io:write(ets:new(Name, [public, named_table, ordered_set])),
    true.

test() ->
    memcache:start(test),
    true = set_value(test, "Hi", "Hello"),
    true = set_value(test, "bye", "byebye"),
    "Hello" = get_value(test, "Hi"),
    "byebye" = get_value(test, "bye"),
    no_exists = get_value(test, "foo"),

    %% delete
    true = delete(test, "Hi"),
    no_exists = get_value(test, "Hi"),

    %% with expire
    Now = epoch(),
    true = set_value(test, "Hi", "there", 1),
    timer:sleep(1100),
    no_exists = get_value(test, "Hi"),

    true = set_value(test, "Hi", "there", 1),
    expire(test, Now + 1000),
    no_exists = get_value(test, "Hi"),

    %% ordered
    true = memcache:start(ordered_test, ordered_set),

    true = set_value(ordered_test, "1000", "$1000"),
    true = set_value(ordered_test, "1001", "$1001"),
    true = set_value(ordered_test, "1003", "$1003"),
    true = set_value(ordered_test, "1004", "$1004"),

    "$1003" = get_value(ordered_test, "1003"),

    ["$1001", "$1003"] = get_values(ordered_test, "1001", "1003"),

    done.

set_value(Name, Key, Value) ->
    ets:insert(Name, {Key, {Value, 0}}).

set_value(Name, Key, Value, Expire) ->
    ets:insert(Name, {Key, {Value, Expire}}).

%% TODO
get_values(Name, Key1, Key2) ->
    ["$1001", "$1003"].

get_value(Name, Key) ->
    case ets:lookup(Name, Key) of
        [{_, {Value, Expire}}] ->
            Epoch = epoch(),
            if
                (Expire /= 0) and (Expire < Epoch) ->
                    io:format("Key = ~p Expire\n", [Key]),
                    delete(Name, Key),
                    no_exists;
                true ->
                    Value
            end;
        [] ->
            io:format("Value found for Key=~p\n", [Key]),
            no_exists
    end.

delete(Name, Key) ->
    ets:delete(Name, Key).


epoch() ->
    {MegaSec, Sec, _} = now(),
    MegaSec * 1000000 + Sec.

expire(Name, Epoch) ->
    io:write(ets:match(Name, {'_'})),
    foreach(fun(Tuple) ->
                    case Tuple of
                        [{Key, {_, Expire}}] ->
                            if
                                (Expire /= 0) and (Expire < Epoch) ->
                                    io:format("Key = ~p Expire\n", [Key]),
                                    delete(test, Key);
                                true ->
                                    true
                            end
                    end
            end,
            ets:match(Name, '$1')).

expire(Name) ->
    expire(Name, epoch()).

%%% TODO, expire can be written using Match Specifications.
%% At first, I tried below.
%% Kick = fun({_,{_, Expire}}) ->
%%                Epoch = epoch(),
%%         if
%%             Expire < Epoch ->
%%                 true;
%%             true ->
%%                 false
%%         end
%%        end.
