%%%-------------------------------------------------------------------
%%% File    : mio_SUITE.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description :
%%%
%%% Created : 2 Aug 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_SUITE).

-compile(export_all).
-include("../include/mio.hrl").

init_per_suite(Config) ->
    ok = application:start(mio),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(mio),
    ok.

set_and_get(_Config) ->
    {ok, _MerlePid} = merle2:connect("localhost", 11211),
    ok = merle2:set("hello", "0", "0", "world"),
    "world" = merle2:getkey("hello"),
    ok = merle2:set("hi", "0", "0", "japan"),
    "japan" = merle2:getkey("hi"),
    ok = merle2:set("ipod", "0", "0", "mp3"),
    "mp3" = merle2:getkey("ipod"),
    ok.

delete(_Config) ->
    {ok, _MerlePid} = merle2:connect("localhost", 11211),
    ok = merle2:set("hello", "0", "0", "world"),
    "world" = merle2:getkey("hello"),
    ok = merle2:delete("hello"),
    not_found = merle2:delete("hello2"),
    undefined = merle2:getkey("hello"),
    ok.

expiration(_Config) ->
    {ok, _MerlePid} = merle2:connect("localhost", 11211),
    ok = merle2:set("myname", "0", "1", "john"),
    "john" = merle2:getkey("myname"),
    timer:sleep(1000),
    undefined = merle2:getkey("myname"),
    undefined = merle2:getkey("myname").

all() ->
    [
     set_and_get,
     delete,
     expiration
    ].
