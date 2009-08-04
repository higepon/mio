%%%-------------------------------------------------------------------
%%% File    : mio_SUITE.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description :
%%%
%%% Created : 2 Aug 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_SUITE).

-compile(export_all).

-define(SERVER, ?MODULE).
-define(L(), error_logger:info_msg("{~p ~p,~p}:~n", [self(), ?MODULE,?LINE])).
-define(LOG(X), error_logger:info_msg("{~p ~p,~p}: ~s = ~p~n", [self(), ?MODULE,?LINE,??X,X])).
-define(LOGF(X, Data), error_logger:info_msg("{~p ~p,~p}: "++X++"~n" , [self(), ?MODULE,?LINE] ++ Data)).

init_per_suite(Config) ->
    %% config file is specified on runtest's command line option
    ok = application:start(mio),
    IsVerbose = ct:get_config(isVerbose),
    ?LOG(IsVerbose),
    if IsVerbose ->
            error_logger:tty(true);
       true ->
            error_logger:tty(false)
    end,

    Config.

end_per_suite(Config) ->
    application:stop(mio),
    ok.

set_and_get(_Config) ->
    {ok, MerlePid} = merle2:connect("localhost", 11211),
    ok = merle2:set("hello", "0", "0", "world"),
    "world" = merle2:getkey("hello"),
    ok = merle2:set("hi", "0", "0", "japan"),
    "japan" = merle2:getkey("hi"),
    ok = merle2:set("ipod", "0", "0", "mp3"),
    "mp3" = merle2:getkey("ipod"),
    ok.

range_search(_Config) ->
    {ok, MerlePid} = merle2:connect("localhost", 11211),
    ok = merle2:set("hello", "0", "0", "world"),
    ok = merle2:set("hi", "0", "0", "japan"),
    ok = merle2:set("ipod", "0", "0", "mp3"),
    merle2:getkey("mio:range-search he ipod 10 desc"),
%    merle:getkey("mio:range-search he ipod 10 desc"),
%%   (test* 'get '((hi . "japan") (hello . "world")) (get m "mio:range-search" "he" "ipod" "10" "desc"))
%%   (test* 'get '((hi . "japan")) (get m "mio:range-search" "he" "ipod" "1" "desc"))
%%   (test* 'get '((hello . "world")) (get m "mio:range-search" "he" "hi" "1"))

    ok.

all() ->
    [set_and_get, range_search].
