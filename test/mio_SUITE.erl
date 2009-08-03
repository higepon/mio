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
    ok = mio:start(),
    IsVerbose = ct:get_config(isVerbose),
    ?LOG(IsVerbose),
    if IsVerbose ->
            error_logger:tty(true);
       true ->
            error_logger:tty(false)
    end,

    Config.

end_per_suite(Config) ->
    ok.

set_and_get(_Config) ->
    {ok, MerlePid} = merle:connect("localhost", 11211),
    ok = merle:set("hello", "0", "0", "world"),
    "world" = merle:getkey("hello"),
    ok = merle:set("hi", "0", "0", "japan"),
    "japan" = merle:getkey("hi"),
    ok = merle:set("ipod", "0", "0", "mp3"),
    "mp3" = merle:getkey("ipod"),
    ok.

range_search(_Config) ->
%%     {ok, MerlePid} = merle:connect("localhost", 11211),
%%     ok = merle:set("hello", "0", "0", "world"),
%%     ok = merle:set("hi", "0", "0", "japan"),
%%     ok = merle:set("ipod", "0", "0", "mp3"),
%%     merle:getkey("ipod hi"),
%    merle:getkey("mio:range-search he ipod 10 desc"),
%%   (test* 'get '((hi . "japan") (hello . "world")) (get m "mio:range-search" "he" "ipod" "10" "desc"))
%%   (test* 'get '((hi . "japan")) (get m "mio:range-search" "he" "ipod" "1" "desc"))
%%   (test* 'get '((hello . "world")) (get m "mio:range-search" "he" "hi" "1"))

    ok.

all() ->
    [set_and_get, range_search].
