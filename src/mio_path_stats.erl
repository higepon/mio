%%    Copyright (C) 2010 Cybozu Labs, Inc., written by Taro Minowa(Higepon) <higepon@labs.cybozu.co.jp>
%%
%%    Redistribution and use in source and binary forms, with or without
%%    modification, are permitted provided that the following conditions
%%    are met:
%%
%%    1. Redistributions of source code must retain the above copyright
%%       notice, this list of conditions and the following disclaimer.
%%
%%    2. Redistributions in binary form must reproduce the above copyright
%%       notice, this list of conditions and the following disclaimer in the
%%       documentation and/or other materials provided with the distribution.
%%
%%    3. Neither the name of the authors nor the names of its contributors
%%       may be used to endorse or promote products derived from this
%%       software without specific prior written permission.
%%
%%    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
%%    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
%%    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
%%    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
%%    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
%%    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
%%    TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
%%    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
%%    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
%%    NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
%%    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-module(mio_path_stats).
-export([init/0, init_attach/1, show/1, push/2, push/3]).
-include_lib("eunit/include/eunit.hrl").
-include("mio.hrl").

-record(path_stat, {key, stat}).

init() ->
    case mnesia:create_schema([node()]) of
        ok -> ok;
        {error, {_, {already_exists, _}}} ->
            ok;
        {error, Reason} ->
            ?FATALF("Error ~p", [Reason])
    end,
    case mnesia:start() of
        ok -> ok;
        {error, Reason2} ->
            ?FATALF("Error ~p", [Reason2])
    end,
    mnesia:delete_table(path_stat),
    case mnesia:create_table(path_stat, [{attributes, record_info(fields, path_stat)}, {ram_copies, [node()]}]) of
        {atomic, ok} -> ok;
        {aborted, {already_exists,path_stat}} ->
            ok;
        {aborted, Reason3} ->
            ?FATALF("Error ~p", [Reason3])
    end,
    case mnesia:wait_for_tables([path_stat], 5000) of
        ok -> ok;
        {timeout, BadTabList} ->
            ?FATALF("wait_for_tables error ~p", [BadTabList]);
        {error, Reason4}  ->
            ?FATALF("Error ~p", [Reason4])
    end,
    mnesia:clear_table(path_stat).

init_attach(PathStatNode) ->
    case mnesia:start() of
        ok -> ok;
        {error, Reason2} ->
            ?FATALF("Error ~p", [Reason2])
    end,
    case mnesia:change_config(extra_db_nodes, [PathStatNode]) of
        {ok, _} -> ok;
        Other ->
            ?FATALF("Error ~p", [Other])
    end,
    case mnesia:wait_for_tables([path_stat], 5000) of
        ok -> ok;
        {timeout, BadTabList} ->
            ?FATALF("wait_for_tables error ~p", [BadTabList]);
        {error, Reason4}  ->
            ?FATALF("Error ~p", [Reason4])
    end.

push(SearchKey, Datum) ->
    case mnesia:dirty_read({path_stat, SearchKey}) of
        [] ->
            mnesia:dirty_write(path_stat, #path_stat{key=SearchKey, stat=[Datum]});
        [{path_stat, SearchKey, Stats}] ->
            mnesia:dirty_write(path_stat, #path_stat{key=SearchKey, stat=[Datum | Stats]});
        Any ->
            ?FATALF("Any=~p", [Any])
    end.

push(Self, SearchKey, Level) ->
    push(SearchKey, {node(), Self, Level}).

show(SearchKey) ->
    case mnesia:dirty_read({path_stat, SearchKey}) of
        [] ->
            ?INFO("no search path stat");
        [{path_stat, SearchKey, Stats}] ->
            ?INFOF("search ~p path stat ~p", [SearchKey, lists:reverse(Stats)]),
            mnesia:dirty_write(path_stat, #path_stat{key=SearchKey, stat=[]});
        Any2 ->
            ?FATALF("Any2=~p", [Any2])
    end.
