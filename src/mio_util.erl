%%%-------------------------------------------------------------------
%%% File    : mio_util.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : Some utilities for mio.
%%%
%%% Created : 17 Nov 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_util).

%% API
-export([random_sleep/1, lists_set_nth/3, cover_start/1, report_cover/1]).

-include("mio.hrl").

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function:
%% Description:
%%--------------------------------------------------------------------
random_sleep(Times) ->
    case (Times rem 10) of
        0 -> erase(random_seed);
        _ -> ok
    end,
    case get(random_seed) of
        undefined ->
            {A1, A2, A3} = now(),
            random:seed(A1, A2, A3 + erlang:phash(node(), 100000));
        _ -> ok
    end,
    T = random:uniform(1000) rem 20 + 1,
    ?INFOF("HERE sleep ~p msec ~n", [T]),
    receive after T -> ok end.

lists_set_nth(Index, Value, List) ->
    lists:append([lists:sublist(List, 1, Index - 1),
                  [Value],
                  lists:sublist(List, Index + 1, length(List))]).

cover_start(EbinDir) ->
    {ok, _Pid} = cover:start(),
    case cover:compile_beam_directory(EbinDir) of
        {error, Reason} ->
            io:format("cover compile failed ~p~n", [Reason]),
            halt(1);
        _ -> ok
    end.

report_cover(TargeDir) ->
    lists:foreach(
      fun(Module) ->
              {ok, _} = cover:analyse_to_file(Module,
                                              filename:join(TargeDir, atom_to_list(Module) ++ ".html"),
                                              [html]),
              {ok, {Module, {Cov, NotCov}}} = cover:analyze(Module, module),
              io:format("~p: ~p%~n",[Module, erlang:trunc(100.0*Cov/(Cov+NotCov))])
      end,
      %%cover:modules()
      [mio_node, mio_memcached]),
    cover:stop().

%%====================================================================
%% Internal functions
%%====================================================================
