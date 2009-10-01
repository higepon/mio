
%%-define(L(), error_logger:info_msg("{~p ~p,~p}:~n", [self(), ?MODULE,?LINE])).
%% -define(L(), io:format("{~p ~p,~p}:~n", [self(), ?MODULE,?LINE])).
%% -define(LOG(X), error_logger:info_msg("{~p ~p,~p}: ~s = ~p~n", [self(), ?MODULE,?LINE,??X,X])).
%% -define(LOGF(X, Data), error_logger:info_msg("{~p ~p,~p}: "++X++"~n" , [self(), ?MODULE,?LINE] ++ Data)).

-define(L(), io:format("~p{~p ~p,~p}:~n", [erlang:now(), self(), ?MODULE,?LINE])).
%% -define(LOG(X), io:format("{~p ~p,~p}: ~s = ~p~n", [self(), ?MODULE,?LINE,??X,X])).
%% -define(LOGF(X, Data), io:format("{~p ~p,~p}: "++X++"~n" , [self(), ?MODULE,?LINE] ++ Data)).
%% -define(SERVER, ?MODULE).
%%-define(TRACE(X), io:format(" *~p{~p ~p,~p}[~p]~n", [erlang:now(), self(), ?MODULE, ?LINE, X])).
-define(TRACE(X), []).


%% -define(L(), []).
-define(LOG(X), []).
-define(LOGF(X, Data), []).

-define(SERVER, ?MODULE).
-define(ERRORF(X, Data), io:format("{~p ~p,~p}: "++X++"~n", [self(), ?MODULE,?LINE] ++ Data)).


-define(ASSERT_MATCH(EXPECTED, X),
        case X of
            EXPECTED -> true;
            Value ->
                io:format("** Assertion failed: ~p expected, but got ~p at ~p:~p~n", [EXPECTED, Value, ?MODULE, ?LINE]),
                exit(assertion_failed)
        end.

-define(ASSERT_NOT_NIL(X),
        case X of
            [] ->
                io:format("** Assertion failed: not [] expected, but got [] at ~p:~p~n", [?MODULE, ?LINE]),
                exit(assertion_failed);
            _ ->
                true
        end.



%%-define(PROFILER_ON, true).

-ifdef (PROFILER_ON).
%% -define(PROFILER_STOP(), eprof:stop_profiling(), eprof:total_analyse()).
%% -define(PROFILER_START(X), eprof:start(), profiling = eprof:start_profiling([X])).
-define(PROFILER_STOP(), fprof:trace([stop]), fprof:profile(), fprof:analyse([totals, {details, true}]), fprof:stop()).
-define(PROFILER_START(X), fprof:trace([start, {procs, [X]}])).

-else.
-define(PROFILER_STOP(), []).
-define(PROFILER_START(X), []).
-endif.
