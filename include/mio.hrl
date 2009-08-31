
-define(L(), error_logger:info_msg("{~p ~p,~p}:~n", [self(), ?MODULE,?LINE])).
-define(LOG(X), error_logger:info_msg("{~p ~p,~p}: ~s = ~p~n", [self(), ?MODULE,?LINE,??X,X])).
-define(LOGF(X, Data), error_logger:info_msg("{~p ~p,~p}: "++X++"~n" , [self(), ?MODULE,?LINE] ++ Data)).
-define(SERVER, ?MODULE).

%%-define(PROFILER_ON, true).

-ifdef (PROFILER_ON).
-define(PROFILER_STOP(), fprof:trace([stop]), fprof:profile(), fprof:analyse(), fprof:stop()).
-define(PROFILER_START(X), fprof:trace([start, {procs, [X]}])).
-else.
-define(PROFILER_STOP(), []).
-define(PROFILER_START(X), []).
-endif.



