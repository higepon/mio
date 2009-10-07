%%-define(PROFILER_ON, true).
%%-define(DEBUG_ON, true).

-ifdef (DEBUG_ON).
  -define(L(), io:format("~p{~p ~p,~p}:~n", [erlang:now(), self(), ?MODULE, ?LINE])).
  -define(LOG(X), io:format("{~p ~p,~p}: ~s = ~p~n", [self(), ?MODULE, ?LINE, X])).
  -define(LOGF(X, Data), io:format("{~p ~p,~p}: "++X++"~n" , [self(), ?MODULE,?LINE] ++ Data)).
  -define(SERVER, ?MODULE).
  -define(TRACE(X), io:format(" *~p{~p ~p,~p}[~p]~n", [erlang:now(), self(), ?MODULE, ?LINE, X])).
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
-else.
  -define(L(), []).
  -define(LOG(X), []).
  -define(LOGF(X, Data), []).
  -define(SERVER, ?MODULE).
  -define(ERRORF(X, Data), io:format("{~p ~p,~p}: "++X++"~n", [self(), ?MODULE,?LINE] ++ Data)).
  -define(ASSERT_MATCH(EXPECTED, X), []).
  -define(ASSERT_NOT_NIL(X), []).
-endif.

-ifdef (PROFILER_ON).
  -define(PROFILER_STOP(), fprof:trace([stop]), fprof:profile(), fprof:analyse([totals, {details, true}]), fprof:stop()).
  -define(PROFILER_START(X), fprof:trace([start, {procs, [X]}])).
-else.
  -define(PROFILER_STOP(), []).
  -define(PROFILER_START(X), []).
-endif.
