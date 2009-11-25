%%-define(PROFILER_ON, true).
-define(DEBUG_ON, true).

  -define(INFOF(Msg, Args), error_logger:info_msg(Msg ++ " ~p:~p~n", Args ++ [?MODULE, ?LINE])).
  -define(INFO(Msg), ?INFOF(Msg, [])).
  -define(ERRORF(Msg, Args), io:format(Msg ++ " ~p:~p~n", Args ++ [?MODULE, ?LINE])).
  -define(ERROR(Msg), ?ERRORF(Msg, [])).


-ifdef (DEBUG_ON).
  -define(CHECK_SANITY(Node, Level), check_sanity(Node, Level, ?MODULE, ?LINE)).
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
                  io:format("** Assertion failed~p: not [] expected, but got [] at ~p:~p~n", [self(), ?MODULE, ?LINE]),
                  exit(assertion_failed);
              _ ->
                  true
          end.
-else.
  -define(CHECK_SANITY(Node, Level), []).
  -define(L(), []).
  -define(LOG(X), []).
  -define(LOGF(X, Data), []).
  -define(SERVER, ?MODULE).
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
