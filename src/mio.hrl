
-record(node, {store, type, min_key, encompass_min, max_key, encompass_max, left, right, membership_vector, gen_mvector,
                expire_time, inserted, deleted, allocator, search_stat
               }).


-define(SERVER, ?MODULE).
-define(INFOF(Msg, Args), error_logger:info_msg(Msg ++ " ~p:~p~p~n", Args ++ [?MODULE, ?LINE, self()])).
-define(INFO(Msg), ?INFOF(Msg, [])).

%%-define(MIN_KEY, <<16#20>>).
-define(MIN_KEY, "").
-define(MAX_KEY, <<16#7F>>).

-define(LOG(Msg), io:format(Msg ++ " ~p:~p~p~n", [?MODULE, ?LINE, self()])).
-define(LOGF(Msg, Args), io:format(Msg ++ " ~p:~p~p~n", Args ++ [?MODULE, ?LINE, self()])).


%% ERROR should be always written to stderr.
-define(ERRORF(Msg, Args), error_logger:error_msg(Msg ++ " ~p:~p~n", Args ++ [?MODULE, ?LINE]), io:format(Msg ++ "~n", Args)).
-define(ERROR(Msg), ?ERRORF(Msg, [])).
-define(FATALF(Msg, Arg), mio_app:fatal(Msg, Arg, ?MODULE, ?LINE)).
-define(WARNF(Msg, Args), error_logger:warn_msg(Msg ++ " ~p:~p~n", Args ++ [?MODULE, ?LINE])).
-define(WARN(Msg), ?WARNF(Msg, [])).

-define(NEVER_EXPIRE, 0).
-define(MARKED_EXPIRED, -1).
