-module(mio_sup).
-behaviour(supervisor).
-export([init/1, start_node/3, start_node/4, start_write_serializer/0, terminate_node/1]).
-include("mio.hrl").

%% supervisor:
%%   On start up, supervisor starts mio_memcached.
%%   mio_memcached starts a dummy node using mio_sup:start_node.
%%   Whenever new node is to create, mio_sup:start_nodes is used.

%% start normal mio_node
start_node(Key, Value, MembershipVector) ->
    start_node(Key, Value, MembershipVector, 0).
start_node(Key, Value, MembershipVector, Expire) ->
    {ok, _} = supervisor:start_child(mio_sup, {getRandomId(),
                                               {mio_node, start_link, [[Key, Value, MembershipVector, Expire]]},
                                               temporary, brutal_kill, worker, [mio_node]}).

start_write_serializer() ->
    {ok, _} = supervisor:start_child(mio_sup,
                                     {mio_write_serializer,
                                      {mio_write_serializer, start_link, []},
                                      temporary, brutal_kill, worker, [mio_write_serializer]}).

terminate_node(TargetPid) ->
    lists:any(fun({Id, Pid, _, _}) ->
                      if Pid =:= TargetPid ->
                              supervisor:terminate_child(mio_sup, Id),
                              true;
                         true -> false
                      end end, supervisor:which_children(mio_sup)).


init(_Args) ->
    crypto:start(), % getRandomId uses crypto server

    {ok, Port} = mio_app:get_env(port, 11211),
    {ok, MaxLevel} = mio_app:get_env(maxlevel, 3),
    {ok, BootNode} = mio_app:get_env(boot_node, false),


       Opts = [{name, logger},
               {file, "./elog"},
               {type, wrap},
               {format, external},
               {force_size, true},
               {size, {1024*1024, 5}}], % 5 files
       gen_event:add_sup_handler(
         error_logger,
         {disk_log_h, logger},
         disk_log_h:init(fun logger:form_no_progress/1, Opts)),



    %% todo
    %% Make this simple_one_for_one
    ?PROFILER_START(self()),
    {ok, {{one_for_one, 10, 20},
          %% logger should be the first.
          [{logger, {logger, start_link, []},
            permanent, 2000, worker, [logger]},
           {mio_memcached, %% this is just id of specification, will not be registered by register/2.
            {mio_memcached, start_link, [Port, MaxLevel, BootNode]},
            permanent, brutal_kill, worker, [mio_memcached]}]}}.

getRandomId() ->
    integer_to_list(crypto:rand_uniform(1, 65536 * 65536)).
