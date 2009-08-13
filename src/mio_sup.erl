-module(mio_sup).
-behaviour(supervisor).
-export([init/1, start_node/3, terminate_node/1]).
-include("mio.hrl").

%% supervisor:
%%   On start up, supervisor starts mio_memcached.
%%   mio_memcached starts a dummy node using mio_sup:start_node.
%%   Whenever new node is to create, mio_sup:start_nodes is used.

%% start normal mio_node
start_node(Key, Value, MembershipVector) ->
    {ok, _} = supervisor:start_child(mio_sup, {getRandomId(),
                                               {mio_node, start_link, [[Key, Value, MembershipVector]]},
                                               temporary, brutal_kill, worker, [mio_node]}).

terminate_node(TargetPid) ->
    lists:any(fun({Id, Pid, _, _}) ->
                      if Pid =:= TargetPid ->
                              supervisor:terminate_child(mio_sup, Id),
                              true;
                         true -> false
                      end end, supervisor:which_children(mio_sup)).


init(_Args) ->
    error_logger:info_msg("~p init\n", [?MODULE]),
    crypto:start(), % getRandomId uses crypto server

    {ok, Port} = mio_app:get_env(port, 11211),
    {ok, MaxLevel} = mio_app:get_env(maxlevel, 3),
    {ok, BootNode} = mio_app:get_env(boot_node, []),
    %% todo
    %% Make this simple_one_for_one
    {ok, {{one_for_one, 10, 20},
          [{mio_memcached, %% this is just id of specification, will not be registered by register/2.
            {mio_memcached, start_link, [Port, MaxLevel, BootNode]},
            permanent, brutal_kill, worker, [mio_memcached]}]}}.

getRandomId() ->
    integer_to_list(crypto:rand_uniform(1, 65536 * 65536)).
