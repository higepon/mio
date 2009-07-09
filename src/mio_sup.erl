-module(mio_sup).
-behaviour(supervisor).

-export([start_link/0, start_node/3]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    error_logger:info_msg("~p start_link\n", [?MODULE]),
    crypto:start(), % getRandomId uses crypto server
    %% register local as ?SERVER.
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% start normal mio_node
start_node(Key, Value, MembershipVector) ->
    {ok, Pid} = supervisor:start_child(mio_sup, {getRandomId(),
                                                 {mio_node, start_link, [[Key, Value, MembershipVector]]},
                                                 permanent, brutal_kill, worker, [mio_node]}).

init(_Args) ->
    error_logger:info_msg("~p init\n", [?MODULE]),
    %% todo
    %% Make this simple_one_for_one
    {ok, {{one_for_one, 10, 20},
          [{mio_node, %% this is just id of specification, will not be registered by register/2.
            {mio_node, start_link, [[myKey, myValue, [1, 0]]]},
            permanent, brutal_kill, worker, [mio_node]}]}}.

getRandomId() ->
    integer_to_list(crypto:rand_uniform(1, 65536 * 65536)).
