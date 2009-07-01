-module(mio_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    error_logger:info_msg("~p start_link\n", [?MODULE]),
    %% register local as ?SERVER.
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init(_Args) ->
    error_logger:info_msg("~p init\n", [?MODULE]),
    {ok, {{one_for_one, 10, 20},
          [{mio_node,
            {mio_node, start_link, [[myKey, myValue]]},
            permanent, brutal_kill, worker, [mio_node]}]}}.

%%           [{mio_node, {mio_node, start_link, []},
%%             permanent, brutal_kill, worker, [mio_node]}]}}.
