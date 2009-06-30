%%%-------------------------------------------------------------------
%%% File    : mio_node_SUITE.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : 
%%%
%%% Created : 30 Jun 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_node_SUITE).

-compile(export_all).

init_per_suite(Config) ->
    {ok, Pid} = mio_sup:start_link(),
    unlink(Pid),
    Config.

end_per_suite(Config) ->
    ok.

all() -> 
    [my_test_case].

my_test_case() ->
    [].

my_test_case(_Config) -> 
    ok = gen_server:call(mio_node, hige),
    ok.
