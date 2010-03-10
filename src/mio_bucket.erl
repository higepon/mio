%%    Copyright (C) 2010 Cybozu Labs, Inc., written by Taro Minowa(Higepon) <higepon@labs.cybozu.co.jp>
%%
%%    Redistribution and use in source and binary forms, with or without
%%    modification, are permitted provided that the following conditions
%%    are met:
%%
%%    1. Redistributions of source code must retain the above copyright
%%       notice, this list of conditions and the following disclaimer.
%%
%%    2. Redistributions in binary form must reproduce the above copyright
%%       notice, this list of conditions and the following disclaimer in the
%%       documentation and/or other materials provided with the distribution.
%%
%%    3. Neither the name of the authors nor the names of its contributors
%%       may be used to endorse or promote products derived from this
%%       software without specific prior written permission.
%%
%%    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
%%    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
%%    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
%%    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
%%    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
%%    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
%%    TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
%%    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
%%    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
%%    NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
%%    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

%%%-------------------------------------------------------------------
%%% File    : mio_bucket.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : bucket
%%%
%%% Created : 9 Mar 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_bucket).
-include("mio.hrl").
-behaviour(gen_server).

%% API
-export([start_link/1,
         get_left_op/1, get_right_op/1,
         insert_op/3
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {left, right}).

%%====================================================================
%%  Bucket layer
%%====================================================================
%%
%%  C: Closed bucket
%%  O: Open bucket
%%  O*: Open and empty bucket
%%  O$: Nearly closed bucket
%%
%%  Three invariants on bucket group.
%%
%%    (1) O
%%      The system has one open bucket.
%%
%%    (2) C-O
%%      Every closed buket is adjacent to an open bucket.
%%
%%    (3) C-O-C
%%      Every open bucket has aclosed bucket to its left.
%%
%%
%%  Insertion patterns.
%%
%%    (a) O* -> O'
%%
%%    (c) O$ -> [C] -> C-O*
%%
%%    (c) C1-O2
%%      Insertion to C1 : C1'-O2'.
%%      Insertion to O2 : C1-O2'.
%%
%%    (d) C1-O2$
%%      Insertion to C1 : C1'-C2 -> C1'-O*-C2.
%%      Insertion to O2$ : C1-C2 -> C1-O*-C2
%%
%%    (e) C1-O2-C3
%%      Insertion to C1 : C1'-O2'-C3
%%      Insertion to C3 : C1-O2'-C3'
%%
%%    (f) C1-O2$-C3
%%      Insertion to C1 : C1'-C2-C3 ->C1'-O2 | C3'-O4
%%
%%  Deletion patterns
%%
%%    (a) C1-O2 | C3-O4
%%      Deletion from C1 causes O2 -> C1.
%%        C1'-O2' | C3-O4.
%%
%%      Deletion from C3. Same as above.
%%
%%    (b) C1-O2 | C3-O4*
%%      Deletion from C1. Same as (a).
%%
%%      Deletion from C3 causes O2 -> C3.
%%        C1-O2'-C3'
%%
%%    (c) C1-O2 | C3-O4-C5
%%      Deletion from C1. Same as (a).
%%
%%      Deletion from C3 causes O4-> C3
%%        C1-O2 | C3'-O4'-C5
%%
%%      Deletion from C5 causes O4-> C5
%%        C1-O2 | C3-O4'-C5'
%%
%%    (d) C1-O2 | C3-O4*-C5
%%      Deletion from C1. Same as (a).
%%
%%      Deletion from C3 causes C5 to C3.
%%        C1-O2 | C3'-O5
%%
%%      Deletion from C5.
%%        C1-O2 | C3-O5
%%
%%    (e) C1-02* | C3-O4
%%      Deletion from C1.
%%        -> O1-O2* | C3-O4 -> C1'-O2* | O3-O4 -> C1'-O2* | C3'-O4'
%%
%%      Deletion from C3. Same as (a).
%%
%%    (f) C1-O2* | C3-O4*
%%      This should be never appears. C1-O2*C3.
%%
%%    (g) C1-O2* | C3-O4-C5
%%      Deletion from C1.
%%        O1-O2* | C3-O4-C5 -> C1'-O2* | O3-O4-C5 -> C1'-O2* | C3'-O4'-C5
%%
%%      Deletion from C3 or C5 same as (c).
%%
%%    (h) C1-O2* | C3-O4*-C5
%%      Deletion from C1.
%%        O1-O2* | C3-O4*-C5 -> C1'-O2* | O3-O4*-C5 -> C1'-O2* | C3'-O5'
%%
%%    (i) C1-O2-C3 | C4-O5
%%       Same as (a) and (c)
%%
%%    (j) C1-O2-C3 | C4-O5*
%%      Deletion from C1 or C3. Same as (c).
%%
%%      Deletion from C4
%%        C1-O2-C3 | O4-O5* -> C1-O2 | C3-O4
%%
%%    (k) C-O2-C3 | C4-O5-C6
%%      Same as (c)
%%
%%    (l) C1-O2-C3 | C4-O5*-C6
%%      Deletion from C1 or C3. Same as (c).
%%
%%      Deletion from C4.
%%        C1-O2-C3 | O4-O5*-C6 -> C1-O2-C3 | C4'-O6
%%
%%      Deletion from C6.
%%        C1-O2-C3 | C4-O5*-O6 -> C1-O2-C3 | C4-O6
%%
%%    (m) C1-O2*-C3 | C4-O5
%%      Deletion from C1.
%%        O1-O2*-C3 | C4-O5 -> C1'-O3' | C4-O5
%%
%%      Deletion from C3.
%%        C1-O2*-O3 | C4-O5 -> C1-O3 | C4-O5
%%
%%    (n) C1-O2*-C3 | C4-O5*
%%      Deletion from C1 or C3. Same as (m).
%%
%%      Deletion from C4.
%%        C1-O2*-C3 | O4-O5* -> C1-O2* | C3-O4
%%
%%    (o) C1-O2*-C3 | C4-O5-C6
%%      Deletion from C1 or C3. Same as (m)
%%      Deletion from C4 or C6. Same as (c)
%%
%%    (p) C1-O2*-C3 | C4-O5*-C6
%%      Same as (m)

%%====================================================================
%% API
%%====================================================================
get_left_op(Bucket) ->
    gen_server:call(Bucket, get_left_op).

get_right_op(Bucket) ->
    gen_server:call(Bucket, get_right_op).

insert_op(Bucket, Key, Value) ->
    ok.

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Capacity]) ->
    {ok, #state{left=[], right=[]}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(get_left_op, _From, State) ->
    {reply, State#state.left, State};

handle_call(get_right_op, _From, State) ->
    {reply, State#state.right, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Function:
%% Description:
%%--------------------------------------------------------------------

%%====================================================================
%% Internal functions
%%====================================================================
