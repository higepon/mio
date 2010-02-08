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
%%% File    : mio_logger.erl
%%% Author  : higepon <higepon@labs.cybozu.co.jp>
%%% Description : Wrapper of error_logger
%%%
%%% Created :  4 Jan 2010 by higepon <higepon@labs.cybozu.co.jp>
%%%-------------------------------------------------------------------
-module(mio_logger).
-include("mio.hrl").
-behaviour(gen_server).

%% API
-export([start_link/0, info_msg/2, error_msg/2, warn_msg/2, is_verbose/0, set_verbose/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%====================================================================
%% API
%%====================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


info_msg(Format, Args) ->
    gen_server:call(?MODULE, {info_msg, Format, Args}).


error_msg(Format, Args) ->
    gen_server:call(?MODULE, {warn_msg, Format, Args}).


warn_msg(Format, Args) ->
    gen_server:call(?MODULE, {warn_msg, Format, Args}).

is_verbose() ->
    gen_server:call(?MODULE, is_verbose).


set_verbose(IsVerbose) ->
    gen_server:call(?MODULE, {set_verbose, IsVerbose}).


%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({info_msg, Format, Args}, From, IsVerbose) ->
    spawn(fun() ->
                  case IsVerbose of
                      true ->
                          error_logger:info_msg(Format, Args);
                      _ ->
                          []
                  end,
                  gen_server:reply(From, ok)
          end),
    {noreply, IsVerbose};


handle_call({error_msg, Format, Args}, From, IsVerbose) ->
    spawn(fun() ->
                  case IsVerbose of
                      true ->
                          error_logger:error_msg(Format, Args);
                      _ ->
                          []
                  end,
                  gen_server:reply(From, ok)
          end),
    {noreply, IsVerbose};


handle_call({warn_msg, Format, Args}, From, IsVerbose) ->
    spawn(fun() ->
                  case IsVerbose of
                      true ->
                          error_logger:warn_msg(Format, Args);
                      _ ->
                          []
                  end,
                  gen_server:reply(From, ok)
          end),
    {noreply, IsVerbose};


handle_call(is_verbose, _From, IsVerbose) ->
    {reply, IsVerbose, IsVerbose};


handle_call({set_verbose, IsVerbose}, _From, _IsVerbose) ->
    {reply, ok, IsVerbose}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
    io:format("cast=~p~n", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(Info, State) ->
    io:format("info=~p~n", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _IsVerbose) ->
    io:format("terminate ~p~n", [self()]).

init([]) ->
    {ok, true}.
