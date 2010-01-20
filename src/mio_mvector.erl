%%    Copyright (c) 2009-2010  Taro Minowa(Higepon) <higepon@users.sourceforge.jp>
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
%%% File    : mio_mvector.erl
%%% Author  : higepon <higepon@users.sourceforge.jp>
%%% Description : Operations of membership vector
%%%
%%% Created : 21 Oct 2009 by higepon <higepon@users.sourceforge.jp>
%%%-------------------------------------------------------------------
-module(mio_mvector).

%% API
-export([make/1, eq/2, eq/3, gt/2, gt/3, get/2, generate/1]).


%%====================================================================
%% API
%%====================================================================
make(Args) ->
    Args.


eq(A, B) ->
    A =:= B.


eq(Level, A, B) ->
    eq(get(A, Level), get(B, Level)).


generate(MaxLevel) ->
    %% each process requires this once.
    {A1, A2, A3} = now(),
    random:seed(A1, A2, A3),
    [random:uniform(2) - 1 || _ <- lists:duplicate(MaxLevel, 0)].

gt(A, B) ->
    A >= B.
gt(Level, A, B) ->
    gt(get(A, Level), get(B, Level)).


get(MVector, Level) ->
    case Level of
        0 ->
            [];
        _ ->
            lists:sublist(MVector, Level)
    end.
