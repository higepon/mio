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

%% API
-export([]).

%%====================================================================
%%  Bucket layer
%%====================================================================
%%
%%  C: Closed bucket
%%  O: Open bucket
%%  O*: Open and empty bucket
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
%%    (a) O -> O'
%%
%%    (b) O -> [C] -> C-O
%%
%%    (c) C-O -> C-O'
%%
%%    (d) C1-O2 -> [C1-C2] -> C1-O3-C2
%%      Insertion new key to C1 causes key transfer from C1 to O2.
%%      O2 becomes closed C2.
%%      A fresh bucket is allocated and inserted into between C1 and C2 to satisfy the invariants.
%%
%%    (e) C-O-C -> C-'O-C
%%
%%    (f) C1-O2-C3 -> C1-O2'-C3'
%%       Insertion new key to C3 caused key transfer from C3 to O2.
%%
%%    (g) C1-O2-C3 -> C1-O2 | C3-O4
%%      Insertion new key to C3 causes a new group C3-O4.
%%
%%    (h) C1-O2-C3 -> [C1-C2-C3] -> C1-O2 | C3-O4
%%      Insertion new key to C1 causes key transfer from C1 to O2.
%%      O2 bacomes closed C2.
%%      C2 transfers a key to C3 in order to stay open.
%%      C3 must then transfer a key to empty bucket O4.
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
%%      Deletion from C3 causes O4-> C3
%%        C1-O2 | C3'-O4'-C5

%%
%%    (a) C-O -> C-O'
%%
%%    (b) C1-O2 -> [O1-O2] -> C1'-O2'
%%      Deletion from C1 caused key transfer from O2 to O1.
%%
%%    (c) C1-O2  | C3-O4* -> [C1-O2 | O3-O4*] -> C1-O2'-C3'
%%      Deletion from C3.
%%
%%    (d) C1-O2*  | C3-O4* -> [C1-O2* | O3-O4*] -> C1-O3
%%      Deletion from C3.
%%
%%    (e) C1-O2-C3 | C4-O5* -> [C1-O2-O3 | C4-O5*] -> C1-O2 | C3'-O4
%%      Deletion from C3 caused key transfer from C4 to O3.




%%====================================================================
%% API
%%====================================================================

%%--------------------------------------------------------------------
%% Function:
%% Description:
%%--------------------------------------------------------------------

%%====================================================================
%% Internal functions
%%====================================================================
