%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(mnesia_utils).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-export([run_transaction/1,
    run_sync_transaction/1,
    get_mnesia_result/1]).

-spec run_transaction(fun(() -> ResType :: term())) -> ResType :: term().
run_transaction(F) ->
    run_operation(transaction, F).

-spec run_sync_transaction(fun(() -> ResType :: term())) -> ResType :: term().
run_sync_transaction(F) ->
    run_operation(sync_transaction, F).

-spec run_operation(atom(), fun(() -> ResType :: term())) -> ResType :: term().
run_operation(ActivityType, F) ->
    get_mnesia_result(mnesia:activity(ActivityType, F)).

-spec get_mnesia_result({atomic, ResType :: term()} | {aborted, reason()} | term()) -> ResType :: term() | {error, reason()}.
get_mnesia_result(ActivityResult) ->
    case ActivityResult of
        {atomic, Res1} -> Res1;
        {aborted, Reason} -> {error, {aborted, Reason}};
        Res2 -> Res2
    end.
