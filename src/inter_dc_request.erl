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

-module(inter_dc_request).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc_repl.hrl").

-export([create_request_record/3,
    create_request_entry/2,
    is_relevant_request_for_responder/1]).

create_request_record({RequestId, RequestType}, {TargetDCID, TargetPartition}, Request) ->
    #request_record{
        request_id = RequestId,
        request_type = RequestType,
        target_dcid = TargetDCID,
        target_partition = TargetPartition,
        source_dcid = gingko_utils:get_my_dcid(),
        source_node = node(),
        request = Request
    }.

create_request_entry(RequestRecord, ReturnFunc) ->
    #request_entry{request_record = RequestRecord, return_func = ReturnFunc}.

is_relevant_request_for_responder(#request_record{target_dcid = TargetDCID, target_partition = TargetPartition}) ->
    CheckPartition =
        case TargetDCID of
            all ->
                true;
            SomeDCID ->
                SomeDCID == gingko_utils:get_my_dcid()
        end,
    case CheckPartition of
        true ->
            case TargetPartition of
                all -> true;
                SomePartition ->
                    lists:member(SomePartition, gingko_utils:get_my_partitions())
            end;
        false ->
            false
    end.

%%TODO equal requests
%%-spec is_equal_request(request_entry(), request_entry()) -> boolean().
%%is_equal_request(#request_entry{request = Request1}, #request_entry{request = Request2}) ->
%%    Request1 == Request2.
