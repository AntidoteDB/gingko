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
-include("inter_dc.hrl").

-export([create_request_record/3,
    create_request_entry/2,
    is_relevant_request_for_responder/1,
    request_record_from_binary/1,
    request_record_to_binary/1,
    response_record_from_binary/1,
    response_record_to_binary/1]).

-spec create_request_record({non_neg_integer(), request_type()}, {target_dcid(), target_partition()}, term()) -> request_record().
create_request_record({RequestId, RequestType}, {TargetDCID, TargetPartition}, RequestArgs) ->
    request_record_from_tuple({{RequestId, RequestType}, {TargetDCID, TargetPartition}, {gingko_utils:get_my_dcid(), node()}, RequestArgs}).

-spec create_request_entry(request_record(), response_return_function() | none) -> request_entry().
create_request_entry(RequestRecord, ReturnFuncOrNone) ->
    #request_entry{request_record = RequestRecord, request_timestamp = gingko_utils:get_timestamp(), return_func_or_none = ReturnFuncOrNone}.

-spec request_record_to_tuple(request_record()) -> {{non_neg_integer(), request_type()}, {target_dcid(), target_partition()}, {dcid(), node()}, term()}.
request_record_to_tuple(#request_record{request_id = RequestId,
    request_type = RequestType,
    target_dcid = TargetDCID,
    target_partition = TargetPartition,
    source_dcid = SourceDCID,
    source_node = SourceNode,
    request_args = RequestArgs}) ->
    {{RequestId, RequestType}, {TargetDCID, TargetPartition}, {SourceDCID, SourceNode}, RequestArgs}.

-spec request_record_from_tuple({{non_neg_integer(), request_type()}, {target_dcid(), target_partition()}, {dcid(), node()}, term()}) -> request_record().
request_record_from_tuple({{RequestId, RequestType}, {TargetDCID, TargetPartition}, {SourceDCID, SourceNode}, RequestArgs}) -> #request_record{
        request_id = RequestId,
        request_type = RequestType,
        target_dcid = TargetDCID,
        target_partition = TargetPartition,
        source_dcid = SourceDCID,
        source_node = SourceNode,
        request_args = RequestArgs}.

-spec request_record_to_binary(request_record()) -> binary().
request_record_to_binary(RequestRecord) ->
    term_to_binary(request_record_to_tuple(RequestRecord)).

-spec request_record_from_binary(binary()) -> request_record().
request_record_from_binary(RequestRecordBinary) ->
    request_record_from_tuple(binary_to_term(RequestRecordBinary)).

-spec response_record_to_binary(response_record()) -> binary().
response_record_to_binary(#response_record{request_record = RequestRecord, response = Response}) ->
    term_to_binary({request_record_to_tuple(RequestRecord), Response}).

-spec response_record_from_binary(binary()) -> response_record().
response_record_from_binary(ResponseBinary) ->
    {RequestTuple, Response} = binary_to_term(ResponseBinary),
    #response_record{request_record = request_record_from_tuple(RequestTuple), response = Response}.

-spec is_relevant_request_for_responder(request_record()) -> boolean().
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
