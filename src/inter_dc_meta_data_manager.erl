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

-module(inter_dc_meta_data_manager).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc.hrl").

-export([get_or_create_dc_info_entry/0,
    store_dc_info_entry/1,
    get_dc_descriptors/0,
    get_connected_dcids/0,
    store_dc_descriptors/1,
    has_dc_started/0,
    start_dc/0,
    start_dc/1,
    is_dc_restart/0]).

-define(TABLE_NAME, dc_info_entry).

%%%===================================================================
%%% Public API
%%%===================================================================

-spec get_or_create_dc_info_entry() -> dc_info_entry().
get_or_create_dc_info_entry() ->
    Nodes = lists:sort(gingko_utils:get_my_dc_nodes()),
    FirstNode = hd(Nodes),
    case node() == FirstNode of
        true ->
            DCID = gingko_utils:get_my_dcid(),
            %%TODO check that table exists
            ReadFunc = fun() -> mnesia:read(?TABLE_NAME, DCID) end,
            DcInfoEntryList = mnesia:activity(transaction, ReadFunc),
            DcInfoEntry =
                case DcInfoEntryList of
                    [] -> #dc_info_entry{dcid = DCID, has_started = false, connected_descriptors = []};
                    [Descriptor] -> Descriptor;
                    [FirstDescriptor | _Descriptors] -> FirstDescriptor
                    %%TODO{error, {"BAD", DCID, Descriptors}}
                end,
            store_dc_info_entry(DcInfoEntry),
            DcInfoEntry;
        false ->
            rpc:call(FirstNode, ?MODULE, get_or_create_dc_info_entry, [])
    end.

-spec store_dc_info_entry(dc_info_entry()) -> ok.
store_dc_info_entry(DcInfoEntry) ->
    Nodes = lists:sort(gingko_utils:get_my_dc_nodes()),
    FirstNode = hd(Nodes),
    case FirstNode == node() of
        true ->
            WriteFunc = fun() -> mnesia:write(?TABLE_NAME, DcInfoEntry, write) end,
            mnesia:activity(transaction, WriteFunc),
            ok;
        false ->
            rpc:call(FirstNode, ?MODULE, store_dc_info_entry, [])
    end.

-spec get_dc_descriptors() -> [descriptor()].
get_dc_descriptors() ->
    DcInfoEntry = get_or_create_dc_info_entry(),
    DcInfoEntry#dc_info_entry.connected_descriptors.

-spec get_connected_dcids() -> [dcid()].
get_connected_dcids() ->
    Descriptors = get_dc_descriptors(),
    lists:map(fun(Descriptor) -> Descriptor#descriptor.dcid end, Descriptors).

-spec start_dc() -> ok.
start_dc() ->
    DcInfoEntry = get_or_create_dc_info_entry(),
    case DcInfoEntry#dc_info_entry.has_started of
        true ->
            ok;
        false ->
            store_dc_info_entry(DcInfoEntry#dc_info_entry{has_started = true})
    end.

-spec start_dc([descriptor()]) -> ok.
start_dc(OtherDcDescriptors) ->
    start_dc(),
    store_dc_descriptors(OtherDcDescriptors).

-spec store_dc_descriptors([descriptor()]) -> ok.
store_dc_descriptors(OtherDcDescriptors) ->
    DcInfoEntry = get_or_create_dc_info_entry(),
    case DcInfoEntry#dc_info_entry.has_started of
        true ->
            ok;
        false ->
            ExistingDcDescriptors = DcInfoEntry#dc_info_entry.connected_descriptors,
            NewDescriptors = merge_two_descriptor_lists(OtherDcDescriptors, ExistingDcDescriptors),
            store_dc_info_entry(DcInfoEntry#dc_info_entry{connected_descriptors = NewDescriptors})
    end.

-spec has_dc_started() -> boolean().
has_dc_started() ->
    DcInfoEntry = get_or_create_dc_info_entry(),
    DcInfoEntry#dc_info_entry.has_started.

-spec is_dc_restart() -> boolean().
is_dc_restart() ->
    has_dc_started().

-spec merge_two_descriptor_lists([descriptor()], [descriptor()]) -> [descriptor()].
merge_two_descriptor_lists(NewDescriptors, OldDescriptors) ->
    merge_descriptors(NewDescriptors ++ OldDescriptors, []).

%%TODO if descriptors are duplicate then make sure connections are severed
-spec merge_descriptors([descriptor()], [descriptor()]) -> [descriptor()].
merge_descriptors([Descriptor | Descriptors], []) -> merge_descriptors(Descriptors, [Descriptor]);
merge_descriptors([], DescriptorAcc) -> DescriptorAcc;
merge_descriptors([Descriptor = #descriptor{dcid = DCID} | OtherDescriptors], DescriptorAcc) ->
    MatchingDescriptors = lists:any(fun(#descriptor{dcid = MatchDCID}) -> DCID == MatchDCID end, DescriptorAcc),
    case MatchingDescriptors of
        false -> merge_descriptors(OtherDescriptors, [Descriptor | DescriptorAcc]);
        true -> merge_descriptors(OtherDescriptors, DescriptorAcc)
    end.
