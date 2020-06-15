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

-module(inter_dc_utils).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc.hrl").

-export([get_request_address/0,
    get_request_address_list/0,
    get_journal_address/0,
    get_journal_address_list/0,
    partition_to_binary/1,
    partition_from_binary/1,
    partition_and_rest_binary/1,
    pad/2,
    pad_or_trim/2]).

-spec get_ip() -> inet:ip_address().
get_ip() ->
    hd(get_ip_list()).

-spec get_ip_list() -> [inet:ip_address()].
get_ip_list() ->
    {ok, IpTripleList} = inet:getif(),
    IpList = [Ip || {Ip, _, _} <- IpTripleList],
    %% get host name from node name
    [_, Hostname] = string:tokens(atom_to_list(erlang:node()), "@"),
    case inet:getaddr(Hostname, inet) of
        {ok, HostIp} -> [HostIp | IpList];
        {error, _} -> IpList
    end.

-spec get_ip_list_without_local_ip() -> [inet:ip_address()].
get_ip_list_without_local_ip() ->
    lists:filter(fun(Ip) -> Ip /= {127, 0, 0, 1} end, get_ip_list()).

-spec get_request_address() -> socket_address().
get_request_address() ->
    Port = application:get_env(?GINGKO_APP_NAME, ?REQUEST_PORT_NAME, ?DEFAULT_REQUEST_PORT),
    {get_ip(), Port}.

-spec get_request_address_list() -> [socket_address()].
get_request_address_list() ->
    Port = application:get_env(?GINGKO_APP_NAME, ?REQUEST_PORT_NAME, ?DEFAULT_REQUEST_PORT),
    [{Ip, Port} || Ip <- get_ip_list_without_local_ip()].

-spec get_journal_address() -> socket_address().
get_journal_address() ->
    Port = application:get_env(?GINGKO_APP_NAME, ?JOURNAL_PORT_NAME, ?DEFAULT_JOURNAL_PORT),
    {get_ip(), Port}.

-spec get_journal_address_list() -> [socket_address()].
get_journal_address_list() ->
    Port = application:get_env(?GINGKO_APP_NAME, ?JOURNAL_PORT_NAME, ?DEFAULT_JOURNAL_PORT),
    [{Ip, Port} || Ip <- get_ip_list_without_local_ip()].

-spec pad(non_neg_integer(), binary()) -> binary().
pad(Width, Binary) ->
    case Width - byte_size(Binary) of
        N when N =< 0 -> Binary;
        N -> <<0:(N * 8), Binary/binary>>
    end.

%% Takes a binary and makes it size width
%% if it is too small than it adds 0s
%% otherwise it trims bits from the left size
-spec pad_or_trim(non_neg_integer(), binary()) -> binary().
pad_or_trim(Width, Binary) ->
    case Width - byte_size(Binary) of
        N when N == 0 -> Binary;
        N when N < 0 ->
            Pos = trunc(abs(N)),
            <<_:Pos/binary, Rest:Width/binary>> = Binary,
            Rest;
        N -> <<0:(N * 8), Binary/binary>>
    end.

-spec partition_to_binary(partition_id()) -> binary().
partition_to_binary(Partition) ->
    pad(?PARTITION_BYTE_LENGTH, binary:encode_unsigned(Partition)).

-spec partition_from_binary(binary()) -> partition_id().
partition_from_binary(PartitionBinary) ->
    binary:decode_unsigned(PartitionBinary).

-spec partition_and_rest_binary(binary()) -> {binary(), binary()}.
partition_and_rest_binary(Binary) ->
    <<Partition:?PARTITION_BYTE_LENGTH/big-unsigned-integer-unit:8, RestBinary/binary>> = Binary,
    {Partition, RestBinary}.
