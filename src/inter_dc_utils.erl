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
    get_txn_address/0,
    get_txn_address_list/0]).

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
    Port = gingko_env_utils:get_request_port(),
    {get_ip(), Port}.

-spec get_request_address_list() -> [socket_address()].
get_request_address_list() ->
    Port = gingko_env_utils:get_request_port(),
    [{Ip, Port} || Ip <- get_ip_list_without_local_ip()].

-spec get_txn_address() -> socket_address().
get_txn_address() ->
    Port = gingko_env_utils:get_txn_port(),
    {get_ip(), Port}.

-spec get_txn_address_list() -> [socket_address()].
get_txn_address_list() ->
    Port = gingko_env_utils:get_txn_port(),
    [{Ip, Port} || Ip <- get_ip_list_without_local_ip()].

-spec sort_dc_address_list(dc_address_list()) -> dc_address_list().
sort_dc_address_list(DcAddressList) ->
    lists:sort(
        lists:map(
            fun({Node, SocketAddressList}) ->
                {Node, lists:sort(SocketAddressList)}
            end, DcAddressList)).

-spec sort_descriptor(descriptor()) -> descriptor().
sort_descriptor(Descriptor = #descriptor{txn_dc_address_list = TxnDcAddressList, request_dc_address_list = RequestDcAddressList}) ->
    Descriptor#descriptor{txn_dc_address_list = sort_dc_address_list(TxnDcAddressList), request_dc_address_list = sort_dc_address_list(RequestDcAddressList)}.
