%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(zmq_utils).
-include("inter_dc.hrl").

-export([create_connect_socket/3,
    create_bind_socket/3,
    set_socket_option/3,
    set_receive_filter/2,
    set_receive_timeout/2,
    close_socket/1,
    try_send/2,
    try_send/3,
    try_recv/1,
    try_recv/2]).

-spec create_socket(zmq_socket_type(), boolean()) -> zmq_socket() | no_return().
create_socket(Type, Active) ->
    ZmqContext = zmq_context:get(),
    TypeArgs = case Active of
                   true -> [Type, {active, true}];
                   false -> Type
               end,
    case erlzmq:socket(ZmqContext, TypeArgs) of
        {ok, Socket} -> Socket;
        Error ->
            general_utils:print_and_return_unexpected_error(?MODULE, ?FUNCTION_NAME, [Type, Active], Error, [ZmqContext, TypeArgs])
    end.

-spec create_connect_socket(zmq_socket_type(), boolean(), socket_address()) -> zmq_socket() | no_return().
create_connect_socket(Type, Active, Address) ->
    Socket = create_socket(Type, Active),
    ConnectionString = connection_string(Address),
    case erlzmq:connect(Socket, ConnectionString) of
        ok -> Socket;
        Error ->
            general_utils:print_and_return_unexpected_error(?MODULE, ?FUNCTION_NAME, [Type, Active, Address], Error, [Socket, ConnectionString])
    end.

-spec create_bind_socket(zmq_socket_type(), boolean(), inet:port_number()) -> zmq_socket() | no_return().
create_bind_socket(Type, Active, Port) ->
    Socket = create_socket(Type, Active),
    ConnectionString = connection_string({"*", Port}),
    case erlzmq:bind(Socket, ConnectionString) of
        ok -> Socket;
        Error ->
            general_utils:print_and_return_unexpected_error(?MODULE, ?FUNCTION_NAME, [Type, Active, Port], Error, [Socket, ConnectionString])
    end.


-spec connection_string(socket_address()) -> string().
connection_string({Ip, Port}) ->
    IpString =
        case Ip of
            "*" -> Ip;
            _ -> inet_parse:ntoa(Ip)
        end,
    lists:flatten(io_lib:format("tcp://~s:~p", [IpString, Port])).

-spec set_socket_option(zmq_socket(), zmq_socket_option(), zmq_socket_option_value()) -> ok | no_return().
set_socket_option(Socket, SocketOption, SocketOptionValue) ->
    general_utils:run_function_with_unexpected_error(
        erlzmq:setsockopt(Socket, SocketOption, SocketOptionValue), ok, ?MODULE, ?FUNCTION_NAME, [Socket, SocketOption, SocketOptionValue], []).

-spec set_receive_filter(zmq_socket(), binary()) -> ok | no_return().
set_receive_filter(Socket, Prefix) ->
    set_socket_option(Socket, subscribe, Prefix).

-spec set_receive_timeout(zmq_socket(), non_neg_integer()) -> ok | no_return().
set_receive_timeout(Socket, Timeout) ->
    set_socket_option(Socket, rcvtimeo, Timeout).

-spec close_socket(zmq_socket()) -> ok | no_return().
close_socket(Socket) ->
    general_utils:run_function_with_unexpected_error(
        erlzmq:close(Socket), ok, ?MODULE, ?FUNCTION_NAME, [Socket], []).

-spec try_send(zmq_socket(), binary()) -> ok | no_return().
try_send(Socket, BinaryMessage) -> try_send(Socket, BinaryMessage, []).

-spec try_send(zmq_socket(), binary(), zmq_send_recv_flags()) -> ok | no_return().
try_send(Socket, BinaryMessage, Flags) ->
    general_utils:run_function_with_unexpected_error(
        erlzmq:send(Socket, BinaryMessage, Flags), ok, ?MODULE, ?FUNCTION_NAME, [Socket, BinaryMessage, Flags], []).

-spec try_recv(zmq_socket()) -> {ok, zmq_data()} | {error, timeout} | no_return().
try_recv(Socket) -> try_recv(Socket, []).

-spec try_recv(zmq_socket(), zmq_send_recv_flags()) -> {ok, zmq_data()} | {error, timeout} | no_return().
try_recv(Socket, Flags) ->
    case erlzmq:recv(Socket, Flags) of
        {ok, Data} -> {ok, Data};
        {error, eagain} -> {error, timeout};
        Error -> general_utils:print_and_return_unexpected_error(?MODULE, ?FUNCTION_NAME, [Socket], Error, [])
    end.
