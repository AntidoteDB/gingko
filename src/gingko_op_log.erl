%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Antidote Consortium.  All Rights Reserved.
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
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc API module to communicate with the operation log.
%% The op-log logs operations to disk in sequential order.
%% It keeps one log file in which operations are kept in a total order.
%% Operations are consistent with the order in which the operations were issued at the original data-center.
%% The server replies only after operations have been written to disk.
%%
%% Provides the following functionality:
%% <ul>
%%   <li> Append a log record </li>
%%   <li> Read all entries (optionally with custom accumulator) </li>
%% </ul>

-module(gingko_op_log).
-include("gingko.hrl").

%% Start-up and shutdown
-export([start_link/2, stop/1]).

%% API functions
-export([append/2, read_log_entries/3, read_log_entries/5]).


%% ==============
%% Implementation
%% ==============

%% @doc Starts and links the operation log process.
%%
%% The first argument is the name for the operation log.
%% The second argument is the process which receives recovery messages.
%%
%% After starting, log recovery starts automatically.
%% For each entry in the log found in the directory denoted by the log name, a message
%%     {log_recovery, {Index, LogEntry}}
%% is sent to the RecoveryReceiver and when recovery is finished a message
%%     'log_recovery_done'
%% is sent.
%%
%% Log recovery can be disabled via the 'log_persistence' flag.
-spec start_link(term(), pid()) -> {ok, pid()} | ignore | {error, Error :: any()}.
start_link(LogName, RecoveryReceiver) ->
  gingko_op_log_server:start_link(LogName, RecoveryReceiver).


%% @doc Appends a log entry to the end of the log.
%%
%% The first argument is the process returned by start_link.
%% The second argument is the log record to append.
%%
%% When the function returns 'ok' the entry is guaranteed to be persistently stored.
-spec append(pid(), #log_record{}) -> ok  | {error, Reason :: term()}.
append(Log, Entry) ->
  case gen_server:call(Log, {add_log_entry, Entry}) of
    %% request got stuck in queue (server busy) and got retry signal
    retry -> logger:debug("Retrying request"), append(Log, Entry);
    Reply -> Reply
  end.


%% @doc Read all log entries belonging to a given node in a certain range.
%% @see read_log_entries/5
-spec read_log_entries(pid(), integer(), integer() | all) -> {ok, [#log_record{}]}.
read_log_entries(Log, FirstIndex, LastIndex) ->
  F = fun(D, Acc) -> Acc ++ [D] end,
  read_log_entries(Log, FirstIndex, LastIndex, F, []).


%% @doc Read all log entries belonging to a given node and in a certain range with a custom accumulator.
%%
%% The function works similar to lists:foldl for reading the entries.
%% The fold function takes a single log entry and the current accumulator and returns the new accumulator value.
%%
%% Returns the accumulator value after reading all matching log entries.
-spec read_log_entries(pid(), integer(), integer() | all,
    fun((#log_record{}, Acc) -> Acc), Acc) -> {ok, Acc}.
read_log_entries(Log, FirstIndex, LastIndex, FoldFunction, Accumulator) ->
  case gen_server:call(Log, {read_log_entries, FirstIndex, LastIndex, FoldFunction, Accumulator}) of
    retry -> logger:debug("Retrying request"), read_log_entries(Log, FirstIndex, LastIndex, FoldFunction, Accumulator);
    Reply -> Reply
  end.


%% @doc Stops the op_log process.
-spec stop(pid()) -> any().
stop(Log) ->
  gen_server:stop(Log).
