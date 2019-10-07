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
-export([stop/1]).

%% API functions
-export([append/2, read_journal_entries/1]).


%% ==============
%% Implementation
%% ==============

-spec append(node(), [journal_entry()]) -> ok  | {error, Reason :: term()}.
append(LogNode, JournalEntries) ->
  case gen_server:call(LogNode, {add_log_entry, JournalEntries}) of
    %% request got stuck in queue (server busy) and got retry signal
    retry -> logger:debug("Retrying request"), append(LogNode, JournalEntries);
    Reply -> Reply
  end.

-spec read_journal_entries(node()) -> [journal_entry()].
read_journal_entries(LogNode) ->
  case gen_server:call(LogNode, {read_log_entries, all}) of
    retry -> logger:debug("Retrying request"), read_journal_entries(LogNode);
    Reply -> Reply
  end.

-spec read_journal_entries(node(), key()) -> [journal_entry()].
read_journal_entries(LogNode, Key) ->
  case gen_server:call(LogNode, {read_log_entries, Key}) of
    retry -> logger:debug("Retrying request"), read_journal_entries(LogNode, Key);
    Reply -> Reply
  end.

-spec stop(node()) -> any().
stop(LogNode) ->
  gen_server:stop(LogNode).
