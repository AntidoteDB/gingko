-module(gingko_op_log).
-include("gingko.hrl").

%% API
-export([]).

%% The op-log logs operations to disk
%% It keeps one log file in which operations are kept in a total order
%% Operations are consistent with the order in which the operations were
%% issued at the original data-center.
%% The server replies only after operations have been written to disk.

-export([start_link/2, append/2, read_log_entries/3, read_log_entries/5, stop/1]).


%% ==============
%% API
%% ==============

% Starts the operation log process
%
% The first argument is the name for the operation log.
%
% The second argument is the process which receives recovery messages.
% After starting, log recovery starts automatically.
% For each entry in the log found in the directory denoted by the log name, a message
%     {log_recovery, {Index, LogEntry}}
% is sent to the RecoveryReceiver and when recovery is finished a message
%     'log_recovery_done'
% is sent.
-spec start_link(term(), pid()) -> {ok, pid()} | ignore | {error, Error :: any()}.


% Appends a log entry to the end of the log.
%
% The first argument is the process returned by start_link.
%
% The second argument is the log entry to store, consisting of index and data.
%
% When the function returns 'ok' the entry is guaranteed to be persistently stored.
-spec append(pid(), #log_record{}) -> ok  | {error, Reason :: term()}.


% Read all log entries belonging to a given node and in a certain range.
%
% The function works similar to lists:foldl for reading the entries.
%
% Arg1: the process returned by start_link
%
% Arg2: the first index to read
%
% Arg3: the last index to read, or 'all' for reading all entries
%
% Arg4: the fold function, which takes a single log entry and the current
%       accumulator and returns the new accumulator value.
%
% Arg5: Acc is the initial accumulator value.
%
% Returns the accumulator value after reading all matching log entries.
-spec read_log_entries(pid(), integer(), integer() | all,
    fun((#log_record{}, Acc) -> Acc), Acc) -> {ok, Acc}.
% simplified read api without accumulator
-spec read_log_entries(pid(), integer(), integer() | all) -> {ok, [#log_record{}]}.

% Stops the op_log process.
-spec stop(pid()) -> any().


% ===========================================
% IMPLEMENTATION
% ===========================================
start_link(LogName, RecoveryReceiver) ->
  gingko_op_log_server:start_link(LogName, RecoveryReceiver).


append(Log, Entry) ->
  gen_server:call(Log, {add_log_entry, Entry}).


read_log_entries(Log, FirstIndex, LastIndex, F, Acc) ->
  gen_server:call(Log, {read_log_entries, FirstIndex, LastIndex, F, Acc}).


read_log_entries(Log, FirstIndex, LastIndex) ->
  F = fun(D, Acc) -> Acc ++ [D] end,
  read_log_entries(Log, FirstIndex, LastIndex, F, []).


stop(Log) ->
  gen_server:stop(Log).

