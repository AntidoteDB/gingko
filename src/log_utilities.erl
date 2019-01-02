-module(log_utilities).

-include("gingko.hrl").

%% API
-export([
  filter_terms_for_key/6
]).


%% @doc Given a list of log_records, this method filters the ones corresponding to Key.
%% If key is undefined then is returns all records for all keys
%% It returns a dict corresponding to all the ops matching Key and
%% a list of the committed operations for that key which have a smaller commit time than MinSnapshotTime.
-spec filter_terms_for_key(
    [{non_neg_integer(), #log_record{}}],   % list of log record tuples, {index, payload}
    key(),                                  % key to filter
    snapshot_time(),                        % minimal snapshot time
    snapshot_time(),                        % maximal snapshot time
    dict:dict(txid(), [any_log_payload()]), % accumulator for any type of operation records (possibly uncommitted)
    dict:dict(key(), [#clocksi_payload{}])  % accumulator for committed operations
) -> {
  dict:dict(txid(), [any_log_payload()]),   % all accumulated operations for key and snapshot filter
  dict:dict(key(), [#clocksi_payload{}])    % accumulated committed operations for key and snapshot filter
}.
filter_terms_for_key([], _Key, _MinSnapshotTime, _MaxSnapshotTime, Ops, CommittedOpsDict) ->
  {Ops, CommittedOpsDict};

filter_terms_for_key([{_Index, LogRecord} | OtherRecords], Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict) ->
  logger:warning("Log record ~p", [LogRecord]),

  #log_record{log_operation = LogOperation} = check_log_record_version(LogRecord),

  #log_operation{tx_id = TxId, op_type = OpType, log_payload = OpPayload} = LogOperation,
  case OpType of
    update ->
      handle_update(TxId, OpPayload, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict);
    commit ->
      handle_commit(TxId, OpPayload, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict);
    _ ->
      filter_terms_for_key(OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict)
  end.


%% @doc Handles one 'update' log record
%%      Filters according to key and snapshot times
%%      If filter matches, appends payload to operations accumulator
-spec handle_update(
    txid(),                                   % used to identify tx id for the 'handle_commit' function
    #update_log_payload{},                    % update payload read from the log
    [{non_neg_integer(), #log_record{}}],     % rest of the log
    key(),                                    % filter for key
    snapshot_time() | undefined,              % minimal snapshot time
    snapshot_time(),                          % maximal snapshot time
    dict:dict(txid(), [any_log_payload()]),   % accumulator for any type of operation records (possibly uncommitted)
    dict:dict(key(), [#clocksi_payload{}])    % accumulator for committed operations
) -> {
  dict:dict(txid(), [any_log_payload()]),     % all accumulated operations for key and snapshot filter
  dict:dict(key(), [#clocksi_payload{}])      % accumulated committed operations for key and snapshot filter
}.
handle_update(TxId, OpPayload, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict) ->
  #update_log_payload{key = PayloadKey} = OpPayload,
  case (Key == {key, PayloadKey}) or (Key == undefined) of
    true ->
      % key matches: append to all operations accumulator
      filter_terms_for_key(OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime,
        dict:append(TxId, OpPayload, Ops), CommittedOpsDict);
    false ->
      % key does not match: skip
      filter_terms_for_key(OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict)
  end.


%% @doc Handles one 'commit' log record
%%      Filters according to key and snapshot times
%%      If filter matches, appends payload to operations accumulator
-spec handle_commit(
    txid(),                                   % searches for operations belonging to this tx id
    #commit_log_payload{},                    % update payload read from the log
    [{non_neg_integer(), #log_record{}}],     % rest of the log
    key(),                                    % filter for key
    snapshot_time() | undefined,              % minimal snapshot time
    snapshot_time(),                          % maximal snapshot time
    dict:dict(txid(), [any_log_payload()]),   % accumulator for any type of operation records (possibly uncommitted)
    dict:dict(key(), [#clocksi_payload{}])    % accumulator for committed operations
) -> {
  dict:dict(txid(), [any_log_payload()]),     % all accumulated operations for key and snapshot filter
  dict:dict(key(), [#clocksi_payload{}])      % accumulated committed operations for key and snapshot filter
}.
handle_commit(TxId, OpPayload, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict) ->

  #commit_log_payload{commit_time = {DcId, TxCommitTime}, snapshot_time = SnapshotTime} = OpPayload,

  case dict:find(TxId, Ops) of
    {ok, OpsList} ->
      NewCommittedOpsDict =
        lists:foldl(fun(#update_log_payload{key = KeyInternal, type = Type, op = Op}, Acc) ->
          case (check_min_time(SnapshotTime, MinSnapshotTime) andalso
            check_max_time(SnapshotTime, MaxSnapshotTime)) of
            true ->
              CommittedDownstreamOp =
                #clocksi_payload{
                  key = KeyInternal,
                  type = Type,
                  op_param = Op,
                  snapshot_time = SnapshotTime,
                  commit_time = {DcId, TxCommitTime},
                  txid = TxId},
              dict:append(KeyInternal, CommittedDownstreamOp, Acc);
            false ->
              Acc
          end
                    end, CommittedOpsDict, OpsList),
      %% TODO committed ops are not found in the ops accumulator?
      filter_terms_for_key(OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, dict:erase(TxId, Ops),
        NewCommittedOpsDict);
    error ->
      filter_terms_for_key(OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict)
  end.


%% @doc Check the version of the log record and convert
%% to a different version if necessary
%% Checked when loading the log from disk, or
%% when log messages are received from another DC
-spec check_log_record_version(#log_record{}) -> #log_record{}.
check_log_record_version(LogRecord) ->
  %% Only support one version for now
  ?LOG_RECORD_VERSION = LogRecord#log_record.version,
  LogRecord.


%%noinspection ErlangUnresolvedFunction
check_min_time(SnapshotTime, MinSnapshotTime) ->
  ((MinSnapshotTime == undefined) orelse (vectorclock:ge(SnapshotTime, MinSnapshotTime))).


%%noinspection ErlangUnresolvedFunction
check_max_time(SnapshotTime, MaxSnapshotTime) ->
  ((MaxSnapshotTime == undefined) orelse (vectorclock:le(SnapshotTime, MaxSnapshotTime))).
