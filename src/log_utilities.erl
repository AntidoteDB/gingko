%% @doc Utility functions for filtering log entries.
-module(log_utilities).

-include("gingko.hrl").

%% API
-export([
  filter_terms_for_key/6
]).

journal_entry_contains_key(Key, JournalEntry) ->
  Operation = JournalEntry#journal_entry.operation,
  if
    is_record(Operation, object_operation) -> Operation#object_operation.key_struct#key_struct.key == Key;
    true -> false
  end.

journal_entries_contains_key(Key, JournalEntries) ->
  lists:any(fun(J) -> journal_entry_contains_key(Key, J) end, JournalEntries).

contains_commit_journal_entry(JournalEntries) ->
  lists:any(fun(J) -> is_commit_journal_entry(J) end, JournalEntries).

is_commit_journal_entry(JournalEntry) ->
  Operation = JournalEntry#journal_entry.operation,
  if
    is_record(Operation, system_operation) -> Operation#system_operation.op_type == commit_txn;
    true -> false
  end.

groupBy(Fun, List) -> lists:foldr(fun({Key, Value}, Dict) -> dict:append(Key, Value, Dict) end , dict:new(), [ {Fun(X), X} || X <- List ]).

get_indexed_journal_entries(JournalEntries) ->
  lists:mapfoldl(fun(J, Index) -> {{Index, J}, Index + 1} end, 0, JournalEntries).

get_committed_journal_entries_for_key(Key, JournalEntries) ->

  IndexedJournalEntries = get_indexed_journal_entries(JournalEntries),
  TxIdToJournalEntries = groupBy(fun({_Index, J}) -> J#journal_entry.tx_id end, IndexedJournalEntries),
  TxIdToJournalEntries = dict:filter(fun({_TxId, JList}) -> journal_entries_contains_key(Key, JList) andalso contains_commit_journal_entry(JList) end, TxIdToJournalEntries),
  TxIdToJournalEntries = dict:map(fun(_TxId, JList) -> lists:sort(fun({Index1, _J1}, {Index2, _J2}) -> Index1 > Index2 end, JList) end, TxIdToJournalEntries),


  CommitJournalEntries = lists:filter(fun(JournalEntry) -> is_commit_journal_entry(JournalEntry) end, JournalEntries),
  CommittedTxIds = lists:map(fun(JournalEntry) -> JournalEntry#journal_entry.tx_id end, CommitJournalEntries),

  ok.

%% @doc Given a list of log_records, this method filters the ones corresponding to Key.
%% If key is undefined then is returns all records for all keys
%% It returns a dict corresponding to all the ops matching Key and
%% a list of the committed operations for that key which have a smaller commit time than MinSnapshotTime.
%%
%% @param OtherRecords list of log record tuples, {index, payload}
%% @param Key key to filter
%% @param MinSnapshotTime minimal snapshot time
%% @param MaxSnapshotTime maximal snapshot time
%% @param Ops dict accumulator for any type of operation records (possibly uncommitted)
%% @param CommittedOpsDict dict accumulator for committed operations
%% @returns a {dict, dict} tuple with accumulated operations and committed operations for key and snapshot filter
-spec filter_terms_for_key(
    [{non_neg_integer(), [journal_entry()]}],
    key(),
    snapshot_time(),
    snapshot_time(),
    dict:dict(tx_id(), [any_log_payload()]),
    dict:dict(key(), [#clocksi_payload{}])
) -> {
  dict:dict(tx_id(), [any_log_payload()]),
  dict:dict(key(), [#clocksi_payload{}])
}.
filter_terms_for_key([], _Key, _MinSnapshotTime, _MaxSnapshotTime, Ops, CommittedOpsDict) ->
  {Ops, CommittedOpsDict};

filter_terms_for_key([LogRecord | OtherRecords], Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict) ->
  logger:debug("Log record ~p", [LogRecord]),

  #journal_entry{operation = LogOperation} = check_log_record_version(LogRecord),

  #operation{tx_id = TxId, op_type = OpType, log_payload = OpPayload} = LogOperation,
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
    tx_id(),                                   % used to identify tx id for the 'handle_commit' function
    #update_log_payload{},                    % update payload read from the log
    [{non_neg_integer(), #log_record{}}],     % rest of the log
    key(),                                    % filter for key
    snapshot_time() | undefined,              % minimal snapshot time
    snapshot_time(),                          % maximal snapshot time
    dict:dict(tx_id(), [any_log_payload()]),   % accumulator for any type of operation records (possibly uncommitted)
    dict:dict(key(), [#clocksi_payload{}])    % accumulator for committed operations
) -> {
  dict:dict(tx_id(), [any_log_payload()]),     % all accumulated operations for key and snapshot filter
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
    tx_id(),                                   % searches for operations belonging to this tx id
    #commit_log_payload{},                    % update payload read from the log
    [{non_neg_integer(), #log_record{}}],     % rest of the log
    key(),                                    % filter for key
    snapshot_time() | undefined,              % minimal snapshot time
    snapshot_time(),                          % maximal snapshot time
    dict:dict(tx_id(), [any_log_payload()]),   % accumulator for any type of operation records (possibly uncommitted)
    dict:dict(key(), [#clocksi_payload{}])    % accumulator for committed operations
) -> {
  dict:dict(tx_id(), [any_log_payload()]),     % all accumulated operations for key and snapshot filter
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
                  tx_id = TxId},
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


%%noinspection ErlangUnresolvedFunction
check_min_time(SnapshotTime, MinSnapshotTime) ->
  ((MinSnapshotTime == undefined) orelse (vectorclock:ge(SnapshotTime, MinSnapshotTime))).


%%noinspection ErlangUnresolvedFunction
check_max_time(SnapshotTime, MaxSnapshotTime) ->
  ((MaxSnapshotTime == undefined) orelse (vectorclock:le(SnapshotTime, MaxSnapshotTime))).
