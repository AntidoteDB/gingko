%% @doc Utility functions for filtering log entries.
-module(log_utilities).

-include("gingko.hrl").

%% API
-export([
  filter_terms_for_key/6,
  group_by/2,
  add_to_value_list_or_create_single_value_list/3
]).

-spec group_by(fun((ListType :: term()) -> GroupKeyType :: term()), [ListType :: term()]) -> [{GroupKeyType :: term(), [ListType :: term()]}].
group_by(Fun, List) -> lists:foldr(fun({Key, Value}, Dict) -> dict:append(Key, Value, Dict) end , dict:new(), [ {Fun(X), X} || X <- List ]).

-spec add_to_value_list_or_create_single_value_list(DictionaryTypeAToValueTypeBList :: dict(), KeyTypeA :: term(), ValueTypeB :: term()) -> dict().
add_to_value_list_or_create_single_value_list(DictionaryTypeAToValueTypeBList, KeyTypeA, ValueTypeB) ->
  dict:update(KeyTypeA, fun(ValueTypeBList) -> [ValueTypeB|ValueTypeBList] end, [ValueTypeB], DictionaryTypeAToValueTypeBList).

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
