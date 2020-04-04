%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Mär 2020 16:40
%%%-------------------------------------------------------------------
-module(gingko_log).
-author("kevin").
-include("gingko.hrl").
%% API
-export([add_journal_entry/2, add_or_update_checkpoint_entry/1, read_journal_entry/2, read_checkpoint_entry/2, read_all_journal_entries/1, match_journal_entries/2, read_journal_entries_with_tx_id/2, perform_tx_read/4, persist_journal_entries/1, clear_journal_entries/1, read_all_journal_entries_sorted/1, read_journal_entries_with_tx_id_sorted/2, checkpoint/2]).

-spec persist_journal_entries(atom()) -> {atomic, ok} | {aborted, reason()}.
persist_journal_entries(TableName) ->
  {atomic, ok} = mnesia:dump_tables([TableName]).

-spec clear_journal_entries(atom()) -> {atomic, ok} | {aborted, reason()}.
clear_journal_entries(TableName) ->
  {atomic, ok} = mnesia:clear_table(TableName),
  persist_journal_entries(TableName).

-spec add_journal_entry(journal_entry(), atom()) -> ok | {error, {already_exists, [journal_entry()]}} | 'transaction abort'.
add_journal_entry(JournalEntry, TableName) ->
  F = fun() ->
    case mnesia:read(TableName, JournalEntry#journal_entry.jsn) of
      [] -> mnesia:write(TableName, JournalEntry, write);
      ExistingJournalEntries -> {error, {already_exists, ExistingJournalEntries}}
    end
      end,
  mnesia:activity(transaction, F).

-spec read_journal_entry(jsn(), atom()) -> journal_entry().%TODO compiler bug | {error, {"Multiple Journal Entries found", jsn(), [journal_entry()]}} | {error, {"No Journal Entry found", jsn()}} .
read_journal_entry(Jsn, TableName) ->
  List = mnesia:activity(transaction, fun() -> mnesia:read(TableName, Jsn) end),
  case List of
    [] -> {error, {"No Journal Entry found", Jsn}};
    [J] -> J;
    Js -> {error, {"Multiple Journal Entries found", Jsn, Js}}
  end.

-spec read_all_journal_entries(atom()) -> [journal_entry()].
read_all_journal_entries(TableName) ->
  mnesia:activity(transaction,
    fun() ->
      mnesia:foldl(fun(J, Acc) -> [J | Acc] end, [], TableName)
    end).

-spec read_all_journal_entries_sorted(atom()) -> [journal_entry()].
read_all_journal_entries_sorted(TableName) ->
  mnesia:activity(transaction,
    fun() ->
      mnesia:foldl(
        fun(J, Acc) ->
          general_utils:sorted_insert(J, Acc,
            fun(J1, J2) ->
              J1#journal_entry.jsn =< J2#journal_entry.jsn
            end)
        end, [], TableName)
    end).

-spec read_journal_entries_with_tx_id(txid(), atom()) -> [journal_entry()].
read_journal_entries_with_tx_id(TxId, TableName) ->
  MatchJournalEntry = #journal_entry{tx_id = TxId, _ = '_'},
  match_journal_entries(MatchJournalEntry, TableName).

-spec read_journal_entries_with_tx_id_sorted(txid(), atom()) -> [journal_entry()].
read_journal_entries_with_tx_id_sorted(TxId, TableName) ->
  gingko_utils:sort_by_jsn_number(read_journal_entries_with_tx_id(TxId, TableName)).

-spec match_journal_entries(journal_entry(), atom()) -> [journal_entry()].
match_journal_entries(MatchJournalEntry, TableName) ->
  mnesia:activity(transaction, fun() -> mnesia:match_object(TableName, MatchJournalEntry, read) end).

-spec read_all_checkpoint_entry_keys() -> [key_struct()].
read_all_checkpoint_entry_keys() ->
  mnesia:activity(transaction,
    fun() ->
      mnesia:foldl(fun(C, Acc) -> [C#checkpoint_entry.key_struct | Acc] end, [], checkpoint_entry)
    end).

-spec read_checkpoint_entry(key_struct(), vectorclock()) -> snapshot().%TODO compiler bug | {error, {"Multiple Snapshots found", key_struct(), [snapshot()]}}.
read_checkpoint_entry(KeyStruct, DependencyVts) ->
  SnapshotList = mnesia:activity(transaction, fun() -> mnesia:read(checkpoint_entry, KeyStruct) end),
  case SnapshotList of
    [] -> gingko_utils:create_new_snapshot(KeyStruct, DependencyVts);
    [C] -> gingko_utils:create_snapshot_from_checkpoint_entry(C, DependencyVts);
    CList -> {error, {"Multiple Snapshots found", KeyStruct, CList}}
  end.

%TODO testing necessary
-spec read_checkpoint_entries([key_struct()], vectorclock()) -> [snapshot()].%TODO compiler bug | {error, {"Multiple Snapshots found", key_struct(), [snapshot()]}}.
read_checkpoint_entries(KeyStructs, DependencyVts) ->
  SnapshotList =
    mnesia:activity(transaction,
      fun() ->
        lists:filtermap(
          fun(K) ->
            case mnesia:read(checkpoint_entry, K) of
              [] -> {true, gingko_utils:create_new_snapshot(K, DependencyVts)};
              [C] -> {true, gingko_utils:create_snapshot_from_checkpoint_entry(C, DependencyVts)};
              CList ->
                {true, {error, {"Multiple snapshots for one key found", K, CList}}}
            end
          end, KeyStructs)
      end),
  AnyErrors =
    lists:any(
      fun(C) ->
        case C of
          {error, _} -> true;
          _ -> false
        end
      end, SnapshotList),
  case AnyErrors of
    true -> {error, {"One or more snapshot keys caused an error", SnapshotList}};
    false -> SnapshotList
  end.

-spec add_or_update_checkpoint_entry(snapshot()) -> ok | 'transaction abort'.
add_or_update_checkpoint_entry(Snapshot) ->
  F = fun() ->
    mnesia:write(#checkpoint_entry{key_struct = Snapshot#snapshot.key_struct, value = Snapshot#snapshot.value}) end,
  mnesia:activity(transaction, F).

-spec add_or_update_checkpoint_entries([snapshot()]) -> ok | 'transaction abort'.
add_or_update_checkpoint_entries(Snapshots) ->
  F =
    fun() ->
      lists:foreach(
        fun(Snapshot) ->
          mnesia:write(#checkpoint_entry{key_struct = Snapshot#snapshot.key_struct, value = Snapshot#snapshot.value})
        end, Snapshots)
    end,
  mnesia:activity(transaction, F).

-spec checkpoint(atom(), pid()) -> ok.
checkpoint(TableName, CacheServerPid) ->
  SortedJournalEntries = read_all_journal_entries_sorted(TableName),
  RelevantJournalList = lists:reverse(SortedJournalEntries),
  Checkpoints = lists:filter(fun(J) ->
    gingko_utils:is_system_operation(J, checkpoint) end, RelevantJournalList),
  {PreviousCheckpointVts, CurrentCheckpointVts} =
    case Checkpoints of
      [] -> {vectorclock:new(), vectorclock:new()}; %Should not happen really
      [CurrentCheckpoint1] ->
        {vectorclock:new(), CurrentCheckpoint1#journal_entry.operation#system_operation.op_args#checkpoint_args.dependency_vts};
      [CurrentCheckpoint2, PreviousCheckpoint | _] ->
        {PreviousCheckpoint#journal_entry.operation#system_operation.op_args#checkpoint_args.dependency_vts, CurrentCheckpoint2#journal_entry.operation#system_operation.op_args#checkpoint_args.dependency_vts}
    end,
  %Get all keys from previous checkpoint and their dependency vts
  CommittedJournalEntries = gingko_materializer:get_committed_journal_entries_for_keys(SortedJournalEntries, all_keys),
  RelevantKeysInJournal =
    sets:to_list(sets:from_list(
      lists:filtermap(
        fun({CommitJ, UpdateJList}) ->
          CommitVts = CommitJ#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_vts,
          case gingko_utils:is_in_vts_range(CommitVts, {PreviousCheckpointVts, CurrentCheckpointVts}) of
            true -> {true, lists:map(fun(UpdateJ) ->
              UpdateJ#journal_entry.operation#object_operation.key_struct end, UpdateJList)};
            false -> false
          end
        end, CommittedJournalEntries))),
  SnapshotsToStore =
    lists:map(
      fun(K) ->
        {ok, Snapshot} = gen_server:call(CacheServerPid, {get, K, CurrentCheckpointVts}),
        Snapshot
      end, RelevantKeysInJournal),
  add_or_update_checkpoint_entries(SnapshotsToStore),
  gen_server:call(CacheServerPid, {checkpoint_cache_cleanup, CurrentCheckpointVts}).

-spec perform_tx_read(key_struct(), txid(), atom(), pid()) -> {ok, snapshot()} | {error, reason()}.
perform_tx_read(KeyStruct, TxId, TableName, CacheServerPid) ->
  CurrentTxJournalEntries = read_journal_entries_with_tx_id_sorted(TxId, TableName),
  Begin = hd(CurrentTxJournalEntries),
  BeginVts = Begin#journal_entry.operation#system_operation.op_args#begin_txn_args.dependency_vts,
  {ok, SnapshotBeforeTx} = gen_server:call(CacheServerPid, {get, KeyStruct, BeginVts}),
  UpdatesToBeAdded =
    lists:filter(
      fun(J) ->
        gingko_utils:is_update_of_keys(J, [KeyStruct])
      end, CurrentTxJournalEntries),
  gingko_materializer:materialize_snapshot_temporarily(SnapshotBeforeTx, UpdatesToBeAdded).
