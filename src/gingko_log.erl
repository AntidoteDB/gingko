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
-export([add_journal_entry/1, add_or_update_snapshot/1, read_journal_entry/1, read_snapshot/1, read_all_journal_entries/0, match_journal_entries/1, read_journal_entries_with_tx_id/1, perform_tx_read/2]).

-spec add_journal_entry(journal_entry()) -> ok | {error, {already_exists, [journal_entry()]}} | 'transaction abort'.
add_journal_entry(JournalEntry) ->
  F = fun() ->
    case mnesia:read(journal_entry, JournalEntry#journal_entry.jsn) of
      [] -> mnesia:write(JournalEntry);
      ExistingJournalEntries -> {error, {already_exists, ExistingJournalEntries}}
    end
      end,
  mnesia:activity(transaction, F).

add_or_update_snapshot(Snapshot) ->
  mnesia:activity(transaction, fun() -> mnesia:write(Snapshot) end).

read_journal_entry(Jsn) ->
  List = mnesia:activity(transaction, fun() -> mnesia:read(journal_entry, Jsn) end),
  case List of
    [] -> {error, {"No Journal Entry found", Jsn}};
    [J] -> J;
    Js -> {error, {"Multiple Journal Entries found", Jsn, Js}}
  end.

read_all_journal_entries() ->
  mnesia:activity(transaction, fun() -> mnesia:foldl(fun(X, Acc) -> [X | Acc] end, [], journal_entry) end).

read_journal_entries_with_tx_id(TxId) ->
  MatchJournalEntry = #journal_entry{jsn = '_', rt_timestamp = '_', tx_id = TxId, operation = '_'},
  match_journal_entries(MatchJournalEntry).

match_journal_entries(MatchJournalEntry) ->
  mnesia:activity(transaction, fun() -> mnesia:match_object(MatchJournalEntry) end).

read_snapshot(KeyStruct) ->
  List = mnesia:activity(transaction, fun() -> mnesia:read(snapshot, KeyStruct) end),
  case List of
    [] -> materializer:create_snapshot(KeyStruct);
    [S] -> S;
    Ss -> {error, {"Multiple Snapshots found", KeyStruct, Ss}}
  end.

checkpoint(DependencyVts) ->
  %TODO
  ok.

  -spec perform_tx_read(journal_entry(), pid()) -> {ok, snapshot_value()} | {error, reason()}.
perform_tx_read(JournalEntry, CacheServerPid) ->
  CurrentJsn = gingko_utils:get_jsn_number(JournalEntry),
  KeyStruct = JournalEntry#journal_entry.operation#object_operation.key_struct,
  CurrentTxJournalEntries = gingko_utils:sort_by_jsn_number(gingko_log:read_journal_entries_with_tx_id(JournalEntry#journal_entry.tx_id)),
  %TODO assure that begin is first (must be)
  Begin = hd(CurrentTxJournalEntries),
  SnapshotByBeforeTx = gen_server:call(CacheServerPid, {get, KeyStruct, Begin#journal_entry.operation#system_operation.op_args#begin_txn_args.dependency_vts}),
  UpdatesToBeAdded = lists:filter(fun(J) ->
    gingko_utils:get_jsn_number(J) < CurrentJsn andalso gingko_utils:is_update_and_contains_key(J, KeyStruct) end, CurrentTxJournalEntries),
  materializer:materialize_snapshot_temporarily(SnapshotByBeforeTx, UpdatesToBeAdded).
