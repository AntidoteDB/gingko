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

-module(gingko_log_utils).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

%% API
-export([add_journal_entry/2, add_or_update_checkpoint_entry/1, read_journal_entry/2, read_checkpoint_entry/2, read_all_journal_entries/1, match_journal_entries/2, read_journal_entries_with_tx_id/2, persist_journal_entries/1, clear_journal_entries/1, read_all_journal_entries_sorted/1, read_journal_entries_with_tx_id_sorted/2]).

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
