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

-module(inter_dc_txn).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc_repl.hrl").

-export([from_journal_entries/1,
    is_local/1,
    to_binary/1,
    from_binary/1]).

send_inter_dc_txn_async(Partitions, TxId) ->
    spawn_link(
        fun() ->
            JournalEntries = collect_journal_entries_internal(Partitions, TxId, []),
            inter_dc_txn_sender:broadcast_journal_entries(JournalEntries)
        end).

%%TODO MULTICALL
collect_journal_entries_internal([], _TxId, Acc) -> merge_journal_entries(Acc, []);
collect_journal_entries_internal([Partition | Partitions], TxId, Acc) ->
    JournalEntries = gingko_utils:call_gingko_sync(Partition, ?GINGKO_LOG, {get_journal_entries_from_tx_id, TxId}),
    collect_journal_entries_internal(Partitions, TxId, JournalEntries ++ Acc).

-spec merge_journal_entries([journal_entry()], [journal_entry()]) -> [journal_entry()].
merge_journal_entries([JournalEntry | JournalEntries], []) -> merge_journal_entries(JournalEntries, [JournalEntry]);
merge_journal_entries([], JournalEntryAcc) -> JournalEntryAcc;
merge_journal_entries([JournalEntry = #journal_entry{operation = #object_operation{}} | OtherJournalEntries], JournalEntryAcc) ->
    merge_journal_entries(OtherJournalEntries, [JournalEntry | JournalEntryAcc]);
merge_journal_entries([JournalEntry = #journal_entry{operation = Operation} | OtherJournalEntries], JournalEntryAcc) ->
    MatchingJournalEntries = lists:any(fun(#journal_entry{operation = MatchOperation}) ->
        Operation == MatchOperation end, JournalEntryAcc),
    case MatchingJournalEntries of
        false -> merge_journal_entries(OtherJournalEntries, [JournalEntry | JournalEntryAcc]);
        true -> merge_journal_entries(OtherJournalEntries, [JournalEntryAcc])
    end.

%%TODO check correctness
-spec from_journal_entries([inter_dc_journal_entry()]) -> inter_dc_txn().
from_journal_entries(InterDcJournalEntries) ->
    #inter_dc_txn{source_dcid = gingko_utils:get_my_dcid(), inter_dc_journal_entries = InterDcJournalEntries}.

-spec is_local(inter_dc_txn()) -> boolean().
is_local(#inter_dc_txn{source_dcid = DCID}) -> DCID == gingko_utils:get_my_dcid().

-spec to_binary(inter_dc_txn()) -> binary().
to_binary(InterDcTxn) ->
    term_to_binary(InterDcTxn).

-spec from_binary(binary()) -> inter_dc_txn().
from_binary(InterDcTxnBinary) ->
    binary_to_term(InterDcTxnBinary).
