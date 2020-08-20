%% -------------------------------------------------------------------
%%
%% Copyright 2020, Kevin Bartik <k_bartik12@cs.uni-kl.de>
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

-module(gingko_materializer).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([materialize_snapshot/3,
    materialize_tx_snapshot/2]).

-spec get_commit_vts_downstream_op_list_tuple_list([journal_entry()], key_struct()) -> [{vectorclock(), [downstream_op()]}].
get_commit_vts_downstream_op_list_tuple_list(JournalEntryList, KeyStruct) ->
    FilteredJournalEntryList =
        lists:filter(
            fun(JournalEntry) ->
                gingko_utils:is_update_of_key_or_commit(JournalEntry, KeyStruct)
            end, JournalEntryList),
    TxJournalEntryListList = general_utils:group_by_only_values(fun(#journal_entry{tx_id = TxId}) ->
        TxId end, FilteredJournalEntryList),
    FilteredTxJournalEntryListList =
        lists:filter(
            fun(TxJournalEntryList) ->
                length(TxJournalEntryList) > 1
                    andalso gingko_utils:contains_journal_entry_type(TxJournalEntryList, commit_txn)
            end, TxJournalEntryListList),
    lists:foldl(
        fun(TxJournalEntryList, CurrentCommitVtsDownstreamOpListTupleList) ->
            SortedJournalEntryList = gingko_utils:sort_journal_entries_of_same_tx(TxJournalEntryList),
            CommitVtsDownstreamOpListTuple =
                lists:foldr(
                    fun(JournalEntry, {CurrentCommitVts, CurrentDownstreamOpList}) ->
                        case JournalEntry of
                            #journal_entry{args = #update_args{downstream_op = DownstreamOp}} ->
                                {CurrentCommitVts, [DownstreamOp | CurrentDownstreamOpList]};
                            #journal_entry{args = #commit_txn_args{commit_vts = CommitVts}} ->
                                {CommitVts, CurrentDownstreamOpList}
                        end
                    end, {ok, []}, SortedJournalEntryList),
            [CommitVtsDownstreamOpListTuple | CurrentCommitVtsDownstreamOpListTupleList]
        %Keep only transactions that were committed and have updates
        end, [], FilteredTxJournalEntryListList).

-spec materialize_snapshot(snapshot(), [journal_entry()], vectorclock()) -> snapshot().
materialize_snapshot(Snapshot = #snapshot{key_struct = KeyStruct, snapshot_vts = SnapshotVts}, JournalEntryList, DependencyVts) ->
    %%TODO Currently order of snapshots maybe changed
    %%TODO Snapshots must have unique keys
    %%TODO List not empty
    CommitVtsDownstreamOpListTupleList = get_commit_vts_downstream_op_list_tuple_list(JournalEntryList, KeyStruct),
    BeforeEqualDependencyVtsCommitVtsDownstreamOpListTupleList =
        lists:filter(
            fun({CommitVts, _}) ->
                vectorclock:gt(CommitVts, SnapshotVts)
                    andalso vectorclock:le(CommitVts, DependencyVts)
            end, CommitVtsDownstreamOpListTupleList),
    AfterDependencyVtsCommitVtsDownstreamOpListTupleList =
        lists:filter(
            fun({CommitVts, _}) ->
                vectorclock:gt(CommitVts, DependencyVts)
            end, CommitVtsDownstreamOpListTupleList),
    SortedBeforeEqualDependencyVtsCommitVtsDownstreamOpListTupleList =
        gingko_utils:sort_by_vts(
            fun({CommitVts, _}) ->
                CommitVts
            end, BeforeEqualDependencyVtsCommitVtsDownstreamOpListTupleList),
    NewSnapshotVts =
        case AfterDependencyVtsCommitVtsDownstreamOpListTupleList of
            [] -> DependencyVts;
            _ ->
                {SmallestCommitVtsAfterDependencyVts, _} =
                    gingko_utils:get_smallest_by_vts(
                        fun({CommitVts, _}) ->
                            CommitVts
                        end, AfterDependencyVtsCommitVtsDownstreamOpListTupleList),
                vectorclock:map(
                    fun(_, DcClockTime) ->
                        DcClockTime - 1
                    end, SmallestCommitVtsAfterDependencyVts)
        end,%%TODO maybe implement more elegant solution
    %%Calculate maximum valid snapshot vts so that a key that rarely updated is valid longer
    NewSnapshot = materialize_update_payload(Snapshot, SortedBeforeEqualDependencyVtsCommitVtsDownstreamOpListTupleList),
    NewSnapshot#snapshot{snapshot_vts = NewSnapshotVts}.

-spec update_snapshot(snapshot(), [downstream_op()], vectorclock(), vectorclock()) -> snapshot().
update_snapshot(Snapshot = #snapshot{value = SnapshotValue, key_struct = #key_struct{type = Type}}, DownstreamOpList, CommitVts, SnapshotVts) ->
    NewSnapshotValue =
        lists:foldl(
            fun(DownstreamOp, CurrentSnapshotValue) ->
                {ok, NewCurrentSnapshotValue} = Type:update(DownstreamOp, CurrentSnapshotValue),
                NewCurrentSnapshotValue
            end, SnapshotValue, DownstreamOpList), %%TODO this will crash if downstream is bad
    Snapshot#snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts, value = NewSnapshotValue}.

-spec materialize_update_payload(snapshot(), [{vectorclock(), [downstream_op()]}]) -> snapshot().
materialize_update_payload(Snapshot, []) ->
    Snapshot;
materialize_update_payload(Snapshot, [{CommitVts, DownstreamOpList} | Rest]) ->
    NewSnapshot = update_snapshot(Snapshot, DownstreamOpList, CommitVts, CommitVts),
    materialize_update_payload(NewSnapshot, Rest).

-spec materialize_tx_snapshot(snapshot(), [downstream_op()]) -> snapshot().
materialize_tx_snapshot(Snapshot, []) ->
    Snapshot;
materialize_tx_snapshot(Snapshot = #snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts}, DownstreamOpList) ->
    update_snapshot(Snapshot, DownstreamOpList, CommitVts, SnapshotVts).
