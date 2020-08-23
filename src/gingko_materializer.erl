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

%%%===================================================================
%%% Public API
%%%===================================================================

-spec materialize_snapshot(snapshot(), [journal_entry()], vectorclock()) -> snapshot().
materialize_snapshot(Snapshot = #snapshot{snapshot_vts = Vts}, _, Vts) -> Snapshot;
materialize_snapshot(Snapshot = #snapshot{key_struct = KeyStruct, snapshot_vts = SnapshotVts}, JournalEntryList, DependencyVts) when length(JournalEntryList) > 0 ->
    %%TODO maybe add additional checks
    %%TODO if list is empty and Dependency Vts is higher then it should crash
    CommitVtsDownstreamOpListTupleList = get_commit_vts_downstream_op_list_tuple_list(JournalEntryList, KeyStruct),
    %%TODO optimization is to create these three lists at the same time
    %%Get commits that are newer than / concurrent to the snapshot vts and older than / equal to the dependency vts
    FilteredCommitVtsDownstreamOpListTupleList =
        lists:filter(
            fun({CommitVts, _}) ->
                (not vectorclock:le(CommitVts, SnapshotVts)) %%Assures that strictly older commits are ignored and concurrent commits are kept
                    andalso vectorclock:le(CommitVts, DependencyVts)
            end, CommitVtsDownstreamOpListTupleList),
    %%Sorts commits so they are applied in the correct order
    SortedFilteredCommitVtsDownstreamOpListTupleList =
        gingko_utils:sort_by_vts(
            fun({CommitVts, _}) ->
                CommitVts
            end, FilteredCommitVtsDownstreamOpListTupleList),
    NewSnapshot = materialize_update_payload(Snapshot, SortedFilteredCommitVtsDownstreamOpListTupleList),

    %%Calculate maximum valid snapshot vts so that a key that rarely updated is valid longer
    NewSnapshotVts = optimize_snapshot_vts(CommitVtsDownstreamOpListTupleList, DependencyVts),

    NewSnapshot#snapshot{snapshot_vts = NewSnapshotVts}.

-spec materialize_tx_snapshot(snapshot(), [downstream_op()]) -> snapshot().
materialize_tx_snapshot(Snapshot, []) ->
    Snapshot;
materialize_tx_snapshot(Snapshot = #snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts}, DownstreamOpList) ->
    update_snapshot(Snapshot, DownstreamOpList, CommitVts, SnapshotVts).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_commit_vts_downstream_op_list_tuple_list([journal_entry()], key_struct())
        -> [{vectorclock(), [downstream_op()]}].
get_commit_vts_downstream_op_list_tuple_list(JournalEntryList, KeyStruct) ->
    %%Keep only journal entries that are updates and commits
    FilteredJournalEntryList =
        lists:filter(
            fun(JournalEntry) ->
                gingko_utils:is_update_of_key_or_commit(JournalEntry, KeyStruct)
            end, JournalEntryList),
    %%Create a list that groups journal entries by their transaction
    TxJournalEntryListList = general_utils:group_by_only_values(fun(#journal_entry{tx_id = TxId}) ->
        TxId end, FilteredJournalEntryList),
    %Keep only transactions in the list that were committed and have updates
    FilteredTxJournalEntryListList =
        lists:filter(
            fun(TxJournalEntryList) ->
                length(TxJournalEntryList) > 1 %%Only updates and commits are remaining so checking the length is good enough (assumption that transactions are correct)
                    andalso gingko_utils:contains_journal_entry_type(TxJournalEntryList, commit_txn)
            end, TxJournalEntryListList),
    %%Tranform the list of transactions to the final return type (strip all unnecessary information and create the tuples)
    lists:foldl(
        fun(TxJournalEntryList, CurrentCommitVtsDownstreamOpListTupleList) ->
            %%Sort journal entries so that updates are in the right order and commit is at end
            SortedJournalEntryList = gingko_utils:sort_journal_entries_of_same_tx(TxJournalEntryList),
            CommitVtsDownstreamOpListTuple =
                lists:foldr( %%foldr to keep sorting of journal entries
                    fun(JournalEntry, {CurrentCommitVts, CurrentDownstreamOpList}) ->
                        case JournalEntry of
                            #journal_entry{args = #update_args{downstream_op = DownstreamOp}} ->
                                {CurrentCommitVts, [DownstreamOp | CurrentDownstreamOpList]};
                            #journal_entry{args = #commit_txn_args{commit_vts = CommitVts}} ->
                                {CommitVts, CurrentDownstreamOpList}
                        end
                    end, {ok, []}, SortedJournalEntryList),
            [CommitVtsDownstreamOpListTuple | CurrentCommitVtsDownstreamOpListTupleList]
        end, [], FilteredTxJournalEntryListList).

-spec optimize_snapshot_vts([{vectorclock(), [downstream_op()]}], vectorclock()) -> vectorclock().
optimize_snapshot_vts(CommitVtsDownstreamOpListTupleList, DependencyVts) ->
    %%TODO this has optimization potential and the snapshot time can also be calculated more precisely in case of concurrent commit vts
    CommitVtsConcurrentDependencyVtsList =
        lists:filtermap(
            fun({CommitVts, _}) ->
                case vectorclock:conc(CommitVts, DependencyVts) of
                    true -> {true, CommitVts};
                    false -> false
                end
            end, CommitVtsDownstreamOpListTupleList),
    CommitVtsAfterDependencyVtsList =
        lists:filtermap(
            fun({CommitVts, _}) ->
                case vectorclock:gt(CommitVts, DependencyVts) of
                    true -> {true, CommitVts};
                    false -> false
                end
            end, CommitVtsDownstreamOpListTupleList),
    case CommitVtsAfterDependencyVtsList of
        [] -> DependencyVts;
        _ ->
            SmallestCommitVtsAfterDependencyVts =
                gingko_utils:get_smallest_vts(CommitVtsAfterDependencyVtsList),
            IsHigherThanAnyConcurrentCommitVts =
                lists:any(fun(CommitVts) ->
                    vectorclock:gt(SmallestCommitVtsAfterDependencyVts, CommitVts) end, CommitVtsConcurrentDependencyVtsList),
            case IsHigherThanAnyConcurrentCommitVts of
                true -> DependencyVts;
                false ->
                    %%TODO maybe implement more elegant solution
                    vectorclock:map(
                        fun(_, DcClockTime) ->
                            DcClockTime - 1
                        end, SmallestCommitVtsAfterDependencyVts)
            end
    end.

-spec materialize_update_payload(snapshot(), [{vectorclock(), [downstream_op()]}]) -> snapshot().
materialize_update_payload(Snapshot, []) ->
    Snapshot;
materialize_update_payload(Snapshot, [{CommitVts, DownstreamOpList} | Rest]) ->
    NewSnapshot = update_snapshot(Snapshot, DownstreamOpList, CommitVts, CommitVts),
    materialize_update_payload(NewSnapshot, Rest).

-spec update_snapshot(snapshot(), [downstream_op()], vectorclock(), vectorclock()) -> snapshot().
update_snapshot(Snapshot, [], CommitVts, SnapshotVts) ->
    Snapshot#snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts};
update_snapshot(Snapshot = #snapshot{value = SnapshotValue, key_struct = #key_struct{type = Type}}, [DownstreamOp | Rest], CommitVts, SnapshotVts) ->
    {ok, NewSnapshotValue} = Type:update(DownstreamOp, SnapshotValue), %%TODO this will crash if downstream is bad
    update_snapshot(Snapshot#snapshot{value = NewSnapshotValue}, Rest, CommitVts, SnapshotVts).
