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

-module(gingko_journal_utils).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-export([create_local_journal_entry/4,
    create_read_operation/2,
    create_update_operation/3,
    create_begin_operation/1,
    create_prepare_operation/0,
    create_commit_operation/2,
    create_abort_operation/0,
    create_checkpoint_operation/2]).

-spec create_local_journal_entry(jsn_state(), txid(), journal_entry_type(), journal_entry_args()) -> journal_entry().
create_local_journal_entry(#jsn_state{next_jsn = Jsn, rt_timestamp = RtTimestamp}, TxId, Type, Args) ->
    DcId = gingko_utils:get_my_dcid(),
    #journal_entry{
        jsn = Jsn,
        dcid = DcId,
        rt_timestamp = RtTimestamp,
        tx_id = TxId,
        type = Type,
        args = Args
    }.

-spec create_read_operation(key_struct(), non_neg_integer()) -> {journal_entry_type(), journal_entry_args()}.
create_read_operation(KeyStruct, TxOpNumber) ->
    {read, #object_op_args{key_struct = KeyStruct, tx_op_num = TxOpNumber}}.

-spec create_update_operation(key_struct(), non_neg_integer(), downstream_op()) -> {journal_entry_type(), journal_entry_args()}.
create_update_operation(KeyStruct, TxOpNumber, DownstreamOp) ->
    {update, #object_op_args{key_struct = KeyStruct, tx_op_num = TxOpNumber, op_args = DownstreamOp}}.

-spec create_begin_operation(vectorclock()) -> {journal_entry_type(), journal_entry_args()}.
create_begin_operation(DependencyVts) ->
    {begin_txn, #begin_txn_args{dependency_vts = DependencyVts}}.

-spec create_prepare_operation() -> {journal_entry_type(), journal_entry_args()}.
create_prepare_operation() ->
    {prepare_txn, #prepare_txn_args{}}.

-spec create_commit_operation(vectorclock(), txn_num()) -> {journal_entry_type(), journal_entry_args()}.
create_commit_operation(CommitVts, LocalTxnNum) ->
    {commit_txn, #commit_txn_args{commit_vts = CommitVts, local_txn_num = LocalTxnNum}}.

-spec create_abort_operation() -> {journal_entry_type(), journal_entry_args()}.
create_abort_operation() ->
    {abort_txn, #abort_txn_args{}}.

-spec create_checkpoint_operation(vectorclock(), dict:dict(dcid(), txn_num())) -> {journal_entry_type(), journal_entry_args()}.
create_checkpoint_operation(DependencyVts, DcIdToLastTxNum) ->
    {checkpoint, #checkpoint_args{dependency_vts = DependencyVts, dcid_to_last_txn_num = DcIdToLastTxNum}}.
