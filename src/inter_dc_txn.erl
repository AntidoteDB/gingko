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
-include("inter_dc.hrl").

-export([from_journal_entries/2,
    is_local/1,
    to_binary/1,
    from_binary/1]).

%%TODO check correctness
-spec from_journal_entries(partition(), [journal_entry()]) -> inter_dc_txn().
from_journal_entries(Partition, JournalEntryList) ->
    #inter_dc_txn{partition = Partition, source_dcid = gingko_dc_utils:get_my_dcid(), journal_entries = JournalEntryList}.

-spec is_local(inter_dc_txn()) -> boolean().
is_local(#inter_dc_txn{source_dcid = DCID}) -> DCID == gingko_dc_utils:get_my_dcid().

to_tuple(#inter_dc_txn{partition = Partition, source_dcid = DCID, journal_entries = JournalEntryList}) ->
    {Partition, DCID, JournalEntryList}.

from_tuple({Partition, DCID, InterDcJournalEntryList}) ->
    #inter_dc_txn{partition = Partition, source_dcid = DCID, journal_entries = InterDcJournalEntryList}.

-spec to_binary(inter_dc_txn()) -> binary().
to_binary(InterDcTxn = #inter_dc_txn{partition = Partition}) ->
    PartitionPrefix = inter_dc_utils:partition_to_binary(Partition),
    InterDcTxnBinary = term_to_binary(to_tuple(InterDcTxn)),
    <<PartitionPrefix/binary, InterDcTxnBinary/binary>>.

-spec from_binary(binary()) -> inter_dc_txn().
from_binary(InterDcTxnBinaryWithPartitionPrefix) ->
    ByteSize = byte_size(InterDcTxnBinaryWithPartitionPrefix),
    InterDcTxnBinary = binary_part(InterDcTxnBinaryWithPartitionPrefix, {?PARTITION_BYTE_LENGTH, ByteSize - ?PARTITION_BYTE_LENGTH}),
    from_tuple(binary_to_term(InterDcTxnBinary)).
