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

-export([from_journal_entries/3,
    is_local/1,
    to_binary/1,
    from_binary/1,
    partition_to_binary/1]).

-spec from_journal_entries(partition(), txn_tracking_num(), [journal_entry()]) -> inter_dc_txn().
from_journal_entries(Partition, LastSentTxnTrackingNum, JournalEntryList) ->
    #inter_dc_txn{partition = Partition, source_dcid = gingko_dc_utils:get_my_dcid(), last_sent_txn_tracking_num = LastSentTxnTrackingNum, journal_entries = JournalEntryList}.

-spec is_local(inter_dc_txn()) -> boolean().
is_local(#inter_dc_txn{source_dcid = DCID}) -> DCID == gingko_dc_utils:get_my_dcid().

to_tuple(#inter_dc_txn{partition = Partition, source_dcid = DCID, last_sent_txn_tracking_num = LastSentTxnTrackingNum, journal_entries = JournalEntryList}) ->
    {Partition, DCID, LastSentTxnTrackingNum, JournalEntryList}.

from_tuple({Partition, DCID, LastSentTxnTrackingNum, InterDcJournalEntryList}) ->
    #inter_dc_txn{partition = Partition, source_dcid = DCID, last_sent_txn_tracking_num = LastSentTxnTrackingNum, journal_entries = InterDcJournalEntryList}.

-spec to_binary(inter_dc_txn()) -> binary().
to_binary(InterDcTxn = #inter_dc_txn{partition = Partition}) ->
    PartitionPrefix = partition_to_binary(Partition),
    InterDcTxnBinary = term_to_binary(to_tuple(InterDcTxn)),
    <<PartitionPrefix/binary, InterDcTxnBinary/binary>>.

-spec from_binary(binary()) -> inter_dc_txn().
from_binary(InterDcTxnBinaryWithPartitionPrefix) ->
    ByteSize = byte_size(InterDcTxnBinaryWithPartitionPrefix),
    InterDcTxnBinary = binary_part(InterDcTxnBinaryWithPartitionPrefix, {?PARTITION_BYTE_LENGTH, ByteSize - ?PARTITION_BYTE_LENGTH}),
    from_tuple(binary_to_term(InterDcTxnBinary)).

-spec pad(non_neg_integer(), binary()) -> binary().
pad(Width, Binary) ->
    case Width - byte_size(Binary) of
        N when N =< 0 -> Binary;
        N -> <<0:(N * 8), Binary/binary>>
    end.

-spec partition_to_binary(partition()) -> binary().
partition_to_binary(Partition) ->
    pad(?PARTITION_BYTE_LENGTH, binary:encode_unsigned(Partition)).

%%TODO Find out if these from AntidoteDB are still relevant
%%%% Takes a binary and makes it size width
%%%% if it is too small than it adds 0s
%%%% otherwise it trims bits from the left size
%%-spec pad_or_trim(non_neg_integer(), binary()) -> binary().
%%pad_or_trim(Width, Binary) ->
%%    case Width - byte_size(Binary) of
%%        N when N == 0 -> Binary;
%%        N when N < 0 ->
%%            Pos = trunc(abs(N)),
%%            <<_:Pos/binary, Rest:Width/binary>> = Binary,
%%            Rest;
%%        N -> <<0:(N * 8), Binary/binary>>
%%    end.
%%
%%-spec partition_from_binary(binary()) -> partition().
%%partition_from_binary(PartitionBinary) ->
%%    binary:decode_unsigned(PartitionBinary).
%%
%%-spec partition_and_rest_binary(binary()) -> {binary(), binary()}.
%%partition_and_rest_binary(Binary) ->
%%    <<Partition:?PARTITION_BYTE_LENGTH/big-unsigned-integer-unit:8, RestBinary/binary>> = Binary,
%%    {Partition, RestBinary}.
