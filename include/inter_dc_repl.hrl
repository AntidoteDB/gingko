-include("gingko.hrl").
-include("gingko_message_types.hrl").
-include_lib("erlzmq/include/erlzmq.hrl").

-export_type([descriptor/0, inter_dc_journal_entry/0, recvr_state/0, request_entry/0]).

-record(recvr_state,
{lastRecvd :: orddict:orddict(), %TODO: this may not be required
    lastCommitted :: orddict:orddict(),
    %%Track timestamps from other DC which have been committed by this DC
    recQ :: orddict:orddict(), %% Holds recieving updates from each DC separately in causal order.
    statestore,
    partition}).
-type recvr_state() :: #recvr_state{}.

-type socket_address() :: {inet:ip_address(), inet:port_number()}.
-type node_address_list() :: [socket_address()].
-type dc_address_list() :: [node_address_list()].
-type zmq_socket() :: term().%%erlzmq_socket().
-type dcid_and_partition() :: {dcid(), partition_id()}.
-type inter_dc_journal_entry() :: {partition_id() | all, journal_entry()}.

-record(inter_dc_txn, {
    source_dcid :: dcid(),
    inter_dc_journal_entries :: [inter_dc_journal_entry()]
}).
-type inter_dc_txn() :: #inter_dc_txn{}.



-record(descriptor, {
    dcid :: dcid(),
    number_of_partitions :: non_neg_integer(),
    journal_dc_address_list :: dc_address_list(),
    request_dc_address_list :: dc_address_list()
}).
-type descriptor() :: #descriptor{}.

-record(dc_info_entry, {
    dcid :: dcid(),
    has_started :: boolean(),
    connected_descriptors :: [descriptor()]
}).
-type dc_info_entry() :: dc_info_entry().

%% This keeps information about an inter-dc request that
%% is waiting for a reply
-record(request_entry, {
    request_record :: request_record(),
    return_func :: fun((binary(), request_entry()) -> ok)
}).
-type request_entry() :: #request_entry{}.


-record(request_record, {
    request_id :: non_neg_integer(),
    request_type :: inter_dc_message_type(),
    target_dcid :: dcid() | all,
    target_partition :: partition_id() | all | none,
    source_dcid :: dcid(),
    source_node :: node(),
    request :: term()
}).
-type request_record() :: #request_record{}.

-record(response_record, {
    request_record :: request_record(),
    response :: term()
}).
-type response_record() :: #response_record{}.

-type zmq_sender_id() :: binary().

%% This keeps information about an inter-dc request
%% on the site that is performing the query
-record(inter_dc_request_state, {
    request_record :: request_record(),
    zmq_sender_id :: zmq_sender_id(),
    local_pid :: pid()
}).
-type inter_dc_request_state() :: #inter_dc_request_state{}.
