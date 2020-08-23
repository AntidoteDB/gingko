-include("gingko.hrl").
-export_type([dc_state/0, response_record/0, partition_vts/0, tx_vts/0, dc_info_entry/0, inter_dc_txn/0, zmq_sender_id/0, zmq_socket_option_value/0, zmq_socket_option/0, zmq_send_recv_flags/0, zmq_data/0, zmq_endpoint/0, zmq_context/0, zmq_socket_type/0, zmq_socket/0]).

-define(DC_CONNECT_RETRIES, 5).
-define(DC_CONNECT_RETRY_SLEEP, 1000).
%% The following are binary codes defining the message
%% types for inter dc communication
-define(OK_MSG, 1).
-define(ERROR_MSG, 2).
-define(HEALTH_CHECK_MSG, 3).
-define(JOURNAL_READ_REQUEST, 4).
-define(BCOUNTER_REQUEST, 5).
-define(DCSF_MSG, 6).
-define(REQUEST_NOT_SUPPORTED_MSG, 7).

%% The number of bytes a partition id is in a message
-define(PARTITION_BYTE_LENGTH, 20).
%% the number of bytes a message id is
-define(REQUEST_ID_BYTE_LENGTH, 2).
-define(REQUEST_ID_BIT_LENGTH, 16).

-define(REQUEST_TYPE_BYTE_LENGTH, 8).

-type request_type() :: ?OK_MSG | ?ERROR_MSG | ?HEALTH_CHECK_MSG | ?JOURNAL_READ_REQUEST | ?BCOUNTER_REQUEST | ?DCSF_MSG.

-type target_dcid() :: dcid() | all.
-type target_partition() :: partition() | all.

-type zmq_socket() :: {pos_integer(), binary()}.%%erlzmq_socket().
-type zmq_socket_type() :: pair | pub | sub | req | rep | dealer | router | xreq | xrep |
pull | push | xpub | xsub.
-type zmq_context() :: binary().
-type zmq_endpoint() :: string() | binary().
-type zmq_data() :: iolist().
-type zmq_send_recv_flag() :: dontwait | sndmore | recvmore | {timeout, timeout()}.
-type zmq_send_recv_flags() :: [zmq_send_recv_flag()].
-type zmq_socket_option() :: affinity | identity | subscribe | unsubscribe | rate | recovery_ivl | sndbuf | rcvbuf | rcvmore | fd | events | linger | reconnect_ivl | backlog |reconnect_ivl_max | maxmsgsize | sndhwm | rcvhwm | multicast_hops | rcvtimeo | sndtimeo | ipv4only.
-type zmq_socket_option_value() :: integer() | iolist() | binary().
%%Empty is ping and all inter_dc_journal_entries must have the same txid

-type response_return_function() :: fun((term(), request_entry()) -> ok).

-type zmq_sender_id() :: binary().

-record(inter_dc_txn, {
    partition :: partition(),
    source_dcid :: dcid(),
    last_sent_txn_tracking_num :: txn_tracking_num(),
    journal_entries :: [journal_entry()]
}).
-type inter_dc_txn() :: #inter_dc_txn{}.

-type socket_address() :: {inet:ip_address() | string(), inet:port_number()}.
-type node_address_list() :: {node(), [socket_address()]}.
-type dc_address_list() :: [node_address_list()].

-record(descriptor, {
    dcid :: dcid(),
    number_of_partitions :: non_neg_integer(),
    txn_dc_address_list :: dc_address_list(),
    request_dc_address_list :: dc_address_list()
}).
-type descriptor() :: #descriptor{}.

-record(dc_info_entry, {
    dcid :: dcid(),
    nodes :: [node()],
    has_started :: boolean(),
    my_descriptor :: descriptor(),
    connected_descriptors :: [descriptor()]
}).
-type dc_info_entry() :: #dc_info_entry{}.

-record(tx_vts, {
    tx_id :: txid(),
    dependency_vts :: vectorclock()
}).
-type tx_vts() :: #tx_vts{}.

-record(partition_vts, {
    partition :: partition(),
    commit_vts :: vectorclock()
}).
-type partition_vts() :: #partition_vts{}.

-record(request_record, {
    request_id :: non_neg_integer(),
    request_type :: request_type(),
    target_dcid :: target_dcid(),
    target_partition :: target_partition(),
    source_dcid :: dcid(),
    source_node :: node(),
    request_args :: term()
}).
-type request_record() :: #request_record{}.

%% This keeps information about an inter-dc request that
%% is waiting for a reply

-record(request_entry, {
    request_record :: request_record(),
    request_timestamp :: timestamp(),
    return_func_or_none :: response_return_function() | none
}).
-type request_entry() :: #request_entry{}.

-record(response_record, {
    request_record :: request_record(),
    response :: term()
}).
-type response_record() :: #response_record{}.

-record(dc_state, {
    dcid :: dcid(),
    last_update :: timestamp(),
    dcsf :: vectorclock()
}).
-type dc_state() :: #dc_state{}.
