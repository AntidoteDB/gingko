-include_lib("riak_core/include/riak_core_vnode.hrl").
-define(BUCKET, <<"gingko">>).
-define(USE_SINGLE_SERVER, false).
-define(USE_EXPERIMENTAL_TIMESTAMP, true).
-define(EXPERIMENTAL_TIMESTAMP_USE_MONOTONIC_TIME, false).
-define(SINGLE_SERVER_PARTITION, 0).

-define(GINGKO_APP_NAME, gingko_app).
-define(GINGKO_LOG_VNODE_MASTER, gingko_log_vnode_master).
-define(GINGKO_LOG_HELPER_VNODE_MASTER, gingko_log_helper_vnode_master).
-define(GINGKO_CACHE_VNODE_MASTER, gingko_cache_vnode_master).
-define(INTER_DC_LOG_VNODE_MASTER, inter_dc_log_vnode_master).
-define(GINGKO_LOG, {gingko_log_server, ?GINGKO_LOG_VNODE_MASTER}).
-define(GINGKO_LOG_HELPER, {gingko_log_helper_server, ?GINGKO_LOG_HELPER_VNODE_MASTER}).
-define(GINGKO_CACHE, {gingko_cache_server, ?GINGKO_CACHE_VNODE_MASTER}).
-define(INTER_DC_LOG, {inter_dc_log_server, ?INTER_DC_LOG_VNODE_MASTER}).


-define(JOURNAL_PORT_NAME, journal_port).
-define(REQUEST_PORT_NAME, request_port).
-define(DEFAULT_JOURNAL_PORT, 8086).
-define(DEFAULT_REQUEST_PORT, 8085).

%% Bounded counter manager parameters.
%% Period during which transfer requests from the same DC to the same key are ignored.
-define(GRACE_PERIOD, 1000000). % in Microseconds
%% Time to forget a pending request.
-define(REQUEST_TIMEOUT, 500000). % In Microseconds
%% Frequency at which manager requests remote resources.


-define(TRANSFER_FREQ, 1000). %in Milliseconds
-define(TXN_PING_FREQ, 1000). %in Milliseconds
-define(ZMQ_TIMEOUT, 5000).
-define(COMM_TIMEOUT, 10000).
-define(DEFAULT_WAIT_TIME_SUPER_SHORT, 10). %% in milliseconds
-define(DEFAULT_WAIT_TIME_SHORT, 100). %% in milliseconds
-define(DEFAULT_WAIT_TIME_MEDIUM, 500). %% in milliseconds
-define(DEFAULT_WAIT_TIME_LONG, 1000). %% in milliseconds
-define(DEFAULT_WAIT_TIME_SUPER_LONG, 10000). %% in milliseconds

-type microsecond() :: non_neg_integer().
-type millisecond() :: non_neg_integer().
-type timestamp() :: non_neg_integer().

-type key() :: term().
-type type() :: atom().
-type txn_properties() :: [{update_clock, boolean()} | {certify, use_default | certify | dont_certify}].

-record(tx_id, {
    local_start_time :: clock_time(),
    server_pid :: atom() | pid()
}).
-type txid() :: #tx_id{}.

-type crdt() :: term().
-type type_op() :: {Op :: atom(), OpArgs :: term()} | atom() | term(). %downstream(type_op, crdt())
-type downstream_op() :: term(). %update(downstream_op, crdt())

-type dcid() :: node() | undefined | {term(), term()}.

-type vectorclock() :: vectorclock:vectorclock().
-type snapshot_time() :: 'undefined' | vectorclock:vectorclock().
-type vts_range() :: {MinVts :: vectorclock() | none, MaxVts :: vectorclock() | none}.
-type clock_time() :: non_neg_integer().
-type clock_range() :: {MinClock :: clock_time() | none, MaxClock :: clock_time() | none}.
-type reason() :: term().
-type map_list() :: [{Key :: term(), Value :: term()}].
-type index_node() :: {partition_id(), node()}.
-type preflist() :: riak_core_apl:preflist().
-type partition_id() :: chash:index_as_int().
-type ct_config() :: map_list().

-type txn_num() :: {non_neg_integer(), txid() | none, clock_time()}.
-type tx_op_num() :: non_neg_integer().

-record(cache_usage, {
    used = true :: boolean(),
    first_used = 0 :: clock_time(),
    last_used = 0 :: clock_time(),
    times_used = 1 :: non_neg_integer()
}).
-type cache_usage() :: #cache_usage{}.

-record(cache_entry, {
    key_struct :: key_struct(),
    commit_vts :: vectorclock(),
    valid_vts :: vectorclock(),
    usage = #cache_usage{} :: cache_usage(),
    blob :: term() | crdt()
}).
-type cache_entry() :: #cache_entry{}.

-record(key_struct, {
    key :: key(),
    type :: type()
}).
-type key_struct() :: #key_struct{}.

-type raw_value() :: term().

-record(checkpoint_entry, {
    key_struct :: key_struct(),
    value :: snapshot_value()
}).
-type checkpoint_entry() :: #checkpoint_entry{}.

-record(snapshot, {
    key_struct :: key_struct(),
    commit_vts :: vectorclock(),
    snapshot_vts :: vectorclock(),
    value :: snapshot_value()
}).
-type snapshot() :: #snapshot{}.
-type snapshot_value() :: crdt() | raw_value() | reference | none. %%TODO define reference

-record(begin_txn_args, {dependency_vts :: vectorclock()}).
-type begin_txn_args() :: #begin_txn_args{}.

-record(prepare_txn_args, {}).
-type prepare_txn_args() :: #prepare_txn_args{}.
-record(commit_txn_args, {
    commit_vts :: vectorclock(),
    local_txn_num :: txn_num()
}).
-type commit_txn_args() :: #commit_txn_args{}.
-record(abort_txn_args, {}).
-type abort_txn_args() :: #abort_txn_args{}.
-record(checkpoint_args, {dependency_vts :: vectorclock(), dcid_to_last_txn_num :: dict:dict(dcid(), txn_num())}).
-type checkpoint_args() :: #checkpoint_args{}.

-record(object_op_args, {
    key_struct :: key_struct(),
    tx_op_num :: non_neg_integer(), %Order in a transaction
    op_args = none :: none | downstream_op() %%TODO specify further if possible
}).
-type object_op_args() :: #object_op_args{}.

-type journal_entry_args() :: begin_txn_args() | prepare_txn_args() | commit_txn_args() | abort_txn_args() | checkpoint_args() | object_op_args().

-type jsn() :: non_neg_integer().
-type journal_entry_type() :: begin_txn | prepare_txn | commit_txn | abort_txn | checkpoint | read | update.

-record(journal_entry, {
    jsn :: jsn(),
    dcid :: dcid(),
    rt_timestamp :: clock_time(),
    tx_id :: txid(),
    type :: journal_entry_type(),
    args :: journal_entry_args()
}).
-type journal_entry() :: #journal_entry{}.

-record(update_payload, {
    key_struct :: key_struct(),
    update_op :: downstream_op() | fun((Value :: term()) -> UpdatedValue :: term()), %%TODO define operations on non crdt types
    commit_vts :: vectorclock(),
    snapshot_vts :: vectorclock(),
    tx_id :: txid()
}).
-type update_payload() :: #update_payload{}.

-record(jsn_state, {
    next_jsn :: jsn(),
    dcid :: dcid(),
    rt_timestamp :: clock_time()
}).
-type jsn_state() :: #jsn_state{}.
