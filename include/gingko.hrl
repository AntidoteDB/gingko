-include_lib("riak_core/include/riak_core_vnode.hrl").
-export_type([cache_entry/0, ct_config/0, reason/0, invalid_txn_tracking_num/0, txn_tracking_num/0, jsn_state/0, journal_entry/0, operation_type/0, checkpoint_entry/0, type_op/0, table_name/0, timestamp/0, millisecond/0, microsecond/0, percent/0, eviction_strategy/0, journal_trimming_mode/0]).

-define(BUCKET, <<"gingko">>).
-define(DATA_DIR_OPTION_NAME, data_dir).
-define(DATA_DIR_OPTION_DEFAULT, "gingko_data").
-define(SINGLE_SERVER_OPTION_NAME, use_single_server).
-define(SINGLE_SERVER_OPTION_DEFAULT, true).
-define(GINGKO_TIMESTAMP_OPTION_NAME, use_gingko_timestamp).
-define(GINGKO_TIMESTAMP_OPTION_DEFAULT, true).
-define(JOURNAL_TRIMMING_MODE_OPTION_NAME, journal_trimming_mode).
-define(JOURNAL_TRIMMING_MODE_OPTION_DEFAULT, keep_all_checkpoints).
-type journal_trimming_mode() :: keep_all_checkpoints | keep_two_checkpoints | keep_one_checkpoint.
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

-define(TXN_PORT_OPTION_NAME, txn_port).
-define(REQUEST_PORT_OPTION_NAME, request_port).
-define(TXN_PORT_OPTION_DEFAULT, 8086).
-define(REQUEST_PORT_OPTION_DEFAULT, 8085).
-define(MAX_TX_RUN_TIME_OPTION_NAME, max_tx_run_time_millis).
-define(CHECKPOINT_INTERVAL_OPTION_NAME, checkpoint_interval_millis).
-define(MAX_TX_RUN_TIME_OPTION_DEFAULT, 30 * ?DEFAULT_WAIT_TIME_SUPER_LONG). %%currently 5 minutes
-define(CHECKPOINT_INTERVAL_OPTION_DEFAULT, 60 * ?DEFAULT_WAIT_TIME_SUPER_LONG). %%currently 10 minutes
-define(MISSING_TXN_CHECK_INTERVAL_OPTION_NAME, missing_txn_check_interval_millis).
-define(MISSING_TXN_CHECK_INTERVAL_OPTION_DEFAULT, 3000).

-define(DC_STATE_INTERVAL_OPTION_NAME, dc_state_interval_millis).
-define(DC_STATE_INTERVAL_OPTION_DEFAULT, 1000).

-define(CACHE_MAX_OCCUPANCY_OPTION_NAME, max_occupancy).
-define(CACHE_MAX_OCCUPANCY_OPTION_DEFAULT, 100).
-define(CACHE_RESET_USED_INTERVAL_OPTION_NAME, reset_used_interval_millis).
-define(CACHE_RESET_USED_INTERVAL_OPTION_DEFAULT, 1000).
-define(CACHE_EVICTION_INTERVAL_OPTION_NAME, eviction_interval_millis).
-define(CACHE_EVICTION_INTERVAL_OPTION_DEFAULT, 2000).
-define(CACHE_EVICTION_THRESHOLD_OPTION_NAME, eviction_threshold_in_percent).
-define(CACHE_EVICTION_THRESHOLD_OPTION_DEFAULT, 90).
-define(CACHE_TARGET_THRESHOLD_OPTION_NAME, target_threshold_in_percent).
-define(CACHE_TARGET_THRESHOLD_OPTION_DEFAULT, 80).
-define(CACHE_EVICTION_STRATEGY_OPTION_NAME, eviction_strategy).
-define(CACHE_EVICTION_STRATEGY_OPTION_DEFAULT, interval).
-type eviction_strategy() :: interval | fifo | lru | lfu.

%% Bounded counter manager parameters.
%% Period during which transfer requests from the same DC to the same key are ignored.
-define(GRACE_PERIOD, 1000000). % in Microseconds
%% Time to forget a pending request.
-define(REQUEST_TIMEOUT, 500000). % In Microseconds
%% Frequency at which manager requests remote resources.

-define(BCOUNTER_TRANSFER_INTERVAL_OPTION_NAME, bcounter_transfer_interval_millis).
-define(BCOUNTER_TRANSFER_INTERVAL_OPTION_DEFAULT, 1000). %in Milliseconds

-define(INTER_DC_TXN_PING_INTERVAL_OPTION_NAME, txn_ping_interval_millis).
-define(INTER_DC_TXN_PING_INTERVAL_OPTION_DEFAULT, 1000). %in Milliseconds

-define(ZMQ_TIMEOUT, 5000).
-define(COMM_TIMEOUT, 10000).
-define(DEFAULT_WAIT_TIME_SUPER_SHORT, 10). %% in milliseconds
-define(DEFAULT_WAIT_TIME_SHORT, 100). %% in milliseconds
-define(DEFAULT_WAIT_TIME_MEDIUM, 500). %% in milliseconds
-define(DEFAULT_WAIT_TIME_LONG, 1000). %% in milliseconds
-define(DEFAULT_WAIT_TIME_SUPER_LONG, 10000). %% in milliseconds

-type percent() :: 0..100.
-type microsecond() :: non_neg_integer().
-type millisecond() :: non_neg_integer().
-type timestamp() :: non_neg_integer().

-type table_name() :: atom().

-type key() :: term().
-type type() :: antidote_crdt:typ().

-record(tx_id, {
    local_start_time :: clock_time(),
    server_pid :: atom() | pid()
}).
-type txid() :: #tx_id{}.

-type crdt() :: term().
-type type_op() :: {Op :: atom(), OpArgs :: term()} | {Op :: atom(), OpArgs :: term(), MoreOpArgs :: term()} | atom() | term(). %downstream(type_op(), crdt())
-type downstream_op() :: term(). %update(downstream_op(), crdt())

-type dcid() :: node() | undefined | {term(), term()}.

-type vectorclock() :: vectorclock:vectorclock().
-type clock_time() :: non_neg_integer().
-type reason() :: term().
-type key_value_list() :: [{Key :: term(), Value :: term()}].
-type ct_config() :: key_value_list().
-type txn_num() :: non_neg_integer().
-type txn_tracking_num() :: {txn_num(), txid() | none, clock_time()}.
-type invalid_txn_tracking_num() :: {txn_tracking_num(), vectorclock()}.
-type tx_op_num() :: non_neg_integer().

-record(cache_usage, {
    used = true :: boolean(),
    first_used = 0 :: clock_time(),
    last_used = 0 :: clock_time(),
    times_used = 0 :: non_neg_integer()
}).
-type cache_usage() :: #cache_usage{}.

-record(cache_entry, {
    snapshot :: snapshot(),
    usage = #cache_usage{} :: cache_usage()
}).
-type cache_entry() :: #cache_entry{}.

-record(key_struct, {
    key :: key(),
    type :: type()
}).
-type key_struct() :: #key_struct{}.

-record(checkpoint_entry, {
    key_struct :: key_struct(),
    value :: crdt() %%TODO later this should probably be a binary
}).
-type checkpoint_entry() :: #checkpoint_entry{}.

-record(snapshot, {
    key_struct :: key_struct(),
    commit_vts :: vectorclock(),
    snapshot_vts :: vectorclock(),
    value :: crdt()
}).
-type snapshot() :: #snapshot{}.

-record(begin_txn_args, {dependency_vts :: vectorclock()}).
-type begin_txn_args() :: #begin_txn_args{}.

-record(prepare_txn_args, {partitions :: [partition()]}).
-type prepare_txn_args() :: #prepare_txn_args{}.
-record(commit_txn_args, {
    commit_vts :: vectorclock(),
    txn_tracking_num :: txn_tracking_num()
}).
-type commit_txn_args() :: #commit_txn_args{}.
-record(abort_txn_args, {}).
-type abort_txn_args() :: #abort_txn_args{}.
-record(checkpoint_args, {
    dependency_vts :: vectorclock(),
    dcid_to_last_txn_tracking_num :: #{dcid() => txn_tracking_num()}}).
-type checkpoint_args() :: #checkpoint_args{}.

-record(update_args, {
    key_struct :: key_struct(),
    tx_op_num :: tx_op_num(), %Order in a transaction
    downstream_op :: downstream_op()
}).
-type update_args() :: #update_args{}.

-type jsn() :: non_neg_integer().
-type journal_entry_type() :: begin_txn | prepare_txn | commit_txn | abort_txn | checkpoint | checkpoint_commit | update.
-type operation_type() :: journal_entry_type() | read | transaction.
-type journal_entry_args() :: begin_txn_args() | prepare_txn_args() | commit_txn_args() | abort_txn_args() | checkpoint_args() | update_args().

-record(journal_entry, {
    jsn :: jsn(),
    dcid :: dcid(),
    tx_id :: txid(),
    type :: journal_entry_type(),
    args :: journal_entry_args()
}).
-type journal_entry() :: #journal_entry{}.

-record(jsn_state, {
    next_jsn :: jsn(),
    dcid :: dcid()
}).
-type jsn_state() :: #jsn_state{}.
