
%% This can be used for testing, so that transactions start with
%% old snapshots to avoid clock-skew.
%% This can break the tests is not set to 0
-define(OLD_SS_MICROSEC, 0).
-define(USE_SINGLE_SERVER, false).
-define(GINGKO_APP_NAME, gingko_app).
-define(GINGKO_LOG_VNODE_MASTER, gingko_log_vnode_master).
-define(GINGKO_CACHE_VNODE_MASTER, gingko_cache_vnode_master).
-define(GINGKO_LOG, {gingko_log_server, ?GINGKO_LOG_VNODE_MASTER}).
-define(GINGKO_CACHE, {gingko_cache_server, ?GINGKO_CACHE_VNODE_MASTER}).
-define(COMM_TIMEOUT, infinity).
-define(ZMQ_TIMEOUT, 5000).
-define(NUM_W, 2).
-define(NUM_R, 2).
-define(DEFAULT_JOURNAL_PORT, 8086).
-define(DEFAULT_REQUEST_PORT, 8085).
%% Allow read concurrency on shared ets tables
%% These are the tables that store materialized objects
%% and information about live transactions, so the assumption
%% is there will be several more reads than writes
-define(TABLE_CONCURRENCY, {read_concurrency,true}).
%% The read concurrency is the maximum number of concurrent
%% readers per vnode.  This is so shared memory can be used
%% in the case of keys that are read frequently.  There is
%% still only 1 writer per vnode
-define(READ_CONCURRENCY, 20).
%% The log reader concurrency is the pool of threads per
%% physical node that handle requests from other DCs
%% for example to request missing log operations
-define(INTER_DC_QUERY_CONCURRENCY, 20).
%% This defines the concurrency for the meta-data tables that
%% are responsible for storing the stable time that a transaction
%% can read.  It is set to false because the erlang docs say
%% you should only set to true if you have long bursts of either
%% reads or writes, but here they should be interleaved (maybe).  But should
%% do some performance testing.
-define(META_TABLE_CONCURRENCY, {read_concurrency, false}, {write_concurrency, false}).
-define(META_TABLE_STABLE_CONCURRENCY, {read_concurrency, true}, {write_concurrency, false}).
%% The number of supervisors that are responsible for
%% supervising transaction coorinator fsms
-define(NUM_SUP, 100).
%% Threads will sleep for this length when they have to wait
%% for something that is not ready after which they
%% wake up and retry. I.e. a read waiting for
%% a transaction currently in the prepare state that is blocking
%% that read.
-define(SPIN_WAIT, 10).
%% HEARTBEAT_PERIOD: Period of sending the heartbeat messages in interDC layer
-define(HEARTBEAT_PERIOD, 1000).
%% VECTORCLOCK_UPDATE_PERIOD: Period of updates of the stable snapshot per partition
-define(VECTORCLOCK_UPDATE_PERIOD, 100).
%% This is the time that nodes will sleep inbetween sending meta-data
%% to other physical nodes within the DC
-define(META_DATA_SLEEP, 1000).
%% Uncomment the following line to use erlang:now()
%% Otherwise os:timestamp() is used which can go backwards
%% which is unsafe for clock-si
-define(SAFE_TIME, true).
%% Version of log records being used
-define(LOG_RECORD_VERSION, 0).
%% Bounded counter manager parameters.
%% Period during which transfer requests from the same DC to the same key are ignored.
-define(GRACE_PERIOD, 1000000). % in Microseconds
%% Time to forget a pending request.
-define(REQUEST_TIMEOUT, 500000). % In Microseconds
%% Frequency at which manager requests remote resources.
-define(TRANSFER_FREQ, 100). %in Milliseconds

-type microsecond() :: non_neg_integer().
-type millisecond() :: non_neg_integer().

-type key() :: term().
-type type() :: atom().
-type txn_properties() :: [{update_clock, boolean()} | {certify, use_default | certify | dont_certify}].
-type inter_dc_conn_err() :: {error, {partition_num_mismatch, non_neg_integer(), non_neg_integer()}
| {error, connection_error}}.

-record(tx_id, {
    local_start_time :: clock_time(),
    server_pid :: atom() | pid()
}).
-type txid() :: #tx_id{}.

-define(BUCKET, <<"antidote">>).

-type crdt() :: term().
-type type_op() :: {Op :: atom(), OpArgs :: term()} | atom() | term(). %downstream(type_op, crdt())
-type downstream_op() :: term(). %update(downstream_op, crdt())

-type dcid() :: undefined | {term(), term()}.

-type vectorclock() :: vectorclock:vectorclock().
-type vts_range() :: {MinVts :: vectorclock() | none, MaxVts :: vectorclock() | none}.
-type clock_time() :: non_neg_integer().
-type clock_range() :: {MinClock :: clock_time() | none, MaxClock :: clock_time() | none}.
-type reason() :: term().
-type map_list() :: [{Key :: term(), Value :: term()}].
-type index_node() :: {partition_id(), node()}.
-type preflist() :: riak_core_apl:preflist().
-type partition_id() :: chash:index_as_int().
-type ct_config() :: map_list().

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
    index = 0 :: non_neg_integer(),
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

-record(prepare_txn_args, {prepare_time :: non_neg_integer()}).
-type prepare_txn_args() :: #prepare_txn_args{}.
-record(commit_txn_args, {
    commit_vts :: vectorclock()
}).
-type commit_txn_args() :: #commit_txn_args{}.
-record(abort_txn_args, {}).
-type abort_txn_args() :: #abort_txn_args{}.
-record(checkpoint_args, {dependency_vts :: vectorclock()}).
-type checkpoint_args() :: #checkpoint_args{}.

-type system_operation_type() :: begin_txn | prepare_txn | commit_txn | abort_txn | checkpoint.
-type system_operation_args() :: begin_txn_args() | prepare_txn_args() | commit_txn_args() | abort_txn_args() | checkpoint_args().

-record(system_operation, {
    op_type :: system_operation_type(),
    op_args :: system_operation_args()
}).
-type system_operation() :: #system_operation{}.


-record(object_operation, {
    key_struct :: key_struct(),
    op_type :: update | read, %%TODO add others
    op_args :: term() %%TODO specify further if possible
}).
-type object_operation() :: #object_operation{}.


-type operation() :: system_operation() | object_operation().

-type jsn() :: non_neg_integer().
-record(dc_info, {
    dcid :: dcid(),
    rt_timestamp :: clock_time(),
    jsn :: jsn(),
    first_message_since_startup = false :: boolean()
}).
-type dc_info() :: #dc_info{}.

-record(journal_entry, {
    jsn :: jsn(),
    dc_info :: dc_info(),
    tx_id :: txid(),
    operation :: operation()
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
