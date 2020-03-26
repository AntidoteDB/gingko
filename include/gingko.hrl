-define(LOGGING_MASTER, gingko_log_server).
%% Version of log records being used
-define(LOG_RECORD_VERSION, 0).

-type key() :: term().
-type type() :: atom().
-type txn_properties() :: [{update_clock, boolean()} | {certify, use_default | certify | dont_certify}].

-record(tx_id, {
  local_start_time :: clock_time(),
  server_pid :: atom() | pid()
}).
-type txid() :: #tx_id{}.

-define(BUCKET, "antidote").

-type crdt() :: term().
-type type_op() :: {Op :: atom(), OpArgs :: term()} | atom() | term(). %downstream(type_op, crdt())
-type downstream_op() :: term(). %update(downstream_op, crdt())

-type dcid() :: undefined | term().%%TODO riak_core_ring:riak_core_ring().

-type dc_and_commit_time() :: {dcid(), clock_time()}.
-type vectorclock() :: vectorclock:vectorclock().
-type vectorclock_or_none() :: vectorclock() | none.
-type vts_range() :: {MinVts :: vectorclock_or_none(), MaxVts :: vectorclock_or_none()}.
-type clock_time() :: non_neg_integer().
-type clock_time_or_none() :: clock_time() | none.
-type clock_range() :: {MinClock :: clock_time_or_none(), MaxClock :: clock_time_or_none()}.
-type reason() :: term().
-type log_names() :: {string(), string()}.


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
  present = true :: boolean(),
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
  commit_vts :: vectorclock(),
  snapshot_vts :: vectorclock()
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

-record(journal_entry, {
  jsn :: jsn(),
  rt_timestamp :: clock_time(),
  tx_id :: txid() | none,
  operation :: operation()
}).
-type journal_entry() :: #journal_entry{}.

-record(jsn, {
  dcid = undefined :: dcid(),
  number :: non_neg_integer()
}).
-type jsn() :: #jsn{}.

-record(update_payload, {
  key_struct :: key_struct(),
  op_param :: downstream_op() | fun(), %%TODO define operations on non crdt types
  commit_vts :: vectorclock(),
  snapshot_vts :: vectorclock(),
  tx_id :: txid()
}).
-type update_payload() :: #update_payload{}.
