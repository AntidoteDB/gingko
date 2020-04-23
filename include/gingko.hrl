
%% This can be used for testing, so that transactions start with
%% old snapshots to avoid clock-skew.
%% This can break the tests is not set to 0
-define(OLD_SS_MICROSEC,0).

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
-type map_list() :: [{Key :: term(), Value :: term()}].
-type index_node() :: {partition_id(), node()}.
-type preflist() :: riak_core_apl:preflist().
-type partition_id() :: chash:index_as_int().
-type snapshot_time() :: 'undefined' | vectorclock:vectorclock().

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

-record(journal_entry, {
  jsn :: jsn(),
  dcid :: dcid(),
  rt_timestamp :: clock_time(),
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
