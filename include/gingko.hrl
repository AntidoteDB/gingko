%TODO link to antidote_crdt types

-type key() :: term().
-type op() :: {update, {key(), type(), term()}} | {read, {key(), type()}}. %TODO what is the term in update?
-type type() :: atom(). 
-type downstream_record() :: term(). 
-type snapshot() :: term().
-type bucket() :: term().
-type tx_id() :: term().
-define(BUCKET, "antidote").




-define(LOGGING_MASTER, gingko_op_log_server).
%% Version of log records being used
-define(LOG_RECORD_VERSION, 0).

-record(op_number, {
    %% TODO 19 undefined is required here, because of the use in inter_dc_log_sender_vnode.
    %% The use there should be refactored.
    node :: undefined | {node(), dcid()},
    global :: undefined | non_neg_integer(),
    local :: undefined | non_neg_integer()
}).

-type op_name() :: atom().
-type op_param() :: term().
-type effect() :: term().
-type dcid() :: 'undefined' | {atom(),tuple()}. %% TODO, is this the only structure that is returned by riak_core_ring:cluster_name(Ring)?
-type snapshot_time() :: 'undefined' | vectorclock:vectorclock().
-type clock_time() :: non_neg_integer().
-type dc_and_commit_time() :: {dcid(), clock_time()}.
-type op_num() :: non_neg_integer().
-type op_id() :: {op_num(), node()}.
-type payload() :: term().
-type partition_id() :: ets:tid() | integer(). % TODO 19 adding integer basically makes the tid type non-opaque, because some places of the code depend on it being an integer. This dependency should be removed, if possible.
-type log_id() :: [partition_id()].
%%chash:index_as_int() is the same as riak_core_apl:index().
%%If it is changed in the future this should be fixed also.
-type index_node() :: {chash:index_as_int(), node()}.


-type log_names() :: {string(), string()}.

%%TODO describe this (first value is the current value and second value is the last committed value)
-type checkpoint_value() :: {term(), term()}.

-type sync_server_state() :: #sync_server_state{}.
-record(sync_server_state, {
  journal_log_name :: string(),
  journal_log :: log() | not_open,
  checkpoint_log_name :: string(),
  checkpoint_log :: log() | not_open
}).

-type log_server_state() :: #log_server_state{}.
-record(log_server_state, {
  % log name, used for storing logs in a directory related to the name
  journal_log_name :: string(),
  checkpoint_log_name :: string(),
  log_data_structure :: log_data_structure() | not_open,
  % handles syncing and opening the log
  sync_server :: pid()
}).

-type key_struct() :: #key_struct{}.
-record(key_struct, {
  key :: key(),
  type :: type()
}).

-type log_data_structure() :: #log_data_structure{}.
-record(log_data_structure, {
  journal_entry_list :: [journal_entry()],
  persistent_journal_log :: log(),
  checkpoint_key_value_map :: dict(),
  persistent_checkpoint_log :: log()
  }).

-type begin_txn_args() :: #begin_txn_args{}.
-record(begin_txn_args, {}).
-type prepare_txn_args() :: #prepare_txn_args{}.
-record(prepare_txn_args, {prepare_time :: non_neg_integer()}).
-type commit_txn_args() :: #commit_txn_args{}.
-record(commit_txn_args, {
  commit_time :: dc_and_commit_time(),
  snapshot_time :: snapshot_time()
}).
-type abort_txn_args() :: #abort_txn_args{}.
-record(abort_txn_args, {}).
-type checkpoint_args() :: #checkpoint_args{}.
-record(checkpoint_args, {}).


-type system_operation_type() :: begin_txn | prepare_txn | commit_txn | abort_txn | checkpoint.
-type system_operation_args() :: begin_txn_args() | prepare_txn_args() | commit_txn_args() | abort_txn_args() | checkpoint_args().

-type system_operation() :: #system_operation{}.
-record(system_operation, {
  op_type :: system_operation_type(),
  op_args :: system_operation_args()
}).


-type object_operation() :: #object_operation{}.
-record(object_operation, {
  key_struct :: key_struct(),
  op_type :: update | read, %%TODO add others
  op_args :: term() %%TODO specify further if possible
}).

-type operation() :: system_operation() | object_operation().

-record(journal_entry, {
  uuid :: term(),
  rt_timestamp :: clock_time(),
  tx_id :: tx_id(),
  operation :: operation()
}).

-type clocksi_payload() :: #clocksi_payload{}.
-record(clocksi_payload, {
    key :: key(),
    type :: type(),
    op_param :: effect(),
    snapshot_time :: snapshot_time(),
    commit_time :: dc_and_commit_time(),
    tx_id :: tx_id()
}).

-record(update_log_payload, {
    key :: key(),
    type :: type(),
    bucket :: term(), %TODO Get rid of that entry?
    op :: op()
}).

-type reason() :: term().
-type preflist() :: riak_core_apl:preflist().
-type cache_id() :: ets:tab().