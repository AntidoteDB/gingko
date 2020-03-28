%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Antidote Consortium.  All Rights Reserved.
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
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(gingko).
-author("kevin").
-include("gingko.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).
-export([create_checkpoint_operation/1, create_update_operation/2, create_journal_entry/3, create_abort_operation/0, create_begin_operation/1, create_commit_operation/1, create_prepare_operation/1, create_read_operation/2, append_journal_entry/1, create_and_append_journal_entry/3]).

-define(SERVER, ?MODULE).

-record(state, {
  dcid :: dcid(),
  cache_server_pid :: pid(),
  next_jsn :: jsn(),
  checkpoint_interval_millis = 10000 :: non_neg_integer(),
  checkpoint_timer = none :: none | reference()
}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link({dcid(), [{atom(), term()}]}) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link({DcId, GingkoConfig}) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, {DcId, GingkoConfig}, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%TODO finish with all parameters
-spec apply_gingko_config(state(), [{atom(), term()}]) -> state().
apply_gingko_config(State, GingkoConfig) ->
  {UpdateCheckpointTimer, CheckpointIntervalMillis} =
    case lists:keyfind(checkpoint_interval_millis, 1, GingkoConfig) of
      {checkpoint_interval_millis, Value1} -> {true, Value1};
      false -> {false, State#state.checkpoint_interval_millis}
    end,
  NewState = State#state{checkpoint_interval_millis = CheckpointIntervalMillis},
  update_timers(NewState, UpdateCheckpointTimer).

-spec update_timers(state(), boolean()) -> state().
update_timers(State, UpdateCheckpointTimer) ->
  TimerCheckpoint =
    case State#state.checkpoint_timer of
      none ->
        erlang:send_after(State#state.checkpoint_timer, self(), checkpoint_event);
      Reference1 ->
        case UpdateCheckpointTimer of
          true ->
            erlang:cancel_timer(Reference1),
            erlang:send_after(State#state.checkpoint_interval_millis, self(), checkpoint_event);
          false -> Reference1
        end
    end,
  State#state{checkpoint_timer = TimerCheckpoint}.

-spec(init({dcid(), [{atom(), term()}]}) ->
  {ok, State :: state()} |
  {ok, State :: state(), timeout() | hibernate} |
  {stop, Reason :: term()} |
  ignore).
init({DcId, GingkoConfig}) ->
  {ok, CacheServerPid} = gingko_cache:start_link({DcId, self(), [{max_occupancy, 100}]}),
  NewState = #state{
    dcid = DcId,
    next_jsn = #jsn{number = 0},
    cache_server_pid = CacheServerPid
  },
  {ok, apply_gingko_config(NewState, GingkoConfig)}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
  {reply, Reply :: term(), NewState :: state()} |
  {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
  {stop, Reason :: term(), NewState :: state()}).
handle_call({{Op, Args}, TxId}, _From, State) ->
  NewState = increment_jsn(State),
  Jsn = NewState#state.next_jsn,
  case Op of
    read ->
      {Key, Type} = Args,
      KeyStruct = #key_struct{key = Key, type = Type},
      CacheServerPid = NewState#state.cache_server_pid,
      {reply, read(KeyStruct, Jsn, TxId, CacheServerPid), NewState};
    update ->
      {Key, Type, TypeOp} = Args,
      KeyStruct = #key_struct{key = Key, type = Type},
      CacheServerPid = NewState#state.cache_server_pid,
      {reply, update(KeyStruct, Jsn, TxId, TypeOp, CacheServerPid), NewState};
    begin_txn ->
      {reply, begin_txn(Jsn, TxId, Args), NewState};
    prepare_txn ->
      PrepareTime = Args,
      {reply, prepare_txn(Jsn, TxId, PrepareTime), NewState};
    commit_txn ->
      CommitTime = Args,
      {reply, commit_txn(Jsn, TxId, CommitTime), NewState};
    abort_txn ->
      {reply, abort_txn(Jsn, TxId), NewState}
  end.
  %TODO check if other arguments are needed

-spec(handle_cast(Request :: term(), State :: state()) ->
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: state()}).
handle_cast(_Request, State) ->
  %TODO handle multi read
  {noreply, State}.

-spec(handle_info(Info :: timeout() | term(), State :: state()) ->
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: state()}).
handle_info(checkpoint_event, State) ->
  %TODO
  NewState = increment_jsn(State),
  Jsn = NewState#state.next_jsn,
  TxId = #tx_id{local_start_time = gingko_utils:get_timestamp(), server_pid = self()},
  Vts = gingko_utils:get_GCSf_vts(),
  checkpoint(Jsn, TxId, Vts),
  {noreply, NewState};
handle_info(_Info, State) ->
  {noreply, State}.

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term()).
terminate(_Reason, _State) ->
  ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) ->
  {ok, NewState :: state()} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%====================================================================
%% API functions
%%====================================================================

%%TODO consider failures

-spec read(key_struct(), jsn(), txid(), pid()) -> {ok, snapshot_value()} | {error, reason()}.
read(KeyStruct, Jsn, TxId, CacheServerPid) ->
  Operation = create_read_operation(KeyStruct, []),
  JournalEntry = create_journal_entry(Jsn, TxId, Operation),
  append_journal_entry(JournalEntry),
  {ok, Snapshot} = gingko_log:perform_tx_read(KeyStruct, TxId, CacheServerPid),
  {ok, Snapshot#snapshot.value}.


-spec update(key_struct(), jsn(), txid(), type_op(), pid()) -> ok.
update(KeyStruct, Jsn, TxId, TypeOp, CacheServerPid) ->
  {ok, DownstreamOp} = gingko_utils:generate_downstream_op(KeyStruct, TxId, TypeOp, CacheServerPid),
  Operation = create_update_operation(KeyStruct, DownstreamOp),
  create_and_append_journal_entry(Jsn, TxId, Operation),
  ok.

-spec begin_txn(jsn(), txid(), vectorclock()) -> ok.
begin_txn(Jsn, TxId, DependencyVts) ->
  Operation = create_begin_operation(DependencyVts),
  create_and_append_journal_entry(Jsn, TxId, Operation).

-spec prepare_txn(jsn(), txid(), non_neg_integer()) -> ok.
prepare_txn(Jsn, TxId, PrepareTime) ->
  Operation = create_prepare_operation(PrepareTime),
  create_and_append_journal_entry(Jsn, TxId, Operation),
  {atomic, ok} = gingko_log:persist_journal_entries(),
  ok.

-spec commit_txn(jsn(), txid(), vectorclock()) -> ok.
commit_txn(Jsn, TxId, CommitTime) ->
  Operation = create_commit_operation(CommitTime),
  create_and_append_journal_entry(Jsn, TxId, Operation).

-spec abort_txn(jsn(), txid()) -> ok.
abort_txn(Jsn, TxId) ->
  Operation = create_abort_operation(),
  create_and_append_journal_entry(Jsn, TxId, Operation).

-spec checkpoint(jsn(), txid(), vectorclock()) -> ok.
checkpoint(Jsn, TxId, DependencyVts) ->
  Operation = create_checkpoint_operation(DependencyVts),
  create_and_append_journal_entry(Jsn, TxId, Operation),
  gingko_log:checkpoint(DependencyVts).



-spec create_and_append_journal_entry(jsn(), txid(), operation()) -> ok.
create_and_append_journal_entry(Jsn, TxId, Operation) ->
  JournalEntry = create_journal_entry(Jsn, TxId, Operation),
  append_journal_entry(JournalEntry).

-spec append_journal_entry(journal_entry()) -> ok.
append_journal_entry(JournalEntry) ->
  gingko_log:add_journal_entry(JournalEntry).

-spec create_journal_entry(jsn(), txid(), operation()) -> journal_entry().
create_journal_entry(Jsn, TxId, Operation) ->
  #journal_entry{
    jsn = Jsn,
    rt_timestamp = gingko_utils:get_timestamp(),
    tx_id = TxId,
    operation = Operation
  }.

-spec create_read_operation(key_struct(), term()) -> object_operation().
create_read_operation(KeyStruct, Args) ->
  #object_operation{
    key_struct = KeyStruct,
    op_type = read,
    op_args = Args
  }.

-spec create_update_operation(key_struct(), downstream_op()) -> object_operation().
create_update_operation(KeyStruct, DownstreamOp) ->
  #object_operation{
    key_struct = KeyStruct,
    op_type = update,
    op_args = DownstreamOp
  }.

-spec create_begin_operation(vectorclock()) -> system_operation().
create_begin_operation(DependencyVts) ->
  #system_operation{
    op_type = begin_txn,
    op_args = #begin_txn_args{dependency_vts = DependencyVts}
  }.

-spec create_prepare_operation(non_neg_integer()) -> system_operation().
create_prepare_operation(PrepareTime) ->
  #system_operation{
    op_type = prepare_txn,
    op_args = #prepare_txn_args{prepare_time = PrepareTime}
  }.

-spec create_commit_operation(vectorclock()) -> system_operation().
create_commit_operation(CommitTime) ->
  #system_operation{
    op_type = commit_txn,
    op_args = #commit_txn_args{commit_vts = CommitTime}
  }.

-spec create_abort_operation() -> system_operation().
create_abort_operation() ->
  #system_operation{
    op_type = abort_txn,
    op_args = #abort_txn_args{}
  }.

-spec create_checkpoint_operation(vectorclock()) -> system_operation().
create_checkpoint_operation(DependencyVts) ->
  #system_operation{
    op_type = checkpoint,
    op_args = #checkpoint_args{dependency_vts = DependencyVts}
  }.

-spec increment_jsn(state()) -> state().
increment_jsn(State) ->
  %%TODO check performance
  CurrentJsn = State#state.next_jsn,
  NextJsn = CurrentJsn#jsn{number = mnesia:table_info(journal_entry, size) + 1},
  State#state{next_jsn = NextJsn}.