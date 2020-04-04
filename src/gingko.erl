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
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).
-export([create_checkpoint_operation/1, create_update_operation/2, create_journal_entry/2, create_abort_operation/0, create_begin_operation/1, create_commit_operation/1, create_prepare_operation/1, create_read_operation/2, append_journal_entry/2, create_and_append_journal_entry/2]).

-define(SERVER, ?MODULE).

-record(state, {
  id :: atom(),
  dcid :: dcid(),
  table_name :: atom(),
  cache_server_pid :: pid(),
  next_jsn = 0 :: non_neg_integer(),
  checkpoint_interval_millis = 10000 :: non_neg_integer(),
  checkpoint_timer = none :: none | reference()
}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link(atom(), map_list()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Id, GingkoConfig) ->
  gen_server:start_link(?MODULE, GingkoConfig, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(map_list()) ->
  {ok, State :: state()} |
  {ok, State :: state(), timeout() | hibernate} |
  {stop, Reason :: term()} |
  ignore).
init(GingkoConfig) ->
  NewState = apply_gingko_config(#state{}, GingkoConfig),
  TableName = NewState#state.table_name,
  NewGingkoConfig = [{table_name, TableName}|GingkoConfig],
  case NewState#state.id == error orelse NewState#state.dcid == error of
    true -> {stop, {"Invalid Configuration", NewGingkoConfig}};
    false ->
        mnesia:create_table(TableName,
        [{attributes, record_info(fields, journal_entry)},
          %{index, [#journal_entry.jsn]},%TODO find out why index doesn't work here
          {ram_copies, [node()]},
          {record_name, journal_entry}
        ]),
      {ok, CacheServerPid} = gingko_cache:start_link(NewGingkoConfig),
      {ok, NewState#state{cache_server_pid = CacheServerPid}}
  end.

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
  DcId = NewState#state.dcid,
  TableName = NewState#state.table_name,
  case Op of
    read ->
      {Key, Type} = Args,
      KeyStruct = #key_struct{key = Key, type = Type},
      CacheServerPid = NewState#state.cache_server_pid,
      {reply, read(KeyStruct, {Jsn, DcId, TxId, TableName}, CacheServerPid), NewState};
    update ->
      {Key, Type, TypeOp} = Args,
      KeyStruct = #key_struct{key = Key, type = Type},
      CacheServerPid = NewState#state.cache_server_pid,
      {reply, update(KeyStruct, {Jsn, DcId, TxId, TableName}, TypeOp, CacheServerPid), NewState};
    begin_txn ->
      {reply, begin_txn({Jsn, DcId, TxId, TableName}, Args), NewState};
    prepare_txn ->
      PrepareTime = Args,
      {reply, prepare_txn({Jsn, DcId, TxId, TableName}, PrepareTime), NewState};
    commit_txn ->
      CommitTime = Args,
      {reply, commit_txn({Jsn, DcId, TxId, TableName}, CommitTime), NewState};
    abort_txn ->
      {reply, abort_txn({Jsn, DcId, TxId, TableName}), NewState}
  end;
handle_call(Something, From, State) ->
  logger:error("Bad: ~p | ~p | ~p", [Something, From, State]),
  {reply, error, State}.

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
  {noreply, checkpoint_event(State)};

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

-spec apply_gingko_config(state(), [{atom(), term()}]) -> state().
apply_gingko_config(State, GingkoConfig) ->
  Id = general_utils:get_or_default_map_list(id, GingkoConfig, error),
  DcId = general_utils:get_or_default_map_list(dcid, GingkoConfig, error),
  TableName = general_utils:concat_and_make_atom([Id, '_journal_entry']),
  {UpdateCheckpointTimer, CheckpointIntervalMillis} =
    general_utils:get_or_default_map_list_check(checkpoint_interval_millis, GingkoConfig, State#state.checkpoint_interval_millis),
  NewState = State#state{id = Id, dcid = DcId, table_name = TableName, checkpoint_interval_millis = CheckpointIntervalMillis},
  update_timers(NewState, UpdateCheckpointTimer).

-spec update_timers(state(), boolean()) -> state().
update_timers(State, UpdateCheckpointTimer) ->
  TimerCheckpoint =
    case State#state.checkpoint_timer of
      none ->
        erlang:send_after(State#state.checkpoint_interval_millis, self(), checkpoint_event);
      Reference1 ->
        case UpdateCheckpointTimer of
          true ->
            erlang:cancel_timer(Reference1),
            erlang:send_after(State#state.checkpoint_interval_millis, self(), checkpoint_event);
          false -> Reference1
        end
    end,
  State#state{checkpoint_timer = TimerCheckpoint}.

-spec checkpoint_event(state()) -> state().
checkpoint_event(State) ->
  OldTimer = State#state.checkpoint_timer,
  DcId = State#state.dcid,
  TableName = State#state.table_name,
  erlang:cancel_timer(OldTimer),
  NewState = increment_jsn(State),
  Jsn = NewState#state.next_jsn,
  TxId = #tx_id{local_start_time = gingko_utils:get_timestamp(), server_pid = self()},
  Vts = gingko_utils:get_GCSf_vts(),
  checkpoint({Jsn, DcId, TxId, TableName}, Vts, NewState#state.cache_server_pid),
  NewTimer = erlang:send_after(NewState#state.checkpoint_interval_millis, self(), eviction_event),
  NewState#state{checkpoint_timer = NewTimer}.

-spec read(key_struct(), {jsn(), dcid(), txid(), atom()}, pid()) -> {ok, snapshot_value()} | {error, reason()}.
read(KeyStruct, {Jsn, DcId, TxId, TableName}, CacheServerPid) ->
  Operation = create_read_operation(KeyStruct, []),
  create_and_append_journal_entry({Jsn, DcId, TxId, TableName}, Operation),
  {ok, Snapshot} = gingko_log:perform_tx_read(KeyStruct, TxId, TableName, CacheServerPid),
  {ok, Snapshot#snapshot.value}.


-spec update(key_struct(), {jsn(), dcid(), txid(), atom()}, type_op(), pid()) -> ok.
update(KeyStruct, {Jsn, DcId, TxId, TableName}, TypeOp, CacheServerPid) ->
  {ok, DownstreamOp} = gingko_utils:generate_downstream_op(KeyStruct, TxId, TypeOp, TableName, CacheServerPid),
  Operation = create_update_operation(KeyStruct, DownstreamOp),
  create_and_append_journal_entry({Jsn, DcId, TxId, TableName}, Operation),
  ok.

-spec begin_txn({jsn(), dcid(), txid(), atom()}, vectorclock()) -> ok.
begin_txn({Jsn, DcId, TxId, TableName}, DependencyVts) ->
  Operation = create_begin_operation(DependencyVts),
  create_and_append_journal_entry({Jsn, DcId, TxId, TableName}, Operation).

-spec prepare_txn({jsn(), dcid(), txid(), atom()}, non_neg_integer()) -> ok.
prepare_txn({Jsn, DcId, TxId, TableName}, PrepareTime) ->
  Operation = create_prepare_operation(PrepareTime),
  create_and_append_journal_entry({Jsn, DcId, TxId, TableName}, Operation),
  {atomic, ok} = gingko_log:persist_journal_entries(TableName),
  ok.

-spec commit_txn({jsn(), dcid(), txid(), atom()}, vectorclock()) -> ok.
commit_txn({Jsn, DcId, TxId, TableName}, CommitTime) ->
  Operation = create_commit_operation(CommitTime),
  create_and_append_journal_entry({Jsn, DcId, TxId, TableName}, Operation).

-spec abort_txn({jsn(), dcid(), txid(), atom()}) -> ok.
abort_txn({Jsn, DcId, TxId, TableName}) ->
  Operation = create_abort_operation(),
  create_and_append_journal_entry({Jsn, DcId, TxId, TableName}, Operation).

-spec checkpoint({jsn(), dcid(), txid(), atom()}, vectorclock(), pid()) -> ok.
checkpoint({Jsn, DcId, TxId, TableName}, DependencyVts, CacheServerPid) ->
  Operation = create_checkpoint_operation(DependencyVts),
  create_and_append_journal_entry({Jsn, DcId, TxId, TableName}, Operation),
  {atomic, ok} = gingko_log:persist_journal_entries(TableName),
  gingko_log:checkpoint(TableName, CacheServerPid).

-spec create_and_append_journal_entry({jsn(), dcid(), txid(), atom()}, operation()) -> ok.
create_and_append_journal_entry({Jsn, DcId, TxId, TableName}, Operation) ->
  JournalEntry = create_journal_entry({Jsn, DcId, TxId}, Operation),
  append_journal_entry(JournalEntry, TableName).

-spec append_journal_entry(journal_entry(), atom()) -> ok.
append_journal_entry(JournalEntry, TableName) ->
  gingko_log:add_journal_entry(JournalEntry, TableName).

-spec create_journal_entry({jsn(), dcid(), txid()}, operation()) -> journal_entry().
create_journal_entry({Jsn, DcId, TxId}, Operation) ->
  #journal_entry{
    jsn = Jsn,
    dcid = DcId,
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
  TableName = State#state.table_name,
  NextJsn = mnesia:table_info(TableName, size) + 1,
  State#state{next_jsn = NextJsn}.