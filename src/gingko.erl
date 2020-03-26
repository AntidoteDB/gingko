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
-export([create_checkpoint_operation/1, create_update_operation/2, create_journal_entry/3, create_abort_operation/0, create_begin_operation/1, create_commit_operation/2, create_prepare_operation/1, create_read_operation/2, append_journal_entry/1, create_and_append_journal_entry/3]).

-define(SERVER, ?MODULE).

-record(state, {
  dcid :: dcid(),
  cache_server_pid :: pid(),
  next_jsn :: jsn()
}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link({dcid(), log_names()}) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link({DcId, LogNames}) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, {DcId, LogNames}, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init({dcid(), log_names()}) ->
  {ok, State :: state()} |
  {ok, State :: state(), timeout() | hibernate} |
  {stop, Reason :: term()} |
  ignore).
init({DcId, _LogNames}) ->
  {ok, CacheServerPid} = gingko_cache:start_link({DcId, self(), [{max_occupancy, 100}]}),
  {ok, #state{
    dcid = DcId,
    next_jsn = #jsn{number = 0},
    cache_server_pid = CacheServerPid
  }}.

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
      {CommitTime, SnapshotTime} = Args,
      {reply, commit_txn(Jsn, TxId, CommitTime, SnapshotTime), NewState};
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

-spec commit_txn(jsn(), txid(), vectorclock(), vectorclock()) -> ok.
commit_txn(Jsn, TxId, CommitTime, SnapshotTime) ->
  Operation = create_commit_operation(CommitTime, SnapshotTime),
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

-spec create_commit_operation(vectorclock(), vectorclock()) -> system_operation().
create_commit_operation(CommitTime, SnapshotTime) ->
  #system_operation{
    op_type = commit_txn,
    op_args = #commit_txn_args{commit_vts = CommitTime, snapshot_vts = SnapshotTime}
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
  %%TODO mnesia transaction to find next jsn
  CurrentJsn = State#state.next_jsn,
  CurrentNumber = CurrentJsn#jsn.number,
  NextJsn = CurrentJsn#jsn{number = CurrentNumber + 1},
  State#state{next_jsn = NextJsn}.