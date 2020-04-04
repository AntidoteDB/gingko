-module(gingko_log_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("gingko.hrl").
-export([init_per_suite/1, end_per_suite/1, all/0]).
-export([simple_write_and_read/1, duplicate_journal_entry_write/1, update_snapshot/1, read_all_journal_entries/1, persistence_test_lost_entry/1, persistence_test_dumped_entry/1, persistence_test_dumped_and_lost_entry/1]).

all() ->
  [simple_write_and_read, duplicate_journal_entry_write, update_snapshot, read_all_journal_entries, persistence_test_lost_entry, persistence_test_dumped_entry, persistence_test_dumped_and_lost_entry].

init_per_suite(Config) ->
  Priv = ?config(priv_dir, Config),
  application:load(mnesia),
  application:set_env(mnesia, dir, Priv),
  application:load(gingko_app),
  mnesia:create_schema([node()]),
  application:start(mnesia),
  case application:start(gingko_app) of
    ok ->
      WorkerId1 = gingko1,
      WorkerId2 = gingko2,
      DcId = undefined,
      GingkoConfig =
        [
          {checkpoint_interval_millis, 10000},
          {max_occupancy, 100},
          {reset_used_interval_millis, 1000},
          {eviction_interval_millis, 2000},
          {eviction_threshold_in_percent, 90},
          {target_threshold_in_percent, 80},
          {eviction_strategy, interval}
        ],
      {ok, Pid1} = gen_server:call(gingko_master, {start_gingko_worker, {WorkerId1, DcId, GingkoConfig}}),
      {ok, Pid2} = gen_server:call(gingko_master, {start_gingko_worker, {WorkerId2, DcId, GingkoConfig}}),
      [{gingko_pids, [Pid1, Pid2 | []]}|Config];
    _ ->
      {ok, Workers} = gen_server:call(gingko_master, get_gingko_workers),
      [{gingko_pids, [lists:map(fun({_Id, Pid}) -> Pid end, Workers)]}|Config]
  end.

end_per_suite(_Config) ->
  application:stop(mnesia),
  ok.

simple_write_and_read(_Config) ->
  gingko_log:clear_journal_entries(gingko1_journal_entry),
  Jsn = 1,
  JournalEntry = #journal_entry{jsn = Jsn},
  ok = gingko_log:add_journal_entry(JournalEntry, gingko1_journal_entry),
  JournalEntry = gingko_log:read_journal_entry(Jsn, gingko1_journal_entry).

duplicate_journal_entry_write(_Config) ->
  Jsn = 1,
  JournalEntry = #journal_entry{jsn = Jsn},
  {error, {already_exists, [JournalEntry]}} = gingko_log:add_journal_entry(JournalEntry, gingko1_journal_entry).

update_snapshot(_Config) ->
  KeyStruct = #key_struct{key = 1},
  Vts = vectorclock:new(),
  Snapshot = #snapshot{key_struct = KeyStruct, commit_vts = Vts, snapshot_vts = Vts, value = none},
  ok = gingko_log:add_or_update_checkpoint_entry(Snapshot),
  true = Snapshot == gingko_log:read_checkpoint_entry(KeyStruct, Vts),
  Snapshot2 = Snapshot#snapshot{value = 1},
  true = Snapshot2 /= gingko_log:read_checkpoint_entry(KeyStruct, Vts),
  ok = gingko_log:add_or_update_checkpoint_entry(Snapshot2),
  true = Snapshot2 == gingko_log:read_checkpoint_entry(KeyStruct, Vts).

read_all_journal_entries(_Config) ->
  Jsn1 = 1,
  Jsn2 = 2,
  Jsn3 = 3,
  JournalEntry1 = #journal_entry{jsn = Jsn1},
  JournalEntry2 = #journal_entry{jsn = Jsn2},
  JournalEntry3 = #journal_entry{jsn = Jsn3},
  ok = gingko_log:add_journal_entry(JournalEntry2, gingko1_journal_entry),
  ok = gingko_log:add_journal_entry(JournalEntry3, gingko1_journal_entry),
  Result = gingko_log:read_all_journal_entries_sorted(gingko1_journal_entry),
  true = lists:member(JournalEntry1, Result) andalso lists:member(JournalEntry2, Result) andalso lists:member(JournalEntry3, Result).

persistence_test_lost_entry(_Config) ->
  Jsn = 1,
  JournalEntry = #journal_entry{jsn = Jsn},
  true = JournalEntry == gingko_log:read_journal_entry(Jsn, gingko1_journal_entry),
  simulate_crash(),
  {error, {"No Journal Entry found", Jsn}} = gingko_log:read_journal_entry(Jsn, gingko1_journal_entry).

persistence_test_dumped_entry(_Config) ->
  Jsn = 1,
  JournalEntry = #journal_entry{jsn = Jsn},
  ok = gingko_log:add_journal_entry(JournalEntry, gingko1_journal_entry),
  true = JournalEntry == gingko_log:read_journal_entry(Jsn, gingko1_journal_entry),
  gingko_log:persist_journal_entries(gingko1_journal_entry),
  simulate_crash(),
  true = JournalEntry == gingko_log:read_journal_entry(Jsn, gingko1_journal_entry).

persistence_test_dumped_and_lost_entry(_Config) ->
  Jsn1 = 1,
  Jsn2 = 2,
  JournalEntry1 = #journal_entry{jsn = Jsn1},
  JournalEntry2 = #journal_entry{jsn = Jsn2},
  gingko_log:add_journal_entry(JournalEntry1, gingko1_journal_entry),
  true = JournalEntry1 == gingko_log:read_journal_entry(Jsn1, gingko1_journal_entry),
  gingko_log:persist_journal_entries(gingko1_journal_entry),
  gingko_log:add_journal_entry(JournalEntry2, gingko1_journal_entry),
  true = JournalEntry2 == gingko_log:read_journal_entry(Jsn2, gingko1_journal_entry),
  simulate_crash(),
  true = JournalEntry1 == gingko_log:read_journal_entry(Jsn1, gingko1_journal_entry),
  {error, {"No Journal Entry found", Jsn2}} = gingko_log:read_journal_entry(Jsn2, gingko1_journal_entry).

multiple_snapshot_test(_Config) ->
  ok.

simulate_crash() ->
  ok = application:stop(mnesia),
  no = mnesia:system_info(is_running),
  ok = application:start(mnesia),
  yes = mnesia:system_info(is_running),
  mnesia:wait_for_tables([gingko1_journal_entry, checkpoint_entry], 5000).

fill_state() ->
  %TODO Test Scenario
  KeyStruct1 = #key_struct{key = 1, type = antidote_crdt_register_mv},
  KeyStruct2 = #key_struct{key = 2, type = antidote_crdt_counter_pn},
  KeyStruct3 = #key_struct{key = 3, type = antidote_crdt_set_rw},
  KeyStruct4 = #key_struct{key = 4, type = antidote_crdt_map_rr},
  MapKeyStruct1 = #key_struct{key = 1, type = antidote_crdt_counter_pn},
  MapKeyStruct2 = #key_struct{key = 2, type = antidote_crdt_set_rw},

  TxId1 = #tx_id{local_start_time = erlang:monotonic_time(), server_pid = self()},
  VC1 = vectorclock:new(),
  %Transaction

  BeginOp1 = gingko:create_begin_operation(VC1),
  gingko:create_and_append_journal_entry(1, TxId1, BeginOp1),

  DownstreamOp1 = crdt_helpers:get_counter_pn_increment(1, antidote_crdt_counter_pn:new()),
  UpdateOp1 = gingko:create_update_operation(KeyStruct2, DownstreamOp1),
  gingko:create_and_append_journal_entry(2, TxId1, UpdateOp1),

  DownstreamOp2 = crdt_helpers:get_counter_fat_increment(3, antidote_crdt_counter_fat:new()),
  UpdateOp2 = gingko:create_update_operation(KeyStruct1, DownstreamOp2),
  gingko:create_and_append_journal_entry(3, TxId1, UpdateOp2),

  ReadOp1 = gingko:create_read_operation(KeyStruct2, []),
  gingko:create_and_append_journal_entry(4, TxId1, ReadOp1),

  DownstreamOp3 = crdt_helpers:get_map_rr_update_downstream(MapKeyStruct1, {increment, 3}, antidote_crdt_map_rr:new()),
  UpdateOp3 = gingko:create_update_operation(KeyStruct4, DownstreamOp3),
  gingko:create_and_append_journal_entry(5, TxId1, UpdateOp3),

  DownstreamOp4 = crdt_helpers:get_set_rw_add_downstream("Hello", antidote_crdt_set_rw:new()),
  UpdateOp4 = gingko:create_update_operation(KeyStruct3, DownstreamOp4),
  gingko:create_and_append_journal_entry(6, TxId1, UpdateOp4),


%%  DownstreamOp4 = crdt_helpers:get_map_rr_update_downstream(MapKeyStruct2, {add, "Hello"}, antidote_crdt_map_rr:new()),
%%  UpdateOp4 = gingko:create_update_operation(KeyStruct4, DownstreamOp4),
%%  gingko:create_and_append_journal_entry(Jsn6, TxId1, UpdateOp4),
ok.