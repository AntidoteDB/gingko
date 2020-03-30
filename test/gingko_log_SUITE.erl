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
  gingko_app:install([node()]),
  application:start(mnesia),
  application:start(gingko_app),
  [{gingko_app_pid, gingko}|Config].

end_per_suite(_Config) ->
  application:stop(mnesia),
  ok.

simple_write_and_read(_Config) ->
  gingko_log:clear_journal_entries(),
  Jsn = #jsn{number = 1},
  JournalEntry = #journal_entry{jsn = Jsn},
  ok = gingko_log:add_journal_entry(JournalEntry),
  JournalEntry = gingko_log:read_journal_entry(Jsn).

duplicate_journal_entry_write(_Config) ->
  Jsn = #jsn{number = 1},
  JournalEntry = #journal_entry{jsn = Jsn},
  {error, {already_exists, [JournalEntry]}} = gingko_log:add_journal_entry(JournalEntry).

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
  Jsn1 = #jsn{number = 1},
  Jsn2 = #jsn{number = 2},
  Jsn3 = #jsn{number = 3},
  JournalEntry1 = #journal_entry{jsn = Jsn1},
  JournalEntry2 = #journal_entry{jsn = Jsn2},
  JournalEntry3 = #journal_entry{jsn = Jsn3},
  ok = gingko_log:add_journal_entry(JournalEntry2),
  ok = gingko_log:add_journal_entry(JournalEntry3),
  Result = gingko_log:read_all_journal_entries_sorted(),
  true = lists:member(JournalEntry1, Result) andalso lists:member(JournalEntry2, Result) andalso lists:member(JournalEntry3, Result).

persistence_test_lost_entry(_Config) ->
  Jsn = #jsn{number = 1, dcid = 'undefined'},
  JournalEntry = #journal_entry{jsn = Jsn},
  true = JournalEntry == gingko_log:read_journal_entry(Jsn),
  simulate_crash(),
  {error, {"No Journal Entry found", Jsn}} = gingko_log:read_journal_entry(Jsn).

persistence_test_dumped_entry(_Config) ->
  Jsn = #jsn{number = 1, dcid = 'undefined'},
  JournalEntry = #journal_entry{jsn = Jsn},
  ok = gingko_log:add_journal_entry(JournalEntry),
  true = JournalEntry == gingko_log:read_journal_entry(Jsn),
  gingko_log:persist_journal_entries(),
  simulate_crash(),
  true = JournalEntry == gingko_log:read_journal_entry(Jsn).

persistence_test_dumped_and_lost_entry(_Config) ->
  Jsn1 = #jsn{number = 1, dcid = 'undefined'},
  Jsn2 = #jsn{number = 2, dcid = 'undefined'},
  JournalEntry1 = #journal_entry{jsn = Jsn1},
  JournalEntry2 = #journal_entry{jsn = Jsn2},
  gingko_log:add_journal_entry(JournalEntry1),
  true = JournalEntry1 == gingko_log:read_journal_entry(Jsn1),
  gingko_log:persist_journal_entries(),
  gingko_log:add_journal_entry(JournalEntry2),
  true = JournalEntry2 == gingko_log:read_journal_entry(Jsn2),
  simulate_crash(),
  true = JournalEntry1 == gingko_log:read_journal_entry(Jsn1),
  {error, {"No Journal Entry found", Jsn2}} = gingko_log:read_journal_entry(Jsn2).

multiple_snapshot_test(_Config) ->
  ok.

simulate_crash() ->
  mnesia:stop(),
  no = mnesia:system_info(is_running),
  mnesia:start(),
  yes = mnesia:system_info(is_running),
  mnesia:wait_for_tables([journal_entry, checkpoint_entry], 5000).

fill_state() ->
  KeyStruct1 = #key_struct{key = 1, type = antidote_crdt_register_mv},
  KeyStruct2 = #key_struct{key = 2, type = antidote_crdt_counter_pn},
  KeyStruct3 = #key_struct{key = 3, type = antidote_crdt_set_rw},
  KeyStruct4 = #key_struct{key = 4, type = antidote_crdt_map_rr},
  MapKeyStruct1 = #key_struct{key = 1, type = antidote_crdt_counter_pn},
  MapKeyStruct2 = #key_struct{key = 2, type = antidote_crdt_set_rw},

  TxId1 = #tx_id{local_start_time = erlang:monotonic_time(), server_pid = self()},
  VC1 = vectorclock:new(),
  %Transaction
  Jsn1 = #jsn{number = 1},
  Jsn2 = #jsn{number = 2},
  Jsn3 = #jsn{number = 3},
  Jsn4 = #jsn{number = 4},
  Jsn5 = #jsn{number = 5},
  Jsn6 = #jsn{number = 6},
  Jsn7 = #jsn{number = 7},
  Jsn8 = #jsn{number = 8},
  Jsn9 = #jsn{number = 9},
  Jsn10 = #jsn{number = 10},
  Jsn11 = #jsn{number = 11},
  Jsn12 = #jsn{number = 12},

  BeginOp1 = gingko:create_begin_operation(VC1),
  gingko:create_and_append_journal_entry(Jsn1, TxId1, BeginOp1),

  DownstreamOp1 = crdt_helpers:get_counter_pn_increment(1, antidote_crdt_counter_pn:new()),
  UpdateOp1 = gingko:create_update_operation(KeyStruct2, DownstreamOp1),
  gingko:create_and_append_journal_entry(Jsn2, TxId1, UpdateOp1),

  DownstreamOp2 = crdt_helpers:get_counter_fat_increment(3, antidote_crdt_counter_fat:new()),
  UpdateOp2 = gingko:create_update_operation(KeyStruct1, DownstreamOp2),
  gingko:create_and_append_journal_entry(Jsn3, TxId1, UpdateOp2),

  ReadOp1 = gingko:create_read_operation(KeyStruct2, []),
  gingko:create_and_append_journal_entry(Jsn4, TxId1, ReadOp1),

  DownstreamOp3 = crdt_helpers:get_map_rr_update_downstream(MapKeyStruct1, {increment, 3}, antidote_crdt_map_rr:new()),
  UpdateOp3 = gingko:create_update_operation(KeyStruct4, DownstreamOp3),
  gingko:create_and_append_journal_entry(Jsn5, TxId1, UpdateOp3),

  DownstreamOp4 = crdt_helpers:get_set_rw_add_downstream("Hello", antidote_crdt_set_rw:new()),
  UpdateOp4 = gingko:create_update_operation(KeyStruct3, DownstreamOp4),
  gingko:create_and_append_journal_entry(Jsn7, TxId1, UpdateOp4),


%%  DownstreamOp4 = crdt_helpers:get_map_rr_update_downstream(MapKeyStruct2, {add, "Hello"}, antidote_crdt_map_rr:new()),
%%  UpdateOp4 = gingko:create_update_operation(KeyStruct4, DownstreamOp4),
%%  gingko:create_and_append_journal_entry(Jsn6, TxId1, UpdateOp4),


  JournalEntry1 = #journal_entry{jsn = Jsn1},
  JournalEntry2 = #journal_entry{jsn = Jsn2},
  JournalEntry3 = #journal_entry{jsn = Jsn3}.