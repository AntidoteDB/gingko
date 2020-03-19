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
  Config.

end_per_suite(_Config) ->
  application:stop(gingko_app),
  ok.

simple_write_and_read(_Config) ->
  Jsn = #jsn{number = 1, dcid = 'undefined'},
  JournalEntry = #journal_entry{jsn = Jsn},
  ok = gingko_log:add_journal_entry(JournalEntry),
  JournalEntry = gingko_log:read_journal_entry(Jsn).

duplicate_journal_entry_write(_Config) ->
  Jsn = #jsn{number = 1, dcid = 'undefined'},
  JournalEntry = #journal_entry{jsn = Jsn},
  {error, {already_exists, [JournalEntry]}} = gingko_log:add_journal_entry(JournalEntry).

update_snapshot(_Config) ->
  KeyStruct = #key_struct{key = 1},
  Snapshot = #snapshot{key_struct = KeyStruct, value = none},
  ok = gingko_log:add_or_update_snapshot(Snapshot),
  true = Snapshot == gingko_log:read_snapshot(KeyStruct),
  Snapshot2 = Snapshot#snapshot{value = 1},
  true = Snapshot2 =/= gingko_log:read_snapshot(KeyStruct),
  ok = gingko_log:add_or_update_snapshot(Snapshot2),
  true = Snapshot2 == gingko_log:read_snapshot(KeyStruct).

read_all_journal_entries(_Config) ->
  Jsn1 = #jsn{number = 1, dcid = 'undefined'},
  Jsn2 = #jsn{number = 2, dcid = 'undefined'},
  Jsn3 = #jsn{number = 3, dcid = 'undefined'},
  JournalEntry1 = #journal_entry{jsn = Jsn1},
  JournalEntry2 = #journal_entry{jsn = Jsn2},
  JournalEntry3 = #journal_entry{jsn = Jsn3},
  ok = gingko_log:add_journal_entry(JournalEntry2),
  ok = gingko_log:add_journal_entry(JournalEntry3),
  Result = gingko_log:read_all_journal_entries(),
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

simulate_crash() ->
  application:stop(gingko_app),
  mnesia:stop(),
  no = mnesia:system_info(is_running),
  mnesia:start(),
  yes = mnesia:system_info(is_running),
  application:start(gingko_app).
