%% -------------------------------------------------------------------
%%
%% Copyright 2020, Kevin Bartik <k_bartik12@cs.uni-kl.de>
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(gingko_log_SUITE).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
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
    mnesia:create_schema([node()]),
    application:start(mnesia),
    ok.

end_per_suite(_Config) ->
    application:stop(mnesia),
    ok.

simple_write_and_read(_Config) ->
    gingko_log_utils:clear_journal_entries(gingko1_journal_entry),
    Jsn = 1,
    JournalEntry = #journal_entry{jsn = Jsn},
    ok = gingko_log_utils:add_journal_entry(JournalEntry, gingko1_journal_entry),
    JournalEntry = gingko_log_utils:read_journal_entry(Jsn, gingko1_journal_entry).

duplicate_journal_entry_write(_Config) ->
    Jsn = 1,
    JournalEntry = #journal_entry{jsn = Jsn},
    {error, {already_exists, [JournalEntry]}} = gingko_log_utils:add_journal_entry(JournalEntry, gingko1_journal_entry).

update_snapshot(_Config) ->
    KeyStruct = #key_struct{key = 1},
    Vts = vectorclock:new(),
    Snapshot = #snapshot{key_struct = KeyStruct, commit_vts = Vts, snapshot_vts = Vts, value = none},
    ok = gingko_log_utils:add_or_update_checkpoint_entry(Snapshot),
    true = Snapshot == gingko_log_utils:read_checkpoint_entry(KeyStruct, Vts),
    Snapshot2 = Snapshot#snapshot{value = 1},
    true = Snapshot2 /= gingko_log_utils:read_checkpoint_entry(KeyStruct, Vts),
    ok = gingko_log_utils:add_or_update_checkpoint_entry(Snapshot2),
    true = Snapshot2 == gingko_log_utils:read_checkpoint_entry(KeyStruct, Vts).

read_all_journal_entries(_Config) ->
    Jsn1 = 1,
    Jsn2 = 2,
    Jsn3 = 3,
    JournalEntry1 = #journal_entry{jsn = Jsn1},
    JournalEntry2 = #journal_entry{jsn = Jsn2},
    JournalEntry3 = #journal_entry{jsn = Jsn3},
    ok = gingko_log_utils:add_journal_entry(JournalEntry2, gingko1_journal_entry),
    ok = gingko_log_utils:add_journal_entry(JournalEntry3, gingko1_journal_entry),
    Result = gingko_log_utils:read_all_journal_entries_sorted(gingko1_journal_entry),
    true = lists:member(JournalEntry1, Result) andalso lists:member(JournalEntry2, Result) andalso lists:member(JournalEntry3, Result).

persistence_test_lost_entry(_Config) ->
    Jsn = 1,
    JournalEntry = #journal_entry{jsn = Jsn},
    true = JournalEntry == gingko_log_utils:read_journal_entry(Jsn, gingko1_journal_entry),
    simulate_crash(),
    {error, {"No Journal Entry found", Jsn}} = gingko_log_utils:read_journal_entry(Jsn, gingko1_journal_entry).

persistence_test_dumped_entry(_Config) ->
    Jsn = 1,
    JournalEntry = #journal_entry{jsn = Jsn},
    ok = gingko_log_utils:add_journal_entry(JournalEntry, gingko1_journal_entry),
    true = JournalEntry == gingko_log_utils:read_journal_entry(Jsn, gingko1_journal_entry),
    gingko_log_utils:persist_journal_entries(gingko1_journal_entry),
    simulate_crash(),
    true = JournalEntry == gingko_log_utils:read_journal_entry(Jsn, gingko1_journal_entry).

persistence_test_dumped_and_lost_entry(_Config) ->
    Jsn1 = 1,
    Jsn2 = 2,
    JournalEntry1 = #journal_entry{jsn = Jsn1},
    JournalEntry2 = #journal_entry{jsn = Jsn2},
    gingko_log_utils:add_journal_entry(JournalEntry1, gingko1_journal_entry),
    true = JournalEntry1 == gingko_log_utils:read_journal_entry(Jsn1, gingko1_journal_entry),
    gingko_log_utils:persist_journal_entries(gingko1_journal_entry),
    gingko_log_utils:add_journal_entry(JournalEntry2, gingko1_journal_entry),
    true = JournalEntry2 == gingko_log_utils:read_journal_entry(Jsn2, gingko1_journal_entry),
    simulate_crash(),
    true = JournalEntry1 == gingko_log_utils:read_journal_entry(Jsn1, gingko1_journal_entry),
    {error, {"No Journal Entry found", Jsn2}} = gingko_log_utils:read_journal_entry(Jsn2, gingko1_journal_entry).

simulate_crash() ->
    ok = application:stop(mnesia),
    no = mnesia:system_info(is_running),
    ok = application:start(mnesia),
    yes = mnesia:system_info(is_running),
    mnesia:wait_for_tables([gingko1_journal_entry, checkpoint_entry], 5000).
