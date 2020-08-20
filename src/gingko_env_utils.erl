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

-module(gingko_env_utils).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("inter_dc.hrl").

%% API
-export([get_default_config/0,
    get_cache_max_occupancy/0,
    set_cache_max_occupancy/1,
    get_cache_reset_used_interval_millis/0,
    set_cache_reset_used_interval_millis/1,
    get_cache_eviction_interval_millis/0,
    set_cache_eviction_interval_millis/1,
    get_cache_eviction_threshold_in_percent/0,
    set_cache_eviction_threshold_in_percent/1,
    get_cache_target_threshold_in_percent/0,
    set_cache_target_threshold_in_percent/1,
    get_cache_eviction_strategy/0,
    set_cache_eviction_strategy/1,
    get_use_single_server/0,
    set_use_single_server/1,
    get_use_gingko_timestamp/0,
    set_use_gingko_timestamp/1,
    get_journal_trimming_mode/0,
    set_journal_trimming_mode/1,
    get_txn_port/0,
    set_txn_port/1,
    get_request_port/0,
    set_request_port/1,
    get_max_tx_run_time_millis/0,
    set_max_tx_run_time_millis/1,
    get_checkpoint_interval_millis/0,
    set_checkpoint_interval_millis/1,
    get_dc_state_interval_millis/0,
    set_dc_state_interval_millis/1,
    get_data_dir/0,
    set_data_dir/1]).

%%TODO consider better type checking

-spec get_default_config() -> key_value_list().
get_default_config() ->
    [
        {max_occupancy, get_cache_max_occupancy()},
        {reset_used_interval_millis, get_cache_reset_used_interval_millis()},
        {eviction_interval_millis, get_cache_eviction_interval_millis()},
        {eviction_threshold_in_percent, get_cache_eviction_threshold_in_percent()},
        {target_threshold_in_percent, get_cache_target_threshold_in_percent()},
        {eviction_strategy, get_cache_eviction_strategy()}
    ].

-spec get_cache_max_occupancy() -> pos_integer().
get_cache_max_occupancy() ->
    get_gingko_env(?CACHE_MAX_OCCUPANCY_OPTION_NAME, ?CACHE_MAX_OCCUPANCY_OPTION_DEFAULT).
-spec set_cache_max_occupancy(pos_integer()) -> ok.
set_cache_max_occupancy(MaxOccupancy) when is_integer(MaxOccupancy) andalso MaxOccupancy > 0 ->
    set_gingko_env(?CACHE_MAX_OCCUPANCY_OPTION_NAME, MaxOccupancy).

-spec get_cache_reset_used_interval_millis() -> millisecond().
get_cache_reset_used_interval_millis() ->
    get_gingko_env(?CACHE_RESET_USED_INTERVAL_OPTION_NAME, ?CACHE_RESET_USED_INTERVAL_OPTION_DEFAULT).
-spec set_cache_reset_used_interval_millis(millisecond()) -> ok.
set_cache_reset_used_interval_millis(Millis) when is_integer(Millis) andalso Millis >= 0 ->
    set_gingko_env(?CACHE_RESET_USED_INTERVAL_OPTION_NAME, Millis).

-spec get_cache_eviction_interval_millis() -> millisecond().
get_cache_eviction_interval_millis() ->
    get_gingko_env(?CACHE_EVICTION_INTERVAL_OPTION_NAME, ?CACHE_EVICTION_INTERVAL_OPTION_DEFAULT).
-spec set_cache_eviction_interval_millis(millisecond()) -> ok.
set_cache_eviction_interval_millis(Millis) when is_integer(Millis) andalso Millis >= 0 ->
    set_gingko_env(?CACHE_EVICTION_INTERVAL_OPTION_NAME, Millis).

-spec get_cache_eviction_threshold_in_percent() -> percent().
get_cache_eviction_threshold_in_percent() ->
    get_gingko_env(?CACHE_EVICTION_THRESHOLD_OPTION_NAME, ?CACHE_EVICTION_THRESHOLD_OPTION_DEFAULT).
-spec set_cache_eviction_threshold_in_percent(percent()) -> ok.
set_cache_eviction_threshold_in_percent(Percent) when is_integer(Percent) andalso Percent >= 0 andalso Percent =< 100 ->
    set_gingko_env(?CACHE_EVICTION_THRESHOLD_OPTION_NAME, Percent).

-spec get_cache_target_threshold_in_percent() -> percent().
get_cache_target_threshold_in_percent() ->
    get_gingko_env(?CACHE_TARGET_THRESHOLD_OPTION_NAME, ?CACHE_TARGET_THRESHOLD_OPTION_DEFAULT).
-spec set_cache_target_threshold_in_percent(percent()) -> ok.
set_cache_target_threshold_in_percent(Percent) when is_integer(Percent) andalso Percent >= 0 andalso Percent =< 100 ->
    set_gingko_env(?CACHE_TARGET_THRESHOLD_OPTION_NAME, Percent).

-spec get_cache_eviction_strategy() -> eviction_strategy().
get_cache_eviction_strategy() ->
    get_gingko_env(?CACHE_EVICTION_STRATEGY_OPTION_NAME, ?CACHE_EVICTION_STRATEGY_OPTION_DEFAULT).
-spec set_cache_eviction_strategy(eviction_strategy()) -> ok.
set_cache_eviction_strategy(EvictionStrategy) when is_atom(EvictionStrategy) ->
    set_gingko_env(?CACHE_EVICTION_STRATEGY_OPTION_NAME, EvictionStrategy).

-spec get_use_single_server() -> boolean().
get_use_single_server() ->
    get_gingko_env(?SINGLE_SERVER_OPTION_NAME, ?SINGLE_SERVER_OPTION_DEFAULT).
-spec set_use_single_server(boolean()) -> ok.
set_use_single_server(UseSingleServer) when is_boolean(UseSingleServer) ->
    set_gingko_env(?SINGLE_SERVER_OPTION_NAME, UseSingleServer).

-spec get_use_gingko_timestamp() -> boolean().
get_use_gingko_timestamp() ->
    get_gingko_env(?GINGKO_TIMESTAMP_OPTION_NAME, ?GINGKO_TIMESTAMP_OPTION_DEFAULT).
-spec set_use_gingko_timestamp(boolean()) -> ok.
set_use_gingko_timestamp(UseGingkoTimestamp) when is_boolean(UseGingkoTimestamp) ->
    set_gingko_env(?GINGKO_TIMESTAMP_OPTION_NAME, UseGingkoTimestamp).

-spec get_journal_trimming_mode() -> journal_trimming_mode().
get_journal_trimming_mode() ->
    get_gingko_env(?JOURNAL_TRIMMING_MODE_OPTION_NAME, ?JOURNAL_TRIMMING_MODE_OPTION_DEFAULT).
-spec set_journal_trimming_mode(journal_trimming_mode()) -> ok.
set_journal_trimming_mode(JournalTrimmingMode) when is_atom(JournalTrimmingMode) ->
    set_gingko_env(?JOURNAL_TRIMMING_MODE_OPTION_NAME, JournalTrimmingMode).

-spec get_txn_port() -> inet:port_number().
get_txn_port() ->
    get_gingko_env(?TXN_PORT_OPTION_NAME, ?TXN_PORT_OPTION_DEFAULT).
-spec set_txn_port(inet:port_number()) -> ok.
set_txn_port(Port) when is_integer(Port) andalso Port >= 0 andalso Port =< 65535 ->
    set_gingko_env(?TXN_PORT_OPTION_NAME, Port).

-spec get_request_port() -> inet:port_number().
get_request_port() ->
    get_gingko_env(?REQUEST_PORT_OPTION_NAME, ?REQUEST_PORT_OPTION_DEFAULT).
-spec set_request_port(inet:port_number()) -> ok.
set_request_port(Port) when is_integer(Port) andalso Port >= 0 andalso Port =< 65535 ->
    set_gingko_env(?REQUEST_PORT_OPTION_NAME, Port).

-spec get_max_tx_run_time_millis() -> millisecond().
get_max_tx_run_time_millis() ->
    get_gingko_env(?MAX_TX_RUN_TIME_OPTION_NAME, ?MAX_TX_RUN_TIME_OPTION_DEFAULT).
-spec set_max_tx_run_time_millis(millisecond()) -> ok.
set_max_tx_run_time_millis(Millis) when is_integer(Millis) andalso Millis >= 0 ->
    set_gingko_env(?MAX_TX_RUN_TIME_OPTION_NAME, Millis).

-spec get_checkpoint_interval_millis() -> millisecond().
get_checkpoint_interval_millis() ->
    get_gingko_env(?CHECKPOINT_INTERVAL_OPTION_NAME, ?CHECKPOINT_INTERVAL_OPTION_DEFAULT).
-spec set_checkpoint_interval_millis(millisecond()) -> ok.
set_checkpoint_interval_millis(Millis) when is_integer(Millis) andalso Millis >= 0 ->
    set_gingko_env(?CHECKPOINT_INTERVAL_OPTION_NAME, Millis).

-spec get_dc_state_interval_millis() -> millisecond().
get_dc_state_interval_millis() ->
    get_gingko_env(?DC_STATE_INTERVAL_OPTION_NAME, ?DC_STATE_INTERVAL_OPTION_DEFAULT).
-spec set_dc_state_interval_millis(millisecond()) -> ok.
set_dc_state_interval_millis(Millis) when is_integer(Millis) andalso Millis >= 0 ->
    set_gingko_env(?DC_STATE_INTERVAL_OPTION_NAME, Millis).

-spec get_data_dir() -> file:filename().
get_data_dir() ->
    get_gingko_env(?DATA_DIR_OPTION_NAME, ?DATA_DIR_OPTION_DEFAULT).
-spec set_data_dir(file:filename()) -> ok.
set_data_dir(DataDir) ->
    set_gingko_env(?DATA_DIR_OPTION_NAME, DataDir),
    application:set_env(mnesia, dir, DataDir).

get_gingko_env(Name, Default) ->
    application:get_env(?GINGKO_APP_NAME, Name, Default).
set_gingko_env(Name, Value) ->
    application:set_env(?GINGKO_APP_NAME, Name, Value).
