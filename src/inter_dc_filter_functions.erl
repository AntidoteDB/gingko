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

-module(inter_dc_filter_functions).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-export([filter_by_jsn/2]).

-spec filter_by_jsn({jsn(), jsn()}, journal_entry()) -> boolean().
filter_by_jsn({MinJsn, MaxJsn}, #journal_entry{dc_info = #dc_info{jsn = Jsn}}) ->
    (Jsn >= MinJsn) andalso (Jsn =< MaxJsn).
