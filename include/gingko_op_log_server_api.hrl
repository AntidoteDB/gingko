%% ==============
%% API
%% Internal api documentation and types for the minidote op log gen_server
%% ==============

%%%===================================================================
%%% State
%%%===================================================================

-record(state, {
  % recovery related state
  % receiver of the recovered log messages
  recovery_receiver :: pid(),
  % if recovering reply with busy status to requests
  recovering :: true | false,

  % log name, used for storing logs in a directory related to the name
  log_name :: atom(),
  % requests waiting until log is not recovering anymore or missing indices have been updated
  waiting_for_reply = [] :: [any()],
  % handles syncing and opening the log
  sync_server :: pid(),

  % stores current writable index
  next_index :: integer()
}).

%%%%%===================================================================
%%%%% Package Functions API
%%%%%===================================================================
%%
%%%% @doc Starts the op log server for given server name and recovery receiver process
%%-spec start_link(term(), pid()) -> {ok, pid()}.
%%
%%%%%===================================================================
%%%%% Private Functions API
%%%%%===================================================================
%%
%%%%% Implemented gen_server functions
%%%%% ================================
%%
%%%% @doc Initializes the internal server state
%%-spec init({log(), pid()}) -> {ok, #state{}}.
%%
%%%% @doc Either
%%%% 1) starts async recovery and does not reply until recovery is finished
%%%% or
%%%% 2) finishes async recovery and replies to waiting processes
%%-spec handle_cast
%%    (start_recovery, #state{}) -> {noreply, #state{}};          %1)
%%    ({finish_recovery, #{}}, #state{}) -> {noreply, #state{}}.  %2)
%%
%%
%%%% @doc Either
%%%% 1) adds a log entry for given node and a {Index, Data} pair
%%%% or
%%%% 2) reads the log
%%-spec handle_call
%%    ({add_log_entry, log_entry()}, gen_from(), #state{}) ->
%%  {noreply, #state{}} | %% if still recovering
%%  {reply, {error, index_already_written}, #state{}} | %% if index is already written
%%  {reply, ok, #state{}}; %% entry is persisted
%%    ({read_log_entries, minidote_node(), integer(), integer(), fun((log_entry(), Acc) -> Acc), Acc}, gen_from(), #state{}) ->
%%  {noreply, #state{}} | %% if still recovering or index for given node behind
%%  {reply, {ok, Acc}, #state{}}. %% accumulated entries
%%
%%
%%%%% Other functions
%%%%% ================================
%%
%%%% @doc Replies a 'retry' message to all waiting process to retry their action again
%%-spec reply_retry_to_waiting([gen_from()]) -> ok.
%%
%%
%%%% @doc recovers all logs for given server name
%%%%      the server name should be the local one
%%%%      also ensures that the directory actually exists
%%%%
%%%% @sends pid() ! {log_recovery, Node, {Index, Data}}
%%%%      for each entry in one log
%%%% @sends pid() ! {log_recovery_done}
%%%%      once after processing finished
%%-spec recover_all_logs(server(), pid(), pid()) -> any().
%%
%%
%%%% @doc reads all terms from given log
%%-spec read_all(log()) -> [term()].
