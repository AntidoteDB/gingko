%% ==============
%% API
%%
%% Internal api documentation and types for the sync timing server
%% ==============

%%%===================================================================
%%% Types
%%%===================================================================

%%%===================================================================
%%% State
%%%===================================================================

-record(state, {
  % open references to logs to be closed after termination
  logs_to_close,

  % local server name
  server_name :: atom()
}).


%%%%%===================================================================
%%%%% Package Functions API
%%%%%===================================================================
%%
%%%% @doc Starts the log sync timing server for given minidote node
%%-spec start_link(minidote_node()) -> {ok, pid()}.
%%
%%%% @doc Returns the base log dir path specified by OP_LOG_DIR and the server name
%%-spec log_dir_base(server()) -> string().
%%
%%%% @doc Returns the path to the log file specified by the server name and given node
%%-spec log_file_path(server(), minidote_node()) -> string().
%%
%%
%%%%%===================================================================
%%%%% Private Functions API
%%%%%===================================================================
%%
%%%%% Implemented gen_server functions
%%%%% ================================
%%
%%%% @doc Initializes the internal server state
%%-spec init({minidote_node()}) -> {ok, #state{}}.
%%
%%%% @doc opens the log given by the server name (second argument) and the target node (third argument)
%%-spec handle_call
%%    ({get_log, atom(), minidote_node()}, gen_from(), #state{}) -> {reply, {ok, log()}, #state{}}.
%%
%%%% @doc Receives a syncing request. Depending on the strategy may or may not sync immediately
%%-spec handle_cast
%%    ({sync_log, atom(), minidote_node(), pid()}, #state{}) -> {noreply, #state{}}.
%%
%%%%% Other functions
%%%%% ===============
%%
%%%% @doc ensures directory where the log is expected and opens the log file.
%%%%      Recovers if required, logging found terms and bad bytes
%%-spec open_log(atom(), minidote_node()) -> {ok, log()}.
