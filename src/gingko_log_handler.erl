%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Okt 2019 13:48
%%%-------------------------------------------------------------------
-module(gingko_log_handler).
-include("gingko.hrl").
-author("kevin").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: log_handler_server_state()} | {ok, State :: log_handler_server_state(), timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init(NumberOfShards) ->

  {ok, #log_handler_server_state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: log_handler_server_state()) ->
  {reply, Reply :: term(), NewState :: log_handler_server_state()} |
  {reply, Reply :: term(), NewState :: log_handler_server_state(), timeout() | hibernate} |
  {noreply, NewState :: log_handler_server_state()} |
  {noreply, NewState :: log_handler_server_state(), timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: log_handler_server_state()} |
  {stop, Reason :: term(), NewState :: log_handler_server_state()}).
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: log_handler_server_state()) ->
  {noreply, NewState :: log_handler_server_state()} |
  {noreply, NewState :: log_handler_server_state(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: log_handler_server_state()}).
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: log_handler_server_state()) ->
  {noreply, NewState :: log_handler_server_state()} |
  {noreply, NewState :: log_handler_server_state(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: log_handler_server_state()}).
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: log_handler_server_state()) -> term()).
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: log_handler_server_state(),
    Extra :: term()) ->
  {ok, NewState :: log_handler_server_state()} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
