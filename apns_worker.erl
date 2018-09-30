%%%-------------------------------------------------------------------
%%% @author linhaibo
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. 八月 2018 上午11:14
%%%-------------------------------------------------------------------
-module(apns_worker).
-author("linhaibo").

-behaviour(gen_server).

%% API
-export([start_link/1, push/4, push/5]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(APNS_MSG_TIMEOUT, 3600 * 24).
-define(VOIP_MSG_TIMEOUT, 60).
-define(EPOCH, 62167219200).
%%%===================================================================
%%% API
%%%===================================================================
push(PoolName, DeviceToken, UId, Content) ->
    push(PoolName, DeviceToken, UId, Content, false).
push(PoolName, DeviceToken, UId, Content, IsVOIP) ->
    try
        Worker = poolboy:checkout(PoolName),
        gen_server:call(Worker, {push, DeviceToken, Content, UId, IsVOIP}),
        poolboy:checkin(PoolName, Worker)
    catch
        _:_ ->
            error_logger:error_msg("bad pkg:~p", [PoolName]),
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link(Arg) ->
    gen_server:start_link(?MODULE, [Arg], []).

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
init([Args]) ->
    process_flag(trap_exit, true),
    Host = proplists:get_value(host, Args, "api.push.apple.com"),
    Port = proplists:get_value(port, Args, 443),
    Topic = proplists:get_value(topic, Args, "com.xiangli.where"),
    Mode = proplists:get_value(mode, Args, cert),
    Tab = ets:new(tab, [set, public]),
    
    case Mode of
        cert ->
            Cert = proplists:get_value(pem_cert, Args),
            Key = proplists:get_value(pem_key, Args),
            gun:open(Host, Port, #{transport => tls, transport_opts => [{certfile, Cert}, {keyfile, Key}]}),
            {ok, #{conn => undefined, topic => Topic, mod => cert, tab => Tab}};
        _ ->
            TokenServer = proplists:get_value(keyid, Args),
            gun:open(Host, Port, #{transport => tls}),
            {ok, #{conn => undefined, topic => Topic, mod => token, tab => Tab, tokenserver => list_to_atom(TokenServer)}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------

handle_call({push, DeviceToken, Content, UId, IsVOIP}, _From, #{conn := ConnPid, topic := Topic, tab := Tab, mod := Mode} = State) when is_pid(ConnPid) ->
    MicTime = os:system_time(microsecond),
    Topic1 = case IsVOIP of
                 true -> Topic ++ ".voip";
                 false -> Topic
             end,
    GMT = calendar:datetime_to_gregorian_seconds(calendar:universal_time()) - ?EPOCH,
    Expiry = case IsVOIP of
                 true -> integer_to_binary(GMT + ?VOIP_MSG_TIMEOUT);
                 false -> integer_to_binary(GMT + ?APNS_MSG_TIMEOUT)
             end,
    StreamRef = case Mode of
                    cert ->
                        gun:post(ConnPid, "/3/device/" ++ DeviceToken, [
                            {<<"apns-expiration">>, Expiry},
                            {<<"apns-priority">>, "10"},
                            {<<"apns-topic">>, Topic1}
                        ], Content, #{reply_to => self()});
                    _ ->
                        TokenServer = maps:get(tokenserver, State),
                        Token = token_server:get_token(TokenServer),
                        gun:post(ConnPid, "/3/device/" ++ DeviceToken, [
                            {<<"authorization">>, <<"bearer ", Token/binary>>},
                            {<<"apns-expiration">>, Expiry},
                            {<<"apns-priority">>, "10"},
                            {<<"apns-topic">>, Topic1}
                        ], Content, #{reply_to => self()})
                end,
    
    ets:insert(Tab, {StreamRef, UId, MicTime, Topic1}),
    {reply, ok, State};

handle_call(Request, _From, State) ->
    error_logger:error_msg("ignore msg:~p", [Request]),
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
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
handle_info({gun_up, PId, http2}, State) ->
    error_logger:info_msg("apns up:~p", [PId]),
    {noreply, State#{conn => PId}};

handle_info({gun_response, _ConnPid, _StreamRef, nofin, _Status, _Headers}, State) ->
    {noreply, State};

handle_info({gun_data, _, StreamRef, fin, Headers}, #{tab := Tab} = State) ->
    case ets:lookup(Tab, StreamRef) of
        [{_, UId, OldMicTime, Topic}] ->
            ets:delete(Tab, StreamRef),
            Cast = get_casttime(OldMicTime),
            try
                Reason = jsx:decode(Headers, [return_maps]),
                case maps:get(<<"reason">>, Reason) of
                    <<"TooManyProviderTokenUpdates">> ->
                        error_server:send_msg("Warning: TooManyProviderTokenUpdates! ", io_lib:format("[~p] to ~p fail [~p sec.]", [Topic, UId, Cast]));
                    _ ->
                        ok
                end
            catch
                _:_ -> ok
            end,
            error_logger:error_msg("[~p] to ~p error,reason:~p [~p sec]", [Topic, UId, Headers, Cast]);
        _ -> ok
    end,
    {noreply, State};

handle_info({gun_response, _, StreamRef, fin, 200, _Headers}, #{tab := Tab} = State) ->
    case ets:lookup(Tab, StreamRef) of
        [{_, UId, OldMicTime, Topic}] ->
            ets:delete(Tab, StreamRef),
            Cast = get_casttime(OldMicTime),
            case Cast > 10 of
                true ->
                    error_server:send_msg("Warning: SLOW apns push!", io_lib:format("[~p] to ~p success [~p sec.]", [Topic, UId, Cast]));
                false -> ok
            end,
            error_logger:info_msg("[~p] to ~p success [~p sec.]", [Topic, UId, Cast]);
        _ -> ok
    end,
    {noreply, State};

handle_info(Info, State) ->
    error_logger:info_msg("info:~p", [Info]),
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
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_casttime(OldMicTime) ->
    MicTime = os:system_time(microsecond),
    (MicTime - OldMicTime) / 1000000.
