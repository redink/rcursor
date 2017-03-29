%%%-------------------------------------------------------------------
%%% @author redink
%%% @copyright (C) , redink
%%% @doc
%%%
%%% @end
%%% Created :  by redink
%%%-------------------------------------------------------------------
-module(rcursor).

-behaviour(gen_server).

%% API
-export([start_link/0, stop/0]).

-export([ new_group_cursor/2
        , write_msg/2
        , read_msg/2
        , batch_read_msg/3
        , ack_msg/3
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(HIBERNATE_TIMEOUT, hibernate).

-record(state, {}).

-type ets_tab() :: ets:tid() | atom().
-type groupid() :: term().
-type userid()  :: term().
-type msgid()   :: term().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, stop).

-spec new_group_cursor(groupid(), integer()) -> ok | already_exist.
new_group_cursor(GroupID, MsgNumLimit) ->
    gen_server:call(?SERVER, {new_group_cursor, GroupID, MsgNumLimit}).

-spec write_msg(groupid(), msgid()) -> {error, group_not_found} | {ok, integer()}.
write_msg(GroupID, MsgID) ->
    case ets:lookup(?SERVER, GroupID) of
        [] ->
            {error, group_not_found};
        [{GroupID, _, MsgIDTable, MsgNumLimit}] ->
            ets:insert(MsgIDTable, {MsgID}),
            case ets:info(MsgIDTable, size) > MsgNumLimit of
                true ->
                    ets:delete(MsgIDTable, ets:first(MsgIDTable));
                _ ->
                    ignore
            end,
            {ok, ets:info(MsgIDTable, size)}
    end.

-spec read_msg(groupid(), userid()) ->
        {error, group_not_found} | {ok, list()}.
read_msg(GroupID, UserID) ->
    batch_read_msg(GroupID, UserID, 1).

-spec batch_read_msg(groupid(), userid(), integer()) ->
        {error, group_not_found} | {ok, list()}.
batch_read_msg(GroupID, UserID, Num) ->
    case ets:lookup(?SERVER, GroupID) of
        [] ->
            {error, group_not_found};
        [{_, UserCursorTable, MsgIDTable, _}] ->
            CurrentMsgID = read_msg(UserCursorTable, UserID, MsgIDTable),
            {ok, batch_read_msg(MsgIDTable, Num, CurrentMsgID, [])}
    end.

-spec ack_msg(groupid(), userid(), msgid()) ->
        {error, group_not_found} | {error, msgid_not_found} | ok.
ack_msg(GroupID, UserID, MsgID) ->
    case ets:lookup(?SERVER, GroupID) of
        [] ->
            {error, group_not_found};
        [{_, UserCursorTable, MsgIDTable, _}] ->
            ack_msg(MsgIDTable, MsgID, UserCursorTable, UserID)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ets:new(?SERVER, [named_table, set, public]),
    {ok, #state{}, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------

handle_call({new_group_cursor, GroupID, MsgNumLimit}, _From, State) ->
    Res =
        case ets:lookup(?SERVER, GroupID) of
            [] ->
                new_group_cursor_do(GroupID, MsgNumLimit),
                ok;
            _ ->
                already_exist
        end,
    {reply, Res, State, ?HIBERNATE_TIMEOUT};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec read_msg(ets_tab(), userid(), ets_tab()) ->
        '$end_of_table' | msgid().
read_msg(UserCursorTable, UserID, MsgIDTable) ->
    case ets:lookup(UserCursorTable, UserID) of
        [] ->
            ets_first(MsgIDTable);
        [{UserID, UserCursorIndex}] ->
            ets_next(MsgIDTable, UserCursorIndex)
    end.

-spec batch_read_msg(ets_tab(), integer(), msgid(), list()) -> list().
batch_read_msg(_MsgIDTable, 0, _, Res) ->
    lists:reverse(Res);
batch_read_msg(_MsgIDTable, _Num, '$end_of_table', Res) ->
    lists:reverse(Res);
batch_read_msg(MsgIDTable, Num, Key, Res) ->
    batch_read_msg(MsgIDTable, Num - 1, ets_next(MsgIDTable, Key), [Key | Res]).

-spec ack_msg(ets_tab(), msgid(), ets_tab(), userid()) ->
        {error, msgid_not_found} | ok.
ack_msg(MsgIDTable, MsgID, UserCursorTable, UserID) ->
    case ets:lookup(MsgIDTable, MsgID) of
        [] ->
            {error, msgid_not_found};
        _ ->
            ets:insert(UserCursorTable, {UserID, MsgID}),
            ok
    end.

-spec new_group_cursor_do(groupid(), integer()) -> ok.
new_group_cursor_do(GroupID, MsgNumLimit) ->
    UserCursorTable = ets:new(user_cursor, [set, public]),
    MsgIDTable      = ets:new(msgid, [ordered_set, public]),
    ets:insert(?SERVER, {GroupID, UserCursorTable, MsgIDTable, MsgNumLimit}),
    ok.

-ifdef(ASCENDING).

ets_first(Table) ->
    ets:first(Table).

ets_next(Table, Key) ->
    ets:next(Table, Key).

-else.

ets_first(Table) ->
    ets:last(Table).

ets_next(Table, Key) ->
    ets:prev(Table, Key).

-endif.

%%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

rcursor_test_() ->
    [ {"whole_flow",
        fun() ->
            start_link(),
            %% 
            ?assertEqual(ok, new_group_cursor("group1", 20)),
            ?assertEqual(already_exist, new_group_cursor("group1", 20)),
            %%
            ?assertEqual({error, group_not_found}, write_msg("group2", 1)),
            [?assertMatch({ok, _}, write_msg("group1", X))
             || X <- lists:seq(1, 21)],
            %%
            ?assertEqual({error, group_not_found}, read_msg("group2", "user1")),
            ?assertEqual({ok, [2]}, read_msg("group1", "user1")),
            ?assertEqual({ok, [2]}, read_msg("group1", "user2")),
            ?assertEqual({ok, [2]}, read_msg("group1", "user3")),
            ?assertEqual({ok, [2]}, read_msg("group1", "user1")),
            %%
            ?assertEqual({error, group_not_found}, ack_msg("group2", "user1", 2)),
            ?assertEqual({error, msgid_not_found}, ack_msg("group1", "user1", fake)),
            ?assertEqual(ok, ack_msg("group1", "user1", 2)),
            ?assertEqual(ok, ack_msg("group1", "user2", 2)),
            %%
            ?assertEqual({ok, [3,4,5,6,7,8,9,10]}, batch_read_msg("group1", "user1", 8)),
            ?assertEqual({ok, [3]}, read_msg("group1", "user1")),
            ?assertEqual(ok, ack_msg("group1", "user1", 10)),
            ?assertEqual({ok, [11,12,13,14,15,16]}, batch_read_msg("group1", "user1", 6)),
            ?assertEqual(ok, ack_msg("group1", "user1", 16)),
            ?assertEqual({ok, [17,18,19,20,21]}, batch_read_msg("group1", "user1", 10)),
            ?assertEqual(ok, ack_msg("group1", "user1", 21)),
            ?assertEqual({ok, []}, batch_read_msg("group1", "user1", 2)),
            stop()
        end}

    ].

-endif.
