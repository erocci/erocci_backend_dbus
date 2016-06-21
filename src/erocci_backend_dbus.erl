%%% @author Jean Parpaillon <jean.parpaillon@free.fr>
%%% @copyright (c) 2013-2016 Jean Parpaillon
%%% 
%%% This file is provided to you under the license described
%%% in the file LICENSE at the root of the project.
%%%
%%% You can also download the LICENSE file from the following URL:
%%% https://github.com/erocci/erocci/blob/master/LICENSE
%%% 
%%% @doc
%%%
%%% @end
%%% Created :  31 Jul 2014 by Jean Parpaillon <jean.parpaillon@free.fr>
-module(erocci_backend_dbus).

-behaviour(erocci_backend).

-include_lib("erocci_core/include/erocci.hrl").
-include_lib("erocci_core/include/erocci_log.hrl").
-include_lib("dbus/include/dbus_client.hrl").

-include("occi_dbus.hrl").

-define(IFACE_BACKEND, <<"org.ow2.erocci.backend.core">>).

%% occi_backend callbacks
-export([init/1,
         terminate/1]).

-export([models/1,
	 get/2,
	 create/4,
	 create/5,
	 update/3,
	 link/4,
	 action/4,
	 delete/2,
	 mixin/4,
	 unmixin/3,
	 collection/5]).

-record(state, { conn       :: dbus_connection(),
		 proxy      :: dbus_proxy() }).

-define(dbus_exception_notfound, <<"org.ow2.erocci.backend.NotFound">>).
-define(dbus_exception_conflict, <<"org.ow2.erocci.backend.Conflict">>).

%%%===================================================================
%%% occi_backend callbacks
%%%===================================================================
-spec init(Opts :: term()) ->
		  {ok, Caps :: [erocci_backend:capability()], State :: term()} |
		  {error, Reason :: term()}.
init(Props) ->
    try parse_opts(Props) of
        {Service, Opts} -> connect_backend(Service, Opts)
    catch throw:Err -> {error, Err}
    end.


-spec terminate(State :: term()) -> ok.
terminate(#state{ proxy=Backend }) ->
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Terminate">>, []) of
        _ -> ok
    end.


-spec models(State :: term()) -> 
    {{ok, [occi_extension:t()]} | {error, erocci_backend:error()}, NewState :: term()}.
models(#state{ proxy=Backend }=S) ->
    ?info("[~s] models()", [?MODULE]),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Models">>, []) of
	{ok, Schemas} ->
	    parse_models(Schemas, [], S);
	{error, _}=Err ->
	    {Err, S}
    end.


-spec get(Location :: binary(), State :: term()) ->
		 {{ok, occi_collection:t() | occi_entity:t(), erocci_creds:user(), erocci_creds:group(), erocci_node:serial()} 
		  | {error, erocci_backend:error()}, NewState :: term()}.
get(Location, #state{ proxy=Backend }=S) ->
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Get">>, [Location]) of
	{ok, {KindId, MixinIds, Attributes, Owner, Group, Serial}} ->
	    Entity = unmarshal_entity(KindId, MixinIds, Attributes),
	    {{ok, [Entity, unmarshal_user(Owner), unmarshal_user(Group), unmarshal_serial(Serial)]}, S};
	{error, Err} ->
	    dbus_errors(Err, S)
    end.


-spec create(Location :: binary(), Entity :: occi_entity:t(), 
	     Owner :: erocci_creds:user(), Group :: erocci_creds:group(), State :: term()) ->
		    {{ok, occi_entity:t(), erocci_node:serial()}
		     | {error, erocci_backend:error()}, NewState :: term()}.
create(Location, Entity, Owner, Group, #state{ proxy=Backend }=S) ->
    ?info("[~p] create(~s)", [?MODULE, Location]),
    {Scheme, Term} = occi_entity:kind(Entity),
    Args = [ 
	     Location,
	     << Scheme/binary, Term/binary >>,
	     lists:foldr(fun ({MixinScheme, MixinTerm}, Acc) ->
				 [ << MixinScheme/binary, MixinTerm/binary >> | Acc ]
			 end, [], occi_entity:mixins(Entity)),
	     occi_entity:raw_attributes(Entity),
	     marshal_user(Owner),
	     marshal_user(Group)
	   ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Create1">>, Args) of
	{ok, {KindId, MixinIds, Attributes, Serial}} ->
	    dbus_ok(KindId, MixinIds, Attributes, 
		    fun (Entity2) ->
			    {ok, Entity2, unmarshal_serial(Serial)}
		    end, S);
	{error, Err} ->
	    dbus_errors(Err, S)
    end.


-spec create(Entity :: occi_entity:t(), Owner :: erocci_creds:user(), Group :: erocci_creds:group(), State :: term()) ->
    {{ok, occi_uri:url(), occi_entity:t(), erocci_node:serial()} 
     | {error, erocci_backend:error()}, NewState :: term()}.
create(Entity, Owner, Group, #state{ proxy=Backend }=S) ->
    {Scheme, Term} = occi_entity:kind(Entity),
    ?info("[~p] create(~s~s)", [?MODULE, Scheme, Term]),
    Args = [ 
	     << Scheme/binary, Term/binary >>,
	     lists:foldr(fun ({MixinScheme, MixinTerm}, Acc) ->
				 [ << MixinScheme/binary, MixinTerm/binary >> | Acc ]
			 end, [], occi_entity:mixins(Entity)),
	     occi_entity:raw_attributes(Entity),
	     marshal_user(Owner),
	     marshal_user(Group)
	   ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Create2">>, Args) of
	{ok, {Location, KindId, MixinIds, Attributes, Serial}} ->
	    dbus_ok(KindId, MixinIds, Attributes,
		    fun (Entity2) ->
			    {ok, Location, Entity2, unmarshal_serial(Serial)}
		    end, S);
	{error, Err} ->
	    dbus_errors(Err, S)
    end.


-spec update(Location :: occi_uri:url(), Attributes :: maps:map(), State :: term()) ->
		    {{ok, Entity2 :: occi_entity:t(), erocci_node:serial()}
		     | {error, erocci_backend:error()}, NewState :: term()}.
update(Location, Attributes, #state{ proxy=Backend }=S) ->
    ?info("[~p] update(~s)", [?MODULE, Location]),
    Args = [ Location, Attributes ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Update">>, Args) of
	{ok, {KindId, MixinIds, Attributes, Serial}} ->
	    dbus_ok(KindId, MixinIds, Attributes, 
		    fun (Entity) ->
			    {ok, Entity, unmarshal_serial(Serial)}
		    end, S);
	{error, Err} ->
	    dbus_errors(Err, S)
    end.


-define(link_types, fun (source) -> 0; (target) -> 1 end).
-spec link(Location :: occi_uri:url(), 
	   Type :: source | target, LinkId :: occi_link:id(), State :: term()) ->
		  {ok | {error, erocci_backend:error()}, NewState :: term()}.
link(Location, Type, LinkId, #state{ proxy=Backend }=S) ->
    ?info("[~p] link(~s)", [?MODULE, Location]),
    Args = [ Location, ?link_types(Type), LinkId ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, Args) of
	ok ->
	    {ok, S};
	{error, Err} ->
	    dbus_errors(Err, S)
    end.


-spec action(Location :: occi_uri:url(), ActionId :: occi_action:id(), Attributes :: maps:map(), State :: term()) ->
		    {{ok, occi_entity:t(), erocci_node:serial()}
		     | {error, error_backend:error()}, NewState :: term()}.
action(Location, {ActionScheme, ActionTerm}, Attributes, #state{ proxy=Backend }=S) ->
    ?info("[~p] action(~s, ~s~s)", [?MODULE, Location, ActionScheme, ActionTerm]),
    Args = [ Location, << ActionScheme/binary, ActionTerm/binary >>, Attributes ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, Args) of
	{ok, {KindId, MixinIds, Attributes, Serial}} ->
	    dbus_ok(KindId, MixinIds, Attributes,
		    fun (Entity) ->
			    {ok, Entity, unmarshal_serial(Serial)}
		    end, S);
	{error, Err} ->
	    dbus_errors(Err, S)
    end.


-spec delete(Location :: binary(), State :: term()) ->
		    {ok | {error, erocci_backend:error()}, NewState :: term()}.
delete(Location, #state{ proxy=Backend }=S) ->
    ?info("[~p] delete(~s)", [?MODULE, Location]),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, [Location]) of
	ok ->
	    {ok, S};
	{error, Err} ->
	    dbus_errors(Err, S)
    end.


-spec mixin(Location :: occi_uri:url(), MixinId :: occi_mixin:t(), Attributes :: maps:map(), State :: term()) ->
		   {{ok, occi_entity:t(), erocci_node:serial()}
		    | {error, erocci_backend:error()}, NewState :: term()}.
mixin(Location, {Scheme, Term}, Attributes, #state{ proxy=Backend }=S) ->
    ?info("[~p] mixin(~s, ~s~s)", [?MODULE, Location, Scheme, Term]),
    Args = [ Location, << Scheme/binary, Term/binary >>, Attributes ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, Args) of
	{ok, {KindId, MixinIds, Attributes, Serial}} ->
	    dbus_ok(KindId, MixinIds, Attributes,
		    fun (Entity) ->
			    {ok, Entity, unmarshal_serial(Serial)}
		    end, S);
	{error, Err} ->
	    dbus_errors(Err, S)
    end.


-spec unmixin(Location :: occi_uri:url(), MixinId :: occi_mixin:t(), State :: term()) ->
		     {{ok, occi_entity:t(), erocci_node:serial()}
		      | {error, erocci_backend:error()}, NewState :: term()}.
unmixin(Location, {Scheme, Term}, #state{ proxy=Backend }=S) ->
    ?info("[~p] unmixin(~s. ~s~s)", [?MODULE, Scheme, Term]),
    Args = [ Location, << Scheme/binary, Term/binary >> ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, Args) of
	{ok, {KindId, MixinIds, Attributes, Serial}} ->
	    dbus_ok(KindId, MixinIds, Attributes,
		    fun (Entity) ->
			    {ok, Entity, unmarshal_serial(Serial)}
		    end, S);
	{error, Err} ->
	    dbus_errors(Err, S)
    end.


-spec collection(Id :: occi_category:id() | binary(),
		 Filter :: erocci_filter:t(),
		 Start :: integer(), Number :: integer() | undefined,
		 State :: term()) ->
			{{ok, [{occi_entity:t(), erocci_creds:user(), erocci_creds:group()}], erocci_node:serial()}
			 | {error, erocci_backend:error()}, NewState :: term()}.
collection({Scheme, Term}, Filter, Start, Number, S) ->
    collection(<< Scheme/binary, Term/binary >>, Filter, Start, Number, S);

collection(Id, Filter, Start, Number, #state{ proxy=Backend }=S) ->
    ?info("[~p] collection(~s)", [?MODULE, Id]),
    Args = [ Id, marshal_filter(Filter, []), Start, Number ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, Args) of
	{ok, Data} ->
	    case unmarshal_entities(Data, []) of
		{ok, Entities} ->
		    {{ok, Entities}, S};
		{error, _}=Err ->
		    Err
	    end;
	{error, Err} ->
	    dbus_errors(Err, S)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
parse_opts(Props) ->
    Srv = case proplists:get_value(service, Props) of
              undefined -> throw({error, {missing_opt, service}});
	      Str when is_atom(Str) -> atom_to_binary(Str, utf8);
	      Str when is_binary(Str) -> Str;
              Str when is_list(Str) -> list_to_binary(Str)
          end,
    Opts = proplists:get_value(opts, Props, []),
    {Srv, Opts}.


connect_backend(Service, Opts) ->
    case dbus_bus_connection:connect(session) of
        {ok, Bus} ->
            ?info("Initializing backend service: ~s~n", [Service]),
            case dbus_proxy:start_link(Bus, Service) of
                {ok, Backend} -> 
                    State = #state{conn=Bus, proxy=Backend},
                    init_service(State, Opts);
                {error, _} = Err -> Err
            end;
        {error, _} = Err -> Err
    end.


init_service(#state{ proxy=Backend }=State, Opts) ->
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Init">>, [Opts]) of
        ok ->
            ?debug("Initialization ok...", []),
            {ok, [], State};
        {ok, Msg} ->
            ?error("Invalid Init: ~p", [Msg]),
            {error, invalid_backend};
        {error, {Code, Err}} ->
            ?debug("Error initializing: ~n"
                   "Code=~s~n"
                   "Reason=~n~s~n", [Code, Err]),
            {error, Err}
    end.


-define(schema_type_xml, 0).
parse_models([], Acc, S) ->
    {{ok, lists:reverse(Acc)}, S};

parse_models([ {?schema_type_xml, Bin} | Schemas ], Acc, S) ->
    Ext = occi_rendering:parse(xml, Bin, occi_extension),
    parse_models(Schemas, [ Ext | Acc ], S);

parse_models([ {Type, _Bin} | _ ], _, S) ->
    {{error, {parse_error, {unsupported_format, Type}}}, S}.


unmarshal_entities([], Acc) ->
    lists:reverse(Acc);

unmarshal_entities([ {KindId, MixinIds, Attributes, Owner, Group, Serial} | Tail ], Acc) ->
    try unmarshal_entity(KindId, MixinIds, Attributes) of
	Entity ->
	    Entry = { Entity, unmarshal_user(Owner), unmarshal_user(Group), unmarshal_serial(Serial) },
	    unmarshal_entities(Tail, [ Entry | Acc ])
    catch throw:Err ->
	    {error, Err}
    end.
    

unmarshal_entity(KindId, MixinIds, Attributes) ->
    E0 = occi_entity:new(KindId),
    E1 = lists:foldl(fun (MixinId, Acc) ->
			     occi_entity:add_mixin(MixinId, Acc)
		     end, E0, MixinIds),
    occi_entity:set(Attributes, server, E1).


-define(filter_ops, fun (eq) -> 0; (like) -> 1 end).
marshal_filter([], Acc) ->
    lists:reverse(Acc);

marshal_filter([ {Op, '_', Value} | Filter ], Acc) ->
    marshal_filter(Filter, [ {?filter_ops(Op), <<>>, Value} | Acc ]);

marshal_filter([ {Op, Key, Value} | Filter ], Acc) ->
    marshal_filter(Filter, [ {?filter_ops(Op), Key, Value} | Acc ]).


unmarshal_serial(<<>>) ->
    undefined;

unmarshal_serial(Bin) ->
    Bin.


marshal_user(anonymous) -> <<>>;

marshal_user(User) when is_binary(User) -> User.


unmarshal_user(<<>>) -> anonymous;

unmarshal_user(User) when is_binary(User) -> User.


dbus_errors({?dbus_exception_notfound, _Msg}, S) ->
    {{error, not_found}, S};

dbus_errors({?dbus_exception_conflict, _Msg}, S) ->
    {{error, conflict}, S};

dbus_errors({Name, Msg}, S) ->
    ?debug("Unknown exception from backend: ~p", [Name]),
    ?debug("Exception: ~p", [Msg]),
    {{error, Name}, S};

dbus_errors(Else, S) ->
    {{error, Else}, S}.


dbus_ok(KindId, MixinIds, Attributes, Fun, S) ->
    try unmarshal_entity(KindId, MixinIds, Attributes) of
	Entity ->
	    {Fun(Entity), S}
    catch throw:Err ->
	    {{error, Err}, S}
    end.
