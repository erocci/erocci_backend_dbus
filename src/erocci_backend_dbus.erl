%%% @author Jean Parpaillon <jean.parpaillon@free.fr>
%%% @copyright (C) 2013, Jean Parpaillon
%%% 
%%% This file is provided to you under the Apache License,
%%% Version 2.0 (the "License"); you may not use this file
%%% except in compliance with the License.  You may obtain
%%% a copy of the License at
%%% 
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%% 
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.
%%% 
%%% @doc
%%%
%%% @end
%%% Created :  31 Jul 2014 by Jean Parpaillon <jean.parpaillon@free.fr>
-module(erocci_backend_dbus).

-behaviour(occi_backend).

-include_lib("erocci_core/include/occi.hrl").
-include_lib("erocci_core/include/occi_log.hrl").
-include_lib("dbus/include/dbus_client.hrl").

-include("occi_dbus.hrl").

-define(IFACE_BACKEND, <<"org.ow2.erocci.backend.core">>).
-define(IFACE_BACKEND_MIXIN, <<"org.ow2.erocci.backend.mixin">>).
-define(IFACE_BACKEND_ACTION, <<"org.ow2.erocci.backend.action">>).

%% occi_backend callbacks
-export([init/1,
         terminate/1]).
-export([update/2,
         save/2,
         delete/2,
         find/2,
         load/3,
         action/3]).

-record(state, {conn       :: dbus_connection(),
                proxy      :: dbus_proxy(),
                i_mixin    :: boolean(),
                i_action   :: boolean()}).

%%%===================================================================
%%% occi_backend callbacks
%%%===================================================================
init(#occi_backend{opts=Props}) ->
    try parse_opts(Props) of
        {Service, Opts} -> connect_backend(Service, Opts)
    catch throw:Err -> {error, Err}
    end.


terminate(#state{proxy=Backend}) ->
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Terminate">>, []) of
        _ -> ok
    end.


save(#state{proxy=Backend}=State, #occi_node{type=occi_resource, data=Res}=Node) ->
    ?info("[~p] save(~p)~n", [?MODULE, Node]),
    Kind = occi_cid:to_binary(occi_resource:get_cid(Res)),
    Mixins = [ occi_cid:to_binary(Cid) || Cid <- sets:to_list(occi_resource:get_mixins(Res))],
    Attrs = [ { occi_attribute:get_id(A), occi_attribute:get_value(A)} 
              || A <- occi_resource:get_attributes(Res) ],
    Owner = io_lib:format("~p", [occi_node:owner(Node)]),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"SaveResource">>, 
                         [Kind, Mixins, Attrs, Owner]) of
        ok -> 
            {ok, State};
        {ok, _Ret} ->
            ?error("Backend invalid answer: ~p", [_Ret]),
            {{error, backend_error}, State};
        {error, Err} -> {{error, Err}, State}
    end;

save(#state{proxy=Backend}=State, #occi_node{type=occi_link, data=Link}=Node) ->
    ?info("[~p] save(~p)~n", [?MODULE, Node]),
    Kind = occi_cid:to_binary(occi_resource:get_cid(Link)),
    Mixins = [ occi_cid:to_binary(Cid) || Cid <- sets:to_list(occi_link:get_mixins(Link))],
    Src = occi_uri:to_binary(occi_link:get_source(Link)),
    Target = occi_uri:to_binary(occi_link:get_target(Link)),
    Attrs = [ { occi_attribute:get_id(A), occi_attribute:get_value(A)} 
              || A <- occi_link:get_attributes(Link) ],
    Owner = io_lib:format("~p", [occi_node:owner(Node)]),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"SaveLink">>, 
                         [Kind, Mixins, Src, Target, Attrs, Owner]) of
        ok -> 
            {ok, State};
        {ok, _Ret} ->
            ?error("Backend invalid answer: ~p", [_Ret]),
            {{error, backend_error}, State};
        {error, Err} -> 
            {{error, Err}, State}
    end;

save(#state{proxy=Backend}=State, #occi_node{type=occi_collection, data=Coll}=Node) ->
    ?info("[~p] save(~p)~n", [?MODULE, Node]),
    Id = occi_cid:to_binary(occi_collection:id(Coll)),
    Entities = [ occi_uri:to_binary(E) || E <- occi_collection:entities(Coll) ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"SaveMixin">>, [Id, Entities]) of
        ok -> 
            {ok, State};
        {ok, _Ret} ->
            ?error("Backend invalid answer: ~p", [_Ret]),
            {{error, backend_error}, State};
        {error, Err} -> 
            {{error, Err}, State}
    end.


delete(#state{i_mixin=false}=State, #occi_node{type=capabilities}) ->
    % TODO: handle this in occi_store: do not query backend in case it does not support and return sthig like 403
    {ok, State};

delete(#state{proxy=Backend}=State, #occi_node{type=capabilities, data=Mixin}=Node) ->
    ?info("[~p] delete(~p)~n", [?MODULE, Node]),
    Id = occi_cid:to_binary(occi_mixin:id(Mixin)),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"DelMixin">>, [Id]) of
        ok ->
            {ok, State};
        {ok, _Ret} ->
            ?error("Backend invalid answer: ~p", [_Ret]),
            {{error, backend_error}, State};
        {error, Err} ->
            {{error, Err}, State}
    end;

delete(#state{proxy=Backend}=State, #occi_node{id=Uri}=Node) ->
    ?info("[~p] delete(~p)~n", [?MODULE, Node]),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Delete">>, [occi_uri:to_binary(Uri)]) of
        ok ->
            {ok, State};
        {ok, _Ret} ->
            ?error("Backend invalid answer: ~p", [_Ret]),
            {{error, backend_error}, State};
        {error, Err} ->
            {{error, Err}, State}
    end.


update(#state{i_mixin=false}=State, #occi_node{type=capabilities}) ->
    % TODO: handle this in occi_store: do not query backend in case it does not support and return sthig like 403
    {ok, State};

update(#state{proxy=Backend}=State, #occi_node{type=capabilities, data=Mixin}=Node) ->
    ?info("[~p] update(~p)~n", [?MODULE, Node]),
    Id = occi_cid:to_binary(occi_mixin:id(Mixin)),
    Location = occi_uri:to_binary(occi_node:id(Node)),
    Owner = io_lib:format("~p", [occi_node:owner(Node)]),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"AddMixin">>, [Id, Location, Owner]) of
        ok ->
            {ok, State};
        {ok, _Ret} ->
            ?error("Backend invalid answer: ~p", [_Ret]),
            {{error, backend_error}, State};
        {error, Err} ->
            {{error, Err}, State}
    end;    
update(#state{proxy=Backend}=State, #occi_node{type=occi_collection, data=Coll}=Node) ->
    ?info("[~p] update(~p)~n", [?MODULE, Node]),
    Id = occi_cid:to_binary(occi_collection:id(Coll)),
    Entities = [ occi_uri:to_binary(E) || E <- occi_collection:entities(Coll) ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"UpdateMixin">>, [Id, Entities]) of
        ok ->
            {ok, State};
        {ok, _Ret} ->
            ?error("Backend invalid answer: ~p", [_Ret]),
            {{error, backend_error}, State};
        {error, Err} ->
            {{error, Err}, State}
    end;    

update(#state{proxy=Backend}=State, #occi_node{data=Entity}=Node) ->
    ?info("[~p] update(~p)~n", [?MODULE, Node]),
    Id = occi_node:id(Node),
    Attrs = [ { occi_attribute:get_id(A), occi_attribute:get_value(A)} 
              || A <- 
                     case Entity of
                         #occi_resource{} -> occi_resource:get_attributes(Entity);
                         #occi_link{} -> occi_link:get_attributes(Entity)
                     end ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Update">>, [Id, Attrs]) of
        ok ->
            {ok, State};
        {ok, _Ret} ->
            ?error("Backend invalid answer: ~p", [_Ret]),
            {{error, backend_error}, State};
        {error, Err} ->
            {{error, Err}, State}
    end.


find(#state{proxy=Backend}=State, Node) ->
    ?info("[~p] find(~p)~n", [?MODULE, Node]),
    case occi_node:type(Node) of
        capabilities ->
			find_user_mixins(State, Node);
        occi_collection ->
            find_bounded(State, Node);
        _ ->
            Id = occi_node:id(Node),
            case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Find">>, [occi_uri:to_binary(Id)]) of
                {ok, []} ->
                    {{ok, []}, State};
                {ok, [?N_ENTITY, OpaqueId, Owner, Serial]} ->
                    Res = #occi_node{id=Id, objid=OpaqueId, owner=Owner, etag=Serial, type=occi_entity},
                    {{ok, [Res]}, State};
                {ok, [?N_UNBOUNDED, _, _, _]} ->
                    find_unbounded(State, Node);
                {ok, _Res} ->
                    ?error("Backend invalid answer: ~p", [_Res]),
                    {{error, backend_error}, State};
                {error, Err} ->
                    {{error, Err}, State}
            end
    end.


find_user_mixins(#state{i_mixin=false}=State, Node) ->
	{{ok, [Node#occi_node{data={[], [], []}}]}, State};

find_user_mixins(#state{proxy=_Backend}=State, _Node) ->
	{{ok, [#occi_node{data={[], [], []}}]}, State}.


find_bounded(#state{proxy=Backend}=State, Node) ->
	Cid = occi_node:objid(Node),
	case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"List">>, [occi_cid:to_binary(Cid)]) of
		{ok, {#dbus_variant{value=ColId}, Serial}} ->
			Res = #occi_node{id=occi_node:id(Node), objid=ColId, etag=Serial, type=occi_collection},
			{{ok, [Res]}, State};
		{error, Err} ->
			{{error, Err}, State}
	end.


find_unbounded(#state{proxy=Backend}=State, Node) ->
	Id = occi_node:objid(Node),
	case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"List">>, [occi_uri:to_binary(Id)]) of
		{ok, {#dbus_variant{value=ColId}, Serial}} ->
			Res = #occi_node{id=occi_node:id(Node), objid=ColId, etag=Serial, type=occi_collection},
			{{ok, [Res]}, State};
		{error, Err} ->
			{{error, Err}, State}
	end.


load(#state{proxy=Backend}=State, Node, Opts) ->
    ?info("[~p] load(~p)~n", [?MODULE, occi_node:objid(Node)]),
    case occi_node:type(Node) of
        occi_collection ->
            next(State, Node, Opts);
		capabilities ->
			{{ok, [Node]}, State};
        _ ->
            Id = occi_node:objid(Node),
            case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Load">>, [Id]) of
                {ok, {Id, Kind, Mixins, Attributes}} ->
                    Entity = occi_entity:new(occi_uri:parse(Id), Kind, Mixins, Attributes),
                    {{ok, occi_node:data(Node, Entity)}, State};
                {error, Err} ->
                    {{error, Err}, State}
            end
    end.


next(#state{proxy=Backend}=State, Node, _Opts) ->
	It = occi_node:objid(Node),
	Start = 0,
	Items = 0,
	case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Next">>, [It, Start, Items]) of
		{ok, Items} ->
			Col = occi_collection:new(occi_node:id(Node), [ occi_uri:parse(Item) || Item <- Items ]),
			Res = Node#occi_node{data=Col},
			{{ok, [Res]}, State};
		{error, Err} ->
			{{error, Err}, State}
	end.


action(#state{i_action=false}=State, #uri{}, #occi_action{}) ->
    % TODO: handle this in occi_store: do not query backend in case it does not 
    % support and return sthig like 403
    {ok, State};

action(#state{proxy=Backend}=State, #uri{}=Id, #occi_action{}=Action) ->
    ?info("[~p] action(~p, ~p)~n", [?MODULE, Id, Action]),
    EntityId = occi_uri:to_binary(Id), 
    ActionId = occi_cid:to_binary(occi_action:id(Action)),
    Attrs = [ { occi_attribute:get_id(A), occi_attribute:get_value(A)} 
              || A <- occi_action:get_attr_list(Action) ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Action">>, [EntityId, ActionId, Attrs]) of
        ok ->
            {ok, State};
        {error, Err} ->
            {{error, Err}, State}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
parse_opts(Props) ->
    Srv = case proplists:get_value(service, Props) of
              undefined -> throw({error, {missing_opt, service}});
              Str -> list_to_binary(Str)
          end,
    Opts = case proplists:get_value(opts, Props) of
               undefined -> [];
               V -> V
           end,
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


init_service(#state{proxy=Backend}=State, Opts) ->
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Init">>, [Opts]) of
        ok ->
            ?debug("Initialization ok...", []),
            Schema = dbus_properties_proxy:get(Backend, ?IFACE_BACKEND, 'schema'),
            {ok, [{schemas, [Schema]}], 
             State#state{
               i_mixin=dbus_proxy:has_interface(Backend, ?IFACE_BACKEND_MIXIN),
               i_action=dbus_proxy:has_interface(Backend, ?IFACE_BACKEND_ACTION)}};
        {error, {Code, Err}} ->
            ?debug("Error initializing: ~n"
                   "Code=~s~n"
                   "Reason=~n~s~n", [Code, Err]),
            {error, Err}
    end.
