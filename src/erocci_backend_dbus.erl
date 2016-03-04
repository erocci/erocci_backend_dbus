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
    Id = occi_uri:to_binary(occi_node:id(Node)),
    Kind = occi_cid:to_binary(occi_resource:get_cid(Res)),
    Mixins = [ occi_cid:to_binary(Cid) || Cid <- sets:to_list(occi_resource:get_mixins(Res))],
    Attrs = [ render_attribute(A)
              || A <- occi_resource:get_attributes(Res), occi_attribute:value(A) =/= undefined ],
    Owner = iolist_to_binary(io_lib:format("~p", [occi_node:owner(Node)])),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"SaveResource">>, 
                         [Id, Kind, Mixins, Attrs, Owner]) of
        {ok, Id} -> 
            {ok, State};
        {ok, _Ret} ->
            ?error("Backend invalid answer: ~p", [_Ret]),
            {{error, backend_error}, State};
        {error, Err} -> {{error, Err}, State}
    end;

save(#state{proxy=Backend}=State, #occi_node{type=occi_link, data=Link}=Node) ->
    ?info("[~p] save(~p)~n", [?MODULE, Node]),
    Id = occi_uri:to_binary(occi_node:id(Node)),
    Kind = occi_cid:to_binary(occi_link:get_cid(Link)),
    Mixins = [ occi_cid:to_binary(Cid) || Cid <- sets:to_list(occi_link:get_mixins(Link))],
    Src = occi_uri:to_binary(occi_link:get_source(Link)),
    Target = occi_uri:to_binary(occi_link:get_target(Link)),
    Attrs = [ render_attribute(A)
              || A <- occi_link:get_attributes(Link) ],
    Owner = iolist_to_binary(io_lib:format("~p", [occi_node:owner(Node)])),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"SaveLink">>, 
                         [Id, Kind, Mixins, Src, Target, Attrs, Owner]) of
        {ok, Id} -> 
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
    case dbus_proxy:call(Backend, ?IFACE_BACKEND_MIXIN, <<"DelMixin">>, [Id]) of
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
    case dbus_proxy:call(Backend, ?IFACE_BACKEND_MIXIN, <<"AddMixin">>, [Id, Location, Owner]) of
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
    #uri{path=Id} = occi_node:id(Node),
    Attrs = [ render_attribute(A)
              || A <- 
                     case Entity of
                         #occi_resource{} -> occi_resource:get_attributes(Entity);
                         #occi_link{} -> occi_link:get_attributes(Entity)
                     end ],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Update">>, [Id, Attrs]) of
        {ok, _Ret} ->
            {ok, State};
        ok ->
            ?error("Backend must return attributes~n", []),
            {{error, backend_error}, State};
        {error, Err} ->
            {{error, Err}, State}
    end.


find(#state{proxy=Backend}=State, Node) ->
    ?info("[~p] find(~p)~n", [?MODULE, Node]),
    case occi_node:type(Node) of
        capabilities ->
            find_user_mixins(State, Node);
        _ ->
            Id = occi_node:id(Node),
            case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Find">>, [occi_uri:to_binary(Id)]) of
                {ok, []} ->
                    {{ok, []}, State};
                {ok, [{?N_ENTITY, OpaqueId, Owner, Serial}]} ->
                    Res = #occi_node{id=Id, objid=OpaqueId, owner=Owner, etag=Serial, type=occi_entity},
                    {{ok, [Res]}, State};
                {ok, [{?N_UNBOUNDED, _, _, _}]} ->
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


find_unbounded(#state{proxy=Backend}=State, Node) ->
    Id = occi_node:id(Node),
    Filters = [],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"List">>, [occi_uri:to_binary(Id), Filters]) of
        {ok, {ColId, Serial}} ->
            ?debug("Set collection id: ~p", [ColId]),
            Res = #occi_node{id=Id, objid=ColId, etag=Serial, type=occi_collection},
            {{ok, [Res]}, State};
        {error, Err} ->
            {{error, Err}, State}
    end.


load(#state{proxy=Backend}=State, Node, Opts) ->
    ?info("[~p] load(~p)~n", [?MODULE, occi_node:objid(Node)]),
    case occi_node:type(Node) of
        occi_collection ->
            case occi_node:objid(Node) of
                #occi_cid{} ->
                    list_bounded(State, Node, Opts);
                _ ->
                    next(State, Node, Opts)
            end;
        capabilities ->
            {{ok, [Node]}, State};
        _ ->
            Id = occi_node:objid(Node),
            case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Load">>, [Id]) of
                {ok, {_Id, Kind, Mixins, Attributes}} ->
                    Entity = occi_entity:new(occi_node:id(Node), Kind, Mixins, parse_attributes(Attributes)),
                    {{ok, occi_node:data(Node, Entity)}, State};
                {error, Err} ->
                    {{error, Err}, State}
            end
    end.


list_bounded(#state{proxy=Backend}=State, Node, Opts) ->
    Cid = occi_node:objid(Node),
    Filters = [],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"List">>, [occi_cid:to_binary(Cid), Filters]) of
        {ok, {ColId, Serial}} ->
            Res = #occi_node{id=occi_node:id(Node), objid=ColId, etag=Serial, type=occi_collection, 
                             data=occi_collection:new(Cid)},
            next(State, Res, Opts);
        {error, Err} ->
            {{error, Err}, State}
    end.    


next(#state{proxy=Backend}=State, Node, _Opts) ->
    It = occi_node:objid(Node),
    Start = 0,
    NrItems = 0,
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"Next">>, [It, Start, NrItems]) of
        {ok, Items} ->
            Entities = [ occi_uri:parse(Item) || {Item, _Owner} <- Items ],
            Col = case Node#occi_node.data of
                      undefined ->
                          occi_collection:new(occi_node:id(Node), Entities);
                      #occi_collection{}=C ->
                          occi_collection:add_entities(C, Entities)
                  end,
            Res = Node#occi_node{data=Col},
            {{ok, Res}, State};
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
        {ok, Msg} ->
            ?error("Invalid Init: ~p", [Msg]),
            {error, invalid_backend};
        {error, {Code, Err}} ->
            ?debug("Error initializing: ~n"
                   "Code=~s~n"
                   "Reason=~n~s~n", [Code, Err]),
            {error, Err}
    end.


render_attribute(A) ->
    Key = case occi_attribute:id(A) of
              K when is_atom(K) -> atom_to_binary(K, utf8);
              K -> K
          end,
    { Key, render_value(occi_attribute:value(A))}.


render_value(V) when is_reference(V) ->
    erlang:phash2(V);

render_value(#uri{path=V}) ->
    V;

render_value(V) ->
    V.


parse_attributes(Attrs) ->
    parse_attributes(Attrs, []).

parse_attributes([], Acc) ->
    lists:reverse(Acc);
parse_attributes([ {Key, #dbus_variant{value=Value} } | Tail ], Acc) ->
    parse_attributes(Tail, [ {?attr_to_atom(Key), Value} | Acc]).
