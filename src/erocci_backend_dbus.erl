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

-define(IFACE_BACKEND, <<"org.ow2.erocci.backend">>).
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

-record(state, {conn      :: dbus_connection(),
		proxy     :: dbus_proxy(),
		i_mixin   :: boolean(),
		i_action  :: boolean()}).

%%%===================================================================
%%% occi_backend callbacks
%%%===================================================================
init(#occi_backend{opts=Props}) ->
    try parse_opts(Props) of
	{Service, Opts} -> connect_backend(Service, Opts)
    catch throw:Err -> {error, Err}
    end.


terminate(#state{proxy=Backend}) ->
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"terminate">>, []) of
	_ -> ok
    end.

save(#state{proxy=Backend}=State, #occi_node{}=Node) ->
    ?info("[~p] save(~p)~n", [?MODULE, Node]),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"save">>, [occi_renderer_dbus:render(Node)]) of
	ok ->
	    {ok, State};
	{error, Err} ->
	    {{error, Err}, State}
    end.

delete(#state{proxy=Backend}=State, #occi_node{id=Uri}=Node) ->
    ?info("[~p] delete(~p)~n", [?MODULE, Node]),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"delete">>, [occi_uri:to_binary(Uri)]) of
	ok ->
	    {ok, State};
	{error, Err} ->
	    {{error, Err}, State}
    end.


update(#state{proxy=Backend}=State, #occi_node{}=Node) ->
    ?info("[~p] update(~p)~n", [?MODULE, Node]),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"update">>, [occi_renderer_dbus:render(Node)]) of
	ok ->
	    {ok, State};
	{error, Err} ->
	    {{error, Err}, State}
    end.


find(#state{proxy=Backend}=State, #occi_node{id=Uri}=_N) ->
    ?info("[~p] find(~p)~n", [?MODULE, _N]),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"find">>, [occi_uri:to_binary(Uri)]) of
	{ok, [Node]} ->
	    {{ok, [occi_parser_dbus:parse(Node)]}, State};
	{error, Err} ->
	    {{error, Err}, State}
    end.


load(#state{proxy=Backend}=State, #occi_node{id=Uri}=Node, _Opts) ->
    ?info("[~p] load(~p)~n", [?MODULE, Uri]),
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"load">>, [occi_renderer_dbus:render(Node)]) of
	{ok, N} ->
	    {{ok, occi_parser_dbus:parse(N)}, State};
	{error, Err} ->
	    {{error, Err}, State}
    end.


action(#state{proxy=Backend}=State, #uri{}=Id, #occi_action{}=A) ->
    ?info("[~p] action(~p, ~p)~n", [?MODULE, Id, A]),
    Args = [occi_renderer_dbus:render(Id), 
	    occi_renderer_dbus:render(A)],
    case dbus_proxy:call(Backend, ?IFACE_BACKEND, <<"load">>, Args) of
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
	    {ok, [Schema], 
	     State#state{
	       i_mixin=dbus_proxy:has_interface(Backend, ?IFACE_BACKEND_MIXIN),
	       i_action=dbus_proxy:has_interface(Backend, ?IFACE_BACKEND_ACTION)}};
	{error, {Code, Err}} ->
	    ?debug("Error initializing: ~n"
		   "Code=~s~n"
		   "Reason=~n~s~n", [Code, Err]),
	    {error, Err}
    end.
