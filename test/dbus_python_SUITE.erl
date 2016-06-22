%%% @author Jean Parpaillon <jean.parpaillon@free.fr>
%%% @copyright (c) 2015-2016 Jean Parpaillon
%%% @doc Execute tests from pOCCI: https://github.com/CESNET/pOCCI
%%%
%%% @end
%%% Created :  20 June 2016 by Jean Parpaillon <jean.parpaillon@free.fr>

-module(dbus_python_SUITE).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(PORT, 9999).
-define(ENDPOINT, "http://localhost:9999").

suite() ->
	[{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
    application:load(erocci_core),

    application:set_env(erocci_core, listeners, 
                        [ {http, erocci_http, [{port, ?PORT}]} ]
					   ),
    BackendOpts = [{service, <<"org.ow2.erocci.backend.SampleService">>}],
    application:set_env(erocci_core, backends,
                        [{python, erocci_backend_dbus, BackendOpts, <<"/">>}]),
    application:set_env(erocci_core, acl,
                        [{allow, '_', '_', '_'}]),

    Config2 = start_dbus(Config),
    Config3 = start_service(Config2),
    timer:sleep(1000),
    
    application:ensure_all_started(erocci_core),
    pocci_config(Config3).

end_per_suite(Config) ->
    error_logger:delete_report_handler(cth_log_redirect),
    stop_cmd(?config(service, Config)),
    _ = stop_dbus(Config),
    application:stop(erocci_core),
    error_logger:add_report_handler(cth_log_redirect),
    ok.

all() ->
	[
	 'OCCI_CORE_DISCOVERY_001'
	,'OCCI_CORE_DISCOVERY_002'
	,'OCCI_CORE_CREATE_001'
	,'OCCI_CORE_CREATE_006'
	,'OCCI_CORE_READ_001'
	,'OCCI_CORE_READ_002'
	,'OCCI_CORE_READ_007'
	,'OCCI_CORE_DELETE_001'
	,'OCCI_CORE_UPDATE_001'
	,'OCCI_CORE_MISC_001'

	,'OCCI_INFRA_CREATE_001'
	,'OCCI_INFRA_CREATE_002'
	,'OCCI_INFRA_CREATE_003'
	,'OCCI_INFRA_CREATE_004'
	,'OCCI_INFRA_CREATE_005'
	,'OCCI_INFRA_CREATE_006'
	,'OCCI_INFRA_CREATE_007'
	].



'OCCI_CORE_DISCOVERY_001'(Config) ->
	Cmd = pocci("OCCI/CORE/DISCOVERY/001", Config),
	?assertCmdOutput("OCCI/CORE/DISCOVERY/001  OK\n", Cmd).


'OCCI_CORE_DISCOVERY_002'(Config) ->
	Cmd = pocci("OCCI/CORE/DISCOVERY/002", Config),
	?assertCmdOutput("OCCI/CORE/DISCOVERY/002  OK\n", Cmd).


'OCCI_CORE_READ_001'(Config) ->
	Cmd = pocci("OCCI/CORE/READ/001", Config),
	?assertCmdOutput("OCCI/CORE/READ/001  OK\n", Cmd).


'OCCI_CORE_READ_002'(Config) ->
	Cmd = pocci("OCCI/CORE/READ/002", Config),
	?assertCmdOutput("OCCI/CORE/READ/002  OK\n", Cmd).


'OCCI_CORE_READ_007'(Config) ->
	Cmd = pocci("OCCI/CORE/READ/007", Config),
	?assertCmdOutput("OCCI/CORE/READ/007  OK\n", Cmd).


'OCCI_CORE_CREATE_001'(Config) ->
	Cmd = pocci("OCCI/CORE/CREATE/001", Config),
	?assertCmdOutput("OCCI/CORE/CREATE/001  OK\n", Cmd).


'OCCI_CORE_CREATE_006'(Config) ->
	Cmd = pocci("OCCI/CORE/CREATE/006", Config),
	?assertCmdOutput("OCCI/CORE/CREATE/006  OK\n", Cmd).


'OCCI_CORE_DELETE_001'(Config) ->
	Cmd = pocci("OCCI/CORE/DELETE/001", Config),
	?assertCmdOutput("OCCI/CORE/DELETE/001  OK\n", Cmd).


'OCCI_CORE_UPDATE_001'(Config) ->
	Cmd = pocci("OCCI/CORE/UPDATE/001", Config),
	?assertCmdOutput("OCCI/CORE/UPDATE/001  OK\n", Cmd).


'OCCI_CORE_MISC_001'(Config) ->
	Cmd = pocci("OCCI/CORE/MISC/001", Config),
	?assertCmdOutput("OCCI/CORE/MISC/001  OK\n", Cmd).


'OCCI_INFRA_CREATE_001'(Config) ->
	Cmd = pocci("OCCI/INFRA/CREATE/001", Config),
	?assertCmdOutput("OCCI/INFRA/CREATE/001  OK\n", Cmd).


'OCCI_INFRA_CREATE_002'(Config) ->
	Cmd = pocci("OCCI/INFRA/CREATE/002", Config),
	?assertCmdOutput("OCCI/INFRA/CREATE/002  OK\n", Cmd).


'OCCI_INFRA_CREATE_003'(Config) ->
	Cmd = pocci("OCCI/INFRA/CREATE/003", Config),
	?assertCmdOutput("OCCI/INFRA/CREATE/003  OK\n", Cmd).


'OCCI_INFRA_CREATE_004'(Config) ->
	Cmd = pocci("OCCI/INFRA/CREATE/004", Config),
	?assertCmdOutput("OCCI/INFRA/CREATE/004  OK\n", Cmd).


'OCCI_INFRA_CREATE_005'(Config) ->
	Cmd = pocci("OCCI/INFRA/CREATE/005", Config),
	Out = string:tokens(os:cmd(Cmd), "\n"),
	?assertMatch([_, _, "OCCI/INFRA/CREATE/005  OK"], Out).


'OCCI_INFRA_CREATE_006'(Config) ->
	Cmd = pocci("OCCI/INFRA/CREATE/006", Config),
	Out = string:tokens(os:cmd(Cmd), "\n"),
	?assertMatch(["OCCI/INFRA/CREATE/006  OK"], Out).


'OCCI_INFRA_CREATE_007'(Config) ->
	Cmd = pocci("OCCI/INFRA/CREATE/007", Config),
	Out = string:tokens(os:cmd(Cmd), "\n"),
	?assertMatch(["OCCI/INFRA/CREATE/007  OK"], Out).

%%%
%%% Priv
%%%
get_data_path(Path, Config) ->
    DataDir = ?config(data_dir, Config),
    filename:join([DataDir, Path]).

pocci(Name, Config) ->
	Cmd = ?config(pocci, Config) 
		++ " --url " ++ ?ENDPOINT
		++ " --mime-type 'text/plain'"
		++ " --format plain"
		++ " --auth-type ''"
		++ " --tests " ++ Name,
	ct:log(info, "cmd = ~s", [Cmd]),
	Cmd.


pocci_config(Config) ->
    PocciConfig = filename:join([?config(data_dir, Config), "pocci.conf"]),
    {ok, [{'POCCI', Path}]} = file:consult(PocciConfig),
    [ {pocci, Path} | Config ].


start_dbus(Config) ->
    DbusConfig = filename:join([?config(data_dir, Config), "dbus.config"]),
    Port = start_cmd("dbus-daemon --config-file=" ++ DbusConfig),
    DBusEnv = "unix:path=/tmp/dbus-test",
    os:putenv("DBUS_SESSION_BUS_ADDRESS", DBusEnv),
    [ {dbus, Port}, {dbus_env, DBusEnv} | Config ].


stop_dbus(Config) ->
    stop_cmd(?config(dbus, Config)),
    proplists:delete(dbus_env, 
		     proplists:delete(dbus, Config)).


start_service(Config) ->
    Service = filename:join([?config(data_dir, Config), "erocci_sample_backend.py"]),
    ServicePort = start_cmd(Service),
    [ {service, ServicePort} | Config ].


start_cmd(Cmd) ->
    spawn(?MODULE, init_cmd, [Cmd]).

init_cmd(Cmd) ->
    ct:log(info, "open_port = ~s", [Cmd]),
    Port = erlang:open_port({spawn, Cmd}, [exit_status]),
    loop(Port, Cmd).


loop(Port, Cmd) ->
    receive
	{Port, {exit_status, Status}} ->
	    throw({command_error, {Cmd, Status}});
	{command, exit} ->
	    {os_pid, Pid} = erlang:port_info(Port, os_pid),
	    erlang:port_close(Port),
	    os:cmd(io_lib:format("kill -9 ~p", [Pid]));
	{command, From, pid} ->
	    {os_pid, Pid} = erlang:port_info(Port, os_pid),
	    From ! {self(), Pid},
	    loop(Port, Cmd);
	_ ->
	    loop(Port, Cmd)
    end.


stop_cmd(Pid) ->
    Pid ! {command, exit}.
