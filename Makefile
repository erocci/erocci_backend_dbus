version = 0.1
PROJECT = erocci_backend_dbus
PROJECT_VERSION = $(shell git describe --always --tags 2> /dev/null || echo $(version))

DEPS = erocci_core dbus
TEST_DEPS = erocci_listener_http pocci

dep_erocci_core = git https://github.com/erocci/erocci_core.git master
dep_erocci_listener_http = git https://github.com/erocci/erocci_listener_http.git master
dep_dbus = git git://github.com/lizenn/erlang-dbus.git 0.6.1
dep_pocci = git https://github.com/jeanparpaillon/pOCCI.git v1.0.0+erocci1.2.0

include erlang.mk

POCCI_DATA = $(TEST_DIR)/dbus_python_SUITE_data/pocci.conf

clean:: clean-local

clean-local:
	rm -f $(POCCI_DATA)

test-build:: $(POCCI_DATA)

$(POCCI_DATA):
	@echo "{'POCCI', \"$(DEPS_DIR)/pocci/pOCCI/pOCCI.py\"}." > $@

test-report: tests
	cat logs/ct_run.*/*.logs/run.*/suite.log

.PHONY: clean-local 
