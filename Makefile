version = 0.1
PROJECT = erocci_backend_dbus
PROJECT_VERSION = $(shell git describe --always --tags 2> /dev/null || echo $(version))

DEPS = erocci_core dbus
dep_erocci_core = git https://github.com/erocci/erocci_core.git master
dep_dbus = git git://github.com/lizenn/erlang-dbus.git 0.5.1

include erlang.mk

fetch: $(ALL_DEPS_DIRS)
	@for d in $(ALL_DEPS_DIRS); do \
	  $(MAKE) --no-print-directory -C $$d $@ || true; \
	done

.PHONY: fetch
