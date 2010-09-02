TARGETS = $(BEAMS) $(TEST_BEAMS) $(APP)

APP_NAME=mio
VERSION=0.0.1-alpha

SOURCE_DIR=src
EXT_SOURCE_DIR=$(SOURCE_DIR)/ext
TEST_DIR=test
EBIN_DIR=ebin
INCLUDE_DIR=include
LOG_PREFIX=mio.log
TEST_SOURCES= test/mio_bucket_tests.erl test/mio_skip_graph_tests.erl test/global_tests.erl test/mio_tests.erl test/mio_mvector_tests.erl test/mio_store_tests.erl test/mio_local_store_tests.erl test/mio_multiple_nodes_tests.erl test/mio_coverage_tests.erl
TEST_BEAMS=$(patsubst $(TEST_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(TEST_SOURCES))
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
EXT_SOURCES=$(wildcard $(EXT_SOURCE_DIR)/*.erl)
BEAMS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES)) $(patsubst $(EXT_SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(EXT_SOURCES))
APP=$(EBIN_DIR)/mio.app
ERLC_FLAGS=+warn_unused_vars \
           +warn_unused_import \
           +warn_shadow_vars \
           -Wall \
           -W \
           -v \
           +debug_info \
           +bin_opt_info \
           +no_strict_record_tests \
           +native +"{hipe, [o3]}"

TARBALL_NAME=$(APP_NAME)-$(VERSION)
DIST_TMP_DIR=tmp
DIST_TARGET=$(DIST_TMP_DIR)/$(TARBALL_NAME)

ERL_CALL=erl_call -c mio -name mio1@suneo.local -e

all: $(TARGETS)

$(APP): $(SOURCE_DIR)/mio.app
	cp -p $< $@

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(SOURCE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/%.beam: $(EXT_SOURCE_DIR)/%.erl $(SOURCE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<


$(EBIN_DIR)/mio_bucket_tests.beam: $(TEST_DIR)/mio_bucket_tests.erl $(SOURCE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/global_tests.beam: $(TEST_DIR)/global_tests.erl $(SOURCE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_tests.beam: $(TEST_DIR)/mio_tests.erl $(SOURCE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_mvector_tests.beam: $(TEST_DIR)/mio_mvector_tests.erl $(SOURCE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_store_tests.beam: $(TEST_DIR)/mio_store_tests.erl $(SOURCE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_local_store_tests.beam: $(TEST_DIR)/mio_local_store_tests.erl $(SOURCE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_skip_graph_tests.beam: $(TEST_DIR)/mio_skip_graph_tests.erl $(SOURCE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_multiple_nodes_tests.beam: $(TEST_DIR)/mio_multiple_nodes_tests.erl $(SOURCE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_coverage_tests.beam: $(TEST_DIR)/mio_coverage_tests.erl $(SOURCE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<



VERBOSE_TEST ?= false

check: all
	@(pgrep -f dialyzer || true) > /tmp/dialyzer.pid
	@((cat /tmp/dialyzer.pid | xargs kill) || true) > /dev/null
	@cd src && ./ext/run_tests.escript ../ebin/ | gor
	@./test/two-nodes.sh |gor
	@google-chrome ./_test/cover/index.html 1>/dev/null
	@$(MAKE) check-error& # quick quick

check_one: all
	@erl -pa `pwd`/ebin -eval 'eunit:test([$(TEST_NAME)_tests]).' -s init stop | gor

vcheck: all
	VERBOSE_TEST=true make check

test: check

install: all install_dirs
	cp -rp ebin $(TARGET_DIR)
	for script in mio mioctl mio-env; do \
		chmod 0755 scripts/$$scripts; \
		cp -p scripts/$$script $(TARGET_DIR)/sbin; \
		[ -e $(SBIN_DIR)/$$script ] || ln -s $(TARGET_DIR)/sbin/$$script $(SBIN_DIR)/$$script; \
	done

install_dirs:
	@[ -n "$(TARGET_DIR)" ] || (echo "Please set TARGET_DIR. e.g. /usr/local/mio"; false)
	@[ -n "$(SBIN_DIR)" ] || (echo "Please set SBIN_DIR. e.g. /usr/sbin/"; false)
	mkdir -p $(SBIN_DIR)
	mkdir -p $(TARGET_DIR)/sbin

dist: distclean
	mkdir -p $(DIST_TARGET)
	cp -r Makefile ebin src example scripts README.md test $(DIST_TARGET)
	chmod 0755 $(DIST_TARGET)/scripts/*
	(cd $(DIST_TMP_DIR) &&	  tar -zcf ../$(TARBALL_NAME).tar.gz $(TARBALL_NAME))
	rm -rf $(DIST_TMP_DIR)

distclean: clean
	rm -f $(LOG_PREFIX)*
	rm -rf src/mio.log.*
	rm -rf src/Mnesia.*
	find . -regex '.*\(~\|#\|\.swp\|\.dump\)' -exec rm {} \;

clean:
	rm -f $(TARGETS) $(TARBALL_NAME).tar.gz
	rm -f test/*.beam
	rm -rf log/ct_run*
	rm -rf log/mio.log.*
	rm -rf mio.log.*

# don't name this target as "dialyzer", which will cause been kill by pkill.
check-error: all
	dialyzer -Wno_return -I $(INCLUDE_DIR) -c $(EBIN_DIR)

create_plt:
	dialyzer --build_plt \
-r /usr/local/lib/erlang/lib/kernel-2.14/ebin \
-r /usr/local/lib/erlang/lib/memcached-client-0.0.1/ebin \
-r /usr/local/lib/erlang/lib/mnesia-4.4.14/ebin \
-r /usr/local/lib/erlang/lib/os_mon-2.2.5/ebin \
-r /usr/local/lib/erlang/lib/stdlib-1.17/ebin
