TARGETS = $(BEAMS) $(TEST_BEAMS) $(APP)

APP_NAME=mio
VERSION=0.0.1

SOURCE_DIR=src
TEST_DIR=test
EBIN_DIR=ebin
INCLUDE_DIR=include
LOG_PREFIX=mio.log
TEST_SOURCES= test/mio_bucket_tests.erl test/mio_skip_graph_tests.erl test/global_tests.erl test/mio_tests.erl test/mio_lock_tests.erl test/mio_mvector_tests.erl test/mio_store_tests.erl test/mio_node_tests.erl
TEST_BEAMS=$(patsubst $(TEST_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(TEST_SOURCES))
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
BEAMS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))
APP=$(EBIN_DIR)/mio.app
ERLC_FLAGS=+warn_unused_vars \
           +warn_unused_import \
           +warn_shadow_vars \
           -Wall \
           -W \
           -v \
           +debug_info \
           +bin_opt_info \
           +no_strict_record_tests

# +native +"{hipe, [o3]}" \

TARBALL_NAME=$(APP_NAME)-$(VERSION)
DIST_TMP_DIR=tmp
DIST_TARGET=$(DIST_TMP_DIR)/$(TARBALL_NAME)

ERL_CALL=erl_call -c mio -name mio1@suneo.local -e

all: $(TARGETS)

$(APP): $(SOURCE_DIR)/mio.app
	cp -p $< $@

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_bucket_tests.beam: $(TEST_DIR)/mio_bucket_tests.erl $(INCLUDE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/global_tests.beam: $(TEST_DIR)/global_tests.erl $(INCLUDE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_tests.beam: $(TEST_DIR)/mio_tests.erl $(INCLUDE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_lock_tests.beam: $(TEST_DIR)/mio_lock_tests.erl $(INCLUDE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_mvector_tests.beam: $(TEST_DIR)/mio_mvector_tests.erl $(INCLUDE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_store_tests.beam: $(TEST_DIR)/mio_store_tests.erl $(INCLUDE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_node_tests.beam: $(TEST_DIR)/mio_node_tests.erl $(INCLUDE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

$(EBIN_DIR)/mio_skip_graph_tests.beam: $(TEST_DIR)/mio_skip_graph_tests.erl $(INCLUDE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<


VERBOSE_TEST ?= false

check: all
# include option for ct:run_test is not recognized.
#	@erl -pa `pwd`/ebin -eval 'ct:run_test([{auto_compile, true}, {dir, "./test"}, {logdir, "./log"}, {refresh_logs, "./log"}, {cover, "./src/mio.coverspec"}]).' -s init stop -mio verbose $(VERBOSE_TEST) log_dir "\"/`pwd`/log\""
	@erl -pa `pwd`/ebin -eval 'eunit:test([mio_skip_graph_tests, mio_bucket_tests, global, mio_tests, mio_lock, mio_mvector, mio_store, mio_node]).' -s init stop

check_one: all
	@erl -pa `pwd`/ebin -eval 'ct:run_test([{auto_compile, true}, {suite, [$(TEST_NAME)_SUITE]}, {dir, "./test"}, {logdir, "./log"}, {refresh_logs, "./log"}, {cover, "./src/mio.coverspec"}]).' -s init stop -mio verbose $(VERBOSE_TEST) log_dir "\"/`pwd`/log\""

vcheck: all
	VERBOSE_TEST=true make check

test: check

install: all install_dirs
	cp -rp ebin include $(TARGET_DIR)
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

dist: dist-clean
	mkdir $(DIST_TARGET)
	cp -r Makefile ebin src include scripts README test $(DIST_TARGET)
	chmod 0755 $(DIST_TARGET)/scripts/*
	tar -zcf $(TARBALL_NAME).tar.gz $(DIST_TARGET)
	rm -rf $(DIST_TMP_DIR)

distclean: clean
	rm -f $(LOG_PREFIX)*
	rm -f *.dump
	find . -regex '.*\(~\|#\|\.swp\|\.dump\)' -exec rm {} \;

clean:
	rm -f $(TARGETS) $(TARBALL_NAME).tar.gz
	rm -f test/*.beam
	rm -rf log/ct_run*
	rm -rf log/mio.log.*
