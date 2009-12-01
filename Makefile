TARGETS = $(BEAMS) $(APP)

APP_NAME=mio
VERSION=0.0.1

SOURCE_DIR=src
EBIN_DIR=ebin
INCLUDE_DIR=include
LOG_PREFIX=mio.log
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
BEAMS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))
APP=$(EBIN_DIR)/mio.app
ERLC_FLAGS=+warn_unused_vars \
           +warn_unused_import \
           +warn_shadow_vars \
           -Wall \
           -v    \
           +debug_info \
           +bin_opt_info \
           +no_strict_record_tests \
           +native +"{hipe, [o3]}" \

TARBALL_NAME=$(APP_NAME)-$(VERSION)
DIST_TMP_DIR=tmp
DIST_TARGET=$(DIST_TMP_DIR)/$(TARBALL_NAME)

all: $(TARGETS)

$(APP): $(SOURCE_DIR)/mio.app
	cp -p $< $@

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDE_DIR)/mio.hrl
	erlc -pa $(EBIN_DIR) -W $(ERLC_FLAGS) -I$(INCLUDE_DIR) -o$(EBIN_DIR) $<

check: all
	/usr/local/lib/erlang/lib/common_test-1.4.1/priv/bin/run_test -dir . -logdir ./log -cover mio.coverspec -pa $(PWD)/ebin -include $(PWD)/include
	@./bin/start.sh &
	@sleep 2
	@gosh test/memcached_compat.ss
	@./bin/stop.sh

vcheck: all # verbose
	/usr/local/lib/erlang/lib/common_test-1.4.1/priv/bin/run_test -config test/config.verbose -dir . -logdir ./log  -cover mio.coverspec -pa $(PWD)/ebin -include $(PWD)/include
	@./bin/start.sh &
	@sleep 1
	@gosh test/memcached_compat.ss;
	@./bin/stop.sh

test: check

install: all install_dirs
	@[ -n "$(TARGET_DIR)" ] || (echo "Please set TARGET_DIR. e.g. /usr/local/mio"; false)
	@[ -n "$(SBIN_DIR)" ] || (echo "Please set SBIN_DIR. e.g. /usr/sbin/"; false)
	mkdir -p $(TARGET_DIR)
	cp -rp ebin include $(TARGET_DIR)
	for script in mio mioctl mio-env; do \
		chmod 0755 scripts/$$scripts; \
		cp -p scripts/$$script $(TARGET_DIR)/sbin; \
		[ -e $(SBIN_DIR)/$$script ] || ln -s $(TARGET_DIR)/sbin/$$script $(SBIN_DIR)/$$script; \
	done

install_dirs:
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


