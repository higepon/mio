all:
	cd src; $(MAKE)

VERSION=0.0.1
TARBALL_NAME=mio-$(VERSION)
DIST_TMP_DIR=tmp
DIST_TARGET=$(DIST_TMP_DIR)/$(TARBALL_NAME)

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

dist-clean: clean

clean:
	cd src; $(MAKE) clean
