all:
	cd src; $(MAKE)

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

clean:
	cd src; $(MAKE) clean
