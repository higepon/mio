all:
	cd src; $(MAKE)

check: all
	/usr/local/lib/erlang/lib/common_test-1.4.1/priv/bin/run_test -dir . -logdir ./log -pa $(PWD)/ebin
	./start.sh; true
	sleep 1
	gosh test/memcached_compat.ss; pgrep -lf mio | cut -d" " -f 2|xargs kill -9

clean:
	cd src; $(MAKE) clean
