all:
	cd src; $(MAKE)

check: all
	/usr/local/lib/erlang/lib/common_test-1.4.1/priv/bin/run_test -dir . -logdir ./log -pa $(PWD)/ebin

clean:
	cd src; $(MAKE) clean
