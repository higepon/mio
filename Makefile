all: memcache.beam sandbox.beam

sandbox.beam: sandbox.erl
	erlc $<

memcache.beam: memcache.erl
	erlc $<
