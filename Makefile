all: sandbox.beam

sandbox.beam: sandbox.erl
	erlc $<
