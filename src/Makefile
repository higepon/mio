EMULATOR=beam
ROOT = ..
EBIN = $(ROOT)/ebin
INCLUDE = $(ROOT)/include

APP_TARGET = $(EBIN)/mio.app

ERL_COMPILE_FLAGS += \
	+warn_unused_vars +nowarn_shadow_vars +warn_unused_import \
	+debug_info

SOURCES = mio mio_sup mio_node

MODS = ${SOURCES:%=$(EBIN)/%.$(EMULATOR)} $(APP_TARGET)

$(EBIN)/%.$(EMULATOR):%.erl
	erlc -pa $(EBIN) -W $(ERL_COMPILE_FLAGS) -I$(INCLUDE) -o$(EBIN) $<

all: $(MODS)

$(MODS):

clean:
	rm -rf $(EBIN)/*.$(EMULATOR) $(APP_TARGET) $(EBIN)/erl_crash.dump *~

$(APP_TARGET) : mio.app
	cp $< $@
