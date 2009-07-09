EMULATOR=beam
ROOT = ..
EBIN = $(ROOT)/ebin
INCLUDE = $(ROOT)/include

APP_TARGET = $(EBIN)/mio.app

 ## N.B. Don't use +nowarn_shadow_vars !!!
ERL_COMPILE_FLAGS += \
	+warn_unused_vars +warn_unused_import +warn_shadow_vars \
	+debug_info \
	+'{parse_transform, smart_exceptions}' ## should be commented out on release

SOURCES = mio mio_sup mio_node mio_mvector

MODS = ${SOURCES:%=$(EBIN)/%.$(EMULATOR)} $(APP_TARGET)

$(EBIN)/%.$(EMULATOR):%.erl
	erlc -pa $(EBIN) -W $(ERL_COMPILE_FLAGS) -I$(INCLUDE) -o$(EBIN) $<

all: $(MODS)

$(MODS):

clean:
	rm -rf $(EBIN)/*.$(EMULATOR) $(APP_TARGET) $(EBIN)/erl_crash.dump *~

$(APP_TARGET) : mio.app
	cp $< $@
