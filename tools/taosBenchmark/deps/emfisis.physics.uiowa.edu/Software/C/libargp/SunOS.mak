# Common Definitions ########################################################

# Pick a default install location, if user doesn't have one defiend
ifeq ($(PREFIX),)
PREFIX=/usr/local
endif

BIN=$(PREFIX)/bin
LIB=$(PREFIX)/lib
INC=$(PREFIX)/include

ARCH=$(shell uname -s).$(shell uname -p)

BUILD:=build.$(ARCH)

#CFLAGS = -xcode=pic32 -g -xc99 -errwarn
CFLAGS = -g -xc99 -errwarn

CC=/opt/SUNWspro/bin/cc

LFLAGS = 

# Specific Definitions ######################################################

HDR_IN = argp.h

SRC_IN = argp-ba.c argp-fs-xinl.c argp-pv.c argp-eexst.c \
 argp-help.c argp-pvh.c argp-fmtstream.c argp-parse.c \
 argp-pin.c getopt.c getopt1.c 
 
TEST_IN = test_argv0

INSTALL_HDRS = $(patsubst %,$(INC)/%,$(HDR_IN))

SRCS = $(patsubst %.c,argp/%.c,$(SRC_IN))

TEST_SRCS = $(patsubst %,test/%.c,$(TEST_IN))

OBJS = $(patsubst argp/%.c,$(BUILD)/%.o,$(SRCS))

TEST_PROGS = $(patsubst %,$(BUILD)/%,$(TEST_IN))


# Pattern Rules #############################################################

# Pattern rule for the test executables
$(BUILD)/%:test/%.c
	$(CC) $(CFLAGS) $< $(BUILD)/libargp.a $(LFLAGS) -o $@

# Pattern rule for the argp library object files
$(BUILD)/%.o:argp/%.c
	$(CC) -c $(CFLAGS) -o $@ $<

# Generic Pattern rule for installing files in the juno library location  
$(LIB)/%:$(BUILD)/%
	if [ ! -d $(LIB) ]; then mkdir -p $(LIB); fi
	cp $< $@
	
# Generic pattern rule for installing header files
$(INC)/%:argp/%
	if [ ! -d $(INC)/argp ]; then mkdir -p $(INC)/argp; fi
	cp $< $@

# Explicit Rules #############################################################

# Direct make not to nuke the intermediate .o files
.SECONDARY: $(OBJS)

build:$(BUILD) $(BUILD)/libargp.a $(TEST_PROGS)
 
$(BUILD):
	@if [ ! -d "$(BUILD)" ]; then echo mkdir $(BUILD); \
	mkdir $(BUILD); fi	

$(BUILD)/libargp.a: $(OBJS)
	ar -crs $@ $(OBJS)

install: $(LIB)/libargp.a $(INSTALL_HDRS)
		
clean:
	rm $(OBJS)  $(TEST_PROGS)

distclean:
	rm -r -f $(BUILD)




