# Common Definitions ########################################################
PREFIX=C:/local/juno

CFG=$(PREFIX)/cfg
DOC=$(PREFIX)/doc
BIN=$(PREFIX)/bin
SRC=$(PREFIX)/src
LIB=$(PREFIX)/lib
PYLIB=$(LIB)/python
INC=$(PREFIX)/include
SHARE=$(PREFIX)/share

DATE=$(shell date "+%Y-%m-%d")
ARCH=$(shell uname -s)

BUILD=build.$(ARCH)

# Flags ######################################################################

DEFINES = -DCFG=$(CFG)
INCFLAGS = -I.
WARNFLAGS = -Wall -Werror

# NOTE: -std=c99 is not supported in windows, just triggers __STRICT_ANSII
#                macro which turns off alot of standard functions such as
#                strcasecmp
CFLAGS = -fexceptions -ggdb $(INCFLAGS) $(WARNFLAGS) $(DEFINES) $(TH_CFLAGS)

CC=gcc

LFLAGS = 

# Specific Definitions ######################################################

HDR_IN = argp.h

SRC_IN = argp-ba.c argp-fs-xinl.c argp-pv.c argp-eexst.c \
 argp-help.c argp-pvh.c argp-fmtstream.c argp-parse.c \
 argp-pin.c getopt.c getopt1.c getopt_init.c
 
TEST_IN = test_argv0

INSTALL_HDRS = $(patsubst %,$(INC)/argp/%,$(HDR_IN))

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
	if [ ! -e $(LIB) ]; then mkdir -p $(LIB); fi
	cp $< $@
	
# Generic pattern rule for installing header files
$(INC)/argp/%:argp/%
	if [ ! -e $(INC)/argp ]; then mkdir -p $(INC)/argp; fi
	cp $< $@

# Explicit Rules #############################################################

# Direct make not to nuke the intermediate .o files
.SECONDARY: $(OBJS)

build:$(BUILD) $(BUILD)/libargp.a $(TEST_PROGS)
 
$(BUILD):
	@if [ ! -e "$(BUILD)" ]; then echo mkdir $(BUILD); \
	mkdir $(BUILD); fi	

$(BUILD)/libargp.a: $(OBJS)
	ar -crs $@ $(OBJS)

install: $(LIB)/libargp.a $(INSTALL_HDRS)
		
clean:
	rm $(OBJS)  $(TEST_PROGS)

distclean:
	rm -r -f $(BUILD)


