
# Build environment can be configured the following
# environment variables:
#   CC : Specify the C compiler to use
#   CFLAGS : Specify compiler options to use
#   LDFLAGS : Specify linker options to use
#   CPPFLAGS : Specify c-preprocessor options to use

# Extract version from libmseed.h, expected line should include LIBMSEED_VERSION "#.#.#"
MAJOR_VER = $(shell grep LIBMSEED_VERSION libmseed.h | grep -Eo '[0-9]+.[0-9]+.[0-9]+' | cut -d . -f 1)
FULL_VER = $(shell grep LIBMSEED_VERSION libmseed.h | grep -Eo '[0-9]+.[0-9]+.[0-9]+')
COMPAT_VER = $(MAJOR_VER).0.0

# Default settings for install target
PREFIX ?= /usr/local
EXEC_PREFIX ?= $(PREFIX)
LIBDIR ?= $(EXEC_PREFIX)/lib
INCLUDEDIR ?= $(PREFIX)/include
DATAROOTDIR ?= $(PREFIX)/share
DOCDIR ?= $(DATAROOTDIR)/doc/libmseed
MANDIR ?= $(DATAROOTDIR)/man
MAN3DIR ?= $(MANDIR)/man3

LIB_SRCS = fileutils.c genutils.c gswap.c msio.c lookup.c parson.c \
           msrutils.c extraheaders.c pack.c packdata.c tracelist.c gmtime64.c \
           crc32c.c parseutils.c unpack.c unpackdata.c selection.c logging.c

LIB_OBJS = $(LIB_SRCS:.c=.o)
LIB_LOBJS = $(LIB_SRCS:.c=.lo)

LIB_NAME = libmseed
LIB_A = $(LIB_NAME).a

OS := $(shell uname -s)

# Build dynamic (.dylib) on macOS/Darwin, otherwise shared (.so)
ifeq ($(OS), Darwin)
	LIB_SO_BASE = $(LIB_NAME).dylib
	LIB_SO_MAJOR = $(LIB_NAME).$(MAJOR_VER).dylib
	LIB_SO = $(LIB_NAME).$(FULL_VER).dylib
	LIB_OPTS = -dynamiclib -compatibility_version $(COMPAT_VER) -current_version $(FULL_VER) -install_name $(LIB_SO)
else
	LIB_SO_BASE = $(LIB_NAME).so
	LIB_SO_MAJOR = $(LIB_NAME).so.$(MAJOR_VER)
	LIB_SO = $(LIB_NAME).so.$(FULL_VER)
	LIB_OPTS = -shared -Wl,--version-script=libmseed.map -Wl,-soname,$(LIB_SO_MAJOR)
endif

# Automatically configure LDFLAGS for URL support if requested and libcurl is available
# Test for LIBMSEED_URL in CFLAGS, then if curl-config is available, implying libcurl is available
ifneq (,$(findstring LIBMSEED_URL,$(CFLAGS)))
  ifneq (,$(shell command -v curl-config;))
	export LM_CURL_VERSION=$(shell curl-config --version)
	export LDLIBS:=$(LDLIBS) $(shell curl-config --libs)
  endif
endif

all: static

static: $(LIB_A)

shared dynamic: $(LIB_SO)

# Build static library
$(LIB_A): $(LIB_OBJS)
	@echo "Building static library $(LIB_A)"
	$(RM) -f $(LIB_A)
	$(AR) -crs $(LIB_A) $(LIB_OBJS)

# Build shared/dynamic library
$(LIB_SO): $(LIB_LOBJS)
	@echo "Building shared library $(LIB_SO)"
	$(RM) -f $(LIB_SO) $(LIB_SO_MAJOR) $(LIB_SO_BASE)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIB_OPTS) -o $(LIB_SO) $(LIB_LOBJS)
	ln -s $(LIB_SO) $(LIB_SO_BASE)
	ln -s $(LIB_SO) $(LIB_SO_MAJOR)

test check: static FORCE
	@$(MAKE) -C test test

example: static FORCE
	@$(MAKE) -C example

clean:
	@$(RM) $(LIB_OBJS) $(LIB_LOBJS) $(LIB_A) $(LIB_SO) $(LIB_SO_MAJOR) $(LIB_SO_BASE)
	@$(MAKE) -C test clean
	@$(MAKE) -C example clean
	@echo "All clean."

install: shared
	@echo "Installing into $(PREFIX)"
	@mkdir -p $(DESTDIR)$(PREFIX)/include
	@cp libmseed.h $(DESTDIR)$(PREFIX)/include
	@mkdir -p $(DESTDIR)$(LIBDIR)/pkgconfig
	@cp -a $(LIB_SO_BASE) $(LIB_SO_MAJOR) $(LIB_SO_NAME) $(LIB_SO) $(DESTDIR)$(LIBDIR)
	@sed -e 's|@PREFIX@|$(PREFIX)|g' \
	     -e 's|@EXEC_PREFIX@|$(EXEC_PREFIX)|g' \
	     -e 's|@LIBDIR@|$(LIBDIR)|g' \
	     -e 's|@INCLUDEDIR@|$(PREFIX)/include|g' \
	     -e 's|@VERSION@|$(FULL_VER)|g' \
	     mseed.pc.in > $(DESTDIR)$(LIBDIR)/pkgconfig/mseed.pc
	@mkdir -p $(DESTDIR)$(DOCDIR)/example
	@cp -r example $(DESTDIR)$(DOCDIR)/

.SUFFIXES: .c .o .lo

# Standard object building
.c.o:
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

# Standard object building for shared library using -fPIC
.c.lo:
	$(CC) $(CPPFLAGS) $(CFLAGS) -fPIC -c $< -o $@

# Print Makefile expanded variables, e.g. % make print-LIB_SO
print-%:
	@echo '$*=$($*)'

FORCE:
