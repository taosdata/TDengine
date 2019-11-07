CFLAGS += -O3 -Wall -Wextra -pedantic

OS = $(shell echo `uname -a | grep Ubuntu | wc -l`)
ifeq ($(OS), 0)
CFLAGS += -std=gnu99
endif #centos

ROOT = ../../../build
TARGET = $(ROOT)/lib/libbase64.a
# Set OBJCOPY if not defined by environment:
OBJCOPY ?= objcopy

OBJS = \
  $(ROOT)/lib/arch/avx2/codec.o \
  $(ROOT)/lib/arch/generic/codec.o \
  $(ROOT)/lib/arch/neon32/codec.o \
  $(ROOT)/lib/arch/neon64/codec.o \
  $(ROOT)/lib/arch/ssse3/codec.o \
  $(ROOT)/lib/arch/sse41/codec.o \
  $(ROOT)/lib/arch/sse42/codec.o \
  $(ROOT)/lib/arch/avx/codec.o \
  $(ROOT)/lib/lib.o \
  $(ROOT)/lib/codec_choose.o

HAVE_AVX2   = 0
HAVE_NEON32 = 0
HAVE_NEON64 = 0
HAVE_SSSE3  = 0
HAVE_SSE41  = 0
HAVE_SSE42  = 0
HAVE_AVX    = 0
HAVE_FAST_UNALIGNED_ACCESS = 0

# The user should supply compiler flags for the codecs they want to build.
# Check which codecs we're going to include:
ifdef AVX2_CFLAGS
  HAVE_AVX2 = 1
  HAVE_FAST_UNALIGNED_ACCESS = 1
endif
ifdef NEON32_CFLAGS
  HAVE_NEON32 = 1
endif
ifdef NEON64_CFLAGS
  HAVE_NEON64 = 1
endif
ifdef SSSE3_CFLAGS
  HAVE_SSSE3 = 1
  HAVE_FAST_UNALIGNED_ACCESS = 1
endif
ifdef SSE41_CFLAGS
  HAVE_SSE41 = 1
  HAVE_FAST_UNALIGNED_ACCESS = 1
endif
ifdef SSE42_CFLAGS
  HAVE_SSE42 = 1
  HAVE_FAST_UNALIGNED_ACCESS = 1
endif
ifdef AVX_CFLAGS
  HAVE_AVX = 1
  HAVE_FAST_UNALIGNED_ACCESS = 1
endif
ifdef OPENMP
  CFLAGS += -fopenmp
endif


.PHONY: all analyze clean

all: $(ROOT)/bin/base64 $(ROOT)/lib/libbase64.o
	cp $(ROOT)/lib/libbase64.o $(TARGET)

$(ROOT)/bin/base64: $(ROOT)/bin/base64.o $(ROOT)/lib/libbase64.o
	$(CC) $(CFLAGS) -o $@ $^

$(ROOT)/lib/libbase64.o: $(OBJS)
	$(LD) -r -o $@ $^
	$(OBJCOPY) --keep-global-symbols=lib/exports.txt $@

lib/config.h:
	@echo "#define HAVE_AVX2                  $(HAVE_AVX2)"                   > $@
	@echo "#define HAVE_NEON32                $(HAVE_NEON32)"                >> $@
	@echo "#define HAVE_NEON64                $(HAVE_NEON64)"                >> $@
	@echo "#define HAVE_SSSE3                 $(HAVE_SSSE3)"                 >> $@
	@echo "#define HAVE_SSE41                 $(HAVE_SSE41)"                 >> $@
	@echo "#define HAVE_SSE42                 $(HAVE_SSE42)"                 >> $@
	@echo "#define HAVE_AVX                   $(HAVE_AVX)"                   >> $@
	@echo "#define HAVE_FAST_UNALIGNED_ACCESS $(HAVE_FAST_UNALIGNED_ACCESS)" >> $@

lib/tables.h: lib/table_generator.c
	$(CC) $(CFLAGS) -o $(ROOT)/lib/table_generator $^
	$(ROOT)/lib/table_generator > $@

VPATH = lib
$(OBJS): lib/config.h

$(ROOT)/lib/lib.o: lib/tables.h

$(ROOT)/lib/arch/avx2/codec.o:   CFLAGS += $(AVX2_CFLAGS)
$(ROOT)/lib/arch/neon32/codec.o: CFLAGS += $(NEON32_CFLAGS)
$(ROOT)/lib/arch/neon64/codec.o: CFLAGS += $(NEON64_CFLAGS)
$(ROOT)/lib/arch/ssse3/codec.o:  CFLAGS += $(SSSE3_CFLAGS)
$(ROOT)/lib/arch/sse41/codec.o:  CFLAGS += $(SSE41_CFLAGS)
$(ROOT)/lib/arch/sse42/codec.o:  CFLAGS += $(SSE42_CFLAGS)
$(ROOT)/lib/arch/avx/codec.o:    CFLAGS += $(AVX_CFLAGS)

$(ROOT)/lib/%.o: %.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -c $< -o $@

$(ROOT)/bin/base64.o: bin/base64.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -c $< -o $@

analyze: clean
	scan-build --use-analyzer=`which clang` --status-bugs make

clean:
	rm -f $(ROOT)/bin/base64 \
		  $(ROOT)/bin/base64.o \
		  $(ROOT)/lib/libbase64.o \
		  $(ROOT)/lib/table_generator.o \
		  $(ROOT)/lib/table_generator \
		  $(OBJS)
