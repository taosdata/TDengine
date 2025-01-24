AM_CPPFLAGS = -I$(top_builddir)/src -I$(top_srcdir)/src
LDADD = $(top_builddir)/src/libjansson.la

if USE_OSSFUZZ_FLAG
FUZZ_FLAG = $(LIB_FUZZING_ENGINE)
else
if USE_OSSFUZZ_STATIC
LDADD += $(LIB_FUZZING_ENGINE)
FUZZ_FLAG =
else
LDADD += libstandaloneengine.a
FUZZ_FLAG =
endif
endif

noinst_PROGRAMS =
noinst_LIBRARIES =

if USE_OSSFUZZERS
noinst_PROGRAMS += \
	json_load_dump_fuzzer

noinst_LIBRARIES += \
	libstandaloneengine.a
endif

json_load_dump_fuzzer_SOURCES = json_load_dump_fuzzer.cc testinput.h
json_load_dump_fuzzer_CXXFLAGS = $(AM_CXXFLAGS) $(FUZZ_FLAG)
json_load_dump_fuzzer_LDFLAGS = $(AM_LDFLAGS) -static

libstandaloneengine_a_SOURCES = standaloneengine.cc
libstandaloneengine_a_CXXFLAGS = $(AM_CXXFLAGS)
