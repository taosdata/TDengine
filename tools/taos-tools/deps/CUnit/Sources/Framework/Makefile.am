## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = -I$(top_srcdir)/CUnit/Headers

SHARED_SOURCES = \
    InternalPoolMalloc.c \
    MessageHandlers.c \
	CUError.c \
	MyMem.c \
	TestDB.c \
	TestRun.c \
	Util.c

noinst_LTLIBRARIES = libcunitfmk.la
libcunitfmk_la_SOURCES = $(SHARED_SOURCES)

if ENABLE_TEST

TEST_DEFINES = -DCUNIT_BUILD_TESTS -DCUNIT_DO_NOT_DEFINE_UNLESS_BUILDING_TESTS -DMEMTRACE
TEST_INCLUDES = -I../Test

%_test.o: %.c
	$(COMPILE) $(TEST_INCLUDES) $(TEST_DEFINES) -o $@ -c $<

TEST_OBJECTS = \
	CUError_test.o \
	MyMem_test.o \
	TestDB_test.o \
	TestRun_test.o \
	Util_test.o

noinst_LIBRARIES = libcunittestfmk.a
libcunittestfmk_a_SOURCES =
libcunittestfmk_a_LIBADD = $(TEST_OBJECTS)
endif
