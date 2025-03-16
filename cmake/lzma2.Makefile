SRC = $(wildcard *.c)
OBJ = $(SRC:.c=.o)
DEP = $(OBJ:.o=.d)

CFLAGS:=-Wall -O2 -pthread -fPIC
CC:=gcc
AR:=ar -rcs
RM:=rm -rf

ASFLAGS :=

SONAME:=libfast-lzma2.so.1
REAL_NAME:=libfast-lzma2.so.1.0
LINKER_NAME=libfast-lzma2.so
STATIC_LIBNAME=libfast-lzma2.a

x86_64:=0

ifeq ($(OS),Windows_NT)
	CFLAGS+=-DFL2_DLL_EXPORT=1
	LINKER_NAME=libfast-lzma2.dll
	SONAME:=$(LINKER_NAME)
	REAL_NAME:=$(LINKER_NAME)
ifeq ($(PROCESSOR_ARCHITECTURE),AMD64)
	ASFLAGS+=-DMS_x64_CALL=1
	x86_64:=1
endif
else
	PROC_ARCH:=$(shell uname -p)
ifneq ($(PROC_ARCH),x86_64)
	PROC_ARCH:=$(shell uname -m)
endif
ifeq ($(PROC_ARCH),x86_64)
	ASFLAGS+=-DMS_x64_CALL=0
	x86_64:=1
endif
endif

ifeq ($(x86_64),1)
	CFLAGS+=-DLZMA2_DEC_OPT
	OBJ+=lzma_dec_x86_64.o
endif

$(STATIC_LIBNAME) : $(OBJ)
	@echo "Build static & dynamic library."
	$(CC) -shared -pthread -Wl,-soname,$(SONAME) -o $(REAL_NAME) $(OBJ)
	$(AR) $(STATIC_LIBNAME) $(OBJ)
	@echo "Library build SUCCESS."
	
-include $(DEP)

%.d: %.c
	@$(CC) $(CFLAGS) $< -MM -MT $(@:.d=.o) >$@

DESTDIR:=
PREFIX:=/usr/local
LIBDIR:=$(DESTDIR)$(PREFIX)/lib

.PHONY: install
install:
ifeq ($(OS),Windows_NT)
	strip -g $(REAL_NAME)
else
	mkdir -p $(LIBDIR)
	install -C $(STATIC_LIBNAME) $(LIBDIR)/$(STATIC_LIBNAME)
	# cp $(REAL_NAME) $(LIBDIR)/$(REAL_NAME)
	# strip -g $(LIBDIR)/$(REAL_NAME)
	# chmod 0755 $(LIBDIR)/$(REAL_NAME)
	# cd $(LIBDIR) && ln -sf $(REAL_NAME) $(LINKER_NAME)
	# ldconfig $(LIBDIR)
	mkdir -p $(DESTDIR)$(PREFIX)/include
	install -C fast-lzma2.h $(DESTDIR)$(PREFIX)/include/
	install -C fl2_errors.h $(DESTDIR)$(PREFIX)/include/
endif

.PHONY: uninstall
uninstall:
ifeq ($(OS),Windows_NT)
	rm -f libfast-lzma2.dll
else
	rm -f $(LIBDIR)/$(LINKER_NAME)
	rm -f $(LIBDIR)/$(REAL_NAME)
	# ldconfig $(LIBDIR)
	rm -f $(DESTDIR)$(PREFIX)/include/fast-lzma2.h
	rm -f $(DESTDIR)$(PREFIX)/include/fl2_errors.h
endif

.PHONY: test
test:libfast-lzma2
	$(MAKE) -C ./test file_test
	test/file_test radix_engine.h
	@echo "File compression/decompression test completed."

.PHONY: clean
clean:
	$(RM) $(REAL_NAME) $(STATIC_LIBNAME) $(OBJ) $(DEP)
	$(MAKE) -C ./test clean
