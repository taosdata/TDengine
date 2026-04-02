#
# Makefile for dlcompat
#
#
# Copyright (c) 2001 Christoph Pfisterer.
#
# Portions Copyright (c) 1999-2001 Apple Computer, Inc. All Rights
# Reserved.
#
# This file contains Original Code and/or Modifications of Original
# Code as defined in and that are subject to the Apple Public Source
# License Version 1.2 (the "License"). You may not use this file
# except in compliance with the License. Please obtain a copy of the
# License at http://www.apple.com/publicsource and read it before
# using this file.
#
# The Original Code and all software distributed under the License are
# distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, EITHER
# EXPRESS OR IMPLIED, AND APPLE HEREBY DISCLAIMS ALL SUCH WARRANTIES,
# INCLUDING WITHOUT LIMITATION, ANY WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE, QUIET ENJOYMENT OR
# NON-INFRINGEMENT. Please see the License for the specific language
# governing rights and limitations under the License.
# 


prefix=/usr/local
DEBUG=0

CC=cc
CFLAGS=-Wall -O2 -DDEBUG=$(DEBUG)
AR=ar cru
RANLIB=ranlib
INSTALL=install

OBJS = dlopen.o


all: libdl.a libdl.dylib

install: all
	if test ! -d $(prefix)/lib ; then mkdir $(prefix)/lib ; fi
	$(INSTALL) -m 644 libdl.a $(prefix)/lib
	$(RANLIB) $(prefix)/lib/libdl.a
	chmod 644 $(prefix)/lib/libdl.a
	$(INSTALL) -m 755 libdl.dylib $(prefix)/lib
	if test ! -d $(prefix)/include ; then mkdir $(prefix)/include ; fi
	$(INSTALL) -c -m 644 dlfcn.h $(prefix)/include

.c.o:
	$(CC) $(CFLAGS) -fno-common -o $@ -c $<

libdl.a: $(OBJS)
	$(AR) libdl.a $(OBJS)
	$(RANLIB) libdl.a

libdl.dylib: $(OBJS)
	$(CC) -dynamiclib -undefined error -o libdl.dylib $(OBJS) -install_name $(prefix)/lib/libdl.dylib

clean:
	rm -f $(OBJS) libdl.*

# EOF
