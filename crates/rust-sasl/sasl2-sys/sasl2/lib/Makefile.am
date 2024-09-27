# Makefile.am for the SASL library
# Rob Earhart
# $Id: Makefile.am,v 1.88 2011/09/05 14:18:10 murch Exp $
# Copyright (c) 2000 Carnegie Mellon University.  All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer. 
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
# 3. The name "Carnegie Mellon University" must not be used to
#    endorse or promote products derived from this software without
#    prior written permission. For permission or any other legal
#    details, please contact  
#      Office of Technology Transfer
#      Carnegie Mellon University
#      5000 Forbes Avenue
#      Pittsburgh, PA  15213-3890
#      (412) 268-4387, fax: (412) 268-7395
#      tech-transfer@andrew.cmu.edu
#
# 4. Redistributions of any form whatsoever must retain the following
#    acknowledgment:
#    "This product includes software developed by Computing Services
#     at Carnegie Mellon University (http://www.cmu.edu/computing/)."
#
# CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
# THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
# AND FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE
# FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
# AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
# OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

# Library version info - here at the top, for sanity
# See <http://www.gnu.org/software/libtool/manual/libtool.html#Versioning>
# CURRENT:REVISION:AGE
sasl_version = 3:0:0

AM_CPPFLAGS=-DLIBSASL_EXPORTS=1 -I$(top_srcdir)/include -I$(top_srcdir)/plugins \
               -I$(top_builddir)/include -I$(top_srcdir)/sasldb -I$(top_srcdir)/common -DCONFIGDIR='"${configdir}"' -DPLUGINDIR='"${plugindir}"'
PLUGIN_COMMON_OBJS = $(top_builddir)/common/libplugin_common.la

EXTRA_DIST = windlopen.c dlopen.c staticopen.h NTMakefile
EXTRA_LIBRARIES = libsasl2.a
noinst_LIBRARIES = @SASL_STATIC_LIB@
libsasl2_a_SOURCES=

BUILT_SOURCES = $(SASL_STATIC_SRCS)

if WINDOWS
DLOPEN_C = windlopen.c
else
DLOPEN_C = dlopen.c
endif

common_headers = saslint.h
common_sources = auxprop.c canonusr.c checkpw.c client.c common.c config.c external.c md5.c saslutil.c server.c seterror.c $(DLOPEN_C)

LTLIBOBJS = @LTLIBOBJS@
LIB_DOOR= @LIB_DOOR@
SASL_STATIC_LIBS = @SASL_STATIC_LIBS@

lib_LTLIBRARIES = libsasl2.la
if BUILD_LIBOBJ
noinst_LTLIBRARIES = libobj.la
else
noinst_LTLIBRARIES =
endif

libobj_la_SOURCES =
libobj_la_LIBADD = $(LTLIBOBJS)

libsasl2_la_SOURCES = $(common_sources) $(common_headers)
libsasl2_la_LDFLAGS = -version-info $(sasl_version) -no-undefined

libsasl2_la_LIBADD = $(SASL_DL_LIB) $(SASL_STATIC_LIBS) $(LIB_SOCKET) $(LIB_DOOR) $(PLUGIN_COMMON_OBJS)
if BUILD_LIBOBJ
libsasl2_la_LIBADD += libobj.la
endif

if MACOSX
framedir = /Library/Frameworks/SASL2.framework
install-exec-hook:
	$(mkinstalldirs) $(framedir)/Versions/A
	ln -fs $(libdir)/libsasl2.dylib $(framedir)/Versions/A/SASL2
	cd $(framedir) ; ln -fs Versions/A/SASL2 .
else
install-exec-hook:
endif

libsasl2.a: libsasl2.la $(SASL_STATIC_OBJS)
	@echo adding static plugins and dependencies
	$(AR) cru .libs/$@ $(SASL_STATIC_OBJS)
	@for i in ./libsasl2.la ../common/libplugin_common.la ../sasldb/libsasldb.la ../plugins/lib*.la; do \
	if test ! -f $$i; then continue; fi; . $$i; \
	for j in $$dependency_libs foo; do \
	case $$j in foo) ;; \
	-L*) for k in $$depdirs foo; do \
	    if test $$k = $$j; then break; fi; done; \
	  if test $$k = foo; then depdirs="$$depdirs $$j"; fi ;; \
	-l*) for k in $$deplibs foo; do \
	    if test $$k = $$j; then break; fi; done; \
	  if test $$k = foo; then deplibs="$$deplibs $$j"; fi ;; \
	esac; done; dependency_libs=""; done; \
	sed -e "/^dependency_libs=/s%=.*%='$${depdirs}$${deplibs}'%" \
	libsasl2.la >TMP.$$ && mv TMP.$$ libsasl2.la
	rm -f $@
	ln -s .libs/$@ $@

$(SASL_STATIC_SRCS): linksrcs

linksrcs:
	-ln -s $(SASL_STATIC_SRCS) .

