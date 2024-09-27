# Makefile.am for the SASL utilities
# Rob Earhart
#
################################################################
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
################################################################

all_sasl_libs = ../lib/libsasl2.la $(SASL_DB_LIB) $(LIB_SOCKET)
all_sasl_static_libs = ../lib/.libs/libsasl2.a $(SASL_DB_LIB) $(LIB_SOCKET) $(GSSAPIBASE_LIBS) $(GSSAPI_LIBS) $(SASL_KRB_LIB) $(LIB_DES) $(PLAIN_LIBS) $(SRP_LIBS) $(LIB_MYSQL) $(LIB_PGSQL) $(LIB_SQLITE)

sbin_PROGRAMS = @SASL_DB_UTILS@ @SMTPTEST_PROGRAM@ pluginviewer
EXTRA_PROGRAMS = saslpasswd2 sasldblistusers2 testsuite testsuitestatic smtptest pluginviewer

noinst_PROGRAMS = dbconverter-2

if NO_SASL_DB_MANS
man_MANS = 
else
man_MANS = saslpasswd2.8 sasldblistusers2.8 pluginviewer.8
endif

saslpasswd2_LDADD = ../sasldb/libsasldb.la $(all_sasl_libs)
saslpasswd2_SOURCES = saslpasswd.c
sasldblistusers2_LDADD = ../sasldb/libsasldb.la $(all_sasl_libs)
sasldblistusers2_SOURCES = sasldblistusers.c
dbconverter_2_LDADD = ../sasldb/libsasldb.la $(all_sasl_libs)
pluginviewer_LDADD = $(all_sasl_libs)
pluginviewer_SOURCES = pluginviewer.c

testsuite_LDADD = $(all_sasl_libs) @DMALLOC_LIBS@

CLEANFILES=$(EXTRA_PROGRAMS)

testsuitestatic_SOURCES = testsuite.c
testsuitestatic_LDADD = $(all_sasl_static_libs) @DMALLOC_LIBS@ @SASL_DL_LIB@
testsuitestatic_DEPENDENCIES = ../lib/.libs/libsasl2.a

smtptest_SOURCES =
smtptest_DEPENDENCIES = ./smtptest.lo ./libsfsasl2.la
smtptest_LDADD = ./smtptest.lo ./libsfsasl2.la @SFIO_LIB_FLAGS@ @DMALLOC_LIBS@ $(all_sasl_libs)

saslincludedir = $(includedir)/sasl

saslinclude_HEADERS = @SASL_UTIL_HEADERS_EXTRA@
EXTRA_HEADERS = sfsasl.h

# Note: we explicitly do *not* link to libsfio, as people will need to
# link to that anyway if they want to use this. 
lib_LTLIBRARIES = @SASL_UTIL_LIBS_EXTRA@
EXTRA_LTLIBRARIES = libsfsasl2.la
libsfsasl2_la_SOURCES = 
libsfsasl2_la_LIBADD = sfsasl.lo
libsfsasl2_la_LDFLAGS = -version-info 1:0:0 -export-dynamic -rpath $(libdir)

AM_CPPFLAGS=-DLIBSASL_EXPORTS=1 -I$(top_srcdir)/include -I$(top_builddir)/include @SASL_DB_INC@ -DPLUGINDIR='"${plugindir}"'

EXTRA_DIST = saslpasswd2.8 sasldblistusers2.8 pluginviewer.8 sfsasl.h sfsasl.c smtptest.c testsuite.c pluginviewer.c NTMakefile

sfsasl.lo: sfsasl.c
	$(LIBTOOL) --mode=compile $(COMPILE) @SFIO_INC_FLAGS@ -c $(srcdir)/sfsasl.c

smtptest.lo: smtptest.c
	$(LIBTOOL) --mode=compile $(COMPILE) @SFIO_INC_FLAGS@ -c $(srcdir)/smtptest.c
