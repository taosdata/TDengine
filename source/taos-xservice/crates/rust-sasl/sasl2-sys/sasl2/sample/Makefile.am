# Makefile.am -- automake input for sample SASL programs
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

AM_CPPFLAGS=-I$(top_srcdir)/include -I$(top_builddir)/include -DPLUGINDIR='"${plugindir}"'

noinst_PROGRAMS = client server http_digest_client
EXTRA_PROGRAMS = sample-client sample-server
EXTRA_DIST = NTMakefile
CLEANFILES=sample-client sample-server ./.libs/*sample-client ./.libs/*sample-server

sample_client_SOURCES = sample-client.c
sample_server_SOURCES = sample-server.c

server_SOURCES = server.c common.c common.h
client_SOURCES = client.c common.c common.h
http_digest_client_SOURCES = http_digest_client.c

server_LDADD = ../lib/libsasl2.la $(GSSAPIBASE_LIBS) $(GSSAPI_LIBS) $(LIB_SOCKET)
client_LDADD = ../lib/libsasl2.la $(GSSAPIBASE_LIBS) $(GSSAPI_LIBS) $(LIB_SOCKET)
http_digest_client_LDADD = ../lib/libsasl2.la $(GSSAPIBASE_LIBS) $(GSSAPI_LIBS) $(LIB_SOCKET)

sample_client_LDADD = ../lib/libsasl2.la $(GSSAPIBASE_LIBS) $(GSSAPI_LIBS) $(LIB_SOCKET)
sample_server_LDADD = ../lib/libsasl2.la $(GSSAPIBASE_LIBS) $(GSSAPI_LIBS) $(LIB_SOCKET)
