# Makefile.am for the common plugin code for SASL
# $Id: Makefile.am,v 1.86 2017/07/25 14:18:10 ajaysusarla Exp $
#
################################################################
# Copyright (c) 2017 Carnegie Mellon University.  All rights reserved.
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

# Library version info - here at the top, for sanity
# See <http://www.gnu.org/software/libtool/manual/libtool.html#Versioning>
# CURRENT:REVISION:AGE
plugin_common_version = 3:0:0
crypto_compat_version = 0:0:0

AM_CPPFLAGS=-fPIC -I$(top_srcdir)/include -I$(top_builddir)/include

noinst_LTLIBRARIES = libplugin_common.la libcrypto_compat.la

libplugin_common_la_SOURCES = plugin_common.c plugin_common.h
libplugin_common_la_LDFLAGS = -version-info $(plugin_common_version) -no-undefined
libplugin_common_la_LIBADD = $(LIB_SOCKET)

libcrypto_compat_la_SOURCES = crypto-compat.c crypto-compat.h
libcrypto_compat_la_LDFLAGS = -version-info $(crypto_compat_version) -no-undefined
