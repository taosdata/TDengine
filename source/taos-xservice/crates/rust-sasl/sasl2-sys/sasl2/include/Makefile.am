# Makefile.am for SASL includes
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

noinst_HEADERS = gai.h exits.h

saslincludedir = $(includedir)/sasl
saslinclude_HEADERS = hmac-md5.h md5.h md5global.h sasl.h saslplug.h saslutil.h prop.h

noinst_PROGRAMS = makemd5

makemd5_SOURCES = makemd5.c

makemd5$(BUILD_EXEEXT) $(makemd5_OBJECTS): CC=$(CC_FOR_BUILD)
makemd5$(BUILD_EXEEXT) $(makemd5_OBJECTS): CFLAGS=$(CFLAGS_FOR_BUILD)
makemd5$(BUILD_EXEEXT): LDFLAGS=$(LDFLAGS_FOR_BUILD)

md5global.h: makemd5$(BUILD_EXEEXT) Makefile
	-rm -f $@
	./$< $@

BUILT_SOURCES = md5global.h

EXTRA_DIST = NTMakefile
DISTCLEANFILES = md5global.h

if MACOSX
framedir = /Library/Frameworks/SASL2.framework
frameheaderdir = $(framedir)/Versions/A/Headers
frameheader_DATA = $(saslinclude_HEADERS)
endif

# TODO: automake, don't build it
makemd5$(EXEEXT):
