# Makefile.am for the pwcheck daemon
# Larry Greenfield
#
#         Copyright 1999 by Carnegie Mellon University
#
#                       All Rights Reserved
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose and without fee is hereby granted,
# provided that the above copyright notice appear in all copies and that
# both that copyright notice and this permission notice appear in
# supporting documentation, and that the name of CMU not be
# used in advertising or publicity pertaining to distribution of the
# software without specific, written prior permission.
#
# CMU DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING
# ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL
# CMU BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR
# ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS,
# WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION,
# ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.
#

sbin_PROGRAMS = pwcheck

AM_CPPFLAGS = -I../include -I../lib

pwcheck_SOURCES = pwcheck.c
EXTRA_pwcheck_SOURCES = pwcheck_getpwnam.c pwcheck_getspnam.c
pwcheck_DEPENDECIES = pwcheck_@PWCHECKMETH@.lo
pwcheck_LDADD = pwcheck_@PWCHECKMETH@.lo @LIB_CRYPT@ @LIB_SOCKET@
