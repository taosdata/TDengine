/* Extended regular expression matching and search library.
   Copyright (C) 2002, 2003 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Isamu Hasegawa <isamu@yamato.ibm.com>.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301 USA.  */

#ifdef _LIBC
/* We have to keep the namespace clean.  */
#  define regfree(preg) __regfree (preg)
#  define regexec(pr, st, nm, pm, ef) __regexec (pr, st, nm, pm, ef)
#  define regcomp(preg, pattern, cflags) __regcomp (preg, pattern, cflags)
#  define regerror(errcode, preg, errbuf, errbuf_size) \
	__regerror(errcode, preg, errbuf, errbuf_size)
#  define re_set_registers(bu, re, nu, st, en) \
	__re_set_registers (bu, re, nu, st, en)
#  define re_match_2(bufp, string1, size1, string2, size2, pos, regs, stop) \
	__re_match_2 (bufp, string1, size1, string2, size2, pos, regs, stop)
#  define re_match(bufp, string, size, pos, regs) \
	__re_match (bufp, string, size, pos, regs)
#  define re_search(bufp, string, size, startpos, range, regs) \
	__re_search (bufp, string, size, startpos, range, regs)
#  define re_compile_pattern(pattern, length, bufp) \
	__re_compile_pattern (pattern, length, bufp)
#  define re_set_syntax(syntax) __re_set_syntax (syntax)
#  define re_search_2(bufp, st1, s1, st2, s2, startpos, range, regs, stop) \
	__re_search_2 (bufp, st1, s1, st2, s2, startpos, range, regs, stop)
#  define re_compile_fastmap(bufp) __re_compile_fastmap (bufp)
#endif

/* POSIX says that <sys/types.h> must be included (by the caller) before
   <regex.h>.  */
#include <sys/types.h>
#include <regex.h>
#include "regex_internal.h"

#include "regex_internal.c"
#include "regcomp.c"
#include "regexec.c"

/* Binary backward compatibility.  */
#if _LIBC
# include <shlib-compat.h>
# if SHLIB_COMPAT (libc, GLIBC_2_0, GLIBC_2_3)
link_warning (re_max_failures, "the 're_max_failures' variable is obsolete and will go away.")
int re_max_failures = 2000;
# endif
#endif
