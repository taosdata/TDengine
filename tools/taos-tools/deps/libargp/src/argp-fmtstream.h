/* Word-wrapping and line-truncating streams.
   Copyright (C) 1997 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Written by Miles Bader <miles@gnu.ai.mit.edu>.

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
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */

/* This package emulates glibc `line_wrap_stream' semantics for systems that
   don't have that.  If the system does have it, it is just a wrapper for
   that.  This header file is only used internally while compiling argp, and
   shouldn't be installed.  */

#ifndef _ARGP_FMTSTREAM_H
#define _ARGP_FMTSTREAM_H

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#ifndef __attribute__
/* This feature is available in gcc versions 2.5 and later.  */
# if __GNUC__ < 2 || (__GNUC__ == 2 && __GNUC_MINOR__ < 5) || __STRICT_ANSI__
#  define __attribute__(Spec) /* empty */
# endif
/* The __-protected variants of `format' and `printf' attributes
   are accepted by gcc versions 2.6.4 (effectively 2.7) and later.  */
# if __GNUC__ < 2 || (__GNUC__ == 2 && __GNUC_MINOR__ < 7) || __STRICT_ANSI__
#  define __format__ format
#  define __printf__ printf
# endif
#endif


/* Guess we have to define our own version.  */
struct argp_fmtstream
{
  FILE *stream;			/* The stream we're outputting to.  */

  size_t lmargin, rmargin;	/* Left and right margins.  */
  ssize_t wmargin;		/* Margin to wrap to, or -1 to truncate.  */

  /* Point in buffer to which we've processed for wrapping, but not output.  */
  size_t point_offs;
  /* Output column at POINT_OFFS, or -1 meaning 0 but don't add lmargin.  */
  ssize_t point_col;

  char *buf;			/* Output buffer.  */
  char *p;			/* Current end of text in BUF. */
  char *end;			/* Absolute end of BUF.  */
};

typedef struct argp_fmtstream *argp_fmtstream_t;

/* Return an argp_fmtstream that outputs to STREAM, and which prefixes lines
   written on it with LMARGIN spaces and limits them to RMARGIN columns
   total.  If WMARGIN >= 0, words that extend past RMARGIN are wrapped by
   replacing the whitespace before them with a newline and WMARGIN spaces.
   Otherwise, chars beyond RMARGIN are simply dropped until a newline.
   Returns NULL if there was an error.  */
extern argp_fmtstream_t argp_make_fmtstream (FILE *__stream,
					     size_t __lmargin,
					     size_t __rmargin,
					     ssize_t __wmargin);

/* Flush __FS to its stream, and free it (but don't close the stream).  */
extern void argp_fmtstream_free (argp_fmtstream_t __fs);


extern ssize_t argp_fmtstream_printf (argp_fmtstream_t __fs,
				      const char *__fmt, ...)
     __attribute__ ((__format__ (printf, 2, 3)));

extern int argp_fmtstream_putc (argp_fmtstream_t __fs, int __ch);

extern int argp_fmtstream_puts (argp_fmtstream_t __fs, const char *__str);

extern size_t argp_fmtstream_write (argp_fmtstream_t __fs,
				    const char *__str, size_t __len);

/* Access macros for various bits of state.  */
#define argp_fmtstream_lmargin(__fs) ((__fs)->lmargin)
#define argp_fmtstream_rmargin(__fs) ((__fs)->rmargin)
#define argp_fmtstream_wmargin(__fs) ((__fs)->wmargin)

/* Set __FS's left margin to LMARGIN and return the old value.  */
extern size_t argp_fmtstream_set_lmargin (argp_fmtstream_t __fs,
					  size_t __lmargin);

/* Set __FS's right margin to __RMARGIN and return the old value.  */
extern size_t argp_fmtstream_set_rmargin (argp_fmtstream_t __fs,
					  size_t __rmargin);

/* Set __FS's wrap margin to __WMARGIN and return the old value.  */
extern size_t argp_fmtstream_set_wmargin (argp_fmtstream_t __fs,
					  size_t __wmargin);

/* Return the column number of the current output point in __FS.  */
extern size_t argp_fmtstream_point (argp_fmtstream_t __fs);

/* Internal routines.  */
extern void _argp_fmtstream_update (argp_fmtstream_t __fs);
extern int _argp_fmtstream_ensure (argp_fmtstream_t __fs, size_t __amount);




#endif /* argp-fmtstream.h */
