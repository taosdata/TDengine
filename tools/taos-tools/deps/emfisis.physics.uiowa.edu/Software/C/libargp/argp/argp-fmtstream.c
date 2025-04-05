/* Word-wrapping and line-truncating streams
   Copyright (C) 1997-1999,2001,2002,2003,2005 Free Software Foundation, Inc.
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
   don't have that.  */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <ctype.h>

#include "argp-fmtstream.h"


#ifndef isblank
#define isblank(ch) ((ch)==' ' || (ch)=='\t')
#endif


#define INIT_BUF_SIZE 200
#define PRINTF_SIZE_GUESS 150

/* The inline functions from argp-fmtstream.h */
size_t
argp_fmtstream_write (argp_fmtstream_t __fs, const char *__str, 
                      size_t __len)
{
  if (__fs->p + __len <= __fs->end || _argp_fmtstream_ensure (__fs, __len))
    {
      memcpy (__fs->p, __str, __len);
      __fs->p += __len;
      return __len;
    }
  else
    return 0;
}

int
argp_fmtstream_puts (argp_fmtstream_t __fs, const char *__str)
{
  size_t __len = strlen (__str);
  if (__len)
    {
      size_t __wrote = argp_fmtstream_write (__fs, __str, __len);
      return __wrote == __len ? 0 : -1;
    }
  else
    return 0;
}

int
argp_fmtstream_putc (argp_fmtstream_t __fs, int __ch)
{
  if (__fs->p < __fs->end || _argp_fmtstream_ensure (__fs, 1))
    return *__fs->p++ = __ch;
  else
    return EOF;
}

/* Set __FS's left margin to __LMARGIN and return the old value.  */
size_t
argp_fmtstream_set_lmargin (argp_fmtstream_t __fs, size_t __lmargin)
{
  size_t __old;
  if ((size_t) (__fs->p - __fs->buf) > __fs->point_offs)
    _argp_fmtstream_update (__fs);
  __old = __fs->lmargin;
  __fs->lmargin = __lmargin;
  return __old;
}

/* Set __FS's right margin to __RMARGIN and return the old value.  */
size_t
argp_fmtstream_set_rmargin (argp_fmtstream_t __fs, size_t __rmargin)
{
  size_t __old;
  if ((size_t) (__fs->p - __fs->buf) > __fs->point_offs)
    _argp_fmtstream_update (__fs);
  __old = __fs->rmargin;
  __fs->rmargin = __rmargin;
  return __old;
}

/* Set FS's wrap margin to __WMARGIN and return the old value.  */
size_t
argp_fmtstream_set_wmargin (argp_fmtstream_t __fs, size_t __wmargin)
{
  size_t __old;
  if ((size_t) (__fs->p - __fs->buf) > __fs->point_offs)
    _argp_fmtstream_update (__fs);
  __old = __fs->wmargin;
  __fs->wmargin = __wmargin;
  return __old;
}

/* Return the column number of the current output point in __FS.  */
size_t
argp_fmtstream_point (argp_fmtstream_t __fs)
{
  if ((size_t) (__fs->p - __fs->buf) > __fs->point_offs)
    _argp_fmtstream_update (__fs);
  return __fs->point_col >= 0 ? __fs->point_col : 0;
}

/* End of the inlines from argp-fmtstream.h */


/* Return an argp_fmtstream that outputs to STREAM, and which prefixes lines
   written on it with LMARGIN spaces and limits them to RMARGIN columns
   total.  If WMARGIN >= 0, words that extend past RMARGIN are wrapped by
   replacing the whitespace before them with a newline and WMARGIN spaces.
   Otherwise, chars beyond RMARGIN are simply dropped until a newline.
   Returns NULL if there was an error.  */
argp_fmtstream_t
argp_make_fmtstream (FILE *stream,
		       size_t lmargin, size_t rmargin, ssize_t wmargin)
{
  argp_fmtstream_t fs;

  fs = (struct argp_fmtstream *) malloc (sizeof (struct argp_fmtstream));
  if (fs != NULL)
    {
      fs->stream = stream;

      fs->lmargin = lmargin;
      fs->rmargin = rmargin;
      fs->wmargin = wmargin;
      fs->point_col = 0;
      fs->point_offs = 0;

      fs->buf = (char *) malloc (INIT_BUF_SIZE);
      if (! fs->buf)
	{
	  free (fs);
	  fs = 0;
	}
      else
	{
	  fs->p = fs->buf;
	  fs->end = fs->buf + INIT_BUF_SIZE;
	}
    }

  return fs;
}


/* Flush FS to its stream, and free it (but don't close the stream).  */
void
argp_fmtstream_free (argp_fmtstream_t fs)
{
  _argp_fmtstream_update (fs);
  if (fs->p > fs->buf)
    {
      fwrite (fs->buf, 1, fs->p - fs->buf, fs->stream);
    }
  free (fs->buf);
  free (fs);
}

/* Process FS's buffer so that line wrapping is done from POINT_OFFS to the
   end of its buffer.  This code is mostly from glibc stdio/linewrap.c.  */
void
_argp_fmtstream_update (argp_fmtstream_t fs)
{
  char *buf, *nl;
  size_t len;

  /* Scan the buffer for newlines.  */
  buf = fs->buf + fs->point_offs;
  while (buf < fs->p)
    {
      size_t r;

      if (fs->point_col == 0 && fs->lmargin != 0)
	{
	  /* We are starting a new line.  Print spaces to the left margin.  */
	  const size_t pad = fs->lmargin;
	  if (fs->p + pad < fs->end)
	    {
	      /* We can fit in them in the buffer by moving the
		 buffer text up and filling in the beginning.  */
	      memmove (buf + pad, buf, fs->p - buf);
	      fs->p += pad; /* Compensate for bigger buffer. */
	      memset (buf, ' ', pad); /* Fill in the spaces.  */
	      buf += pad; /* Don't bother searching them.  */
	    }
	  else
	    {
	      /* No buffer space for spaces.  Must flush.  */
	      size_t i;
	      for (i = 0; i < pad; i++)
		{
		    putc (' ', fs->stream);
		}
	    }
	  fs->point_col = pad;
	}

      len = fs->p - buf;
      nl = memchr (buf, '\n', len);

      if (fs->point_col < 0)
	fs->point_col = 0;

      if (!nl)
	{
	  /* The buffer ends in a partial line.  */

	  if (fs->point_col + len < fs->rmargin)
	    {
	      /* The remaining buffer text is a partial line and fits
		 within the maximum line width.  Advance point for the
		 characters to be written and stop scanning.  */
	      fs->point_col += len;
	      break;
	    }
	  else
	    /* Set the end-of-line pointer for the code below to
	       the end of the buffer.  */
	    nl = fs->p;
	}
      else if (fs->point_col + (nl - buf) < (ssize_t) fs->rmargin)
	{
	  /* The buffer contains a full line that fits within the maximum
	     line width.  Reset point and scan the next line.  */
	  fs->point_col = 0;
	  buf = nl + 1;
	  continue;
	}

      /* This line is too long.  */
      r = fs->rmargin - 1;

      if (fs->wmargin < 0)
	{
	  /* Truncate the line by overwriting the excess with the
	     newline and anything after it in the buffer.  */
	  if (nl < fs->p)
	    {
	      memmove (buf + (r - fs->point_col), nl, fs->p - nl);
	      fs->p -= buf + (r - fs->point_col) - nl;
	      /* Reset point for the next line and start scanning it.  */
	      fs->point_col = 0;
	      buf += r + 1; /* Skip full line plus \n. */
	    }
	  else
	    {
	      /* The buffer ends with a partial line that is beyond the
		 maximum line width.  Advance point for the characters
		 written, and discard those past the max from the buffer.  */
	      fs->point_col += len;
	      fs->p -= fs->point_col - r;
	      break;
	    }
	}
      else
	{
	  /* Do word wrap.  Go to the column just past the maximum line
	     width and scan back for the beginning of the word there.
	     Then insert a line break.  */

	  char *p, *nextline;
	  int i;

	  p = buf + (r + 1 - fs->point_col);
	  while (p >= buf && !isblank (*p))
	    --p;
	  nextline = p + 1;	/* This will begin the next line.  */

	  if (nextline > buf)
	    {
	      /* Swallow separating blanks.  */
	      if (p >= buf)
		do
		  --p;
		while (p >= buf && isblank (*p));
	      nl = p + 1;	/* The newline will replace the first blank. */
	    }
	  else
	    {
	      /* A single word that is greater than the maximum line width.
		 Oh well.  Put it on an overlong line by itself.  */
	      p = buf + (r + 1 - fs->point_col);
	      /* Find the end of the long word.  */
	      do
		++p;
	      while (p < nl && !isblank (*p));
	      if (p == nl)
		{
		  /* It already ends a line.  No fussing required.  */
		  fs->point_col = 0;
		  buf = nl + 1;
		  continue;
		}
	      /* We will move the newline to replace the first blank.  */
	      nl = p;
	      /* Swallow separating blanks.  */
	      do
		++p;
	      while (isblank (*p));
	      /* The next line will start here.  */
	      nextline = p;
	    }

	  /* Note: There are a bunch of tests below for
	     NEXTLINE == BUF + LEN + 1; this case is where NL happens to fall
	     at the end of the buffer, and NEXTLINE is in fact empty (and so
	     we need not be careful to maintain its contents).  */

	  if ((nextline == buf + len + 1
	       ? fs->end - nl < fs->wmargin + 1
	       : nextline - (nl + 1) < fs->wmargin)
	      && fs->p > nextline)
	    {
	      /* The margin needs more blanks than we removed.  */
	      if (fs->end - fs->p > fs->wmargin + 1)
		/* Make some space for them.  */
		{
		  size_t mv = fs->p - nextline;
		  memmove (nl + 1 + fs->wmargin, nextline, mv);
		  nextline = nl + 1 + fs->wmargin;
		  len = nextline + mv - buf;
		  *nl++ = '\n';
		}
	      else
		/* Output the first line so we can use the space.  */
		{

		  if (nl > fs->buf)
		    fwrite (fs->buf, 1, nl - fs->buf, fs->stream);
		  putc ('\n', fs->stream);

		  len += buf - fs->buf;
		  nl = buf = fs->buf;
		}
	    }
	  else
	    /* We can fit the newline and blanks in before
	       the next word.  */
	    *nl++ = '\n';

	  if (nextline - nl >= fs->wmargin
	      || (nextline == buf + len + 1 && fs->end - nextline >= fs->wmargin))
	    /* Add blanks up to the wrap margin column.  */
	    for (i = 0; i < fs->wmargin; ++i)
	      *nl++ = ' ';
	  else
	    for (i = 0; i < fs->wmargin; ++i)

		putc (' ', fs->stream);

	  /* Copy the tail of the original buffer into the current buffer
	     position.  */
	  if (nl < nextline)
	    memmove (nl, nextline, buf + len - nextline);
	  len -= nextline - buf;

	  /* Continue the scan on the remaining lines in the buffer.  */
	  buf = nl;

	  /* Restore bufp to include all the remaining text.  */
	  fs->p = nl + len;

	  /* Reset the counter of what has been output this line.  If wmargin
	     is 0, we want to avoid the lmargin getting added, so we set
	     point_col to a magic value of -1 in that case.  */
	  fs->point_col = fs->wmargin ? fs->wmargin : -1;
	}
    }

  /* Remember that we've scanned as far as the end of the buffer.  */
  fs->point_offs = fs->p - fs->buf;
}

/* Ensure that FS has space for AMOUNT more bytes in its buffer, either by
   growing the buffer, or by flushing it.  True is returned iff we succeed. */
int
_argp_fmtstream_ensure (struct argp_fmtstream *fs, size_t amount)
{
  if ((size_t) (fs->end - fs->p) < amount)
    {
      ssize_t wrote;

      /* Flush FS's buffer.  */
      _argp_fmtstream_update (fs);

      wrote = fwrite (fs->buf, 1, fs->p - fs->buf, fs->stream);
		
      if (wrote == fs->p - fs->buf)
	{
	  fs->p = fs->buf;
	  fs->point_offs = 0;
	}
      else
	{
	  fs->p -= wrote;
	  fs->point_offs -= wrote;
	  memmove (fs->buf, fs->buf + wrote, fs->p - fs->buf);
	  return 0;
	}

      if ((size_t) (fs->end - fs->buf) < amount)
	/* Gotta grow the buffer.  */
	{
	  size_t old_size = fs->end - fs->buf;
	  size_t new_size = old_size + amount;
	  char *new_buf;

	  if (new_size < old_size || ! (new_buf = realloc (fs->buf, new_size)))
	    {
	      errno = ENOMEM;
	      return 0;
	    }

	  fs->buf = new_buf;
	  fs->end = new_buf + new_size;
	  fs->p = fs->buf;
	}
    }

  return 1;
}

ssize_t
argp_fmtstream_printf (struct argp_fmtstream *fs, const char *fmt, ...)
{
  int out;
  size_t avail;
  size_t size_guess = PRINTF_SIZE_GUESS; /* How much space to reserve. */

  do
    {
      va_list args;

      if (! _argp_fmtstream_ensure (fs, size_guess))
	return -1;

      va_start (args, fmt);
      avail = fs->end - fs->p;
      out = vsnprintf (fs->p, avail, fmt, args);
      va_end (args);
      if ((size_t) out >= avail)
	size_guess = out + 1;
    }
  while ((size_t) out >= avail);

  fs->p += out;

  return out;
}

