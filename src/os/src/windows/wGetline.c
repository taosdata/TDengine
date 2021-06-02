/* getline.c -- Replacement for GNU C library function getline

Copyright (C) 1993 Free Software Foundation, Inc.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as
published by the Free Software Foundation; either version 2 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License for more details.  */

/* Written by Jan Brittenson, bson@gnu.ai.mit.edu.  */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <sys/types.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <stdint.h>

#if STDC_HEADERS
#include <stdlib.h>
#else
char *malloc(), *realloc();
#endif

/* Always add at least this many bytes when extending the buffer.  */
#define MIN_CHUNK 64

/* Read up to (and including) a TERMINATOR from STREAM into *LINEPTR
+ OFFSET (and null-terminate it). *LINEPTR is a pointer returned from
malloc (or NULL), pointing to *N characters of space.  It is realloc'd
as necessary.  Return the number of characters read (not including the
null terminator), or -1 on error or EOF.  On a -1 return, the caller
should check feof(), if not then errno has been set to indicate
the error.  */

int32_t getstr(char **lineptr, size_t *n, FILE *stream, char terminator, int32_t offset) {
  int32_t nchars_avail; /* Allocated but unused chars in *LINEPTR.  */
  char *  read_pos;     /* Where we're reading into *LINEPTR. */
  int32_t ret;

  if (!lineptr || !n || !stream) {
    errno = EINVAL;
    return -1;
  }

  if (!*lineptr) {
    *n = MIN_CHUNK;
    *lineptr = malloc(*n);
    if (!*lineptr) {
      errno = ENOMEM;
      return -1;
    }
  }

  nchars_avail = (int32_t)(*n - offset);
  read_pos = *lineptr + offset;

  for (;;) {
    int32_t          save_errno;
    register int32_t c = getc(stream);

    save_errno = errno;

    /* We always want at least one char left in the buffer, since we
    always (unless we get an error while reading the first char)
    NUL-terminate the line buffer.  */

    assert((*lineptr + *n) == (read_pos + nchars_avail));
    if (nchars_avail < 2) {
      if (*n > MIN_CHUNK)
        *n *= 2;
      else
        *n += MIN_CHUNK;

      nchars_avail = (int32_t)(*n + *lineptr - read_pos);
      *lineptr = realloc(*lineptr, *n);
      if (!*lineptr) {
        errno = ENOMEM;
        return -1;
      }
      read_pos = *n - nchars_avail + *lineptr;
      assert((*lineptr + *n) == (read_pos + nchars_avail));
    }

    if (ferror(stream)) {
      /* Might like to return partial line, but there is no
      place for us to store errno.  And we don't want to just
      lose errno.  */
      errno = save_errno;
      return -1;
    }

    if (c == EOF) {
      /* Return partial line, if any.  */
      if (read_pos == *lineptr)
        return -1;
      else
        break;
    }

    *read_pos++ = c;
    nchars_avail--;

    if (c == terminator) /* Return the line.  */
      break;
  }

  /* Done - NUL terminate and return the number of chars read.  */
  *read_pos = '\0';

  ret = (int32_t)(read_pos - (*lineptr + offset));
  return ret;
}

int32_t tgetline(char **lineptr, size_t *n, FILE *stream) { return getstr(lineptr, n, stream, '\n', 0); }