/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "os.h"
#include <wchar.h>
#include <wctype.h>

typedef struct CharsetPair {
  char *oldCharset;
  char *newCharset;
} CharsetPair;

char *taosCharsetReplace(char *charsetstr) {
  CharsetPair charsetRep[] = {
      { "utf8", "UTF-8" }, { "936", "CP936" },
  };

  for (int32_t i = 0; i < tListLen(charsetRep); ++i) {
    if (strcasecmp(charsetRep[i].oldCharset, charsetstr) == 0) {
      return strdup(charsetRep[i].newCharset);
    }
  }

  return strdup(charsetstr);
}

int64_t taosStr2int64(char *str) {
  char *endptr = NULL;
  return strtoll(str, &endptr, 10);
}

#ifdef USE_LIBICONV
#include "iconv.h"

int32_t taosUcs4ToMbs(void *ucs4, int32_t ucs4_max_len, char *mbs) {
  iconv_t cd = iconv_open(tsCharset, DEFAULT_UNICODE_ENCODEC);
  size_t  ucs4_input_len = ucs4_max_len;
  size_t  outLen = ucs4_max_len;
  if (iconv(cd, (char **)&ucs4, &ucs4_input_len, &mbs, &outLen) == -1) {
    iconv_close(cd);
    return -1;
  }

  iconv_close(cd);
  return (int32_t)(ucs4_max_len - outLen);
}

bool taosMbsToUcs4(char *mbs, size_t mbsLength, char *ucs4, int32_t ucs4_max_len, int32_t *len) {
  memset(ucs4, 0, ucs4_max_len);
  iconv_t cd = iconv_open(DEFAULT_UNICODE_ENCODEC, tsCharset);
  size_t  ucs4_input_len = mbsLength;
  size_t  outLeft = ucs4_max_len;
  if (iconv(cd, &mbs, &ucs4_input_len, &ucs4, &outLeft) == -1) {
    iconv_close(cd);
    return false;
  }

  iconv_close(cd);
  if (len != NULL) {
    *len = (int32_t)(ucs4_max_len - outLeft);
    if (*len < 0) {
      return false;
    }
  }

  return true;
}

bool taosValidateEncodec(const char *encodec) {
  iconv_t cd = iconv_open(encodec, DEFAULT_UNICODE_ENCODEC);
  if (cd == (iconv_t)(-1)) {
    return false;
  }

  iconv_close(cd);
  return true;
}

#else

int32_t taosUcs4ToMbs(void *ucs4, int32_t ucs4_max_len, char *mbs) {
  mbstate_t state = {0};
  int32_t   len = (int32_t)wcsnrtombs(NULL, (const wchar_t **)&ucs4, ucs4_max_len / 4, 0, &state);
  if (len < 0) {
    return -1;
  }

  memset(&state, 0, sizeof(state));
  len = wcsnrtombs(mbs, (const wchar_t **)&ucs4, ucs4_max_len / 4, (size_t)len, &state);
  if (len < 0) {
    return -1;
  }

  return len;
}

bool taosMbsToUcs4(char *mbs, size_t mbsLength, char *ucs4, int32_t ucs4_max_len, int32_t *len) {
  memset(ucs4, 0, ucs4_max_len);
  mbstate_t state = {0};
  int32_t retlen = mbsnrtowcs((wchar_t *)ucs4, (const char **)&mbs, mbsLength, ucs4_max_len / 4, &state);
  *len = retlen;

  return retlen >= 0;
}

bool taosValidateEncodec(const char *encodec) {
  return true;
}

#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

/*
 * windows implementation
 */

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
      char* lineptr1 = realloc(*lineptr, *n);
      if (!lineptr1) {
        errno = ENOMEM;
        return -1;
      }
      *lineptr = lineptr1;

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


/*
 * Get next token from string *stringp, where tokens are possibly-empty
 * strings separated by characters from delim.
 *
 * Writes NULs into the string at *stringp to end tokens.
 * delim need not remain constant from call to call.
 * On return, *stringp points past the last NUL written (if there might
 * be further tokens), or is NULL (if there are definitely no moretokens).
 *
 * If *stringp is NULL, strsep returns NULL.
 */
char *strsep(char **stringp, const char *delim) {
  char *      s;
  const char *spanp;
  int32_t     c, sc;
  char *tok;
  if ((s = *stringp) == NULL)
    return (NULL);
  for (tok = s;;) {
    c = *s++;
    spanp = delim;
    do {
      if ((sc = *spanp++) == c) {
        if (c == 0)
          s = NULL;
        else
          s[-1] = 0;
        *stringp = s;
        return (tok);
      }
    } while (sc != 0);
  }
  /* NOTREACHED */
}

char *getpass(const char *prefix) {
  static char passwd[TSDB_KEY_LEN] = {0};
  memset(passwd, 0, TSDB_KEY_LEN);
  printf("%s", prefix);

  int32_t index = 0;
  char    ch;
  while (index < TSDB_KEY_LEN) {
    ch = getch();
    if (ch == '\n' || ch == '\r') {
      break;
    } else {
      passwd[index++] = ch;
    }
  }

  return passwd;
}

int32_t twcslen(const wchar_t *wcs) {
  int32_t *wstr = (int32_t *)wcs;
  if (NULL == wstr) {
    return 0;
  }

  int32_t n = 0;
  while (1) {
    if (0 == *wstr++) {
      break;
    }
    n++;
  }

  return n;
}

int32_t tasoUcs4Compare(void *f1_ucs4, void *f2_ucs4, int32_t bytes) {
  for (int32_t i = 0; i < bytes; ++i) {
    int32_t f1 = *(int32_t *)((char *)f1_ucs4 + i * 4);
    int32_t f2 = *(int32_t *)((char *)f2_ucs4 + i * 4);

    if ((f1 == 0 && f2 != 0) || (f1 != 0 && f2 == 0)) {
      return f1 - f2;
    } else if (f1 == 0 && f2 == 0) {
      return 0;
    }

    if (f1 != f2) {
      return f1 - f2;
    }
  }

  return 0;

#if 0
  int32_t ucs4_max_len = bytes + 4;
  char *f1_mbs = calloc(bytes, 1);
  char *f2_mbs = calloc(bytes, 1);
  if (taosUcs4ToMbs(f1_ucs4, ucs4_max_len, f1_mbs) < 0) {
    return -1;
  }
  if (taosUcs4ToMbs(f2_ucs4, ucs4_max_len, f2_mbs) < 0) {
    return -1;
  }
  int32_t ret = strcmp(f1_mbs, f2_mbs);
  free(f1_mbs);
  free(f2_mbs);
  return ret;
#endif
}

/* Copy memory to memory until the specified number of bytes
has been copied, return pointer to following byte.
Overlap is NOT handled correctly. */
void *mempcpy(void *dest, const void *src, size_t len) {
  return (char*)memcpy(dest, src, len) + len;
}

/* Copy SRC to DEST, returning the address of the terminating '\0' in DEST.  */
char *stpcpy (char *dest, const char *src) {
  size_t len = strlen (src);
  return (char*)memcpy(dest, src, len + 1) + len;
}

/* Copy no more than N characters of SRC to DEST, returning the address of
   the terminating '\0' in DEST, if any, or else DEST + N.  */
char *stpncpy (char *dest, const char *src, size_t n) {
  size_t size = strnlen (src, n);
  memcpy (dest, src, size);
  dest += size;
  if (size == n)
    return dest;
  return memset (dest, '\0', n - size);
}

#else

/*
 * linux and darwin implementation
 */

int32_t tasoUcs4Compare(void *f1_ucs4, void *f2_ucs4, int32_t bytes, int8_t ncharSize) {
  return wcsncmp((wchar_t *)f1_ucs4, (wchar_t *)f2_ucs4, bytes / ncharSize);
}

#endif