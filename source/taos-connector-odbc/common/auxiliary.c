/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#define _XOPEN_SOURCE

#include "helpers.h"

#include "iconv_wrapper.h"

#include <errno.h>
#ifndef _WIN32
#include <libgen.h>
#endif
#include <time.h>

#ifdef _WIN32
// NOTE: moved to win_port_cpp.cpp
#else
const char* tod_strptime(const char *s, const char *format, struct tm *tm)
{
  return strptime(s, format, tm);
}
#endif

const char* tod_strptime_with_len(const char *s, size_t len, const char *fmt, struct tm *tm)
{
  char buf[4096]; *buf = '\0';
  int n = snprintf(buf, sizeof(buf), "%.*s", (int)len, s);
  if (n < 0) {
    errno = EINVAL; // FIXME:
    return NULL;
  }
  if ((size_t)n >= sizeof(buf)) {
    errno = E2BIG;
    return NULL;
  }
  const char *next = tod_strptime(buf, fmt, tm);
  if (!next) return NULL;

  return s + (next - buf);
}

static time_t _local_timezone = 0;

static void _init_local_timezone(void)
{
  // const char *dt  = "1970-01-01 00:00:00";
  // const char *fmt = "%Y-%m-%d %H:%M:%S";
  // struct tm tm0_local;
  // tod_strptime(dt, fmt, &tm0_local);
  // _local_timezone = 0 - mktime(&tm0_local);

  // https://learn.microsoft.com/en-us/cpp/c-runtime-library/reference/mktime-mktime32-mktime64?view=msvc-170
  time_t t0 = 12*60*60;
  struct tm tm;
  gmtime_r(&t0, &tm);
  _local_timezone = t0 - mktime(&tm);
}

time_t tod_get_local_timezone(void)
{
  static int init = 0;
  if (!init) {
    static pthread_once_t once = PTHREAD_ONCE_INIT;
    pthread_once(&once, _init_local_timezone);
    init = 1;
  }
  return _local_timezone;
}

static void _get_local_time(struct timeval *tv0, struct tm *tm0)
{
  gettimeofday(tv0, NULL);
  time_t t0 = (time_t)tv0->tv_sec;
  localtime_r(&t0, tm0);
}

const char* tod_get_format_current_local_timestamp_ms(char *s, size_t n)
{
  struct timeval tv0;
  struct tm tm0;
  _get_local_time(&tv0, &tm0);

  snprintf(s, n, "%04d-%02d-%02d %02d:%02d:%02d.%03zd",
    tm0.tm_year + 1900, tm0.tm_mon + 1, tm0.tm_mday,
    tm0.tm_hour, tm0.tm_min, tm0.tm_sec,
    (size_t)(tv0.tv_usec/1000));

  return s;
}

const char* tod_get_format_current_local_timestamp_us(char *s, size_t n)
{
  struct timeval tv0;
  struct tm tm0;
  _get_local_time(&tv0, &tm0);

  snprintf(s, n, "%04d-%02d-%02d %02d:%02d:%02d.%06zd",
    tm0.tm_year + 1900, tm0.tm_mon + 1, tm0.tm_mday,
    tm0.tm_hour, tm0.tm_min, tm0.tm_sec,
    (size_t)tv0.tv_usec);

  return s;
}

#ifdef _WIN32
char* tod_basename(const char *path, char *buf, size_t sz)
{
  char *file = NULL;
  DWORD dw;
  dw = GetFullPathName(path, (DWORD)sz, buf, &file);
  if (dw == 0) {
    errno = GetLastError();
    return NULL;
  }
  if (dw >= sz) {
    errno = E2BIG;
    return NULL;
  }

  if (!file) {
    size_t nr = strlen(buf);
    if (nr > 3) buf[nr-1] = '\0';
    char tmp[PATH_MAX+1];
    int n;
    n = snprintf(tmp, sizeof(tmp), "%s", buf);
    if (n < 0) {
      return NULL;
    }
    if ((size_t)n >= sizeof(tmp)) {
      errno = E2BIG;
      return NULL;
    }

    dw = GetFullPathName(tmp, (DWORD)sz, buf, &file);
    if (dw == 0) {
      errno = GetLastError();
      return NULL;
    }
    if (dw >= sz) {
      errno = E2BIG;
      return NULL;
    }

    if (!file) file = buf;
  }

  return file;
}
#elif defined(__APPLE__)
char* tod_basename(const char *path, char *buf, size_t sz)
{
  char tmp[PATH_MAX+1];
  int n;
  n = snprintf(tmp, sizeof(tmp), "%s", path);
  if (n < 0) {
    return NULL;
  }
  if ((size_t)n >= sizeof(tmp)) {
    errno = E2BIG;
    return NULL;
  }
  if ((size_t)n >= sz) {
    errno = E2BIG;
    return NULL;
  }
  return basename_r(tmp, buf);
}
#else
char* tod_basename(const char *path, char *buf, size_t sz)
{
  char tmp[PATH_MAX+1];
  int n;
  n = snprintf(tmp, sizeof(tmp), "%s", path);
  if (n < 0) {
    return NULL;
  }
  if ((size_t)n >= sizeof(tmp)) {
    errno = E2BIG;
    return NULL;
  }
  char *p = basename(tmp);
  if (!p) return NULL;

  n = snprintf(buf, sz, "%s", p);
  if (n < 0) {
    return NULL;
  }
  if ((size_t)n >= sz) {
    errno = E2BIG;
    return NULL;
  }
  return buf;
}
#endif

#ifdef _WIN32
char* tod_dirname(const char *path, char *buf, size_t sz)
{
  char *file = NULL;
  DWORD dw;
  dw = GetFullPathName(path, (DWORD)sz, buf, &file);
  if (dw == 0) {
    errno = GetLastError();
    return NULL;
  }
  if (dw >= sz) {
    errno = E2BIG;
    return NULL;
  }

  if (!file) {
    size_t nr = strlen(buf);
    if (nr > 3) buf[nr-1] = '\0';
    char tmp[PATH_MAX+1];
    int n;
    n = snprintf(tmp, sizeof(tmp), "%s", buf);
    if (n < 0) {
      return NULL;
    }
    if ((size_t)n >= sizeof(tmp)) {
      errno = E2BIG;
      return NULL;
    }

    dw = GetFullPathName(tmp, (DWORD)sz, buf, &file);
    if (dw == 0) {
      errno = GetLastError();
      return NULL;
    }
    if (dw >= sz) {
      errno = E2BIG;
      return NULL;
    }
  }

  if (file) {
    size_t nr = strlen(buf);
    if (nr > 3) file[-1] = '\0';
  }

  return buf;
}
#elif defined(__APPLE__)
char* tod_dirname(const char *path, char *buf, size_t sz)
{
  char tmp[PATH_MAX+1];
  int n = snprintf(tmp, sizeof(tmp), "%s", path);
  if (n < 0) return NULL;
  if ((size_t)n >= sizeof(tmp)) {
    errno = E2BIG;
    return NULL;
  }
  if ((size_t)n >= sz) {
    errno = E2BIG;
    return NULL;
  }

  return dirname_r(tmp, buf);
}
#else
char* tod_dirname(const char *path, char *buf, size_t sz)
{
  char tmp[PATH_MAX+1];
  int n;
  n = snprintf(tmp, sizeof(tmp), "%s", path);
  if (n < 0) {
    return NULL;
  }
  if ((size_t)n >= sizeof(tmp)) {
    errno = E2BIG;
    return NULL;
  }
  char *p = dirname(tmp);
  if (!p) return NULL;

  n = snprintf(buf, sz, "%s", p);
  if (n < 0) {
    return NULL;
  }
  if ((size_t)n >= sz) {
    errno = E2BIG;
    return NULL;
  }
  return buf;
}
#endif

char* tod_strncpy(char *dest, const char *src, size_t n)
{
#if (!defined(_WIN32)) && (!defined(__APPLE__))        /* { */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstringop-truncation"
#endif                                                 /* } */
  return strncpy(dest, src, n);
#if (!defined(_WIN32)) && (!defined(__APPLE__))        /* { */
#pragma GCC diagnostic pop
#endif                                                 /* } */
}

int tod_conv(const char *fromcode, const char *tocode, const char *src, size_t slen, char *dst, size_t dlen)
{
  iconv_t cnv = iconv_open(tocode, fromcode);
  if (cnv == (iconv_t)-1) return -1;

  char         *inbuf                   = (char*)src;
  char         *outbuf                  = dst;
  size_t        inbytesleft             = slen;
  size_t        outbytesleft            = dlen;

  size_t n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  iconv_close(cnv);

  if (n == (size_t)-1) return -1;
  if (n > 0) return -1;

  n = dlen - outbytesleft;

  for (size_t i=0; i<outbytesleft && i<4; ++i) {
    outbuf[i] = '\0';
  }

  return (int)n;
}

static const uint8_t _hex_map[] = {
  /* 00 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* 10 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* 20 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* 30 */   0,  1,  2,  3,  4,  5,  6,  7,  8,  9,'_','_','_','_','_','_',
  /* 40 */ '_', 10, 11, 12, 13, 14, 15,'_','_','_','_','_','_','_','_','_',
  /* 50 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* 60 */ '_', 10, 11, 12, 13, 14, 15,'_','_','_','_','_','_','_','_','_',
  /* 70 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* 80 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* 90 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* A0 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* B0 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* C0 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* D0 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* E0 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
  /* F0 */ '_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_',
};

void tod_hex2bytes_unsafe(
    const char *hex, size_t n,
    unsigned char *bytes)
{
  const uint8_t *s = (const uint8_t*)hex;
  uint8_t       *d = (uint8_t*)bytes;
  for (size_t i=0; i<n; i+=2) {
    uint16_t v1 = _hex_map[*s++];
    uint16_t v2 = _hex_map[*s++];
    *d++ = (v1 << 4) | v2;
  }
}

unsigned char* tod_hex2bytes(unsigned char *buf, size_t n, const char *hex, size_t nr)
{
  if (nr & 1) {
    errno = EINVAL;
    return NULL;
  }
  if (n <= nr / 2) {
    errno = E2BIG;
    return NULL;
  }
  tod_hex2bytes_unsafe(hex, nr, (unsigned char*)buf);
  return buf;
}

const char* tod_hexify(char *hex, size_t nr, const unsigned char *bin, size_t n)
{
  static const char digits[] = {
    '0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'
  };
  if (nr / 2 <= n) {
    errno = E2BIG;
    return NULL;
  }
  char *p = hex;
  for (size_t i=0; i<n; ++i) {
    unsigned char c = *bin++;
    *hex++ = digits[c>>4];
    *hex++ = digits[c&0x0f];
  }
  *hex = '\0';
  return p;
}

size_t tod_wstrnlen(const char *buf, size_t len)
{
  size_t i = 0;
  for (i=0; i+1 < len; i += 2) {
    const char c1 = buf[i];
    const char c2 = buf[i+1];
    if (c1) continue;
    if (c2) continue;
    break;
  }

  return i;
}

size_t tod_wstrlen(const char *buf)
{
  size_t i = 0;

  for (i=0; 1; i += 2) {
    const char c1 = buf[i];
    const char c2 = buf[i+1];
    if (c1) continue;
    if (c2) continue;
    break;
  }

  return i;
}


