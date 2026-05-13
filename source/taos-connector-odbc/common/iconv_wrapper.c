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

#include "iconv_wrapper.h"

#include "helpers.h"
#include "logger.h"

#ifdef _WIN32                       /* { */
#ifndef USE_WIN_ICONV        /* { */

#ifdef LOG_ICONV          /* { */
#define LOGE_ICONV(file, line, func, fmt, ...) do {                                               \
  tod_logger_write(tod_get_system_logger(), LOGGER_ERROR, tod_get_system_logger_level(),          \
    file, line, func,                                                                             \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)
#else                     /* }{ */
#define LOGE_ICONV(...)
#endif                    /* } */

static int call_MultiByteToWideChar(const char *file, int line, const char *func,
  UINT CodePage, DWORD dwFlags, LPCCH lpMultiByteStr, int cbMultiByte, LPWSTR lpWideCharStr, int cchWideChar)
{
  LOGE_ICONV(file, line, func, "MultiByteToWideChar(codepage:%d,dwFlags:0x%x,lpMultiByteStr:%p,chMultiByte:%d,lpWideCharStr:%p,cchWideChar:%d) ...",
    CodePage, dwFlags, lpMultiByteStr, cbMultiByte, lpWideCharStr, cchWideChar);
  int n = MultiByteToWideChar(CodePage, dwFlags, lpMultiByteStr, cbMultiByte, lpWideCharStr, cchWideChar);
  LOGE_ICONV(file, line, func, "MultiByteToWideChar(codepage:%d,dwFlags:0x%x,lpMultiByteStr:%p,chMultiByte:%d,lpWideCharStr:%p,cchWideChar:%d) -> %d",
    CodePage, dwFlags, lpMultiByteStr, cbMultiByte, lpWideCharStr, cchWideChar, n);
  return n;
}

static int call_WideCharToMultiByte(const char *file, int line, const char *func,
  UINT CodePage, DWORD dwFlags, LPCWCH lpWideCharStr, int cchWideChar, LPSTR lpMultiByteStr, int cbMultiByte, LPCCH lpDefaultChar, LPBOOL lpUsedDefaultChar)
{
  LOGE_ICONV(file, line, func, "WideCharToMultiByte(CodePage:%d,dwFlags:0x%x,lpWideCharStr:%p,cchWideChar:%d,lpMultiByteStr:%p,cbMultiByte:%d,lpDefaultChar:%p,lpUsedDefaultChar:%p) ...",
    CodePage, dwFlags, lpWideCharStr, cchWideChar, lpMultiByteStr, cbMultiByte, lpDefaultChar, lpUsedDefaultChar);
  int n = WideCharToMultiByte(CodePage, dwFlags, lpWideCharStr, cchWideChar, lpMultiByteStr, cbMultiByte, lpDefaultChar, lpUsedDefaultChar);
  LOGE_ICONV(file, line, func, "WideCharToMultiByte(CodePage:%d,dwFlags:0x%x,lpWideCharStr:%p,cchWideChar:%d,lpMultiByteStr:%p,cbMultiByte:%d,lpDefaultChar:%p,lpUsedDefaultChar:%p) -> %d",
    CodePage, dwFlags, lpWideCharStr, cchWideChar, lpMultiByteStr, cbMultiByte, lpDefaultChar, lpUsedDefaultChar, n);
  return n;
}

#define CALL_MultiByteToWideChar(...) call_MultiByteToWideChar(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_WideCharToMultiByte(...) call_WideCharToMultiByte(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

typedef struct cache_s           cache_t;
struct cache_s {
  char                 *buf;
  size_t                sz;
  size_t                nr;
};

static void cache_reset(cache_t *cache)
{
  cache->nr = 0;
}

static void cache_release(cache_t *cache)
{
  cache_reset(cache);
  if (cache->buf) {
    free(cache->buf);
    cache->buf = NULL;
  }
  cache->sz = 0;
}

static int cache_keep(cache_t *cache, size_t sz)
{
  if (sz <= cache->sz) return 0;
  sz = (sz + 255) / 256 * 256;
  char *p = (char*)realloc(cache->buf, sz);
  if (!p) return -1;
  cache->buf = p;
  cache->sz  = sz;
  // TOD_LOGE("cached expanded:%zd", cache->sz);
  return 0;
}

typedef struct cpinfo_s               cpinfo_t;
struct cpinfo_s {
  UINT           cp;
  CPINFO         info;
  char           name[64];
  char          *buf;
};

static void cpinfo_release(cpinfo_t *info)
{
  if (info->buf) {
    free(info->buf);
    info->buf = NULL;
  }
}

typedef struct buffer_s                buffer_t;
struct buffer_s {
  char                  *buf;
  size_t                 len;
};

typedef struct codepage_s                  codepage_t;
struct codepage_s {
  char                  name[64];
  UINT                  cp;
  CPINFOEX              info;

  DWORD                 flags_m2w;
  DWORD                 flags_w2m;
  BOOL                  usedDefaultChar;
  LPBOOL                lpUsedDefaultChar;

  int (*src_to_widechar)(codepage_t *codepage, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft);
  int (*widechar_to_dst)(codepage_t *codepage, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft);
  int (*ilseq_or_inval)(codepage_t *codepage, const unsigned char *remain, size_t remainlen);

  uint8_t               ucs2:1;
  uint8_t               be:1;
};

struct iconv_s {
  cpinfo_t        from;
  cpinfo_t        to;

  cache_t         wchar;
  cache_t         mbcs;

  size_t (*conv)(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft);

  codepage_t      codepage_from;
  codepage_t      codepage_to;

  uint8_t         use_codepage:1;
};

/*
 * http://www.faqs.org/rfcs/rfc2781.html
 */
static uint32_t utf16_to_ucs4(const uint16_t *wbuf, uint32_t *nbuf)
{
    uint32_t wc = wbuf[0];
    *nbuf = 1;
    if (0xD800 <= wbuf[0] && wbuf[0] <= 0xDBFF) {
        wc = ((wbuf[0] & 0x3FF) << 10) + (wbuf[1] & 0x3FF) + 0x10000;
        *nbuf = 2;
    }
    return wc;
}

static void ucs4_to_utf16(uint32_t wc, uint16_t *wbuf, uint32_t *wbufsize)
{
    if (wc < 0x10000)
    {
        wbuf[0] = wc;
        *wbufsize = 1;
    }
    else
    {
        wc -= 0x10000;
        wbuf[0] = 0xD800 | ((wc >> 10) & 0x3FF);
        wbuf[1] = 0xDC00 | (wc & 0x3FF);
        *wbufsize = 2;
    }
}

static int m2w(codepage_t *codepage, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int err = 0;

  UINT     cp         = codepage->cp;
  LPCCH    src        = *inbuf;
  int      src_len    = (int)*inbytesleft;
  LPWSTR   dst        = (LPWSTR)*outbuf;
  int      dst_len    = (int)(*outbytesleft / sizeof(*dst));

  DWORD dw_m2w = codepage->flags_m2w;
  DWORD dw_w2m = codepage->flags_w2m;

  int   recover = codepage->info.MaxCharSize;

  int nr = 0;
  LPBOOL lpUsedDefaultChar = codepage->lpUsedDefaultChar;

  LPSTR prev = NULL;

again:

  nr = CALL_MultiByteToWideChar(cp, dw_m2w, src, src_len, dst, dst_len);
  if (nr > 0) {
    codepage->usedDefaultChar = FALSE;
    // TODO: nonreversible convertion
    int n = CALL_WideCharToMultiByte(cp, dw_w2m, dst, nr, NULL, 0, NULL, lpUsedDefaultChar);
    if (n > 0) {
      if (!codepage->usedDefaultChar) {
        *inbuf           += n;
        *inbytesleft     -= n;
        n = nr * sizeof(*dst);
        *outbuf          += n;
        *outbytesleft    -= n;
        return 0;
      }
      // TOD_LOGE("back convertion (wchar_t->%d) result in DEFAULT_CHAR_USED", cp);
      err = EILSEQ;
    } else {
      // TODO: nonreversible convertion
      // TOD_LOGE("back convertion (wchar_t->%d) failed", cp);
      err = EINVAL; // FIXME: for the moment
    }
  } else {
    // TOD_LOGE("convertion (%d->wchar_t) failed", cp);
    switch (GetLastError()) {
      case ERROR_INSUFFICIENT_BUFFER:
        err = E2BIG;
        break;
      case ERROR_INVALID_FLAGS:
      case ERROR_INVALID_PARAMETER:
        err = EINVAL;
        break;
      case ERROR_NO_UNICODE_TRANSLATION:
        err = EINVAL;
        break;
      default:
        err = EINVAL;
        break;
    }
  }

  if (recover-- > 1) {
    --src_len;
    if (src_len == 0) return err;

    goto again;
  }

  recover = codepage->info.MaxCharSize;
  src_len /= 2;
  if (src_len == 0) return err;

  goto again;
}

// all-in-bytes
static int w2m(codepage_t *codepage, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int err = 0;

  UINT        cp            = codepage->cp;
  LPCWSTR     src           = (LPCWSTR)*inbuf;
  int         src_len       = (int)(*inbytesleft / sizeof(*src));
  LPSTR       dst           = (LPSTR)*outbuf;
  int         dst_len       = (int)*outbytesleft;

  DWORD dw_m2w = codepage->flags_m2w;
  DWORD dw_w2m = codepage->flags_w2m;

  int n = 0;
  LPBOOL lpUsedDefaultChar = codepage->lpUsedDefaultChar;

again:
  codepage->usedDefaultChar = FALSE;

  n = CALL_WideCharToMultiByte(cp, dw_w2m, src, src_len, dst, dst_len, NULL, lpUsedDefaultChar);
  if (n > 0) {
    if (!codepage->usedDefaultChar) {
      int nr = CALL_MultiByteToWideChar(cp, dw_m2w, dst, n, NULL, 0);
      if (nr > 0) {
        *outbuf              += n;
        *outbytesleft        -= n;
        n = nr * sizeof(*src);
        *inbuf               += n;
        *inbytesleft         -= n;
        return 0;
      }
      // TOD_LOGE("back convertion (%d->wchar_t) failed", cp);
      err = EINVAL;
    } else {
      // TOD_LOGE("back convertion (%d->wchar_t) result in DEFAULT_CHAR_USED", cp);
      switch (GetLastError()) {
        case ERROR_INSUFFICIENT_BUFFER:
          err = E2BIG;
          break;
        case ERROR_INVALID_FLAGS:
        case ERROR_INVALID_PARAMETER:
          err = EINVAL;
          break;
        case ERROR_NO_UNICODE_TRANSLATION:
          err = EINVAL;
          break;
        default:
          err = EINVAL;
          break;
      }
    }
  } else {
    // WLOGE(err, "convertion (wchar_t->%d)", cp);
    switch (GetLastError()) {
      case ERROR_INSUFFICIENT_BUFFER:
        err = E2BIG;
        break;
      case ERROR_INVALID_FLAGS:
      case ERROR_INVALID_PARAMETER:
        err = EINVAL;
        break;
      case ERROR_NO_UNICODE_TRANSLATION:
        err = EINVAL;
        break;
      default:
        err = EINVAL;
        break;
    }
  }

  src_len /= 2;

  if (src_len == 0) return err;

  goto again;
}


static int iconv_get_acp(const char *code, cpinfo_t *info)
{
#define RECORD(x, y) { x, y}
  struct {
    const char          *name;
    UINT                 cp;
  } names[] = {
    RECORD("GB18030",        54936),
    RECORD("UTF8",           CP_UTF8),
    RECORD("UTF-8",          CP_UTF8),
    RECORD("UCS-2LE",        1200),
    RECORD("UCS-2BE",        1201),
    RECORD("UCS-4LE",        12000),
    RECORD("CP437",          437),
    RECORD("CP850",          850),
    RECORD("CP858",          858),
    RECORD("CP1252",         1252),
  };
  const size_t nr_names = sizeof(names) / sizeof(names[0]);
#undef RECORD

  for (size_t i=0; i<nr_names; ++i) {
    if (tod_strcasecmp(code, names[i].name)) continue;
    info->cp = names[i].cp;
    goto cpinfo;
  }

  // TOD_LOGE("not implemented yet: encoder %s", code);
  goto err;

cpinfo:
  snprintf(info->name, sizeof(info->name), "%s", code);
  if (info->cp == 1200) return 0;
  if (info->cp == 1201) return 0;
  if (info->cp == 12000) return 0;
  if (GetCPInfo(info->cp, &info->info)) {
    info->buf = calloc(info->info.MaxCharSize, sizeof(*info->buf));
    if (!info->buf) goto err;
    return 0;
  } else {
    // TOD_LOGE("GetCPInfo(%d) failed:[0x%x]", info->cp, GetLastError());
  }

err:
  errno = EINVAL;
  return (size_t)-1;
}

static int keep_wcs(iconv_t cd, size_t wnr)
{
  return cache_keep(&cd->wchar, wnr * 2);
}

static int add_wcs(iconv_t cd, WCHAR wc)
{
  int r = keep_wcs(cd, cd->wchar.sz + 2);
  if (r) return -1;
  memcpy(cd->wchar.buf + cd->wchar.nr, &wc, 2);
  cd->wchar.nr += 2;
  return 0;
}

static int keep_cs(iconv_t cd, size_t sz)
{
  return cache_keep(&cd->mbcs, sz);
}

static size_t do_finalize(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft, buffer_t *input, buffer_t *output)
{
  int n = 0;

  size_t input_delta  = *inbytesleft - input->len;
  size_t output_delta = *outbytesleft - output->len;
  // TOD_LOGE("delta:#%zd -> %zd", input_delta, output_delta);
  // TOD_LOGE("len:#%zd;#%zd", input->len, output->len);

  *inbuf               = input->buf;
  *inbytesleft         = input->len;

  *outbuf              = output->buf;
  *outbytesleft        = output->len;

  if (input->len == 0) {
    errno = 0;
    return 0;
  }

  DWORD dwFlags = MB_ERR_INVALID_CHARS;
  n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags, input->buf, (int)input->len, NULL, 0);
  if (n > 0) {
    errno = E2BIG;
    return (size_t)-1;
  }

  char buf[4096];
  DWORD e = GetLastError();
  n = FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM|FORMAT_MESSAGE_IGNORE_INSERTS, NULL, e, MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT), buf, sizeof(buf), NULL);
  buf[n] = '\0';
  switch (e) {
    case ERROR_INSUFFICIENT_BUFFER:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = E2BIG;
      return (size_t)-1;
    case ERROR_INVALID_FLAGS:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return (size_t)-1;
    case ERROR_INVALID_PARAMETER:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return (size_t)-1;
    case ERROR_NO_UNICODE_TRANSLATION:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return (size_t)-1;
    default:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return (size_t)-1;
  }
}

static int to_wchar(iconv_t cd, buffer_t *mbcs)
{
  int r = 0;
  int n = 0;
  DWORD e = 0;
  char buf[4096];

  const char * const    src         = mbcs->buf;
  size_t       const    src_len     = mbcs->len;

  DWORD dwFlags = MB_ERR_INVALID_CHARS;

again:
  n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags, src, (int)src_len, (WCHAR*)cd->wchar.buf, (int)(cd->wchar.sz / 2));
  if (n > 0) {
    if (cd->wchar.buf) {
      cd->wchar.nr   = n * 2;
      mbcs->buf     += src_len;
      mbcs->len     -= src_len;
      // TOD_LOGE("src_len:%zd;wchar.nr:%zd;n:%d", src_len, cd->wchar.nr, n);
      return 0;
    }
    r = keep_wcs(cd, n * 2);
    if (r) {
      // TOD_LOGE("");
      return (size_t)-1;
    }
    goto again;
  }

  e = GetLastError();
  n = FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM|FORMAT_MESSAGE_IGNORE_INSERTS, NULL, e, MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT), buf, sizeof(buf), NULL);
  buf[n] = '\0';
  switch (e) {
    case ERROR_INSUFFICIENT_BUFFER:
      r = keep_wcs(cd, cd->wchar.sz + src_len);
      if (r) {
        // TOD_LOGE("");
        return (size_t)-1;
      }
      goto again;
    case ERROR_INVALID_FLAGS:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      // TOD_LOGE("");
      return -1;
    case ERROR_INVALID_PARAMETER:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      // TOD_LOGE("");
      return -1;
    case ERROR_NO_UNICODE_TRANSLATION:
      break;
    default:
      break;
  }

  // NOTE: really bothering
  cd->wchar.nr = 0;
  size_t nr = 0;
  size_t i = 0;
  while (i < src_len) {
    size_t j = 0;
    while (j < cd->from.info.MaxCharSize) {
      if (i+j >= src_len) break;
      cd->from.buf[j] = src[i + j];
      WCHAR wc;
      n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags, cd->from.buf, (int)(j+1), &wc, 1);
      if (n > 0) {
        add_wcs(cd, wc);
        nr += j+1;
        i = nr;
        break;
      }
      ++j;
    }
    if (j == cd->from.info.MaxCharSize) break;
    if (i+j >= src_len) break;
  }

  mbcs->buf     += nr;
  mbcs->len     -= nr;

  // TOD_LOGE("");
  return 0;
}

static int to_mbcs(iconv_t cd, buffer_t *input, buffer_t *output)
{
  int r = 0;
  int n = 0;

  char               *dst               = output->buf;
  size_t              dst_len           = output->len;

  DWORD dwFlags = 0;
  n = CALL_WideCharToMultiByte(cd->to.cp, dwFlags, (const WCHAR*)cd->wchar.buf, (int)(cd->wchar.nr / 2), dst, (int)dst_len, NULL, NULL);
  if (n > 0) {
    output->buf         += n;
    output->len         -= n;
    errno = 0;
    return 0;
  }

  // NOTE: really bothering
  size_t wnr = 0;
  size_t i = 0;
  size_t delta = 0;
  int too_big = 0;
  int incomplete = 0;
  // TOD_LOGE("dst_len:#%zd;wchar.nr:#%zd", dst_len, cd->wchar.nr);
  for (;; i += 2) {
    // TOD_LOGE("i:%zd;nr:%zd", i, cd->wchar.nr);
    if (i + 2 > cd->wchar.nr) break;
    n = CALL_WideCharToMultiByte(cd->to.cp, dwFlags, (const WCHAR*)(cd->wchar.buf + i), 1, dst, (int)dst_len, NULL, NULL);
    if (n > 0) {
      dst          += n;
      dst_len      -= n;
      ++wnr;
      if (dst_len == 0) {
        // TOD_LOGE("dst_len:#%zd;wchar.nr:#%zd;wnr:#%zd", dst_len, cd->wchar.nr, wnr);
        break;
      }
      continue;
    }

    // TOD_LOGE("");
    goto err;
  }

  // TOD_LOGE("");
  goto adjust;

err:
  int bad = 0;
  DWORD e = GetLastError();
  char buf[4096];
  e = GetLastError();
  n = FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM|FORMAT_MESSAGE_IGNORE_INSERTS, NULL, e, MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT), buf, sizeof(buf), NULL);
  buf[n] = '\0';
  switch (e) {
    case ERROR_INSUFFICIENT_BUFFER:
      // TOD_LOGE("[0x%x]%s", e, buf);
      bad = 1;
      errno = E2BIG;
      break;
    case ERROR_INVALID_FLAGS:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return -1;
    case ERROR_INVALID_PARAMETER:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return -1;
    case ERROR_NO_UNICODE_TRANSLATION:
      // TOD_LOGE("[0x%x]%s", e, buf);
      bad = 1;
      errno = EINVAL;
      break;
    default:
      // TOD_LOGE("[0x%x]%s", e, buf);
      bad = 1;
      errno = EINVAL;
      break;
  }

adjust:
  output->buf           = dst;
  output->len           = dst_len;
  delta = cd->wchar.nr - wnr * 2;
  cd->wchar.nr          = wnr * 2;
  // TOD_LOGE("delta:#%zd;wnr:#%zd;i:#%zd", delta, wnr, i);

  n = CALL_WideCharToMultiByte(cd->from.cp, 0, (const WCHAR*)(cd->wchar.buf + cd->wchar.nr), (int)(delta / 2), NULL, 0, NULL, NULL);
  input->buf    -= n;
  input->len    += n;

  // if (bad) return (size_t)-1;
  // if (output->len) {
  //   errno = E2BIG;
  //   return (size_t)-1;
  // }

  errno = 0;
  return 0;
}

static size_t mbcs_to_mbcs(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  // TOD_LOGE("%p/%zd;%p/%zd", *inbuf, *inbytesleft, *outbuf, *outbytesleft);

  int r = 0;
  int n = 0;

  buffer_t         input = {
    .buf       = *inbuf,
    .len       = *inbytesleft,
  };
  buffer_t         output = {
    .buf       = *outbuf,
    .len       = *outbytesleft,
  };

  r = to_wchar(cd, &input);
  if (r) return (size_t)-1;
  // TOD_LOGE("input_len:#%zd;output_len:#%zd", input.len, output.len);
  r = to_mbcs(cd, &input, &output);
  if (r) return (size_t)-1;
  // TOD_LOGE("input_len:#%zd;output_len:#%zd", input.len, output.len);

  return do_finalize(cd, inbuf, inbytesleft, outbuf, outbytesleft, &input, &output);
}

static size_t mbcs_to_ucs2le(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int r = 0;
  int n = 0;
  const char            *src      = *inbuf;
  size_t                 src_len  = *inbytesleft;
  char                  *dst      = *outbuf;
  size_t                 dst_len  = *outbytesleft;

  if (dst_len < 2) {
    // TOD_LOGE("no sufficient room for outbuf");
    errno = E2BIG;
    return (size_t)-1;
  }

  DWORD dwFlags0 = MB_ERR_INVALID_CHARS;
  n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, (WCHAR*)dst, (int)(dst_len / 2));
  if (n > 0) {
    *inbuf             += src_len;
    *inbytesleft       -= src_len;
    *outbuf            += n * 2;
    *outbytesleft      -= n * 2;
    errno = 0;
    return 0;
  }

  n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, NULL, 0);
  if (n == 0) {
    // TOD_LOGE("not supported yet");
    errno = ENOTSUP;
    return (size_t)-1;
  }
  r = keep_wcs(cd, n);
  if (r) return (size_t)-1;
  CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, (WCHAR*)cd->wchar.buf, (int)(cd->wchar.sz / 2));
  n = CALL_WideCharToMultiByte(cd->from.cp, 0, (const WCHAR*)cd->wchar.buf, (int)(dst_len/2), NULL, 0, NULL, NULL);
  *inbuf              += n;
  *inbytesleft        -= n;

  memcpy(dst, cd->wchar.buf, dst_len);
  *outbuf             += dst_len / 2;
  *outbytesleft       -= dst_len / 2;

  if (*inbytesleft) {
    errno = E2BIG;
    return (size_t)-1;
  }

  errno = 0;
  return 0;
}

static void swap_endian(unsigned char *dst, size_t nr)
{
  for (size_t i=0; i<nr; i+=2) {
    unsigned char *x1 = dst + i;
    unsigned char *x2 = dst + i + 1;
    unsigned char c;
    c   = *x1;
    *x1 = *x2;
    *x2 = c;
  }
}
static size_t mbcs_to_ucs2be(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int r = 0;
  int n = 0;
  const char            *src      = *inbuf;
  size_t                 src_len  = *inbytesleft;
  char                  *dst      = *outbuf;
  size_t                 dst_len  = *outbytesleft;

  size_t nr = mbcs_to_ucs2le(cd, inbuf, inbytesleft, outbuf, outbytesleft);
  if (nr == (size_t)-1) return -1;

  nr = dst_len - *outbytesleft;
  swap_endian(dst, nr);

  errno = 0;
  return 0;
}

static size_t ucs2le_to_mbcs(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int r = 0;
  int n = 0;
  const char            *src      = *inbuf;
  size_t                 src_len  = *inbytesleft;
  char                  *dst      = *outbuf;
  size_t                 dst_len  = *outbytesleft;

  if (src_len == 1) {
    // TOD_LOGE("incomplete sequence");
    errno = EINVAL;
    return (size_t)-1;
  }

  if (dst_len < 1) {
    // TOD_LOGE("no sufficient room for outbuf");
    errno = E2BIG;
    return (size_t)-1;
  }

  DWORD dwFlags0 = 0;
  n = CALL_WideCharToMultiByte(cd->to.cp, dwFlags0, (const WCHAR*)src, (int)(src_len/2), dst, (int)dst_len, NULL, NULL);
  if (n == 0) {
    // TOD_LOGE("not supported yet:%p;%zd;%p;%zd", src, src_len, dst, dst_len);
    errno = ENOTSUP;
    return (size_t)-1;
  }
  *outbuf            += n;
  *outbytesleft      -= n;

  DWORD dwFlags1 = MB_ERR_INVALID_CHARS;
  n = CALL_MultiByteToWideChar(cd->to.cp, dwFlags1, dst, n, NULL, 0);
  *inbuf            += n * 2;
  *inbytesleft      -= n * 2;

  return 0;
}

static size_t ucs2be_to_mbcs(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int r = 0;
  int n = 0;
  const char            *src      = *inbuf;
  size_t                 src_len  = *inbytesleft;
  char                  *dst      = *outbuf;
  size_t                 dst_len  = *outbytesleft;

  if (src_len == 1) {
    // TOD_LOGE("incomplete sequence");
    errno = EINVAL;
    return (size_t)-1;
  }

  if (dst_len < 1) {
    // TOD_LOGE("no sufficient room for outbuf");
    errno = E2BIG;
    return (size_t)-1;
  }

  const char *ptr = src;
  size_t      len = src_len;
  while (len) {
    char buf[4096]; *buf = '\0';
    size_t nr = len;
    if (nr > sizeof(buf)) nr = sizeof(buf);
    memcpy(buf, ptr, nr);
    swap_endian(buf, nr);
    char       *in      = buf;
    size_t      inbytes = nr;
    size_t n = ucs2le_to_mbcs(cd, &in, &inbytes, outbuf, outbytesleft);
    if (n == (size_t)-1) return -1;

    size_t delta = nr - inbytes;
    *inbuf       += delta;
    *inbytesleft -= delta;

    if (inbytes) {
      // TOD_LOGE("no sufficient room for outbuf");
      errno = E2BIG;
      return (size_t)-1;
    }


    len -= nr;
    ptr += nr;
  }

  return 0;
}

static size_t mbcs_to_ucs4le(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int r = 0;
  int n = 0;
  const char            *src      = *inbuf;
  size_t                 src_len  = *inbytesleft;
  char                  *dst      = *outbuf;
  size_t                 dst_len  = *outbytesleft;

  DWORD dwFlags0 = MB_ERR_INVALID_CHARS;
  n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, (WCHAR*)cd->wchar.buf, (int)(cd->wchar.sz / 2));
  if (n == 0 || cd->wchar.sz == 0) {
    n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, NULL, 0);
    if (n == 0) {
      // TOD_LOGE("not supported yet");
      errno = ENOTSUP;
      return (size_t)-1;
    }

    r = keep_wcs(cd, n);
    if (r) return (size_t)-1;
    n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, (WCHAR*)cd->wchar.buf, (int)(cd->wchar.sz / 2));
  }

  cd->wchar.nr = n * 2;

  int incomplete = 0;
  int too_big = 0;

  size_t i = 0;
  while (i<cd->wchar.nr) {
    size_t n = cd->wchar.nr - i;
    int16_t v[2] = {0};
    if (n >= 4) {
      memcpy(v, cd->wchar.buf + i, 4);
    } else if (n == 2 || n == 3) {
      memcpy(v, cd->wchar.buf + i, 2);
    } else {
      incomplete = 1;
      break;
    }
    uint32_t nbuf = 0;
    uint32_t wc = utf16_to_ucs4(v, &nbuf);
    if (n<4 && nbuf == 2) {
      incomplete = 1;
      break;
    }
    if (dst_len < 4) {
      too_big = 1;
      break;
    }
    memcpy(dst, &wc, 4);
    i += nbuf * 2;
    dst += 4;
    dst_len -= 4;
  }

  DWORD dwFlags1 = 0;
  n = CALL_WideCharToMultiByte(cd->from.cp, dwFlags1, (const WCHAR*)cd->wchar.buf, (int)(i/2), NULL, 0, NULL, NULL);
  *inbuf         += n;
  *inbytesleft   -= n;
  *outbuf         = dst;
  *outbytesleft   = dst_len;

  if (*inbytesleft == 0) {
  }

  if (incomplete) {
    errno = EINVAL;
    return (size_t)-1;
  }
  if (too_big) {
    errno = E2BIG;
    return (size_t)-1;
  }

  errno = 0;
  return 0;
}

static size_t ucs4le_to_mbcs(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int r = 0;
  int n = 0;
  const char            *src      = *inbuf;
  size_t                 src_len  = *inbytesleft;
  char                  *dst      = *outbuf;
  size_t                 dst_len  = *outbytesleft;

  if (src_len < 4) {
    errno = EINVAL;
    return (size_t)-1;
  }

  r = keep_wcs(cd, src_len);

  int too_big = 0;
  int incomplete = 1;

  char    *s_cache = cd->wchar.buf;
  size_t   n_cache = cd->wchar.sz;

  while (src_len >= 4) {
    uint32_t wc;
    memcpy(&wc, src, 4);
    uint16_t vbuf[2];
    uint32_t nbuf;
    ucs4_to_utf16(wc, vbuf, &nbuf);
    if (nbuf == 1 && n_cache < 2) {
      too_big = 1;
      break;
    }
    if (nbuf == 2 && n_cache < 4) {
      too_big = 1;
      break;
    }
    if (n_cache < nbuf * 2) {
      // TODO: test case
      size_t old = cd->wchar.sz - n_cache;
      r = keep_wcs(cd, cd->wchar.sz + src_len);
      if (r) return (size_t)-1;
      s_cache  = cd->wchar.buf + old;
      n_cache  = cd->wchar.sz - old;
    }
    memcpy(s_cache, vbuf, nbuf * 2);
    src         += 4;
    src_len     -= 4;
    s_cache     += nbuf * 2;
    n_cache     -= nbuf * 2;
  }

  if (src_len >0 && src_len < 4) {
    too_big = 0;
    incomplete = 1;
  }

  DWORD dwFlags0 = 0;
  n = CALL_WideCharToMultiByte(cd->to.cp, dwFlags0, (const WCHAR*)cd->wchar.buf, (int)((s_cache - cd->wchar.buf)/2), dst, (int)dst_len, NULL, NULL);
  if (n == 0) {
    // TOD_LOGE("not supported yet");
    errno = ENOTSUP;
    return (size_t)-1;
  }

  *inbuf               = (char*)src;
  *inbytesleft         = src_len;
  *outbuf             += n;
  *outbytesleft       -= n;
  if (*inbytesleft == 0) return 0;
  if (incomplete) {
    errno = EINVAL;
    return (size_t)-1;
  }
  if (too_big) {
    errno = E2BIG;
    return (size_t)-1;
  }

  errno = 0;
  return 0;
}

static int iconv_init(iconv_t cd)
{
  if (cd->from.cp == 1200) {
    if (cd->to.cp == 12000) goto err;
    cd->conv = ucs2le_to_mbcs;
    return 0;
  } else if (cd->from.cp == 1201) {
    if (cd->to.cp == 12000) goto err;
    cd->conv = ucs2be_to_mbcs;
    return 0;
  }
  if (cd->from.cp == 12000) {
    if (cd->to.cp == 1200 || cd->to.cp == 1201) goto err;
    cd->conv = ucs4le_to_mbcs;
    return 0;
  }

  if (cd->to.cp   == 1200) {
    cd->conv = mbcs_to_ucs2le;
    return 0;
  }

  if (cd->to.cp   == 1201) {
    cd->conv = mbcs_to_ucs2be;
    return 0;
  }

  if (cd->to.cp   == 12000) {
    cd->conv = mbcs_to_ucs4le;
    return 0;
  }

  cd->conv = mbcs_to_mbcs;
  return 0;

err:
  // TOD_LOGE("not supported yet:convert from %s[%d] -> %s[%d]", cd->from.name, cd->from.cp, cd->to.name, cd->to.cp);
  errno = EINVAL;
  return -1;
}

static int UTF_16LE_to_widechar(codepage_t *codepage, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  (void)codepage;

  while (*inbytesleft >= 2 && *outbytesleft >= 2) {
    uint16_t wc = *(uint16_t*)*inbuf;
    if (codepage->be) wc = ((wc & 0xff)<<8) | (wc >> 8);
    if (codepage->ucs2 && wc >= 0xD800 && wc <= 0xDBFF) return EILSEQ;
    *(uint16_t*)*outbuf = wc;
    *inbuf           += 2;
    *inbytesleft     -= 2;
    *outbuf          += 2;
    *outbytesleft    -= 2;
  }

  if (*inbytesleft) return E2BIG;
  return 0;
}

static int widechar_to_UTF_16LE(codepage_t *codepage, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  (void)codepage;

  while (*inbytesleft >= 2 && *outbytesleft >= 2) {
    uint16_t wc = *(uint16_t*)*inbuf;
    if (codepage->ucs2 && wc >= 0xD800 && wc <= 0xDBFF) return EILSEQ;
    if (codepage->be) wc = ((wc & 0xff)<<8) | (wc >> 8);
    *(uint16_t*)*outbuf = wc;
    *inbuf           += 2;
    *inbytesleft     -= 2;
    *outbuf          += 2;
    *outbytesleft    -= 2;
  }

  if (*inbytesleft) return E2BIG;
  return 0;
}

static int UTF_32LE_to_widechar(codepage_t *codepage, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  (void)codepage;

  while (*inbytesleft >= 4) {
    uint32_t wc = *(uint32_t*)*inbuf;
    if (codepage->be) wc = ((wc & 0xff)<<24) | ((wc & 0xff00)<<8) | ((wc & 0xff0000)>>8) | (wc >> 24);
    uint16_t *dst = (uint16_t*)*outbuf;
    if (wc < 0x10000) {
        if (*outbytesleft<2) return E2BIG;
        dst[0] = (uint16_t)wc;
        *outbuf       += 2;
        *outbytesleft -= 2;
    } else {
        if (*outbytesleft<4) return E2BIG;
        wc -= 0x10000;
        dst[0] = (uint16_t)(0xD800 | ((wc >> 10) & 0x3FF));
        dst[1] = (uint16_t)(0xDC00 | (wc & 0x3FF));
        *outbuf       += 4;
        *outbytesleft -= 4;
    }
    *inbuf        += 4;
    *inbytesleft  -= 4;
  }
  if (*inbytesleft) return E2BIG;
  return 0;
}

static int widechar_to_UTF_32LE(codepage_t *codepage, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  (void)codepage;

  while (*inbytesleft >= 2) {
    if (*outbytesleft < 4) return E2BIG;
    uint16_t *wbuf = (uint16_t*)*inbuf;
    uint32_t wc = wbuf[0];
    int n = 2;
    if (0xD800 <= wc && wc <= 0xDBFF) {
      if (*inbytesleft < 4) return E2BIG;

      wc = ((wc & 0x3FF) << 10) + (wbuf[1] & 0x3FF) + 0x10000;
      n = 4;
    }

    if (codepage->be) wc = ((wc & 0xff)<<24) | ((wc & 0xff00)<<8) | ((wc & 0xff0000)>>8) | (wc >> 24);

    *inbuf        += n;
    *inbytesleft  -= n;
    *(uint32_t*)*outbuf = wc;
    *outbuf       += 4;
    *outbytesleft -= 4;
  }

  if (*inbytesleft) return E2BIG;
  return 0;
}

static int ilseq_or_inval_utf8(codepage_t *codepage, const unsigned char *remain, size_t remainlen)
{
  size_t len = 0;

  if ((remain[0] & 0xE0) == 0xC0) len = 2;
  else if ((remain[0] & 0xF0) == 0xE0) len = 3;
  else if ((remain[0] & 0xF8) == 0xF0) len = 4;
  else if ((remain[0] & 0xFC) == 0xF8) len = 5;
  else if ((remain[0] & 0xFE) == 0xFC) len = 6;
  else return EILSEQ;

  for (size_t i=1; i<len && i<codepage->info.MaxCharSize; ++i) {
    if (remainlen <= i) return EINVAL;
    if ((remain[i] & 0xC0) == 0x80) continue;
    return EILSEQ;
  }

  return EINVAL;
}

static int ilseq_or_inval_others(codepage_t *codepage, const unsigned char *remain, size_t remainlen)
{
  int len = 0;
  if (codepage->info.MaxCharSize == 1) return EINVAL;
  if (codepage->info.MaxCharSize == 2) {
    len = IsDBCSLeadByteEx(codepage->cp, remain[0]) ? 2 : 1;
    if (len == 1) return EINVAL;
    if (remainlen < 2) return EINVAL;
    return EILSEQ;
  }

  if (codepage->cp == 54936) {
    // https://github.com/win-iconv/win-iconv.git v0.0.8
    // win_iconv.c[1297]::mbcs_mblen(...)
    // 1: [0x00~0x7F]
    // 2: [0x81~0xFE]([0x40~0x7E]|[0x80~0xFE])
    // 4: [0x81~0xFE][0x30~0x39]..
    if (remainlen == 1) return EINVAL;
    if (remain[0] < 0x81 || remain[0] > 0xFE) return EILSEQ;
    if (remainlen < 2) return EINVAL;
    if (remainlen == 2) return EILSEQ;
    return EINVAL;
  }

  return EINVAL;
}

#define M2W_FLAGS            (MB_PRECOMPOSED | MB_ERR_INVALID_CHARS)
#define W2M_FLAGS            (WC_COMPOSITECHECK | WC_ERR_INVALID_CHARS | WC_NO_BEST_FIT_CHARS)

static int codepage_init(codepage_t *codepage, const char *name)
{
  int r = 0;

#define RECORD(x, y, z, a) { x, y, z, a}
  struct {
    const char          *name;
    UINT                 cp;
    uint8_t              ucs2:1;
    uint8_t              be:1;
  } names[] = {
    RECORD("GB18030",        54936         ,0,        0),
    RECORD("UTF8",           CP_UTF8       ,0,        0),
    RECORD("UTF-8",          CP_UTF8       ,0,        0),

    RECORD("UTF-16LE",       1200          ,0,        0),
    RECORD("UCS-2LE",        1200          ,1,        0),

    RECORD("UTF-32LE",       12000         ,0,        0),
    RECORD("UCS-4LE",        12000         ,0,        0),

    RECORD("UTF-16BE",       1201          ,0,        1),
    RECORD("UCS-2BE",        1201          ,1,        1),

    RECORD("UTF-32BE",       12001         ,0,        1),
    RECORD("UCS-4BE",        12001         ,0,        1),

    RECORD("CP437",          437           ,0,        0),
    RECORD("CP850",          850           ,0,        0),
    RECORD("CP858",          858           ,0,        0),
    RECORD("CP1252",         1252          ,0,        0),
  };
  const size_t nr_names = sizeof(names) / sizeof(names[0]);
#undef RECORD

  r = -1;
  for (size_t i=0; i<nr_names; ++i) {
    if (tod_strcasecmp(name, names[i].name)) continue;
    codepage->cp   = names[i].cp;
    codepage->ucs2 = names[i].ucs2;
    codepage->be   = names[i].be;
    r = 0;
  }

  if (r) {
    // TOD_LOGE("codepage [%s] not supported yet", name);
    errno = EINVAL;
    return -1;
  }

  UINT cp = codepage->cp;

  snprintf(codepage->name, sizeof(codepage->name), "%s", name);

  if (cp < 57002 || cp > 57011) {
    switch (cp) {
      case 50220:
      case 50221:
      case 50222:
      case 50225:
      case 50227:
      case 50229:
      case CP_UTF7:// 65000: // UTF-7
      case 42:
        break;
      case CP_UTF8: // 65001: // UTF-8
      case 54936: // GB18030
        codepage->flags_m2w = MB_ERR_INVALID_CHARS;
        codepage->flags_w2m = WC_ERR_INVALID_CHARS;
        break;
      default:
        codepage->flags_m2w = M2W_FLAGS;
        codepage->flags_w2m = W2M_FLAGS;
        break;
    }
  }

  switch (cp) {
    case CP_UTF7:
    case CP_UTF8:
      codepage->lpUsedDefaultChar = NULL;
      break;
    case 54936:
      // FIXME: different than https://learn.microsoft.com/en-us/windows/win32/api/stringapiset/nf-stringapiset-widechartomultibyte
      codepage->lpUsedDefaultChar = NULL;
      break;
    default:
      codepage->lpUsedDefaultChar = &codepage->usedDefaultChar;
      break;
  }

  switch (cp) {
    case 1200:  // UTF-16LE
    case 1201:  // UTF-16BE
      codepage->src_to_widechar = UTF_16LE_to_widechar;
      codepage->widechar_to_dst = widechar_to_UTF_16LE;
      break;
    case 12000: // UTF-32LE
    case 12001: // UTF-32BE
      codepage->src_to_widechar = UTF_32LE_to_widechar;
      codepage->widechar_to_dst = widechar_to_UTF_32LE;
      break;
    default:
      if (!GetCPInfoEx(cp, 0, &codepage->info)) {
        errno = EINVAL;
        return -1;
      }
      codepage->src_to_widechar = m2w;
      codepage->widechar_to_dst = w2m;
      if (cp == CP_UTF8) {
        codepage->ilseq_or_inval = ilseq_or_inval_utf8;
      } else {
        codepage->ilseq_or_inval = ilseq_or_inval_others;
      }
      break;
  }

  // char defaultChar[1024];               *defaultChar        = '\0';
  // char leadByte[1024];                  *leadByte           = '\0';
  // char unicodeDefaultChar[1024];        *unicodeDefaultChar = '\0';
  // char codePageName[1024];              *codePageName = '\0';

  // fprintf(stderr, "\n");
  // fprintf(stderr, "%s[%d]\n", name, cp);
  // fprintf(stderr, "MaxCharSize:%d\n", codepage->info.MaxCharSize);
  // fprintf(stderr, "DefaultChar:0x%s\n", tod_hexify(defaultChar, sizeof(defaultChar), codepage->info.DefaultChar, sizeof(codepage->info.DefaultChar)));
  // fprintf(stderr, "LeadByte:%s\n", tod_hexify(leadByte, sizeof(leadByte), codepage->info.LeadByte, sizeof(codepage->info.LeadByte)));
  // fprintf(stderr, "UnicodeDefaultChar:%s\n", tod_hexify(unicodeDefaultChar, sizeof(unicodeDefaultChar), (const unsigned char*)&codepage->info.UnicodeDefaultChar, sizeof(codepage->info.UnicodeDefaultChar)));
  // fprintf(stderr, "CodePageName:%s\n", tod_hexify(codePageName, sizeof(codePageName), codepage->info.CodePageName, sizeof(codepage->info.CodePageName)));

  return 0;
}

iconv_t iconv_open(const char *tocode, const char *fromcode)
{
  int r = 0;

  iconv_t cnv = (iconv_t)calloc(1, sizeof(*cnv));
  if (!cnv) {
    // TOD_LOGE("out of memory");
    errno = ENOMEM;
    return (iconv_t)-1;
  }

  BOOL ok = TRUE;

  cnv->use_codepage = 1;

  do {
    if (cnv->use_codepage) {
      r = codepage_init(&cnv->codepage_from, fromcode);
      if (r) break;
      r = codepage_init(&cnv->codepage_to, tocode);
      if (r) break;
    } else {
      r = iconv_get_acp(fromcode, &cnv->from);
      if (r) break;
      r = iconv_get_acp(tocode, &cnv->to);
      if (r) break;

      r = iconv_init(cnv);
      if (r) break;
    }

    // TOD_LOGE("from %s -> %s", fromcode, tocode);
    // TOD_LOGE("from %d -> %d", cnv->from.cp, cnv->to.cp);
    // TOD_LOGE("ACP:%d", GetACP());
    return cnv;
  } while (0);

  iconv_close(cnv);
  return (iconv_t)-1;
}

int iconv_close(iconv_t cd)
{
  if (!cd) return 0;
  cache_release(&cd->wchar);
  cache_release(&cd->mbcs);
  cpinfo_release(&cd->from);
  cpinfo_release(&cd->to);
  free(cd);
  return 0;
}

static size_t do_iconv(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  char      *src        = *inbuf;
  size_t     src_len    = *inbytesleft;
  char      *dst        = *outbuf;
  size_t     dst_len    = *outbytesleft;

  char buf[4096]; *buf = '\0';

  while (*inbytesleft) {
    if (*outbytesleft == 0) {
      errno = E2BIG;
      return (size_t)-1;
    }

    char       *p            = buf;
    size_t      n            = sizeof(buf);

    int err1 = 0;
    int err2 = 0;

    // TOD_LOGE("m2w:([%d]%s->wchar_t)[%zd]->[%zd]", cd->codepage_from.cp, cd->codepage_from.name, *inbytesleft, n);
    // dump_buf(stderr, *inbuf, *inbytesleft, "I:", "\n");
    err1 = cd->codepage_from.src_to_widechar(&cd->codepage_from, inbuf, inbytesleft, &p, &n);
    // dump_buf(stderr, *inbuf, *inbytesleft, "R:", "\n");
    // dump_buf(stderr, buf, sizeof(buf) - n, "C:", "\n");
    if (err1 && (err1 != E2BIG)) {
      errno = err1;
      if (!cd->codepage_from.ilseq_or_inval) return (size_t)-1;
      errno = cd->codepage_from.ilseq_or_inval(&cd->codepage_from, (const unsigned char*)*inbuf, *inbytesleft);
      return (size_t)-1;
    }

    char    *pp = buf;
    size_t   nn = sizeof(buf) - n;

    char    *qq = *outbuf;
    size_t   mm = *outbytesleft;

    // TOD_LOGE("w2m:(wchar_t->[%d]%s)[%zd]->[%zd]", cd->codepage_to.cp, cd->codepage_to.name, nn, *outbytesleft);
    // dump_buf(stderr, pp, nn, "I:", "\n");
    err2 = cd->codepage_to.widechar_to_dst(&cd->codepage_to, &pp, &nn, outbuf, outbytesleft);
    // dump_buf(stderr, pp, nn, "R:", "\n");
    // dump_buf(stderr, qq, mm - *outbytesleft, "C:", "\n");

    if (nn) {
      // TOD_LOGE("calc:(wchar_t->%d)[%zd]", cd->codepage_from.cp, nn);
      // dump_buf(stderr, pp, nn, "I:", "\n");
      int nnn = CALL_WideCharToMultiByte(cd->codepage_from.cp, cd->codepage_from.flags_w2m, (LPCWCH)pp, (int)(nn/2), NULL, 0, NULL, NULL);
      // TOD_LOGE("nnn:%d", nnn);
      *inbuf       -= nnn;
      *inbytesleft += nnn;
      err1          = 0;
    }

    if (err2) {
      // WLOGE(err, "convertion (wchar_t->%d)", cd->codepage_to.cp);
      errno = err2;
      return (size_t)-1;
    }
  }

  return 0;
}

size_t iconv(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  if (inbuf == NULL || *inbuf == NULL) {
    if (outbuf == NULL || *outbuf == NULL) {
      return 0;
    }
    // TOD_LOGE("not supported yet");
    errno = ENOTSUP;
    return (size_t)-1;
  }

  if (outbuf == NULL || *outbuf == NULL) {
    // TOD_LOGE("invalid argument");
    errno = EINVAL;
    return (size_t)-1;
  }

  if (inbytesleft == NULL || outbytesleft == NULL) {
    // TOD_LOGE("invalid argument");
    errno = EINVAL;
    return (size_t)-1;
  }

  char   *s_inbuf        = *inbuf;
  size_t  n_inbytes      = *inbytesleft;
  char   *s_outbuf       = *outbuf;
  size_t  n_outbytes     = *outbytesleft;
  const size_t szWchar   = sizeof(WCHAR);

  if (n_inbytes == 0) {
    errno = 0;
    return 0;
  }

  if (n_outbytes == 0) {
    // TOD_LOGE("no sufficient room for outbuf");
    errno = E2BIG;
    return (size_t)-1;
  }

  if (cd->use_codepage) {
    return do_iconv(cd, inbuf, inbytesleft, outbuf, outbytesleft);
  }

  // TOD_LOGE("inbytes:%zd;outbytes:%zd", n_inbytes, n_outbytes);
  // dump_for_debug(*inbuf, *inbytesleft);
  // TOD_LOGE("enter: from %s[%zd] -> %s[%zd]", cd->from.name, *inbytesleft, cd->to.name, *outbytesleft);
  size_t r = cd->conv(cd, inbuf, inbytesleft, outbuf, outbytesleft);
  // TOD_LOGE("leave: from %s[%zd] -> %s[%zd]", cd->from.name, *inbytesleft, cd->to.name, *outbytesleft);
  return r;
}

#endif                       /* } */
#endif                              /* } */
