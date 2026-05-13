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

#include "internal.h"

#include "charset.h"
#include "log.h"

#include <errno.h>

iconv_t charset_conv_get(charset_conv_t *cnv)
{
  return cnv->cnv;
}

size_t iconv_x(const char *file, int line, const char *func,
    iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  tod_logger_write(tod_get_system_logger(), LOGGER_DEBUG, tod_get_system_logger_level(),
      file, line, func, "iconv(inbuf:%p(%p);inbytesleft:%p(%zd);outbuf:%p(%p);outbytesleft:%p(%zd)) ...",
      inbuf, inbuf ? *inbuf : NULL,
      inbytesleft, inbytesleft ? *inbytesleft : 0,
      outbuf, outbuf ? *outbuf : NULL,
      outbytesleft, outbytesleft ? *outbytesleft : 0);
  size_t n = iconv(cd, inbuf, inbytesleft, outbuf, outbytesleft);
  int e = errno;
  if (n == (size_t)-1) {
    tod_logger_write(tod_get_system_logger(), LOGGER_DEBUG, tod_get_system_logger_level(),
        file, line, func, "iconv(inbuf:%p(%p);inbytesleft:%p(%zd);outbuf:%p(%p);outbytesleft:%p(%zd)) => %zd[%d]%s[%d]",
        inbuf, inbuf ? *inbuf : NULL,
        inbytesleft, inbytesleft ? *inbytesleft : 0,
        outbuf, outbuf ? *outbuf : NULL,
        outbytesleft, outbytesleft ? *outbytesleft : 0,
        n, e, strerror(e), E2BIG);
  } else {
    tod_logger_write(tod_get_system_logger(), LOGGER_DEBUG, tod_get_system_logger_level(),
        file, line, func, "iconv(inbuf:%p(%p);inbytesleft:%p(%zd);outbuf:%p(%p);outbytesleft:%p(%zd)) => %zd",
        inbuf, inbuf ? *inbuf : NULL,
        inbytesleft, inbytesleft ? *inbytesleft : 0,
        outbuf, outbuf ? *outbuf : NULL,
        outbytesleft, outbytesleft ? *outbytesleft : 0,
        n);
  }
  errno = e;
  return n;
}

void charset_conv_release(charset_conv_t *cnv)
{
  if (!cnv) return;
  if (cnv->cnv && cnv->cnv!=(iconv_t)-1) {
    iconv_close(cnv->cnv);
    cnv->cnv = (iconv_t)-1;
  }
  cnv->from[0] = '\0';
  cnv->to[0] = '\0';
}

int charset_conv_reset(charset_conv_t *cnv, const char *from, const char *to)
{
  if ((cnv->from == from || tod_strcasecmp(cnv->from, from) == 0) &&
      (cnv->to == to || tod_strcasecmp(cnv->to, to) == 0))
  {
    return 0;
  }

  charset_conv_release(cnv);

  cnv->cnv = iconv_open(to, from);
  if (cnv->cnv == (iconv_t)-1) return -1;
  if (cnv->cnv == NULL) return -1;

  snprintf(cnv->from, sizeof(cnv->from), "%s", from);
  snprintf(cnv->to, sizeof(cnv->to), "%s", to);
  return 0;
}

void charset_conv_mgr_release(charset_conv_mgr_t *mgr)
{
  if (mgr->convs) {
    hash_table_release(mgr->convs);
    free(mgr->convs);
    mgr->convs = NULL;
  }
}

charset_conv_t* charset_conv_mgr_get_charset_conv(charset_conv_mgr_t *mgr, const char *fromcode, const char *tocode)
{
  char key[256]; // NOTE: big enough?
  snprintf(key, sizeof(key), "%s,%s", fromcode, tocode);
  void *val = NULL;
  hash_table_get(mgr->convs, key, &val);
  if (val) return (charset_conv_t*)val;
  charset_conv_t *conv = (charset_conv_t*)calloc(1, sizeof(*conv));
  if (!conv) return NULL;
  int r = charset_conv_reset(conv, fromcode, tocode);
  if (r == 0) {
    r = hash_table_set(mgr->convs, key, conv);
    if (r == 0) return conv;
  }
  charset_conv_release(conv);
  free(conv);
  return NULL;
}

