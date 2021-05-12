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

#include "todbc_string.h"

#include "todbc_log.h"
#include "todbc_tls.h"

#include <stdlib.h>

static int do_calc_bytes(todbc_string_t *str);

todbc_string_t todbc_string_init(const char *enc, const unsigned char *src, const size_t bytes) {
  DASSERT(enc);
  DASSERT(src);

  todbc_string_t s = {0};
  todbc_string_t *str = &s;

  todbc_iconv_t *cnv = todbc_tls_iconv_get(enc, UTF8_ENC);
  if (!cnv) return s;

  if (snprintf(str->enc, sizeof(str->enc), "%s", enc)>=sizeof(str->enc)) {
    return s;
  }
  str->buf          = src;
  str->total_bytes  = bytes; // need to recalc
  str->bytes        = 0;

  if (do_calc_bytes(str)) {
    str->buf         = NULL;
    str->total_bytes = 0;
    str->bytes       = 0;
  }

  return s;
}

todbc_string_t todbc_string_copy(todbc_string_t *str, const char *enc, unsigned char *dst, const size_t target_bytes) {
  todbc_string_t val = {0};
  DASSERT(str);
  DASSERT(dst);
  DASSERT(str->buf);
  DASSERT(str->bytes<=INT64_MAX);
  DASSERT(str->total_bytes<=INT64_MAX);

  if (snprintf(val.enc, sizeof(val.enc), "%s", enc)>=sizeof(val.enc)) {
    return val;
  }

  todbc_iconv_t *icnv = todbc_tls_iconv_get(enc, str->enc);
  if (!icnv) return val;
  iconv_t   cnv      = todbc_iconv_get(icnv);
  if (cnv==(iconv_t)-1) return val;

  val.buf         = dst;
  val.total_bytes = target_bytes;

  const int    null_bytes = todbc_iconv_to(icnv).null_size;

  if (target_bytes<=null_bytes) return val;
  size_t       estsize    = todbc_iconv_est_bytes(icnv, str->bytes);
  if (estsize>INT64_MAX) return val;

  // smaller is better!!!
  const size_t outblock = (estsize > target_bytes) ? estsize = target_bytes : estsize;

  char              *inbuf    = (char*)str->buf;
  size_t             inbytes  = str->bytes;       // not counting null-terminator
  char              *outbuf   = (char*)dst;
  size_t             outbytes = outblock;

  int r = todbc_iconv_raw(icnv, (const unsigned char*)inbuf, &inbytes, (unsigned char*)outbuf, &outbytes);
  if (r) {
    DASSERT(outbytes > 0);
    val.bytes        = outblock - outbytes;
    val.total_bytes  = outblock;
    return val;
  } else {
    val.bytes        = outblock - outbytes;
    val.total_bytes  = val.bytes;
    if (inbytes > 0) {
      val.total_bytes += 1;  // to indicate truncation
    }
    return val;
  }
}

todbc_string_t todbc_copy(const char *from_enc, const unsigned char *src, size_t *inbytes, const char *to_enc, unsigned char *dst, const size_t dlen) {
  DASSERT(from_enc);
  DASSERT(src);
  DASSERT(inbytes);
  DASSERT(to_enc);
  DASSERT(dst);
  DASSERT(dlen <= INT64_MAX);

  todbc_string_t s_from = todbc_string_init(from_enc, src, *inbytes);
  DASSERT(s_from.buf == src);

  return todbc_string_copy(&s_from, to_enc, dst, dlen);
}

todbc_string_t todbc_string_conv_to(todbc_string_t *str, const char *enc, todbc_buf_t *buf) {
  DASSERT(str);
  DASSERT(str->buf);
  DASSERT(str->bytes<=INT64_MAX);
  DASSERT(str->total_bytes<=INT64_MAX);

  todbc_string_t nul = {0};

  todbc_iconv_t *icnv = todbc_tls_iconv_get(enc, str->enc);
  if (!icnv) return nul;

  size_t       estsize    = todbc_iconv_est_bytes(icnv, str->bytes);
  if (estsize>INT64_MAX) return nul;
  char        *out        = NULL;
  if (!buf) out = (char*)todbc_tls_buf_alloc(estsize);
  else      out = (char*)todbc_buf_alloc(buf, estsize);
  if (!out) return nul;

  return todbc_string_copy(str, enc, (unsigned char*)out, estsize);
}

static int do_calc_bytes(todbc_string_t *str) {
  iconv_t cnv = todbc_tls_iconv(UTF8_ENC, str->enc);
  if (cnv == (iconv_t)-1) return -1;

  size_t  total_bytes = 0;

  char    buf[1024*16];

  char   *inbuf      = (char*)str->buf;

  while (1) {
    size_t          outblock   = sizeof(buf);

    size_t          inblock    = outblock;
    size_t          remain     = (size_t)-1;
    if (str->total_bytes <= INT64_MAX) {
      remain = str->total_bytes - total_bytes;
      if (remain==0) break;
      if (inblock > remain) inblock = remain;
    }

    size_t          inbytes    = inblock;
    char           *outbuf     = buf;
    size_t          outbytes   = outblock;

    size_t n = iconv(cnv, &inbuf, &inbytes, &outbuf, &outbytes);
    total_bytes += inblock - inbytes;

    int e = 0;
    if (n==(size_t)-1) {
      e = errno;
      if (str->total_bytes<=INT64_MAX) {
        D("iconv failed @[%zu], inbytes[%zd->%zd], outbytes[%zd->%zd]: [%d]%s",
          (inblock-inbytes), inblock, inbytes, outblock, outbytes, e, strerror(e));
      }
      DASSERT(e==EILSEQ || e==E2BIG || e==EINVAL);
    }
    if (n>0 && n<=INT64_MAX) {
      D("iconv found non-reversible seq");
    }

    size_t outlen  = outblock - outbytes;
    size_t utf8len = strnlen(buf, outlen);
    if (utf8len < outlen) {
      // null-terminator found
      // revert
      inbuf       -= inblock - inbytes;
      total_bytes -= inblock - inbytes;

      if (utf8len==0) break;

      inbytes    = inblock;
      outbuf     = buf;
      outbytes   = utf8len;

      n = iconv(cnv, &inbuf, &inbytes, &outbuf, &outbytes);
      total_bytes += inblock - inbytes;
      DASSERT(n==(size_t)-1);
      e = errno;
      DASSERT(e==E2BIG);

      break;
    }

    if (e==EILSEQ) break;
    if (e==EINVAL) {
      if (inbytes == remain) {
        // this is the last stuff
        break;
      }
    }
  }

  if (str->total_bytes > INT64_MAX) {
    str->total_bytes = total_bytes;
  }
  str->bytes = total_bytes;

  iconv(cnv, NULL, NULL, NULL, NULL);

  return 0;
}

