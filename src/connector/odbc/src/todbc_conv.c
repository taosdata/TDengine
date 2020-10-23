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

#include "todbc_conv.h"

#include "todbc_util.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

typedef struct buf_s            buf_t;

struct buf_s {
  char                  buf[1024*16+1];

  char                 *ptr;
};

static char* buf_init(buf_t *buf, size_t len);
static void buf_clean(buf_t *buf);

static char* buf_init(buf_t *buf, size_t len) {
  if (len>sizeof(buf->buf)) {
    buf->ptr = (char*)malloc(len);
  } else if (len>0) {
    buf->ptr = &buf->buf[0];
  } else {
    buf->ptr = NULL;
  }
  return buf->ptr;
}

static void buf_clean(buf_t *buf) {
  if (buf->ptr && buf->ptr != buf->buf) {
    free(buf->ptr);
    buf->ptr = NULL;
  }
}

const char* tsdb_conv_code_str(TSDB_CONV_CODE code) {
  switch (code) {
    case TSDB_CONV_OK:                 return "TSDB_CONV_OK";
    case TSDB_CONV_OOM:                return "TSDB_CONV_OOM";
    case TSDB_CONV_OOR:                return "TSDB_CONV_OOR";
    case TSDB_CONV_TRUNC_FRACTION:     return "TSDB_CONV_TRUNC_FRACTION";
    case TSDB_CONV_TRUNC:              return "TSDB_CONV_TRUNC";
    case TSDB_CONV_CHAR_NOT_NUM:       return "TSDB_CONV_CHAR_NOT_NUM";
    case TSDB_CONV_GENERAL:            return "TSDB_CONV_GENERAL";
    case TSDB_CONV_BAD_CHAR:           return "TSDB_CONV_BAD_CHAR";
    default: return "UNKNOWN";
  };
}

TSDB_CONV_CODE tsdb_iconv_conv(iconv_t cnv, const unsigned char *src, size_t *slen, unsigned char *dst, size_t *dlen) {
  if(cnv == (iconv_t)-1) return TSDB_CONV_GENERAL;

  char *s = (char*)src;
  char *d = (char*)dst;
  size_t sl = *slen;
  size_t dl = *dlen;

  int n = iconv(cnv, &s, &sl, &d, &dl);
  int e = errno;
  if (dl) *d = '\0';  // what if all consumed?

  *slen = sl;
  *dlen = dl;

  if (e==0) {
    if (n) return TSDB_CONV_BAD_CHAR;
    return TSDB_CONV_OK;
  }

  iconv(cnv, NULL, NULL, NULL, NULL);

  switch (e) {
    case E2BIG:  return TSDB_CONV_TRUNC;
    case EILSEQ: return TSDB_CONV_BAD_CHAR;
    case EINVAL: return TSDB_CONV_BAD_CHAR;
    default:     return TSDB_CONV_GENERAL;
  }
}

// src: int
TSDB_CONV_CODE tsdb_int64_to_bit(int64_t src, int8_t *dst) {
  *dst = (int8_t)src;
  if (src==0 || src==1) return TSDB_CONV_OK;
  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_tinyint(int64_t src, int8_t *dst) {
  *dst = (int8_t)src;
  if (src == *dst) return TSDB_CONV_OK;
  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_smallint(int64_t src, int16_t *dst) {
  *dst = (int16_t)src;
  if (src == *dst) return TSDB_CONV_OK;
  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_int(int64_t src, int32_t *dst) {
  *dst = (int32_t)src;
  if (src == *dst) return TSDB_CONV_OK;
  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_bigint(int64_t src, int64_t *dst) {
  *dst = src;
  return TSDB_CONV_OK;
}

TSDB_CONV_CODE tsdb_int64_to_ts(int64_t src, int64_t *dst) {
  *dst = src;

  time_t t = (time_t)(src / 1000);
  struct tm tm = {0};
  if (localtime_r(&t, &tm)) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_float(int64_t src, float *dst) {
  *dst = (float)src;

  int64_t v = (int64_t)*dst;
  if (v==src) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_double(int64_t src, double *dst) {
  *dst = (double)src;

  int64_t v = (int64_t)*dst;
  if (v==src) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_char(int64_t src, char *dst, size_t dlen) {
  int n = snprintf(dst, dlen, "%" PRId64 "", src);

  if (n<dlen) return TSDB_CONV_OK;

  return TSDB_CONV_TRUNC;
}

// src: double
TSDB_CONV_CODE tsdb_double_to_bit(double src, int8_t *dst) {
  *dst = (int8_t)src;

  if (src<0 || src>=2) return TSDB_CONV_OOR;
  if (src == *dst) return TSDB_CONV_OK;

  int64_t v = (int64_t)src;
  if (v == *dst) return TSDB_CONV_TRUNC_FRACTION;

  return TSDB_CONV_TRUNC;
}

TSDB_CONV_CODE tsdb_double_to_tinyint(double src, int8_t *dst) {
  *dst = (int8_t)src;

  if (src<SCHAR_MIN || src>SCHAR_MAX) return TSDB_CONV_OOR;
  if (src == *dst) return TSDB_CONV_OK;

  int64_t v = (int64_t)src;
  if (v == *dst) return TSDB_CONV_TRUNC_FRACTION;

  return TSDB_CONV_TRUNC;
}

TSDB_CONV_CODE tsdb_double_to_smallint(double src, int16_t *dst) {
  *dst = (int16_t)src;

  if (src<SHRT_MIN || src>SHRT_MAX) return TSDB_CONV_OOR;
  if (src == *dst) return TSDB_CONV_OK;

  int64_t v = (int64_t)src;
  if (v == *dst) return TSDB_CONV_TRUNC_FRACTION;

  return TSDB_CONV_TRUNC;
}

TSDB_CONV_CODE tsdb_double_to_int(double src, int32_t *dst) {
  *dst = (int32_t)src;

  if (src<LONG_MIN || src>LONG_MAX) return TSDB_CONV_OOR;
  if (src == *dst) return TSDB_CONV_OK;

  int64_t v = (int64_t)src;
  if (v == *dst) return TSDB_CONV_TRUNC_FRACTION;

  return TSDB_CONV_TRUNC;
}

TSDB_CONV_CODE tsdb_double_to_bigint(double src, int64_t *dst) {
  *dst = (int64_t)src;

  if (src<LLONG_MIN || src>LLONG_MAX) return TSDB_CONV_OOR;
  if (src == *dst) return TSDB_CONV_OK;

  int64_t v = (int64_t)src;
  if (v == *dst) return TSDB_CONV_TRUNC_FRACTION;

  return TSDB_CONV_TRUNC;
}

TSDB_CONV_CODE tsdb_double_to_ts(double src, int64_t *dst) {
  TSDB_CONV_CODE code = tsdb_double_to_bigint(src, dst);

  if (code==TSDB_CONV_OK || code==TSDB_CONV_TRUNC_FRACTION) {
    int64_t v = (int64_t)src;
    time_t t = (time_t)(v / 1000);
    struct tm tm = {0};
    if (localtime_r(&t, &tm)) return TSDB_CONV_OK;

    return TSDB_CONV_OOR;
  }

  return code;
}

TSDB_CONV_CODE tsdb_double_to_char(double src, char *dst, size_t dlen) {
  int n = snprintf(dst, dlen, "%lg", src);

  if (n<dlen) return TSDB_CONV_OK;

  return TSDB_CONV_TRUNC;
}

// src: SQL_TIMESTAMP_STRUCT
TSDB_CONV_CODE tsdb_timestamp_to_char(SQL_TIMESTAMP_STRUCT src, char *dst, size_t dlen) {
  int n = snprintf(dst, dlen, "%04d-%02d-%02d %02d:%02d:%02d.%03d",
                   src.year, src.month, src.day,
                   src.hour, src.minute, src.second,
                   src.fraction / 1000000);
  if (n<dlen) return TSDB_CONV_OK;

  if (strlen(dst)>=19) return TSDB_CONV_TRUNC_FRACTION;

  return TSDB_CONV_TRUNC;
}

// src: chars
TSDB_CONV_CODE tsdb_chars_to_bit(const char *src, size_t smax, int8_t *dst) {
  if (strcmp(src, "0")==0) {
    *dst = 0;
    return TSDB_CONV_OK;
  }

  if (strcmp(src, "1")==0) {
    *dst = 1;
    return TSDB_CONV_OK;
  }

  double v;
  int bytes;
  int n = sscanf(src, "%lg%n", &v, &bytes);

  if (n!=1) return TSDB_CONV_CHAR_NOT_NUM;
  if (bytes!=strlen(src)) return TSDB_CONV_CHAR_NOT_NUM;

  if (v<0 || v>=2) return TSDB_CONV_OOR;

  return TSDB_CONV_TRUNC_FRACTION;
}

TSDB_CONV_CODE tsdb_chars_to_tinyint(const char *src, size_t smax, int8_t *dst) {
  int64_t v;
  TSDB_CONV_CODE code = tsdb_chars_to_bigint(src, smax, &v);
  if (code!=TSDB_CONV_OK) return code;

  *dst = (int8_t)v;

  if (v==*dst) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_chars_to_smallint(const char *src, size_t smax, int16_t *dst) {
  int64_t v;
  TSDB_CONV_CODE code = tsdb_chars_to_bigint(src, smax, &v);
  if (code!=TSDB_CONV_OK) return code;

  *dst = (int16_t)v;

  if (v==*dst) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_chars_to_int(const char *src, size_t smax, int32_t *dst) {
  int64_t v;
  TSDB_CONV_CODE code = tsdb_chars_to_bigint(src, smax, &v);
  if (code!=TSDB_CONV_OK) return code;

  *dst = (int32_t)v;

  if (v==*dst) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_chars_to_bigint(const char *src, size_t smax, int64_t *dst) {
  int bytes;
  int n = sscanf(src, "%" PRId64 "%n", dst, &bytes);

  if (n!=1) return TSDB_CONV_CHAR_NOT_NUM;
  if (bytes==strlen(src)) {
    return TSDB_CONV_OK;
  }

  double v;
  n = sscanf(src, "%lg%n", &v, &bytes);
  if (n!=1) return TSDB_CONV_CHAR_NOT_NUM;
  if (bytes==strlen(src)) {
    return TSDB_CONV_TRUNC_FRACTION;
  }

  return TSDB_CONV_OK;
}

TSDB_CONV_CODE tsdb_chars_to_ts(const char *src, size_t smax, int64_t *dst) {
  int64_t v;
  TSDB_CONV_CODE code = tsdb_chars_to_bigint(src, smax, &v);
  if (code!=TSDB_CONV_OK) return code;

  *dst = v;

  if (v==*dst) {
    time_t t = (time_t)(v / 1000);
    struct tm tm = {0};
    if (localtime_r(&t, &tm)) return TSDB_CONV_OK;
  }

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_chars_to_float(const char *src, size_t smax, float *dst) {
  int bytes;
  int n = sscanf(src, "%g%n", dst, &bytes);

  if (n==1 && bytes==strlen(src)) {
    return TSDB_CONV_OK;
  }

  return TSDB_CONV_CHAR_NOT_NUM;
}

TSDB_CONV_CODE tsdb_chars_to_double(const char *src, size_t smax, double *dst) {
  int bytes;
  int n = sscanf(src, "%lg%n", dst, &bytes);

  if (n==1 && bytes==strlen(src)) {
    return TSDB_CONV_OK;
  }

  return TSDB_CONV_CHAR_NOT_NUM;
}

TSDB_CONV_CODE tsdb_chars_to_char(const char *src, size_t smax, char *dst, size_t dmax) {
  int n = snprintf(dst, dmax, "%s", src);
  if (n<dmax) return TSDB_CONV_OK;

  return TSDB_CONV_TRUNC;
}


// src: wchars
TSDB_CONV_CODE tsdb_wchars_to_bit(iconv_t cnv, const unsigned char *src, size_t smax, int8_t *dst) {
  if(cnv == (iconv_t)-1) return TSDB_CONV_GENERAL;

  size_t len = smax * 2;
  buf_t buf;
  buf_init(&buf, len+1);
  if (!buf.ptr) return TSDB_CONV_OOM;

  size_t dmax = len + 1;
  TSDB_CONV_CODE code = tsdb_iconv_conv(cnv, src, &smax, (unsigned char*)buf.ptr, &dmax);
  if (code==TSDB_CONV_OK) {
    code = tsdb_chars_to_bit(buf.ptr, len+1-dmax, dst);
  }

  buf_clean(&buf);

  return code;
}

TSDB_CONV_CODE tsdb_wchars_to_tinyint(iconv_t cnv, const unsigned char *src, size_t smax, int8_t *dst) {
  if(cnv == (iconv_t)-1) return TSDB_CONV_GENERAL;

  size_t len = smax * 2;
  buf_t buf;
  buf_init(&buf, len+1);
  if (!buf.ptr) return TSDB_CONV_OOM;

  size_t dmax = len + 1;
  TSDB_CONV_CODE code = tsdb_iconv_conv(cnv, src, &smax, (unsigned char*)buf.ptr, &dmax);
  if (code==TSDB_CONV_OK) {
    code = tsdb_chars_to_tinyint(buf.ptr, len+1-dmax, dst);
  }

  buf_clean(&buf);

  return code;
}

TSDB_CONV_CODE tsdb_wchars_to_smallint(iconv_t cnv, const unsigned char *src, size_t smax, int16_t *dst) {
  if(cnv == (iconv_t)-1) return TSDB_CONV_GENERAL;

  size_t len = smax * 2;
  buf_t buf;
  buf_init(&buf, len+1);
  if (!buf.ptr) return TSDB_CONV_OOM;

  size_t dmax = len + 1;
  TSDB_CONV_CODE code = tsdb_iconv_conv(cnv, src, &smax, (unsigned char*)buf.ptr, &dmax);
  if (code==TSDB_CONV_OK) {
    code = tsdb_chars_to_smallint(buf.ptr, len+1-dmax, dst);
  }

  buf_clean(&buf);

  return code;
}

TSDB_CONV_CODE tsdb_wchars_to_int(iconv_t cnv, const unsigned char *src, size_t smax, int32_t *dst) {
  if(cnv == (iconv_t)-1) return TSDB_CONV_GENERAL;

  size_t len = smax * 2;
  buf_t buf;
  buf_init(&buf, len+1);
  if (!buf.ptr) return TSDB_CONV_OOM;

  size_t dmax = len + 1;
  TSDB_CONV_CODE code = tsdb_iconv_conv(cnv, src, &smax, (unsigned char*)buf.ptr, &dmax);
  if (code==TSDB_CONV_OK) {
    code = tsdb_chars_to_int(buf.ptr, len+1-dmax, dst);
  }

  buf_clean(&buf);

  return code;
}

TSDB_CONV_CODE tsdb_wchars_to_bigint(iconv_t cnv, const unsigned char *src, size_t smax, int64_t *dst) {
  if(cnv == (iconv_t)-1) return TSDB_CONV_GENERAL;

  size_t len = smax * 2;
  buf_t buf;
  buf_init(&buf, len+1);
  if (!buf.ptr) return TSDB_CONV_OOM;

  size_t dmax = len + 1;
  TSDB_CONV_CODE code = tsdb_iconv_conv(cnv, src, &smax, (unsigned char*)buf.ptr, &dmax);
  if (code==TSDB_CONV_OK) {
    code = tsdb_chars_to_bigint(buf.ptr, len+1-dmax, dst);
  }

  buf_clean(&buf);

  return code;
}

TSDB_CONV_CODE tsdb_wchars_to_ts(iconv_t cnv, const unsigned char *src, size_t smax, int64_t *dst) {
  return tsdb_wchars_to_bigint(cnv, src, smax, dst);
}

TSDB_CONV_CODE tsdb_wchars_to_float(iconv_t cnv, const unsigned char *src, size_t smax, float *dst) {
  if(cnv == (iconv_t)-1) return TSDB_CONV_GENERAL;

  size_t len = smax * 2;
  buf_t buf;
  buf_init(&buf, len+1);
  if (!buf.ptr) return TSDB_CONV_OOM;

  size_t dmax = len + 1;
  TSDB_CONV_CODE code = tsdb_iconv_conv(cnv, src, &smax, (unsigned char*)buf.ptr, &dmax);
  if (code==TSDB_CONV_OK) {
    code = tsdb_chars_to_float(buf.ptr, len+1-dmax, dst);
  }

  buf_clean(&buf);

  return code;
}

TSDB_CONV_CODE tsdb_wchars_to_double(iconv_t cnv, const unsigned char *src, size_t smax, double *dst) {
  if(cnv == (iconv_t)-1) return TSDB_CONV_GENERAL;

  size_t len = smax * 2;
  buf_t buf;
  buf_init(&buf, len+1);
  if (!buf.ptr) return TSDB_CONV_OOM;

  size_t dmax = len + 1;
  TSDB_CONV_CODE code = tsdb_iconv_conv(cnv, src, &smax, (unsigned char*)buf.ptr, &dmax);
  if (code==TSDB_CONV_OK) {
    code = tsdb_chars_to_double(buf.ptr, len+1-dmax, dst);
  }

  buf_clean(&buf);

  return code;
}

TSDB_CONV_CODE tsdb_wchars_to_char(iconv_t cnv, const unsigned char *src, size_t smax, char *dst, size_t dmax) {
  return tsdb_iconv_conv(cnv, src, &smax, (unsigned char*)dst, &dmax);
}



