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

#include "todbc_log.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

const char* tsdb_conv_code_str(TSDB_CONV_CODE code) {
  switch (code) {
    case TSDB_CONV_OK:                 return "TSDB_CONV_OK";
    case TSDB_CONV_NOT_AVAIL:          return "TSDB_CONV_NOT_AVAIL";
    case TSDB_CONV_OOM:                return "TSDB_CONV_OOM";
    case TSDB_CONV_OOR:                return "TSDB_CONV_OOR";
    case TSDB_CONV_TRUNC_FRACTION:     return "TSDB_CONV_TRUNC_FRACTION";
    case TSDB_CONV_TRUNC:              return "TSDB_CONV_TRUNC";
    case TSDB_CONV_CHAR_NOT_NUM:       return "TSDB_CONV_CHAR_NOT_NUM";
    case TSDB_CONV_CHAR_NOT_TS:        return "TSDB_CONV_CHAR_NOT_TS";
    case TSDB_CONV_NOT_VALID_TS:       return "TSDB_CONV_NOT_VALID_TS";
    case TSDB_CONV_GENERAL:            return "TSDB_CONV_GENERAL";
    case TSDB_CONV_SRC_TOO_LARGE:      return "TSDB_CONV_SRC_TOO_LARGE";
    case TSDB_CONV_SRC_BAD_SEQ:        return "TSDB_CONV_SRC_BAD_SEQ";
    case TSDB_CONV_SRC_INCOMPLETE:     return "TSDB_CONV_SRC_INCOMPLETE";
    case TSDB_CONV_SRC_GENERAL:        return "TSDB_CONV_SRC_GENERAL";
    case TSDB_CONV_BAD_CHAR:           return "TSDB_CONV_BAD_CHAR";
    default: return "UNKNOWN";
  };
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
  DASSERT(n>=0);

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
  DASSERT(n>=0);

  if (n<dlen) return TSDB_CONV_OK;

  return TSDB_CONV_TRUNC;
}

// src: SQL_TIMESTAMP_STRUCT
TSDB_CONV_CODE tsdb_timestamp_to_char(SQL_TIMESTAMP_STRUCT src, char *dst, size_t dlen) {
  int n = snprintf(dst, dlen, "%04d-%02d-%02d %02d:%02d:%02d.%03d",
                   src.year, src.month, src.day,
                   src.hour, src.minute, src.second,
                   src.fraction / 1000000);
  DASSERT(n>=0);
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

TSDB_CONV_CODE tsdb_chars_to_timestamp(const char *src, size_t smax, SQL_TIMESTAMP_STRUCT *dst) {
  int64_t v = 0;
  // why cast to 'char*' ?
  int r = taosParseTime((char*)src, &v, (int32_t)smax, TSDB_TIME_PRECISION_MILLI, 0);

  if (r) {
    return TSDB_CONV_CHAR_NOT_TS;
  }

  time_t t = v/1000;
  struct tm vtm = {0};
  localtime_r(&t, &vtm);
  dst->year     = (SQLSMALLINT)(vtm.tm_year + 1900);
  dst->month    = (SQLUSMALLINT)(vtm.tm_mon + 1);
  dst->day      = (SQLUSMALLINT)(vtm.tm_mday);
  dst->hour     = (SQLUSMALLINT)(vtm.tm_hour);
  dst->minute   = (SQLUSMALLINT)(vtm.tm_min);
  dst->second   = (SQLUSMALLINT)(vtm.tm_sec);
  dst->fraction = (SQLUINTEGER)(v%1000 * 1000000);

  return TSDB_CONV_OK;
}

TSDB_CONV_CODE tsdb_chars_to_timestamp_ts(const char *src, size_t smax, int64_t *dst) {
  // why cast to 'char*' ?
  int r = taosParseTime((char*)src, dst, (int32_t)smax, TSDB_TIME_PRECISION_MILLI, 0);

  if (r) {
    return TSDB_CONV_CHAR_NOT_TS;
  }

  return TSDB_CONV_OK;
}

TSDB_CONV_CODE tsdb_chars_to_char(const char *src, size_t smax, char *dst, size_t dmax) {
  int n = snprintf(dst, dmax, "%s", src);
  DASSERT(n>=0);
  if (n<dmax) return TSDB_CONV_OK;

  return TSDB_CONV_TRUNC;
}


char* stack_buffer_alloc(stack_buffer_t *buffer, size_t bytes) {
  if (!buffer) return NULL;
  // align-by-size_of-size_t-bytes
  if (bytes==0) bytes = sizeof(size_t);
  bytes = (bytes + sizeof(size_t) - 1) / sizeof(size_t) * sizeof(size_t);

  size_t next = buffer->next + bytes;
  if (next>sizeof(buffer->buf)) return NULL;

  char *p = buffer->buf + buffer->next;
  buffer->next = next;
  return p;
}

int is_owned_by_stack_buffer(stack_buffer_t *buffer, const char *ptr) {
  if (!buffer) return 0;
  if (ptr>=buffer->buf && ptr<buffer->buf+buffer->next) return 1;
  return 0;
}


struct tsdb_conv_s {
  iconv_t             cnv;
  unsigned int        direct:1;
};

static tsdb_conv_t      no_conversion = {0};
static pthread_once_t   once          = PTHREAD_ONCE_INIT;
static void once_init(void) {
  no_conversion.cnv    = (iconv_t)-1;
  no_conversion.direct = 1;
}

tsdb_conv_t* tsdb_conv_direct() { // get a non-conversion-converter
  pthread_once(&once, once_init);
  return &no_conversion;
}

tsdb_conv_t* tsdb_conv_open(const char *from_enc, const char *to_enc) {
  pthread_once(&once, once_init);
  tsdb_conv_t *cnv = (tsdb_conv_t*)calloc(1, sizeof(*cnv));
  if (!cnv) return NULL;
  if (strcmp(from_enc, to_enc)==0 && 0) {
    cnv->cnv = (iconv_t)-1;
    cnv->direct = 1;
    return cnv;
  }
  cnv->cnv = iconv_open(to_enc, from_enc);
  if (cnv->cnv == (iconv_t)-1) {
    free(cnv);
    return NULL;
  }
  cnv->direct = 0;
  return cnv;
}

void tsdb_conv_close(tsdb_conv_t *cnv) {
  if (!cnv) return;
  if (cnv == &no_conversion) return;
  if (!cnv->direct) {
    if (cnv->cnv != (iconv_t)-1) {
      iconv_close(cnv->cnv);
    }
  }
  cnv->cnv = (iconv_t)-1;
  cnv->direct = 0;
  free(cnv);
}

TSDB_CONV_CODE tsdb_conv_write(tsdb_conv_t *cnv, const char *src, size_t *slen, char *dst, size_t *dlen) {
  if (!cnv) return TSDB_CONV_NOT_AVAIL;
  if (cnv->direct) {
    size_t n = (*slen > *dlen) ? *dlen : *slen;
    memcpy(dst, src, n);
    *slen -= n;
    *dlen -= n;
    if (*dlen) dst[n] = '\0';
    return TSDB_CONV_OK;
  }
  if (!cnv->cnv) return TSDB_CONV_NOT_AVAIL;
  size_t r = iconv(cnv->cnv, (char**)&src, slen, &dst, dlen);
  if (r==(size_t)-1) return TSDB_CONV_BAD_CHAR;
  if (*slen)         return TSDB_CONV_TRUNC;
  if (*dlen) *dst = '\0';
  return TSDB_CONV_OK;
}

TSDB_CONV_CODE tsdb_conv_write_int64(tsdb_conv_t *cnv, int64_t val, char *dst, size_t *dlen) {
  char utf8[64];
  int n = snprintf(utf8, sizeof(utf8), "%" PRId64 "", val);
  DASSERT(n>=0);
  DASSERT(n<sizeof(utf8));
  size_t len = (size_t)n;
  TSDB_CONV_CODE code = tsdb_conv_write(cnv, utf8, &len, dst, dlen);
  *dlen = (size_t)n+1;
  return code;
}

TSDB_CONV_CODE tsdb_conv_write_double(tsdb_conv_t *cnv, double val, char *dst, size_t *dlen) {
  char utf8[256];
  int n = snprintf(utf8, sizeof(utf8), "%g", val);
  DASSERT(n>=0);
  DASSERT(n<sizeof(utf8));
  size_t len = (size_t)n;
  TSDB_CONV_CODE code = tsdb_conv_write(cnv, utf8, &len, dst, dlen);
  *dlen = (size_t)n+1;
  return code;
}

TSDB_CONV_CODE tsdb_conv_write_timestamp(tsdb_conv_t *cnv, SQL_TIMESTAMP_STRUCT val, char *dst, size_t *dlen) {
  char utf8[256];
  int n = snprintf(utf8, sizeof(utf8), "%04d-%02d-%02d %02d:%02d:%02d.%03d",
                   val.year, val.month, val.day,
                   val.hour, val.minute, val.second,
                   val.fraction / 1000000);
  DASSERT(n>=0);
  DASSERT(n<sizeof(utf8));
  size_t len = (size_t)n;
  TSDB_CONV_CODE code = tsdb_conv_write(cnv, utf8, &len, dst, dlen);
  *dlen = (size_t)n+1;
  return code;
}

TSDB_CONV_CODE tsdb_conv_chars_to_bit(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int8_t *dst) {
  const char *utf8 = NULL;
  TSDB_CONV_CODE code = tsdb_conv(cnv, buffer, src, slen, &utf8, NULL);
  if (code) return code;
  code = tsdb_chars_to_bit(utf8, sizeof(utf8), dst);
  tsdb_conv_free(cnv, utf8, buffer, src);
  return code;
}

TSDB_CONV_CODE tsdb_conv_chars_to_tinyint(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int8_t *dst) {
  const char *utf8 = NULL;
  TSDB_CONV_CODE code = tsdb_conv(cnv, buffer, src, slen, &utf8, NULL);
  if (code) return code;
  code = tsdb_chars_to_tinyint(utf8, sizeof(utf8), dst);
  tsdb_conv_free(cnv, utf8, buffer, src);
  return code;
}

TSDB_CONV_CODE tsdb_conv_chars_to_smallint(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int16_t *dst) {
  const char *utf8 = NULL;
  TSDB_CONV_CODE code = tsdb_conv(cnv, buffer, src, slen, &utf8, NULL);
  if (code) return code;
  code = tsdb_chars_to_smallint(utf8, sizeof(utf8), dst);
  tsdb_conv_free(cnv, utf8, buffer, src);
  return code;
}

TSDB_CONV_CODE tsdb_conv_chars_to_int(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int32_t *dst) {
  const char *utf8 = NULL;
  TSDB_CONV_CODE code = tsdb_conv(cnv, buffer, src, slen, &utf8, NULL);
  if (code) return code;
  code = tsdb_chars_to_int(utf8, sizeof(utf8), dst);
  tsdb_conv_free(cnv, utf8, buffer, src);
  return code;
}

TSDB_CONV_CODE tsdb_conv_chars_to_bigint(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int64_t *dst) {
  const char *utf8 = NULL;
  TSDB_CONV_CODE code = tsdb_conv(cnv, buffer, src, slen, &utf8, NULL);
  if (code) return code;
  code = tsdb_chars_to_bigint(utf8, sizeof(utf8), dst);
  tsdb_conv_free(cnv, utf8, buffer, src);
  return code;
}

TSDB_CONV_CODE tsdb_conv_chars_to_ts(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int64_t *dst) {
  const char *utf8 = NULL;
  TSDB_CONV_CODE code = tsdb_conv(cnv, buffer, src, slen, &utf8, NULL);
  if (code) return code;
  code = tsdb_chars_to_ts(utf8, sizeof(utf8), dst);
  tsdb_conv_free(cnv, utf8, buffer, src);
  return code;
}

TSDB_CONV_CODE tsdb_conv_chars_to_float(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, float *dst) {
  const char *utf8 = NULL;
  TSDB_CONV_CODE code = tsdb_conv(cnv, buffer, src, slen, &utf8, NULL);
  if (code) return code;
  code = tsdb_chars_to_float(utf8, sizeof(utf8), dst);
  tsdb_conv_free(cnv, utf8, buffer, src);
  return code;
}

TSDB_CONV_CODE tsdb_conv_chars_to_double(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, double *dst) {
  const char *utf8 = NULL;
  TSDB_CONV_CODE code = tsdb_conv(cnv, buffer, src, slen, &utf8, NULL);
  if (code) return code;
  code = tsdb_chars_to_double(utf8, sizeof(utf8), dst);
  tsdb_conv_free(cnv, utf8, buffer, src);
  return code;
}

TSDB_CONV_CODE tsdb_conv_chars_to_timestamp(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, SQL_TIMESTAMP_STRUCT *dst) {
  const char *utf8 = NULL;
  TSDB_CONV_CODE code = tsdb_conv(cnv, buffer, src, slen, &utf8, NULL);
  if (code) return code;
  code = tsdb_chars_to_timestamp(utf8, sizeof(utf8), dst);
  tsdb_conv_free(cnv, utf8, buffer, src);
  return code;
}

TSDB_CONV_CODE tsdb_conv_chars_to_timestamp_ts(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int64_t *dst) {
  const char *utf8 = NULL;
  TSDB_CONV_CODE code = tsdb_conv(cnv, buffer, src, slen, &utf8, NULL);
  if (code) return code;
  code = tsdb_chars_to_timestamp_ts(utf8, sizeof(utf8), dst);
  tsdb_conv_free(cnv, utf8, buffer, src);
  return code;
}

TSDB_CONV_CODE tsdb_conv(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, const char **dst, size_t *dlen) {
  if (!cnv) return TSDB_CONV_NOT_AVAIL;

  char *buf;
  size_t blen;
  if (cnv->direct) {
    if (src[slen]=='\0') { // access violation?
      *dst = src;
      if (dlen) *dlen = slen;
      return TSDB_CONV_OK;
    }
    blen = slen + 1;
  } else {
    blen = (slen + 1) * 4;
  }

  buf = stack_buffer_alloc(buffer, blen);
  if (!buf) {
    buf = (char*)malloc(blen);
    if (!buf) return TSDB_CONV_OOM;
  }

  if (cnv->direct) {
    size_t n = slen;
    DASSERT(blen > n);
    memcpy(buf, src, n);
    buf[n] = '\0';
    *dst = buf;
    if (dlen) *dlen = n;
    return TSDB_CONV_OK;
  }

  const char *orig_s = src;
  char       *orig_d = buf;
  size_t      orig_blen = blen;

  TSDB_CONV_CODE code;
  size_t r = iconv(cnv->cnv, (char**)&src, &slen, &buf, &blen);
  do {
    if (r==(size_t)-1) {
      switch(errno) {
        case E2BIG: {
          code = TSDB_CONV_SRC_TOO_LARGE;
        } break;
        case EILSEQ: {
          code = TSDB_CONV_SRC_BAD_SEQ;
        } break;
        case EINVAL: {
          code = TSDB_CONV_SRC_INCOMPLETE;
        } break;
        default: {
          code = TSDB_CONV_SRC_GENERAL;
        } break;
      }
      break;
    }
    if (slen) {
      code = TSDB_CONV_TRUNC;
      break;
    }
    DASSERT(blen);
    *buf = '\0';
    *dst = orig_d;
    if (dlen) *dlen = orig_blen - blen;
    return TSDB_CONV_OK;
  } while (0);

  if (orig_d!=(char*)orig_s && !is_owned_by_stack_buffer(buffer, orig_d)) free(orig_d);
  return code;
}

void tsdb_conv_free(tsdb_conv_t *cnv, const char *ptr, stack_buffer_t *buffer, const char *src) {
  if (ptr!=src && !is_owned_by_stack_buffer(buffer, ptr)) free((char*)ptr);
}

