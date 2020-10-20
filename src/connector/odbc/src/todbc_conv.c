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

#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <time.h>


// src: int
TSDB_CONV_CODE tsdb_int64_to_bit(int todb, int64_t src, int8_t *dst) {
  *dst = (int8_t)src;
  if (src==0 || src==1) return TSDB_CONV_OK;
  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_tinyint(int todb, int64_t src, int8_t *dst) {
  *dst = (int8_t)src;
  if (src == *dst) return TSDB_CONV_OK;
  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_smallint(int todb, int64_t src, int16_t *dst) {
  *dst = (int16_t)src;
  if (src == *dst) return TSDB_CONV_OK;
  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_int(int todb, int64_t src, int32_t *dst) {
  *dst = (int32_t)src;
  if (src == *dst) return TSDB_CONV_OK;
  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_bigint(int todb, int64_t src, int64_t *dst) {
  *dst = src;
  return TSDB_CONV_OK;
}

TSDB_CONV_CODE tsdb_int64_to_ts(int todb, int64_t src, int64_t *dst) {
  *dst = src;

  time_t t = (time_t)(src / 1000);
  struct tm tm = {0};
  if (localtime_r(&t, &tm)) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_float(int todb, int64_t src, float *dst) {
  *dst = (float)src;

  int64_t v = (int64_t)*dst;
  if (v==src) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_double(int todb, int64_t src, double *dst) {
  *dst = (double)src;

  int64_t v = (int64_t)*dst;
  if (v==src) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_int64_to_char(int todb, int64_t src, char *dst, size_t dlen) {
  int n = snprintf(dst, dlen, "%" PRId64 "", src);

  if (n<dlen) return TSDB_CONV_OK;

  return TSDB_CONV_TRUNC;
}

// src: double
TSDB_CONV_CODE tsdb_double_to_bit(int todb, double src, int8_t *dst) {
  *dst = (int8_t)src;

  if (src<0 || src>=2) return TSDB_CONV_OOR;
  if (src == *dst) return TSDB_CONV_OK;

  int64_t v = (int64_t)src;
  if (v == *dst) return TSDB_CONV_TRUNC_FRACTION;

  return TSDB_CONV_TRUNC;
}

TSDB_CONV_CODE tsdb_double_to_tinyint(int todb, double src, int8_t *dst) {
  *dst = (int8_t)src;

  if (src<SCHAR_MIN || src>SCHAR_MAX) return TSDB_CONV_OOR;
  if (src == *dst) return TSDB_CONV_OK;

  int64_t v = (int64_t)src;
  if (v == *dst) return TSDB_CONV_TRUNC_FRACTION;

  return TSDB_CONV_TRUNC;
}

TSDB_CONV_CODE tsdb_double_to_smallint(int todb, double src, int16_t *dst) {
  *dst = (int16_t)src;

  if (src<SHRT_MIN || src>SHRT_MAX) return TSDB_CONV_OOR;
  if (src == *dst) return TSDB_CONV_OK;

  int64_t v = (int64_t)src;
  if (v == *dst) return TSDB_CONV_TRUNC_FRACTION;

  return TSDB_CONV_TRUNC;
}

TSDB_CONV_CODE tsdb_double_to_int(int todb, double src, int32_t *dst) {
  *dst = (int32_t)src;

  if (src<LONG_MIN || src>LONG_MAX) return TSDB_CONV_OOR;
  if (src == *dst) return TSDB_CONV_OK;

  int64_t v = (int64_t)src;
  if (v == *dst) return TSDB_CONV_TRUNC_FRACTION;

  return TSDB_CONV_TRUNC;
}

TSDB_CONV_CODE tsdb_double_to_bigint(int todb, double src, int64_t *dst) {
  *dst = (int64_t)src;

  if (src<LLONG_MIN || src>LLONG_MAX) return TSDB_CONV_OOR;
  if (src == *dst) return TSDB_CONV_OK;

  int64_t v = (int64_t)src;
  if (v == *dst) return TSDB_CONV_TRUNC_FRACTION;

  return TSDB_CONV_TRUNC;
}

TSDB_CONV_CODE tsdb_double_to_ts(int todb, double src, int64_t *dst) {
  TSDB_CONV_CODE code = tsdb_double_to_bigint(todb, src, dst);

  if (code==TSDB_CONV_OK || code==TSDB_CONV_TRUNC_FRACTION) {
    int64_t v = (int64_t)src;
    time_t t = (time_t)(v / 1000);
    struct tm tm = {0};
    if (localtime_r(&t, &tm)) return TSDB_CONV_OK;

    return TSDB_CONV_OOR;
  }

  return code;
}

// src: chars
TSDB_CONV_CODE tsdb_chars_to_bit(int todb, const char *src, int8_t *dst) {
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

TSDB_CONV_CODE tsdb_chars_to_tinyint(int todb, const char *src, int8_t *dst) {
  int64_t v;
  TSDB_CONV_CODE code = tsdb_chars_to_bigint(todb, src, &v);
  if (code!=TSDB_CONV_OK) return code;
  
  *dst = (int8_t)v;

  if (v==*dst) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_chars_to_smallint(int todb, const char *src, int16_t *dst) {
  int64_t v;
  TSDB_CONV_CODE code = tsdb_chars_to_bigint(todb, src, &v);
  if (code!=TSDB_CONV_OK) return code;
  
  *dst = (int16_t)v;

  if (v==*dst) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_chars_to_int(int todb, const char *src, int32_t *dst) {
  int64_t v;
  TSDB_CONV_CODE code = tsdb_chars_to_bigint(todb, src, &v);
  if (code!=TSDB_CONV_OK) return code;
  
  *dst = (int32_t)v;

  if (v==*dst) return TSDB_CONV_OK;

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_chars_to_bigint(int todb, const char *src, int64_t *dst) {
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

TSDB_CONV_CODE tsdb_chars_to_ts(int todb, const char *src, int64_t *dst) {
  int64_t v;
  TSDB_CONV_CODE code = tsdb_chars_to_bigint(todb, src, &v);
  if (code!=TSDB_CONV_OK) return code;
  
  *dst = v;

  if (v==*dst) {
    time_t t = (time_t)(v / 1000);
    struct tm tm = {0};
    if (localtime_r(&t, &tm)) return TSDB_CONV_OK;
  }

  return TSDB_CONV_OOR;
}

TSDB_CONV_CODE tsdb_chars_to_float(int todb, const char *src, float *dst) {
  int bytes;
  int n = sscanf(src, "%g%n", dst, &bytes);

  if (n==1 && bytes==strlen(src)) {
    return TSDB_CONV_OK;
  }

  return TSDB_CONV_CHAR_NOT_NUM;
}

TSDB_CONV_CODE tsdb_chars_to_double(int todb, const char *src, double *dst) {
  int bytes;
  int n = sscanf(src, "%lg%n", dst, &bytes);

  if (n==1 && bytes==strlen(src)) {
    return TSDB_CONV_OK;
  }

  return TSDB_CONV_CHAR_NOT_NUM;
}

TSDB_CONV_CODE tsdb_chars_to_char(int todb, const char *src, char *dst, size_t dlen) {
  int n = snprintf(dst, dlen, "%s", src);
  if (n<dlen) return TSDB_CONV_OK;

  return TSDB_CONV_TRUNC;
}


// src: wchars
TSDB_CONV_CODE tsdb_wchars_to_bit(int todb, const unsigned char *src, size_t slen, int8_t *dst) {
  char buf[4096];
  char *p = buf;
  size_t plen = sizeof(buf);
  if (slen * 2 + 1 >= sizeof(buf)) {
    plen = slen * 2 + 1;
    p = (char*)malloc(plen);
    if (!p) return TSDB_CONV_OOM;
  }

  size_t n = wchars_to_chars2((const SQLWCHAR*)src, slen, (SQLCHAR*)p, plen);

  TSDB_CONV_CODE code = TSDB_CONV_OK;
  do {
    if (n<0) {
      code = TSDB_CONV_CHAR_NOT_NUM;
      break;
    }
    if (n>=plen) {
      code = TSDB_CONV_CHAR_NOT_NUM;
      break;
    }

    p[n] = '\0';
    code = tsdb_chars_to_bit(todb, p, dst);
  } while (0);

  if (p!=buf) {
    free(p);
  }

  return code;
}

TSDB_CONV_CODE tsdb_wchars_to_tinyint(int todb, const unsigned char *src, size_t slen, int8_t *dst) {
  char buf[4096];
  char *p = buf;
  size_t plen = sizeof(buf);
  if (slen * 2 + 1 >= sizeof(buf)) {
    plen = slen * 2 + 1;
    p = (char*)malloc(plen);
    if (!p) return TSDB_CONV_OOM;
  }

  size_t n = wchars_to_chars2((const SQLWCHAR*)src, slen, (SQLCHAR*)p, plen);
  TSDB_CONV_CODE code = TSDB_CONV_OK;
  do {
    if (n<0) {
      code = TSDB_CONV_CHAR_NOT_NUM;
      break;
    }
    if (n>=sizeof(buf)) {
      code = TSDB_CONV_CHAR_NOT_NUM;
      break;
    }

    buf[n] = '\0';
    code = tsdb_chars_to_tinyint(todb, buf, dst);
  } while (0);

  if (p!=buf) {
    free(p);
  }

  return code;
}




