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

#ifndef _todbc_conv_h_
#define _todbc_conv_h_

#include <inttypes.h>
#include <stddef.h>

typedef enum {
  TSDB_CONV_OK             = 0,
  TSDB_CONV_OOM,
  TSDB_CONV_OOR,
  TSDB_CONV_TRUNC_FRACTION,
  TSDB_CONV_TRUNC,
  TSDB_CONV_CHAR_NOT_NUM,
} TSDB_CONV_CODE;

TSDB_CONV_CODE tsdb_int64_to_bit(int todb, int64_t src, int8_t *dst);
TSDB_CONV_CODE tsdb_int64_to_tinyint(int todb, int64_t src, int8_t *dst);
TSDB_CONV_CODE tsdb_int64_to_smallint(int todb, int64_t src, int16_t *dst);
TSDB_CONV_CODE tsdb_int64_to_int(int todb, int64_t src, int32_t *dst);
TSDB_CONV_CODE tsdb_int64_to_bigint(int todb, int64_t src, int64_t *dst);
TSDB_CONV_CODE tsdb_int64_to_ts(int todb, int64_t src, int64_t *dst);
TSDB_CONV_CODE tsdb_int64_to_float(int todb, int64_t src, float *dst);
TSDB_CONV_CODE tsdb_int64_to_double(int todb, int64_t src, double *dst);
TSDB_CONV_CODE tsdb_int64_to_char(int todb, int64_t src, char *dst, size_t dlen);

TSDB_CONV_CODE tsdb_double_to_bit(int todb, double src, int8_t *dst);
TSDB_CONV_CODE tsdb_double_to_tinyint(int todb, double src, int8_t *dst);
TSDB_CONV_CODE tsdb_double_to_smallint(int todb, double src, int16_t *dst);
TSDB_CONV_CODE tsdb_double_to_int(int todb, double src, int32_t *dst);
TSDB_CONV_CODE tsdb_double_to_bigint(int todb, double src, int64_t *dst);
TSDB_CONV_CODE tsdb_double_to_ts(int todb, double src, int64_t *dst);

TSDB_CONV_CODE tsdb_chars_to_bit(int todb, const char *src, int8_t *dst);
TSDB_CONV_CODE tsdb_chars_to_tinyint(int todb, const char *src, int8_t *dst);
TSDB_CONV_CODE tsdb_chars_to_smallint(int todb, const char *src, int16_t *dst);
TSDB_CONV_CODE tsdb_chars_to_int(int todb, const char *src, int32_t *dst);
TSDB_CONV_CODE tsdb_chars_to_bigint(int todb, const char *src, int64_t *dst);
TSDB_CONV_CODE tsdb_chars_to_ts(int todb, const char *src, int64_t *dst);
TSDB_CONV_CODE tsdb_chars_to_float(int todb, const char *src, float *dst);
TSDB_CONV_CODE tsdb_chars_to_double(int todb, const char *src, double *dst);
TSDB_CONV_CODE tsdb_chars_to_char(int todb, const char *src, char *dst, size_t dlen);

TSDB_CONV_CODE tsdb_wchars_to_bit(int todb, const unsigned char *src, size_t slen, int8_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_tinyint(int todb, const unsigned char *src, size_t slen, int8_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_smallint(int todb, const unsigned char *src, size_t slen, int16_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_int(int todb, const unsigned char *src, size_t slen, int32_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_bigint(int todb, const unsigned char *src, size_t slen, int64_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_ts(int todb, const unsigned char *src, size_t slen, int64_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_float(int todb, const unsigned char *src, size_t slen, float *dst);
TSDB_CONV_CODE tsdb_wchars_to_double(int todb, const unsigned char *src, size_t slen, double *dst);
TSDB_CONV_CODE tsdb_wchars_to_char(int todb, const unsigned char *src, size_t slen, char *dst, size_t dlen);

#endif // _todbc_conv_h_

