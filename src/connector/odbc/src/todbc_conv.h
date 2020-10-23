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
#include <sqltypes.h>
#include <stddef.h>

#include "iconv.h"


typedef enum {
  TSDB_CONV_OK             = 0,
  TSDB_CONV_OOM,
  TSDB_CONV_OOR,
  TSDB_CONV_TRUNC_FRACTION,
  TSDB_CONV_TRUNC,
  TSDB_CONV_CHAR_NOT_NUM,
  TSDB_CONV_GENERAL,
  TSDB_CONV_BAD_CHAR,
} TSDB_CONV_CODE;

const char* tsdb_conv_code_str(TSDB_CONV_CODE code);

TSDB_CONV_CODE tsdb_iconv_conv(iconv_t cnv, const unsigned char *src, size_t *slen, unsigned char *dst, size_t *dlen);



TSDB_CONV_CODE tsdb_int64_to_bit(int64_t src, int8_t *dst);
TSDB_CONV_CODE tsdb_int64_to_tinyint(int64_t src, int8_t *dst);
TSDB_CONV_CODE tsdb_int64_to_smallint(int64_t src, int16_t *dst);
TSDB_CONV_CODE tsdb_int64_to_int(int64_t src, int32_t *dst);
TSDB_CONV_CODE tsdb_int64_to_bigint(int64_t src, int64_t *dst);
TSDB_CONV_CODE tsdb_int64_to_ts(int64_t src, int64_t *dst);
TSDB_CONV_CODE tsdb_int64_to_float(int64_t src, float *dst);
TSDB_CONV_CODE tsdb_int64_to_double(int64_t src, double *dst);
TSDB_CONV_CODE tsdb_int64_to_char(int64_t src, char *dst, size_t dlen);

TSDB_CONV_CODE tsdb_double_to_bit(double src, int8_t *dst);
TSDB_CONV_CODE tsdb_double_to_tinyint(double src, int8_t *dst);
TSDB_CONV_CODE tsdb_double_to_smallint(double src, int16_t *dst);
TSDB_CONV_CODE tsdb_double_to_int(double src, int32_t *dst);
TSDB_CONV_CODE tsdb_double_to_bigint(double src, int64_t *dst);
TSDB_CONV_CODE tsdb_double_to_ts(double src, int64_t *dst);
TSDB_CONV_CODE tsdb_double_to_char(double src, char *dst, size_t dlen);

TSDB_CONV_CODE tsdb_timestamp_to_char(SQL_TIMESTAMP_STRUCT src, char *dst, size_t dlen);

TSDB_CONV_CODE tsdb_chars_to_bit(const char *src, size_t smax, int8_t *dst);
TSDB_CONV_CODE tsdb_chars_to_tinyint(const char *src, size_t smax, int8_t *dst);
TSDB_CONV_CODE tsdb_chars_to_smallint(const char *src, size_t smax, int16_t *dst);
TSDB_CONV_CODE tsdb_chars_to_int(const char *src, size_t smax, int32_t *dst);
TSDB_CONV_CODE tsdb_chars_to_bigint(const char *src, size_t smax, int64_t *dst);
TSDB_CONV_CODE tsdb_chars_to_ts(const char *src, size_t smax, int64_t *dst);
TSDB_CONV_CODE tsdb_chars_to_float(const char *src, size_t smax, float *dst);
TSDB_CONV_CODE tsdb_chars_to_double(const char *src, size_t smax, double *dst);
TSDB_CONV_CODE tsdb_chars_to_char(const char *src, size_t smax, char *dst, size_t dmax);

TSDB_CONV_CODE tsdb_wchars_to_bit(iconv_t cnv, const unsigned char *src, size_t smax, int8_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_tinyint(iconv_t cnv, const unsigned char *src, size_t smax, int8_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_smallint(iconv_t cnv, const unsigned char *src, size_t smax, int16_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_int(iconv_t cnv, const unsigned char *src, size_t smax, int32_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_bigint(iconv_t cnv, const unsigned char *src, size_t smax, int64_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_ts(iconv_t cnv, const unsigned char *src, size_t smax, int64_t *dst);
TSDB_CONV_CODE tsdb_wchars_to_float(iconv_t cnv, const unsigned char *src, size_t smax, float *dst);
TSDB_CONV_CODE tsdb_wchars_to_double(iconv_t cnv, const unsigned char *src, size_t smax, double *dst);
TSDB_CONV_CODE tsdb_wchars_to_char(iconv_t cnv, const unsigned char *src, size_t smax, char *dst, size_t dmax);

#endif // _todbc_conv_h_

