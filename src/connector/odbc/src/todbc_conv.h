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

#include "os.h"
#include <iconv.h>
#include <sql.h>


typedef enum {
  TSDB_CONV_OK             = 0,
  TSDB_CONV_NOT_AVAIL,
  TSDB_CONV_OOM,
  TSDB_CONV_OOR,
  TSDB_CONV_TRUNC_FRACTION,
  TSDB_CONV_TRUNC,
  TSDB_CONV_CHAR_NOT_NUM,
  TSDB_CONV_CHAR_NOT_TS,
  TSDB_CONV_NOT_VALID_TS,
  TSDB_CONV_GENERAL,
  TSDB_CONV_BAD_CHAR,
  TSDB_CONV_SRC_TOO_LARGE,
  TSDB_CONV_SRC_BAD_SEQ,
  TSDB_CONV_SRC_INCOMPLETE,
  TSDB_CONV_SRC_GENERAL,
} TSDB_CONV_CODE;

const char* tsdb_conv_code_str(TSDB_CONV_CODE code);

typedef struct stack_buffer_s           stack_buffer_t;
struct stack_buffer_s {
  char                  buf[1024*16];
  size_t                next;
};

char* stack_buffer_alloc(stack_buffer_t *buffer, size_t bytes);
int is_owned_by_stack_buffer(stack_buffer_t *buffer, const char *ptr);

typedef struct tsdb_conv_s             tsdb_conv_t;
tsdb_conv_t*   tsdb_conv_direct(); // get a non-conversion-converter
tsdb_conv_t*   tsdb_conv_open(const char *from_enc, const char *to_enc);
void           tsdb_conv_close(tsdb_conv_t *cnv);

TSDB_CONV_CODE tsdb_conv_write(tsdb_conv_t *cnv, const char *src, size_t *slen, char *dst, size_t *dlen);
TSDB_CONV_CODE tsdb_conv_write_int64(tsdb_conv_t *cnv, int64_t val, char *dst, size_t *dlen);
TSDB_CONV_CODE tsdb_conv_write_double(tsdb_conv_t *cnv, double val, char *dst, size_t *dlen);
TSDB_CONV_CODE tsdb_conv_write_timestamp(tsdb_conv_t *cnv, SQL_TIMESTAMP_STRUCT val, char *dst, size_t *dlen);

TSDB_CONV_CODE tsdb_conv_chars_to_bit(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int8_t *dst);
TSDB_CONV_CODE tsdb_conv_chars_to_tinyint(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int8_t *dst);
TSDB_CONV_CODE tsdb_conv_chars_to_smallint(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int16_t *dst);
TSDB_CONV_CODE tsdb_conv_chars_to_int(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int32_t *dst);
TSDB_CONV_CODE tsdb_conv_chars_to_bigint(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int64_t *dst);
TSDB_CONV_CODE tsdb_conv_chars_to_ts(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int64_t *dst);
TSDB_CONV_CODE tsdb_conv_chars_to_float(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, float *dst);
TSDB_CONV_CODE tsdb_conv_chars_to_double(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, double *dst);
TSDB_CONV_CODE tsdb_conv_chars_to_timestamp(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, SQL_TIMESTAMP_STRUCT *dst);
TSDB_CONV_CODE tsdb_conv_chars_to_timestamp_ts(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, int64_t *dst);
TSDB_CONV_CODE tsdb_conv(tsdb_conv_t *cnv, stack_buffer_t *buffer, const char *src, size_t slen, const char **dst, size_t *dlen);
void           tsdb_conv_free(tsdb_conv_t *cnv, const char *ptr, stack_buffer_t *buffer, const char *src);


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
TSDB_CONV_CODE tsdb_chars_to_timestamp(const char *src, size_t smax, SQL_TIMESTAMP_STRUCT *dst);
TSDB_CONV_CODE tsdb_chars_to_char(const char *src, size_t smax, char *dst, size_t dmax);

#endif // _todbc_conv_h_

