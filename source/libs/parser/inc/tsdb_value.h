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

#ifndef _TD_TSDB_VALUE_H_
#define _TD_TSDB_VALUE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "taos.h"
#include "parser.h"

struct tsdb_value_s {
  int                         type;
  union { 
    uint8_t                   b;
    int8_t                    i8;
    int16_t                   i16;
    int32_t                   i32;
    int64_t                   i64;
    uint8_t                   u8;
    uint16_t                  u16;
    uint32_t                  u32;
    uint64_t                  u64;
    float                     flt;
    double                    dbl;
    struct {
      const char             *str;     // NOTE: no ownership
      size_t                  bytes;
    } varchar;
    struct {
      const unsigned char    *uni;     // NOTE: no ownership
      size_t                  bytes;
    } nchar;
    struct {
      const unsigned char    *bin;     // NOTE: no ownership
      size_t                  bytes;
    } varbinary;
    struct {
      union {
        int64_t               epoch_nano;
        struct {
          int                 yr;
          int                 mon;
          int                 day;
          int                 hr;
          int                 min;
          int                 sec;
          int                 nano;
        } interval;
      };
      uint8_t                 is_interval:1;
    } ts;
  };
  char                       *buf;
  size_t                      nr;
  size_t                      cap;
};

int tsdb_value_add(tsdb_value_t *v, tsdb_value_t *l, tsdb_value_t *r);
int tsdb_value_sub(tsdb_value_t *v, tsdb_value_t *l, tsdb_value_t *r);
int tsdb_value_mul(tsdb_value_t *v, tsdb_value_t *l, tsdb_value_t *r);
int tsdb_value_div(tsdb_value_t *v, tsdb_value_t *l, tsdb_value_t *r);
int tsdb_value_neg(tsdb_value_t *v, tsdb_value_t *x);
int tsdb_value_as_integer(tsdb_value_t *v, const char *s, size_t n);
int tsdb_value_as_number(tsdb_value_t *v, const char *s, size_t n);
int tsdb_value_as_str_ref(tsdb_value_t *v, const char *s, size_t n);
int tsdb_value_as_str(tsdb_value_t *v, const char *s, size_t n);
int tsdb_value_as_bool(tsdb_value_t *v, const char *s, size_t n);
int tsdb_value_as_interval(tsdb_value_t *v, const char *s, size_t n);
int tsdb_value_as_ts(tsdb_value_t *v, const char *s, size_t n);

void tsdb_value_reset(tsdb_value_t *value);
void tsdb_value_release(tsdb_value_t *value);
void tsdb_value_swap(tsdb_value_t *l, tsdb_value_t *r);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_VALUE_H_*/

