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

#ifndef TDENGINE_TREF_H
#define TDENGINE_TREF_H

#include "os.h"

typedef void (*_ref_fn_t)(const void* pObj);

#define T_REF_DECLARE() \
  struct {              \
    int16_t val;        \
  } _ref;

#define T_REF_REGISTER_FUNC(s, e) \
  struct {                        \
    _ref_fn_t start;              \
    _ref_fn_t end;                \
  } _ref_func = {.begin = (s), .end = (e)};

#define T_REF_INC(x) (atomic_add_fetch_16(&((x)->_ref.val), 1));

#define T_REF_INC_WITH_CB(x, p)                           \
  do {                                                    \
    int32_t v = atomic_add_fetch_32(&((x)->_ref.val), 1); \
    if (v == 1 && (p)->_ref_func.begin != NULL) {         \
      (p)->_ref_func.begin((x));                          \
    }                                                     \
  } while (0)

#define T_REF_DEC(x) (atomic_sub_fetch_16(&((x)->_ref.val), 1));

#define T_REF_DEC_WITH_CB(x, p)                           \
  do {                                                    \
    int32_t v = atomic_sub_fetch_16(&((x)->_ref.val), 1); \
    if (v == 0 && (p)->_ref_func.end != NULL) {           \
      (p)->_ref_func.end((x));                            \
    }                                                     \
  } while (0)

#define T_REF_VAL_CHECK(x) assert((x)->_ref.val >= 0);

#define T_REF_VAL_GET(x) (x)->_ref.val

#endif  // TDENGINE_TREF_H
