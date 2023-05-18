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

#include "talgo.h"

#ifndef _TD_UTIL_TARRAY2_H_
#define _TD_UTIL_TARRAY2_H_

#ifdef __cplusplus
extern "C" {
#endif

// a: a
// e: element
// ep: element pointer
// cmp: compare function
// idx: index
// cb: callback function

#define TARRAY2(TYPE) \
  struct {            \
    int32_t size;     \
    int32_t capacity; \
    TYPE   *data;     \
  }

#define TARRAY2_MIN_SIZE 16

#define TARRAY2_INITIALIZER \
  { 0, 0, NULL }
#define TARRAY2_SIZE(a)    ((a)->size)
#define TARRAY2_GET(a, i)  ((a)->data[i])
#define TARRAY2_GETP(a, i) (&((a)->data[i]))

static FORCE_INLINE int32_t tarray2_make_room(int32_t *c, void **pp, int32_t elem_size) {
  int32_t capacity = c[0] ? (c[0] << 1) : TARRAY2_MIN_SIZE;
  void   *p = taosMemoryRealloc(pp[0], capacity * elem_size);
  if (p == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  c[0] = capacity;
  pp[0] = p;
  return 0;
}

#define TARRAY2_INIT(a) \
  do {                  \
    (a)->size = 0;      \
    (a)->capacity = 0;  \
    (a)->data = NULL;   \
  } while (0)

#define TARRAY2_FREE(a)        \
  do {                         \
    if ((a)->data) {           \
      taosMemoryFree(a->data); \
    }                          \
  } while (0)

#define TARRAY2_CLEAR(a, cb)                    \
  do {                                          \
    if (cb) {                                   \
      for (int32_t i = 0; i < (a)->size; ++i) { \
        cb((a)->data + i);                      \
      }                                         \
    }                                           \
    (a)->size = 0;                              \
  } while (0)

#define TARRAY2_SEARCH(a, ep, cmp, flag) \
  (((a)->size == 0) ? NULL : taosbsearch(ep, (a)->data, (a)->size, sizeof((a)->data[0]), cmp, flag))

#define TARRAY2_INSERT(a, idx, e)                                                                    \
  ({                                                                                                 \
    int32_t __ret = 0;                                                                               \
    if ((a)->size >= (a)->capacity) {                                                                \
      __ret = tarray2_make_room(&(a)->capacity, (void **)&(a)->data, sizeof(*(a)->data));            \
    }                                                                                                \
    if (!__ret) {                                                                                    \
      if ((a)->size > (idx)) {                                                                       \
        memmove((a)->data + (idx) + 1, (a)->data + (idx), sizeof(*(a)->data) * ((a)->size - (idx))); \
      }                                                                                              \
      (a)->data[(idx)] = e;                                                                          \
      (a)->size++;                                                                                   \
    }                                                                                                \
    __ret;                                                                                           \
  })

#define TARRAY2_INSERT_P(a, idx, ep) TARRAY2_INSERT(a, idx, *(ep))
#define TARRAY2_APPEND(a, e)         TARRAY2_INSERT(a, (a)->size, e)
#define TARRAY2_APPEND_P(a, ep)      TARRAY2_APPEND(a, *(ep))

#define TARRAY2_REMOVE(a, idx, cb)                                                                     \
  do {                                                                                                 \
    if ((idx) < (a)->size) {                                                                           \
      if (cb) cb((a)->data + (idx));                                                                   \
      if ((idx) < (a)->size - 1)                                                                       \
        memmove((a)->data + (idx), (a)->data + (idx) + 1, sizeof(*(a)->data) * ((a)->size - (idx)-1)); \
      (a)->size--;                                                                                     \
    }                                                                                                  \
  } while (0)

#define TARRAY2_FOREACH(a, e)         for (int32_t __i = 0; __i < (a)->size && ((e) = (a)->data[__i], 1); __i++)
#define TARRAY2_FOREACH_REVERSE(a, e) for (int32_t __i = (a)->size - 1; __i >= 0 && ((e) = (a)->data[__i], 1); __i--)
#define TARRAY2_FOREACH_PTR(a, ep)    for (int32_t __i = 0; __i < (a)->size && ((ep) = &(a)->data[__i], 1); __i++)
#define TARRAY2_FOREACH_PTR_REVERSE(a, ep) \
  for (int32_t __i = (a)->size - 1; __i >= 0 && ((ep) = &(a)->data[__i], 1); __i--)

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_TARRAY2_H_*/