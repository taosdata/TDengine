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

typedef void (*TArray2Cb)(void *);

#define TARRAY2_MIN_SIZE 16

#define TARRAY2_SIZE(a)       ((a)->size)
#define TARRAY2_CAPACITY(a)   ((a)->capacity)
#define TARRAY2_DATA(a)       ((a)->data)
#define TARRAY2_GET(a, i)     ((a)->data[i])
#define TARRAY2_GET_PTR(a, i) ((a)->data + i)
#define TARRAY2_FIRST(a)      ((a)->data[0])
#define TARRAY2_LAST(a)       ((a)->data[(a)->size - 1])
#define TARRAY2_DATA_LEN(a)   ((a)->size * sizeof(typeof((a)->data[0])))

static FORCE_INLINE int32_t tarray2_make_room(  //
    void   *arg,                                // array
    int32_t es,                                 // expected size
    int32_t sz                                  // size of element
) {
  TARRAY2(void) *a = arg;
  int32_t capacity = (a->capacity > 0) ? (a->capacity << 1) : TARRAY2_MIN_SIZE;
  while (capacity < es) {
    capacity <<= 1;
  }
  void *p = taosMemoryRealloc(a->data, capacity * sz);
  if (p == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  a->capacity = capacity;
  a->data = p;
  return 0;
}

#define TARRAY2_INIT_EX(a, size_, capacity_, data_) \
  do {                                              \
    (a)->size = (size_);                            \
    (a)->capacity = (capacity_);                    \
    (a)->data = (data_);                            \
  } while (0)

#define TARRAY2_INIT(a) TARRAY2_INIT_EX(a, 0, 0, NULL)

#define TARRAY2_FREE(a)          \
  do {                           \
    if ((a)->data) {             \
      taosMemoryFree((a)->data); \
    }                            \
  } while (0)

#define TARRAY2_CLEAR(a, cb)                    \
  do {                                          \
    if ((cb) && (a)->size > 0) {                \
      TArray2Cb cb_ = (TArray2Cb)(cb);          \
      for (int32_t i = 0; i < (a)->size; ++i) { \
        cb_((a)->data + i);                     \
      }                                         \
    }                                           \
    (a)->size = 0;                              \
  } while (0)

#define TARRAY2_CLEAR_FREE(a, cb) \
  do {                            \
    TARRAY2_CLEAR(a, cb);         \
    TARRAY2_FREE(a);              \
  } while (0)

#define TARRAY2_INSERT(a, idx, e)                                                                              \
  ({                                                                                                           \
    int32_t __ret = 0;                                                                                         \
    if ((a)->size >= (a)->capacity) {                                                                          \
      __ret = tarray2_make_room((a), (a)->size + 1, sizeof(typeof((a)->data[0])));                             \
    }                                                                                                          \
    if (!__ret) {                                                                                              \
      if ((a)->size > (idx)) {                                                                                 \
        memmove((a)->data + (idx) + 1, (a)->data + (idx), sizeof(typeof((a)->data[0])) * ((a)->size - (idx))); \
      }                                                                                                        \
      (a)->data[(idx)] = (e);                                                                                  \
      (a)->size++;                                                                                             \
    }                                                                                                          \
    __ret;                                                                                                     \
  })

#define TARRAY2_INSERT_PTR(a, idx, ep) TARRAY2_INSERT(a, idx, *(ep))
#define TARRAY2_APPEND(a, e)           TARRAY2_INSERT(a, (a)->size, e)
#define TARRAY2_APPEND_PTR(a, ep)      TARRAY2_APPEND(a, *(ep))

// return (TYPE *)
#define TARRAY2_SEARCH(a, ep, cmp, flag)                                                                     \
  ({                                                                                                         \
    typeof((a)->data) __ep = (ep);                                                                           \
    typeof((a)->data) __p;                                                                                   \
    if ((a)->size > 0) {                                                                                     \
      __p = taosbsearch(__ep, (a)->data, (a)->size, sizeof(typeof((a)->data[0])), (__compar_fn_t)cmp, flag); \
    } else {                                                                                                 \
      __p = NULL;                                                                                            \
    }                                                                                                        \
    __p;                                                                                                     \
  })

// return (TYPE)
#define TARRAY2_SEARCH_EX(a, ep, cmp, flag)                   \
  ({                                                          \
    typeof((a)->data) __p = TARRAY2_SEARCH(a, ep, cmp, flag); \
    __p ? __p[0] : NULL;                                      \
  })

#define TARRAY2_SEARCH_IDX(a, ep, cmp, flag)                  \
  ({                                                          \
    typeof((a)->data) __p = TARRAY2_SEARCH(a, ep, cmp, flag); \
    __p ? __p - (a)->data : -1;                               \
  })

#define TARRAY2_SORT_INSERT(a, e, cmp)                       \
  ({                                                         \
    int32_t __idx = TARRAY2_SEARCH_IDX(a, &(e), cmp, TD_GT); \
    TARRAY2_INSERT(a, __idx < 0 ? (a)->size : __idx, e);     \
  })

#define TARRAY2_SORT_INSERT_P(a, ep, cmp) TARRAY2_SORT_INSERT(a, *(ep), cmp)

#define TARRAY2_REMOVE(a, idx, cb)                                                                             \
  do {                                                                                                         \
    if ((idx) < (a)->size) {                                                                                   \
      if (cb) {                                                                                                \
        TArray2Cb cb_ = (TArray2Cb)(cb);                                                                       \
        cb_((a)->data + (idx));                                                                                \
      }                                                                                                        \
      if ((idx) < (a)->size - 1) {                                                                             \
        memmove((a)->data + (idx), (a)->data + (idx) + 1, sizeof(typeof(*(a)->data)) * ((a)->size - (idx)-1)); \
      }                                                                                                        \
      (a)->size--;                                                                                             \
    }                                                                                                          \
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