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

#define TARRAY2_SIZE(a)       ((a)->size)
#define TARRAY2_CAPACITY(a)   ((a)->capacity)
#define TARRAY2_DATA(a)       ((a)->data)
#define TARRAY2_GET(a, i)     ((a)->data[i])
#define TARRAY2_GET_PTR(a, i) ((a)->data + i)
#define TARRAY2_FIRST(a)      ((a)->data[0])
#define TARRAY2_LAST(a)       ((a)->data[(a)->size - 1])
#define TARRAY2_DATA_LEN(a)   ((a)->size * sizeof(((a)->data[0])))

static FORCE_INLINE int32_t tarray2_make_room(void *arr, int32_t expSize, int32_t eleSize) {
  TARRAY2(void) *a = arr;

  int32_t capacity = (a->capacity > 0) ? (a->capacity << 1) : 32;
  while (capacity < expSize) {
    capacity <<= 1;
  }
  void *p = taosMemoryRealloc(a->data, capacity * eleSize);
  if (p == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  a->capacity = capacity;
  a->data = p;
  return 0;
}

static FORCE_INLINE int32_t tarray2InsertBatch(void *arr, int32_t idx, const void *elePtr, int32_t numEle,
                                               int32_t eleSize) {
  TARRAY2(uint8_t) *a = arr;

  int32_t ret = 0;
  if (a->size + numEle > a->capacity) {
    ret = tarray2_make_room(a, a->size + numEle, eleSize);
  }
  if (ret == 0) {
    if (idx < a->size) {
      memmove(a->data + (idx + numEle) * eleSize, a->data + idx * eleSize, (a->size - idx) * eleSize);
    }
    memcpy(a->data + idx * eleSize, elePtr, numEle * eleSize);
    a->size += numEle;
  }
  return ret;
}

static FORCE_INLINE void *tarray2Search(void *arr, const void *elePtr, int32_t eleSize, __compar_fn_t compar,
                                        int32_t flag) {
  TARRAY2(void) *a = arr;
  return taosbsearch(elePtr, a->data, a->size, eleSize, compar, flag);
}

static FORCE_INLINE int32_t tarray2SearchIdx(void *arr, const void *elePtr, int32_t eleSize, __compar_fn_t compar,
                                             int32_t flag) {
  TARRAY2(void) *a = arr;
  void *p = taosbsearch(elePtr, a->data, a->size, eleSize, compar, flag);
  if (p == NULL) {
    return -1;
  } else {
    return (int32_t)(((uint8_t *)p - (uint8_t *)a->data) / eleSize);
  }
}

static FORCE_INLINE int32_t tarray2SortInsert(void *arr, const void *elePtr, int32_t eleSize, __compar_fn_t compar) {
  TARRAY2(void) *a = arr;
  int32_t idx = tarray2SearchIdx(arr, elePtr, eleSize, compar, TD_GT);
  return tarray2InsertBatch(arr, idx < 0 ? a->size : idx, elePtr, 1, eleSize);
}

#define TARRAY2_INIT_EX(a, size_, capacity_, data_) \
  do {                                              \
    (a)->size = (size_);                            \
    (a)->capacity = (capacity_);                    \
    (a)->data = (data_);                            \
  } while (0)

#define TARRAY2_INIT(a) TARRAY2_INIT_EX(a, 0, 0, NULL)

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

#define TARRAY2_DESTROY(a, cb)   \
  do {                           \
    TARRAY2_CLEAR(a, cb);        \
    if ((a)->data) {             \
      taosMemoryFree((a)->data); \
      (a)->data = NULL;          \
    }                            \
    (a)->capacity = 0;           \
  } while (0)

#define TARRAY2_INSERT_PTR(a, idx, ep) tarray2InsertBatch(a, idx, ep, 1, sizeof((a)->data[0]))
#define TARRAY2_APPEND_PTR(a, ep)      tarray2InsertBatch(a, (a)->size, ep, 1, sizeof((a)->data[0]))
#define TARRAY2_APPEND_BATCH(a, ep, n) tarray2InsertBatch(a, (a)->size, ep, n, sizeof((a)->data[0]))
#define TARRAY2_APPEND(a, e)           TARRAY2_APPEND_PTR(a, &(e))

// return (TYPE *)
#define TARRAY2_SEARCH(a, ep, cmp, flag) tarray2Search(a, ep, sizeof(((a)->data[0])), (__compar_fn_t)cmp, flag)

#define TARRAY2_SEARCH_IDX(a, ep, cmp, flag) tarray2SearchIdx(a, ep, sizeof(((a)->data[0])), (__compar_fn_t)cmp, flag)

#define TARRAY2_SORT_INSERT(a, e, cmp)    tarray2SortInsert(a, &(e), sizeof(((a)->data[0])), (__compar_fn_t)cmp)
#define TARRAY2_SORT_INSERT_P(a, ep, cmp) tarray2SortInsert(a, ep, sizeof(((a)->data[0])), (__compar_fn_t)cmp)

#define TARRAY2_REMOVE(a, idx, cb)                                                                       \
  do {                                                                                                   \
    if ((idx) < (a)->size) {                                                                             \
      if (cb) {                                                                                          \
        TArray2Cb cb_ = (TArray2Cb)(cb);                                                                 \
        cb_((a)->data + (idx));                                                                          \
      }                                                                                                  \
      if ((idx) < (a)->size - 1) {                                                                       \
        memmove((a)->data + (idx), (a)->data + (idx) + 1, sizeof((*(a)->data)) * ((a)->size - (idx)-1)); \
      }                                                                                                  \
      (a)->size--;                                                                                       \
    }                                                                                                    \
  } while (0)

#define TARRAY2_FOREACH(a, e)         for (int32_t __i = 0; __i < (a)->size && ((e) = (a)->data[__i], 1); __i++)
#define TARRAY2_FOREACH_REVERSE(a, e) for (int32_t __i = (a)->size - 1; __i >= 0 && ((e) = (a)->data[__i], 1); __i--)
#define TARRAY2_FOREACH_PTR(a, ep)    for (int32_t __i = 0; __i < (a)->size && ((ep) = &(a)->data[__i], 1); __i++)
#define TARRAY2_FOREACH_PTR_REVERSE(a, ep) \
  for (int32_t __i = (a)->size - 1; __i >= 0 && ((ep) = &(a)->data[__i], 1); __i--)

#define TARRAY2_SORT(a, cmp)                                                    \
  do {                                                                          \
    if ((a)->size > 1) {                                                        \
      taosSort((a)->data, (a)->size, sizeof((a)->data[0]), (__compar_fn_t)cmp); \
    }                                                                           \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_TARRAY2_H_*/
