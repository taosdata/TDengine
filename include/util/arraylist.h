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

#ifndef _TD_UTIL_ARRAY_LIST_H_
#define _TD_UTIL_ARRAY_LIST_H_

#include "taosdef.h"

#ifdef __cplusplus
extern "C" {
#endif

// Array-based double linked list ================
#define TD_ALIST_NODE    \
  struct {               \
    int32_t alPrevIndex; \
    int32_t alNextIndex; \
  }

#define TD_ALIST(TYPE)     \
  struct {                 \
    TYPE   *data;          \
    int32_t headIndexUsed; \
    int32_t tailIndexUsed; \
    int32_t headIndexFree; \
    int32_t tailIndexFree; \
    int32_t size;          \
    int32_t capacity;      \
  }

#define TD_ALIST_CAPACITY(al) ((al)->capacity)
#define TD_ALIST_SIZE(al)     ((al)->size)
#define TD_ALIST_IS_EMPTY(al) (TD_ALIST_SIZE(al) == 0)
#define TD_ALIST_IS_FULL(al)  (TD_ALIST_SIZE(al) >= TD_ALIST_CAPACITY(al))

#define TD_ALIST_INVALID_IDX           (-1)
#define TD_ALIST_IS_VALID_IDX(al, idx) ((idx) >= 0 && (idx) < TD_ALIST_CAPACITY(al))
#define TD_ALIST_GET_NODE(al, idx)     ((TD_ALIST_IS_VALID_IDX(al, idx)) ? &((al)->data[idx]) : NULL)
#define TD_ALIST_HEAD(al)              TD_ALIST_GET_NODE(al, (al)->headIndexUsed)
#define TD_ALIST_TAIL(al)              TD_ALIST_GET_NODE(al, (al)->tailIndexUsed)
#define TD_ALIST_NODE_PREV(al, aln)    TD_ALIST_GET_NODE(al, (aln)->alPrevIndex)
#define TD_ALIST_NODE_NEXT(al, aln)    TD_ALIST_GET_NODE(al, (aln)->alNextIndex)

static FORCE_INLINE int32_t arrayListExtend(void *alist, int32_t expSize, int32_t eleSize) {
  TD_ALIST(void) *al = alist;

  int32_t capacity = TD_ALIST_CAPACITY(al);
  if (expSize <= capacity) return 0;
  if (capacity == 0) {
    capacity = TMAX(64 / eleSize, 1);  // at least 1 element
  }
  while (capacity < expSize) {
    capacity <<= 1;
  }

  void *p = taosMemoryRealloc(al->data, capacity * eleSize);
  if (p == NULL) return terrno;
  al->data = p;
  TD_ALIST_CAPACITY(al) = capacity;
  return 0;
}

#define TD_ALIST_APPEND_TO_INTERNAL_LIST(al, aln, name)     \
  do {                                                      \
    int32_t idx = POINTER_DISTANCE((al)->data, (aln));      \
    if (TD_ALIST_IS_VALID_IDX(al, (al)->tailIndex##name)) { \
      (al)->data[(al)->tailIndex##name].alNextIndex = idx;  \
      (aln)->alPrevIndex = (al)->tailIndex##name;           \
    } else {                                                \
      (al)->headIndex##name = idx;                          \
      (aln)->alPrevIndex = TD_ALIST_INVALID_IDX;            \
    }                                                       \
    (al)->tailIndex##name = idx;                            \
    (aln)->alNextIndex = TD_ALIST_INVALID_IDX;              \
  } while (0)

#define TD_ALIST_PREPEND_TO_INTERNAL_LIST(al, aln, name)    \
  do {                                                      \
    int32_t idx = POINTER_DISTANCE((al)->data, (aln));      \
    if (TD_ALIST_IS_VALID_IDX(al, (al)->headIndex##name)) { \
      (al)->data[(al)->headIndex##name].alPrevIndex = idx;  \
      (aln)->alNextIndex = (al)->headIndex##name;           \
    } else {                                                \
      (al)->tailIndex##name = idx;                          \
      (aln)->alNextIndex = TD_ALIST_INVALID_IDX;            \
    }                                                       \
    (al)->headIndex##name = idx;                            \
    (aln)->alPrevIndex = TD_ALIST_INVALID_IDX;              \
  } while (0)

#define TD_ALIST_REMOVE_FROM_INTERNAL_LIST(al, aln, name) \
  do {                                                    \
    int32_t idx = POINTER_DISTANCE((al)->data, (aln));    \
    int32_t prevIdx = (aln)->alPrevIndex;                 \
    int32_t nextIdx = (aln)->alNextIndex;                 \
    if (TD_ALIST_IS_VALID_IDX(al, prevIdx)) {             \
      (al)->data[prevIdx].alNextIndex = nextIdx;          \
    }                                                     \
    if (TD_ALIST_IS_VALID_IDX(al, nextIdx)) {             \
      (al)->data[nextIdx].alPrevIndex = prevIdx;          \
    }                                                     \
    if ((al)->headIndex##name == idx) {                   \
      (al)->headIndex##name = nextIdx;                    \
    }                                                     \
    if ((al)->tailIndex##name == idx) {                   \
      (al)->tailIndex##name = prevIdx;                    \
    }                                                     \
    (aln)->alPrevIndex = TD_ALIST_INVALID_IDX;            \
    (aln)->alNextIndex = TD_ALIST_INVALID_IDX;            \
  } while (0)

#define TD_ALIST_INIT(al)                       \
  do {                                          \
    (al)->data = NULL;                          \
    (al)->headIndexUsed = TD_ALIST_INVALID_IDX; \
    (al)->tailIndexUsed = TD_ALIST_INVALID_IDX; \
    (al)->headIndexFree = TD_ALIST_INVALID_IDX; \
    (al)->tailIndexFree = TD_ALIST_INVALID_IDX; \
    (al)->size = 0;                             \
    (al)->capacity = 0;                         \
  } while (0)

#define TD_ALIST_RESERVE(al, nele, code)                          \
  do {                                                            \
    int32_t cap = TD_ALIST_CAPACITY(al);                          \
    if (nele <= cap) break;                                       \
    code = arrayListExtend(al, nele, sizeof((al)->data[0]));      \
    if (code != 0) break;                                         \
    for (int32_t i = cap; i < TD_ALIST_CAPACITY(al); i++) {       \
      TD_ALIST_APPEND_TO_INTERNAL_LIST(al, &(al)->data[i], Free); \
    }                                                             \
  } while (0)

#define TD_ALIST_DESTROY(al)                    \
  do {                                          \
    if ((al)->data) {                           \
      taosMemoryFreeClear((al)->data);          \
    }                                           \
    (al)->headIndexUsed = TD_ALIST_INVALID_IDX; \
    (al)->tailIndexUsed = TD_ALIST_INVALID_IDX; \
    (al)->headIndexFree = TD_ALIST_INVALID_IDX; \
    (al)->tailIndexFree = TD_ALIST_INVALID_IDX; \
    (al)->size = 0;                             \
    (al)->capacity = 0;                         \
  } while (0)

#define TD_ALIST_APPEND(al, ele, code)                              \
  do {                                                              \
    TD_ALIST_RESERVE(al, TD_ALIST_SIZE(al) + 1, code);              \
    if (code != 0) break;                                           \
    int32_t idx = (al)->headIndexFree;                              \
    TD_ALIST_REMOVE_FROM_INTERNAL_LIST(al, &(al)->data[idx], Free); \
    (al)->data[idx] = (ele);                                        \
    TD_ALIST_APPEND_TO_INTERNAL_LIST(al, &(al)->data[idx], Used);   \
    TD_ALIST_SIZE(al)++;                                            \
  } while (0)

#define TD_ALIST_PREPEND(al, ele, code)                             \
  do {                                                              \
    TD_ALIST_RESERVE(al, TD_ALIST_SIZE(al) + 1, code);              \
    if (code != 0) break;                                           \
    int32_t idx = (al)->headIndexFree;                              \
    TD_ALIST_REMOVE_FROM_INTERNAL_LIST(al, &(al)->data[idx], Free); \
    (al)->data[idx] = (ele);                                        \
    TD_ALIST_PREPEND_TO_INTERNAL_LIST(al, &(al)->data[idx], Used);  \
    TD_ALIST_SIZE(al)++;                                            \
  } while (0)

#define TD_ALIST_REMOVE(al, aln)                       \
  do {                                                 \
    TD_ALIST_REMOVE_FROM_INTERNAL_LIST(al, aln, Used); \
    TD_ALIST_APPEND_TO_INTERNAL_LIST(al, aln, Free);   \
    TD_ALIST_SIZE(al)--;                               \
  } while (0)

// Iteration macros
#define TD_ALIST_FOREACH(aln, al) \
  for (aln = TD_ALIST_HEAD(al); aln != NULL; aln = TD_ALIST_GET_NODE(al, aln->alNextIdx))

#define TD_ALIST_FOREACH_REVERSE(aln, al) \
  for (aln = TD_ALIST_TAIL(al); aln != NULL; aln = TD_ALIST_GET_NODE(al, aln->alPrevIdx))

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_ARRAY_LIST_H_*/
