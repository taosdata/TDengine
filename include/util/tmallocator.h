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

#ifndef _TD_UTIL_MALLOCATOR_H_
#define _TD_UTIL_MALLOCATOR_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

// Memory allocator
#define TD_MEM_ALCT(TYPE)                           \
  struct {                                          \
    void *(*malloc_)(struct TYPE *, uint64_t size); \
    void (*free_)(struct TYPE *, void *ptr);        \
  }
#define TD_MA_MALLOC_FUNC(TMA) (TMA)->malloc_
#define TD_MA_FREE_FUNC(TMA)   (TMA)->free_

#define TD_MA_MALLOC(TMA, SIZE) (*((TMA)->malloc_))(TMA, (SIZE))
#define TD_MA_FREE(TMA, PTR)    (*((TMA)->free_))(TMA, (PTR))

typedef struct SMemAllocator {
  void *impl;
  TD_MEM_ALCT(SMemAllocator);
} SMemAllocator;

#define tMalloc(pMA, SIZE) TD_MA_MALLOC(PMA, SIZE)
#define tFree(pMA, PTR)    TD_MA_FREE(PMA, PTR)

typedef struct SMemAllocatorFactory {
  void *impl;
  SMemAllocator *(*create)(struct SMemAllocatorFactory *);
  void (*destroy)(struct SMemAllocatorFactory *, SMemAllocator *);
} SMemAllocatorFactory;

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_MALLOCATOR_H_*/