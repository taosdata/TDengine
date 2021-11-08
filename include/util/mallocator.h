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

#ifndef _TD_MALLOCATOR_H_
#define _TD_MALLOCATOR_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMemAllocator SMemAllocator;

#define MALLOCATOR_APIS                                        \
  void *(*malloc)(SMemAllocator *, size_t size);               \
  void *(*calloc)(SMemAllocator *, size_t nmemb, size_t size); \
  void *(*realloc)(SMemAllocator *, size_t size);              \
  void (*free)(SMemAllocator *, void *ptr);                    \
  size_t (*usage)(SMemAllocator *);

// Interfaces to implement
typedef struct {
  MALLOCATOR_APIS
} SMemAllocatorIf;

struct SMemAllocator {
  void * impl;
  size_t usize;
  MALLOCATOR_APIS
};

// heap allocator
SMemAllocator *tdCreateHeapAllocator();
void           tdDestroyHeapAllocator(SMemAllocator *pMemAllocator);

// arena allocator
SMemAllocator *tdCreateArenaAllocator(size_t size);
void           tdDestroyArenaAllocator(SMemAllocator *);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MALLOCATOR_H_*/