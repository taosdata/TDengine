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

#define _DEFAULT_SOURCE
#include "tmallocator.h"

/* ------------------------ HEAP ALLOCATOR ------------------------ */
#if 0
typedef struct {
  size_t tusage;
} SHeapAllocator;

static void * haMalloc(SMemAllocator *pma, size_t size);
static void * haCalloc(SMemAllocator *pma, size_t nmemb, size_t size);
static void * haRealloc(SMemAllocator *pma, void *ptr, size_t size);
static void   haFree(SMemAllocator *pma, void *ptr);
static size_t haUsage(SMemAllocator *pma);

SMemAllocator *tdCreateHeapAllocator() {
  SMemAllocator *pma = NULL;

  pma = taosMemoryCalloc(1, sizeof(SMemAllocator) + sizeof(SHeapAllocator));
  if (pma) {
    pma->impl = POINTER_SHIFT(pma, sizeof(SMemAllocator));
    pma->malloc = haMalloc;
    pma->calloc = haCalloc;
    pma->realloc = haRealloc;
    pma->free = haFree;
    pma->usage = haUsage;
  }

  return pma;
}

void tdDestroyHeapAllocator(SMemAllocator *pMemAllocator) {
  // TODO
}

static void *haMalloc(SMemAllocator *pma, size_t size) {
  void *          ptr;
  size_t          tsize = size + sizeof(size_t);
  SHeapAllocator *pha = (SHeapAllocator *)(pma->impl);

  ptr = taosMemoryMalloc(tsize);
  if (ptr) {
    *(size_t *)ptr = size;
    ptr = POINTER_SHIFT(ptr, sizeof(size_t));
    atomic_fetch_add_64(&(pha->tusage), tsize);
  }

  return ptr;
}

static void *haCalloc(SMemAllocator *pma, size_t nmemb, size_t size) {
  void * ptr;
  size_t tsize = nmemb * size;

  ptr = haMalloc(pma, tsize);
  if (ptr) {
    memset(ptr, 0, tsize);
  }

  return ptr;
}

static void *haRealloc(SMemAllocator *pma, void *ptr, size_t size) {
  size_t psize;
  size_t tsize = size + sizeof(size_t);

  if (ptr == NULL) {
    psize = 0;
  } else {
    psize = *(size_t *)POINTER_SHIFT(ptr, -sizeof(size_t));
  }

  if (psize < size) {
    // TODO
  } else {
    return ptr;
  }
}

static void haFree(SMemAllocator *pma, void *ptr) { /* TODO */
  SHeapAllocator *pha = (SHeapAllocator *)(pma->impl);
  if (ptr) {
    size_t tsize = *(size_t *)POINTER_SHIFT(ptr, -sizeof(size_t)) + sizeof(size_t);
    atomic_fetch_sub_64(&(pha->tusage), tsize);
    taosMemoryFree(POINTER_SHIFT(ptr, -sizeof(size_t)));
  }
}

static size_t haUsage(SMemAllocator *pma) { return ((SHeapAllocator *)(pma->impl))->tusage; }

/* ------------------------ ARENA ALLOCATOR ------------------------ */
typedef struct {
  size_t usage;
} SArenaAllocator;
#endif