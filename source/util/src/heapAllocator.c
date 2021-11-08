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

#include "mallocator.h"

typedef struct {
  char name[64];
} SHeapAllocator;

static SHeapAllocator *haNew();
static void            haDestroy(SHeapAllocator *pha);
static void *          haMalloc(SMemAllocator *pMemAllocator, size_t size);
void *                 haCalloc(SMemAllocator *pMemAllocator, size_t nmemb, size_t size);
static void            haFree(SMemAllocator *pMemAllocator, void *ptr);

SMemAllocator *tdCreateHeapAllocator() {
  SMemAllocator *pMemAllocator = NULL;

  pMemAllocator = (SMemAllocator *)calloc(1, sizeof(*pMemAllocator));
  if (pMemAllocator == NULL) {
    // TODO: handle error
    return NULL;
  }

  pMemAllocator->impl = haNew();
  if (pMemAllocator->impl == NULL) {
    tdDestroyHeapAllocator(pMemAllocator);
    return NULL;
  }

  pMemAllocator->usage = 0;
  pMemAllocator->malloc = haMalloc;
  pMemAllocator->calloc = haCalloc;
  pMemAllocator->realloc = NULL;
  pMemAllocator->free = haFree;
  pMemAllocator->usage = NULL;

  return pMemAllocator;
}

void tdDestroyHeapAllocator(SMemAllocator *pMemAllocator) {
  if (pMemAllocator) {
    // TODO
  }
}

/* ------------------------ STATIC METHODS ------------------------ */
static SHeapAllocator *haNew() {
  SHeapAllocator *pha = NULL;
  /* TODO */
  return pha;
}

static void haDestroy(SHeapAllocator *pha) {
  // TODO
}

static void *haMalloc(SMemAllocator *pMemAllocator, size_t size) {
  void *ptr = NULL;

  ptr = malloc(size);
  if (ptr) {
  }

  return ptr;
}

void *haCalloc(SMemAllocator *pMemAllocator, size_t nmemb, size_t size) {
  /* TODO */
  return NULL;
}

static void haFree(SMemAllocator *pMemAllocator, void *ptr) { /* TODO */
}