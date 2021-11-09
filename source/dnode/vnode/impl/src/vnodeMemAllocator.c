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

#include "vnodeDef.h"

typedef struct {
  uint64_t tsize;
  uint64_t used;
} SVHeapAllocator;

typedef struct SVArenaNode {
  struct SVArenaNode *prev;
} SVArenaNode;

typedef struct {
} SVArenaAllocator;

typedef struct {
  int8_t   type;
  uint64_t tsize;
  T_REF_DECLARE()
  union {
    SVHeapAllocator  vha;
    SVArenaAllocator vaa;
  };
} SVMemAllocator;

SMemAllocator *vnodeCreateMemAllocator(int8_t type, uint64_t size) {
  /* TODO */
  return NULL;
}

void vnodeDestroyMemAllocator(SMemAllocator *pma) {
  // TODO
}

void vnodeRefMemAllocator(SMemAllocator *pma) {
  // TODO
}

void vnodeUnrefMemAllocator(SMemAllocator *pma) {
  // TODO
}

/* ------------------------ Heap Allocator IMPL ------------------------ */

SMemAllocator *vhaCreate(uint64_t size) {
  SMemAllocator *pma;

  pma = (SMemAllocator *)calloc(1, sizeof(*pma) + sizeof(SVHeapAllocator));
  if (pma == NULL) {
    return NULL;
  }

  pma->impl = POINTER_SHIFT(pma, sizeof(*pma));

  /* TODO */
  return NULL;
}

void vhaDestroy(SMemAllocator *pma) { /* TODO */
}

static void *vhaMalloc(SMemAllocator *pma, uint64_t size) {
  SVHeapAllocator *pvha = (SVHeapAllocator *)(pma->impl);
  /* TODO */
  return NULL;
}

static void *vhaCalloc(SMemAllocator *pma, size_t nmemb, uint64_t size) {
  // todo
  return NULL;
}

static void *vhaRealloc(SMemAllocator *pma, void *ptr, uint64_t size) {
  /* TODO */
  return NULL;
}

static void vhaFree(SMemAllocator *pma, void *ptr) { /* TODO */
}

static uint64_t vhaUsage(SMemAllocator *pma) {
  /* TODO */
  return 0;
}

/* ------------------------ Arena Allocator IMPL ------------------------ */