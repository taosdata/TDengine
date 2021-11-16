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

SMemAllocator *vnodeCreateMemAllocator(SVnode *pVnode) {
  SMemAllocator *pma = NULL;
  /* TODO */
  return pma;
}

void vnodeDestroyMemAllocator(SMemAllocator *pma) {
  // TODO
}

#if 0
#define VNODE_HEAP_ALLOCATOR 0
#define VNODE_ARENA_ALLOCATOR 1

typedef struct {
  uint64_t tsize;
  uint64_t used;
} SVHeapAllocator;

typedef struct SVArenaNode {
  struct SVArenaNode *prev;
  void *              nptr;
  char                data[];
} SVArenaNode;

typedef struct {
  SVArenaNode *inuse;
  SVArenaNode  node;
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

SMemAllocator *vnodeCreateMemAllocator(int8_t type, uint64_t tsize, uint64_t ssize /* step size only for arena */) {
  SMemAllocator * pma;
  uint64_t        msize;
  SVMemAllocator *pva;

  msize = sizeof(*pma) + sizeof(SVMemAllocator);
  if (type == VNODE_ARENA_ALLOCATOR) {
    msize += tsize;
  }

  pma = (SMemAllocator *)calloc(1, msize);
  if (pma == NULL) {
    return NULL;
  }

  pma->impl = POINTER_SHIFT(pma, sizeof(*pma));
  pva = (SVMemAllocator *)(pma->impl);
  pva->type = type;
  pva->tsize = tsize;

  if (type == VNODE_HEAP_ALLOCATOR) {
    pma->malloc = NULL;
    pma->calloc = NULL;
    pma->realloc = NULL;
    pma->free = NULL;
    pma->usage = NULL;
  } else if (type == VNODE_ARENA_ALLOCATOR) {
    pma->malloc = NULL;
    pma->calloc = NULL;
    pma->realloc = NULL;
    pma->free = NULL;
    pma->usage = NULL;
  } else {
    ASSERT(0);
  }

  return pma;
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

/* ------------------------ Arena Allocator IMPL ------------------------ */

#endif