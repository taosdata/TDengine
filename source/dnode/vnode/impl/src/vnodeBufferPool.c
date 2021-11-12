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

/* ------------------------ STRUCTURES ------------------------ */
struct SVBufPool {
  SList      free;
  SList      incycle;
  SListNode *inuse;
};

typedef enum { E_V_HEAP_ALLOCATOR = 0, E_V_ARENA_ALLOCATOR } EVMemAllocatorT;

typedef struct {
} SVHeapAllocator;

typedef struct SVArenaNode {
  struct SVArenaNode *prev;
  uint64_t            size;
  void *              ptr;
  char                data[];
} SVArenaNode;

typedef struct {
  SVArenaNode *inuse;
  SVArenaNode  node;
} SVArenaAllocator;

typedef struct {
  uint64_t        capacity;
  EVMemAllocatorT type;
  T_REF_DECLARE()
  union {
    SVHeapAllocator  vha;
    SVArenaAllocator vaa;
  };
} SVMemAllocator;

static SListNode *vBufPoolNewNode(uint64_t capacity, EVMemAllocatorT type);
static void       vBufPoolFreeNode(SListNode *pNode);
static int        vArenaAllocatorInit(SVArenaAllocator *pvaa);
static void       vArenaAllocatorClear(SVArenaAllocator *pvaa);
static int        vHeapAllocatorInit(SVHeapAllocator *pvha);
static void       vHeapAllocatorClear(SVHeapAllocator *pvha);

int vnodeOpenBufPool(SVnode *pVnode) {
  uint64_t        capacity;
  EVMemAllocatorT type = E_V_ARENA_ALLOCATOR;

  if ((pVnode->pBufPool = (SVBufPool *)calloc(1, sizeof(SVBufPool))) == NULL) {
    /* TODO */
    return -1;
  }

  tdListInit(&(pVnode->pBufPool->free), 0);
  tdListInit(&(pVnode->pBufPool->incycle), 0);

  capacity = pVnode->options.wsize / 3;
  if (pVnode->options.isHeapAllocator) {
    type = E_V_HEAP_ALLOCATOR;
  }

  for (int i = 0; i < 3; i++) {
    SListNode *pNode = vBufPoolNewNode(capacity, type);
    if (pNode == NULL) {
      vnodeCloseBufPool(pVnode);
      return -1;
    }

    tdListAppendNode(&(pVnode->pBufPool->free), pNode);
  }

  pVnode->pBufPool->inuse = tdListPopHead(&(pVnode->pBufPool->free));

  return 0;
}

void vnodeCloseBufPool(SVnode *pVnode) {
  SListNode *pNode;
  if (pVnode->pBufPool) {
    // Clear free list
    while ((pNode = tdListPopHead(&(pVnode->pBufPool->free))) != NULL) {
      vBufPoolFreeNode(pNode);
    }

    // Clear incycle list
    while ((pNode = tdListPopHead(&(pVnode->pBufPool->incycle))) != NULL) {
      vBufPoolFreeNode(pNode);
    }

    // Free inuse node
    vBufPoolFreeNode(pVnode->pBufPool->inuse);

    free(pVnode->pBufPool);
    pVnode->pBufPool = NULL;
  }
}

SMemAllocator *vnodeCreateMemAllocator(SVnode *pVnode) {
  SMemAllocator *pma;

  pma = (SMemAllocator *)calloc(1, sizeof(*pma));
  if (pma == NULL) {
    /* TODO */
    return NULL;
  }

  pma->impl = pVnode;
  if (pVnode->options.isHeapAllocator) {
    /* TODO */
    pma->malloc = NULL;
    pma->calloc = NULL;
    pma->realloc = NULL;
    pma->free = NULL;
    pma->usage = NULL;
  } else {
    /* TODO */
    pma->malloc = NULL;
    pma->calloc = NULL;
    pma->realloc = NULL;
    pma->free = NULL;
    pma->usage = NULL;
  }

  return pma;
}

void vnodeDestroyMemAllocator(SMemAllocator *pma) { tfree(pma); }

void vnodeRefMemAllocator(SMemAllocator *pma) {
  SVnode *        pVnode = (SVnode *)pma->impl;
  SVMemAllocator *pvma = (SVMemAllocator *)(pVnode->pBufPool->inuse->data);

  T_REF_INC(pvma);
}

void vnodeUnrefMemAllocator(SMemAllocator *pma) {
  SVnode *        pVnode = (SVnode *)pma->impl;
  SVMemAllocator *pvma = (SVMemAllocator *)(pVnode->pBufPool->inuse->data);

  if (T_REF_DEC(pvma) == 0) {
    /* TODO */
  }
}

/* ------------------------ STATIC METHODS ------------------------ */
static SListNode *vBufPoolNewNode(uint64_t capacity, EVMemAllocatorT type) {
  SListNode *     pNode;
  SVMemAllocator *pvma;

  pNode = (SListNode *)calloc(1, sizeof(*pNode) + sizeof(SVMemAllocator));
  if (pNode == NULL) {
    return NULL;
  }

  pvma = (SVMemAllocator *)(pNode->data);
  pvma->capacity = capacity;
  pvma->type = type;

  switch (type) {
    case E_V_HEAP_ALLOCATOR:
      vHeapAllocatorInit(&(pvma->vha));
      break;
    case E_V_ARENA_ALLOCATOR:
      vArenaAllocatorInit(&(pvma->vaa));
      break;
    default:
      ASSERT(0);
  }

  return pNode;
}

static void vBufPoolFreeNode(SListNode *pNode) {
  if (pNode) {
    free(pNode);
  }
}

// --------------- For arena allocator
static int vArenaAllocatorInit(SVArenaAllocator *pvaa) {
  // TODO
  return 0;
}

static void vArenaAllocatorClear(SVArenaAllocator *pvaa) {
  // TODO
}

// --------------- For heap allocator
static int vHeapAllocatorInit(SVHeapAllocator *pvha) {
  // TODO
  return 0;
}

static void vHeapAllocatorClear(SVHeapAllocator *pvha) {
  // TODO
}