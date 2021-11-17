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
#define VNODE_BUF_POOL_SHARDS 3

struct SVBufPool {
  // buffer pool impl
  SList      free;
  SList      incycle;
  SListNode *inuse;
  // MAF for submodules
  SMemAllocatorFactory maf;
};

typedef enum {
  // Heap allocator
  E_V_HEAP_ALLOCATOR = 0,
  // Arena allocator
  E_V_ARENA_ALLOCATOR
} EVMemAllocatorT;

typedef struct {
  /* TODO */
} SVHeapAllocator;

typedef struct SVArenaNode {
  struct SVArenaNode *prev;
  uint64_t            size;
  void *              ptr;
  char                data[];
} SVArenaNode;

typedef struct {
  uint64_t     ssize;  // step size
  uint64_t     lsize;  // limit size
  SVArenaNode *inuse;
  SVArenaNode  node;
} SVArenaAllocator;

typedef struct {
  SVnode *   pVnode;
  SListNode *pNode;
} SVMAWrapper;

typedef struct {
  T_REF_DECLARE()
  uint64_t        capacity;
  EVMemAllocatorT type;
  union {
    SVHeapAllocator  vha;
    SVArenaAllocator vaa;
  };
} SVMemAllocator;

static SListNode *    vBufPoolNewNode(uint64_t capacity, EVMemAllocatorT type);
static void           vBufPoolFreeNode(SListNode *pNode);
static SMemAllocator *vBufPoolCreateMA(SMemAllocatorFactory *pmaf);
static void           vBufPoolDestroyMA(SMemAllocatorFactory *pmaf, SMemAllocator *pma);

int vnodeOpenBufPool(SVnode *pVnode) {
  uint64_t        capacity;
  EVMemAllocatorT type = E_V_ARENA_ALLOCATOR;

  if ((pVnode->pBufPool = (SVBufPool *)calloc(1, sizeof(SVBufPool))) == NULL) {
    /* TODO */
    return -1;
  }

  tdListInit(&(pVnode->pBufPool->free), 0);
  tdListInit(&(pVnode->pBufPool->incycle), 0);

  capacity = pVnode->options.wsize / VNODE_BUF_POOL_SHARDS;
  if (pVnode->options.isHeapAllocator) {
    type = E_V_HEAP_ALLOCATOR;
  }

  for (int i = 0; i < VNODE_BUF_POOL_SHARDS; i++) {
    SListNode *pNode = vBufPoolNewNode(capacity, type);
    if (pNode == NULL) {
      vnodeCloseBufPool(pVnode);
      return -1;
    }

    tdListAppendNode(&(pVnode->pBufPool->free), pNode);
  }

  pVnode->pBufPool->maf.impl = pVnode;
  pVnode->pBufPool->maf.create = vBufPoolCreateMA;
  pVnode->pBufPool->maf.destroy = vBufPoolDestroyMA;

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
    if (pVnode->pBufPool->inuse) {
      vBufPoolFreeNode(pVnode->pBufPool->inuse);
    }

    free(pVnode->pBufPool);
    pVnode->pBufPool = NULL;
  }
}

/* ------------------------ STATIC METHODS ------------------------ */
static void vArenaAllocatorInit(SVArenaAllocator *pvaa, uint64_t capacity, uint64_t ssize, uint64_t lsize) { /* TODO */
  pvaa->ssize = ssize;
  pvaa->lsize = lsize;
  pvaa->inuse = &pvaa->node;

  pvaa->node.prev = NULL;
  pvaa->node.size = capacity;
  pvaa->node.ptr = pvaa->node.data;
}

static void vArenaAllocatorClear(SVArenaAllocator *pvaa) { /* TODO */
  while (pvaa->inuse != &(pvaa->node)) {
    SVArenaNode *pANode = pvaa->inuse;
    pvaa->inuse = pANode->prev;
    free(pANode);
  }
}

static SListNode *vBufPoolNewNode(uint64_t capacity, EVMemAllocatorT type) {
  SListNode *     pNode;
  SVMemAllocator *pvma;
  uint64_t        msize;
  uint64_t        ssize = 0;  // TODO
  uint64_t        lsize = 0;  // TODO

  msize = sizeof(SListNode) + sizeof(SVMemAllocator);
  if (type == E_V_ARENA_ALLOCATOR) {
    msize += capacity;
  }

  pNode = (SListNode *)calloc(1, msize);
  if (pNode == NULL) {
    // TODO: handle error
    return NULL;
  }

  pvma = (SVMemAllocator *)(pNode->data);
  pvma->capacity = capacity;
  pvma->type = type;

  switch (type) {
    case E_V_ARENA_ALLOCATOR:
      vArenaAllocatorInit(&(pvma->vaa), capacity, ssize, lsize);
      break;
    case E_V_HEAP_ALLOCATOR:
      // vHeapAllocatorInit(&(pvma->vha));
      break;
    default:
      ASSERT(0);
  }

  return pNode;
}

static void vBufPoolFreeNode(SListNode *pNode) {
  SVMemAllocator *pvma = (SVMemAllocator *)(pNode->data);

  switch (pvma->type) {
    case E_V_ARENA_ALLOCATOR:
      vArenaAllocatorClear(&(pvma->vaa));
      break;
    case E_V_HEAP_ALLOCATOR:
      // vHeapAllocatorClear(&(pvma->vha));
      break;
    default:
      break;
  }

  free(pNode);
}

static void *vBufPoolMalloc(SMemAllocator *pma, uint64_t size) {
  SVMAWrapper *   pvmaw = (SVMAWrapper *)(pma->impl);
  SVMemAllocator *pvma = (SVMemAllocator *)(pvmaw->pNode->data);
  void *          ptr = NULL;

  if (pvma->type == E_V_ARENA_ALLOCATOR) {
    SVArenaAllocator *pvaa = &(pvma->vaa);

    if (POINTER_DISTANCE(pvaa->inuse->ptr, pvaa->inuse->data) + size > pvaa->inuse->size) {
      SVArenaNode *pNode = (SVArenaNode *)malloc(sizeof(*pNode) + MAX(size, pvaa->ssize));
      if (pNode == NULL) {
        // TODO: handle error
        return NULL;
      }

      pNode->prev = pvaa->inuse;
      pNode->size = MAX(size, pvaa->ssize);
      pNode->ptr = pNode->data;

      pvaa->inuse = pNode;
    }

    ptr = pvaa->inuse->ptr;
    pvaa->inuse->ptr = POINTER_SHIFT(ptr, size);
  } else if (pvma->type == E_V_HEAP_ALLOCATOR) {
    /* TODO */
  }

  return NULL;
}

static SMemAllocator *vBufPoolCreateMA(SMemAllocatorFactory *pmaf) {
  SVnode *        pVnode;
  SMemAllocator * pma;
  SVMemAllocator *pvma;
  SVMAWrapper *   pvmaw;

  pVnode = (SVnode *)(pmaf->impl);
  pma = (SMemAllocator *)calloc(1, sizeof(*pma) + sizeof(SVMAWrapper));
  if (pma == NULL) {
    // TODO: handle error
    return NULL;
  }
  pvmaw = (SVMAWrapper *)POINTER_SHIFT(pma, sizeof(*pma));

  // No allocator used currently
  if (pVnode->pBufPool->inuse == NULL) {
    while (listNEles(&(pVnode->pBufPool->free)) == 0) {
      // TODO: wait until all released ro kill query
      // tsem_wait();
      ASSERT(0);
    }

    pVnode->pBufPool->inuse = tdListPopHead(&(pVnode->pBufPool->free));
    pvma = (SVMemAllocator *)(pVnode->pBufPool->inuse->data);
    T_REF_INIT_VAL(pvma, 1);
  } else {
    pvma = (SVMemAllocator *)(pVnode->pBufPool->inuse->data);
  }

  T_REF_INC(pvma);

  pvmaw->pVnode = pVnode;
  pvmaw->pNode = pVnode->pBufPool->inuse;

  pma->impl = pvmaw;
  pma->malloc = vBufPoolMalloc;
  pma->calloc = NULL;  /* TODO */
  pma->realloc = NULL; /* TODO */
  pma->free = NULL;    /* TODO */
  pma->usage = NULL;   /* TODO */

  return pma;
}

static void vBufPoolDestroyMA(SMemAllocatorFactory *pmaf, SMemAllocator *pma) { /* TODO */
}