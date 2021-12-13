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
  TD_DLIST(SVMemAllocator) free;
  TD_DLIST(SVMemAllocator) incycle;
  SVMemAllocator *inuse;
  // MAF for submodules
  // SMemAllocatorFactory maf;
};

int vnodeOpenBufPool(SVnode *pVnode) {
  uint64_t capacity;
  // EVMemAllocatorT type = E_V_ARENA_ALLOCATOR;

  if ((pVnode->pBufPool = (SVBufPool *)calloc(1, sizeof(SVBufPool))) == NULL) {
    /* TODO */
    return -1;
  }

  tlistInit(&(pVnode->pBufPool->free));
  tlistInit(&(pVnode->pBufPool->incycle));

  pVnode->pBufPool->inuse = NULL;

  // TODO
  capacity = pVnode->config.wsize / VNODE_BUF_POOL_SHARDS;

  for (int i = 0; i < VNODE_BUF_POOL_SHARDS; i++) {
    SVMemAllocator *pVMA = vmaCreate(capacity, pVnode->config.ssize, pVnode->config.lsize);
    if (pVMA == NULL) {
      // TODO: handle error
      return -1;
    }

    tlistAppend(&(pVnode->pBufPool->free), pVMA);
  }

  return 0;
}

void vnodeCloseBufPool(SVnode *pVnode) {
  if (pVnode->pBufPool) {
    vmaDestroy(pVnode->pBufPool->inuse);

    while (true) {
      SVMemAllocator *pVMA = tlistHead(&(pVnode->pBufPool->incycle));
      if (pVMA == NULL) break;
      tlistPop(&(pVnode->pBufPool->incycle), pVMA);
      vmaDestroy(pVMA);
    }

    while (true) {
      SVMemAllocator *pVMA = tlistHead(&(pVnode->pBufPool->free));
      if (pVMA == NULL) break;
      tlistPop(&(pVnode->pBufPool->free), pVMA);
      vmaDestroy(pVMA);
    }

    free(pVnode->pBufPool);
    pVnode->pBufPool = NULL;
  }
}

void *vnodeMalloc(SVnode *pVnode, uint64_t size) {
  SVBufPool *pBufPool = pVnode->pBufPool;

  if (pBufPool->inuse == NULL) {
    while (true) {
      // TODO: add sem_wait and sem_post
      pBufPool->inuse = tlistHead(&(pBufPool->free));
      if (pBufPool->inuse) {
        tlistPop(&(pBufPool->free), pBufPool->inuse);
        break;
      }
    }
  }

  return vmaMalloc(pBufPool->inuse, size);
}

bool vnodeBufPoolIsFull(SVnode *pVnode) {
  if (pVnode->pBufPool->inuse == NULL) return false;
  return vmaIsFull(pVnode->pBufPool->inuse);
}

#if 0

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


static SListNode *    vBufPoolNewNode(uint64_t capacity, EVMemAllocatorT type);
static void           vBufPoolFreeNode(SListNode *pNode);
static SMemAllocator *vBufPoolCreateMA(SMemAllocatorFactory *pmaf);
static void           vBufPoolDestroyMA(SMemAllocatorFactory *pmaf, SMemAllocator *pma);
static void *         vBufPoolMalloc(SVMemAllocator *pvma, uint64_t size);

/* ------------------------ STATIC METHODS ------------------------ */
static SListNode *vBufPoolNewNode(uint64_t capacity, EVMemAllocatorT type) {
  SListNode *     pNode;
  SVMemAllocator *pvma;
  uint64_t        msize;
  uint64_t        ssize = 4096;  // TODO
  uint64_t        lsize = 1024;  // TODO

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

static void *vBufPoolMalloc(SVMemAllocator *pvma, uint64_t size) {
  void *ptr = NULL;

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

  return ptr;
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
  pma->malloc = NULL;
  pma->calloc = NULL;  /* TODO */
  pma->realloc = NULL; /* TODO */
  pma->free = NULL;    /* TODO */
  pma->usage = NULL;   /* TODO */

  return pma;
}

static void vBufPoolDestroyMA(SMemAllocatorFactory *pmaf, SMemAllocator *pma) { /* TODO */
  SVnode *        pVnode = (SVnode *)(pmaf->impl);
  SListNode *     pNode = ((SVMAWrapper *)(pma->impl))->pNode;
  SVMemAllocator *pvma = (SVMemAllocator *)(pNode->data);

  if (T_REF_DEC(pvma) == 0) {
    if (pvma->type == E_V_ARENA_ALLOCATOR) {
      SVArenaAllocator *pvaa = &(pvma->vaa);
      while (pvaa->inuse != &(pvaa->node)) {
        SVArenaNode *pNode = pvaa->inuse;
        pvaa->inuse = pNode->prev;
        /* code */
      }

      pvaa->inuse->ptr = pvaa->inuse->data;
    } else if (pvma->type == E_V_HEAP_ALLOCATOR) {
    } else {
      ASSERT(0);
    }

    // Move node from incycle to free
    tdListAppendNode(&(pVnode->pBufPool->free), tdListPopNode(&(pVnode->pBufPool->incycle), pNode));
    // tsem_post(); todo: sem_post
  }
}
#endif