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

#include "vnd.h"

/* ------------------------ STRUCTURES ------------------------ */
#define VNODE_BUF_POOL_SHARDS 3

struct SVBufPool {
  TdThreadMutex mutex;
  TdThreadCond  hasFree;
  TD_DLIST(SVMemAllocator) free;
  TD_DLIST(SVMemAllocator) incycle;
  SVMemAllocator *inuse;
  // MAF for submodules to use
  SMemAllocatorFactory *pMAF;
};

static SMemAllocator *vBufPoolCreateMA(SMemAllocatorFactory *pMAF);
static void           vBufPoolDestroyMA(SMemAllocatorFactory *pMAF, SMemAllocator *pMA);

int vnodeOpenBufPool(SVnode *pVnode) {
  uint64_t capacity;

  if ((pVnode->pBufPool = (SVBufPool *)calloc(1, sizeof(SVBufPool))) == NULL) {
    /* TODO */
    return -1;
  }

  TD_DLIST_INIT(&(pVnode->pBufPool->free));
  TD_DLIST_INIT(&(pVnode->pBufPool->incycle));

  pVnode->pBufPool->inuse = NULL;

  // TODO
  capacity = pVnode->config.wsize / VNODE_BUF_POOL_SHARDS;

  for (int i = 0; i < VNODE_BUF_POOL_SHARDS; i++) {
    SVMemAllocator *pVMA = vmaCreate(capacity, pVnode->config.ssize, pVnode->config.lsize);
    if (pVMA == NULL) {
      // TODO: handle error
      return -1;
    }

    TD_DLIST_APPEND(&(pVnode->pBufPool->free), pVMA);
  }

  pVnode->pBufPool->pMAF = (SMemAllocatorFactory *)malloc(sizeof(SMemAllocatorFactory));
  if (pVnode->pBufPool->pMAF == NULL) {
    // TODO: handle error
    return -1;
  }
  pVnode->pBufPool->pMAF->impl = pVnode;
  pVnode->pBufPool->pMAF->create = vBufPoolCreateMA;
  pVnode->pBufPool->pMAF->destroy = vBufPoolDestroyMA;

  return 0;
}

void vnodeCloseBufPool(SVnode *pVnode) {
  if (pVnode->pBufPool) {
    tfree(pVnode->pBufPool->pMAF);
    vmaDestroy(pVnode->pBufPool->inuse);

    while (true) {
      SVMemAllocator *pVMA = TD_DLIST_HEAD(&(pVnode->pBufPool->incycle));
      if (pVMA == NULL) break;
      TD_DLIST_POP(&(pVnode->pBufPool->incycle), pVMA);
      vmaDestroy(pVMA);
    }

    while (true) {
      SVMemAllocator *pVMA = TD_DLIST_HEAD(&(pVnode->pBufPool->free));
      if (pVMA == NULL) break;
      TD_DLIST_POP(&(pVnode->pBufPool->free), pVMA);
      vmaDestroy(pVMA);
    }

    free(pVnode->pBufPool);
    pVnode->pBufPool = NULL;
  }
}

int vnodeBufPoolSwitch(SVnode *pVnode) {
  SVMemAllocator *pvma = pVnode->pBufPool->inuse;

  pVnode->pBufPool->inuse = NULL;

  if (pvma) {
    TD_DLIST_APPEND(&(pVnode->pBufPool->incycle), pvma);
  }
  return 0;
}

int vnodeBufPoolRecycle(SVnode *pVnode) {
  SVBufPool *     pBufPool = pVnode->pBufPool;
  SVMemAllocator *pvma = TD_DLIST_HEAD(&(pBufPool->incycle));
  if (pvma == NULL) return 0;
  // ASSERT(pvma != NULL);

  TD_DLIST_POP(&(pBufPool->incycle), pvma);
  vmaReset(pvma);
  TD_DLIST_APPEND(&(pBufPool->free), pvma);

  return 0;
}

void *vnodeMalloc(SVnode *pVnode, uint64_t size) {
  SVBufPool *pBufPool = pVnode->pBufPool;

  if (pBufPool->inuse == NULL) {
    while (true) {
      // TODO: add sem_wait and sem_post
      pBufPool->inuse = TD_DLIST_HEAD(&(pBufPool->free));
      if (pBufPool->inuse) {
        TD_DLIST_POP(&(pBufPool->free), pBufPool->inuse);
        break;
      } else {
        // tsem_wait(&(pBufPool->hasFree));
      }
    }
  }

  return vmaMalloc(pBufPool->inuse, size);
}

bool vnodeBufPoolIsFull(SVnode *pVnode) {
  if (pVnode->pBufPool->inuse == NULL) return false;
  return vmaIsFull(pVnode->pBufPool->inuse);
}

SMemAllocatorFactory *vBufPoolGetMAF(SVnode *pVnode) { return pVnode->pBufPool->pMAF; }

/* ------------------------ STATIC METHODS ------------------------ */
typedef struct {
  SVnode *        pVnode;
  SVMemAllocator *pVMA;
} SVMAWrapper;

static FORCE_INLINE void *vmaMaloocCb(SMemAllocator *pMA, uint64_t size) {
  SVMAWrapper *pWrapper = (SVMAWrapper *)(pMA->impl);

  return vmaMalloc(pWrapper->pVMA, size);
}

// TODO: Add atomic operations here
static SMemAllocator *vBufPoolCreateMA(SMemAllocatorFactory *pMAF) {
  SMemAllocator *pMA;
  SVnode *       pVnode = (SVnode *)(pMAF->impl);
  SVMAWrapper *  pWrapper;

  pMA = (SMemAllocator *)calloc(1, sizeof(*pMA) + sizeof(SVMAWrapper));
  if (pMA == NULL) {
    return NULL;
  }

  pVnode->pBufPool->inuse->_ref.val++;
  pWrapper = POINTER_SHIFT(pMA, sizeof(*pMA));
  pWrapper->pVnode = pVnode;
  pWrapper->pVMA = pVnode->pBufPool->inuse;

  pMA->impl = pWrapper;
  TD_MA_MALLOC_FUNC(pMA) = vmaMaloocCb;

  return pMA;
}

static void vBufPoolDestroyMA(SMemAllocatorFactory *pMAF, SMemAllocator *pMA) {
  SVMAWrapper *   pWrapper = (SVMAWrapper *)(pMA->impl);
  SVnode *        pVnode = pWrapper->pVnode;
  SVMemAllocator *pVMA = pWrapper->pVMA;

  free(pMA);
  if (--pVMA->_ref.val == 0) {
    TD_DLIST_POP(&(pVnode->pBufPool->incycle), pVMA);
    vmaReset(pVMA);
    TD_DLIST_APPEND(&(pVnode->pBufPool->free), pVMA);
  }
}
