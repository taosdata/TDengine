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
static int vnodeBufPoolCreate(SVnode *pVnode, int32_t id, int64_t size, SVBufPool **ppPool) {
  SVBufPool *pPool;

  pPool = taosMemoryMalloc(sizeof(SVBufPool) + size);
  if (pPool == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  memset(pPool, 0, sizeof(SVBufPool));

  // query handle list
  taosThreadMutexInit(&pPool->mutex, NULL);
  pPool->nQuery = 0;
  pPool->qList.pNext = &pPool->qList;
  pPool->qList.ppNext = &pPool->qList.pNext;

  pPool->pVnode = pVnode;
  pPool->id = id;
  pPool->ptr = pPool->node.data;
  pPool->pTail = &pPool->node;
  pPool->node.prev = NULL;
  pPool->node.pnext = &pPool->pTail;
  pPool->node.size = size;

  if (VND_IS_RSMA(pVnode)) {
    pPool->lock = taosMemoryMalloc(sizeof(TdThreadSpinlock));
    if (!pPool->lock) {
      taosMemoryFree(pPool);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    if (taosThreadSpinInit(pPool->lock, 0) != 0) {
      taosMemoryFree((void *)pPool->lock);
      taosMemoryFree(pPool);
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  } else {
    pPool->lock = NULL;
  }

  *ppPool = pPool;
  return 0;
}

static int vnodeBufPoolDestroy(SVBufPool *pPool) {
  vnodeBufPoolReset(pPool);
  if (pPool->lock) {
    taosThreadSpinDestroy(pPool->lock);
    taosMemoryFree((void *)pPool->lock);
  }
  taosThreadMutexDestroy(&pPool->mutex);
  taosMemoryFree(pPool);
  return 0;
}

int vnodeOpenBufPool(SVnode *pVnode) {
  int64_t size = pVnode->config.szBuf / VNODE_BUFPOOL_SEGMENTS;

  for (int i = 0; i < VNODE_BUFPOOL_SEGMENTS; i++) {
    // create pool
    if (vnodeBufPoolCreate(pVnode, i, size, &pVnode->aBufPool[i])) {
      vError("vgId:%d, failed to open vnode buffer pool since %s", TD_VID(pVnode), tstrerror(terrno));
      vnodeCloseBufPool(pVnode);
      return -1;
    }

    // add to free list
    pVnode->aBufPool[i]->freeNext = pVnode->freeList;
    pVnode->freeList = pVnode->aBufPool[i];
  }

  vDebug("vgId:%d, vnode buffer pool is opened, size:%" PRId64, TD_VID(pVnode), size);
  return 0;
}

int vnodeCloseBufPool(SVnode *pVnode) {
  for (int32_t i = 0; i < VNODE_BUFPOOL_SEGMENTS; i++) {
    if (pVnode->aBufPool[i]) {
      vnodeBufPoolDestroy(pVnode->aBufPool[i]);
      pVnode->aBufPool[i] = NULL;
    }
  }

  vDebug("vgId:%d, vnode buffer pool is closed", TD_VID(pVnode));
  return 0;
}

void vnodeBufPoolReset(SVBufPool *pPool) {
  ASSERT(pPool->nQuery == 0);
  for (SVBufPoolNode *pNode = pPool->pTail; pNode->prev; pNode = pPool->pTail) {
    ASSERT(pNode->pnext == &pPool->pTail);
    pNode->prev->pnext = &pPool->pTail;
    pPool->pTail = pNode->prev;
    pPool->size = pPool->size - sizeof(*pNode) - pNode->size;
    taosMemoryFree(pNode);
  }

  ASSERT(pPool->size == pPool->ptr - pPool->node.data);

  pPool->size = 0;
  pPool->ptr = pPool->node.data;
}

void *vnodeBufPoolMallocAligned(SVBufPool *pPool, int size) {
  SVBufPoolNode *pNode;
  void          *p = NULL;
  uint8_t       *ptr = NULL;
  int            paddingLen = 0;
  ASSERT(pPool != NULL);

  if (pPool->lock) taosThreadSpinLock(pPool->lock);

  ptr = pPool->ptr;
  paddingLen = (((long)ptr + 7) & ~7) - (long)ptr;

  if (pPool->node.size >= pPool->ptr - pPool->node.data + size + paddingLen) {
    // allocate from the anchor node
    p = pPool->ptr + paddingLen;
    size += paddingLen;
    pPool->ptr = pPool->ptr + size;
    pPool->size += size;
  } else {
    // allocate a new node
    pNode = taosMemoryMalloc(sizeof(*pNode) + size);
    if (pNode == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      if (pPool->lock) taosThreadSpinUnlock(pPool->lock);
      return NULL;
    }

    p = pNode->data;
    pNode->size = size;
    pNode->prev = pPool->pTail;
    pNode->pnext = &pPool->pTail;
    pPool->pTail->pnext = &pNode->prev;
    pPool->pTail = pNode;

    pPool->size = pPool->size + sizeof(*pNode) + size;
  }
  if (pPool->lock) taosThreadSpinUnlock(pPool->lock);
  return p;
}

void *vnodeBufPoolMalloc(SVBufPool *pPool, int size) {
  SVBufPoolNode *pNode;
  void          *p = NULL;
  ASSERT(pPool != NULL);

  if (pPool->lock) taosThreadSpinLock(pPool->lock);
  if (pPool->node.size >= pPool->ptr - pPool->node.data + size) {
    // allocate from the anchor node
    p = pPool->ptr;
    pPool->ptr = pPool->ptr + size;
    pPool->size += size;
  } else {
    // allocate a new node
    pNode = taosMemoryMalloc(sizeof(*pNode) + size);
    if (pNode == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      if (pPool->lock) taosThreadSpinUnlock(pPool->lock);
      return NULL;
    }

    p = pNode->data;
    pNode->size = size;
    pNode->prev = pPool->pTail;
    pNode->pnext = &pPool->pTail;
    pPool->pTail->pnext = &pNode->prev;
    pPool->pTail = pNode;

    pPool->size = pPool->size + sizeof(*pNode) + size;
  }
  if (pPool->lock) taosThreadSpinUnlock(pPool->lock);
  return p;
}

void vnodeBufPoolFree(SVBufPool *pPool, void *p) {
  // uint8_t       *ptr = (uint8_t *)p;
  // SVBufPoolNode *pNode;

  // if (ptr < pPool->node.data || ptr >= pPool->node.data + pPool->node.size) {
  //   pNode = &((SVBufPoolNode *)p)[-1];
  //   *pNode->pnext = pNode->prev;
  //   pNode->prev->pnext = pNode->pnext;

  //   pPool->size = pPool->size - sizeof(*pNode) - pNode->size;
  //   taosMemoryFree(pNode);
  // }
}

void vnodeBufPoolRef(SVBufPool *pPool) {
  int32_t nRef = atomic_fetch_add_32(&pPool->nRef, 1);
  ASSERT(nRef > 0);
}

void vnodeBufPoolAddToFreeList(SVBufPool *pPool) {
  SVnode *pVnode = pPool->pVnode;

  int64_t size = pVnode->config.szBuf / VNODE_BUFPOOL_SEGMENTS;
  if (pPool->node.size != size) {
    SVBufPool *pNewPool = NULL;
    if (vnodeBufPoolCreate(pVnode, pPool->id, size, &pNewPool) < 0) {
      vWarn("vgId:%d, failed to change buffer pool of id %d size from %" PRId64 " to %" PRId64 " since %s",
            TD_VID(pVnode), pPool->id, pPool->node.size, size, tstrerror(errno));
    } else {
      vInfo("vgId:%d, buffer pool of id %d size changed from %" PRId64 " to %" PRId64, TD_VID(pVnode), pPool->id,
            pPool->node.size, size);

      vnodeBufPoolDestroy(pPool);
      pPool = pNewPool;
      pVnode->aBufPool[pPool->id] = pPool;
    }
  }

  // add to free list
  vDebug("vgId:%d, buffer pool %p of id %d is added to free list", TD_VID(pVnode), pPool, pPool->id);
  vnodeBufPoolReset(pPool);
  pPool->freeNext = pVnode->freeList;
  pVnode->freeList = pPool;
  taosThreadCondSignal(&pVnode->poolNotEmpty);
}

void vnodeBufPoolUnRef(SVBufPool *pPool, bool proactive) {
  if (pPool == NULL) return;

  SVnode *pVnode = pPool->pVnode;

  if (proactive) taosThreadMutexLock(&pVnode->mutex);

  if (atomic_sub_fetch_32(&pPool->nRef, 1) > 0) goto _exit;

  // remove from recycle queue or on-recycle position
  if (pVnode->onRecycle == pPool) {
    pVnode->onRecycle = NULL;
  } else {
    if (pPool->recyclePrev) {
      pPool->recyclePrev->recycleNext = pPool->recycleNext;
    } else {
      pVnode->recycleHead = pPool->recycleNext;
    }

    if (pPool->recycleNext) {
      pPool->recycleNext->recyclePrev = pPool->recyclePrev;
    } else {
      pVnode->recycleTail = pPool->recyclePrev;
    }
    pPool->recyclePrev = pPool->recycleNext = NULL;
  }

  vnodeBufPoolAddToFreeList(pPool);

_exit:
  if (proactive) taosThreadMutexUnlock(&pVnode->mutex);
  return;
}

int32_t vnodeBufPoolRegisterQuery(SVBufPool *pPool, SQueryNode *pQNode) {
  int32_t code = 0;

  taosThreadMutexLock(&pPool->mutex);

  pQNode->pNext = pPool->qList.pNext;
  pQNode->ppNext = &pPool->qList.pNext;
  pPool->qList.pNext->ppNext = &pQNode->pNext;
  pPool->qList.pNext = pQNode;
  pPool->nQuery++;

  taosThreadMutexUnlock(&pPool->mutex);

_exit:
  return code;
}

void vnodeBufPoolDeregisterQuery(SVBufPool *pPool, SQueryNode *pQNode, bool proactive) {
  int32_t code = 0;

  if (proactive) taosThreadMutexLock(&pPool->mutex);

  pQNode->pNext->ppNext = pQNode->ppNext;
  *pQNode->ppNext = pQNode->pNext;
  pPool->nQuery--;

  if (proactive) taosThreadMutexUnlock(&pPool->mutex);
}

int32_t vnodeBufPoolRecycle(SVBufPool *pPool) {
  int32_t code = 0;

  SVnode *pVnode = pPool->pVnode;

  vDebug("vgId:%d, recycle buffer pool %p of id %d", TD_VID(pVnode), pPool, pPool->id);

  taosThreadMutexLock(&pPool->mutex);

  SQueryNode *pNode = pPool->qList.pNext;
  while (pNode != &pPool->qList) {
    SQueryNode *pTNode = pNode->pNext;

    int32_t rc = pNode->reseek(pNode->pQHandle);
    if (rc == 0 || rc == TSDB_CODE_VND_QUERY_BUSY) {
      pNode = pTNode;
    } else {
      code = rc;
      goto _exit;
    }
  }

_exit:
  taosThreadMutexUnlock(&pPool->mutex);
  return code;
}
