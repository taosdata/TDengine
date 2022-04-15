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

#include "vnodeInt.h"

/* ------------------------ STRUCTURES ------------------------ */

static int vnodeBufPoolCreate(int size, SVBufPool **ppPool);
static int vnodeBufPoolDestroy(SVBufPool *pPool);

int vnodeOpenBufPool(SVnode *pVnode, int64_t size) {
  SVBufPool *pPool = NULL;
  int        ret;

  ASSERT(pVnode->pPool == NULL);

  for (int i = 0; i < 3; i++) {
    // create pool
    ret = vnodeBufPoolCreate(size, &pPool);
    if (ret < 0) {
      vError("vgId:%d failed to open vnode buffer pool since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
      vnodeCloseBufPool(pVnode);
      return -1;
    }

    // add pool to queue
    pPool->next = pVnode->pPool;
    pVnode->pPool = pPool;
  }

  vDebug("vgId:%d vnode buffer pool is opened, pool size: %" PRId64, TD_VNODE_ID(pVnode), size);

  return 0;
}

int vnodeCloseBufPool(SVnode *pVnode) {
  SVBufPool *pPool;

  for (pPool = pVnode->pPool; pPool; pPool = pVnode->pPool) {
    pVnode->pPool = pPool->next;
    vnodeBufPoolDestroy(pPool);
  }

  vDebug("vgId:%d vnode buffer pool is closed", TD_VNODE_ID(pVnode));

  return 0;
}

void vnodeBufPoolReset(SVBufPool *pPool) {
  SVBufPoolNode *pNode;

  for (pNode = pPool->pTail; pNode->prev; pNode = pPool->pTail) {
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

void *vnodeBufPoolMalloc(SVBufPool *pPool, size_t size) {
  SVBufPoolNode *pNode;
  void          *p;

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

  return p;
}

void vnodeBufPoolFree(SVBufPool *pPool, void *p) {
  uint8_t       *ptr = (uint8_t *)p;
  SVBufPoolNode *pNode;

  if (ptr < pPool->node.data || ptr >= pPool->node.data + pPool->node.size) {
    pNode = &((SVBufPoolNode *)p)[-1];
    *pNode->pnext = pNode->prev;
    pNode->prev->pnext = pNode->pnext;

    pPool->size = pPool->size - sizeof(*pNode) - pNode->size;
    taosMemoryFree(pNode);
  }
}

// STATIC METHODS -------------------
static int vnodeBufPoolCreate(int size, SVBufPool **ppPool) {
  SVBufPool *pPool;

  pPool = taosMemoryMalloc(sizeof(SVBufPool) + size);
  if (pPool == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pPool->next = NULL;
  pPool->nRef = 0;
  pPool->size = 0;
  pPool->ptr = pPool->node.data;
  pPool->pTail = &pPool->node;
  pPool->node.prev = NULL;
  pPool->node.pnext = &pPool->pTail;
  pPool->node.size = size;

  *ppPool = pPool;
  return 0;
}

static int vnodeBufPoolDestroy(SVBufPool *pPool) {
  vnodeBufPoolReset(pPool);
  taosMemoryFree(pPool);
  return 0;
}