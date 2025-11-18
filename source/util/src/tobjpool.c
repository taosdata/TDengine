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

#include "tobjpool.h"

#define TOBJPOOL_MIN_SIZE     4
#define BOUNDARY_SIZE         1024 * 1024 * 1024  // 1G
#define BOUNDARY_SMALL_FACTOR 1.2
#define BOUNDARY_BIG_FACTOR   2

int32_t taosObjPoolInit(SObjPool *pPool, int64_t cap, size_t objSize) {
  if (pPool == NULL || objSize == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (cap < TOBJPOOL_MIN_SIZE) {
    cap = TOBJPOOL_MIN_SIZE;
  }

  pPool->nodeSize = sizeof(SObjPoolNode) + objSize;
  pPool->pData = taosMemoryCalloc(cap, pPool->nodeSize);
  if (pPool->pData == NULL) {
    return terrno;
  }

  // initialize free list
  for (int64_t i = 0; i < cap; i++) {
    SObjPoolNode *pNode = TOBJPOOL_GET_NODE(pPool, i);
    pNode->prevIdx = i - 1;
    pNode->nextIdx = i + 1;
  }
  TOBJPOOL_GET_NODE(pPool, 0)->prevIdx = TOBJPOOL_INVALID_IDX;
  TOBJPOOL_GET_NODE(pPool, cap - 1)->nextIdx = TOBJPOOL_INVALID_IDX;
  pPool->freeHeadIdx = 0;
  pPool->freeTailIdx = cap - 1;

  pPool->size = 0;
  pPool->capacity = cap;

  return TSDB_CODE_SUCCESS;
}

int32_t taosObjPoolEnsureCap(SObjPool *pPool, int64_t newCap) {
  if (newCap < pPool->capacity) {
    return TSDB_CODE_SUCCESS;
  }
  double  factor = (newCap * pPool->nodeSize > BOUNDARY_SIZE) ? BOUNDARY_SMALL_FACTOR : BOUNDARY_BIG_FACTOR;
  int64_t tsize = (pPool->capacity * factor);
  while (newCap > tsize) {
    int64_t newSize = tsize * factor;
    tsize = (newSize == tsize) ? (tsize + 2) : newSize;
  }

  char *p = taosMemoryRealloc(pPool->pData, tsize * pPool->nodeSize);
  if (p == NULL) {
    return terrno;
  }
  pPool->pData = p;

  // append new nodes to free list
  for (int64_t i = pPool->capacity; i < tsize; i++) {
    SObjPoolNode *pNode = TOBJPOOL_GET_NODE(pPool, i);
    pNode->prevIdx = i - 1;
    pNode->nextIdx = i + 1;
  }
  TOBJPOOL_GET_NODE(pPool, pPool->capacity)->prevIdx = pPool->freeTailIdx;
  TOBJPOOL_GET_NODE(pPool, tsize - 1)->nextIdx = TOBJPOOL_INVALID_IDX;
  if (pPool->freeTailIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pPool->freeTailIdx)->nextIdx = pPool->capacity;
  } else {
    pPool->freeHeadIdx = pPool->capacity;
  }
  pPool->freeTailIdx = tsize - 1;

  pPool->capacity = tsize;
  return TSDB_CODE_SUCCESS;
}

void taosObjPoolDestroy(SObjPool *pPool) {
  if (pPool != NULL) {
    if (pPool->size != 0) {
      uWarn("destroying non-empty obj pool, size:%" PRId64 ", capacity:%" PRId64, pPool->size, pPool->capacity);
    }
    taosMemoryFreeClear(pPool->pData);
    pPool->freeHeadIdx = pPool->freeTailIdx = TOBJPOOL_INVALID_IDX;
    pPool->size = 0;
    pPool->capacity = 0;
  }
}

int32_t taosObjListInit(SObjList *pList, SObjPool *pPool) {
  if (pList == NULL || pPool == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  pList->pPool = pPool;
  pList->neles = 0;
  pList->headIdx = TOBJPOOL_INVALID_IDX;
  pList->tailIdx = TOBJPOOL_INVALID_IDX;
  return TSDB_CODE_SUCCESS;
}

void taosObjListClear(SObjList *pList) {
  if (pList == NULL || pList->headIdx == TOBJPOOL_INVALID_IDX) {
    return;
  }

  SObjPool     *pPool = pList->pPool;
  SObjPoolNode *pTail = TOBJPOOL_GET_NODE(pPool, pList->tailIdx);
  pTail->nextIdx = pPool->freeHeadIdx;
  if (pPool->freeHeadIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pPool->freeHeadIdx)->prevIdx = pList->tailIdx;
  } else {
    pPool->freeTailIdx = pList->tailIdx;
  }
  pPool->freeHeadIdx = pList->headIdx;
  pPool->size -= pList->neles;

  pList->headIdx = pList->tailIdx = TOBJPOOL_INVALID_IDX;
  pList->neles = 0;
}

void taosObjListClearEx(SObjList *pList, FDelete fp) {
  if (pList == NULL || pList->pPool == NULL || pList->headIdx == TOBJPOOL_INVALID_IDX) {
    return;
  }

  if (fp != NULL) {
    SObjPool *pPool = pList->pPool;
    int64_t   idx = pList->headIdx;
    while (idx != TOBJPOOL_INVALID_IDX) {
      SObjPoolNode *pNode = TOBJPOOL_GET_NODE(pPool, idx);
      fp(TOBJPOOL_NODE_GET_OBJ(pNode));
      idx = pNode->nextIdx;
    }
  }

  taosObjListClear(pList);
}

int32_t taosObjListPrepend(SObjList *pList, const void *pData) {
  if (pList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SObjPool *pPool = pList->pPool;
  if (pPool->freeHeadIdx == TOBJPOOL_INVALID_IDX) {
    int32_t code = taosObjPoolEnsureCap(pPool, pPool->capacity + 1);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  // allocate a new node from pool
  int64_t       newIdx = pPool->freeHeadIdx;
  SObjPoolNode *pNewNode = TOBJPOOL_GET_NODE(pPool, newIdx);
  pPool->freeHeadIdx = pNewNode->nextIdx;
  if (pPool->freeHeadIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pPool->freeHeadIdx)->prevIdx = TOBJPOOL_INVALID_IDX;
  } else {
    pPool->freeTailIdx = TOBJPOOL_INVALID_IDX;
  }
  pPool->size += 1;

  TAOS_MEMCPY(TOBJPOOL_NODE_GET_OBJ(pNewNode), pData, pPool->nodeSize - sizeof(SObjPoolNode));

  // insert the new node to the head of the list
  pNewNode->prevIdx = TOBJPOOL_INVALID_IDX;
  pNewNode->nextIdx = pList->headIdx;
  if (pList->headIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pList->headIdx)->prevIdx = newIdx;
  } else {
    pList->tailIdx = newIdx;
  }
  pList->headIdx = newIdx;
  pList->neles += 1;

  return TSDB_CODE_SUCCESS;
}

int32_t taosObjListAppend(SObjList *pList, const void *pData) {
  if (pList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SObjPool *pPool = pList->pPool;
  if (pPool->freeHeadIdx == TOBJPOOL_INVALID_IDX) {
    int32_t code = taosObjPoolEnsureCap(pPool, pPool->capacity + 1);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  // allocate a new node from pool
  int64_t       newIdx = pPool->freeHeadIdx;
  SObjPoolNode *pNewNode = TOBJPOOL_GET_NODE(pPool, newIdx);
  pPool->freeHeadIdx = pNewNode->nextIdx;
  if (pPool->freeHeadIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pPool->freeHeadIdx)->prevIdx = TOBJPOOL_INVALID_IDX;
  } else {
    pPool->freeTailIdx = TOBJPOOL_INVALID_IDX;
  }
  pPool->size += 1;

  TAOS_MEMCPY(TOBJPOOL_NODE_GET_OBJ(pNewNode), pData, pPool->nodeSize - sizeof(SObjPoolNode));

  // insert the new node to the tail of the list
  pNewNode->nextIdx = TOBJPOOL_INVALID_IDX;
  pNewNode->prevIdx = pList->tailIdx;
  if (pList->tailIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pList->tailIdx)->nextIdx = newIdx;
  } else {
    pList->headIdx = newIdx;
  }
  pList->tailIdx = newIdx;
  pList->neles += 1;

  return TSDB_CODE_SUCCESS;
}

void taosObjListPopHeadTo(SObjList *pList, void *pObj, int32_t nele) {
  if (pList == NULL || pList->headIdx == TOBJPOOL_INVALID_IDX) {
    return;
  }

  if (pObj == NULL) {
    taosObjListClear(pList);
    return;
  }

  SObjPool     *pPool = pList->pPool;
  SObjPoolNode *pNode = TOBJPOOL_OBJ_GET_NODE(pObj);
  int64_t       idx = TOBJPOOL_GET_IDX(pPool, pNode);
  if (idx < 0 || idx >= pPool->capacity || pNode->prevIdx == TOBJPOOL_INVALID_IDX) {
    return;
  }
  SObjPoolNode *pPrevNode = TOBJPOOL_GET_NODE(pPool, pNode->prevIdx);

  // add all nodes before pNode back to pool
  pPrevNode->nextIdx = pPool->freeHeadIdx;
  if (pPool->freeHeadIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pPool->freeHeadIdx)->prevIdx = pNode->prevIdx;
  } else {
    pPool->freeTailIdx = pNode->prevIdx;
  }
  pPool->freeHeadIdx = pList->headIdx;
  pPool->size -= nele;

  // remove the nodes from the list
  pNode->prevIdx = TOBJPOOL_INVALID_IDX;
  pList->headIdx = idx;
  pList->neles -= nele;
}

void taosObjListPopHead(SObjList *pList) {
  if (pList == NULL || pList->headIdx == TOBJPOOL_INVALID_IDX) {
    return;
  }

  SObjPool     *pPool = pList->pPool;
  int64_t       idx = pList->headIdx;
  SObjPoolNode *pNode = TOBJPOOL_GET_NODE(pPool, idx);

  // remove the node from the list
  pList->headIdx = pNode->nextIdx;
  if (pList->headIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pList->headIdx)->prevIdx = TOBJPOOL_INVALID_IDX;
  } else {
    pList->tailIdx = TOBJPOOL_INVALID_IDX;
  }
  pList->neles -= 1;

  // add the node back to pool
  pNode->prevIdx = TOBJPOOL_INVALID_IDX;
  pNode->nextIdx = pPool->freeHeadIdx;
  if (pPool->freeHeadIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pPool->freeHeadIdx)->prevIdx = idx;
  } else {
    pPool->freeTailIdx = idx;
  }
  pPool->freeHeadIdx = idx;
  pPool->size -= 1;
}

void taosObjListPopHeadEx(SObjList *pList, FDelete fp) {
  if (pList == NULL || pList->headIdx == TOBJPOOL_INVALID_IDX) {
    return;
  }

  if (fp != NULL) {
    SObjPool     *pPool = pList->pPool;
    int64_t       idx = pList->headIdx;
    SObjPoolNode *pNode = TOBJPOOL_GET_NODE(pPool, idx);
    fp(TOBJPOOL_NODE_GET_OBJ(pNode));
  }

  taosObjListPopHead(pList);
}

void taosObjListPopTail(SObjList *pList) {
  if (pList == NULL || pList->tailIdx == TOBJPOOL_INVALID_IDX) {
    return;
  }

  SObjPool     *pPool = pList->pPool;
  int64_t       idx = pList->tailIdx;
  SObjPoolNode *pNode = TOBJPOOL_GET_NODE(pPool, idx);

  // remove the node from the list
  pList->tailIdx = pNode->prevIdx;
  if (pList->tailIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pList->tailIdx)->nextIdx = TOBJPOOL_INVALID_IDX;
  } else {
    pList->headIdx = TOBJPOOL_INVALID_IDX;
  }
  pList->neles -= 1;

  // add the node back to pool
  pNode->prevIdx = TOBJPOOL_INVALID_IDX;
  pNode->nextIdx = pPool->freeHeadIdx;
  if (pPool->freeHeadIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pPool->freeHeadIdx)->prevIdx = idx;
  } else {
    pPool->freeTailIdx = idx;
  }
  pPool->freeHeadIdx = idx;
  pPool->size -= 1;
}

void taosObjListPopTailEx(SObjList *pList, FDelete fp) {
  if (pList == NULL || pList->tailIdx == TOBJPOOL_INVALID_IDX) {
    return;
  }

  if (fp != NULL) {
    SObjPool     *pPool = pList->pPool;
    int64_t       idx = pList->tailIdx;
    SObjPoolNode *pNode = TOBJPOOL_GET_NODE(pPool, idx);
    fp(TOBJPOOL_NODE_GET_OBJ(pNode));
  }

  taosObjListPopTail(pList);
}

void taosObjListPopObj(SObjList *pList, void *pObj) {
  if (pList == NULL || pObj == NULL) {
    return;
  }

  SObjPool     *pPool = pList->pPool;
  SObjPoolNode *pNode = TOBJPOOL_OBJ_GET_NODE(pObj);
  int64_t       idx = TOBJPOOL_GET_IDX(pPool, pNode);
  if (idx < 0 || idx >= pPool->capacity) {
    return;
  }

  // remove the node from the list
  if (pNode->prevIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pNode->prevIdx)->nextIdx = pNode->nextIdx;
  } else {
    pList->headIdx = pNode->nextIdx;
  }
  if (pNode->nextIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pNode->nextIdx)->prevIdx = pNode->prevIdx;
  } else {
    pList->tailIdx = pNode->prevIdx;
  }
  pList->neles -= 1;

  // add the node back to pool
  pNode->prevIdx = TOBJPOOL_INVALID_IDX;
  pNode->nextIdx = pPool->freeHeadIdx;
  if (pPool->freeHeadIdx != TOBJPOOL_INVALID_IDX) {
    TOBJPOOL_GET_NODE(pPool, pPool->freeHeadIdx)->prevIdx = idx;
  } else {
    pPool->freeTailIdx = idx;
  }
  pPool->freeHeadIdx = idx;
  pPool->size -= 1;
}

void taosObjListPopObjEx(SObjList *pList, void *pObj, FDelete fp) {
  if (pList == NULL || pObj == NULL) {
    return;
  }

  if (fp != NULL) {
    fp(pObj);
  }

  taosObjListPopObj(pList, pObj);
}

void *taosObjListGetHead(SObjList *pList) {
  if (pList == NULL || pList->headIdx == TOBJPOOL_INVALID_IDX) {
    return NULL;
  }

  SObjPoolNode *pNode = TOBJPOOL_GET_NODE(pList->pPool, pList->headIdx);
  return TOBJPOOL_NODE_GET_OBJ(pNode);
}

void *taosObjListGetTail(SObjList *pList) {
  if (pList == NULL || pList->tailIdx == TOBJPOOL_INVALID_IDX) {
    return NULL;
  }

  SObjPoolNode *pNode = TOBJPOOL_GET_NODE(pList->pPool, pList->tailIdx);
  return TOBJPOOL_NODE_GET_OBJ(pNode);
}

void taosObjListInitIter(SObjList *pList, SObjListIter *pIter, EObjListIterDirection direction) {
  if (pIter == NULL) {
    return;
  }

  pIter->direction = direction;
  if (pList == NULL) {
    pIter->pPool = NULL;
    pIter->nextIdx = TOBJPOOL_INVALID_IDX;
  } else {
    pIter->pPool = pList->pPool;
    pIter->nextIdx = (direction == TOBJLIST_ITER_FORWARD) ? pList->headIdx : pList->tailIdx;
  }
}

void *taosObjListIterNext(SObjListIter *pIter) {
  if (pIter == NULL || pIter->nextIdx == TOBJPOOL_INVALID_IDX) {
    return NULL;
  }

  SObjPoolNode *pNode = TOBJPOOL_GET_NODE(pIter->pPool, pIter->nextIdx);
  void         *pObj = TOBJPOOL_NODE_GET_OBJ(pNode);
  pIter->nextIdx = (pIter->direction == TOBJLIST_ITER_FORWARD) ? pNode->nextIdx : pNode->prevIdx;

  return pObj;
}
