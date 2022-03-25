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

static SVArenaNode *vArenaNodeNew(uint64_t capacity);
static void         vArenaNodeFree(SVArenaNode *pNode);

SVMemAllocator *vmaCreate(uint64_t capacity, uint64_t ssize, uint64_t lsize) {
  SVMemAllocator *pVMA = (SVMemAllocator *)taosMemoryMalloc(sizeof(*pVMA));
  if (pVMA == NULL) {
    return NULL;
  }

  pVMA->capacity = capacity;
  pVMA->ssize = ssize;
  pVMA->lsize = lsize;
  TD_SLIST_INIT(&(pVMA->nlist));

  pVMA->pNode = vArenaNodeNew(capacity);
  if (pVMA->pNode == NULL) {
    taosMemoryFree(pVMA);
    return NULL;
  }

  TD_SLIST_PUSH(&(pVMA->nlist), pVMA->pNode);

  return pVMA;
}

void vmaDestroy(SVMemAllocator *pVMA) {
  if (pVMA) {
    while (TD_SLIST_NELES(&(pVMA->nlist)) > 1) {
      SVArenaNode *pNode = TD_SLIST_HEAD(&(pVMA->nlist));
      TD_SLIST_POP(&(pVMA->nlist));
      vArenaNodeFree(pNode);
    }

    taosMemoryFree(pVMA);
  }
}

void vmaReset(SVMemAllocator *pVMA) {
  while (TD_SLIST_NELES(&(pVMA->nlist)) > 1) {
    SVArenaNode *pNode = TD_SLIST_HEAD(&(pVMA->nlist));
    TD_SLIST_POP(&(pVMA->nlist));
    vArenaNodeFree(pNode);
  }

  SVArenaNode *pNode = TD_SLIST_HEAD(&(pVMA->nlist));
  pNode->ptr = pNode->data;
}

void *vmaMalloc(SVMemAllocator *pVMA, uint64_t size) {
  SVArenaNode *pNode = TD_SLIST_HEAD(&(pVMA->nlist));
  void *       ptr;

  if (pNode->size < POINTER_DISTANCE(pNode->ptr, pNode->data) + size) {
    uint64_t capacity = TMAX(pVMA->ssize, size);
    pNode = vArenaNodeNew(capacity);
    if (pNode == NULL) {
      // TODO: handle error
      return NULL;
    }

    TD_SLIST_PUSH(&(pVMA->nlist), pNode);
  }

  ptr = pNode->ptr;
  pNode->ptr = POINTER_SHIFT(ptr, size);

  return ptr;
}

void vmaFree(SVMemAllocator *pVMA, void *ptr) {
  // TODO
}

bool vmaIsFull(SVMemAllocator *pVMA) {
  SVArenaNode *pNode = TD_SLIST_HEAD(&(pVMA->nlist));

  return (TD_SLIST_NELES(&(pVMA->nlist)) > 1) ||
         (pNode->size < POINTER_DISTANCE(pNode->ptr, pNode->data) + pVMA->lsize);
}

/* ------------------------ STATIC METHODS ------------------------ */
static SVArenaNode *vArenaNodeNew(uint64_t capacity) {
  SVArenaNode *pNode = NULL;

  pNode = (SVArenaNode *)taosMemoryMalloc(sizeof(*pNode) + capacity);
  if (pNode == NULL) {
    return NULL;
  }

  pNode->size = capacity;
  pNode->ptr = pNode->data;

  return pNode;
}

static void vArenaNodeFree(SVArenaNode *pNode) {
  if (pNode) {
    taosMemoryFree(pNode);
  }
}
