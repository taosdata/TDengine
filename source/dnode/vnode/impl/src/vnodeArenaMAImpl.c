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

static SVArenaNode *vArenaNodeNew(uint64_t capacity);
static void         vArenaNodeFree(SVArenaNode *pNode);

SVMemAllocator *vmaCreate(uint64_t capacity, uint64_t ssize, uint64_t lsize) {
  SVMemAllocator *pVMA = (SVMemAllocator *)malloc(sizeof(*pVMA));
  if (pVMA == NULL) {
    return NULL;
  }

  pVMA->capacity = capacity;
  pVMA->ssize = ssize;
  pVMA->lsize = lsize;
  tlistInit(&(pVMA->nlist));

  SVArenaNode *pNode = vArenaNodeNew(capacity);
  if (pNode == NULL) {
    free(pVMA);
    return NULL;
  }

  tlistAppend(&(pVMA->nlist), pNode);

  return pVMA;
}

void vmaDestroy(SVMemAllocator *pVMA) {
  if (pVMA) {
    while (true) {
      SVArenaNode *pNode = tlistTail(&(pVMA->nlist));

      if (pNode) {
        tlistPop(&(pVMA->nlist), pNode);
        vArenaNodeFree(pNode);
      } else {
        break;
      }
    }

    free(pVMA);
  }
}

void vmaReset(SVMemAllocator *pVMA) {
  while (tlistNEles(&(pVMA->nlist)) > 1) {
    SVArenaNode *pNode = tlistTail(&(pVMA->nlist));
    tlistPop(&(pVMA->nlist), pNode);
    vArenaNodeFree(pNode);
  }

  SVArenaNode *pNode = tlistHead(&(pVMA->nlist));
  pNode->ptr = pNode->data;
}

void *vmaMalloc(SVMemAllocator *pVMA, uint64_t size) {
  SVArenaNode *pNode = tlistTail(&(pVMA->nlist));
  void *       ptr;

  if (pNode->size < POINTER_DISTANCE(pNode->ptr, pNode->data) + size) {
    uint64_t capacity = MAX(pVMA->ssize, size);
    pNode = vArenaNodeNew(capacity);
    if (pNode == NULL) {
      // TODO: handle error
      return NULL;
    }

    tlistAppend(&(pVMA->nlist), pNode);
  }

  ptr = pNode->ptr;
  pNode->ptr = POINTER_SHIFT(ptr, size);

  return ptr;
}

void vmaFree(SVMemAllocator *pVMA, void *ptr) {
  // TODO
}

bool vmaIsFull(SVMemAllocator *pVMA) {
  SVArenaNode *pNode = tlistTail(&(pVMA->nlist));

  return (tlistNEles(&(pVMA->nlist)) > 1) || (pNode->size < POINTER_DISTANCE(pNode->ptr, pNode->data) + pVMA->lsize);
}

/* ------------------------ STATIC METHODS ------------------------ */
static SVArenaNode *vArenaNodeNew(uint64_t capacity) {
  SVArenaNode *pNode = NULL;

  pNode = (SVArenaNode *)malloc(sizeof(*pNode) + capacity);
  if (pNode == NULL) {
    return NULL;
  }

  pNode->size = capacity;
  pNode->ptr = pNode->data;

  return pNode;
}

static void vArenaNodeFree(SVArenaNode *pNode) {
  if (pNode) {
    free(pNode);
  }
}