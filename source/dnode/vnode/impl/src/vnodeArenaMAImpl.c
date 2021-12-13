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
    // TODO: handle error
    return NULL;
  }

  pVMA->capacity = capacity;
  pVMA->ssize = ssize;
  pVMA->lsize = lsize;
  tlistInit(&(pVMA->nlist));

  SVArenaNode *pNode = vArenaNodeNew(capacity);
  if (pNode == NULL) {
    // TODO
    return NULL;
  }

  tlistAppend(&(pVMA->nlist), pNode);

  return pVMA;
}

void vmaDestroy(SVMemAllocator *pVMA) {
  // TODO
}

void vmaReset(SVMemAllocator *pVMA) {
  // TODO
}

void *vmaMalloc(SVMemAllocator *pVMA, uint64_t size) {
  // TODO
  return NULL;
}

void vmaFree(SVMemAllocator *pVMA, void *ptr) {
  // TODO
}

/* ------------------------ STATIC METHODS ------------------------ */
static SVArenaNode *vArenaNodeNew(uint64_t capacity) {
  SVArenaNode *pNode = NULL;
  // TODO
  return pNode;
}

static void vArenaNodeFree(SVArenaNode *pNode) {
  // TODO
}