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

#include "vnodeMemAllocator.h"

#define VMA_IS_FULL(pvma) \
  (((pvma)->inuse != &((pvma)->node)) || ((pvma)->inuse->tsize - (pvma)->inuse->used < (pvma)->threshold))

static SVMANode *VMANodeNew(size_t size);
static void      VMANodeFree(SVMANode *node);

SVnodeMemAllocator *VMACreate(size_t size, size_t ssize, size_t threshold) {
  SVnodeMemAllocator *pvma = NULL;

  if (size < threshold) {
    return NULL;
  }

  pvma = (SVnodeMemAllocator *)malloc(sizeof(*pvma) + size);
  if (pvma) {
    pvma->full = false;
    pvma->threshold = threshold;
    pvma->ssize = ssize;
    pvma->inuse = &(pvma->node);

    pvma->inuse->prev = NULL;
    pvma->inuse->tsize = size;
    pvma->inuse->used = 0;
  }

  return pvma;
}

void VMADestroy(SVnodeMemAllocator *pvma) {
  if (pvma) {
    VMAReset(pvma);
    free(pvma);
  }
}

void VMAReset(SVnodeMemAllocator *pvma) {
  while (pvma->inuse != &(pvma->node)) {
    SVMANode *node = pvma->inuse;
    pvma->inuse = node->prev;
    VMANodeFree(node);
  }

  pvma->inuse->used = 0;
  pvma->full = false;
}

void *VMAMalloc(SVnodeMemAllocator *pvma, size_t size) {
  void * ptr = NULL;
  size_t tsize = size + sizeof(size_t);

  if (pvma->inuse->tsize - pvma->inuse->used < tsize) {
    SVMANode *pNode = VMANodeNew(MAX(pvma->ssize, tsize));
    if (pNode == NULL) {
      return NULL;
    }

    pNode->prev = pvma->inuse;
    pvma->inuse = pNode;
  }

  ptr = pvma->inuse->data + pvma->inuse->used;
  pvma->inuse->used += tsize;
  *(size_t *)ptr = size;
  ptr = POINTER_SHIFT(ptr, sizeof(size_t));

  pvma->full = VMA_IS_FULL(pvma);

  return ptr;
}

void VMAFree(SVnodeMemAllocator *pvma, void *ptr) {
  if (ptr) {
    size_t size = *(size_t *)POINTER_SHIFT(ptr, -sizeof(size_t));
    if (POINTER_SHIFT(ptr, size) == pvma->inuse->data + pvma->inuse->used) {
      pvma->inuse->used -= (size + sizeof(size_t));

      if ((pvma->inuse->used == 0) && (pvma->inuse != &(pvma->node))) {
        SVMANode *node = pvma->inuse;
        pvma->inuse = node->prev;
        VMANodeFree(node);
      }

      pvma->full = VMA_IS_FULL(pvma);
    }
  }
}

bool VMAIsFull(SVnodeMemAllocator *pvma) { return pvma->full; }

static SVMANode *VMANodeNew(size_t size) {
  SVMANode *node = NULL;

  node = (SVMANode *)malloc(sizeof(*node) + size);
  if (node) {
    node->prev = NULL;
    node->tsize = size;
    node->used = 0;
  }

  return node;
}

static void VMANodeFree(SVMANode *node) {
  if (node) {
    free(node);
  }
}