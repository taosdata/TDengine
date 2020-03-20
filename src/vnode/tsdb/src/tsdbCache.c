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
#include <stdlib.h>

#include "tsdbCache.h"

static int tsdbAllocBlockFromPool(STsdbCache *pCache);
static void tsdbFreeBlockList(SList *list);

STsdbCache *tsdbInitCache(int maxBytes, int cacheBlockSize) {
  STsdbCache *pCache = (STsdbCache *)calloc(1, sizeof(STsdbCache));
  if (pCache == NULL) return NULL;

  pCache->maxBytes = maxBytes;
  pCache->cacheBlockSize = cacheBlockSize;

  int nBlocks = maxBytes / cacheBlockSize + 1;
  if (nBlocks <= 1) nBlocks = 2;

  STsdbCachePool *pPool = &(pCache->pool);
  pPool->index = 0;
  pPool->memPool = tdListNew(sizeof(STsdbCacheBlock *));
  if (pPool->memPool == NULL) goto _err;

  for (int i = 0; i < nBlocks; i++) {
    STsdbCacheBlock *pBlock = (STsdbCacheBlock *)malloc(sizeof(STsdbCacheBlock) + cacheBlockSize);
    if (pBlock == NULL) {
      goto _err;
    }
    pBlock->offset = 0;
    pBlock->remain = cacheBlockSize;
    tdListAppend(pPool->memPool, (void *)(&pBlock));
  }

  pCache->mem = tdListNew(sizeof(STsdbCacheBlock *));
  if (pCache->mem == NULL) goto _err;

  pCache->imem = tdListNew(sizeof(STsdbCacheBlock *));
  if (pCache->imem == NULL) goto _err;

  return pCache;

_err:
  tsdbFreeCache(pCache);
  return NULL;
}

void tsdbFreeCache(STsdbCache *pCache) {
  tsdbFreeBlockList(pCache->imem);
  tsdbFreeBlockList(pCache->mem);
  tsdbFreeBlockList(pCache->pool.memPool);
  free(pCache);
}

void *tsdbAllocFromCache(STsdbCache *pCache, int bytes) {
  if (pCache == NULL) return NULL;
  if (bytes > pCache->cacheBlockSize) return NULL;

  if (isListEmpty(pCache->imem)) {
    if (tsdbAllocBlockFromPool(pCache) < 0) {
      // TODO: deal with the error
    }
  }

  if (pCache->curBlock->remain < bytes) {
    if (tsdbAllocBlockFromPool(pCache) < 0) {
      // TODO: deal with the error
    }
  } 

  void *ptr = (void *)(pCache->curBlock->data + pCache->curBlock->offset);
  pCache->curBlock->offset += bytes;
  pCache->curBlock->remain -= bytes;

  return ptr;
}

static void tsdbFreeBlockList(SList *list) {
  if (list == NULL) return;
  SListNode *      node = NULL;
  STsdbCacheBlock *pBlock = NULL;
  while ((node = tdListPopHead(list)) != NULL) {
    tdListNodeGetData(list, node, (void *)(&pBlock));
    free(pBlock);
    listNodeFree(node);
  }
  tdListFree(list);
}

static int tsdbAllocBlockFromPool(STsdbCache *pCache) {
  STsdbCachePool *pPool = &(pCache->pool);
  if (listNEles(pPool->memPool) == 0) return -1;

  SListNode *node = tdListPopHead(pPool->memPool);
  
  STsdbCacheBlock *pBlock = NULL;
  tdListNodeGetData(pPool->memPool, node, (void *)(&pBlock));
  pBlock->blockId = pPool->index++;
  pBlock->offset = 0;
  pBlock->remain = pCache->cacheBlockSize;

  tdListAppendNode(pPool->memPool, node);
  pCache->curBlock = pBlock;

  return 0;
}