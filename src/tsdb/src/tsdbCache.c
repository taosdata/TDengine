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

#include "tsdb.h"
#include "tsdbMain.h"

static int  tsdbAllocBlockFromPool(STsdbCache *pCache);
static void tsdbFreeBlockList(SList *list);
static void tsdbFreeCacheMem(SCacheMem *mem);
static int  tsdbAddCacheBlockToPool(STsdbCache *pCache);

STsdbCache *tsdbInitCache(int cacheBlockSize, int totalBlocks, TsdbRepoT *pRepo) {
  STsdbCache *pCache = (STsdbCache *)calloc(1, sizeof(STsdbCache));
  if (pCache == NULL) return NULL;

  if (cacheBlockSize < 0) cacheBlockSize = TSDB_DEFAULT_CACHE_BLOCK_SIZE;
  cacheBlockSize *= (1024 * 1024);

  if (totalBlocks <= 1) totalBlocks = TSDB_DEFAULT_TOTAL_BLOCKS;

  pCache->cacheBlockSize = cacheBlockSize;
  pCache->totalCacheBlocks = totalBlocks;
  pCache->pRepo = pRepo;

  STsdbCachePool *pPool = &(pCache->pool);
  pPool->index = 0;
  pPool->memPool = tdListNew(sizeof(STsdbCacheBlock *));
  if (pPool->memPool == NULL) goto _err;

  for (int i = 0; i < totalBlocks; i++) {
    if (tsdbAddCacheBlockToPool(pCache) < 0) goto _err;
  }

  pCache->mem = NULL;
  pCache->imem = NULL;

  return pCache;

_err:
  tsdbFreeCache(pCache);
  return NULL;
}

void tsdbFreeCache(STsdbCache *pCache) {
  tsdbFreeCacheMem(pCache->imem);
  tsdbFreeCacheMem(pCache->mem);
  tsdbFreeBlockList(pCache->pool.memPool);
  free(pCache);
}

void *tsdbAllocFromCache(STsdbCache *pCache, int bytes, TSKEY key) {
  if (pCache == NULL) return NULL;
  if (bytes > pCache->cacheBlockSize) return NULL;

  if (pCache->curBlock == NULL || pCache->curBlock->remain < bytes) {
    if (pCache->curBlock !=NULL && listNEles(pCache->mem->list) >= pCache->totalCacheBlocks/2) {
      tsdbTriggerCommit(pCache->pRepo);
    }

    while (tsdbAllocBlockFromPool(pCache) < 0) {
      // TODO: deal with the error
      // printf("Failed to allocate from cache pool\n");
    }
  } 

  void *ptr = (void *)(pCache->curBlock->data + pCache->curBlock->offset);
  pCache->curBlock->offset += bytes;
  pCache->curBlock->remain -= bytes;
  memset(ptr, 0, bytes);
  if (key < pCache->mem->keyFirst) pCache->mem->keyFirst = key;
  if (key > pCache->mem->keyLast) pCache->mem->keyLast = key;
  pCache->mem->numOfRows++;

  return ptr;
}

static void tsdbFreeBlockList(SList *list) {
  SListNode *      node = NULL;
  STsdbCacheBlock *pBlock = NULL;
  while ((node = tdListPopHead(list)) != NULL) {
    tdListNodeGetData(list, node, (void *)(&pBlock));
    free(pBlock);
    listNodeFree(node);
  }
  tdListFree(list);
}

static void tsdbFreeCacheMem(SCacheMem *mem) {
  if (mem == NULL) return;
  SList *list = mem->list;
  tsdbFreeBlockList(list);
  free(mem);
}

static int tsdbAllocBlockFromPool(STsdbCache *pCache) {
  STsdbCachePool *pPool = &(pCache->pool);
  
  tsdbLockRepo(pCache->pRepo);
  if (listNEles(pPool->memPool) == 0) {
    tsdbUnLockRepo(pCache->pRepo);
    return -1;
  }

  SListNode *node = tdListPopHead(pPool->memPool);
  
  STsdbCacheBlock *pBlock = NULL;
  tdListNodeGetData(pPool->memPool, node, (void *)(&pBlock));
  pBlock->blockId = pPool->index++;
  pBlock->offset = 0;
  pBlock->remain = pCache->cacheBlockSize;

  if (pCache->mem == NULL) { // Create a new one
    pCache->mem = (SCacheMem *)malloc(sizeof(SCacheMem));
    if (pCache->mem == NULL) return -1;
    pCache->mem->keyFirst = INT64_MAX;
    pCache->mem->keyLast = 0;
    pCache->mem->numOfRows = 0;
    pCache->mem->list = tdListNew(sizeof(STsdbCacheBlock *));
  }

  tdListAppendNode(pCache->mem->list, node);
  pCache->curBlock = pBlock;

  tsdbUnLockRepo(pCache->pRepo);

  return 0;
}

int tsdbAlterCacheTotalBlocks(STsdbRepo *pRepo, int totalBlocks) {
  STsdbCache *pCache = pRepo->tsdbCache;
  int         oldNumOfBlocks = pCache->totalCacheBlocks;

  tsdbLockRepo((TsdbRepoT *)pRepo);

  ASSERT(pCache->totalCacheBlocks != totalBlocks);

  if (pCache->totalCacheBlocks < totalBlocks) {
    ASSERT(pCache->totalCacheBlocks == pCache->pool.numOfCacheBlocks);
    int blocksToAdd = pCache->totalCacheBlocks - totalBlocks;
    pCache->totalCacheBlocks = totalBlocks;
    for (int i = 0; i < blocksToAdd; i++) {
      if (tsdbAddCacheBlockToPool(pCache) < 0) {
        tsdbUnLockRepo((TsdbRepoT *)pRepo);
        tsdbError("tsdbId:%d, failed to add cache block to cache pool", pRepo->config.tsdbId);
        return -1;
      }
    }
  } else {
    pCache->totalCacheBlocks = totalBlocks;
    tsdbAdjustCacheBlocks(pCache);
  }
  pRepo->config.totalBlocks = totalBlocks;

  tsdbUnLockRepo((TsdbRepoT *)pRepo);
  tsdbTrace("vgId:%d, tsdb total cache blocks changed from %d to %d", pRepo->config.tsdbId, oldNumOfBlocks, totalBlocks);
  return 0;
}

static int tsdbAddCacheBlockToPool(STsdbCache *pCache) {
  STsdbCachePool *pPool = &pCache->pool;

  STsdbCacheBlock *pBlock = malloc(sizeof(STsdbCacheBlock) + pCache->cacheBlockSize);
  if (pBlock == NULL) return -1;

  pBlock->offset = 0;
  pBlock->remain = pCache->cacheBlockSize;
  tdListAppend(pPool->memPool, (void *)(&pBlock));
  pPool->numOfCacheBlocks++;

  return 0;
}

static int tsdbRemoveCacheBlockFromPool(STsdbCache *pCache) {
  STsdbCachePool *pPool = &pCache->pool;
  STsdbCacheBlock *pBlock = NULL;

  ASSERT(pCache->totalCacheBlocks >= 0);

  SListNode *node = tdListPopHead(pPool->memPool);
  if (node == NULL) return -1;

  tdListNodeGetData(pPool->memPool, node, &pBlock);
  free(pBlock);
  listNodeFree(node);
  pPool->numOfCacheBlocks--;

  return 0;
}

void tsdbAdjustCacheBlocks(STsdbCache *pCache) {
  while (pCache->totalCacheBlocks < pCache->pool.numOfCacheBlocks) {
    if (tsdbRemoveCacheBlockFromPool(pCache) < 0) break;
  }
}