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

#include "tsdbint.h"
#include "tsdbHealth.h"

#define POOL_IS_EMPTY(b) (listNEles((b)->bufBlockList) == 0)


// ---------------- INTERNAL FUNCTIONS ----------------
STsdbBufPool *tsdbNewBufPool() {
  STsdbBufPool *pBufPool = (STsdbBufPool *)calloc(1, sizeof(*pBufPool));
  if (pBufPool == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  int code = pthread_cond_init(&(pBufPool->poolNotEmpty), NULL);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  pBufPool->bufBlockList = tdListNew(sizeof(STsdbBufBlock *));
  if (pBufPool->bufBlockList == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  return pBufPool;

_err:
  tsdbFreeBufPool(pBufPool);
  return NULL;
}

void tsdbFreeBufPool(STsdbBufPool *pBufPool) {
  if (pBufPool) {
    if (pBufPool->bufBlockList) {
      ASSERT(listNEles(pBufPool->bufBlockList) == 0);
      tdListFree(pBufPool->bufBlockList);
    }

    pthread_cond_destroy(&pBufPool->poolNotEmpty);

    free(pBufPool);
  }
}

int tsdbOpenBufPool(STsdbRepo *pRepo) {
  STsdbCfg *    pCfg = &(pRepo->config);
  STsdbBufPool *pPool = pRepo->pPool;

  ASSERT(pPool != NULL);

  pPool->bufBlockSize = pCfg->cacheBlockSize * 1024 * 1024; // MB
  pPool->tBufBlocks = pCfg->totalBlocks;
  pPool->nBufBlocks = 0;
  pPool->nElasticBlocks = 0;
  pPool->index = 0;
  pPool->nRecycleBlocks = 0;

  for (int i = 0; i < pCfg->totalBlocks; i++) {
    STsdbBufBlock *pBufBlock = tsdbNewBufBlock(pPool->bufBlockSize);
    if (pBufBlock == NULL) goto _err;

    if (tdListAppend(pPool->bufBlockList, (void *)(&pBufBlock)) < 0) {
      tsdbFreeBufBlock(pBufBlock);
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }

    pPool->nBufBlocks++;
  }

  tsdbDebug("vgId:%d buffer pool is opened! bufBlockSize:%d tBufBlocks:%d nBufBlocks:%d", REPO_ID(pRepo),
            pPool->bufBlockSize, pPool->tBufBlocks, pPool->nBufBlocks);

  return 0;

_err:
  tsdbCloseBufPool(pRepo);
  return -1;
}

void tsdbCloseBufPool(STsdbRepo *pRepo) {
  if (pRepo == NULL) return;

  STsdbBufPool * pBufPool = pRepo->pPool;
  STsdbBufBlock *pBufBlock = NULL;

  if (pBufPool) {
    SListNode *pNode = NULL;
    while ((pNode = tdListPopHead(pBufPool->bufBlockList)) != NULL) {
      tdListNodeGetData(pBufPool->bufBlockList, pNode, (void *)(&pBufBlock));
      tsdbFreeBufBlock(pBufBlock);
      free(pNode);
    }
  }

  tsdbDebug("vgId:%d, buffer pool is closed", REPO_ID(pRepo));
}

SListNode *tsdbAllocBufBlockFromPool(STsdbRepo *pRepo) {
  ASSERT(pRepo != NULL && pRepo->pPool != NULL);
  ASSERT(IS_REPO_LOCKED(pRepo));

  STsdbBufPool *pBufPool = pRepo->pPool;

  while (POOL_IS_EMPTY(pBufPool)) {
   if(tsDeadLockKillQuery) {
      // supply new Block 
      if(tsdbInsertNewBlock(pRepo) > 0) {
        tsdbWarn("vgId:%d add new elastic block . elasticBlocks=%d cur free Blocks=%d", REPO_ID(pRepo), pBufPool->nElasticBlocks, pBufPool->bufBlockList->numOfEles);
        break;
      } else {
        // no newBlock, kill query free
        if(!tsdbUrgeQueryFree(pRepo))
          tsdbWarn("vgId:%d Urge query free thread start failed.", REPO_ID(pRepo));
      }
    }

    pRepo->repoLocked = false;
    pthread_cond_wait(&(pBufPool->poolNotEmpty), &(pRepo->mutex));
    pRepo->repoLocked = true;
  }

  SListNode *    pNode = tdListPopHead(pBufPool->bufBlockList);
  ASSERT(pNode != NULL);
  STsdbBufBlock *pBufBlock = NULL;
  tdListNodeGetData(pBufPool->bufBlockList, pNode, (void *)(&pBufBlock));

  pBufBlock->blockId = pBufPool->index++;
  pBufBlock->offset = 0;
  pBufBlock->remain = pBufPool->bufBlockSize;

  tsdbDebug("vgId:%d, buffer block is allocated, blockId:%" PRId64, REPO_ID(pRepo), pBufBlock->blockId);
  return pNode;
}

// ---------------- LOCAL FUNCTIONS ----------------
STsdbBufBlock *tsdbNewBufBlock(int bufBlockSize) {
  STsdbBufBlock *pBufBlock = (STsdbBufBlock *)malloc(sizeof(*pBufBlock) + bufBlockSize);
  if (pBufBlock == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  pBufBlock->blockId = 0;
  pBufBlock->offset = 0;
  pBufBlock->remain = bufBlockSize;

  return pBufBlock;
}

void tsdbFreeBufBlock(STsdbBufBlock *pBufBlock) { tfree(pBufBlock); }

int tsdbExpandPool(STsdbRepo* pRepo, int32_t oldTotalBlocks) {
  if (oldTotalBlocks == pRepo->config.totalBlocks) {
    return TSDB_CODE_SUCCESS;
  }

  int err = TSDB_CODE_SUCCESS;

  if (tsdbLockRepo(pRepo) < 0) return terrno;
  STsdbBufPool* pPool = pRepo->pPool;

  if (pRepo->config.totalBlocks > oldTotalBlocks) {
    for (int i = 0; i < pRepo->config.totalBlocks - oldTotalBlocks; i++) {
      STsdbBufBlock *pBufBlock = tsdbNewBufBlock(pPool->bufBlockSize);
      if (pBufBlock == NULL) goto err;

      if (tdListAppend(pPool->bufBlockList, (void *)(&pBufBlock)) < 0) {
        tsdbFreeBufBlock(pBufBlock);
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        err = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto err;
      }

      pPool->nBufBlocks++;      
    }
    pthread_cond_signal(&pPool->poolNotEmpty);
  } else {
    pPool->nRecycleBlocks = oldTotalBlocks - pRepo->config.totalBlocks;
  } 

err:
  tsdbUnlockRepo(pRepo);
  return err;
}

void tsdbRecycleBufferBlock(STsdbBufPool* pPool, SListNode *pNode, bool bELastic) {
  STsdbBufBlock *pBufBlock = NULL;
  tdListNodeGetData(pPool->bufBlockList, pNode, (void *)(&pBufBlock));
  tsdbFreeBufBlock(pBufBlock);
  free(pNode);
  if(bELastic) {
    pPool->nElasticBlocks--;
    tsdbWarn("pPool=%p elastic block reduce one . nElasticBlocks=%d cur free Blocks=%d", pPool, pPool->nElasticBlocks, pPool->bufBlockList->numOfEles);
  }
  else
    pPool->nBufBlocks--;
}
