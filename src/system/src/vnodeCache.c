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

#include <arpa/inet.h>
#include <sys/time.h>
#include <unistd.h>

#include "taosmsg.h"
#include "vnode.h"
#include "vnodeCache.h"
#include "vnodeUtil.h"

void vnodeSearchPointInCache(SMeterObj *pObj, SQuery *pQuery);
void vnodeProcessCommitTimer(void *param, void *tmrId);

void *vnodeOpenCachePool(int vnode) {
  SCachePool *pCachePool;
  SVnodeCfg * pCfg = &vnodeList[vnode].cfg;
  int         blockId = 0;
  char *      pMem = NULL;

  pCachePool = (SCachePool *)malloc(sizeof(SCachePool));
  if (pCachePool == NULL) {
    dError("no memory to allocate cache pool!");
    return NULL;
  }

  memset(pCachePool, 0, sizeof(SCachePool));
  pCachePool->count = 1;
  pCachePool->vnode = vnode;

  pthread_mutex_init(&(pCachePool->vmutex), NULL);

  size_t size = sizeof(char *) * pCfg->cacheNumOfBlocks.totalBlocks;
  pCachePool->pMem = malloc(size);
  if (pCachePool->pMem == NULL) {
    dError("no memory to allocate cache blocks!");
    pthread_mutex_destroy(&(pCachePool->vmutex));
    tfree(pCachePool);
    return NULL;
  }

  memset(pCachePool->pMem, 0, size);
  pCachePool->threshold = pCfg->cacheNumOfBlocks.totalBlocks * 0.6;

  int maxAllocBlock = (1024 * 1024 * 1024) / pCfg->cacheBlockSize;
  if (maxAllocBlock < 1) {
    dError("Cache block size is too large");
    pthread_mutex_destroy(&(pCachePool->vmutex));
    tfree(pCachePool->pMem);
    tfree(pCachePool);
    return NULL;
  }
  while (blockId < pCfg->cacheNumOfBlocks.totalBlocks) {
    // TODO : Allocate real blocks
    int allocBlocks = MIN(pCfg->cacheNumOfBlocks.totalBlocks - blockId, maxAllocBlock);
    pMem = calloc(allocBlocks, pCfg->cacheBlockSize);
    if (pMem == NULL) {
      dError("failed to allocate cache memory");
      goto _err_exit;
    }

    for (int i = 0; i < allocBlocks; i++) {
      pCachePool->pMem[blockId] = pMem + i * pCfg->cacheBlockSize;
      blockId++;
    }
  }

  dTrace("vid:%d, cache pool is allocated:0x%x", vnode, pCachePool);

  return pCachePool;

_err_exit:
  pthread_mutex_destroy(&(pCachePool->vmutex));
  // TODO : Free the cache blocks and return
  blockId = 0;
  while (blockId < pCfg->cacheNumOfBlocks.totalBlocks) {
    tfree(pCachePool->pMem[blockId]);
    blockId = blockId + (MIN(maxAllocBlock, pCfg->cacheNumOfBlocks.totalBlocks - blockId));
  }
  tfree(pCachePool->pMem);
  tfree(pCachePool);
  return NULL;
}

void vnodeCloseCachePool(int vnode) {
  SVnodeObj * pVnode = vnodeList + vnode;
  SCachePool *pCachePool = (SCachePool *)pVnode->pCachePool;
  int         blockId = 0;

  taosTmrStopA(&pVnode->commitTimer);
  if (pVnode->commitInProcess) pthread_cancel(pVnode->commitThread);

  dTrace("vid:%d, cache pool closed, count:%d", vnode, pCachePool->count);

  int maxAllocBlock = (1024 * 1024 * 1024) / pVnode->cfg.cacheBlockSize;
  while (blockId < pVnode->cfg.cacheNumOfBlocks.totalBlocks) {
    tfree(pCachePool->pMem[blockId]);
    blockId = blockId + (MIN(maxAllocBlock, pVnode->cfg.cacheNumOfBlocks.totalBlocks - blockId));
  }
  tfree(pCachePool->pMem);
  pthread_mutex_destroy(&(pCachePool->vmutex));
  tfree(pCachePool);
  pVnode->pCachePool = NULL;
}

void *vnodeAllocateCacheInfo(SMeterObj *pObj) {
  SCacheInfo *pInfo;
  size_t      size;
  SVnodeCfg * pCfg = &vnodeList[pObj->vnode].cfg;

  size = sizeof(SCacheInfo);
  pInfo = (SCacheInfo *)malloc(size);
  if (pInfo == NULL) {
    dError("id:%s, no memory for cacheInfo", pObj->meterId);
    return NULL;
  }
  memset(pInfo, 0, size);
  pInfo->maxBlocks = vnodeList[pObj->vnode].cfg.blocksPerMeter;
  size = sizeof(SCacheBlock *) * pInfo->maxBlocks;
  pInfo->cacheBlocks = (SCacheBlock **)malloc(size);
  if (pInfo->cacheBlocks == NULL) {
    dError("id:%s, no memory for cacheBlocks", pObj->meterId);
    return NULL;
  }
  memset(pInfo->cacheBlocks, 0, size);
  pInfo->currentSlot = -1;

  pObj->pointsPerBlock =
      (pCfg->cacheBlockSize - sizeof(SCacheBlock) - pObj->numOfColumns * sizeof(char *)) / pObj->bytesPerPoint;
  if (pObj->pointsPerBlock > pObj->pointsPerFileBlock) pObj->pointsPerBlock = pObj->pointsPerFileBlock;
  pObj->pCache = (void *)pInfo;

  pObj->freePoints = pObj->pointsPerBlock * pInfo->maxBlocks;

  return (void *)pInfo;
}

int vnodeFreeCacheBlock(SCacheBlock *pCacheBlock) {
  SMeterObj * pObj;
  SCacheInfo *pInfo;

  if (pCacheBlock == NULL) return -1;

  pObj = pCacheBlock->pMeterObj;
  pInfo = (SCacheInfo *)pObj->pCache;

  if (pObj) {
    pInfo->numOfBlocks--;

    if (pInfo->numOfBlocks < 0) {
      dError("vid:%d sid:%d id:%s, numOfBlocks:%d shall never be negative", pObj->vnode, pObj->sid, pObj->meterId,
           pInfo->numOfBlocks);
    }

    if (pCacheBlock->blockId == 0) {
      dError("vid:%d sid:%d id:%s, double free", pObj->vnode, pObj->sid, pObj->meterId);
    }

    SCachePool *pPool = (SCachePool *)vnodeList[pObj->vnode].pCachePool;
    if (pCacheBlock->notFree) {
      pPool->notFreeSlots--;
      dTrace("vid:%d sid:%d id:%s, cache block is not free, slot:%d, index:%d notFreeSlots:%d",
             pObj->vnode, pObj->sid, pObj->meterId, pCacheBlock->slot, pCacheBlock->index, pPool->notFreeSlots);
    }

    dTrace("vid:%d sid:%d id:%s, free a cache block, numOfBlocks:%d, slot:%d, index:%d notFreeSlots:%d",
           pObj->vnode, pObj->sid, pObj->meterId, pInfo->numOfBlocks, pCacheBlock->slot, pCacheBlock->index,
           pPool->notFreeSlots);

    memset(pCacheBlock, 0, sizeof(SCacheBlock));

  } else {
    dError("BUG, pObj is null");
  }

  return 0;
}

void vnodeFreeCacheInfo(SMeterObj *pObj) {
  SCacheInfo * pInfo;
  SCacheBlock *pCacheBlock;
  SCachePool * pPool;
  int          slot, numOfBlocks;

  if (pObj == NULL || pObj->pCache == NULL) return;

  pPool = (SCachePool *)vnodeList[pObj->vnode].pCachePool;
  pInfo = (SCacheInfo *)pObj->pCache;
  if (pPool == NULL || pInfo == NULL) return;

  pthread_mutex_lock(&pPool->vmutex);
  numOfBlocks = pInfo->numOfBlocks;
  slot = pInfo->currentSlot;

  for (int i = 0; i < numOfBlocks; ++i) {
    pCacheBlock = pInfo->cacheBlocks[slot];
    vnodeFreeCacheBlock(pCacheBlock);
    slot = (slot - 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
  }

  pObj->pCache = NULL;
  tfree(pInfo->cacheBlocks);
  tfree(pInfo);
  pthread_mutex_unlock(&pPool->vmutex);
}

uint64_t vnodeGetPoolCount(SVnodeObj *pVnode) {
  SCachePool *pPool;

  pPool = (SCachePool *)pVnode->pCachePool;

  return pPool->count;
}

void vnodeUpdateCommitInfo(SMeterObj *pObj, int slot, int pos, uint64_t count) {
  SCacheInfo * pInfo;
  SCacheBlock *pBlock;
  SCachePool * pPool;

  pInfo = (SCacheInfo *)pObj->pCache;
  pPool = (SCachePool *)vnodeList[pObj->vnode].pCachePool;

  int tslot =
      (pInfo->commitPoint == pObj->pointsPerBlock) ? (pInfo->commitSlot + 1) % pInfo->maxBlocks : pInfo->commitSlot;
  int slots = 0;

  while (tslot != slot || ((tslot == slot) && (pos == pObj->pointsPerBlock))) {
    slots++;
    pthread_mutex_lock(&pPool->vmutex);
    pBlock = pInfo->cacheBlocks[tslot];
    assert(pBlock->notFree);
    pBlock->notFree = 0;
    pInfo->unCommittedBlocks--;
    pPool->notFreeSlots--;
    pthread_mutex_unlock(&pPool->vmutex);

    dTrace("vid:%d sid:%d id:%s, cache block is committed, slot:%d, index:%d notFreeSlots:%d, unCommittedBlocks:%d",
           pObj->vnode, pObj->sid, pObj->meterId, pBlock->slot, pBlock->index, pPool->notFreeSlots,
           pInfo->unCommittedBlocks);
    if (tslot == slot) break;
    tslot = (tslot + 1) % pInfo->maxBlocks;
  }

  __sync_fetch_and_add(&pObj->freePoints, pObj->pointsPerBlock * slots);
  pInfo->commitSlot = slot;
  pInfo->commitPoint = pos;
  pObj->commitCount = count;
}

TSKEY vnodeGetFirstKey(int vnode) {
  SMeterObj *  pObj;
  SCacheInfo * pInfo;
  SCacheBlock *pCacheBlock;

  SVnodeCfg *pCfg = &vnodeList[vnode].cfg;
  TSKEY      key = taosGetTimestamp(pCfg->precision);

  for (int sid = 0; sid < pCfg->maxSessions; ++sid) {
    pObj = vnodeList[vnode].meterList[sid];
    if (pObj == NULL || pObj->pCache == NULL) continue;

    pInfo = (SCacheInfo *)pObj->pCache;
    pCacheBlock = pInfo->cacheBlocks[0];

    if (pCacheBlock == NULL || pCacheBlock->numOfPoints <= 0) continue;

    if (*((TSKEY *)(pCacheBlock->offset[0])) < key) key = *((TSKEY *)(pCacheBlock->offset[0]));
  }

  return key;
}

pthread_t vnodeCreateCommitThread(SVnodeObj *pVnode) {
  // this function has to mutex locked before it is called

  pthread_attr_t thattr;
  SCachePool *   pPool = (SCachePool *)pVnode->pCachePool;

  if (pPool->commitInProcess) {
    dTrace("vid:%d, commit is already in process", pVnode->vnode);
    return pVnode->commitThread;
  }

  taosTmrStopA(&pVnode->commitTimer);

  if (pVnode->status == TSDB_STATUS_UNSYNCED) {
    taosTmrReset(vnodeProcessCommitTimer, pVnode->cfg.commitTime * 1000, pVnode, vnodeTmrCtrl, &pVnode->commitTimer);
    dTrace("vid:%d, it is in unsyc state, commit later", pVnode->vnode);
    return pVnode->commitThread;
  }

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  if (pthread_create(&(pVnode->commitThread), &thattr, vnodeCommitToFile, pVnode) != 0) {
    dError("vid:%d, failed to create thread to commit file, reason:%s", pVnode->vnode, strerror(errno));
  } else {
    pPool->commitInProcess = 1;
    dTrace("vid:%d, commit thread: 0x%lx is created", pVnode->vnode, pVnode->commitThread);
  }

  pthread_attr_destroy(&thattr);

  return pVnode->commitThread;
}

void vnodeProcessCommitTimer(void *param, void *tmrId) {
  SVnodeObj * pVnode = (SVnodeObj *)param;
  SCachePool *pPool = (SCachePool *)pVnode->pCachePool;

  pthread_mutex_lock(&pPool->vmutex);

  vnodeCreateCommitThread(pVnode);

  pthread_mutex_unlock(&pPool->vmutex);
}

void vnodeCommitOver(SVnodeObj *pVnode) {
  SCachePool *pPool = (SCachePool *)(pVnode->pCachePool);

  taosTmrReset(vnodeProcessCommitTimer, pVnode->cfg.commitTime * 1000, pVnode, vnodeTmrCtrl, &pVnode->commitTimer);

  pthread_mutex_lock(&pPool->vmutex);

  pPool->commitInProcess = 0;
  dTrace("vid:%d, commit is over, notFreeSlots:%d", pPool->vnode, pPool->notFreeSlots);

  pthread_mutex_unlock(&pPool->vmutex);
}

static void vnodeWaitForCommitComplete(SVnodeObj *pVnode) {
  SCachePool *pPool = (SCachePool *)(pVnode->pCachePool);

  // wait for 100s at most
  const int32_t totalCount = 1000;
  int32_t count = 0;

  // all meter is marked as dropped, so the commit will abort very quickly
  while(count++ < totalCount) {
    int32_t commitInProcess = 0;

    pthread_mutex_lock(&pPool->vmutex);
    commitInProcess = pPool->commitInProcess;
    pthread_mutex_unlock(&pPool->vmutex);

    if (commitInProcess) {
      dWarn("vid:%d still in commit, wait for completed", pVnode->vnode);
      taosMsleep(10);
    }
  }
}

void vnodeCancelCommit(SVnodeObj *pVnode) {
  SCachePool *pPool = (SCachePool *)(pVnode->pCachePool);
  if (pPool == NULL) return;

  vnodeWaitForCommitComplete(pVnode);
  taosTmrReset(vnodeProcessCommitTimer, pVnode->cfg.commitTime * 1000, pVnode, vnodeTmrCtrl, &pVnode->commitTimer);
}

int vnodeAllocateCacheBlock(SMeterObj *pObj) {
  int          index;
  SCachePool * pPool;
  SCacheBlock *pCacheBlock;
  SCacheInfo * pInfo;
  SVnodeObj *  pVnode;
  int          skipped = 0, commit = 0;

  pVnode = vnodeList + pObj->vnode;
  pPool = (SCachePool *)pVnode->pCachePool;
  pInfo = (SCacheInfo *)pObj->pCache;
  SVnodeCfg *pCfg = &(vnodeList[pObj->vnode].cfg);

  if (pPool == NULL) return -1;
  pthread_mutex_lock(&pPool->vmutex);

  if (pInfo == NULL || pInfo->cacheBlocks == NULL) {
    pthread_mutex_unlock(&pPool->vmutex);
    dError("vid:%d sid:%d id:%s, meter is not there", pObj->vnode, pObj->sid, pObj->meterId);
    return -1;
  }

  if (pPool->count <= 1) {
    if (pVnode->commitTimer == NULL)
      pVnode->commitTimer = taosTmrStart(vnodeProcessCommitTimer, pCfg->commitTime * 1000, pVnode, vnodeTmrCtrl);
  }

  if (pInfo->unCommittedBlocks >= pInfo->maxBlocks-1) {
    vnodeCreateCommitThread(pVnode);
    pthread_mutex_unlock(&pPool->vmutex);
    dError("vid:%d sid:%d id:%s, all blocks are not committed yet....", pObj->vnode, pObj->sid, pObj->meterId);
    return -1;
  }

  while (1) {
    pCacheBlock = (SCacheBlock *)(pPool->pMem[((int64_t)pPool->freeSlot)]);
    if (pCacheBlock->blockId == 0) break;

    if (pCacheBlock->notFree) {
      pPool->freeSlot++;
      pPool->freeSlot = pPool->freeSlot % pCfg->cacheNumOfBlocks.totalBlocks;
      skipped++;
      if (skipped > pPool->threshold) {
        vnodeCreateCommitThread(pVnode);
        pthread_mutex_unlock(&pPool->vmutex);
        dError("vid:%d sid:%d id:%s, committing process is too slow, notFreeSlots:%d....",
               pObj->vnode, pObj->sid, pObj->meterId, pPool->notFreeSlots);
        return -1;
      }
    } else {
      SMeterObj  *pRelObj = pCacheBlock->pMeterObj;
      SCacheInfo *pRelInfo = (SCacheInfo *)pRelObj->pCache;
      int firstSlot = (pRelInfo->currentSlot - pRelInfo->numOfBlocks + 1 + pRelInfo->maxBlocks) % pRelInfo->maxBlocks;
      pCacheBlock = pRelInfo->cacheBlocks[firstSlot];
      if (pCacheBlock) {
        pPool->freeSlot = pCacheBlock->index;
        vnodeFreeCacheBlock(pCacheBlock);
        break;
      } else {
        pPool->freeSlot = (pPool->freeSlot + 1) % pCfg->cacheNumOfBlocks.totalBlocks;
        skipped++;
      }
    }
  }

  index = pPool->freeSlot;
  pPool->freeSlot++;
  pPool->freeSlot = pPool->freeSlot % pCfg->cacheNumOfBlocks.totalBlocks;
  pPool->notFreeSlots++;

  pCacheBlock->pMeterObj = pObj;
  pCacheBlock->notFree = 1;
  pCacheBlock->index = index;

  pCacheBlock->offset[0] = ((char *)(pCacheBlock)) + sizeof(SCacheBlock) + pObj->numOfColumns * sizeof(char *);
  for (int col = 1; col < pObj->numOfColumns; ++col)
    pCacheBlock->offset[col] = pCacheBlock->offset[col - 1] + pObj->schema[col - 1].bytes * pObj->pointsPerBlock;

  pInfo->numOfBlocks++;
  pInfo->blocks++;
  pInfo->unCommittedBlocks++;
  pInfo->currentSlot = (pInfo->currentSlot + 1) % pInfo->maxBlocks;
  pCacheBlock->blockId = pInfo->blocks;
  pCacheBlock->slot = pInfo->currentSlot;
  if (pInfo->numOfBlocks > pInfo->maxBlocks) {
    pCacheBlock = pInfo->cacheBlocks[pInfo->currentSlot];
    vnodeFreeCacheBlock(pCacheBlock);
  }

  pInfo->cacheBlocks[pInfo->currentSlot] = (SCacheBlock *)(pPool->pMem[(int64_t)index]);
  dTrace("vid:%d sid:%d id:%s, allocate a cache block, numOfBlocks:%d, slot:%d, index:%d notFreeSlots:%d blocks:%d",
         pObj->vnode, pObj->sid, pObj->meterId, pInfo->numOfBlocks, pInfo->currentSlot, index, pPool->notFreeSlots,
         pInfo->blocks);

  if (((pPool->notFreeSlots > pPool->threshold) || (pInfo->unCommittedBlocks >= pInfo->maxBlocks / 2))) {
    dTrace("vid:%d sid:%d id:%s, too many unCommitted slots, unCommitted:%d notFreeSlots:%d",
           pObj->vnode, pObj->sid, pObj->meterId, pInfo->unCommittedBlocks, pPool->notFreeSlots);
    vnodeCreateCommitThread(pVnode);
    commit = 1;
  }

  pthread_mutex_unlock(&pPool->vmutex);

  return commit;
}

int vnodeInsertPointToCache(SMeterObj *pObj, char *pData) {
  SCacheBlock *pCacheBlock;
  SCacheInfo * pInfo;
  SCachePool * pPool;

  pInfo = (SCacheInfo *)pObj->pCache;
  pPool = (SCachePool *)vnodeList[pObj->vnode].pCachePool;

  if (pInfo->numOfBlocks == 0) {
    if (vnodeAllocateCacheBlock(pObj) < 0) {
      return -1;
    }
  }

  if (pInfo->currentSlot < 0) return -1;
  pCacheBlock = pInfo->cacheBlocks[pInfo->currentSlot];
  if (pCacheBlock->numOfPoints >= pObj->pointsPerBlock) {
    if (vnodeAllocateCacheBlock(pObj) < 0) return -1;
    pCacheBlock = pInfo->cacheBlocks[pInfo->currentSlot];
  }

  for (int col = 0; col < pObj->numOfColumns; ++col) {
    memcpy(pCacheBlock->offset[col] + pCacheBlock->numOfPoints * pObj->schema[col].bytes, pData,
           pObj->schema[col].bytes);
    pData += pObj->schema[col].bytes;
  }

  __sync_fetch_and_sub(&pObj->freePoints, 1);
  pCacheBlock->numOfPoints++;
  pPool->count++;

  return 0;
}

void vnodeUpdateQuerySlotPos(SCacheInfo *pInfo, SQuery *pQuery) {
  SCacheBlock *pCacheBlock;

  int step = QUERY_IS_ASC_QUERY(pQuery) ? -1 : 1;

  if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->slot == pQuery->currentSlot)) ||
      (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->slot == pQuery->firstSlot))) {
    pQuery->over = 1;

  } else {
    pQuery->slot = (pQuery->slot - step + pInfo->maxBlocks) % pInfo->maxBlocks;
    pCacheBlock = pInfo->cacheBlocks[pQuery->slot];
    pQuery->pos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : pCacheBlock->numOfPoints - 1;
  }
}

static FORCE_INLINE TSKEY vnodeGetTSInCacheBlock(SCacheBlock *pCacheBlock, int32_t pos) {
  return *(TSKEY *)(pCacheBlock->offset[PRIMARYKEY_TIMESTAMP_COL_INDEX] + pos * TSDB_KEYSIZE);
}

int vnodeQueryFromCache(SMeterObj *pObj, SQuery *pQuery) {
  SCacheBlock *pCacheBlock;
  int          col, step;
  char *       pRead, *pData;
  SCacheInfo * pInfo;
  int          lastPos = -1;
  int          startPos, numOfReads, numOfPoints;

  pQuery->pointsRead = 0;
  if (pQuery->over) return 0;

  vnodeFreeFields(pQuery);

  pInfo = (SCacheInfo *)pObj->pCache;
  if ((pInfo == NULL) || (pInfo->numOfBlocks == 0)) {
    pQuery->over = 1;
    return 0;
  }

  if (pQuery->slot < 0 || pQuery->pos < 0)  // it means a new query, we need to find the point first
    vnodeSearchPointInCache(pObj, pQuery);

  if (pQuery->slot < 0 || pQuery->pos < 0) {
    pQuery->over = 1;
    return 0;
  }

  step = QUERY_IS_ASC_QUERY(pQuery) ? -1 : 1;
  pCacheBlock = pInfo->cacheBlocks[pQuery->slot];
  numOfPoints = pCacheBlock->numOfPoints;

  int maxReads = QUERY_IS_ASC_QUERY(pQuery) ? numOfPoints - pQuery->pos : pQuery->pos + 1;
  if (maxReads <= 0) {
    vnodeUpdateQuerySlotPos(pInfo, pQuery);
    return 0;
  }

  TSKEY startkey = vnodeGetTSInCacheBlock(pCacheBlock, 0);
  TSKEY endkey   = vnodeGetTSInCacheBlock(pCacheBlock, numOfPoints - 1);

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    if (endkey < pQuery->ekey) {
      numOfReads = maxReads;
    } else {
      lastPos = (*vnodeSearchKeyFunc[pObj->searchAlgorithm])(
          pCacheBlock->offset[PRIMARYKEY_TIMESTAMP_COL_INDEX] + TSDB_KEYSIZE * pQuery->pos, maxReads, pQuery->ekey, 0);
      numOfReads = (lastPos >= 0) ? lastPos + 1 : 0;
    }
  } else {
    if (startkey > pQuery->ekey) {
      numOfReads = maxReads;
    } else {
      lastPos = (*vnodeSearchKeyFunc[pObj->searchAlgorithm])(pCacheBlock->offset[PRIMARYKEY_TIMESTAMP_COL_INDEX],
                                                             maxReads, pQuery->ekey, 1);
      numOfReads = (lastPos >= 0) ? pQuery->pos - lastPos + 1 : 0;
    }
  }

  if (numOfReads > pQuery->pointsToRead - pQuery->pointsRead) {
    numOfReads = pQuery->pointsToRead - pQuery->pointsRead;
  } else {
    if (lastPos >= 0 || numOfReads == 0) {
      pQuery->keyIsMet = 1;
      pQuery->over = 1;
    }
  }

  startPos = QUERY_IS_ASC_QUERY(pQuery) ? pQuery->pos : pQuery->pos - numOfReads + 1;

  int32_t numOfQualifiedPoints = 0;
  int32_t numOfActualRead = numOfReads;

  if (pQuery->numOfFilterCols == 0) {
    for (col = 0; col < pQuery->numOfOutputCols; ++col) {
      int16_t colIdx = pQuery->pSelectExpr[col].pBase.colInfo.colIdx;

      int16_t bytes = GET_COLUMN_BYTES(pQuery, col);
      int16_t type = GET_COLUMN_TYPE(pQuery, col);

      pData = pQuery->sdata[col]->data + pQuery->pointsOffset * bytes;
      /* this column is absent from current block, fill this block with null value */
      if (colIdx < 0 || colIdx >= pObj->numOfColumns ||
          pObj->schema[colIdx].colId != pQuery->pSelectExpr[col].pBase.colInfo.colId) {  // set null
        setNullN(pData, type, bytes, pCacheBlock->numOfPoints);
      } else {
        pRead = pCacheBlock->offset[colIdx] + startPos * bytes;
        memcpy(pData, pRead, numOfReads * bytes);
      }
    }
    numOfQualifiedPoints = numOfReads;
  } else {  // check each data one by one
    // set the input column data
    for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
      int16_t colIdx = pQuery->pFilterInfo[k].pFilter.colIdx;

      if (colIdx < 0) { // current data has not specified column
        pQuery->pFilterInfo[k].pData = NULL;
      } else {
        pQuery->pFilterInfo[k].pData = pCacheBlock->offset[colIdx];
      }
    }

    int32_t *ids = calloc(1, numOfReads * sizeof(int32_t));
    numOfActualRead = 0;

    if (QUERY_IS_ASC_QUERY(pQuery)) {
      for (int32_t j = startPos; j < pCacheBlock->numOfPoints; ++j) {
        TSKEY key = vnodeGetTSInCacheBlock(pCacheBlock, j);
        if (key < startkey || key > endkey) {
          dError("vid:%d sid:%d id:%s, timestamp in cache slot is disordered. slot:%d, pos:%d, ts:%lld, block "
                 "range:%lld-%lld", pObj->vnode, pObj->sid, pObj->meterId, pQuery->slot, j, key, startkey, endkey);
          tfree(ids);
          return -TSDB_CODE_FILE_BLOCK_TS_DISORDERED;
        }

        if (key > pQuery->ekey) {
          break;
        }

        if (!vnodeFilterData(pQuery, &numOfActualRead, j)) {
          continue;
        }

        ids[numOfQualifiedPoints] = j;
        if (++numOfQualifiedPoints == numOfReads) {
          // qualified data are enough
          break;
        }
      }
    } else {
      startPos = pQuery->pos;
      for (int32_t j = startPos; j >= 0; --j) {
        TSKEY key = vnodeGetTSInCacheBlock(pCacheBlock, j);
        if (key < startkey || key > endkey) {
          dError("vid:%d sid:%d id:%s, timestamp in cache slot is disordered. slot:%d, pos:%d, ts:%lld, block "
                 "range:%lld-%lld", pObj->vnode, pObj->sid, pObj->meterId, pQuery->slot, j, key, startkey, endkey);
          tfree(ids);
          return -TSDB_CODE_FILE_BLOCK_TS_DISORDERED;
        }

        if (key < pQuery->ekey) {
          break;
        }

        if (!vnodeFilterData(pQuery, &numOfActualRead, j)) {
          continue;
        }

        ids[numOfReads - numOfQualifiedPoints - 1] = j;
        if (++numOfQualifiedPoints == numOfReads) {
          // qualified data are enough
          break;
        }
      }
    }

    int32_t start = QUERY_IS_ASC_QUERY(pQuery) ? 0 : numOfReads - numOfQualifiedPoints;
    for (int32_t j = 0; j < numOfQualifiedPoints; ++j) {
      for (int32_t col = 0; col < pQuery->numOfOutputCols; ++col) {
        int16_t colIndex = pQuery->pSelectExpr[col].pBase.colInfo.colIdx;

        int32_t bytes = pObj->schema[colIndex].bytes;
        pData = pQuery->sdata[col]->data + (pQuery->pointsOffset + j) * bytes;
        pRead = pCacheBlock->offset[colIndex] + ids[j + start] * bytes;

        memcpy(pData, pRead, bytes);
      }
    }

    tfree(ids);
    assert(numOfQualifiedPoints <= numOfReads);
  }

  pQuery->pointsRead += numOfQualifiedPoints;
  pQuery->pos -= numOfActualRead * step;

  // update the skey/lastkey
  int32_t lastAccessPos = pQuery->pos + step;
  pQuery->lastKey = vnodeGetTSInCacheBlock(pCacheBlock, lastAccessPos);
  pQuery->skey = pQuery->lastKey - step;

  int update = 0;  // go to next slot after this round
  if ((pQuery->pos < 0 || pQuery->pos >= pObj->pointsPerBlock || numOfReads == 0) && (pQuery->over == 0)) update = 1;

  // if block is changed, it shall be thrown away, it won't happen for committing
  if (pObj != pCacheBlock->pMeterObj || pCacheBlock->blockId > pQuery->blockId) {
    update = 1;
    pQuery->pointsRead = 0;
    dWarn("vid:%d sid:%d id:%s, cache block is overwritten, slot:%d blockId:%d qBlockId:%d",
          pObj->vnode, pObj->sid, pObj->meterId, pQuery->slot, pCacheBlock->blockId, pQuery->blockId);
  }

  if (update) vnodeUpdateQuerySlotPos(pInfo, pQuery);

  for (col = 0; col < pQuery->numOfOutputCols; ++col) {
    int16_t bytes = GET_COLUMN_BYTES(pQuery, col);
    pQuery->sdata[col]->len = bytes * (pQuery->pointsRead + pQuery->pointsOffset);
  }
  return pQuery->pointsRead;
}

void vnodeSearchPointInCache(SMeterObj *pObj, SQuery *pQuery) {
  int          numOfBlocks;
  int          firstSlot, lastSlot, midSlot;
  TSKEY        keyFirst, keyLast;
  SCacheBlock *pBlock;
  SCacheInfo * pInfo = (SCacheInfo *)pObj->pCache;
  SCachePool * pPool = (SCachePool *)vnodeList[pObj->vnode].pCachePool;

  pQuery->slot = -1;
  pQuery->pos = -1;

  // save these variables first in case it may be changed by write operation
  pthread_mutex_lock(&pPool->vmutex);
  numOfBlocks = pInfo->numOfBlocks;
  lastSlot = pInfo->currentSlot;
  pthread_mutex_unlock(&pPool->vmutex);
  if (numOfBlocks <= 0) return;

  firstSlot = (lastSlot - numOfBlocks + 1 + pInfo->maxBlocks) % pInfo->maxBlocks;

  // make sure it is there, otherwise, return right away
  pBlock = pInfo->cacheBlocks[firstSlot];
  keyFirst = vnodeGetTSInCacheBlock(pBlock, 0);

  pBlock = pInfo->cacheBlocks[lastSlot];
  keyLast = vnodeGetTSInCacheBlock(pBlock, pBlock->numOfPoints - 1);

  pQuery->blockId = pBlock->blockId;
  pQuery->currentSlot = lastSlot;
  pQuery->numOfBlocks = numOfBlocks;
  pQuery->firstSlot = firstSlot;

  if (!QUERY_IS_ASC_QUERY(pQuery)) {
    if (pQuery->skey < keyFirst) return;
    if (pQuery->ekey > keyLast) return;
  } else {
    if (pQuery->skey > keyLast) return;
    if (pQuery->ekey < keyFirst) return;
  }

  while (1) {
    numOfBlocks = (lastSlot - firstSlot + 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
    if (numOfBlocks == 0) numOfBlocks = pInfo->maxBlocks;
    midSlot = (firstSlot + (numOfBlocks >> 1)) % pInfo->maxBlocks;
    pBlock = pInfo->cacheBlocks[midSlot];

    keyFirst = vnodeGetTSInCacheBlock(pBlock, 0);
    keyLast = vnodeGetTSInCacheBlock(pBlock, pBlock->numOfPoints - 1);

    if (numOfBlocks == 1) break;

    if (pQuery->skey > keyLast) {
      if (numOfBlocks == 2) break;
      if (!QUERY_IS_ASC_QUERY(pQuery)) {
        int          nextSlot = (midSlot + 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
        SCacheBlock *pNextBlock = pInfo->cacheBlocks[nextSlot];
        TSKEY        nextKeyFirst = vnodeGetTSInCacheBlock(pNextBlock, 0);
        if (pQuery->skey < nextKeyFirst) break;
      }
      firstSlot = (midSlot + 1) % pInfo->maxBlocks;
    } else if (pQuery->skey < keyFirst) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        int          prevSlot = (midSlot - 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
        SCacheBlock *pPrevBlock = pInfo->cacheBlocks[prevSlot];
        TSKEY        prevKeyLast = vnodeGetTSInCacheBlock(pPrevBlock, pPrevBlock->numOfPoints - 1);

        if (pQuery->skey > prevKeyLast) break;
      }
      lastSlot = (midSlot - 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
    } else {
      break;  // got the slot
    }
  }

  pQuery->slot = midSlot;
  if (!QUERY_IS_ASC_QUERY(pQuery)) {
    if (pQuery->skey < keyFirst) return;

    if (pQuery->ekey > keyLast) {
      pQuery->slot = (midSlot + 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
      return;
    }
  } else {
    if (pQuery->skey > keyLast) {
      pQuery->slot = (midSlot + 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
      return;
    }

    if (pQuery->ekey < keyFirst) return;
  }

  // midSlot and pBlock is the search result

  pBlock = pInfo->cacheBlocks[midSlot];
  pQuery->pos = (*vnodeSearchKeyFunc[pObj->searchAlgorithm])(pBlock->offset[0], pBlock->numOfPoints, pQuery->skey,
                                                             pQuery->order.order);
  pQuery->key = vnodeGetTSInCacheBlock(pBlock, pQuery->pos);

  if (pQuery->limit.offset > 0 && pQuery->numOfFilterCols == 0) {
    int maxReads = QUERY_IS_ASC_QUERY(pQuery) ? pBlock->numOfPoints - pQuery->pos : pQuery->pos + 1;

    if (pQuery->limit.offset < maxReads) {  // start position in current block
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        pQuery->pos += pQuery->limit.offset;
      } else {
        pQuery->pos -= pQuery->limit.offset;
      }

      pQuery->key = vnodeGetTSInCacheBlock(pBlock, pQuery->pos);
      pQuery->limit.offset = 0;
    } else if (pInfo->numOfBlocks == 1) {
      pQuery->pos = -1;  // no qualified data
    } else {
      int step = QUERY_IS_ASC_QUERY(pQuery) ? 1 : -1;

      pQuery->limit.offset -= maxReads;
      midSlot = (midSlot + step + pInfo->maxBlocks) % pInfo->maxBlocks;

      bool hasData = true;
      while (pQuery->limit.offset > pInfo->cacheBlocks[midSlot]->numOfPoints) {
        pQuery->limit.offset -= pInfo->cacheBlocks[midSlot]->numOfPoints;

        if ((QUERY_IS_ASC_QUERY(pQuery) && midSlot == pQuery->currentSlot) ||
            (!QUERY_IS_ASC_QUERY(pQuery) && midSlot == pQuery->firstSlot)) {  // no qualified data in cache
          hasData = false;
          break;
        }
        midSlot = (midSlot + step + pInfo->maxBlocks) % pInfo->maxBlocks;
      }

      if (hasData) {
        if (QUERY_IS_ASC_QUERY(pQuery)) {
          pQuery->pos = pQuery->limit.offset;
        } else {
          pQuery->pos = pInfo->cacheBlocks[midSlot]->numOfPoints - pQuery->limit.offset - 1;
        }
        pQuery->limit.offset = 0;
        pQuery->slot = midSlot;

        pQuery->key = vnodeGetTSInCacheBlock(pInfo->cacheBlocks[midSlot], pQuery->pos);
      } else {
        pQuery->pos = -1;  // no qualified data

        pBlock = pInfo->cacheBlocks[midSlot];
        if (QUERY_IS_ASC_QUERY(pQuery)) {
          pQuery->lastKey = vnodeGetTSInCacheBlock(pBlock, pBlock->numOfPoints - 1);
          pQuery->skey = pQuery->lastKey + 1;
        } else {
          pQuery->lastKey = vnodeGetTSInCacheBlock(pBlock, 0);
          pQuery->skey = pQuery->lastKey - 1;
        }
      }
    }
  }

  return;
}

void vnodeSetCommitQuery(SMeterObj *pObj, SQuery *pQuery) {
  SCacheInfo *pInfo = (SCacheInfo *)pObj->pCache;
  SCachePool *pPool = (SCachePool *)vnodeList[pObj->vnode].pCachePool;
  SVnodeObj * pVnode = vnodeList + pObj->vnode;

  pQuery->order.order = TSQL_SO_ASC;
  pQuery->numOfCols = pObj->numOfColumns;
  pQuery->numOfOutputCols = pObj->numOfColumns;

  for (int16_t col = 0; col < pObj->numOfColumns; ++col) {
    pQuery->colList[col].colIdxInBuf = col;

    pQuery->colList[col].data.colId = pObj->schema[col].colId;
    pQuery->colList[col].data.bytes = pObj->schema[col].bytes;
    pQuery->colList[col].data.type = pObj->schema[col].type;

    SColIndexEx *pColIndexEx = &pQuery->pSelectExpr[col].pBase.colInfo;

    pColIndexEx->colId = pObj->schema[col].colId;
    pColIndexEx->colIdx = col;
    pColIndexEx->colIdxInBuf = col;
    pColIndexEx->isTag = false;
  }

  pQuery->slot = pInfo->commitSlot;
  pQuery->pos = pInfo->commitPoint;
  pQuery->over = 0;

  pthread_mutex_lock(&pPool->vmutex);
  pQuery->currentSlot = pInfo->currentSlot;
  pQuery->numOfBlocks = pInfo->numOfBlocks;
  pthread_mutex_unlock(&pPool->vmutex);

  if (pQuery->numOfBlocks <= 0 || pQuery->firstSlot < 0) {
    pQuery->over = 1;
    return;
  }

  pQuery->firstSlot = (pQuery->currentSlot - pQuery->numOfBlocks + 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
  pQuery->blockId = pInfo->cacheBlocks[pQuery->currentSlot]->blockId;

  SCacheBlock *pCacheBlock;
  pCacheBlock = pInfo->cacheBlocks[pInfo->commitSlot];
  if (pInfo->commitSlot == pQuery->currentSlot && pInfo->commitPoint == pCacheBlock->numOfPoints) {
    dTrace("vid:%d sid:%d id:%s, no new data to commit", pObj->vnode, pObj->sid, pObj->meterId);
    pQuery->over = 1;
    return;
  }

  if (pQuery->pos == pObj->pointsPerBlock) {
    pQuery->slot = (pQuery->slot + 1) % pInfo->maxBlocks;
    pQuery->pos = 0;
  }

  pCacheBlock = pInfo->cacheBlocks[pQuery->slot];
  TSKEY firstKey = *((TSKEY *)(pCacheBlock->offset[0] + pQuery->pos * pObj->schema[0].bytes));

  if (firstKey < pQuery->skey) {
    pQuery->over = 1;
    dTrace("vid:%d sid:%d id:%s, first key is small, keyFirst:%ld commitFirstKey:%ld",
        pObj->vnode, pObj->sid, pObj->meterId, firstKey, pQuery->skey);
    pthread_mutex_lock(&(pVnode->vmutex));
    if (firstKey < pVnode->firstKey) pVnode->firstKey = firstKey;
    pthread_mutex_unlock(&(pVnode->vmutex));
  }
}

int vnodeIsCacheCommitted(SMeterObj *pObj) {
  if (pObj->pCache == NULL) return 1;

  SCacheInfo *pInfo = (SCacheInfo *)pObj->pCache;
  if (pInfo->currentSlot < 0) return 1;

  SCacheBlock *pBlock = pInfo->cacheBlocks[pInfo->currentSlot];
  if (pInfo->commitSlot != pInfo->currentSlot) return 0;
  if (pInfo->commitPoint != pBlock->numOfPoints) return 0;

  return 1;
}
