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

#define _DEFAULT_SOURCE
#include "tidpool.h"
#include "tlog.h"

void *taosInitIdPool(int32_t maxId) {
  id_pool_t *pIdPool = taosMemoryCalloc(1, sizeof(id_pool_t));
  if (pIdPool == NULL) return NULL;

  pIdPool->freeList = taosMemoryCalloc(maxId, sizeof(bool));
  if (pIdPool->freeList == NULL) {
    taosMemoryFree(pIdPool);
    return NULL;
  }

  pIdPool->maxId = maxId;
  pIdPool->numOfFree = maxId;
  pIdPool->freeSlot = 0;

  taosThreadMutexInit(&pIdPool->mutex, NULL);

  uDebug("pool:%p is setup, maxId:%d", pIdPool, pIdPool->maxId);

  return pIdPool;
}

int32_t taosAllocateId(id_pool_t *pIdPool) {
  if (pIdPool == NULL) return -1;

  int32_t slot = -1;
  taosThreadMutexLock(&pIdPool->mutex);

  if (pIdPool->numOfFree > 0) {
    for (int32_t i = 0; i < pIdPool->maxId; ++i) {
      slot = (i + pIdPool->freeSlot) % pIdPool->maxId;
      if (!pIdPool->freeList[slot]) {
        pIdPool->freeList[slot] = true;
        pIdPool->freeSlot = slot + 1;
        pIdPool->numOfFree--;
        break;
      }
    }
  }

  taosThreadMutexUnlock(&pIdPool->mutex);
  return slot + 1;
}

void taosFreeId(id_pool_t *pIdPool, int32_t id) {
  if (pIdPool == NULL) return;

  taosThreadMutexLock(&pIdPool->mutex);

  int32_t slot = (id - 1) % pIdPool->maxId;
  if (pIdPool->freeList[slot]) {
    pIdPool->freeList[slot] = false;
    pIdPool->numOfFree++;
  }

  taosThreadMutexUnlock(&pIdPool->mutex);
}

void taosIdPoolCleanUp(id_pool_t *pIdPool) {
  if (pIdPool == NULL) return;

  uDebug("pool:%p is cleaned", pIdPool);

  if (pIdPool->freeList) taosMemoryFree(pIdPool->freeList);

  taosThreadMutexDestroy(&pIdPool->mutex);

  memset(pIdPool, 0, sizeof(id_pool_t));

  taosMemoryFree(pIdPool);
}

int32_t taosIdPoolNumOfUsed(id_pool_t *pIdPool) {
  taosThreadMutexLock(&pIdPool->mutex);
  int32_t ret = pIdPool->maxId - pIdPool->numOfFree;
  taosThreadMutexUnlock(&pIdPool->mutex);

  return ret;
}

bool taosIdPoolMarkStatus(id_pool_t *pIdPool, int32_t id) {
  bool ret = false;
  taosThreadMutexLock(&pIdPool->mutex);

  int32_t slot = (id - 1) % pIdPool->maxId;
  if (!pIdPool->freeList[slot]) {
    pIdPool->freeList[slot] = true;
    pIdPool->numOfFree--;
    ret = true;
  } else {
    ret = false;
  }

  taosThreadMutexUnlock(&pIdPool->mutex);
  return ret;
}

int32_t taosUpdateIdPool(id_pool_t *pIdPool, int32_t maxId) {
  if (maxId <= pIdPool->maxId) {
    return 0;
  }

  bool *idList = taosMemoryCalloc(maxId, sizeof(bool));
  if (idList == NULL) {
    return -1;
  }

  taosThreadMutexLock(&pIdPool->mutex);

  memcpy(idList, pIdPool->freeList, sizeof(bool) * pIdPool->maxId);
  pIdPool->numOfFree += (maxId - pIdPool->maxId);
  pIdPool->maxId = maxId;

  bool *oldIdList = pIdPool->freeList;
  pIdPool->freeList = idList;
  taosMemoryFree(oldIdList);

  taosThreadMutexUnlock(&pIdPool->mutex);

  return 0;
}

int32_t taosIdPoolMaxSize(id_pool_t *pIdPool) {
  taosThreadMutexLock(&pIdPool->mutex);
  int32_t ret = pIdPool->maxId;
  taosThreadMutexUnlock(&pIdPool->mutex);

  return ret;
}