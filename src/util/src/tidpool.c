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

#include "os.h"
#include "tulog.h"
#include <stdbool.h>

typedef struct {
  int             maxId;
  int             numOfFree;
  int             freeSlot;
  bool *          freeList;
  pthread_mutex_t mutex;
} id_pool_t;

void *taosInitIdPool(int maxId) {
  id_pool_t *pIdPool = calloc(1, sizeof(id_pool_t));
  if (pIdPool == NULL) return NULL;

  pIdPool->freeList = calloc(maxId, sizeof(bool));
  if (pIdPool->freeList == NULL) {
    free(pIdPool);
    return NULL;
  }

  pIdPool->maxId = maxId;
  pIdPool->numOfFree = maxId;
  pIdPool->freeSlot = 0;

  pthread_mutex_init(&pIdPool->mutex, NULL);

  uDebug("pool:%p is setup, maxId:%d", pIdPool, pIdPool->maxId);

  return pIdPool;
}

int taosAllocateId(void *handle) {
  id_pool_t *pIdPool = handle;
  if (handle == NULL) {
    return -1;
  }

  int slot = -1;
  pthread_mutex_lock(&pIdPool->mutex);

  if (pIdPool->numOfFree > 0) {
    for (int i = 0; i < pIdPool->maxId; ++i) {
      slot = (i + pIdPool->freeSlot) % pIdPool->maxId;
      if (!pIdPool->freeList[slot]) {
        pIdPool->freeList[slot] = true;
        pIdPool->freeSlot = slot + 1;
        pIdPool->numOfFree--;
        break;
      }
    }
  }

  pthread_mutex_unlock(&pIdPool->mutex);
  return slot + 1;
}

void taosFreeId(void *handle, int id) {
  id_pool_t *pIdPool = handle;
  if (handle == NULL) return;

  pthread_mutex_lock(&pIdPool->mutex);

  int slot = (id - 1) % pIdPool->maxId;
  if (pIdPool->freeList[slot]) {
    pIdPool->freeList[slot] = false;
    pIdPool->numOfFree++;
  }

  pthread_mutex_unlock(&pIdPool->mutex);
}

void taosIdPoolCleanUp(void *handle) {
  id_pool_t *pIdPool = handle;

  if (pIdPool == NULL) return;

  uDebug("pool:%p is cleaned", pIdPool);

  if (pIdPool->freeList) free(pIdPool->freeList);

  pthread_mutex_destroy(&pIdPool->mutex);

  memset(pIdPool, 0, sizeof(id_pool_t));

  free(pIdPool);
}

int taosIdPoolNumOfUsed(void *handle) {
  id_pool_t *pIdPool = handle;

  pthread_mutex_lock(&pIdPool->mutex);
  int ret = pIdPool->maxId - pIdPool->numOfFree;
  pthread_mutex_unlock(&pIdPool->mutex);

  return ret;
}

bool taosIdPoolMarkStatus(void *handle, int id) {
  bool ret = false;
  id_pool_t *pIdPool = handle;
  pthread_mutex_lock(&pIdPool->mutex);

  int slot = (id - 1) % pIdPool->maxId;
  if (!pIdPool->freeList[slot]) {
    pIdPool->freeList[slot] = true;
    pIdPool->numOfFree--;
    ret = true;
  } else {
    ret = false;
    uError("pool:%p, id:%d is already used by other obj", pIdPool, id);
  }

  pthread_mutex_unlock(&pIdPool->mutex);
  return ret;
}

int taosUpdateIdPool(id_pool_t *handle, int maxId) {
  id_pool_t *pIdPool = (id_pool_t*)handle;
  if (maxId <= pIdPool->maxId) {
    return 0;
  }

  bool *idList = calloc(maxId, sizeof(bool));
  if (idList == NULL) {
    return -1;
  }

  pthread_mutex_lock(&pIdPool->mutex);

  memcpy(idList, pIdPool->freeList, sizeof(bool) * pIdPool->maxId);
  pIdPool->numOfFree += (maxId - pIdPool->maxId);
  pIdPool->maxId = maxId;

  bool *oldIdList = pIdPool->freeList;
  pIdPool->freeList = idList;
  free(oldIdList);

  pthread_mutex_unlock(&pIdPool->mutex);

  return 0;
}

int taosIdPoolMaxSize(void *handle) {
  id_pool_t *pIdPool = (id_pool_t *)handle;

  pthread_mutex_lock(&pIdPool->mutex);
  int ret = pIdPool->maxId;
  pthread_mutex_unlock(&pIdPool->mutex);

  return ret;
}