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

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tlog.h"

typedef struct {
  int             maxId;
  int             numOfFree;
  int             freeSlot;
  int *           freeList;
  pthread_mutex_t mutex;
} id_pool_t;

void *taosInitIdPool(int maxId) {
  id_pool_t *pIdPool;
  int *      idList, i;

  if (maxId < 3) maxId = 3;

  pIdPool = (id_pool_t *)malloc(sizeof(id_pool_t));
  if (pIdPool == NULL) return NULL;

  idList = (int *)malloc(sizeof(int) * (size_t)maxId);
  if (idList == NULL) {
    free(pIdPool);
    return NULL;
  }

  memset(pIdPool, 0, sizeof(id_pool_t));
  pIdPool->maxId = maxId;
  pIdPool->numOfFree = maxId - 1;
  pIdPool->freeSlot = 0;
  pIdPool->freeList = idList;

  pthread_mutex_init(&pIdPool->mutex, NULL);

  for (i = 1; i < maxId; ++i) idList[i - 1] = i;

  pTrace("pool:%p is setup, maxId:%d", pIdPool, pIdPool->maxId);

  return (void *)pIdPool;
}

int taosAllocateId(void *handle) {
  id_pool_t *pIdPool;
  int        id = -1;
  if (handle == NULL) return id;

  pIdPool = (id_pool_t *)handle;

  if (pIdPool->maxId < 3) pError("pool:%p is messed up, maxId:%d", pIdPool, pIdPool->maxId);

  if (pthread_mutex_lock(&pIdPool->mutex) != 0) perror("lock pIdPool Mutex");

  if (pIdPool->numOfFree > 0) {
    id = pIdPool->freeList[pIdPool->freeSlot];
    pIdPool->freeSlot = (pIdPool->freeSlot + 1) % pIdPool->maxId;
    pIdPool->numOfFree--;
  }

  if (pthread_mutex_unlock(&pIdPool->mutex) != 0) perror("unlock pIdPool Mutex");

  return id;
}

void taosFreeId(void *handle, int id) {
  id_pool_t *pIdPool;
  int        slot;

  pIdPool = (id_pool_t *)handle;
  if (pIdPool->freeList == NULL || pIdPool->maxId == 0) return;
  if (id <= 0 || id >= pIdPool->maxId) return;
  if (pthread_mutex_lock(&pIdPool->mutex) != 0) perror("lock pIdPool Mutex");

  slot = (pIdPool->freeSlot + pIdPool->numOfFree) % pIdPool->maxId;
  pIdPool->freeList[slot] = id;
  pIdPool->numOfFree++;

  if (pthread_mutex_unlock(&pIdPool->mutex) != 0) perror("unlock pIdPool Mutex");
}

void taosIdPoolCleanUp(void *handle) {
  id_pool_t *pIdPool;

  if (handle == NULL) return;
  pIdPool = (id_pool_t *)handle;

  pTrace("pool:%p is cleaned", pIdPool);

  if (pIdPool->freeList) free(pIdPool->freeList);

  pthread_mutex_destroy(&pIdPool->mutex);

  memset(pIdPool, 0, sizeof(id_pool_t));

  free(pIdPool);
}

int taosIdPoolNumOfUsed(void *handle) {
  id_pool_t *pIdPool = (id_pool_t *)handle;

  return pIdPool->maxId - pIdPool->numOfFree - 1;
}

void taosIdPoolReinit(void *handle) {
  id_pool_t *pIdPool;

  pIdPool = (id_pool_t *)handle;
  pIdPool->numOfFree = 0;
  pIdPool->freeSlot = 0;

  for (int i = 0; i < pIdPool->maxId; ++i) pIdPool->freeList[i] = 0;
}

void taosIdPoolMarkStatus(void *handle, int id, int status) {
  id_pool_t *pIdPool = (id_pool_t *)handle;

  pIdPool->freeList[id] = status;
}

void taosIdPoolSetFreeList(void *handle) {
  id_pool_t *pIdPool;
  int        pos = 0;

  pIdPool = (id_pool_t *)handle;
  pIdPool->numOfFree = 0;
  pIdPool->freeSlot = 0;

  for (int i = 1; i < pIdPool->maxId; ++i) {
    if (pIdPool->freeList[i] == 0) {
      pIdPool->freeList[pos] = i;
      pIdPool->numOfFree++;
      pos++;
    }
  }
}
