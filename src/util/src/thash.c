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
#include <semaphore.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "tmempool.h"

typedef struct _long_hash_t {
  unsigned int         id;
  struct _long_hash_t *prev;
  struct _long_hash_t *next;
  uint64_t             cont;
} SLongHash;

typedef struct {
  SLongHash **longHashList;
  mpool_h     longHashMemPool;
  int (*hashFp)(void *, uint64_t);
  int             maxSessions;
  pthread_mutex_t mutex;
} SHashObj;

uint64_t taosHashUInt64(uint64_t handle) {
  uint64_t hash = handle >> 16;
  hash += handle & 0xFFFF;
  return hash;
}

int taosHashLong(void *handle, uint64_t ip) {
  SHashObj *pObj = (SHashObj *)handle;
  int       hash = 0;

  hash = (int)(ip >> 16);
  hash += (int)(ip & 0xFFFF);

  hash = hash % pObj->maxSessions;

  return hash;
}

int taosAddHash(void *handle, uint64_t cont, unsigned int id) {
  int        hash;
  SLongHash *pNode;
  SHashObj * pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return -1;

  pthread_mutex_lock(&pObj->mutex);

  hash = (*pObj->hashFp)(pObj, cont);

  pNode = (SLongHash *)taosMemPoolMalloc(pObj->longHashMemPool);
  pNode->cont = cont;
  pNode->id = id;
  pNode->prev = 0;
  pNode->next = pObj->longHashList[hash];

  if (pObj->longHashList[hash] != 0) (pObj->longHashList[hash])->prev = pNode;
  pObj->longHashList[hash] = pNode;

  pthread_mutex_unlock(&pObj->mutex);

  return 0;
}

void taosDeleteHash(void *handle, uint64_t cont) {
  int        hash;
  SLongHash *pNode;
  SHashObj * pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  hash = (*pObj->hashFp)(pObj, cont);

  pthread_mutex_lock(&pObj->mutex);

  pNode = pObj->longHashList[hash];
  while (pNode) {
    if (pNode->cont == cont) break;

    pNode = pNode->next;
  }

  if (pNode) {
    if (pNode->prev) {
      pNode->prev->next = pNode->next;
    } else {
      pObj->longHashList[hash] = pNode->next;
    }

    if (pNode->next) {
      pNode->next->prev = pNode->prev;
    }

    taosMemPoolFree(pObj->longHashMemPool, (char *)pNode);
  }

  pthread_mutex_unlock(&pObj->mutex);
}

int32_t taosGetIdFromHash(void *handle, uint64_t cont) {
  int        hash;
  SLongHash *pNode;
  SHashObj * pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return -1;

  hash = (*pObj->hashFp)(pObj, cont);

  pthread_mutex_lock(&pObj->mutex);

  pNode = pObj->longHashList[hash];

  while (pNode) {
    if (pNode->cont == cont) {
      break;
    }

    pNode = pNode->next;
  }

  pthread_mutex_unlock(&pObj->mutex);

  if (pNode) return (int32_t)pNode->id;

  return -1;
}

void *taosOpenHash(int maxSessions, int (*fp)(void *, uint64_t)) {
  SLongHash **longHashList;
  mpool_h     longHashMemPool;
  SHashObj *  pObj;

  longHashMemPool = taosMemPoolInit(maxSessions, sizeof(SLongHash));
  if (longHashMemPool == 0) return NULL;

  longHashList = calloc(sizeof(SLongHash *), (size_t)maxSessions);
  if (longHashList == 0) {
    taosMemPoolCleanUp(longHashMemPool);
    return NULL;
  }

  pObj = malloc(sizeof(SHashObj));
  if (pObj == NULL) {
    taosMemPoolCleanUp(longHashMemPool);
    free(longHashList);
    return NULL;
  }

  pObj->maxSessions = maxSessions;
  pObj->longHashMemPool = longHashMemPool;
  pObj->longHashList = longHashList;
  pObj->hashFp = fp;

  pthread_mutex_init(&pObj->mutex, NULL);

  return pObj;
}

void taosCloseHash(void *handle) {
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  pthread_mutex_lock(&pObj->mutex);

  if (pObj->longHashMemPool) taosMemPoolCleanUp(pObj->longHashMemPool);

  if (pObj->longHashList) free(pObj->longHashList);

  pthread_mutex_unlock(&pObj->mutex);

  pthread_mutex_destroy(&pObj->mutex);

  memset(pObj, 0, sizeof(SHashObj));
  free(pObj);
}
