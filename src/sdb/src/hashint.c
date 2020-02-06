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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "tmempool.h"
#include "tsdb.h"

typedef struct _long_hash_t {
  uint32_t             key;
  int                  hash;
  struct _long_hash_t *prev;
  struct _long_hash_t *next;
  char                 data[];
} SLongHash;

typedef struct {
  SLongHash **longHashList;
  mpool_h     longHashMemPool;
  int         maxSessions;
  int         dataSize;
} SHashObj;

int sdbHashLong(void *handle, uint32_t ip) {
  SHashObj *pObj = (SHashObj *)handle;
  int       hash = 0;

  hash = ip >> 16;
  hash += (ip & 0xFFFF);

  hash = hash % pObj->maxSessions;

  return hash;
}

void *sdbAddIntHash(void *handle, void *pKey, void *data) {
  int        hash;
  SLongHash *pNode;
  SHashObj * pObj;
  uint32_t   key = *((uint32_t *)pKey);

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  hash = sdbHashLong(pObj, key);
  pNode = (SLongHash *)taosMemPoolMalloc(pObj->longHashMemPool);
  pNode->key = key;
  memcpy(pNode->data, data, pObj->dataSize);
  pNode->prev = 0;
  pNode->next = pObj->longHashList[hash];
  pNode->hash = hash;

  if (pObj->longHashList[hash] != 0) (pObj->longHashList[hash])->prev = pNode;
  pObj->longHashList[hash] = pNode;

  return pObj;
}

void sdbDeleteIntHash(void *handle, void *pKey) {
  int        hash;
  SLongHash *pNode;
  SHashObj * pObj;
  uint32_t   key = *((uint32_t *)pKey);

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  hash = sdbHashLong(pObj, key);

  pNode = pObj->longHashList[hash];
  while (pNode) {
    if (pNode->key == key) break;

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
}

void *sdbGetIntHashData(void *handle, void *pKey) {
  int        hash;
  SLongHash *pNode;
  SHashObj * pObj;
  uint32_t   key = *((uint32_t *)pKey);

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  hash = sdbHashLong(pObj, key);
  pNode = pObj->longHashList[hash];

  while (pNode) {
    if (pNode->key == key) {
      break;
    }
    pNode = pNode->next;
  }

  if (pNode) return pNode->data;

  return NULL;
}

void *sdbOpenIntHash(int maxSessions, int dataSize) {
  SLongHash **longHashList;
  mpool_h     longHashMemPool;
  SHashObj *  pObj;

  longHashMemPool = taosMemPoolInit(maxSessions, sizeof(SLongHash) + dataSize);
  if (longHashMemPool == 0) return NULL;

  longHashList = calloc(sizeof(SLongHash *), maxSessions);
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
  pObj->dataSize = dataSize;

  return pObj;
}

void sdbCloseIntHash(void *handle) {
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  if (pObj->longHashMemPool) taosMemPoolCleanUp(pObj->longHashMemPool);

  if (pObj->longHashList) free(pObj->longHashList);

  memset(pObj, 0, sizeof(SHashObj));
  free(pObj);
}

void *sdbFetchIntHashData(void *handle, void *ptr, void **ppMeta) {
  SHashObj * pObj = (SHashObj *)handle;
  SLongHash *pNode = (SLongHash *)ptr;
  int        hash = 0;

  *ppMeta = NULL;
  if (pObj == NULL || pObj->maxSessions <= 0) return NULL;
  if (pObj->longHashList == NULL) return NULL;

  if (pNode) {
    hash = pNode->hash + 1;
    pNode = pNode->next;
  }

  if (pNode == NULL) {
    for (int i = hash; i < pObj->maxSessions; ++i) {
      pNode = pObj->longHashList[i];
      if (pNode) break;
    }
  }

  if (pNode) *ppMeta = pNode->data;

  return pNode;
}
