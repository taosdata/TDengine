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

#include <assert.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "tglobalcfg.h"
#include "tlog.h"
#include "tmempool.h"
#include "tsclient.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

typedef struct _c_hash_t {
  uint32_t          ip;
  short             port;
  struct _c_hash_t *prev;
  struct _c_hash_t *next;
  void *            data;
  uint64_t          time;
} SConnHash;

typedef struct {
  SConnHash **    connHashList;
  mpool_h         connHashMemPool;
  int             maxSessions;
  int             total;
  int *           count;
  int64_t         keepTimer;
  pthread_mutex_t mutex;
  void (*cleanFp)(void *);
  void *tmrCtrl;
  void *pTimer;
} SConnCache;

int taosHashConn(void *handle, uint32_t ip, short port, char *user) {
  SConnCache *pObj = (SConnCache *)handle;
  int         hash = 0;
  // size_t    user_len = strlen(user);

  hash = ip >> 16;
  hash += (unsigned short)(ip & 0xFFFF);
  hash += (unsigned short)port;
  while (*user != '\0') {
    hash += *user;
    user++;
  }

  hash = hash % pObj->maxSessions;

  return hash;
}

void taosRemoveExpiredNodes(SConnCache *pObj, SConnHash *pNode, int hash, uint64_t time) {
  if (pNode == NULL) return;
  if (time < pObj->keepTimer + pNode->time) return;

  SConnHash *pPrev = pNode->prev, *pNext;

  while (pNode) {
    (*pObj->cleanFp)(pNode->data);
    pNext = pNode->next;
    pObj->total--;
    pObj->count[hash]--;
    tscTrace("%p ip:0x%x:%d:%d:%p removed, connections in cache:%d", pNode->data, pNode->ip, pNode->port, hash, pNode,
             pObj->count[hash]);
    taosMemPoolFree(pObj->connHashMemPool, (char *)pNode);
    pNode = pNext;
  }

  if (pPrev)
    pPrev->next = NULL;
  else
    pObj->connHashList[hash] = NULL;
}

void *taosAddConnIntoCache(void *handle, void *data, uint32_t ip, short port, char *user) {
  int         hash;
  SConnHash * pNode;
  SConnCache *pObj;

  uint64_t time = taosGetTimestampMs();

  pObj = (SConnCache *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;
  if (data == NULL || ip == 0) {
    tscTrace("data:%p ip:0x%x:%d not valid, not added in cache", data, ip, port);
    return NULL;
  }

  hash = taosHashConn(pObj, ip, port, user);
  pNode = (SConnHash *)taosMemPoolMalloc(pObj->connHashMemPool);
  pNode->ip = ip;
  pNode->port = port;
  pNode->data = data;
  pNode->prev = NULL;
  pNode->time = time;

  pthread_mutex_lock(&pObj->mutex);

  pNode->next = pObj->connHashList[hash];
  if (pObj->connHashList[hash] != NULL) (pObj->connHashList[hash])->prev = pNode;
  pObj->connHashList[hash] = pNode;

  pObj->total++;
  pObj->count[hash]++;
  taosRemoveExpiredNodes(pObj, pNode->next, hash, time);

  pthread_mutex_unlock(&pObj->mutex);

  tscTrace("%p ip:0x%x:%d:%d:%p added, connections in cache:%d", data, ip, port, hash, pNode, pObj->count[hash]);

  return pObj;
}

void taosCleanConnCache(void *handle, void *tmrId) {
  int         hash;
  SConnHash * pNode;
  SConnCache *pObj;

  pObj = (SConnCache *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;
  if (pObj->pTimer != tmrId) return;

  uint64_t time = taosGetTimestampMs();

  for (hash = 0; hash < pObj->maxSessions; ++hash) {
    pthread_mutex_lock(&pObj->mutex);
    pNode = pObj->connHashList[hash];
    taosRemoveExpiredNodes(pObj, pNode, hash, time);
    pthread_mutex_unlock(&pObj->mutex);
  }

  // tscTrace("timer, total connections in cache:%d", pObj->total);
  taosTmrReset(taosCleanConnCache, pObj->keepTimer * 2, pObj, pObj->tmrCtrl, &pObj->pTimer);
}

void *taosGetConnFromCache(void *handle, uint32_t ip, short port, char *user) {
  int         hash;
  SConnHash * pNode;
  SConnCache *pObj;
  void *      pData = NULL;

  pObj = (SConnCache *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  uint64_t time = taosGetTimestampMs();

  hash = taosHashConn(pObj, ip, port, user);
  pthread_mutex_lock(&pObj->mutex);

  pNode = pObj->connHashList[hash];
  while (pNode) {
    if (time >= pObj->keepTimer + pNode->time) {
      taosRemoveExpiredNodes(pObj, pNode, hash, time);
      pNode = NULL;
      break;
    }

    if (pNode->ip == ip && pNode->port == port) break;

    pNode = pNode->next;
  }

  if (pNode) {
    taosRemoveExpiredNodes(pObj, pNode->next, hash, time);

    if (pNode->prev) {
      pNode->prev->next = pNode->next;
    } else {
      pObj->connHashList[hash] = pNode->next;
    }

    if (pNode->next) {
      pNode->next->prev = pNode->prev;
    }

    pData = pNode->data;
    taosMemPoolFree(pObj->connHashMemPool, (char *)pNode);
    pObj->total--;
    pObj->count[hash]--;
  }

  pthread_mutex_unlock(&pObj->mutex);

  if (pData) {
    tscTrace("%p ip:0x%x:%d:%d:%p retrieved, connections in cache:%d", pData, ip, port, hash, pNode, pObj->count[hash]);
  }

  return pData;
}

void *taosOpenConnCache(int maxSessions, void (*cleanFp)(void *), void *tmrCtrl, int64_t keepTimer) {
  SConnHash **connHashList;
  mpool_h     connHashMemPool;
  SConnCache *pObj;

  connHashMemPool = taosMemPoolInit(maxSessions, sizeof(SConnHash));
  if (connHashMemPool == 0) return NULL;

  connHashList = calloc(sizeof(SConnHash *), maxSessions);
  if (connHashList == 0) {
    taosMemPoolCleanUp(connHashMemPool);
    return NULL;
  }

  pObj = malloc(sizeof(SConnCache));
  if (pObj == NULL) {
    taosMemPoolCleanUp(connHashMemPool);
    free(connHashList);
    return NULL;
  }
  memset(pObj, 0, sizeof(SConnCache));

  pObj->count = calloc(sizeof(int), maxSessions);
  pObj->total = 0;
  pObj->keepTimer = keepTimer;
  pObj->maxSessions = maxSessions;
  pObj->connHashMemPool = connHashMemPool;
  pObj->connHashList = connHashList;
  pObj->cleanFp = cleanFp;
  pObj->tmrCtrl = tmrCtrl;
  taosTmrReset(taosCleanConnCache, pObj->keepTimer * 2, pObj, pObj->tmrCtrl, &pObj->pTimer);

  pthread_mutex_init(&pObj->mutex, NULL);

  return pObj;
}

void taosCloseConnCache(void *handle) {
  SConnCache *pObj;

  pObj = (SConnCache *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  pthread_mutex_lock(&pObj->mutex);

  taosTmrStopA(&(pObj->pTimer));

  if (pObj->connHashMemPool) taosMemPoolCleanUp(pObj->connHashMemPool);

  tfree(pObj->connHashList);
  tfree(pObj->count)

      pthread_mutex_unlock(&pObj->mutex);

  pthread_mutex_destroy(&pObj->mutex);

  memset(pObj, 0, sizeof(SConnCache));
  free(pObj);
}
