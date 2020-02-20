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

#include "tglobalcfg.h"
#include "tlog.h"
#include "tmempool.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "rpcCache.h"

typedef struct _c_hash_t {
  uint32_t          ip;
  uint16_t          port;
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
  void          (*cleanFp)(void *);
  void           *tmrCtrl;
  void           *pTimer;
} SConnCache;

int rpcHashConn(void *handle, uint32_t ip, uint16_t port, char *user) {
  SConnCache *pCache = (SConnCache *)handle;
  int         hash = 0;
  // size_t    user_len = strlen(user);

  hash = ip >> 16;
  hash += (unsigned short)(ip & 0xFFFF);
  hash += port;
  while (*user != '\0') {
    hash += *user;
    user++;
  }

  hash = hash % pCache->maxSessions;

  return hash;
}

void rpcRemoveExpiredNodes(SConnCache *pCache, SConnHash *pNode, int hash, uint64_t time) {
  if (pNode == NULL || (time < pCache->keepTimer + pNode->time) ) return;

  SConnHash *pPrev = pNode->prev, *pNext;

  while (pNode) {
    (*pCache->cleanFp)(pNode->data);
    pNext = pNode->next;
    pCache->total--;
    pCache->count[hash]--;
    tTrace("%p ip:0x%x:%hu:%d:%p removed from cache, connections:%d", pNode->data, pNode->ip, pNode->port, hash, pNode,
             pCache->count[hash]);
    taosMemPoolFree(pCache->connHashMemPool, (char *)pNode);
    pNode = pNext;
  }

  if (pPrev)
    pPrev->next = NULL;
  else
    pCache->connHashList[hash] = NULL;
}

void rpcAddConnIntoCache(void *handle, void *data, uint32_t ip, uint16_t port, char *user) {
  int         hash;
  SConnHash * pNode;
  SConnCache *pCache;

  uint64_t time = taosGetTimestampMs();

  pCache = (SConnCache *)handle;
  assert(pCache); 
  assert(data);

  hash = rpcHashConn(pCache, ip, port, user);
  pNode = (SConnHash *)taosMemPoolMalloc(pCache->connHashMemPool);
  pNode->ip = ip;
  pNode->port = port;
  pNode->data = data;
  pNode->prev = NULL;
  pNode->time = time;

  pthread_mutex_lock(&pCache->mutex);

  pNode->next = pCache->connHashList[hash];
  if (pCache->connHashList[hash] != NULL) (pCache->connHashList[hash])->prev = pNode;
  pCache->connHashList[hash] = pNode;

  pCache->total++;
  pCache->count[hash]++;
  rpcRemoveExpiredNodes(pCache, pNode->next, hash, time);

  pthread_mutex_unlock(&pCache->mutex);

  tTrace("%p ip:0x%x:%hu:%d:%p added into cache, connections:%d", data, ip, port, hash, pNode, pCache->count[hash]);

  return;
}

void rpcCleanConnCache(void *handle, void *tmrId) {
  int         hash;
  SConnHash * pNode;
  SConnCache *pCache;

  pCache = (SConnCache *)handle;
  if (pCache == NULL || pCache->maxSessions == 0) return;
  if (pCache->pTimer != tmrId) return;

  uint64_t time = taosGetTimestampMs();

  for (hash = 0; hash < pCache->maxSessions; ++hash) {
    pthread_mutex_lock(&pCache->mutex);
    pNode = pCache->connHashList[hash];
    rpcRemoveExpiredNodes(pCache, pNode, hash, time);
    pthread_mutex_unlock(&pCache->mutex);
  }

  // tTrace("timer, total connections in cache:%d", pCache->total);
  taosTmrReset(rpcCleanConnCache, pCache->keepTimer * 2, pCache, pCache->tmrCtrl, &pCache->pTimer);
}

void *rpcGetConnFromCache(void *handle, uint32_t ip, uint16_t port, char *user) {
  int         hash;
  SConnHash * pNode;
  SConnCache *pCache;
  void *      pData = NULL;

  pCache = (SConnCache *)handle;
  assert(pCache); 

  uint64_t time = taosGetTimestampMs();

  hash = rpcHashConn(pCache, ip, port, user);
  pthread_mutex_lock(&pCache->mutex);

  pNode = pCache->connHashList[hash];
  while (pNode) {
    if (time >= pCache->keepTimer + pNode->time) {
      rpcRemoveExpiredNodes(pCache, pNode, hash, time);
      pNode = NULL;
      break;
    }

    if (pNode->ip == ip && pNode->port == port) break;

    pNode = pNode->next;
  }

  if (pNode) {
    rpcRemoveExpiredNodes(pCache, pNode->next, hash, time);

    if (pNode->prev) {
      pNode->prev->next = pNode->next;
    } else {
      pCache->connHashList[hash] = pNode->next;
    }

    if (pNode->next) {
      pNode->next->prev = pNode->prev;
    }

    pData = pNode->data;
    taosMemPoolFree(pCache->connHashMemPool, (char *)pNode);
    pCache->total--;
    pCache->count[hash]--;
  }

  pthread_mutex_unlock(&pCache->mutex);

  if (pData) {
    tTrace("%p ip:0x%x:%hu:%d:%p retrieved from cache, connections:%d", pData, ip, port, hash, pNode, pCache->count[hash]);
  }

  return pData;
}

void *rpcOpenConnCache(int maxSessions, void (*cleanFp)(void *), void *tmrCtrl, int64_t keepTimer) {
  SConnHash **connHashList;
  mpool_h     connHashMemPool;
  SConnCache *pCache;

  connHashMemPool = taosMemPoolInit(maxSessions, sizeof(SConnHash));
  if (connHashMemPool == 0) return NULL;

  connHashList = calloc(sizeof(SConnHash *), maxSessions);
  if (connHashList == 0) {
    taosMemPoolCleanUp(connHashMemPool);
    return NULL;
  }

  pCache = malloc(sizeof(SConnCache));
  if (pCache == NULL) {
    taosMemPoolCleanUp(connHashMemPool);
    free(connHashList);
    return NULL;
  }
  memset(pCache, 0, sizeof(SConnCache));

  pCache->count = calloc(sizeof(int), maxSessions);
  pCache->total = 0;
  pCache->keepTimer = keepTimer;
  pCache->maxSessions = maxSessions;
  pCache->connHashMemPool = connHashMemPool;
  pCache->connHashList = connHashList;
  pCache->cleanFp = cleanFp;
  pCache->tmrCtrl = tmrCtrl;
  taosTmrReset(rpcCleanConnCache, pCache->keepTimer * 2, pCache, pCache->tmrCtrl, &pCache->pTimer);

  pthread_mutex_init(&pCache->mutex, NULL);

  return pCache;
}

void rpcCloseConnCache(void *handle) {
  SConnCache *pCache;

  pCache = (SConnCache *)handle;
  if (pCache == NULL || pCache->maxSessions == 0) return;

  pthread_mutex_lock(&pCache->mutex);

  taosTmrStopA(&(pCache->pTimer));

  if (pCache->connHashMemPool) taosMemPoolCleanUp(pCache->connHashMemPool);

  tfree(pCache->connHashList);
  tfree(pCache->count)

      pthread_mutex_unlock(&pCache->mutex);

  pthread_mutex_destroy(&pCache->mutex);

  memset(pCache, 0, sizeof(SConnCache));
  free(pCache);
}
