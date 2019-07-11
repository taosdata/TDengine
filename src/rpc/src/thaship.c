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

#include "stdint.h"
#include "tlog.h"
#include "tmempool.h"

typedef struct _ip_hash_t {
  uint32_t           ip;
  short              port;
  int                hash;
  struct _ip_hash_t *prev;
  struct _ip_hash_t *next;
  void *             data;
} SIpHash;

typedef struct {
  SIpHash **ipHashList;
  mpool_h   ipHashMemPool;
  int       maxSessions;
} SHashObj;

int taosHashIp(void *handle, uint32_t ip, short port) {
  SHashObj *pObj = (SHashObj *)handle;
  int       hash = 0;

  hash = (int)(ip >> 16);
  hash += (unsigned short)(ip & 0xFFFF);
  hash += (unsigned short)port;

  hash = hash % pObj->maxSessions;

  return hash;
}

void *taosAddIpHash(void *handle, void *data, uint32_t ip, short port) {
  int       hash;
  SIpHash * pNode;
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  hash = taosHashIp(pObj, ip, port);
  pNode = (SIpHash *)taosMemPoolMalloc(pObj->ipHashMemPool);
  pNode->ip = ip;
  pNode->port = port;
  pNode->data = data;
  pNode->prev = 0;
  pNode->next = pObj->ipHashList[hash];
  pNode->hash = hash;

  if (pObj->ipHashList[hash] != 0) (pObj->ipHashList[hash])->prev = pNode;
  pObj->ipHashList[hash] = pNode;

  return pObj;
}

void taosDeleteIpHash(void *handle, uint32_t ip, short port) {
  int       hash;
  SIpHash * pNode;
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  hash = taosHashIp(pObj, ip, port);

  pNode = pObj->ipHashList[hash];
  while (pNode) {
    if (pNode->ip == ip && pNode->port == port) break;

    pNode = pNode->next;
  }

  if (pNode) {
    if (pNode->prev) {
      pNode->prev->next = pNode->next;
    } else {
      pObj->ipHashList[hash] = pNode->next;
    }

    if (pNode->next) {
      pNode->next->prev = pNode->prev;
    }

    taosMemPoolFree(pObj->ipHashMemPool, (char *)pNode);
  }
}

void *taosGetIpHash(void *handle, uint32_t ip, short port) {
  int       hash;
  SIpHash * pNode;
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  hash = taosHashIp(pObj, ip, port);
  pNode = pObj->ipHashList[hash];

  while (pNode) {
    if (pNode->ip == ip && pNode->port == port) {
      break;
    }
    pNode = pNode->next;
  }

  if (pNode) {
    return pNode->data;
  }
  return NULL;
}

void *taosOpenIpHash(int maxSessions) {
  SIpHash **ipHashList;
  mpool_h   ipHashMemPool;
  SHashObj *pObj;

  ipHashMemPool = taosMemPoolInit(maxSessions, sizeof(SIpHash));
  if (ipHashMemPool == 0) return NULL;

  ipHashList = calloc(sizeof(SIpHash *), (size_t)maxSessions);
  if (ipHashList == 0) {
    taosMemPoolCleanUp(ipHashMemPool);
    return NULL;
  }

  pObj = malloc(sizeof(SHashObj));
  if (pObj == NULL) {
    taosMemPoolCleanUp(ipHashMemPool);
    free(ipHashList);
    return NULL;
  }

  pObj->maxSessions = maxSessions;
  pObj->ipHashMemPool = ipHashMemPool;
  pObj->ipHashList = ipHashList;

  return pObj;
}

void taosCloseIpHash(void *handle) {
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  if (pObj->ipHashMemPool) taosMemPoolCleanUp(pObj->ipHashMemPool);

  if (pObj->ipHashList) free(pObj->ipHashList);

  memset(pObj, 0, sizeof(SHashObj));
  free(pObj);
}
