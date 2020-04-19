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
#include "tmempool.h"
#include "rpcLog.h"

typedef struct SIpHash {
  uint32_t           ip;
  uint16_t           port;
  int                hash;
  struct SIpHash    *prev;
  struct SIpHash    *next;
  void              *data;
} SIpHash;

typedef struct {
  SIpHash **ipHashList;
  mpool_h   ipHashMemPool;
  int       maxSessions;
} SHashObj;

int rpcHashIp(void *handle, uint32_t ip, uint16_t port) {
  SHashObj *pObj = (SHashObj *)handle;
  int       hash = 0;

  hash = (int)(ip >> 16);
  hash += (unsigned short)(ip & 0xFFFF);
  hash += port;

  hash = hash % pObj->maxSessions;

  return hash;
}

void *rpcAddIpHash(void *handle, void *data, uint32_t ip, uint16_t port) {
  int       hash;
  SIpHash  *pNode;
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  hash = rpcHashIp(pObj, ip, port);
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

void rpcDeleteIpHash(void *handle, uint32_t ip, uint16_t port) {
  int       hash;
  SIpHash  *pNode;
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  hash = rpcHashIp(pObj, ip, port);

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

void *rpcGetIpHash(void *handle, uint32_t ip, uint16_t port) {
  int       hash;
  SIpHash  *pNode;
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  hash = rpcHashIp(pObj, ip, port);
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

void *rpcOpenIpHash(int maxSessions) {
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

void rpcCloseIpHash(void *handle) {
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  if (pObj->ipHashMemPool) taosMemPoolCleanUp(pObj->ipHashMemPool);

  if (pObj->ipHashList) free(pObj->ipHashList);

  memset(pObj, 0, sizeof(SHashObj));
  free(pObj);
}
