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
#include "os.h"
#include "tulog.h"
#include "ttimer.h"
#include "tutil.h"
#include "tcache.h"
#include "tref.h"

typedef struct SCacheStatis {
  int64_t missCount;
  int64_t hitCount;
  int64_t totalAccess;
  int64_t refreshCount;
} SCacheStatis;

typedef struct SCacheNode {
  uint64_t           expireTime;   // expire time
  uint64_t           signature;
  uint64_t           rid;          // reference ID
  int32_t            hash;         // hash value
  uint16_t           expired;      // expired state
  struct SCacheNode *prev;
  struct SCacheNode *next;
  struct SCacheObj  *pCacheObj;
  uint16_t           keyLen;       // max key size: 32kb
  char              *key;
  char               data[];
} SCacheNode;

typedef struct {
  int64_t         totalSize;          // total allocated buffer in this hash table, SCacheObj is not included.
  int64_t         refreshTime;
  int32_t         count;
  char            name[24];           // for debug purpose
  SCacheStatis    statistics;
  SCacheNode    **nodeList;
  int64_t        *lockedBy;
  uint8_t         deleting;           // set the deleting flag to stop refreshing ASAP.
  bool            extendLifespan;     // auto extend life span when one item is accessed.
} SCacheObj;

#define pNodeFromData(data) ((SCacheNode *) ((char*)(data) - sizeof(SCacheNode)))

void *taosCacheInit(int64_t refreshTimeInSeconds, bool extendLifespan, const char* cacheName) {
  int totalSize = 1000;
  int cacheObjSize = sizeof(SCacheObj);
  int nodeListSize = sizeof(SCacheNode *) * totalSize;
  int lockedBySize = sizeof(int64_t) * totalSize;

  char *p = calloc(cacheObjSize + nodeListSize + lockedBySize, 1);
  if (p == NULL) {
    terrno =
    uError("no enoug memory");
    return NULL;
  }

  SCacheObj *pCacheObj = (SCacheObj *)p;
  strncpy(pCacheObj->name, cacheName, sizeof(pCacheObj->cacheName)-1);
  pCacheObj->totalSize = totalSize;
  pCacheObj->refreshTime = refreshTimeInSeconds * 1000;
  pCacheObj->extendLifespan = extendLifespan;
  
  pCacheObj->nodeList = (SCacheNode **) (p + cacheObjSize); 
  pCacheObj->lockedBy = (int64_t *) (p + cacheObjSize + nodeListSize);
 
  // open reference 
  pCacheObj->rsetId = taosOpenRef(1000, taosCacheFreeNode);

  // start timer
  taosTmrStart(taosCacheProcessTimer, pCacheObj->refreshTime, pCacheObj, XXX);  

  return pCacheObj;
}

void *taosCachePut(void *param, const void *key, size_t keyLen, const void *pData, size_t dataSize, int durationMS) {
  SCacheObj *pCacheObj = param;
  if (pCacheObj == NULL || pCacheObj->deleting == 1) {
    terrno = 
    return NULL;
  }
  
  int hash = taosCacheHash(key, keyLen);
  taosCacheLock(&pCacheObj->lockedBy[hash]);

  SCacheNode *pNode = pCacheObj->nodeList[hash];
  while (pNode) {
    if (memcmp(pNode->key, key, keyLen) == 0) break;
    pNode = pNode->next;
  }
 
  // if key is already there, it means an update, remove the old
  if (pNode) taosRemoveRef(pNode->rid);

  // create a new node
  size_t totalSize = size + sizeof(SCacheNode) + keyLen;
  pNode = calloc(1, totalSize);
  if (pNode == NULL) {
    terrno =
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  memcpy(pNode->data, pData, size);
  pNode->key = (char *)pNode + sizeof(SCacheNode) + size;
  pNode->keyLen = (uint16_t)keyLen;
  memcpy(pNode->key, key, keyLen);

  pNode->hash = hash;
  pNode->expireTime   = (uint64_t)taosGetTimestampMs() + duration; 
  pNode->signature    = (uint64_t)pNode;
  pNode->rid = taosAddRef(pCacheObj->rsetId, pNode);
  pNode->pCacheObj = pCacheObj;

  atomic_add_fetch_32(&pCacheObj->count, 1);

  // add into the head of the list, so app always acquires the new cached data
  pNode->next = pCacheObj->nodeList[hash];
  if (pCacheObj->nodeList[hash]) pCacheObj->nodeList[hash].prev = pNode;
  pCacheObj->nodeList[hash] = pNode; 

  taosCacheUnlock(&pCacheObj->lockedBy[hash]);

  return pNode->data;
}

void *taosCacheAcquireByKey(void *param, const void *key, size_t keyLen) {
  SCacheObj *pCacheObj = param;
  if (pCacheObj == NULL || pCacheObj->deleting == 1) {
    terrno = 
    return NULL;
  }

  void *p = NULL;

  // Based on key and keyLen, find the node in hash list 
  int hash = taosCacheHash(key, keyLen);
  taosCacheLock(&pCacheObj->lockedBy[hash]);

  pNode = pCacheObj->nodeList[hash];
  while (pNode) {
    if (memcmp(pNode->key, key, keyLen) == 0) break;
    pNode = pNode->next;
  }
 
  if (pNode && pNode->expired == 0) {
    taosAcquireRef(pNode->rid);
    p = pNode->data;
    if (pCacheObj->extendLifespan)
      pNode->expireTime = (uint64_t)taosGetTimestampMs() + duration;
  } else {
    terrno = 
  }

  taosCacheUnlock(&pCacheObj->lockedBy[hash])

  return p;
}

void *taosCacheAcquireByData(void *param, void *data) {
  SCacheObj *pCacheObj = param;
  if (pCacheObj == NULL || data == NULL) {
    terrno = 
    return NULL;
  }
  
  SCacheDataNode *pNode = pNodeFromData(data); 
  
  if (pNode->signature != (uint64_t)pNode) { 
    uError("cache:%s, key: %p the data from cache is invalid", pCacheObj->name, pNode);
    terrno = 
    return NULL;
  }

  taosAcquireRef(pNode->rid);
  if (pCacheObj->extendLifespan)
    pNode->expireTime = (uint64_t)taosGetTimestampMs() + duration;

  return data;
}

int taosCacheRelease(void *param, void **data, bool _remove) {
  SCacheObj *CachepObj = param;
  if (pCacheObj == NULL || *data == NULL) {
    uError("invalid parameters", pCacheObj->name);
    terrno =
    return -1;
  }
  
  SCacheNode *pNode = pNodeFromData(*data);
  if (pNode->signature != (uint64_t)pNode) {
    uError("cache:%s, %p, release invalid cache data", pCacheObj->name, pNode);
    terrno = 
    return -1;
  }

  *data = NULL;
  taosReleaseRef(pCacheObj->rsetId, pNode->rid);
}

void taosCacheCleanup(void *param) {
  SCacheObj *pCacheObj = param;
  if (pCacheObj == NULL) return;
 
  // only set the flag, since pNode may be referenced, and timer call back may be called
  pCacheObj->deleting = 1;
}

void taosProcessCacheTimer(void *param) {
  SCacheObj *pCacheObj = param;

  int64_t ctime = taosGetTimestampMs();
  int     rsetId = pCacheObj->rsetId;      // pCacheObj may be released after the while loop
  uint8_t deleting = pCacheObj->deleting;  // pCacheObj may be released after the while loop
  
  SCacheNode *pNode = taosIterateRef(rsetId, 0);
  while (pNode) {
    int rid = pNode->rid;
    if (pNode->expireTime < ctime) {
      // if it is expired, remove the node 
      pNode->expired = 1;
      taosRemoveRef(rsetId, rid);
    }

    taosIterateRef(rsetId, rid);
  }

  if (deleting == 0) {
    taosTmrStart(taosCacheProcessTimer, pCacheObj->refreshTime, pCacheObj, XXX);  
    return;
  } 

  // if pCacheObj is deleted, iterate all nodes again
  SCacheNode *pNode = taosIterateRef(rsetId, 0);
  while (pNode) {
    int rid = pNode->rid;
    if (pNode->expired == 0) {
      pNode->expired = 1;
      taosRemoveRef(rsetId, rid);
    }

    taosIterateRef(rsetId, rid);
  }
}

static void taosCacheFreeNode(void *param) {
  SCacheNode *pNode = param;
  SCacheObj  *pCacheObj = pNode->pCacheObj;

  // remove from the list
  int hash = pNode->hash;
  taosCacheLock(&pCacheObj->lockedBy[hash]);

  if (pNode->prev) {
    pNode->prev->next = pNode->next;
  } else {
    pSet->nodeList[hash] = pNode->next;
  }

  if (pNode->next) {
    pNode->next->prev = pNode->prev;
  }

  taosCacheUnlock(&pCacheObj->lockedBy[hash]);

  free(pNode);

  int count = atomic_sub_fetch_32(&pCacheObj->count, 1);
  if (pCacheObj->deleting && count == 0) {
    // clean pCacheObj
    free (pCacheObj);
  }
}

static int taosCacheHash(char *key, int keyLen) {
  

}

static void taosCacheLock() {

}

static void taosCacheUnlock() {

}


