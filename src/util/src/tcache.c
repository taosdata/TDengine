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
#include "taoserror.h"

typedef struct SCacheStatis {
  int64_t missCount;
  int64_t hitCount;
  int64_t totalAccess;
  int64_t refreshCount;
} SCacheStatis;

typedef struct SCacheNode {
  uint64_t           expireTime;   // expire time
  int32_t            hash;         // hash value
  int32_t            duration;
  int32_t            count;
  uint16_t           expired;      // expired state
  struct SCacheNode *prev;
  struct SCacheNode *next;
  struct SCacheObj  *pCacheObj;
  uint16_t           keyLen;       // max key size: 32kb
  char              *key;
  char               data[];
} SCacheNode;

typedef struct SCacheObj {
  int64_t         totalSize;          // total allocated buffer in this hash table, SCacheObj is not included.
  int64_t         refreshTime;
  int32_t         count;
  int             cacheId;
  char            name[24];           // for debug purpose
  SCacheStatis    statistics;
  SCacheNode    **nodeList;
  int64_t        *lockedBy;
  tmr_h           pTimer;
  uint8_t         deleting;           // set the deleting flag to stop refreshing ASAP.
  bool            extendLifespan;     // auto extend life span when one item is accessed.
} SCacheObj;

#define pNodeFromData(data) ((SCacheNode *) ((char*)(data) - sizeof(SCacheNode)))

#define MAX_CACHE_OBJS 5
SCacheObj *cacheObjList[MAX_CACHE_OBJS];
static pthread_once_t  tsCacheModuleInit = PTHREAD_ONCE_INIT;
static pthread_mutex_t tsCacheMutex;
static int             tsCacheNextId = 0;

static int  taosCacheHash(const void *key, int keyLen);
static void taosCacheLock(int64_t *lock);
static void taosCacheUnlock(int64_t *lock);
static int  taosCacheReleaseNode(SCacheNode *pNode);
static void taosCacheProcessTimer(void *param, void *);

static void taosInitCacheModule(void) {
  pthread_mutex_init(&tsCacheMutex, NULL);
}

int taosCacheInit(int64_t refreshTimeInSeconds, bool extendLifespan, const char* cacheName) {
  pthread_once(&tsCacheModuleInit, taosInitCacheModule);

  int cacheId = -1;
  int totalSize = 1000;
  int cacheObjSize = sizeof(SCacheObj);
  int nodeListSize = sizeof(SCacheNode *) * totalSize;
  int lockedBySize = sizeof(int64_t) * totalSize;

  char *p = calloc(cacheObjSize + nodeListSize + lockedBySize, 1);
  if (p == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    uError("no enoug memory");
    return -1;
  }

  pthread_mutex_lock(&tsCacheMutex);

  int i;
  for (i=0; i < MAX_CACHE_OBJS; ++i) {
    tsCacheNextId = (tsCacheNextId + 1) % MAX_CACHE_OBJS;
    if (tsCacheNextId == 0) tsCacheNextId = 1;
    if (cacheObjList[tsCacheNextId] == NULL) break;
  }

  if (i >= MAX_CACHE_OBJS) {
    terrno = TSDB_CODE_CACHE_TOO_MANY; 
    uError("too many cache objs");
    free(p);
  } else {
    cacheId = tsCacheNextId;

    SCacheObj *pCacheObj = (SCacheObj *)p;
    strncpy(pCacheObj->name, cacheName, sizeof(pCacheObj->name)-1);
    pCacheObj->totalSize = totalSize;
    pCacheObj->refreshTime = refreshTimeInSeconds * 1000;
    pCacheObj->extendLifespan = extendLifespan;
    pCacheObj->cacheId = cacheId;  

    pCacheObj->nodeList = (SCacheNode **) (p + cacheObjSize); 
    pCacheObj->lockedBy = (int64_t *) (p + cacheObjSize + nodeListSize);
 
    // start timer
    pCacheObj->pTimer = taosTmrStart(taosCacheProcessTimer, pCacheObj->refreshTime, pCacheObj, NULL);  
    cacheId = tsCacheNextId;
  }

  pthread_mutex_unlock(&tsCacheMutex);

  return cacheId;
}

void *taosCachePut(int cacheId, const void *key, size_t keyLen, const void *pData, size_t dataSize, int durationMS) {
  if (cacheId < 0 || cacheId >= MAX_CACHE_OBJS) {
    terrno = TSDB_CODE_CACHE_INVALID_ID;
    return NULL;
  }

  SCacheObj *pCacheObj = cacheObjList[cacheId];
  if (pCacheObj == NULL) {
    terrno = TSDB_CODE_CACHE_NOT_EXIST;
    return NULL;
  }
  
  int hash = taosCacheHash(key, keyLen);
  taosCacheLock(pCacheObj->lockedBy + hash);

  SCacheNode *pNode = pCacheObj->nodeList[hash];
  while (pNode) {
    if (memcmp(pNode->key, key, keyLen) == 0) break;
    pNode = pNode->next;
  }
 
  // if key is already there, it means an update, remove the old
  if (pNode) taosCacheReleaseNode(pNode);

  // create a new node
  size_t totalSize = dataSize + sizeof(SCacheNode) + keyLen;
  pNode = calloc(1, totalSize);
  if (pNode == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    uError("failed to allocate memory, reason:%s", strerror(errno));
  } else {
    memcpy(pNode->data, pData, dataSize);
    pNode->key = (char *)pNode + sizeof(SCacheNode) + dataSize;
    pNode->keyLen = (uint16_t)keyLen;
    memcpy(pNode->key, key, keyLen);

    pNode->hash = hash;
    pNode->expireTime   = (uint64_t)taosGetTimestampMs() + durationMS; 
    pNode->count = 1; 
    pNode->duration = durationMS;
    pNode->pCacheObj = pCacheObj;

    atomic_add_fetch_32(&pCacheObj->count, 1);

    // add into the head of the list, so app always acquires the new cached data
    pNode->next = pCacheObj->nodeList[hash];
    if (pCacheObj->nodeList[hash]) pCacheObj->nodeList[hash]->prev = pNode;
    pCacheObj->nodeList[hash] = pNode; 
  }

  taosCacheUnlock(pCacheObj->lockedBy + hash);

  return pNode->data;
}

void *taosCacheAcquireByKey(int cacheId, const void *key, size_t keyLen) {
  if (cacheId < 0 || cacheId >= MAX_CACHE_OBJS) {
    terrno = TSDB_CODE_CACHE_INVALID_ID;
    return NULL;
  }

  SCacheObj *pCacheObj = cacheObjList[cacheId];
  if (pCacheObj == NULL) {
    terrno = TSDB_CODE_CACHE_NOT_EXIST;
    return NULL;
  }
  
  void *p = NULL;

  // Based on key and keyLen, find the node in hash list 
  int hash = taosCacheHash(key, keyLen);
  taosCacheLock(pCacheObj->lockedBy + hash);

  SCacheNode *pNode = pCacheObj->nodeList[hash];
  while (pNode) {
    if (memcmp(pNode->key, key, keyLen) == 0) break;
    pNode = pNode->next;
  }
 
  if (pNode && pNode->expired == 0) {
    pNode->count++;
    p = pNode->data;
    if (pCacheObj->extendLifespan)
      pNode->expireTime = (uint64_t)taosGetTimestampMs() + pNode->duration;
  } else {
    terrno = TSDB_CODE_CACHE_KEY_NOT_THERE;
  }

  taosCacheUnlock(pCacheObj->lockedBy + hash);

  return p;
}

void *taosCacheAcquireByData(void *data) {
  SCacheNode *pNode = pNodeFromData(data); 
  SCacheObj  *pCacheObj = pNode->pCacheObj;
  int hash = pNode->hash;

  taosCacheLock(pCacheObj->lockedBy + hash);

  pNode->count++;
  if (pCacheObj->extendLifespan)
    pNode->expireTime = (uint64_t)taosGetTimestampMs() + pNode->duration;

  taosCacheUnlock(pCacheObj->lockedBy + hash);

  return data;
}

void taosCacheRelease(void **data) {
  if (*data == NULL) return;  

  SCacheNode *pNode = pNodeFromData(*data); 
  SCacheObj  *pCacheObj = pNode->pCacheObj;
  int hash = pNode->hash;
  *data = NULL;

  taosCacheLock(pCacheObj->lockedBy + hash);
  taosCacheReleaseNode(pNode);
  taosCacheUnlock(pCacheObj->lockedBy + hash);
}

void taosCacheCleanup(int cacheId) {
  if (cacheId < 0 || cacheId >= MAX_CACHE_OBJS) {
    return;
  }

  pthread_mutex_lock(&tsCacheMutex);

  SCacheObj *pCacheObj = cacheObjList[cacheId];
  if (pCacheObj == NULL) {
    pthread_mutex_unlock(&tsCacheMutex);
    return;
  }
  
  cacheObjList[cacheId] = NULL;
  pCacheObj->deleting = 1;

  // stop timer
  taosTmrStop(pCacheObj->pTimer);
  int count = 0;

  for (int hash = 0; hash < pCacheObj->totalSize; ++hash) {
    int64_t *lock = pCacheObj->lockedBy + hash;
    taosCacheLock(lock);

    SCacheNode *pNode = pCacheObj->nodeList[hash];
    while (pNode) {
      SCacheNode *pNext = pNode->next;
      if (pNode->expired == 0) {
        pNode->expired = 1;

        // pCacheObj may be freed
        count = taosCacheReleaseNode(pNode);
      }
      pNode = pNext;
    }

    if (count <= 0) break;
    taosCacheUnlock(lock);
  }

  pthread_mutex_unlock(&tsCacheMutex);
}

void taosCacheProcessTimer(void *param, void *tmrId) {
  int64_t cacheId = (int64_t) param;
  if (cacheId < 0 || cacheId >= MAX_CACHE_OBJS) return;

  int64_t ctime = taosGetTimestampMs();
  pthread_mutex_lock(&tsCacheMutex);

  SCacheObj *pCacheObj = cacheObjList[cacheId];
  if (pCacheObj == NULL) {
    pthread_mutex_unlock(&tsCacheMutex);
    return;
  }

  for (int hash = 0; hash < pCacheObj->totalSize; ++hash) {
      int64_t *lock = pCacheObj->lockedBy + hash;
      taosCacheLock(lock);

      SCacheNode *pNode = pCacheObj->nodeList[hash];
      while (pNode) {
        SCacheNode *pNext = pNode->next;
        if (pNode->expireTime < ctime) {
          // if it is expired, remove the node 
          pNode->expired = 1;
          taosCacheReleaseNode(pNode);
        }
        pNode= pNext;
      }  

      taosCacheUnlock(lock);
  }

  pCacheObj->pTimer = taosTmrStart(taosCacheProcessTimer, pCacheObj->refreshTime, pCacheObj, NULL);  

  pthread_mutex_unlock(&tsCacheMutex);
}

void *taosCacheIterate(int cacheId, void *indata) {
  if (cacheId < 0 || cacheId >= MAX_CACHE_OBJS) {
    return NULL;
  }

  pthread_mutex_lock(&tsCacheMutex);

  SCacheObj *pCacheObj = cacheObjList[cacheId];
  if (pCacheObj == NULL) {
    pthread_mutex_unlock(&tsCacheMutex);
    return NULL;
  }
  
  SCacheNode *pNode, *pNext = NULL;
  void *outdata = NULL;
  int  hash = 0;

  if (indata == NULL) {
    hash = 0;
  } else {
    pNode = pNodeFromData(indata);
    hash = pNode->hash;
  }

  while (hash < pCacheObj->totalSize) {
    int64_t *lock = pCacheObj->lockedBy + hash;
    taosCacheLock(lock);
  
    if (pNode) { 
      pNext = pNode->next;
      taosCacheReleaseNode(pNode);
    } else {
      pNext = pCacheObj->nodeList[hash];
    }

    if (pNext) { 
      pNext->count++;  // including the expired ones
      outdata = pNext->data;
    }

    taosCacheUnlock(lock);
    hash++;

    if (pNext) break;
  } 

  pthread_mutex_unlock(&tsCacheMutex);
  return outdata;
}

static int taosCacheReleaseNode(SCacheNode *pNode) {
  SCacheObj *pCacheObj = pNode->pCacheObj;

  pNode->count--;
  if (pNode->count > 0) return pCacheObj->count;

  // remove from the list
  int hash = pNode->hash;

  if (pNode->prev) {
    pNode->prev->next = pNode->next;
  } else {
    pCacheObj->nodeList[hash] = pNode->next;
  }

  if (pNode->next) {
    pNode->next->prev = pNode->prev;
  }

  free(pNode);

  int count = atomic_sub_fetch_32(&pCacheObj->count, 1);
  if (pCacheObj->deleting && count == 0) {
    // clean pCacheObj
    free (pCacheObj);
  }

  return count;
}

static int taosCacheHash(const void *key, int keyLen) {
  return 0;  
}

static void taosCacheLock(int64_t *lock) {

}

static void taosCacheUnlock(int64_t *lock) {

}


