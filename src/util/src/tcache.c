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
#include "tlockfree.h"
#include "hashfunc.h"

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
  int32_t         size;           // hash module 
  int32_t         count;          // number of cached items 
  int32_t         iterNum;            
  int64_t         refreshTime;
  int             cacheId;
  int             state;          // 0: empty, 1: active;  2: deleted
  char            name[24];       // for debug purpose
  uint32_t      (*hashFp)(const char *, uint32_t);
  __cache_free_fn_t freeFp;
  SCacheStatis    statistics;
  SCacheNode    **nodeList;
  SRWLatch        olock;          // lock for cache obj
  SRWLatch       *nlock;          // lock for each node list
  tmr_h           pTimer;
  bool            extendLifespan; // auto extend life span when one item is accessed.
} SCacheObj;

#define pNodeFromData(data) ((SCacheNode *) ((char*)(data) - sizeof(SCacheNode)))
#define TAOS_CACHE_MAX_OBJS 100
#define TAOS_CACHE_MAX_SIZE (1024 * 1024 * 16)
#define TAOS_CACHE_LOAD_FACTOR (0.75)
#define TAOS_CACHE_STATE_EMPTY    0
#define TAOS_CACHE_STATE_ACTIVE   1
#define TAOS_CACHE_STATE_DELETING 2 
#define TAOS_CACHE_STATE_DELETED  3 


SCacheObj cacheObjList[TAOS_CACHE_MAX_OBJS];
static pthread_once_t  tsCacheModuleInit = PTHREAD_ONCE_INIT;
static pthread_mutex_t tsCacheMutex;
static int             tsCacheNextId = 0;
static void           *tsCacheTmrCtrl = NULL;
static int             tsCacheNum = 0;

static void taosCacheResize(SCacheObj *pCacheObj); 
static int  taosCacheReleaseNode(SCacheNode *pNode);
static void taosCacheProcessTimer(void *param, void *);
static void taosCacheResetObj(SCacheObj *pCacheObj);

static void taosInitCacheModule(void) {
  pthread_mutex_init(&tsCacheMutex, NULL);
}

int32_t taosCacheInit(int32_t keyType, int64_t refreshTimeInSeconds, bool extendLifespan,  __cache_free_fn_t fn, const char* cacheName) {
  pthread_once(&tsCacheModuleInit, taosInitCacheModule);

  int32_t cacheId = -1;
  int size = 100000;

  void *p1 = calloc(sizeof(SCacheNode *) * size, 1);
  void *p2 = (SRWLatch *) calloc(sizeof(SRWLatch) * size, 1);
  if (p1 == NULL || p2 == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    uError("cache:%s no enoug memory", cacheName);
    tfree(p1);
    tfree(p2);
    return -1;
  }

  pthread_mutex_lock(&tsCacheMutex);

  int i;
  for (i=0; i < TAOS_CACHE_MAX_OBJS; ++i) {
    tsCacheNextId = (tsCacheNextId + 1) % TAOS_CACHE_MAX_OBJS;
    if (cacheObjList[tsCacheNextId].state == TAOS_CACHE_STATE_EMPTY) break;
  }

  if (i >= TAOS_CACHE_MAX_OBJS) {
    terrno = TSDB_CODE_CACHE_TOO_MANY; 
    uError("cache:%s too many cache objs", cacheName);
    free(p1);
    free(p2);
  } else {
    cacheId = tsCacheNextId;
    SCacheObj *pCacheObj = cacheObjList + cacheId;

    strncpy(pCacheObj->name, cacheName, sizeof(pCacheObj->name)-1);
    pCacheObj->size = size;
    pCacheObj->refreshTime = refreshTimeInSeconds * 1000;
    pCacheObj->extendLifespan = extendLifespan;
    pCacheObj->cacheId = cacheId;  
    pCacheObj->hashFp = taosGetDefaultHashFunction(keyType);
    pCacheObj->freeFp = fn;
    pCacheObj->nodeList = p1;
    pCacheObj->nlock = p2;
    pCacheObj->state = TAOS_CACHE_STATE_ACTIVE;
 
    // start timer
    if (tsCacheNum == 0) 
      tsCacheTmrCtrl = taosTmrInit( TAOS_CACHE_MAX_OBJS+1, 1000, 1000000, "cache");
    pCacheObj->pTimer = taosTmrStart(taosCacheProcessTimer, pCacheObj->refreshTime, pCacheObj, tsCacheTmrCtrl);  
    cacheId = tsCacheNextId;
    tsCacheNum++;
  }


  pthread_mutex_unlock(&tsCacheMutex);

  uTrace("cache:%s is initialized, cacheId:%d total cache:%d", cacheName, cacheId, tsCacheNum);
  return cacheId;
}

void *taosCachePut(int32_t cacheId, const void *key, size_t keyLen, const void *pData, size_t dataSize, int durationMS) {
  if (cacheId < 0 || cacheId >= TAOS_CACHE_MAX_OBJS) {
    terrno = TSDB_CODE_CACHE_INVALID_ID;
    return NULL;
  }

  // create a new node
  size_t size = dataSize + sizeof(SCacheNode) + keyLen;
  SCacheNode *pNewNode = calloc(1, size);
  if (pNewNode == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    return NULL;
  } 

  SCacheObj *pCacheObj = cacheObjList + cacheId;
  if (pCacheObj->count * TAOS_CACHE_LOAD_FACTOR > pCacheObj->size && pCacheObj->iterNum <= 0) {
    taosWLockLatch(&pCacheObj->olock);
    taosCacheResize(pCacheObj);
    taosWUnLockLatch(&pCacheObj->olock);
  }
 
  int hash = (*pCacheObj->hashFp)(key, (uint32_t)keyLen) % pCacheObj->size;
  taosRLockLatch(&pCacheObj->olock);
  if (pCacheObj->state != TAOS_CACHE_STATE_ACTIVE) {
    terrno = TSDB_CODE_CACHE_NOT_EXIST;
    taosRUnLockLatch(&pCacheObj->olock);
    return NULL;
  }

  taosWLockLatch(pCacheObj->nlock + hash);

  SCacheNode *pNode = pCacheObj->nodeList[hash];
  while (pNode) {
    if (memcmp(pNode->key, key, keyLen) == 0) break;
    pNode = pNode->next;
  }
 
  // if key is already there, it means an update, remove the old
  if (pNode) taosCacheReleaseNode(pNode);

  pNode = pNewNode;
  memcpy(pNode->data, pData, dataSize);
  pNode->key = (char *)pNode + sizeof(SCacheNode) + dataSize;
  pNode->keyLen = (uint16_t)keyLen;
  memcpy(pNode->key, key, keyLen);

  pNode->hash = hash;
  pNode->expireTime   = (uint64_t)taosGetTimestampMs() + durationMS; 
  pNode->count = 2; 
  pNode->duration = durationMS;
  pNode->pCacheObj = pCacheObj;

  atomic_add_fetch_32(&pCacheObj->count, 1);

  // add into the head of the list, so app always acquires the new cached data
  pNode->next = pCacheObj->nodeList[hash];
  if (pCacheObj->nodeList[hash]) pCacheObj->nodeList[hash]->prev = pNode;
  pCacheObj->nodeList[hash] = pNode; 

  uTrace("cache:%s %p is added into cache, key:%p", pCacheObj->name, pNode->data, key);  

  taosWUnLockLatch(pCacheObj->nlock + hash);
  taosRUnLockLatch(&pCacheObj->olock);

  return pNode->data;
}

void *taosCacheAcquireByKey(int32_t cacheId, const void *key, size_t keyLen) {
  if (cacheId < 0 || cacheId >= TAOS_CACHE_MAX_OBJS) {
    terrno = TSDB_CODE_CACHE_INVALID_ID;
    return NULL;
  }

  SCacheObj *pCacheObj = cacheObjList + cacheId;
  int  hash = (*pCacheObj->hashFp)(key, (uint32_t)keyLen) % pCacheObj->size;
  void *p = NULL;

  taosRLockLatch(&pCacheObj->olock);
  if (pCacheObj->state != TAOS_CACHE_STATE_ACTIVE) {
    terrno = TSDB_CODE_CACHE_NOT_EXIST;
    taosRUnLockLatch(&pCacheObj->olock);
    return NULL;
  }
  
  taosRLockLatch(pCacheObj->nlock + hash);

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
    uTrace("cache:%s %p is acuqired via key, count:%d", pCacheObj->name, pNode->data, pNode->count);
  } else {
    terrno = TSDB_CODE_CACHE_KEY_NOT_THERE;
  }

  taosRUnLockLatch(pCacheObj->nlock + hash);
  taosRUnLockLatch(&pCacheObj->olock);
  return p;
}

void *taosCacheAcquireByData(void *data) {
  if (data == NULL) return NULL;
  SCacheNode *pNode = pNodeFromData(data); 
  SCacheObj  *pCacheObj = pNode->pCacheObj;
  int hash = pNode->hash;

  taosRLockLatch(&pCacheObj->olock);
  taosRLockLatch(pCacheObj->nlock + hash);

  pNode->count++;
  if (pCacheObj->extendLifespan)
    pNode->expireTime = (uint64_t)taosGetTimestampMs() + pNode->duration;

  uTrace("cache:%s %p is acuqired via data, count:%d", pCacheObj->name, pNode->data, pNode->count);

  taosRUnLockLatch(pCacheObj->nlock + hash);
  taosRUnLockLatch(&pCacheObj->olock);
  return data;
}

void taosCacheRelease(void *data) {
  if (data == NULL) return;  
  SCacheNode *pNode = pNodeFromData(data); 
  SCacheObj  *pCacheObj = pNode->pCacheObj;
  int hash = pNode->hash;

  taosRLockLatch(&pCacheObj->olock);
  taosWLockLatch(pCacheObj->nlock + hash);

  int count = taosCacheReleaseNode(pNode);

  taosWUnLockLatch(pCacheObj->nlock + hash);
  taosRUnLockLatch(&pCacheObj->olock);

  if (pCacheObj->state == TAOS_CACHE_STATE_DELETED && count == 0)
    taosCacheResetObj(pCacheObj); 
}

void *taosCacheTransfer(void **data) {
  if (data == NULL || *data == NULL) return NULL;

  void *d = *data;
  *data = NULL;

  return d;
}

/*
void taosCacheCleanup(int32_t cacheId) {
  if (cacheId < 0 || cacheId >= TAOS_CACHE_MAX_OBJS) {
    return;
  }

  SCacheObj *pCacheObj = cacheObjList + cacheId;

  pthread_mutex_lock(&tsCacheMutex);
  if (pCacheObj->state != TAOS_CACHE_STATE_ACTIVE) {
    pthread_mutex_unlock(&tsCacheMutex);
    return;
  }
  
  uTrace("cache:%s try to clean up", pCacheObj->name);

  // stop timer
  taosTmrStop(pCacheObj->pTimer);

  // write lock is applied. so from this point on, no actions will be allowed on pCacheObj from other threads
  taosWLockLatch(&pCacheObj->olock);

  for (int hash = 0; hash < pCacheObj->size; ++hash) {
    taosWLockLatch(pCacheObj->nlock + hash);
    SCacheNode *pNode = pCacheObj->nodeList[hash];

    while (pNode) {
      SCacheNode *pNext = pNode->next;
      if (pNode->expired == 0) {
        pNode->expired = 1;
        taosCacheReleaseNode(pNode);
      }
      pNode = pNext;
    }

    taosWUnLockLatch(pCacheObj->nlock + hash);
  }

  pCacheObj->state = TAOS_CACHE_STATE_DELETED;
  if (pCacheObj->count <= 0) 
    taosCacheResetObj(pCacheObj);

  taosWUnLockLatch(&pCacheObj->olock);
  pthread_mutex_unlock(&tsCacheMutex);
}
*/

void taosCacheCleanup(int32_t cacheId) {
  if (cacheId < 0 || cacheId >= TAOS_CACHE_MAX_OBJS) return; 
  SCacheObj *pCacheObj = cacheObjList + cacheId;

  pthread_mutex_lock(&tsCacheMutex);
  if (pCacheObj->state != TAOS_CACHE_STATE_ACTIVE) {
    pthread_mutex_unlock(&tsCacheMutex);
    return;
  }

  uTrace("cache:%s try to clean up", pCacheObj->name);
  pCacheObj->state = TAOS_CACHE_STATE_DELETING;
  taosTmrStop(pCacheObj->pTimer);

  void *p = taosCacheIterate(cacheId, NULL);
  while (p) {
    taosCacheRelease(p);
    p = taosCacheIterate(cacheId, p);
  }

  pCacheObj->state = TAOS_CACHE_STATE_DELETED;
  if (pCacheObj->count <= 0) 
    taosCacheResetObj(pCacheObj);

  pthread_mutex_unlock(&tsCacheMutex);
}

void *taosCacheIterate(int32_t cacheId, void *indata) {
  if (cacheId < 0 || cacheId >= TAOS_CACHE_MAX_OBJS) return NULL; 

  SCacheNode *pNode = NULL;
  SCacheNode *pNext = NULL;
  void       *outdata = NULL;
  int         hash = 0;
  SCacheObj  *pCacheObj = cacheObjList + cacheId;

  taosRLockLatch(&pCacheObj->olock);
  if (pCacheObj->state == TAOS_CACHE_STATE_EMPTY) {
    if (indata) atomic_sub_fetch_32(&pCacheObj->iterNum, 1);
    taosRUnLockLatch(&pCacheObj->olock);
    return NULL;
  }
 
  if (indata == NULL) {
    hash = 0;
  } else {
    pNode = pNodeFromData(indata);
    hash = pNode->hash;
  }

  while (hash < pCacheObj->size) {
    taosWLockLatch(pCacheObj->nlock + hash);
  
    if (pNode) { 
      pNext = pNode->next;
      taosCacheReleaseNode(pNode);
      pNode = NULL;
    } else {
      pNext = pCacheObj->nodeList[hash];
    }

    while (pNext) { // skip the expired ones
      if (pNext->expired == 0) break;
      pNext = pNext->next;
    }

    if (pNext) {  // acquire the node
      pNext->count++;  
      outdata = pNext->data;
    }

    taosWUnLockLatch(pCacheObj->nlock + hash);
    hash++;

    if (pNext) break;
  } 

  if (outdata) atomic_add_fetch_32(&pCacheObj->iterNum, 1); 
  if (indata) atomic_sub_fetch_32(&pCacheObj->iterNum, 1);

  taosRUnLockLatch(&pCacheObj->olock);

  uTrace("cache:%s in:%p out:%p is iterated, iterNum:%d", pCacheObj->name, indata, outdata, pCacheObj->iterNum);
  return outdata;
}

void taosCacheRefresh(int32_t cacheId, __cache_free_fn_t fp) {
  void *p = taosCacheIterate(cacheId, NULL);
  while (p) {
    fp(p);
    p = taosCacheIterate(cacheId, p);
  }  
}

void taosCacheEmpty(int32_t cacheId) {
  taosCacheRefresh(cacheId, taosCacheRelease);
}

int32_t taosCacheGetCount(int32_t cacheId) {
  if (cacheId < 0 || cacheId >= TAOS_CACHE_MAX_OBJS) return -1; 

  SCacheObj *pCacheObj = cacheObjList + cacheId;
  return pCacheObj->count;
}

static void taosCacheProcessTimer(void *param, void *tmrId) {
  int64_t cacheId = (int64_t) param;
  if (cacheId < 0 || cacheId >= TAOS_CACHE_MAX_OBJS) return;

  int64_t ctime = taosGetTimestampMs();
  pthread_mutex_lock(&tsCacheMutex);

  SCacheObj *pCacheObj = cacheObjList + cacheId;
  if (pCacheObj->state != TAOS_CACHE_STATE_ACTIVE) {
    pthread_mutex_unlock(&tsCacheMutex);
    return;
  }

  for (int hash = 0; hash < pCacheObj->size; ++hash) {
      taosRLockLatch(&pCacheObj->olock);
      taosWLockLatch(pCacheObj->nlock + hash);

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

      taosWUnLockLatch(pCacheObj->nlock + hash);
      taosRUnLockLatch(&pCacheObj->olock);
  }

  pCacheObj->pTimer = taosTmrStart(taosCacheProcessTimer, pCacheObj->refreshTime, pCacheObj, tsCacheTmrCtrl);  

  pthread_mutex_unlock(&tsCacheMutex);
}

static int taosCacheReleaseNode(SCacheNode *pNode) {
  SCacheObj *pCacheObj = pNode->pCacheObj;

  pNode->count--;
  uTrace("cache:%s %p is released, count:%d", pCacheObj->name, pNode->data, pNode->count);
  if (pNode->count > 0) return pNode->count;

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

  uTrace("cache:%s %p is removed from cache", pCacheObj->name, pNode->data);
  pCacheObj->freeFp(pNode->data);
  free(pNode);

  int count = atomic_sub_fetch_32(&pCacheObj->count, 1);
  return count;
}

static void taosCacheResize(SCacheObj *pCacheObj) {
  if (pCacheObj->iterNum > 0) return;
  if (pCacheObj->state != TAOS_CACHE_STATE_ACTIVE) return;

  int32_t newSize = pCacheObj->size << 1;
  if (newSize > TAOS_CACHE_MAX_SIZE) return;

  void *p1 = realloc(pCacheObj->nodeList, newSize * sizeof(SCacheNode *));
  if (p1 == NULL) return;

  void *p2 = realloc(pCacheObj->nlock, newSize * sizeof(SRWLatch));
  if (p2 == NULL) return;

  int64_t st = taosGetTimestampUs();

  pCacheObj->nodeList = (SCacheNode **)p1;
  pCacheObj->nlock = (SRWLatch *)p2;

  for (int hash=0; hash < pCacheObj->size; ++hash) {
    SCacheNode *pNode = pCacheObj->nodeList[hash];
    if (pNode == NULL) continue;

    while (pNode) {
      SCacheNode *pNext = pNode->next;
      int nhash = (*pCacheObj->hashFp)(pNode->key, pNode->keyLen) % newSize;
      if (hash != nhash) {
        // remove from current list 
        if (pNode->prev) {
          pNode->prev->next = pNode->next;
        } else {
          pCacheObj->nodeList[hash] = pNode->next;
        }

        if (pNode->next) pNode->next->prev = pNode->prev;
       
        // add into new list
        pNode->prev = NULL;
        pNode->next = pCacheObj->nodeList[nhash];
        if (pCacheObj->nodeList[nhash]) pCacheObj->nodeList[nhash] = pNode;
      }

      pNode = pNext;
    }
  }

  pCacheObj->size = newSize;

  int64_t et = taosGetTimestampUs();
  uDebug("cache:%s, hash table resized, new size:%d, elapsed time:%fms", pCacheObj->name, pCacheObj->size, (et - st)/1000.0); 
}
 
static void taosCacheResetObj(SCacheObj *pCacheObj) {
  if (pCacheObj->state == TAOS_CACHE_STATE_EMPTY) return;
  uTrace("cache:%s is cleaned, cacheId:%d", pCacheObj->name, pCacheObj->cacheId);

  free(pCacheObj->nodeList);
  free(pCacheObj->nlock);
  memset(pCacheObj, 0, sizeof(SCacheObj));
  pCacheObj->state = TAOS_CACHE_STATE_EMPTY;

  tsCacheNum--;
  if (tsCacheNum <= 0) {
    taosTmrCleanUp(tsCacheTmrCtrl);
    tsCacheTmrCtrl = NULL;
  }
}

