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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <errno.h>
#include <pthread.h>
#include <string.h>

#include "tcache.h"
#include "tlog.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

#define HASH_VALUE_IN_TRASH (-1)

/**
 * todo: refactor to extract the hash table out of cache structure
 */
typedef struct SCacheStatis {
  int64_t missCount;
  int64_t hitCount;
  int64_t totalAccess;
  int64_t refreshCount;
  int32_t numOfCollision;
} SCacheStatis;

typedef struct _cache_node_t {
  char *                key; /* null-terminated string */
  struct _cache_node_t *prev;
  struct _cache_node_t *next;
  uint64_t              addTime;  // the time when this element is added or updated into cache
  uint64_t              time;     // end time when this element should be remove from cache
  uint64_t              signature;

  /*
   * reference count for this object
   * if this value is larger than 0, this value will never be released
   */
  uint32_t refCount;
  int32_t  hashVal;  /* the hash value of key, if hashVal == HASH_VALUE_IN_TRASH, this node is moved to trash*/
  uint32_t nodeSize; /* allocated size for current SDataNode */
  char     data[];
} SDataNode;

typedef int (*_hashFunc)(int, char *, uint32_t);

typedef struct {
  SDataNode **hashList;
  int         maxSessions;
  int         total;

  int64_t totalSize; /* total allocated buffer in this hash table, SCacheObj is not included. */
  int64_t refreshTime;

  /*
   * to accommodate the old datanode which has the same key value of new one in hashList
   * when an new node is put into cache, if an existed one with the same key:
   * 1. if the old one does not be referenced, update it.
   * 2. otherwise, move the old one to pTrash, add the new one.
   *
   * when the node in pTrash does not be referenced, it will be release at the expired time
   */
  SDataNode *pTrash;
  int        numOfElemsInTrash; // number of element in trash

  void *tmrCtrl;
  void *pTimer;

  SCacheStatis statistics;
  _hashFunc    hashFp;

  /*
   * pthread_rwlock_t will block ops on the windows platform, when refresh is called.
   * so use pthread_mutex_t as an alternative
   */
#if defined LINUX
  pthread_rwlock_t lock;
#else
  pthread_mutex_t  mutex;
#endif

} SCacheObj;

static FORCE_INLINE int32_t taosNormalHashTableLength(int32_t length) {
  int32_t i = 4;
  while (i < length) i = (i << 1);
  return i;
}

/**
 * @param key      key of object for hash, usually a null-terminated string
 * @param keyLen   length of key
 * @param pData    actually data. required a consecutive memory block, no pointer is allowed
 *                 in pData. Pointer copy causes memory access error.
 * @param size     size of block
 * @param lifespan total survial time from now
 * @return         SDataNode
 */
static SDataNode *taosCreateHashNode(const char *key, uint32_t keyLen, const char *pData, size_t dataSize,
                                     uint64_t lifespan) {
  size_t totalSize = dataSize + sizeof(SDataNode) + keyLen;

  SDataNode *pNewNode = calloc(1, totalSize);
  if (pNewNode == NULL) {
    pError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  memcpy(pNewNode->data, pData, dataSize);

  pNewNode->addTime = (uint64_t) taosGetTimestampMs();
  pNewNode->time = pNewNode->addTime + lifespan;

  pNewNode->key = pNewNode->data + dataSize;
  strcpy(pNewNode->key, key);

  pNewNode->signature = (uint64_t)pNewNode;
  pNewNode->nodeSize = (uint32_t)totalSize;

  return pNewNode;
}

/**
 * hash key function
 * @param pObj   cache object
 * @param key    key string
 * @param len    length of key
 * @return       hash value
 */
static FORCE_INLINE int taosHashKey(int maxSessions, char *key, uint32_t len) {
  uint32_t hash = MurmurHash3_32(key, len);

  // avoid the costly remainder operation
  assert((maxSessions & (maxSessions - 1)) == 0);
  hash = hash & (maxSessions - 1);

  return hash;
}

/**
 * add object node into trash, and this object is closed for referencing if it is add to trash
 * It will be removed until the pNode->refCount == 0
 * @param pObj    Cache object
 * @param pNode   Cache slot object
 */
static void taosAddToTrash(SCacheObj *pObj, SDataNode *pNode) {
  if (pNode->hashVal == HASH_VALUE_IN_TRASH) {
    /* node is already in trash */
    return;
  }

  pNode->next = pObj->pTrash;
  if (pObj->pTrash) {
    pObj->pTrash->prev = pNode;
  }

  pNode->prev = NULL;
  pObj->pTrash = pNode;

  pNode->hashVal = HASH_VALUE_IN_TRASH;
  pObj->numOfElemsInTrash++;

  pTrace("key:%s %p move to trash, numOfElem in trash:%d", pNode->key, pNode, pObj->numOfElemsInTrash);
}

static void taosRemoveFromTrash(SCacheObj *pObj, SDataNode *pNode) {
  if (pNode->signature != (uint64_t)pNode) {
    pError("key:sig:%d %p data has been released, ignore", pNode->signature, pNode);
    return;
  }

  pObj->numOfElemsInTrash--;
  if (pNode->prev) {
    pNode->prev->next = pNode->next;
  } else {
    /* pnode is the header, update header */
    pObj->pTrash = pNode->next;
  }

  if (pNode->next) {
    pNode->next->prev = pNode->prev;
  }

  pNode->signature = 0;
  free(pNode);
}
/**
 * remove nodes in trash with refCount == 0 in cache
 * @param pNode
 * @param pObj
 * @param force   force model, if true, remove data in trash without check refcount.
 *                may cause corruption. So, forece model only applys before cache is closed
 */
static void taosClearCacheTrash(SCacheObj *pObj, _Bool force) {
#if defined LINUX
  pthread_rwlock_wrlock(&pObj->lock);
#else
  pthread_mutex_lock(&pObj->mutex);
#endif

  if (pObj->numOfElemsInTrash == 0) {
    if (pObj->pTrash != NULL) {
      pError("key:inconsistency data in cache, numOfElem in trash:%d", pObj->numOfElemsInTrash);
    }
    pObj->pTrash = NULL;
#if defined LINUX
    pthread_rwlock_unlock(&pObj->lock);
#else
    pthread_mutex_unlock(&pObj->mutex);
#endif    
    return;
  }

  SDataNode *pNode = pObj->pTrash;

  while (pNode) {
    if (pNode->refCount < 0) {
      pError("key:%s %p in trash released more than referenced, removed", pNode->key, pNode);
      pNode->refCount = 0;
    }

    if (pNode->next == pNode) {
      pNode->next = NULL;
    }

    if (force || (pNode->refCount == 0)) {
      pTrace("key:%s %p removed from trash. numOfElem in trash:%d", pNode->key, pNode, pObj->numOfElemsInTrash - 1)
      SDataNode *pTmp = pNode;
      pNode = pNode->next;
      taosRemoveFromTrash(pObj, pTmp);
    } else {
      pNode = pNode->next;
    }
  }

  assert(pObj->numOfElemsInTrash >= 0);
#if defined LINUX
  pthread_rwlock_unlock(&pObj->lock);
#else
  pthread_mutex_unlock(&pObj->mutex);
#endif
}

/**
 * add data node into cache
 * @param pObj    cache object
 * @param pNode   Cache slot object
 */
static void taosAddToHashTable(SCacheObj *pObj, SDataNode *pNode) {
  assert(pNode->hashVal >= 0);

  pNode->next = pObj->hashList[pNode->hashVal];

  if (pObj->hashList[pNode->hashVal] != 0) {
    (pObj->hashList[pNode->hashVal])->prev = pNode;
    pObj->statistics.numOfCollision++;
  }
  pObj->hashList[pNode->hashVal] = pNode;

  pObj->total++;
  pObj->totalSize += pNode->nodeSize;

  pTrace("key:%s %p add to hash table", pNode->key, pNode);
}

/**
 * remove node in hash list
 * @param pObj
 * @param pNode
 */
static void taosRemoveNodeInHashTable(SCacheObj *pObj, SDataNode *pNode) {
  if (pNode->hashVal == HASH_VALUE_IN_TRASH) return;

  SDataNode *pNext = pNode->next;
  if (pNode->prev) {
    pNode->prev->next = pNext;
  } else {
    /* the node is in hashlist, remove it */
    pObj->hashList[pNode->hashVal] = pNext;
  }

  if (pNext) {
    pNext->prev = pNode->prev;
  }

  pObj->total--;
  pObj->totalSize -= pNode->nodeSize;

  pNode->next = NULL;
  pNode->prev = NULL;

  pTrace("key:%s %p remove from hashtable", pNode->key, pNode);
}

/**
 * in-place node in hashlist
 * @param pObj      cache object
 * @param pNode     data node
 */
static void taosUpdateInHashTable(SCacheObj *pObj, SDataNode *pNode) {
  assert(pNode->hashVal >= 0);

  if (pNode->prev) {
    pNode->prev->next = pNode;
  } else {
    pObj->hashList[pNode->hashVal] = pNode;
  }

  if (pNode->next) {
    (pNode->next)->prev = pNode;
  }

  pTrace("key:%s %p update hashtable", pNode->key, pNode);
}

/**
 * get SDataNode from hashlist, nodes from trash are not included.
 * @param pObj      Cache objection
 * @param key       key for hash
 * @param keyLen    key length
 * @return
 */
static SDataNode *taosGetNodeFromHashTable(SCacheObj *pObj, char *key, uint32_t keyLen) {
  int hash = (*pObj->hashFp)(pObj->maxSessions, key, keyLen);

  SDataNode *pNode = pObj->hashList[hash];
  while (pNode) {
    if (strcmp(pNode->key, key) == 0) break;

    pNode = pNode->next;
  }

  if (pNode) {
    assert(pNode->hashVal == hash);
  }

  return pNode;
}

/**
 * release node
 * @param pObj      cache object
 * @param pNode     data node
 */
static FORCE_INLINE void taosCacheReleaseNode(SCacheObj *pObj, SDataNode *pNode) {
  taosRemoveNodeInHashTable(pObj, pNode);
  if (pNode->signature != (uint64_t)pNode) {
    pError("key:%s, %p data is invalid, or has been released", pNode->key, pNode);
    return;
  }

  pTrace("key:%s is removed from cache,total:%d,size:%ldbytes", pNode->key, pObj->total, pObj->totalSize);
  pNode->signature = 0;
  free(pNode);
}

/**
 * move the old node into trash
 * @param pObj
 * @param pNode
 */
static FORCE_INLINE void taosCacheMoveNodeToTrash(SCacheObj *pObj, SDataNode *pNode) {
  taosRemoveNodeInHashTable(pObj, pNode);
  taosAddToTrash(pObj, pNode);
}

/**
 * update data in cache
 * @param pObj
 * @param pNode
 * @param key
 * @param keyLen
 * @param pData
 * @param dataSize
 * @return
 */
static SDataNode *taosUpdateCacheImpl(SCacheObj *pObj, SDataNode *pNode, char *key, int32_t keyLen, void *pData,
                                      uint32_t dataSize, uint64_t keepTime) {
  SDataNode *pNewNode = NULL;

  /* only a node is not referenced by any other object, in-place update it */
  if (pNode->refCount == 0) {
    size_t newSize = sizeof(SDataNode) + dataSize + keyLen;

    pNewNode = (SDataNode *)realloc(pNode, newSize);
    if (pNewNode == NULL) {
      return NULL;
    }

    pNewNode->signature = (uint64_t)pNewNode;
    memcpy(pNewNode->data, pData, dataSize);

    pNewNode->key = pNewNode->data + dataSize;
    strcpy(pNewNode->key, key);

    __sync_add_and_fetch_32(&pNewNode->refCount, 1);
    taosUpdateInHashTable(pObj, pNewNode);
  } else {
    int32_t hashVal = pNode->hashVal;
    taosCacheMoveNodeToTrash(pObj, pNode);

    pNewNode = taosCreateHashNode(key, keyLen, pData, dataSize, keepTime);
    if (pNewNode == NULL) {
      return NULL;
    }

    __sync_add_and_fetch_32(&pNewNode->refCount, 1);

    assert(hashVal == (*pObj->hashFp)(pObj->maxSessions, key, keyLen - 1));
    pNewNode->hashVal = hashVal;

    /* add new one to hashtable */
    taosAddToHashTable(pObj, pNewNode);
  }

  return pNewNode;
}

/**
 * add data into hash table
 * @param key
 * @param pData
 * @param size
 * @param pObj
 * @param keyLen
 * @param pNode
 * @return
 */
static FORCE_INLINE SDataNode *taosAddToCacheImpl(SCacheObj *pObj, char *key, uint32_t keyLen, const char *pData,
                                                  int dataSize, uint64_t lifespan) {
  SDataNode *pNode = taosCreateHashNode(key, keyLen, pData, dataSize, lifespan);
  if (pNode == NULL) {
    return NULL;
  }

  __sync_add_and_fetch_32(&pNode->refCount, 1);
  pNode->hashVal = (*pObj->hashFp)(pObj->maxSessions, key, keyLen - 1);
  taosAddToHashTable(pObj, pNode);

  return pNode;
}

/**
 * add data into cache
 *
 * @param handle        cache object
 * @param key           key
 * @param pData         cached data
 * @param dataSize      data size
 * @param keepTime      survival time in second
 * @return              cached element
 */
void *taosAddDataIntoCache(void *handle, char *key, char *pData, int dataSize, int keepTime) {
  SDataNode *pNode;
  SCacheObj *pObj;

  pObj = (SCacheObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  uint32_t keyLen = (uint32_t)strlen(key) + 1;

#if defined LINUX
  pthread_rwlock_wrlock(&pObj->lock);
#else
  pthread_mutex_lock(&pObj->mutex);
#endif

  SDataNode *pOldNode = taosGetNodeFromHashTable(pObj, key, keyLen - 1);

  if (pOldNode == NULL) {  // do add to cache
    pNode = taosAddToCacheImpl(pObj, key, keyLen, pData, dataSize, keepTime * 1000L);
    pTrace("key:%s %p added into cache, slot:%d, addTime:%lld, expireTime:%lld, cache total:%d, "
           "size:%lldbytes, collision:%d", pNode->key, pNode, pNode->hashVal, pNode->addTime, pNode->time, pObj->total,
           pObj->totalSize, pObj->statistics.numOfCollision);
  } else {  // old data exists, update the node
    pNode = taosUpdateCacheImpl(pObj, pOldNode, key, keyLen, pData, dataSize, keepTime * 1000L);
    pTrace("key:%s %p exist in cache, updated", key, pNode);
  }

#if defined LINUX
  pthread_rwlock_unlock(&pObj->lock);
#else
  pthread_mutex_unlock(&pObj->mutex);
#endif

  return (pNode != NULL) ? pNode->data : NULL;
}

/**
 * remove data in cache, the data will not be removed immediately.
 * if it is referenced by other object, it will be remain in cache
 * @param handle
 * @param data
 */
void taosRemoveDataFromCache(void *handle, void **data, bool remove) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0 || (*data) == NULL || (pObj->total + pObj->numOfElemsInTrash == 0)) return;

  size_t     offset = offsetof(SDataNode, data);
  SDataNode *pNode = (SDataNode *)((char *)(*data) - offset);

  if (pNode->signature != (uint64_t)pNode) {
    pError("key: %p release invalid cache data", pNode);
    return;
  }

  if (pNode->refCount > 0) {
    __sync_add_and_fetch_32(&pNode->refCount, -1);
    pTrace("key:%s is released by app.refcnt:%d", pNode->key, pNode->refCount);
  } else {
    /*
     * safety check.
     * app may false releases cached object twice, to decrease the refcount more than acquired
     */
    pError("key:%s is released by app more than referenced.refcnt:%d", pNode->key, pNode->refCount);
  }

  *data = NULL;
  
  if (remove) {
#if defined LINUX
    pthread_rwlock_wrlock(&pObj->lock);
#else
    pthread_mutex_lock(&pObj->mutex);
#endif

    taosCacheMoveNodeToTrash(pObj, pNode);

#if defined LINUX
    pthread_rwlock_unlock(&pObj->lock);
#else
    pthread_mutex_unlock(&pObj->mutex);
#endif
  }
}

/**
 * get data from cache
 * @param handle        cache object
 * @param key           key
 * @return              cached data or NULL
 */
void *taosGetDataFromCache(void *handle, char *key) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  uint32_t keyLen = (uint32_t)strlen(key);

#if defined LINUX
  pthread_rwlock_rdlock(&pObj->lock);
#else
  pthread_mutex_lock(&pObj->mutex);
#endif

  SDataNode *ptNode = taosGetNodeFromHashTable(handle, key, keyLen);
  if (ptNode != NULL) {
    __sync_add_and_fetch_32(&ptNode->refCount, 1);
  }

#if defined LINUX
  pthread_rwlock_unlock(&pObj->lock);
#else
  pthread_mutex_unlock(&pObj->mutex);
#endif

  if (ptNode != NULL) {
    __sync_add_and_fetch_64(&pObj->statistics.hitCount, 1);

    pTrace("key:%s is retrieved from cache,refcnt:%d", key, ptNode->refCount);
  } else {
    __sync_add_and_fetch_64(&pObj->statistics.missCount, 1);
    pTrace("key:%s not in cache,retrieved failed", key);
  }
  __sync_add_and_fetch_64(&pObj->statistics.totalAccess, 1);
  return (ptNode != NULL) ? ptNode->data : NULL;
}

/**
 * update data in cache
 * @param handle hash object handle(pointer)
 * @param key    key for hash
 * @param pData  actually data
 * @param size   length of data
 * @return       new referenced data
 */
void *taosUpdateDataFromCache(void *handle, char *key, char *pData, int size, int duration) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  SDataNode *pNew = NULL;

  uint32_t keyLen = strlen(key) + 1;

#if defined LINUX
  pthread_rwlock_wrlock(&pObj->lock);
#else
  pthread_mutex_lock(&pObj->mutex);
#endif

  SDataNode *pNode = taosGetNodeFromHashTable(handle, key, keyLen - 1);
  if (pNode == NULL) {  // object has been released, do add operation
    pNew = taosAddToCacheImpl(pObj, key, keyLen, pData, size, duration * 1000L);
    pWarn("key:%s does not exist, update failed,do add to cache.total:%d,size:%ldbytes", key, pObj->total,
          pObj->totalSize);
  } else {
    pNew = taosUpdateCacheImpl(pObj, pNode, key, keyLen, pData, size, duration * 1000L);
    pTrace("key:%s updated.expireTime:%lld.refCnt:%d", key, pNode->time, pNode->refCount);
  }

#if defined LINUX
  pthread_rwlock_unlock(&pObj->lock);
#else
  pthread_mutex_unlock(&pObj->mutex);
#endif

  return (pNew != NULL) ? pNew->data : NULL;
}

/**
 * refresh cache to remove data in both hashlist and trash, if any nodes' refcount == 0, every pObj->refreshTime
 * @param handle   Cache object handle
 */
void taosRefreshDataCache(void *handle, void *tmrId) {
  SDataNode *pNode, *pNext;
  SCacheObj *pObj = (SCacheObj *)handle;

  if (pObj == NULL || (pObj->total == 0 && pObj->numOfElemsInTrash == 0)) {
    taosTmrReset(taosRefreshDataCache, pObj->refreshTime, pObj, pObj->tmrCtrl, &pObj->pTimer);
    return;
  }

  uint64_t time = taosGetTimestampMs();
  uint32_t numOfCheck = 0;
  pObj->statistics.refreshCount++;

  int32_t num = pObj->total;
  for (int hash = 0; hash < pObj->maxSessions; ++hash) {
#if defined LINUX
    pthread_rwlock_wrlock(&pObj->lock);
#else
    pthread_mutex_lock(&pObj->mutex);
#endif

    pNode = pObj->hashList[hash];

    while (pNode) {
      numOfCheck++;
      pNext = pNode->next;

      if (pNode->time <= time && pNode->refCount <= 0) {
        taosCacheReleaseNode(pObj, pNode);
      }
      pNode = pNext;
    }

    /* all data have been checked, not need to iterate further */
    if (numOfCheck == num || pObj->total <= 0) {
#if defined LINUX
      pthread_rwlock_unlock(&pObj->lock);
#else
      pthread_mutex_unlock(&pObj->mutex);
#endif
      break;
    }

#if defined LINUX
    pthread_rwlock_unlock(&pObj->lock);
#else
    pthread_mutex_unlock(&pObj->mutex);
#endif
  }

  taosClearCacheTrash(pObj, false);
  taosTmrReset(taosRefreshDataCache, pObj->refreshTime, pObj, pObj->tmrCtrl, &pObj->pTimer);
}

/**
 *
 * @param handle
 * @param tmrId
 */
void taosClearDataCache(void *handle) {
  SDataNode *pNode, *pNext;
  SCacheObj *pObj = (SCacheObj *)handle;

  for (int hash = 0; hash < pObj->maxSessions; ++hash) {
#if defined LINUX
    pthread_rwlock_wrlock(&pObj->lock);
#else
    pthread_mutex_lock(&pObj->mutex);
#endif

    pNode = pObj->hashList[hash];

    while (pNode) {
      pNext = pNode->next;
      taosCacheMoveNodeToTrash(pObj, pNode);
      pNode = pNext;
    }
#if defined LINUX
    pthread_rwlock_unlock(&pObj->lock);
#else
    pthread_mutex_unlock(&pObj->mutex);
#endif
  }

  taosClearCacheTrash(pObj, false);
}

/**
 *
 * @param maxSessions       maximum slots available for hash elements
 * @param tmrCtrl           timer ctrl
 * @param refreshTime       refresh operation interval time, the maximum survival time when one element is expired and
 *                          not referenced by other objects
 * @return
 */
void *taosInitDataCache(int maxSessions, void *tmrCtrl, int64_t refreshTime) {
  if (tmrCtrl == NULL || refreshTime <= 0 || maxSessions <= 0) {
    return NULL;
  }

  SCacheObj *pObj = (SCacheObj *)calloc(1, sizeof(SCacheObj));
  if (pObj == NULL) {
    pError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  pObj->maxSessions = taosNormalHashTableLength(maxSessions);

  pObj->hashFp = taosHashKey;
  pObj->refreshTime = refreshTime * 1000;

  pObj->hashList = (SDataNode **)calloc(1, sizeof(SDataNode *) * pObj->maxSessions);
  if (pObj->hashList == NULL) {
    free(pObj);
    pError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  pObj->tmrCtrl = tmrCtrl;
  taosTmrReset(taosRefreshDataCache, pObj->refreshTime, pObj, pObj->tmrCtrl, &pObj->pTimer);

#if defined LINUX
  if (pthread_rwlock_init(&pObj->lock, NULL) != 0) {
#else
  if (pthread_mutex_init(&pObj->mutex, NULL) != 0) {
#endif
    taosTmrStopA(&pObj->pTimer);
    free(pObj->hashList);
    free(pObj);

    pError("failed to init lock, reason:%s", strerror(errno));
    return NULL;
  }

  return (void *)pObj;
}

/**
 * release all allocated memory and destroy the cache object
 *
 * @param handle
 */
void taosCleanUpDataCache(void *handle) {
  SCacheObj *pObj;
  SDataNode *pNode, *pNext;

  pObj = (SCacheObj *)handle;
  if (pObj == NULL || pObj->maxSessions <= 0) {
#if defined LINUX
    pthread_rwlock_destroy(&pObj->lock);
#else
    pthread_mutex_destroy(&pObj->mutex);
#endif
    free(pObj);
    return;
  }

  taosTmrStopA(&pObj->pTimer);

#if defined LINUX
  pthread_rwlock_wrlock(&pObj->lock);
#else
  pthread_mutex_lock(&pObj->mutex);
#endif

  if (pObj->hashList && pObj->total > 0) {
    for (int i = 0; i < pObj->maxSessions; ++i) {
      pNode = pObj->hashList[i];
      while (pNode) {
        pNext = pNode->next;
        free(pNode);
        pNode = pNext;
      }
    }

    free(pObj->hashList);
  }

#if defined LINUX
  pthread_rwlock_unlock(&pObj->lock);
#else
  pthread_mutex_unlock(&pObj->mutex);
#endif

  taosClearCacheTrash(pObj, true);

#if defined LINUX
  pthread_rwlock_destroy(&pObj->lock);
#else
  pthread_mutex_destroy(&pObj->mutex);
#endif
  memset(pObj, 0, sizeof(SCacheObj));

  free(pObj);
}
