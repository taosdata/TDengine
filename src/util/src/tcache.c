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
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "tcache.h"
#include "hash.h"
#include "hashfunc.h"

static FORCE_INLINE void __cache_wr_lock(SCacheObj *pCacheObj) {
#if defined(LINUX)
  pthread_rwlock_wrlock(&pCacheObj->lock);
#else
  pthread_mutex_lock(&pCacheObj->lock);
#endif
}

static FORCE_INLINE void __cache_rd_lock(SCacheObj *pCacheObj) {
#if defined(LINUX)
  pthread_rwlock_rdlock(&pCacheObj->lock);
#else
  pthread_mutex_lock(&pCacheObj->lock);
#endif
}

static FORCE_INLINE void __cache_unlock(SCacheObj *pCacheObj) {
#if defined(LINUX)
  pthread_rwlock_unlock(&pCacheObj->lock);
#else
  pthread_mutex_unlock(&pCacheObj->lock);
#endif
}

static FORCE_INLINE int32_t __cache_lock_init(SCacheObj *pCacheObj) {
#if defined(LINUX)
  return pthread_rwlock_init(&pCacheObj->lock, NULL);
#else
  return pthread_mutex_init(&pCacheObj->lock, NULL);
#endif
}

static FORCE_INLINE void __cache_lock_destroy(SCacheObj *pCacheObj) {
#if defined(LINUX)
  pthread_rwlock_destroy(&pCacheObj->lock);
#else
  pthread_mutex_destroy(&pCacheObj->lock);
#endif
}

#if 0
static FORCE_INLINE void taosFreeNode(void *data) {
  SCacheDataNode *pNode = *(SCacheDataNode **)data;
  free(pNode);
}
#endif

/**
 * @param key      key of object for hash, usually a null-terminated string
 * @param keyLen   length of key
 * @param pData    actually data. required a consecutive memory block, no pointer is allowed
 *                 in pData. Pointer copy causes memory access error.
 * @param size     size of block
 * @param lifespan total survial expiredTime from now
 * @return         SCacheDataNode
 */
static SCacheDataNode *taosCreateCacheNode(const char *key, size_t keyLen, const char *pData, size_t size, uint64_t duration);

/**
 * addedTime object node into trash, and this object is closed for referencing if it is addedTime to trash
 * It will be removed until the pNode->refCount == 0
 * @param pCacheObj    Cache object
 * @param pNode   Cache slot object
 */
static void taosAddToTrash(SCacheObj *pCacheObj, SCacheDataNode *pNode);

/**
 * remove node in trash can
 * @param pCacheObj 
 * @param pElem 
 */
static void taosRemoveFromTrashCan(SCacheObj *pCacheObj, STrashElem *pElem);

/**
 * remove nodes in trash with refCount == 0 in cache
 * @param pNode
 * @param pCacheObj
 * @param force   force model, if true, remove data in trash without check refcount.
 *                may cause corruption. So, forece model only applys before cache is closed
 */
static void taosTrashCanEmpty(SCacheObj *pCacheObj, bool force);

/**
 * release node
 * @param pCacheObj      cache object
 * @param pNode     data node
 */
static FORCE_INLINE void taosCacheReleaseNode(SCacheObj *pCacheObj, SCacheDataNode *pNode) {
  if (pNode->signature != (uint64_t)pNode) {
    uError("key:%s, %p data is invalid, or has been released", pNode->key, pNode);
    return;
  }
  
  int32_t size = pNode->size;
  taosHashRemove(pCacheObj->pHashTable, pNode->key, pNode->keySize);
  
  uDebug("key:%p, is removed from cache,total:%" PRId64 ",size:%dbytes", pNode->key, pCacheObj->totalSize, size);
  if (pCacheObj->freeFp) pCacheObj->freeFp(pNode->data);
  free(pNode);
}

/**
 * move the old node into trash
 * @param pCacheObj
 * @param pNode
 */
static FORCE_INLINE void taosCacheMoveToTrash(SCacheObj *pCacheObj, SCacheDataNode *pNode) {
  taosHashRemove(pCacheObj->pHashTable, pNode->key, pNode->keySize);
  taosAddToTrash(pCacheObj, pNode);
}

/**
 * update data in cache
 * @param pCacheObj
 * @param pNode
 * @param key
 * @param keyLen
 * @param pData
 * @param dataSize
 * @return
 */
static SCacheDataNode *taosUpdateCacheImpl(SCacheObj *pCacheObj, SCacheDataNode *pNode, const char *key, int32_t keyLen,
                                           const void *pData, uint32_t dataSize, uint64_t duration) {
  SCacheDataNode *pNewNode = NULL;
  
  // only a node is not referenced by any other object, in-place update it
  if (T_REF_VAL_GET(pNode) == 0) {
    size_t newSize = sizeof(SCacheDataNode) + dataSize + keyLen + 1;
    
    pNewNode = (SCacheDataNode *)realloc(pNode, newSize);
    if (pNewNode == NULL) {
      return NULL;
    }
    
    memset(pNewNode, 0, newSize);
    pNewNode->signature = (uint64_t)pNewNode;
    memcpy(pNewNode->data, pData, dataSize);
    
    pNewNode->key = (char *)pNewNode + sizeof(SCacheDataNode) + dataSize;
    pNewNode->keySize = keyLen;
    memcpy(pNewNode->key, key, keyLen);
    
    // update the timestamp information for updated key/value
    pNewNode->addedTime = taosGetTimestampMs();
    pNewNode->lifespan = duration;
    
    T_REF_INC(pNewNode);
    
    // the address of this node may be changed, so the prev and next element should update the corresponding pointer
    taosHashPut(pCacheObj->pHashTable, key, keyLen, &pNewNode, sizeof(void *));
  } else {
    taosCacheMoveToTrash(pCacheObj, pNode);
    
    pNewNode = taosCreateCacheNode(key, keyLen, pData, dataSize, duration);
    if (pNewNode == NULL) {
      return NULL;
    }
    
    T_REF_INC(pNewNode);
    
    // addedTime new element to hashtable
    taosHashPut(pCacheObj->pHashTable, key, keyLen, &pNewNode, sizeof(void *));
  }
  
  return pNewNode;
}

/**
 * addedTime data into hash table
 * @param key
 * @param pData
 * @param size
 * @param pCacheObj
 * @param keyLen
 * @param pNode
 * @return
 */
static FORCE_INLINE SCacheDataNode *taosAddToCacheImpl(SCacheObj *pCacheObj, const char *key, size_t keyLen, const void *pData,
                                                       size_t dataSize, uint64_t duration) {
  SCacheDataNode *pNode = taosCreateCacheNode(key, keyLen, pData, dataSize, duration);
  if (pNode == NULL) {
    return NULL;
  }
  
  T_REF_INC(pNode);
  taosHashPut(pCacheObj->pHashTable, key, keyLen, &pNode, sizeof(void *));
  return pNode;
}

/**
 * do cleanup the taos cache
 * @param pCacheObj
 */
static void doCleanupDataCache(SCacheObj *pCacheObj);

/**
 * refresh cache to remove data in both hash list and trash, if any nodes' refcount == 0, every pCacheObj->refreshTime
 * @param handle   Cache object handle
 */
static void* taosCacheRefresh(void *handle);

SCacheObj *taosCacheInitWithCb(int32_t keyType, int64_t refreshTimeInSeconds, bool extendLifespan, __cache_freeres_fn_t fn) {
  if (refreshTimeInSeconds <= 0) {
    return NULL;
  }
  
  SCacheObj *pCacheObj = (SCacheObj *)calloc(1, sizeof(SCacheObj));
  if (pCacheObj == NULL) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }
  
  pCacheObj->pHashTable = taosHashInit(128, taosGetDefaultHashFunction(keyType), false);
  if (pCacheObj->pHashTable == NULL) {
    free(pCacheObj);
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }
  
  // set free cache node callback function for hash table
  pCacheObj->freeFp = fn;
  pCacheObj->refreshTime = refreshTimeInSeconds * 1000;
  pCacheObj->extendLifespan = extendLifespan;

  if (__cache_lock_init(pCacheObj) != 0) {
    taosHashCleanup(pCacheObj->pHashTable);
    free(pCacheObj);
    
    uError("failed to init lock, reason:%s", strerror(errno));
    return NULL;
  }

  pthread_attr_t thattr = {{0}};
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  pthread_create(&pCacheObj->refreshWorker, &thattr, taosCacheRefresh, pCacheObj);

  pthread_attr_destroy(&thattr);
  return pCacheObj;
}

SCacheObj *taosCacheInit(int32_t keyType, int64_t refreshTimeInSeconds, bool extendLifespan, __cache_freeres_fn_t fn) {
  return taosCacheInitWithCb(keyType, refreshTimeInSeconds, extendLifespan, fn);
}

void *taosCachePut(SCacheObj *pCacheObj, const void *key, size_t keyLen, const void *pData, size_t dataSize, int duration) {
  SCacheDataNode *pNode;
  
  if (pCacheObj == NULL || pCacheObj->pHashTable == NULL) {
    return NULL;
  }

  __cache_wr_lock(pCacheObj);
  SCacheDataNode **pt = (SCacheDataNode **)taosHashGet(pCacheObj->pHashTable, key, keyLen);
  SCacheDataNode * pOld = (pt != NULL) ? (*pt) : NULL;
  
  if (pOld == NULL) {  // do addedTime to cache
    pNode = taosAddToCacheImpl(pCacheObj, key, keyLen, pData, dataSize, duration * 1000L);
    if (NULL != pNode) {
      pCacheObj->totalSize += pNode->size;
      
      uDebug("key:%p, %p added into cache, added:%" PRIu64 ", expire:%" PRIu64 ", total:%" PRId64 ", size:%" PRId64 " bytes",
             key, pNode, pNode->addedTime, (pNode->lifespan * pNode->extendFactor + pNode->addedTime), pCacheObj->totalSize, dataSize);
    } else {
      uError("key:%p, failed to added into cache, out of memory", key);
    }
  } else {  // old data exists, update the node
    pNode = taosUpdateCacheImpl(pCacheObj, pOld, key, keyLen, pData, dataSize, duration * 1000L);
    uDebug("key:%p, %p exist in cache, updated", key, pNode);
  }
  
  __cache_unlock(pCacheObj);
  
  return (pNode != NULL) ? pNode->data : NULL;
}

void *taosCacheAcquireByKey(SCacheObj *pCacheObj, const void *key, size_t keyLen) {
  if (pCacheObj == NULL || taosHashGetSize(pCacheObj->pHashTable) == 0) {
    return NULL;
  }

  __cache_rd_lock(pCacheObj);
  
  SCacheDataNode **ptNode = (SCacheDataNode **)taosHashGet(pCacheObj->pHashTable, key, keyLen);

  int32_t ref = 0;
  if (ptNode != NULL) {
    ref = T_REF_INC(*ptNode);

    // if the remained life span is less then the (*ptNode)->lifeSpan, add up one lifespan
    if (pCacheObj->extendLifespan) {
      int64_t now = taosGetTimestampMs();

      if ((now - (*ptNode)->addedTime) < (*ptNode)->lifespan * (*ptNode)->extendFactor) {
        (*ptNode)->extendFactor += 1;
        uDebug("key:%p extend life time to %"PRId64, key, (*ptNode)->lifespan * (*ptNode)->extendFactor + (*ptNode)->addedTime);
      }
    }
  }
  __cache_unlock(pCacheObj);
  
  if (ptNode != NULL) {
    atomic_add_fetch_32(&pCacheObj->statistics.hitCount, 1);
    uDebug("key:%p, is retrieved from cache, %p refcnt:%d", key, (*ptNode), ref);
  } else {
    atomic_add_fetch_32(&pCacheObj->statistics.missCount, 1);
    uDebug("key:%p, not in cache, retrieved failed", key);
  }
  
  atomic_add_fetch_32(&pCacheObj->statistics.totalAccess, 1);
  return (ptNode != NULL) ? (*ptNode)->data : NULL;
}

void* taosCacheUpdateExpireTimeByName(SCacheObj *pCacheObj, const char *key, size_t keyLen, uint64_t expireTime) {
  if (pCacheObj == NULL || taosHashGetSize(pCacheObj->pHashTable) == 0) {
    return NULL;
  }

  __cache_rd_lock(pCacheObj);
  
  SCacheDataNode **ptNode = (SCacheDataNode **)taosHashGet(pCacheObj->pHashTable, key, keyLen);
  if (ptNode != NULL) {
     T_REF_INC(*ptNode);
    (*ptNode)->extendFactor += 1;
//    (*ptNode)->lifespan = expireTime;
  }
  
  __cache_unlock(pCacheObj);
  
  if (ptNode != NULL) {
    atomic_add_fetch_32(&pCacheObj->statistics.hitCount, 1);
    uDebug("key:%p, expireTime is updated in cache, %p refcnt:%d", key, (*ptNode), T_REF_VAL_GET(*ptNode));
  } else {
    atomic_add_fetch_32(&pCacheObj->statistics.missCount, 1);
    uDebug("key:%p, not in cache, retrieved failed", key);
  }
  
  atomic_add_fetch_32(&pCacheObj->statistics.totalAccess, 1);
  return (ptNode != NULL) ? (*ptNode)->data : NULL;
}

void *taosCacheAcquireByData(SCacheObj *pCacheObj, void *data) {
  if (pCacheObj == NULL || data == NULL) return NULL;
  
  size_t          offset = offsetof(SCacheDataNode, data);
  SCacheDataNode *ptNode = (SCacheDataNode *)((char *)data - offset);
  
  if (ptNode->signature != (uint64_t)ptNode) {
    uError("key: %p the data from cache is invalid", ptNode);
    return NULL;
  }
  
  int32_t ref = T_REF_INC(ptNode);
  uDebug("%p acquired by data in cache, refcnt:%d", ptNode, ref)

  // if the remained life span is less then the (*ptNode)->lifeSpan, add up one lifespan
  if (pCacheObj->extendLifespan) {
    int64_t now = taosGetTimestampMs();

    if ((now - ptNode->addedTime) < ptNode->lifespan * ptNode->extendFactor) {
      ptNode->extendFactor += 1;
      uDebug("key:%p extend life time to %"PRId64, ptNode, ptNode->lifespan * ptNode->extendFactor + ptNode->addedTime);
    }
  }

  // the data if referenced by at least one object, so the reference count must be greater than the value of 2.
  assert(ref >= 2);
  return data;
}

void *taosCacheTransfer(SCacheObj *pCacheObj, void **data) {
  if (pCacheObj == NULL || data == NULL) return NULL;
  
  size_t          offset = offsetof(SCacheDataNode, data);
  SCacheDataNode *ptNode = (SCacheDataNode *)((char *)(*data) - offset);
  
  if (ptNode->signature != (uint64_t)ptNode) {
    uError("key: %p the data from cache is invalid", ptNode);
    return NULL;
  }
  
  assert(T_REF_VAL_GET(ptNode) >= 1);
  
  char *d = *data;
  
  // clear its reference to old area
  *data = NULL;
  
  return d;
}

void taosCacheRelease(SCacheObj *pCacheObj, void **data, bool _remove) {
  if (pCacheObj == NULL || (*data) == NULL || (taosHashGetSize(pCacheObj->pHashTable) + pCacheObj->numOfElemsInTrash == 0)) {
    return;
  }
  
  size_t offset = offsetof(SCacheDataNode, data);
  
  SCacheDataNode *pNode = (SCacheDataNode *)((char *)(*data) - offset);
  if (pNode->signature != (uint64_t)pNode) {
    uError("key:%p, release invalid cache data", pNode);
    return;
  }
  
  *data = NULL;
  int16_t ref = T_REF_DEC(pNode);
  uDebug("%p data released, refcnt:%d", pNode, ref);
  
  if (_remove && (!pNode->inTrashCan)) {
    __cache_wr_lock(pCacheObj);

    if (T_REF_VAL_GET(pNode) == 0) {
      // remove directly, if not referenced by other users
      taosCacheReleaseNode(pCacheObj, pNode);
    } else {
      // pNode may be released immediately by other thread after the reference count of pNode is set to 0,
      // So we need to lock it in the first place.
      taosCacheMoveToTrash(pCacheObj, pNode);
    }

    __cache_unlock(pCacheObj);
  }
}

void taosCacheEmpty(SCacheObj *pCacheObj) {
  SHashMutableIterator *pIter = taosHashCreateIter(pCacheObj->pHashTable);
  
  __cache_wr_lock(pCacheObj);
  while (taosHashIterNext(pIter)) {
    if (pCacheObj->deleting == 1) {
      break;
    }
    
    SCacheDataNode *pNode = *(SCacheDataNode **)taosHashIterGet(pIter);
    taosCacheMoveToTrash(pCacheObj, pNode);
  }
  __cache_unlock(pCacheObj);
  
  taosHashDestroyIter(pIter);
  taosTrashCanEmpty(pCacheObj, false);
}

void taosCacheCleanup(SCacheObj *pCacheObj) {
  if (pCacheObj == NULL) {
    return;
  }

  pCacheObj->deleting = 1;
  pthread_join(pCacheObj->refreshWorker, NULL);

  doCleanupDataCache(pCacheObj);
}

SCacheDataNode *taosCreateCacheNode(const char *key, size_t keyLen, const char *pData, size_t size,
                                           uint64_t duration) {
  size_t totalSize = size + sizeof(SCacheDataNode) + keyLen + 1;

  SCacheDataNode *pNewNode = calloc(1, totalSize);
  if (pNewNode == NULL) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  memcpy(pNewNode->data, pData, size);

  pNewNode->key = (char *)pNewNode + sizeof(SCacheDataNode) + size;
  pNewNode->keySize = keyLen;

  memcpy(pNewNode->key, key, keyLen);

  pNewNode->addedTime    = (uint64_t)taosGetTimestampMs();
  pNewNode->lifespan     = duration;
  pNewNode->extendFactor = 1;
  pNewNode->signature    = (uint64_t)pNewNode;
  pNewNode->size         = (uint32_t)totalSize;

  return pNewNode;
}

void taosAddToTrash(SCacheObj *pCacheObj, SCacheDataNode *pNode) {
  if (pNode->inTrashCan) { /* node is already in trash */
    return;
  }

  STrashElem *pElem = calloc(1, sizeof(STrashElem));
  pElem->pData = pNode;

  pElem->next = pCacheObj->pTrash;
  if (pCacheObj->pTrash) {
    pCacheObj->pTrash->prev = pElem;
  }

  pElem->prev = NULL;
  pCacheObj->pTrash = pElem;

  pNode->inTrashCan = true;
  pCacheObj->numOfElemsInTrash++;

  uDebug("key:%p, %p move to trash, numOfElem in trash:%d", pNode->key, pNode, pCacheObj->numOfElemsInTrash);
}

void taosRemoveFromTrashCan(SCacheObj *pCacheObj, STrashElem *pElem) {
  if (pElem->pData->signature != (uint64_t)pElem->pData) {
    uError("key:sig:0x%" PRIx64 " %p data has been released, ignore", pElem->pData->signature, pElem->pData);
    return;
  }

  pCacheObj->numOfElemsInTrash--;
  if (pElem->prev) {
    pElem->prev->next = pElem->next;
  } else { /* pnode is the header, update header */
    pCacheObj->pTrash = pElem->next;
  }

  if (pElem->next) {
    pElem->next->prev = pElem->prev;
  }

  pElem->pData->signature = 0;
  if (pCacheObj->freeFp) {
    pCacheObj->freeFp(pElem->pData->data);
  }

  uError("-------------------free obj:%p", pElem->pData);
  free(pElem->pData);
  free(pElem);
}

void taosTrashCanEmpty(SCacheObj *pCacheObj, bool force) {
  __cache_wr_lock(pCacheObj);

  if (pCacheObj->numOfElemsInTrash == 0) {
    if (pCacheObj->pTrash != NULL) {
      uError("key:inconsistency data in cache, numOfElem in trash:%d", pCacheObj->numOfElemsInTrash);
    }
    pCacheObj->pTrash = NULL;

    __cache_unlock(pCacheObj);
    return;
  }

  STrashElem *pElem = pCacheObj->pTrash;

  while (pElem) {
    T_REF_VAL_CHECK(pElem->pData);
    if (pElem->next == pElem) {
      pElem->next = NULL;
    }

    if (force || (T_REF_VAL_GET(pElem->pData) == 0)) {
      uDebug("key:%p, %p removed from trash. numOfElem in trash:%d", pElem->pData->key, pElem->pData,
             pCacheObj->numOfElemsInTrash - 1);
      STrashElem *p = pElem;

      pElem = pElem->next;
      taosRemoveFromTrashCan(pCacheObj, p);
    } else {
      pElem = pElem->next;
    }
  }

  __cache_unlock(pCacheObj);
}

void doCleanupDataCache(SCacheObj *pCacheObj) {
  __cache_wr_lock(pCacheObj);

  SHashMutableIterator *pIter = taosHashCreateIter(pCacheObj->pHashTable);
  while (taosHashIterNext(pIter)) {
    SCacheDataNode *pNode = *(SCacheDataNode **)taosHashIterGet(pIter);
    // if (pNode->expiredTime <= expiredTime && T_REF_VAL_GET(pNode) <= 0) {
    if (T_REF_VAL_GET(pNode) <= 0) {
      taosCacheReleaseNode(pCacheObj, pNode);
    } else {
      uDebug("key:%p, will not remove from cache, refcnt:%d", pNode->key, T_REF_VAL_GET(pNode));
    }
  }
  taosHashDestroyIter(pIter);

  taosHashCleanup(pCacheObj->pHashTable); 
  __cache_unlock(pCacheObj);

  taosTrashCanEmpty(pCacheObj, true);
  __cache_lock_destroy(pCacheObj);

  memset(pCacheObj, 0, sizeof(SCacheObj));
  free(pCacheObj);
}

void* taosCacheRefresh(void *handle) {
  SCacheObj *pCacheObj = (SCacheObj *)handle;
  if (pCacheObj == NULL) {
    uDebug("object is destroyed. no refresh retry");
    return NULL;
  }

  const int32_t SLEEP_DURATION = 500; //500 ms
  int64_t totalTick = pCacheObj->refreshTime / SLEEP_DURATION;

  int64_t count = 0;
  while(1) {
    taosMsleep(500);

    // check if current cache object will be deleted every 500ms.
    if (pCacheObj->deleting) {
      break;
    }

    if (++count < totalTick) {
      continue;
    }

    // reset the count value
    count = 0;
    size_t elemInHash = taosHashGetSize(pCacheObj->pHashTable);
    if (elemInHash + pCacheObj->numOfElemsInTrash == 0) {
      continue;
    }

    pCacheObj->statistics.refreshCount++;

    // refresh data in hash table
    if (elemInHash > 0) {
      int64_t expiredTime = taosGetTimestampMs();

      SHashMutableIterator *pIter = taosHashCreateIter(pCacheObj->pHashTable);

      __cache_wr_lock(pCacheObj);
      while (taosHashIterNext(pIter)) {
        SCacheDataNode *pNode = *(SCacheDataNode **)taosHashIterGet(pIter);
        if ((pNode->addedTime + pNode->lifespan * pNode->extendFactor) <= expiredTime && T_REF_VAL_GET(pNode) <= 0) {
          taosCacheReleaseNode(pCacheObj, pNode);
        }
      }

      __cache_unlock(pCacheObj);

      taosHashDestroyIter(pIter);
    }

    taosTrashCanEmpty(pCacheObj, false);
  }

  return NULL;
}
