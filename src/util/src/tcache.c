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
#include "hash.h"
#include "hashfunc.h"

static FORCE_INLINE void __cache_wr_lock(SCacheObj *pCacheObj) {
#if defined(LINUX)
  pthread_rwlock_wrlock(&pCacheObj->lock);
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
static void taosAddToTrashcan(SCacheObj *pCacheObj, SCacheDataNode *pNode);

/**
 * remove nodes in trash with refCount == 0 in cache
 * @param pNode
 * @param pCacheObj
 * @param force   force model, if true, remove data in trash without check refcount.
 *                may cause corruption. So, forece model only applys before cache is closed
 */
static void taosTrashcanEmpty(SCacheObj *pCacheObj, bool force);

/**
 * release node
 * @param pCacheObj      cache object
 * @param pNode          data node
 */
static FORCE_INLINE void taosCacheReleaseNode(SCacheObj *pCacheObj, SCacheDataNode *pNode) {
  if (pNode->signature != (uint64_t)pNode) {
    uError("key:%s, %p data is invalid, or has been released", pNode->key, pNode);
    return;
  }

  pCacheObj->totalSize -= pNode->size;
  int32_t size = (int32_t)taosHashGetSize(pCacheObj->pHashTable);
  assert(size > 0);

  uDebug("cache:%s, key:%p, %p is destroyed from cache, size:%dbytes, totalNum:%d size:%" PRId64 "bytes",
         pCacheObj->name, pNode->key, pNode->data, pNode->size, size - 1, pCacheObj->totalSize);

  if (pCacheObj->freeFp) {
    pCacheObj->freeFp(pNode->data);
  }

  free(pNode);
}

static FORCE_INLINE STrashElem* doRemoveElemInTrashcan(SCacheObj* pCacheObj, STrashElem *pElem) {
  if (pElem->pData->signature != (uint64_t) pElem->pData) {
    uWarn("key:sig:0x%" PRIx64 " %p data has been released, ignore", pElem->pData->signature, pElem->pData);
    return NULL;
  }

  STrashElem* next = pElem->next;

  pCacheObj->numOfElemsInTrash--;
  if (pElem->prev) {
    pElem->prev->next = pElem->next;
  } else { // pnode is the header, update header
    pCacheObj->pTrash = pElem->next;
  }

  if (next) {
    next->prev = pElem->prev;
  }

  if (pCacheObj->numOfElemsInTrash == 0) {
    assert(pCacheObj->pTrash == NULL);
  }

  return next;
}

static FORCE_INLINE void doDestroyTrashcanElem(SCacheObj* pCacheObj, STrashElem *pElem) {
  if (pCacheObj->freeFp) {
    pCacheObj->freeFp(pElem->pData->data);
  }

  free(pElem->pData);
  free(pElem);
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
static void* taosCacheTimedRefresh(void *handle);

SCacheObj *taosCacheInit(int32_t keyType, int64_t refreshTimeInSeconds, bool extendLifespan, __cache_free_fn_t fn, const char* cacheName) {
  if (refreshTimeInSeconds <= 0) {
    return NULL;
  }
  
  SCacheObj *pCacheObj = (SCacheObj *)calloc(1, sizeof(SCacheObj));
  if (pCacheObj == NULL) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }
  
  pCacheObj->pHashTable = taosHashInit(4096, taosGetDefaultHashFunction(keyType), false, HASH_ENTRY_LOCK);
  pCacheObj->name = strdup(cacheName);
  if (pCacheObj->pHashTable == NULL) {
    free(pCacheObj);
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }
  
  // set free cache node callback function
  pCacheObj->freeFp = fn;
  pCacheObj->refreshTime = refreshTimeInSeconds * 1000;
  pCacheObj->extendLifespan = extendLifespan;

  if (__cache_lock_init(pCacheObj) != 0) {
    taosHashCleanup(pCacheObj->pHashTable);
    free(pCacheObj);
    
    uError("failed to init lock, reason:%s", strerror(errno));
    return NULL;
  }

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  pthread_create(&pCacheObj->refreshWorker, &thattr, taosCacheTimedRefresh, pCacheObj);

  pthread_attr_destroy(&thattr);
  return pCacheObj;
}

void *taosCachePut(SCacheObj *pCacheObj, const void *key, size_t keyLen, const void *pData, size_t dataSize, int durationMS) {
  if (pCacheObj == NULL || pCacheObj->pHashTable == NULL || pCacheObj->deleting == 1) {
    return NULL;
  }

  SCacheDataNode *pNode1 = taosCreateCacheNode(key, keyLen, pData, dataSize, durationMS);
  if (pNode1 == NULL) {
    uError("cache:%s, key:%p, failed to added into cache, out of memory", pCacheObj->name, key);
    return NULL;
  }

  T_REF_INC(pNode1);

  int32_t succ = taosHashPut(pCacheObj->pHashTable, key, keyLen, &pNode1, sizeof(void *));
  if (succ == 0) {
    atomic_add_fetch_64(&pCacheObj->totalSize, pNode1->size);
    uDebug("cache:%s, key:%p, %p added into cache, added:%" PRIu64 ", expire:%" PRIu64
           ", totalNum:%d totalSize:%" PRId64 "bytes size:%" PRId64 "bytes",
           pCacheObj->name, key, pNode1->data, pNode1->addedTime, pNode1->expireTime,
           (int32_t)taosHashGetSize(pCacheObj->pHashTable), pCacheObj->totalSize, (int64_t)dataSize);
  } else {  // duplicated key exists
    while (1) {
      SCacheDataNode* p = NULL;
      int32_t ret = taosHashRemoveWithData(pCacheObj->pHashTable, key, keyLen, (void*) &p, sizeof(void*));

      // add to trashcan
      if (ret == 0) {
        if (T_REF_VAL_GET(p) == 0) {
          if (pCacheObj->freeFp) {
            pCacheObj->freeFp(p->data);
          }

          tfree(p);
        } else {
          taosAddToTrashcan(pCacheObj, p);
          uDebug("cache:%s, key:%p, %p exist in cache, updated old:%p", pCacheObj->name, key, pNode1->data, p->data);
        }
      }

      assert(T_REF_VAL_GET(pNode1) == 1);

      ret = taosHashPut(pCacheObj->pHashTable, key, keyLen, &pNode1, sizeof(void *));
      if (ret == 0) {
        atomic_add_fetch_64(&pCacheObj->totalSize, pNode1->size);

        uDebug("cache:%s, key:%p, %p added into cache, added:%" PRIu64 ", expire:%" PRIu64
               ", totalNum:%d totalSize:%" PRId64 "bytes size:%" PRId64 "bytes",
               pCacheObj->name, key, pNode1->data, pNode1->addedTime, pNode1->expireTime,
               (int32_t)taosHashGetSize(pCacheObj->pHashTable), pCacheObj->totalSize, (int64_t)dataSize);

        return pNode1->data;

      } else {
        // failed, try again
      }
    }
  }

  return pNode1->data;
}

static void incRefFn(void* ptNode) {
  assert(ptNode != NULL);

  SCacheDataNode** p = (SCacheDataNode**) ptNode;
  assert(T_REF_VAL_GET(*p) >= 0);

  int32_t ret = T_REF_INC(*p);
  assert(ret > 0);
}

void *taosCacheAcquireByKey(SCacheObj *pCacheObj, const void *key, size_t keyLen) {
  if (pCacheObj == NULL || pCacheObj->deleting == 1) {
    return NULL;
  }

  if (taosHashGetSize(pCacheObj->pHashTable) == 0) {
    atomic_add_fetch_32(&pCacheObj->statistics.missCount, 1);
    return NULL;
  }

  SCacheDataNode* ptNode = NULL;
  taosHashGetCB(pCacheObj->pHashTable, key, keyLen, incRefFn, &ptNode, sizeof(void*));

  void* pData = (ptNode != NULL)? ptNode->data:NULL;

  if (pData != NULL) {
    atomic_add_fetch_32(&pCacheObj->statistics.hitCount, 1);
    uDebug("cache:%s, key:%p, %p is retrieved from cache, refcnt:%d", pCacheObj->name, key, pData, T_REF_VAL_GET(ptNode));
  } else {
    atomic_add_fetch_32(&pCacheObj->statistics.missCount, 1);
    uDebug("cache:%s, key:%p, not in cache, retrieved failed", pCacheObj->name, key);
  }

  atomic_add_fetch_32(&pCacheObj->statistics.totalAccess, 1);
  return pData;
}

void *taosCacheAcquireByData(SCacheObj *pCacheObj, void *data) {
  if (pCacheObj == NULL || data == NULL) return NULL;
  
  size_t          offset = offsetof(SCacheDataNode, data);
  SCacheDataNode *ptNode = (SCacheDataNode *)((char *)data - offset);
  
  if (ptNode->signature != (uint64_t)ptNode) {
    uError("cache:%s, key: %p the data from cache is invalid", pCacheObj->name, ptNode);
    return NULL;
  }

  int32_t ref = T_REF_INC(ptNode);
  uDebug("cache:%s, data: %p acquired by data in cache, refcnt:%d", pCacheObj->name, ptNode->data, ref);

  // the data if referenced by at least one object, so the reference count must be greater than the value of 2.
  assert(ref >= 2);
  return data;
}

void *taosCacheTransfer(SCacheObj *pCacheObj, void **data) {
  if (pCacheObj == NULL || data == NULL || (*data) == NULL) return NULL;
  
  size_t          offset = offsetof(SCacheDataNode, data);
  SCacheDataNode *ptNode = (SCacheDataNode *)((char *)(*data) - offset);
  
  if (ptNode->signature != (uint64_t)ptNode) {
    uError("cache:%s, key: %p the data from cache is invalid", pCacheObj->name, ptNode);
    return NULL;
  }
  
  assert(T_REF_VAL_GET(ptNode) >= 1);
  
  char *d = *data;
  
  // clear its reference to old area
  *data = NULL;
  
  return d;
}

void taosCacheRelease(SCacheObj *pCacheObj, void **data, bool _remove) {
  if (pCacheObj == NULL) {
    return;
  }

  if ((*data) == NULL) {
    uError("cache:%s, NULL data to release", pCacheObj->name);
    return;
  }


  // The operation of removal from hash table and addition to trashcan is not an atomic operation,
  // therefore the check for the empty of both the hash table and the trashcan has a race condition.
  // It happens when there is only one object in the cache, and two threads which has referenced this object
  // start to free the it simultaneously [TD-1569].
  size_t offset = offsetof(SCacheDataNode, data);
  
  SCacheDataNode *pNode = (SCacheDataNode *)((char *)(*data) - offset);
  if (pNode->signature != (uint64_t)pNode) {
    uError("cache:%s, %p, release invalid cache data", pCacheObj->name, pNode);
    return;
  }

  *data = NULL;

  // note: extend lifespan before dec ref count
  bool inTrashcan = pNode->inTrashcan;

  if (pCacheObj->extendLifespan && (!inTrashcan) && (!_remove)) {
    atomic_store_64(&pNode->expireTime, pNode->lifespan + taosGetTimestampMs());
    uDebug("cache:%s data:%p extend expire time: %"PRId64, pCacheObj->name, pNode->data, pNode->expireTime);
  }

  if (_remove) {
    // NOTE: once refcount is decrease, pNode may be freed by other thread immediately.
    char* key = pNode->key;
    char* d = pNode->data;

    int32_t ref = T_REF_VAL_GET(pNode);
    uDebug("cache:%s, key:%p, %p is released, refcnt:%d, in trashcan:%d", pCacheObj->name, key, d, ref - 1, inTrashcan);

    /*
     * If it is not referenced by other users, remove it immediately. Otherwise move this node to trashcan wait for all users
     * releasing this resources.
     *
     * NOTE: previous ref is 0, and current ref is still 0, remove it. If previous is not 0, there is another thread
     * that tries to do the same thing.
     */
    if (inTrashcan) {
      ref = T_REF_VAL_GET(pNode);

      if (ref == 1) {
        // If it is the last ref, remove it from trashcan linked-list first, and then destroy it.Otherwise, it may be
        // destroyed by refresh worker if decrease ref count before removing it from linked-list.
        assert(pNode->pTNodeHeader->pData == pNode);

        __cache_wr_lock(pCacheObj);
        doRemoveElemInTrashcan(pCacheObj, pNode->pTNodeHeader);
        __cache_unlock(pCacheObj);

        ref = T_REF_DEC(pNode);
        assert(ref == 0);

        doDestroyTrashcanElem(pCacheObj, pNode->pTNodeHeader);
      } else {
        ref = T_REF_DEC(pNode);
        assert(ref >= 0);
      }
    } else {
      // NOTE: remove it from hash in the first place, otherwise, the pNode may have been released by other thread
      // when reaches here.
      SCacheDataNode *p = NULL;
      int32_t ret = taosHashRemoveWithData(pCacheObj->pHashTable, pNode->key, pNode->keySize, &p, sizeof(void *));
      ref = T_REF_DEC(pNode);

      // successfully remove from hash table, if failed, this node must have been move to trash already, do nothing.
      // note that the remove operation can be executed only once.
      if (ret == 0) {
        if (p != pNode) {
          uDebug( "cache:%s, key:%p, successfully removed a new entry:%p, refcnt:%d, prev entry:%p has been removed by "
              "others already", pCacheObj->name, pNode->key, p->data, T_REF_VAL_GET(p), pNode->data);

          assert(p->pTNodeHeader == NULL);
          taosAddToTrashcan(pCacheObj, p);
        } else {
          uDebug("cache:%s, key:%p, %p successfully removed from hash table, refcnt:%d", pCacheObj->name, pNode->key,
                 pNode->data, ref);
          if (ref > 0) {
            assert(pNode->pTNodeHeader == NULL);

            taosAddToTrashcan(pCacheObj, pNode);
          } else {  // ref == 0
            atomic_sub_fetch_64(&pCacheObj->totalSize, pNode->size);

            int32_t size = (int32_t)taosHashGetSize(pCacheObj->pHashTable);
            uDebug("cache:%s, key:%p, %p is destroyed from cache, size:%dbytes, totalNum:%d size:%" PRId64 "bytes",
                   pCacheObj->name, pNode->key, pNode->data, pNode->size, size, pCacheObj->totalSize);

            if (pCacheObj->freeFp) {
              pCacheObj->freeFp(pNode->data);
            }

            free(pNode);
          }
        }
      } else {
        uDebug("cache:%s, key:%p, %p has been removed from hash table by others already, refcnt:%d",
               pCacheObj->name, pNode->key, pNode->data, ref);
      }
    }

  } else {
    // NOTE: once refcount is decrease, pNode may be freed by other thread immediately.
    char* key = pNode->key;
    char* p = pNode->data;

//    int32_t ref = T_REF_VAL_GET(pNode);
//
//    if (ref == 1 && inTrashcan) {
//      // If it is the last ref, remove it from trashcan linked-list first, and then destroy it.Otherwise, it may be
//      // destroyed by refresh worker if decrease ref count before removing it from linked-list.
//      assert(pNode->pTNodeHeader->pData == pNode);
//
//      __cache_wr_lock(pCacheObj);
//      doRemoveElemInTrashcan(pCacheObj, pNode->pTNodeHeader);
//      __cache_unlock(pCacheObj);
//
//      ref = T_REF_DEC(pNode);
//      assert(ref == 0);
//
//      doDestroyTrashcanElem(pCacheObj, pNode->pTNodeHeader);
//    } else {
//      ref = T_REF_DEC(pNode);
//      assert(ref >= 0);
//    }

    int32_t ref = T_REF_DEC(pNode);
    uDebug("cache:%s, key:%p, %p released, refcnt:%d, data in trashcan:%d", pCacheObj->name, key, p, ref, inTrashcan);
  }
}

typedef struct SHashTravSupp {
  SCacheObj* pCacheObj;
  int64_t    time;
  __cache_free_fn_t fp;
} SHashTravSupp;

static bool travHashTableEmptyFn(void* param, void* data) {
  SHashTravSupp* ps = (SHashTravSupp*) param;
  SCacheObj* pCacheObj= ps->pCacheObj;

  SCacheDataNode *pNode = *(SCacheDataNode **) data;

  if (T_REF_VAL_GET(pNode) == 0) {
    taosCacheReleaseNode(pCacheObj, pNode);
  } else { // do add to trashcan
    taosAddToTrashcan(pCacheObj, pNode);
  }

  // this node should be remove from hash table
  return false;
}

void taosCacheEmpty(SCacheObj *pCacheObj) {
  SHashTravSupp sup = {.pCacheObj = pCacheObj, .fp = NULL, .time = taosGetTimestampMs()};

  taosHashCondTraverse(pCacheObj->pHashTable, travHashTableEmptyFn, &sup);
  taosTrashcanEmpty(pCacheObj, false);
}

void taosCacheCleanup(SCacheObj *pCacheObj) {
  if (pCacheObj == NULL) {
    return;
  }

  pCacheObj->deleting = 1;
  pthread_join(pCacheObj->refreshWorker, NULL);

  uInfo("cache:%s will be cleaned up", pCacheObj->name);
  doCleanupDataCache(pCacheObj);
}

SCacheDataNode *taosCreateCacheNode(const char *key, size_t keyLen, const char *pData, size_t size, uint64_t duration) {
  size_t totalSize = size + sizeof(SCacheDataNode) + keyLen;

  SCacheDataNode *pNewNode = calloc(1, totalSize);
  if (pNewNode == NULL) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  memcpy(pNewNode->data, pData, size);

  pNewNode->key = (char *)pNewNode + sizeof(SCacheDataNode) + size;
  pNewNode->keySize = (uint16_t)keyLen;

  memcpy(pNewNode->key, key, keyLen);

  pNewNode->addedTime    = (uint64_t)taosGetTimestampMs();
  pNewNode->lifespan     = duration;
  pNewNode->expireTime   = pNewNode->addedTime + pNewNode->lifespan;
  pNewNode->signature    = (uint64_t)pNewNode;
  pNewNode->size         = (uint32_t)totalSize;

  return pNewNode;
}

void taosAddToTrashcan(SCacheObj *pCacheObj, SCacheDataNode *pNode) {
  if (pNode->inTrashcan) { /* node is already in trash */
    assert(pNode->pTNodeHeader != NULL && pNode->pTNodeHeader->pData == pNode);
    return;
  }

  __cache_wr_lock(pCacheObj);
  STrashElem *pElem = calloc(1, sizeof(STrashElem));
  pElem->pData = pNode;
  pElem->prev = NULL;
  pElem->next = NULL;
  pNode->inTrashcan = true;
  pNode->pTNodeHeader = pElem;

  pElem->next = pCacheObj->pTrash;
  if (pCacheObj->pTrash) {
    pCacheObj->pTrash->prev = pElem;
  }

  pCacheObj->pTrash = pElem;
  pCacheObj->numOfElemsInTrash++;
  __cache_unlock(pCacheObj);

  uDebug("cache:%s key:%p, %p move to trashcan, pTrashElem:%p, numOfElem in trashcan:%d", pCacheObj->name, pNode->key,
         pNode->data, pElem, pCacheObj->numOfElemsInTrash);
}

void taosTrashcanEmpty(SCacheObj *pCacheObj, bool force) {
  __cache_wr_lock(pCacheObj);

  if (pCacheObj->numOfElemsInTrash == 0) {
    if (pCacheObj->pTrash != NULL) {
      pCacheObj->pTrash = NULL;
      uError("cache:%s, key:inconsistency data in cache, numOfElem in trashcan:%d", pCacheObj->name, pCacheObj->numOfElemsInTrash);
    }

    __cache_unlock(pCacheObj);
    return;
  }

  const char* stat[] = {"false", "true"};
  uDebug("cache:%s start to cleanup trashcan, numOfElem in trashcan:%d, free:%s", pCacheObj->name,
      pCacheObj->numOfElemsInTrash, (force? stat[1]:stat[0]));

  STrashElem *pElem = pCacheObj->pTrash;
  while (pElem) {
    T_REF_VAL_CHECK(pElem->pData);
    assert(pElem->next != pElem && pElem->prev != pElem);

    if (force || (T_REF_VAL_GET(pElem->pData) == 0)) {
      uDebug("cache:%s, key:%p, %p removed from trashcan. numOfElem in trashcan:%d", pCacheObj->name, pElem->pData->key, pElem->pData->data,
             pCacheObj->numOfElemsInTrash - 1);

      doRemoveElemInTrashcan(pCacheObj, pElem);
      doDestroyTrashcanElem(pCacheObj, pElem);
      pElem = pCacheObj->pTrash;
    } else {
      pElem = pElem->next;
    }
  }

  __cache_unlock(pCacheObj);
}

void doCleanupDataCache(SCacheObj *pCacheObj) {
  SHashTravSupp sup = {.pCacheObj = pCacheObj, .fp = NULL, .time = taosGetTimestampMs()};
  taosHashCondTraverse(pCacheObj->pHashTable, travHashTableEmptyFn, &sup);

  // todo memory leak if there are object with refcount greater than 0 in hash table?
  taosHashCleanup(pCacheObj->pHashTable);
  taosTrashcanEmpty(pCacheObj, true);

  __cache_lock_destroy(pCacheObj);
  
  tfree(pCacheObj->name);
  memset(pCacheObj, 0, sizeof(SCacheObj));
  free(pCacheObj);
}

bool travHashTableFn(void* param, void* data) {
  SHashTravSupp* ps = (SHashTravSupp*) param;
  SCacheObj*     pCacheObj= ps->pCacheObj;

  SCacheDataNode* pNode = *(SCacheDataNode **) data;
  if ((int64_t)pNode->expireTime < ps->time && T_REF_VAL_GET(pNode) <= 0) {
    taosCacheReleaseNode(pCacheObj, pNode);

    // this node should be remove from hash table
    return false;
  }

  if (ps->fp) {
    (ps->fp)(pNode->data);
  }

  // do not remove element in hash table
  return true;
}

static void doCacheRefresh(SCacheObj* pCacheObj, int64_t time, __cache_free_fn_t fp) {
  assert(pCacheObj != NULL);

  SHashTravSupp sup = {.pCacheObj = pCacheObj, .fp = fp, .time = time};
  taosHashCondTraverse(pCacheObj->pHashTable, travHashTableFn, &sup);
}

void* taosCacheTimedRefresh(void *handle) {
  SCacheObj* pCacheObj = handle;
  if (pCacheObj == NULL) {
    uDebug("object is destroyed. no refresh retry");
    return NULL;
  }

  const int32_t SLEEP_DURATION = 500; //500 ms
  int64_t totalTick = pCacheObj->refreshTime / SLEEP_DURATION;

  int64_t count = 0;
  while(1) {
#if defined LINUX
    usleep(500*1000);
#else
    taosMsleep(500);
#endif

    // check if current cache object will be deleted every 500ms.
    if (pCacheObj->deleting) {
      uDebug("%s refresh threads quit", pCacheObj->name);
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

    uDebug("%s refresh thread timed scan", pCacheObj->name);
    pCacheObj->statistics.refreshCount++;

    // refresh data in hash table
    if (elemInHash > 0) {
      int64_t now = taosGetTimestampMs();
      doCacheRefresh(pCacheObj, now, NULL);
    }

    taosTrashcanEmpty(pCacheObj, false);
  }

  return NULL;
}

void taosCacheRefresh(SCacheObj *pCacheObj, __cache_free_fn_t fp) {
  if (pCacheObj == NULL) {
    return;
  }

  int64_t now = taosGetTimestampMs();
  doCacheRefresh(pCacheObj, now, fp);
}
