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
#include "tcache.h"
#include "osThread.h"
#include "taoserror.h"
#include "tlog.h"
#include "tutil.h"

#define CACHE_MAX_CAPACITY     1024 * 1024 * 16
#define CACHE_DEFAULT_CAPACITY 1024 * 4

static TdThread      cacheRefreshWorker = {0};
static TdThreadOnce  cacheThreadInit = PTHREAD_ONCE_INIT;
static TdThreadMutex guard;
static SArray       *pCacheArrayList = NULL;
static bool          stopRefreshWorker = false;
static bool          refreshWorkerNormalStopped = false;
static bool          refreshWorkerUnexpectedStopped = false;

typedef struct SCacheNode {
  uint64_t           addedTime;   // the added time when this element is added or updated into cache
  uint64_t           lifespan;    // life duration when this element should be remove from cache
  int64_t            expireTime;  // expire time
  void              *signature;
  struct STrashElem *pTNodeHeader;    // point to trash node head
  uint16_t           keyLen : 15;     // max key size: 32kb
  bool               inTrashcan : 1;  // denote if it is in trash or not
  uint32_t           size;            // allocated size for current SCacheNode
  uint32_t           dataLen;
  T_REF_DECLARE()
  struct SCacheNode *pNext;
  char              *key;
  char              *data;
} SCacheNode;

typedef struct SCacheEntry {
  int32_t     num;    // number of elements in current entry
  SRWLatch    latch;  // entry latch
  SCacheNode *next;
} SCacheEntry;

struct STrashElem {
  struct STrashElem *prev;
  struct STrashElem *next;
  SCacheNode        *pData;
};

struct SCacheIter {
  SCacheObj   *pCacheObj;
  SCacheNode **pCurrent;
  int32_t      entryIndex;
  int32_t      index;
  int32_t      numOfObj;
};

/*
 * to accommodate the old data which has the same key value of new one in hashList
 * when an new node is put into cache, if an existed one with the same key:
 * 1. if the old one does not be referenced, update it.
 * 2. otherwise, move the old one to pTrash, addedTime the new one.
 *
 * when the node in pTrash does not be referenced, it will be release at the expired expiredTime
 */
struct SCacheObj {
  int64_t      sizeInBytes;  // total allocated buffer in this hash table, SCacheObj is not included.
  int64_t      refreshTime;
  char        *name;
  SCacheStatis statistics;

  SCacheEntry      *pEntryList;
  size_t            capacity;    // number of slots
  size_t            numOfElems;  // number of elements in cache
  _hash_fn_t        hashFp;      // hash function
  __cache_free_fn_t freeFp;

  uint32_t    numOfElemsInTrash;  // number of element in trash
  STrashElem *pTrash;

  uint8_t  deleting;  // set the deleting flag to stop refreshing ASAP.
  TdThread refreshWorker;
  bool     extendLifespan;  // auto extend life span when one item is accessed.
  int64_t  checkTick;       // tick used to record the check times of the refresh threads
#if defined(LINUX)
  TdThreadRwlock lock;
#else
  TdThreadMutex lock;
#endif
};

typedef struct SCacheObjTravSup {
  SCacheObj        *pCacheObj;
  int64_t           time;
  __cache_trav_fn_t fp;
  void             *param1;
} SCacheObjTravSup;

static FORCE_INLINE void __trashcan_wr_lock(SCacheObj *pCacheObj) {
#if defined(LINUX)
  taosThreadRwlockWrlock(&pCacheObj->lock);
#else
  taosThreadMutexLock(&pCacheObj->lock);
#endif
}

static FORCE_INLINE void __trashcan_unlock(SCacheObj *pCacheObj) {
#if defined(LINUX)
  taosThreadRwlockUnlock(&pCacheObj->lock);
#else
  taosThreadMutexUnlock(&pCacheObj->lock);
#endif
}

static FORCE_INLINE int32_t __trashcan_lock_init(SCacheObj *pCacheObj) {
#if defined(LINUX)
  return taosThreadRwlockInit(&pCacheObj->lock, NULL);
#else
  return taosThreadMutexInit(&pCacheObj->lock, NULL);
#endif
}

static FORCE_INLINE void __trashcan_lock_destroy(SCacheObj *pCacheObj) {
#if defined(LINUX)
  taosThreadRwlockDestroy(&pCacheObj->lock);
#else
  taosThreadMutexDestroy(&pCacheObj->lock);
#endif
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
static void *taosCacheTimedRefresh(void *handle);

static void doInitRefreshThread(void) {
  pCacheArrayList = taosArrayInit(4, POINTER_BYTES);

  taosThreadMutexInit(&guard, NULL);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

  taosThreadCreate(&cacheRefreshWorker, &thattr, taosCacheTimedRefresh, NULL);
  taosThreadAttrDestroy(&thattr);
}

TdThread doRegisterCacheObj(SCacheObj *pCacheObj) {
  taosThreadOnce(&cacheThreadInit, doInitRefreshThread);

  taosThreadMutexLock(&guard);
  taosArrayPush(pCacheArrayList, &pCacheObj);
  taosThreadMutexUnlock(&guard);

  return cacheRefreshWorker;
}

/**
 * @param key      key of object for hash, usually a null-terminated string
 * @param keyLen   length of key
 * @param pData    actually data. required a consecutive memory block, no pointer is allowed
 *                 in pData. Pointer copy causes memory access error.
 * @param size     size of block
 * @param lifespan total survial expiredTime from now
 * @return         SCacheNode
 */
static SCacheNode *taosCreateCacheNode(const char *key, size_t keyLen, const char *pData, size_t size,
                                       uint64_t duration);

/**
 * addedTime object node into trash, and this object is closed for referencing if it is addedTime to trash
 * It will be removed until the pNode->refCount == 0
 * @param pCacheObj    Cache object
 * @param pNode   Cache slot object
 */
static void taosAddToTrashcan(SCacheObj *pCacheObj, SCacheNode *pNode);

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
static FORCE_INLINE void taosCacheReleaseNode(SCacheObj *pCacheObj, SCacheNode *pNode) {
  if (pNode->signature != pNode) {
    uError("key:%s, %p data is invalid, or has been released", pNode->key, pNode);
    return;
  }

  atomic_sub_fetch_64(&pCacheObj->sizeInBytes, pNode->size);

  uDebug("cache:%s, key:%p, %p is destroyed from cache, size:%dbytes, total num:%d size:%" PRId64 "bytes",
         pCacheObj->name, pNode->key, pNode->data, pNode->size, (int)pCacheObj->numOfElems - 1, pCacheObj->sizeInBytes);

  if (pCacheObj->freeFp) {
    pCacheObj->freeFp(pNode->data);
  }

  taosMemoryFree(pNode);
}

static FORCE_INLINE STrashElem *doRemoveElemInTrashcan(SCacheObj *pCacheObj, STrashElem *pElem) {
  if (pElem->pData->signature != pElem->pData) {
    uWarn("key:sig:0x%" PRIx64 " %p data has been released, ignore", (int64_t)pElem->pData->signature, pElem->pData);
    return NULL;
  }

  STrashElem *next = pElem->next;

  pCacheObj->numOfElemsInTrash--;
  if (pElem->prev) {
    pElem->prev->next = pElem->next;
  } else {  // pnode is the header, update header
    pCacheObj->pTrash = pElem->next;
  }

  if (next) {
    next->prev = pElem->prev;
  }
  return next;
}

static FORCE_INLINE void doDestroyTrashcanElem(SCacheObj *pCacheObj, STrashElem *pElem) {
  if (pCacheObj->freeFp) {
    pCacheObj->freeFp(pElem->pData->data);
  }

  taosMemoryFree(pElem->pData);
  taosMemoryFree(pElem);
}

static void pushfrontNodeInEntryList(SCacheEntry *pEntry, SCacheNode *pNode) {
  pNode->pNext = pEntry->next;
  pEntry->next = pNode;
  pEntry->num += 1;
  ASSERT((pEntry->next && pEntry->num > 0) || (NULL == pEntry->next && pEntry->num == 0));
}

static void removeNodeInEntryList(SCacheEntry *pe, SCacheNode *prev, SCacheNode *pNode) {
  if (prev == NULL) {
    pe->next = pNode->pNext;
  } else {
    prev->pNext = pNode->pNext;
  }

  pNode->pNext = NULL;
  pe->num -= 1;
  ASSERT((pe->next && pe->num > 0) || (NULL == pe->next && pe->num == 0));
}

static FORCE_INLINE SCacheEntry *doFindEntry(SCacheObj *pCacheObj, const void *key, size_t keyLen) {
  uint32_t hashVal = (*pCacheObj->hashFp)(key, keyLen);
  int32_t  slot = hashVal % pCacheObj->capacity;
  return &pCacheObj->pEntryList[slot];
}

static FORCE_INLINE SCacheNode *doSearchInEntryList(SCacheEntry *pe, const void *key, size_t keyLen,
                                                    SCacheNode **prev) {
  SCacheNode *pNode = pe->next;
  while (pNode) {
    if ((pNode->keyLen == keyLen) && memcmp(pNode->key, key, keyLen) == 0) {
      break;
    }
    *prev = pNode;
    pNode = pNode->pNext;
  }

  return pNode;
}

static bool doRemoveExpiredFn(void *param, SCacheNode *pNode) {
  SCacheObjTravSup *ps = (SCacheObjTravSup *)param;
  SCacheObj        *pCacheObj = ps->pCacheObj;

  if ((int64_t)pNode->expireTime < ps->time && T_REF_VAL_GET(pNode) <= 0) {
    taosCacheReleaseNode(pCacheObj, pNode);

    // this node should be remove from hash table
    return false;
  }

  if (ps->fp) {
    (ps->fp)(pNode->data, ps->param1);
  }

  // do not remove element in hash table
  return true;
}

static bool doRemoveNodeFn(void *param, SCacheNode *pNode) {
  SCacheObjTravSup *ps = (SCacheObjTravSup *)param;
  SCacheObj        *pCacheObj = ps->pCacheObj;

  if (T_REF_VAL_GET(pNode) == 0) {
    taosCacheReleaseNode(pCacheObj, pNode);
  } else {  // do add to trashcan
    taosAddToTrashcan(pCacheObj, pNode);
  }

  // this node should be remove from hash table
  return false;
}

static FORCE_INLINE int32_t getCacheCapacity(int32_t length) {
  int32_t len = 0;
  if (length < CACHE_DEFAULT_CAPACITY) {
    len = CACHE_DEFAULT_CAPACITY;
    return len;
  } else if (length > CACHE_MAX_CAPACITY) {
    len = CACHE_MAX_CAPACITY;
    return len;
  }

  len = CACHE_DEFAULT_CAPACITY;
  while (len < length && len < CACHE_MAX_CAPACITY) {
    len = (len << 1u);
  }

  return len > CACHE_MAX_CAPACITY ? CACHE_MAX_CAPACITY : len;
}

SCacheObj *taosCacheInit(int32_t keyType, int64_t refreshTimeInMs, bool extendLifespan, __cache_free_fn_t fn,
                         const char *cacheName) {
  const int32_t SLEEP_DURATION = 500;  // 500 ms

  if (refreshTimeInMs <= 0) {
    return NULL;
  }

  SCacheObj *pCacheObj = (SCacheObj *)taosMemoryCalloc(1, sizeof(SCacheObj));
  if (pCacheObj == NULL) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  // TODO add the auto extend procedure
  pCacheObj->capacity = 4096;
  pCacheObj->pEntryList = taosMemoryCalloc(pCacheObj->capacity, sizeof(SCacheEntry));
  if (pCacheObj->pEntryList == NULL) {
    taosMemoryFree(pCacheObj);
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  // set free cache node callback function
  pCacheObj->hashFp = taosGetDefaultHashFunction(keyType);
  pCacheObj->freeFp = fn;
  pCacheObj->refreshTime = refreshTimeInMs;
  pCacheObj->checkTick = pCacheObj->refreshTime / SLEEP_DURATION;
  pCacheObj->extendLifespan = extendLifespan;  // the TTL after the last access

  if (__trashcan_lock_init(pCacheObj) != 0) {
    taosMemoryFreeClear(pCacheObj->pEntryList);
    taosMemoryFree(pCacheObj);

    uError("failed to init lock, reason:%s", strerror(errno));
    return NULL;
  }

  pCacheObj->name = taosStrdup(cacheName);
  doRegisterCacheObj(pCacheObj);
  return pCacheObj;
}

void *taosCachePut(SCacheObj *pCacheObj, const void *key, size_t keyLen, const void *pData, size_t dataSize,
                   int32_t durationMS) {
  if (pCacheObj == NULL || pCacheObj->pEntryList == NULL || pCacheObj->deleting == 1) {
    return NULL;
  }

  SCacheNode *pNode1 = taosCreateCacheNode(key, keyLen, pData, dataSize, durationMS);
  if (pNode1 == NULL) {
    uError("cache:%s, key:%p, failed to added into cache, out of memory", pCacheObj->name, key);
    return NULL;
  }

  T_REF_INC(pNode1);

  SCacheEntry *pe = doFindEntry(pCacheObj, key, keyLen);

  taosWLockLatch(&pe->latch);

  SCacheNode *prev = NULL;
  SCacheNode *pNode = doSearchInEntryList(pe, key, keyLen, &prev);

  if (pNode == NULL) {
    pushfrontNodeInEntryList(pe, pNode1);
    atomic_add_fetch_ptr(&pCacheObj->numOfElems, 1);
    atomic_add_fetch_ptr(&pCacheObj->sizeInBytes, pNode1->size);
    uDebug("cache:%s, key:%p, %p added into cache, added:%" PRIu64 ", expire:%" PRIu64
           ", totalNum:%d sizeInBytes:%" PRId64 "bytes size:%" PRId64 "bytes",
           pCacheObj->name, key, pNode1->data, pNode1->addedTime, pNode1->expireTime, (int32_t)pCacheObj->numOfElems,
           pCacheObj->sizeInBytes, (int64_t)dataSize);
  } else {  // duplicated key exists
    // move current node to trashcan
    removeNodeInEntryList(pe, prev, pNode);

    if (T_REF_VAL_GET(pNode) == 0) {
      if (pCacheObj->freeFp) {
        pCacheObj->freeFp(pNode->data);
      }

      atomic_sub_fetch_64(&pCacheObj->sizeInBytes, pNode->size);
      taosMemoryFreeClear(pNode);
    } else {
      taosAddToTrashcan(pCacheObj, pNode);
      uDebug("cache:%s, key:%p, %p exist in cache, updated old:%p", pCacheObj->name, key, pNode1->data, pNode->data);
    }

    pushfrontNodeInEntryList(pe, pNode1);
    atomic_add_fetch_64(&pCacheObj->sizeInBytes, pNode1->size);
    uDebug("cache:%s, key:%p, %p added into cache, added:%" PRIu64 ", expire:%" PRIu64
           ", totalNum:%d sizeInBytes:%" PRId64 "bytes size:%" PRId64 "bytes",
           pCacheObj->name, key, pNode1->data, pNode1->addedTime, pNode1->expireTime, (int32_t)pCacheObj->numOfElems,
           pCacheObj->sizeInBytes, (int64_t)dataSize);
  }

  taosWUnLockLatch(&pe->latch);
  return pNode1->data;
}

void *taosCacheAcquireByKey(SCacheObj *pCacheObj, const void *key, size_t keyLen) {
  if (pCacheObj == NULL || pCacheObj->deleting == 1) {
    return NULL;
  }

  if (pCacheObj->numOfElems == 0) {
    atomic_add_fetch_64(&pCacheObj->statistics.missCount, 1);
    return NULL;
  }

  SCacheNode  *prev = NULL;
  SCacheEntry *pe = doFindEntry(pCacheObj, key, keyLen);

  taosRLockLatch(&pe->latch);

  SCacheNode *pNode = doSearchInEntryList(pe, key, keyLen, &prev);
  if (pNode != NULL) {
    int32_t ref = T_REF_INC(pNode);
  }

  taosRUnLockLatch(&pe->latch);

  void *pData = (pNode != NULL) ? pNode->data : NULL;
  if (pData != NULL) {
    atomic_add_fetch_64(&pCacheObj->statistics.hitCount, 1);
    uDebug("cache:%s, key:%p, %p is retrieved from cache, refcnt:%d", pCacheObj->name, key, pData,
           T_REF_VAL_GET(pNode));
  } else {
    atomic_add_fetch_64(&pCacheObj->statistics.missCount, 1);
    uDebug("cache:%s, key:%p, not in cache, retrieved failed", pCacheObj->name, key);
  }

  atomic_add_fetch_64(&pCacheObj->statistics.totalAccess, 1);
  return pData;
}

void *taosCacheAcquireByData(SCacheObj *pCacheObj, void *data) {
  if (pCacheObj == NULL || data == NULL) return NULL;

  SCacheNode *ptNode = (SCacheNode *)((char *)data - sizeof(SCacheNode));
  if (ptNode->signature != ptNode) {
    uError("cache:%s, key: %p the data from cache is invalid", pCacheObj->name, ptNode);
    return NULL;
  }

  int32_t ref = T_REF_INC(ptNode);
  uDebug("cache:%s, data: %p acquired by data in cache, refcnt:%d", pCacheObj->name, ptNode->data, ref);

  // the data if referenced by at least one object, so the reference count must be greater than the value of 2.
  ASSERT(ref >= 2);
  return data;
}

void *taosCacheTransferData(SCacheObj *pCacheObj, void **data) {
  if (pCacheObj == NULL || data == NULL || (*data) == NULL) return NULL;

  SCacheNode *ptNode = (SCacheNode *)((char *)(*data) - sizeof(SCacheNode));
  if (ptNode->signature != ptNode) {
    uError("cache:%s, key: %p the data from cache is invalid", pCacheObj->name, ptNode);
    return NULL;
  }

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
  SCacheNode *pNode = (SCacheNode *)((char *)(*data) - sizeof(SCacheNode));
  if (pNode->signature != pNode) {
    uError("cache:%s, %p, release invalid cache data", pCacheObj->name, pNode);
    return;
  }

  *data = NULL;

  // note: extend lifespan before dec ref count
  bool inTrashcan = pNode->inTrashcan;

  if (pCacheObj->extendLifespan && (!inTrashcan) && (!_remove)) {
    atomic_store_64(&pNode->expireTime, pNode->lifespan + taosGetTimestampMs());
    uDebug("cache:%s, data:%p extend expire time: %" PRId64, pCacheObj->name, pNode->data, pNode->expireTime);
  }

  if (_remove) {
    // NOTE: once refcount is decrease, pNode may be freed by other thread immediately.
    char *key = pNode->key;
    char *d = pNode->data;

    int32_t ref = T_REF_VAL_GET(pNode);
    uDebug("cache:%s, key:%p, %p is released, refcnt:%d, in trashcan:%d", pCacheObj->name, key, d, ref - 1, inTrashcan);

    /*
     * If it is not referenced by other users, remove it immediately. Otherwise move this node to trashcan wait for all
     * users releasing this resources.
     *
     * NOTE: previous ref is 0, and current ref is still 0, remove it. If previous is not 0, there is another thread
     * that tries to do the same thing.
     */
    if (inTrashcan) {
      ref = T_REF_VAL_GET(pNode);

      if (ref == 1) {
        // If it is the last ref, remove it from trashcan linked-list first, and then destroy it.Otherwise, it may be
        // destroyed by refresh worker if decrease ref count before removing it from linked-list.
        ASSERT(pNode->pTNodeHeader->pData == pNode);

        __trashcan_wr_lock(pCacheObj);
        doRemoveElemInTrashcan(pCacheObj, pNode->pTNodeHeader);
        __trashcan_unlock(pCacheObj);

        ref = T_REF_DEC(pNode);
        ASSERT(ref == 0);

        doDestroyTrashcanElem(pCacheObj, pNode->pTNodeHeader);
      } else {
        ref = T_REF_DEC(pNode);
        ASSERT(ref >= 0);
      }
    } else {
      // NOTE: remove it from hash in the first place, otherwise, the pNode may have been released by other thread
      // when reaches here.
      SCacheNode  *prev = NULL;
      SCacheEntry *pe = doFindEntry(pCacheObj, pNode->key, pNode->keyLen);

      taosWLockLatch(&pe->latch);
      ref = T_REF_DEC(pNode);

      SCacheNode *p = doSearchInEntryList(pe, pNode->key, pNode->keyLen, &prev);

      if (p != NULL) {
        // successfully remove from hash table, if failed, this node must have been move to trash already, do nothing.
        // note that the remove operation can be executed only once.
        if (p != pNode) {
          uDebug(
              "cache:%s, key:%p, a new entry:%p found, refcnt:%d, prev entry:%p, refcnt:%d has been removed by "
              "others already, prev must in trashcan",
              pCacheObj->name, pNode->key, p->data, T_REF_VAL_GET(p), pNode->data, T_REF_VAL_GET(pNode));

          ASSERT(p->pTNodeHeader == NULL && pNode->pTNodeHeader != NULL);
        } else {
          removeNodeInEntryList(pe, prev, p);
          uDebug("cache:%s, key:%p, %p successfully removed from hash table, refcnt:%d", pCacheObj->name, pNode->key,
                 pNode->data, ref);
          if (ref > 0) {
            taosAddToTrashcan(pCacheObj, pNode);
          } else {  // ref == 0
            atomic_sub_fetch_64(&pCacheObj->sizeInBytes, pNode->size);

            int32_t size = (int32_t)pCacheObj->numOfElems;
            uDebug("cache:%s, key:%p, %p is destroyed from cache, size:%dbytes, totalNum:%d size:%" PRId64 "bytes",
                   pCacheObj->name, pNode->key, pNode->data, pNode->size, size, pCacheObj->sizeInBytes);

            if (pCacheObj->freeFp) {
              pCacheObj->freeFp(pNode->data);
            }

            taosMemoryFree(pNode);
          }
        }

        taosWUnLockLatch(&pe->latch);
      } else {
        uDebug("cache:%s, key:%p, %p has been removed from hash table by others already, refcnt:%d", pCacheObj->name,
               pNode->key, pNode->data, ref);
      }
    }

  } else {
    // NOTE: once refcount is decrease, pNode may be freed by other thread immediately.
    char *key = pNode->key;
    char *p = pNode->data;

    int32_t ref = T_REF_DEC(pNode);
    uDebug("cache:%s, key:%p, %p released, refcnt:%d, data in trashcan:%d", pCacheObj->name, key, p, ref, inTrashcan);
  }
}

void doTraverseElems(SCacheObj *pCacheObj, bool (*fp)(void *param, SCacheNode *pNode), SCacheObjTravSup *pSup) {
  int32_t numOfEntries = (int32_t)pCacheObj->capacity;
  for (int32_t i = 0; i < numOfEntries; ++i) {
    SCacheEntry *pEntry = &pCacheObj->pEntryList[i];
    if (pEntry->num == 0) {
      continue;
    }

    taosWLockLatch(&pEntry->latch);

    SCacheNode **pPre = &pEntry->next;
    SCacheNode  *pNode = pEntry->next;
    while (pNode != NULL) {
      SCacheNode *next = pNode->pNext;

      if (fp(pSup, pNode)) {
        pPre = &pNode->pNext;
        pNode = pNode->pNext;
      } else {
        *pPre = next;
        pEntry->num -= 1;
        ASSERT((pEntry->next && pEntry->num > 0) || (NULL == pEntry->next && pEntry->num == 0));

        atomic_sub_fetch_ptr(&pCacheObj->numOfElems, 1);
        pNode = next;
      }
    }

    taosWUnLockLatch(&pEntry->latch);
  }
}

void taosCacheEmpty(SCacheObj *pCacheObj) {
  SCacheObjTravSup sup = {.pCacheObj = pCacheObj, .fp = NULL, .time = taosGetTimestampMs()};
  doTraverseElems(pCacheObj, doRemoveNodeFn, &sup);
  taosTrashcanEmpty(pCacheObj, false);
}

void taosCacheCleanup(SCacheObj *pCacheObj) {
  if (pCacheObj == NULL) {
    return;
  }

  pCacheObj->deleting = 1;

  // wait for the refresh thread quit before destroying the cache object.
  // But in the dll, the child thread will be killed before atexit takes effect.
  while (atomic_load_8(&pCacheObj->deleting) != 0) {
    if (refreshWorkerNormalStopped) break;
    if (refreshWorkerUnexpectedStopped) return;
    taosMsleep(50);
  }

  uTrace("cache:%s will be cleaned up", pCacheObj->name);
  doCleanupDataCache(pCacheObj);
}

SCacheNode *taosCreateCacheNode(const char *key, size_t keyLen, const char *pData, size_t size, uint64_t duration) {
  size_t sizeInBytes = size + sizeof(SCacheNode) + keyLen;

  SCacheNode *pNewNode = taosMemoryCalloc(1, sizeInBytes);
  if (pNewNode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  pNewNode->data = (char *)pNewNode + sizeof(SCacheNode);
  pNewNode->dataLen = size;
  memcpy(pNewNode->data, pData, size);

  pNewNode->key = (char *)pNewNode + sizeof(SCacheNode) + size;
  pNewNode->keyLen = (uint16_t)keyLen;

  memcpy(pNewNode->key, key, keyLen);

  pNewNode->addedTime = (uint64_t)taosGetTimestampMs();
  pNewNode->lifespan = duration;
  pNewNode->expireTime = pNewNode->addedTime + pNewNode->lifespan;
  pNewNode->signature = pNewNode;
  pNewNode->size = (uint32_t)sizeInBytes;

  return pNewNode;
}

void taosAddToTrashcan(SCacheObj *pCacheObj, SCacheNode *pNode) {
  if (pNode->inTrashcan) { /* node is already in trash */
    ASSERT(pNode->pTNodeHeader != NULL && pNode->pTNodeHeader->pData == pNode);
    return;
  }

  __trashcan_wr_lock(pCacheObj);
  STrashElem *pElem = taosMemoryCalloc(1, sizeof(STrashElem));
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
  __trashcan_unlock(pCacheObj);

  uDebug("cache:%s key:%p, %p move to trashcan, pTrashElem:%p, numOfElem in trashcan:%d", pCacheObj->name, pNode->key,
         pNode->data, pElem, pCacheObj->numOfElemsInTrash);
}

void taosTrashcanEmpty(SCacheObj *pCacheObj, bool force) {
  __trashcan_wr_lock(pCacheObj);

  if (pCacheObj->numOfElemsInTrash == 0) {
    if (pCacheObj->pTrash != NULL) {
      pCacheObj->pTrash = NULL;
      uError("cache:%s, key:inconsistency data in cache, numOfElem in trashcan:%d", pCacheObj->name,
             pCacheObj->numOfElemsInTrash);
    }

    __trashcan_unlock(pCacheObj);
    return;
  }

  const char *stat[] = {"false", "true"};
  uDebug("cache:%s start to cleanup trashcan, numOfElem in trashcan:%d, free:%s", pCacheObj->name,
         pCacheObj->numOfElemsInTrash, (force ? stat[1] : stat[0]));

  STrashElem *pElem = pCacheObj->pTrash;
  while (pElem) {
    T_REF_VAL_CHECK(pElem->pData);
    ASSERT(pElem->next != pElem && pElem->prev != pElem);

    if (force || (T_REF_VAL_GET(pElem->pData) == 0)) {
      uDebug("cache:%s, key:%p, %p removed from trashcan. numOfElem in trashcan:%d", pCacheObj->name, pElem->pData->key,
             pElem->pData->data, pCacheObj->numOfElemsInTrash - 1);

      doRemoveElemInTrashcan(pCacheObj, pElem);
      doDestroyTrashcanElem(pCacheObj, pElem);
      pElem = pCacheObj->pTrash;
    } else {
      pElem = pElem->next;
    }
  }

  __trashcan_unlock(pCacheObj);
}

void doCleanupDataCache(SCacheObj *pCacheObj) {
  SCacheObjTravSup sup = {.pCacheObj = pCacheObj, .fp = NULL, .time = taosGetTimestampMs()};
  doTraverseElems(pCacheObj, doRemoveNodeFn, &sup);

  // todo memory leak if there are object with refcount greater than 0 in hash table?
  taosTrashcanEmpty(pCacheObj, true);

  __trashcan_lock_destroy(pCacheObj);

  taosMemoryFreeClear(pCacheObj->pEntryList);
  taosMemoryFreeClear(pCacheObj->name);
  taosMemoryFree(pCacheObj);
}

static void doCacheRefresh(SCacheObj *pCacheObj, int64_t time, __cache_trav_fn_t fp, void *param1) {
  SCacheObjTravSup sup = {.pCacheObj = pCacheObj, .fp = fp, .time = time, .param1 = param1};
  doTraverseElems(pCacheObj, doRemoveExpiredFn, &sup);
}

void taosCacheRefreshWorkerUnexpectedStopped(void) {
  if (!refreshWorkerNormalStopped) {
    refreshWorkerUnexpectedStopped = true;
  }
}

void *taosCacheTimedRefresh(void *handle) {
  uDebug("cache refresh thread starts");
  setThreadName("cacheRefresh");

  const int32_t SLEEP_DURATION = 500;  // 500 ms
  int64_t       count = 0;
#ifdef WINDOWS
  if (taosCheckCurrentInDll()) {
    atexit(taosCacheRefreshWorkerUnexpectedStopped);
  }
#endif

  while (1) {
    taosMsleep(SLEEP_DURATION);
    if (stopRefreshWorker) {
      goto _end;
    }

    taosThreadMutexLock(&guard);
    size_t size = taosArrayGetSize(pCacheArrayList);
    taosThreadMutexUnlock(&guard);

    count += 1;

    for (int32_t i = 0; i < size; ++i) {
      taosThreadMutexLock(&guard);
      SCacheObj *pCacheObj = taosArrayGetP(pCacheArrayList, i);

      if (pCacheObj == NULL) {
        uError("object is destroyed. ignore and try next");
        taosThreadMutexUnlock(&guard);
        continue;
      }

      // check if current cache object will be deleted every 500ms.
      if (pCacheObj->deleting) {
        taosArrayRemove(pCacheArrayList, i);
        size = taosArrayGetSize(pCacheArrayList);

        uDebug("%s is destroying, remove it from refresh list, remain cache obj:%" PRIzu, pCacheObj->name, size);
        pCacheObj->deleting = 0;  // reset the deleting flag to enable pCacheObj to continue releasing resources.

        taosThreadMutexUnlock(&guard);
        continue;
      }

      taosThreadMutexUnlock(&guard);

      if ((count % pCacheObj->checkTick) != 0) {
        continue;
      }

      size_t elemInHash = pCacheObj->numOfElems;
      if (elemInHash + pCacheObj->numOfElemsInTrash == 0) {
        continue;
      }

      uDebug("%s refresh thread scan", pCacheObj->name);
      pCacheObj->statistics.refreshCount++;

      // refresh data in hash table
      if (elemInHash > 0) {
        int64_t now = taosGetTimestampMs();
        doCacheRefresh(pCacheObj, now, NULL, NULL);
      }

      taosTrashcanEmpty(pCacheObj, false);
    }
  }

_end:
  taosArrayDestroy(pCacheArrayList);

  pCacheArrayList = NULL;
  taosThreadMutexDestroy(&guard);
  refreshWorkerNormalStopped = true;

  uDebug("cache refresh thread quits");
  return NULL;
}

void taosCacheRefresh(SCacheObj *pCacheObj, __cache_trav_fn_t fp, void *param1) {
  if (pCacheObj == NULL) {
    return;
  }

  int64_t now = taosGetTimestampMs();
  doCacheRefresh(pCacheObj, now, fp, param1);
}

void taosStopCacheRefreshWorker(void) {
  stopRefreshWorker = true;
  TdThreadOnce tmp = PTHREAD_ONCE_INIT;
  if (memcmp(&cacheThreadInit, &tmp, sizeof(TdThreadOnce)) != 0) taosThreadJoin(cacheRefreshWorker, NULL);
  taosArrayDestroy(pCacheArrayList);
}

size_t taosCacheGetNumOfObj(const SCacheObj *pCacheObj) { return pCacheObj->numOfElems + pCacheObj->numOfElemsInTrash; }

SCacheIter *taosCacheCreateIter(const SCacheObj *pCacheObj) {
  SCacheIter *pIter = taosMemoryCalloc(1, sizeof(SCacheIter));
  pIter->pCacheObj = (SCacheObj *)pCacheObj;
  pIter->entryIndex = -1;
  pIter->index = -1;
  return pIter;
}

bool taosCacheIterNext(SCacheIter *pIter) {
  SCacheObj *pCacheObj = pIter->pCacheObj;

  if (pIter->index + 1 >= pIter->numOfObj) {
    // release the reference for all objects in the snapshot
    for (int32_t i = 0; i < pIter->numOfObj; ++i) {
      char *p = pIter->pCurrent[i]->data;
      taosCacheRelease(pCacheObj, (void **)&p, false);
      pIter->pCurrent[i] = NULL;
    }

    if (pIter->entryIndex + 1 >= pCacheObj->capacity) {
      return false;
    }

    while (1) {
      pIter->entryIndex++;
      if (pIter->entryIndex >= pCacheObj->capacity) {
        return false;
      }

      SCacheEntry *pEntry = &pCacheObj->pEntryList[pIter->entryIndex];
      taosRLockLatch(&pEntry->latch);

      if (pEntry->num == 0) {
        taosRUnLockLatch(&pEntry->latch);
        continue;
      }

      if (pIter->numOfObj < pEntry->num) {
        char *tmp = taosMemoryRealloc(pIter->pCurrent, pEntry->num * POINTER_BYTES);
        if (tmp == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          taosRUnLockLatch(&pEntry->latch);
          return false;
        }

        pIter->pCurrent = (SCacheNode **)tmp;
      }

      SCacheNode *pNode = pEntry->next;
      for (int32_t i = 0; i < pEntry->num; ++i) {
        pIter->pCurrent[i] = pNode;
        int32_t ref = T_REF_INC(pIter->pCurrent[i]);
        pNode = pNode->pNext;
      }

      pIter->numOfObj = pEntry->num;
      taosRUnLockLatch(&pEntry->latch);

      pIter->index = -1;
      break;
    }
  }

  pIter->index += 1;
  return true;
}

void *taosCacheIterGetData(const SCacheIter *pIter, size_t *len) {
  SCacheNode *pNode = pIter->pCurrent[pIter->index];
  *len = pNode->dataLen;
  return pNode->data;
}

void *taosCacheIterGetKey(const SCacheIter *pIter, size_t *len) {
  SCacheNode *pNode = pIter->pCurrent[pIter->index];
  *len = pNode->keyLen;
  return pNode->key;
}

void taosCacheDestroyIter(SCacheIter *pIter) {
  for (int32_t i = 0; i < pIter->numOfObj; ++i) {
    if (!pIter->pCurrent[i]) continue;
    char *p = pIter->pCurrent[i]->data;
    taosCacheRelease(pIter->pCacheObj, (void **)&p, false);
    pIter->pCurrent[i] = NULL;
  }
  taosMemoryFreeClear(pIter->pCurrent);
  taosMemoryFreeClear(pIter);
}

void taosCacheTryExtendLifeSpan(SCacheObj *pCacheObj, void **data) {
  if (!pCacheObj || !(*data)) return;

  SCacheNode *pNode = (SCacheNode *)((char *)(*data) - sizeof(SCacheNode));
  if (pNode->signature != pNode) return;

  if (!pNode->inTrashcan) {
    atomic_store_64(&pNode->expireTime, pNode->lifespan + taosGetTimestampMs());
    uDebug("cache:%s, data:%p extend expire time: %" PRId64, pCacheObj->name, pNode->data, pNode->expireTime);
  }
}
