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

// these append/remove nodes function could be defined with macro to make
// the code not this ugly.
#define append_node_impl(pCacheObj, pNode, typ)                    \
do {                                                               \
  assert((pCacheObj));                                             \
  assert((pNode));                                                 \
  assert((pNode)->next==NULL);                                     \
  assert((pNode)->prev==NULL);                                     \
  assert((pNode)->owner==NULL);                                    \
                                                                   \
  SCacheDataNode *tail              = (pCacheObj)->typ##_tail;     \
  (pNode)->prev                     = tail;                        \
  if (tail) tail->next              = (pNode);                     \
  else      (pCacheObj)->typ##_head = (pNode);                     \
  (pCacheObj)->typ##_tail           = (pNode);                     \
                                                                   \
  (pNode)->owner  = (pCacheObj);                                   \
} while (0)

#define remove_node_impl(pCacheObj, pNode, typ, pNext)             \
do {                                                               \
  assert((pCacheObj));                                             \
  assert((pNode));                                                 \
  assert((pNode)->owner == (pCacheObj));                           \
                                                                   \
  SCacheDataNode *next              = (pNode)->next;               \
  SCacheDataNode *prev              = (pNode)->prev;               \
  if (next) next->prev              = prev;                        \
  else      (pCacheObj)->typ##_tail = prev;                        \
  if (prev) prev->next              = next;                        \
  else      (pCacheObj)->typ##_head = next;                        \
  (pNode)->next   = NULL;                                          \
  (pNode)->prev   = NULL;                                          \
  (pNode)->owner  = NULL;                                          \
                                                                   \
  pNext = next;                                                    \
} while (0)

static void append_node_in_hash(SCacheObj *pCacheObj, SCacheDataNode *pNode) {
  append_node_impl(pCacheObj, pNode, hash);
}

static SCacheDataNode* remove_node_in_hash(SCacheObj *pCacheObj, SCacheDataNode *pNode) {
  SCacheDataNode *pNext = NULL;
  remove_node_impl(pCacheObj, pNode, hash, pNext);
  return pNext;
}

static void append_node_in_trash(SCacheObj *pCacheObj, SCacheDataNode *pNode) {
  append_node_impl(pCacheObj, pNode, trash);
}

static SCacheDataNode* remove_node_in_trash(SCacheObj *pCacheObj, SCacheDataNode *pNode) {
  SCacheDataNode *pNext = NULL;
  remove_node_impl(pCacheObj, pNode, trash, pNext);
  return pNext;
}

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

static void wakeup(SCacheObj *pCacheObj) {
  if (pCacheObj->efd == -1) return;

  int64_t v = 1;
  write(pCacheObj->efd, &v, sizeof(v));
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
 * release node
 * @param pCacheObj      cache object
 * @param pNode     data node
 */
static FORCE_INLINE SCacheDataNode* taosCacheReleaseNode(SCacheObj *pCacheObj, SCacheDataNode *pNode) {
  if (pNode->signature != (uint64_t)pNode) {
    uError("key:%s, %p data is invalid, or has been released", pNode->key, pNode);
    // shall not reach here
    assert(0);
    return NULL;
  }

  int32_t ref = T_REF_VAL_GET(pNode);
  assert(ref == 0);
  assert(pNode->inTrashCan);
  if (pCacheObj->freeFp) {
    pthread_t me = pthread_self();
    pNode->cb_thread = &me;

    __cache_unlock(pCacheObj);
    pCacheObj->freeFp(pNode->data);
    __cache_wr_lock(pCacheObj);

    pNode->cb_thread = NULL;
  }

  SCacheDataNode *pNext = remove_node_in_trash(pCacheObj, pNode);

  free(pNode);

  return pNext;
}

/**
 * move the old node into trash
 * @param pCacheObj
 * @param pNode
 */
// refactor: once node is moved to trash, it's user's responsibility to release, because hash set does
//           not hold ref any further.
//           although, this module might behave as a broker to liberately release the nodes in trash,
//           this might break up the philosophy: taosCacheAcquire../taosCacheRelease.. shall be called
//           in pair, and make the logic of this module ugly complex.
static FORCE_INLINE SCacheDataNode* taosCacheMoveToTrash(SCacheObj *pCacheObj, SCacheDataNode *pNode) {
  taosHashRemove(pCacheObj->pHashTable, pNode->key, pNode->keySize);
  int ret = T_REF_DEC(pNode);
  assert(ret > 0);

  SCacheDataNode *pNext = remove_node_in_hash(pCacheObj, pNode);

  append_node_in_trash(pCacheObj, pNode);
  pCacheObj->numOfElemsInTrash++;
  pNode->inTrashCan = 1;

  pCacheObj->totalSize -= pNode->size;
  uDebug("cache:%s, key:%p, %p is moved to trash, totalNum:%d totalSize:%" PRId64 "bytes size:%dbytes",
         pCacheObj->name, pNode->key, pNode->data, (int32_t)taosHashGetSize(pCacheObj->pHashTable), pCacheObj->totalSize,
         pNode->size);

  return pNext;
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

  taosHashPut(pCacheObj->pHashTable, key, keyLen, &pNode, sizeof(void *));
  append_node_in_hash(pCacheObj, pNode);
  // let the cacher itself hold 1 ref in hash set
  T_REF_INC(pNode);
  return pNode;
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
  // potential bug: what if user double call taosCachePut/taosCacheRelease with same params?
  SCacheDataNode *pNewNode = NULL;

  int32_t ref = T_REF_VAL_GET(pNode);
  assert(ref>=1);
  if (ref == 1 && !pNode->inTrashCan) {
    // only the cacher itself holding the ref in hash set
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

    // the address of this node may be changed, so the prev and next element should update the corresponding pointer
    taosHashPut(pCacheObj->pHashTable, key, keyLen, &pNewNode, sizeof(void *));
    // not need to refresh the node list
  } else {
    if (!pNode->inTrashCan) {
      taosCacheMoveToTrash(pCacheObj, pNode);
    }

    pNewNode = taosAddToCacheImpl(pCacheObj, key, keyLen, pData, dataSize, duration);
    // what if failed here? we simply don't recover pNode from trash back to hash set
  }

  return pNewNode;
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

  pCacheObj->efd = eventfd(0, 0);
  if (pCacheObj->efd == -1) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    free(pCacheObj);
    return NULL;
  }

  pCacheObj->pHashTable = taosHashInit(128, taosGetDefaultHashFunction(keyType), false);
  pCacheObj->name = strdup(cacheName);
  if (pCacheObj->pHashTable == NULL) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    close(pCacheObj->efd); pCacheObj->efd = -1;
    free(pCacheObj);
    return NULL;
  }

  // set free cache node callback function for hash table
  pCacheObj->freeFp = fn;
  pCacheObj->refreshTime = refreshTimeInSeconds * 1000;
  pCacheObj->extendLifespan = extendLifespan;

  if (__cache_lock_init(pCacheObj) != 0) {
    uError("failed to init lock, reason:%s", strerror(errno));

    taosHashCleanup(pCacheObj->pHashTable);
    close(pCacheObj->efd); pCacheObj->efd = -1;
    free(pCacheObj);
    return NULL;
  }

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  pthread_create(&pCacheObj->refreshWorker, &thattr, taosCacheTimedRefresh, pCacheObj);

  pthread_attr_destroy(&thattr);
  return pCacheObj;
}

void *taosCachePut(SCacheObj *pCacheObj, const void *key, size_t keyLen, const void *pData, size_t dataSize, int duration) {
  SCacheDataNode *pNode;
  assert(pCacheObj);
  assert(pCacheObj->pHashTable);
  __cache_wr_lock(pCacheObj);

  if (pCacheObj->deleting) {
    __cache_unlock(pCacheObj);
    return NULL;
  }

  SCacheDataNode **pt = (SCacheDataNode **)taosHashGet(pCacheObj->pHashTable, key, keyLen);
  SCacheDataNode * pOld = (pt != NULL) ? (*pt) : NULL;

  if (pOld == NULL) {  // do addedTime to cache
    pNode = taosAddToCacheImpl(pCacheObj, key, keyLen, pData, dataSize, duration * 1000L);
    if (NULL != pNode) {
      pCacheObj->totalSize += pNode->size;

      uDebug("cache:%s, key:%p, %p added into cache, added:%" PRIu64 ", expire:%" PRIu64 ", totalNum:%d totalSize:%" PRId64
             "bytes size:%" PRId64 "bytes",
             pCacheObj->name, key, pNode->data, pNode->addedTime, pNode->expireTime,
             (int32_t)taosHashGetSize(pCacheObj->pHashTable), pCacheObj->totalSize, (int64_t)dataSize);
    } else {
      uError("cache:%s, key:%p, failed to added into cache, out of memory", pCacheObj->name, key);
    }
  } else {  // old data exists, update the node
    pNode = taosUpdateCacheImpl(pCacheObj, pOld, key, (int32_t)keyLen, pData, (uint32_t)dataSize, duration * 1000L);
    uDebug("cache:%s, key:%p, %p exist in cache, updated old:%p", pCacheObj->name, key, pNode->data, pOld);
  }
  if (pNode) {
    // inc ref before return pNode to caller
    T_REF_INC(pNode);
  }

  __cache_unlock(pCacheObj);

  return (pNode != NULL) ? pNode->data : NULL;
}

void *taosCacheAcquireByKey(SCacheObj *pCacheObj, const void *key, size_t keyLen) {
  assert(pCacheObj);
  assert(pCacheObj->pHashTable);
  __cache_rd_lock(pCacheObj);

  if (pCacheObj->deleting) {
    __cache_unlock(pCacheObj);
    return NULL;
  }

  SCacheDataNode *pNode = NULL;
  void *pData           = NULL;

  SCacheDataNode **ptNode = (SCacheDataNode **)taosHashGet(pCacheObj->pHashTable, key, keyLen);
  if (ptNode) {
    pNode = *ptNode;
    assert(pNode);

    if (pNode->cb_thread == NULL) {
      T_REF_INC(pNode);
      pData = pNode->data;
    }
  }

  __cache_unlock(pCacheObj);

  if (pNode != NULL) {
    int32_t ref = T_REF_VAL_GET(pNode);
    atomic_add_fetch_32(&pCacheObj->statistics.hitCount, 1);
    uDebug("cache:%s, key:%p, %p is retrieved from cache, refcnt:%d", pCacheObj->name, key, pData, ref);
  } else {
    atomic_add_fetch_32(&pCacheObj->statistics.missCount, 1);
    uDebug("cache:%s, key:%p, not in cache, retrieved failed", pCacheObj->name, key);
  }

  atomic_add_fetch_32(&pCacheObj->statistics.totalAccess, 1);
  return pData;
}

void* taosCacheUpdateExpireTimeByName(SCacheObj *pCacheObj, void *key, size_t keyLen, uint64_t expireTime) {
  assert(pCacheObj);
  assert(pCacheObj->pHashTable);
  __cache_rd_lock(pCacheObj);

  if (pCacheObj->deleting) {
    __cache_unlock(pCacheObj);
    return NULL;
  }

  SCacheDataNode *pNode = NULL;
  void *pData           = NULL;

  SCacheDataNode **ptNode = (SCacheDataNode **)taosHashGet(pCacheObj->pHashTable, key, keyLen);
  if (ptNode) {
    pNode = *ptNode;
    assert(pNode);

    if (pNode->cb_thread == NULL) {
      T_REF_INC(pNode);
      pNode->expireTime = expireTime; // taosGetTimestampMs() + (*ptNode)->lifespan;
      pData = pNode->data;
      // no need to wakeup working thread, since this node is still holded by the caller
    }
  }

  __cache_unlock(pCacheObj);

  if (pNode != NULL) {
    int32_t ref = T_REF_VAL_GET(pNode);
    atomic_add_fetch_32(&pCacheObj->statistics.hitCount, 1);
    uDebug("cache:%s, key:%p, %p is retrieved from cache, refcnt:%d", pCacheObj->name, key, pData, ref);
  } else {
    atomic_add_fetch_32(&pCacheObj->statistics.missCount, 1);
    uDebug("cache:%s, key:%p, not in cache, retrieved failed", pCacheObj->name, key);
  }

  atomic_add_fetch_32(&pCacheObj->statistics.totalAccess, 1);
  return pData;
}

void *taosCacheAcquireByData(SCacheObj *pCacheObj, void *data) {
  assert(pCacheObj);
  assert(data);

  __cache_rd_lock(pCacheObj);
  if (pCacheObj->deleting) {
    __cache_unlock(pCacheObj);
    return NULL;
  }

  size_t          offset = offsetof(SCacheDataNode, data);
  SCacheDataNode *pNode = (SCacheDataNode *)((char *)data - offset);

  if (pNode->signature != (uint64_t)pNode) {
    uError("key: %p the data from cache is invalid", pNode);
    __cache_unlock(pCacheObj);
    // shall we assert here to punish?
    return NULL;
  }

  if (pNode->cb_thread) {
    __cache_unlock(pCacheObj);
    return NULL;
  }

  int32_t ref = T_REF_INC(pNode);
  uDebug("cache:%s, data: %p acquired by data in cache, refcnt:%d", pCacheObj->name, pNode->data, ref);

  __cache_unlock(pCacheObj);

  return data;
}

void *taosCacheTransfer(SCacheObj *pCacheObj, void **data) {
  assert(pCacheObj);
  assert(data);
  assert(*data);

  __cache_rd_lock(pCacheObj);
  if (pCacheObj->deleting) {
    __cache_unlock(pCacheObj);
    return NULL;
  }

  size_t          offset = offsetof(SCacheDataNode, data);
  SCacheDataNode *pNode  = (SCacheDataNode *)((char *)(*data) - offset);

  if (pNode->signature != (uint64_t)pNode) {
    uError("key: %p the data from cache is invalid", pNode);
    __cache_unlock(pCacheObj);
    return NULL;
  }

  if (pNode->cb_thread) {
    __cache_unlock(pCacheObj);
    return NULL;
  }

  int32_t ref = T_REF_VAL_GET(pNode);
  assert(ref >= 2);

  char *d = *data;

  // clear its reference to old area
  *data = NULL;

  __cache_unlock(pCacheObj);
  return d;
}

void taosCacheRelease(SCacheObj *pCacheObj, void **data, bool _remove) {
  assert(pCacheObj);
  assert(data);
  assert(*data);
  __cache_rd_lock(pCacheObj);

  size_t offset = offsetof(SCacheDataNode, data);
  SCacheDataNode *pNode = (SCacheDataNode *)((char *)(*data) - offset);

  if (pNode->signature != (uint64_t)pNode) {
    uError("%p, release invalid cache data", pNode);
    __cache_unlock(pCacheObj);
    return;
  }

  if (pNode->cb_thread) {
    assert(pNode->cb_thread == NULL); // impossible by design
    __cache_unlock(pCacheObj);
    return;
  }

  int32_t ref = T_REF_INC(pNode);
  assert(ref >= 2);

  *data = NULL;

  // note: extend lifespan before dec ref count
  bool inTrashCan = pNode->inTrashCan;

  if (pCacheObj->extendLifespan && (!inTrashCan) && (!_remove)) {
    atomic_store_64(&pNode->expireTime, pNode->lifespan + taosGetTimestampMs());
    uDebug("cache:%s data:%p extend life time to %"PRId64 "  before release", pCacheObj->name, pNode->data, pNode->expireTime);
  }

  if (_remove) {
    if (!pNode->inTrashCan) {
      taosCacheMoveToTrash(pCacheObj, pNode);
    }
  }

  ref = T_REF_DEC(pNode);
  ref = T_REF_DEC(pNode);
  assert(ref >= 0);
  if (ref == 0) {
    if (!pCacheObj->deleting) {
      wakeup(pCacheObj); // let working thread to take care
    }
  }

  __cache_unlock(pCacheObj);
}

void taosCacheEmpty(SCacheObj *pCacheObj) {
  // ref to comments in doCacheRefresh
  __cache_rd_lock(pCacheObj);
  int zombi_found = 0;
  SCacheDataNode *pNode = pCacheObj->hash_head;
  while (pNode) {
    uInfo("==%s[%d]==", __FILE__, __LINE__);
    if (pNode->cb_thread) {
      // pNode is undergoing call back, let it be
      pNode = pNode->next;
      continue;
    }

    SCacheDataNode *pNext = NULL;

    assert(!pNode->inTrashCan);
    int32_t ref = T_REF_INC(pNode);
    pNext = taosCacheMoveToTrash(pCacheObj, pNode);
    ref = T_REF_DEC(pNode);
    assert(ref >= 0);
    if (ref==0) zombi_found = 1;

    pNode = pNext;
  }
  if (zombi_found) {
    if (!pCacheObj->deleting) {
      wakeup(pCacheObj); // let working thread to take care
    }
  }
  __cache_unlock(pCacheObj);
}

void taosCacheCleanup(SCacheObj *pCacheObj) {
  uInfo("==%s[%d]==", __FILE__, __LINE__);
  __cache_wr_lock(pCacheObj);
  uInfo("==%s[%d]==", __FILE__, __LINE__);
  // it's caller's duty to keep it from double-cleanup!
  if (pCacheObj == NULL) {
    __cache_unlock(pCacheObj);
    return;
  }

  assert(pCacheObj->deleting==0);
  pCacheObj->deleting = 1;
  wakeup(pCacheObj);
  uInfo("cache:%s waiting thread to exit", pCacheObj->name);
  __cache_unlock(pCacheObj);
  pthread_join(pCacheObj->refreshWorker, NULL);
  __cache_wr_lock(pCacheObj);

  uInfo("cache:%s will be cleaned up", pCacheObj->name);
  doCleanupDataCache(pCacheObj);
  if (pCacheObj->efd != -1) {
    close(pCacheObj->efd);
    pCacheObj->efd = -1;
  }
  __cache_unlock(pCacheObj);

  __cache_lock_destroy(pCacheObj);

  taosTFree(pCacheObj->name);
  memset(pCacheObj, 0, sizeof(SCacheObj));
  free(pCacheObj);
}

SCacheDataNode *taosCreateCacheNode(const char *key, size_t keyLen, const char *pData, size_t size,
                                           uint64_t duration) {
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

static void doSwipeTrash(SCacheObj* pCacheObj) {
  SCacheDataNode *pNode = pCacheObj->trash_head;
  while (pNode) {
    int32_t ref = T_REF_VAL_GET(pNode);
    assert(ref >= 0);
    if (ref == 0) {
      assert(pNode->inTrashCan); 
      pNode = taosCacheReleaseNode(pCacheObj, pNode);
    } else {
      pNode = pNode->next;
    }
  }
}

void doCleanupDataCache(SCacheObj *pCacheObj) {
  // ref to comments in doCacheRefresh
  int busy = 0;
  pthread_t me = pthread_self();
  do {
    SCacheDataNode *pNode = pCacheObj->hash_head;
    while (pNode) {
      uInfo("==%s[%d]==", __FILE__, __LINE__);
      if (pNode->cb_thread) {
        if (pthread_equal(*pNode->cb_thread, me)) {
          uError("seems like you are calling from within cache callback, and want to `suicide`, which is forbidden now!");
          assert(0);
          // note:
          // we might create another side thread to delay the `suicide`, but what if thread creation failed here at run time?
        }
        // let it be, we'll go back later on
        pNode = pNode->next;
        busy = 1;
        continue;
      }

      SCacheDataNode *pNext = NULL;

      assert(!pNode->inTrashCan);
      int32_t ref = T_REF_INC(pNode);
      pNext = taosCacheMoveToTrash(pCacheObj, pNode);
      ref = T_REF_DEC(pNode);
      assert(ref >= 0);
      // doSwipeTrash will take care later when ref == 0

      // we believe a correct user of this module, will definitely trigger dec-ref sooner or later
      // otherwise, we might deliberately trigger cache release procedure as usual here, only in case
      // whenever caller chooses to call taosCacheCleanup, it also means to this module,
      // that it'll never call any other api of this module.
      // this contract/behavior shall be clearly stated in the header file. currently, we don't have.
      // thus we choose not to trigger now!

      pNode = pNext;
    }
    if (busy) {
      uInfo("==%s[%d]==", __FILE__, __LINE__);
      // yield control to other threads
      __cache_unlock(pCacheObj);
      taosMsleep(100);
      __cache_wr_lock(pCacheObj);
    }
  } while (busy);

  while (pCacheObj->trash_head) {
    uInfo("==%s[%d]==", __FILE__, __LINE__);
    doSwipeTrash(pCacheObj);
    if (pCacheObj->trash_head == NULL) break;
    // see above comments
    // simply saying: this module has no ownership of data in trash
    __cache_unlock(pCacheObj);
    taosMsleep(100);
    __cache_wr_lock(pCacheObj);
  }

  taosHashCleanup(pCacheObj->pHashTable);
}

static void doCacheRefresh(SCacheObj* pCacheObj, int64_t time, __cache_free_fn_t fp) {
  // SHashMutableIterator is not thread-safe
  // and holding the whole hash-set in lock will result in performance-set-back
  SCacheDataNode *pNode = pCacheObj->hash_head;
  while (pNode) {
    if (pNode->cb_thread) {
      // pNode is undergoing call back, let it be
      pNode = pNode->next;
      continue;
    }

    int32_t ref = T_REF_VAL_GET(pNode);
    assert(ref >= 1);
    assert(!pNode->inTrashCan);
    if ((pCacheObj->deleting || pNode->expireTime < (uint64_t)time) && ref == 1) {
      // if it's in deleting-state or it's timeup, and only this module holding the ref
      int32_t ref = T_REF_INC(pNode);
      SCacheDataNode *pNext = taosCacheMoveToTrash(pCacheObj, pNode);
      ref = T_REF_DEC(pNode);
      assert(ref >= 0);
      // working thread will take care in doSwipeTrash when ref == 0
      pNode = pNext;
      continue;
    }

    if (fp) {
      pthread_t me = pthread_self();
      pNode->cb_thread = &me;

      __cache_unlock(pCacheObj);
      fp(pNode->data);
      __cache_wr_lock(pCacheObj);

      pNode->cb_thread = NULL;
    }

    pNode = pNode->next;
  }
}

static int64_t get_closest_ms(SCacheObj *pCacheObj) {
  SCacheDataNode *pNode = pCacheObj->hash_head;
  int64_t closest_ms = -1;
  while (pNode) {
    if (pNode->cb_thread) {
      // pNode is undergoing call back, let it be
      pNode = pNode->next;
      continue;
    }

    assert(!pNode->inTrashCan);
    if (closest_ms==-1 || closest_ms>pNode->expireTime) {
      closest_ms = pNode->expireTime;
    }

    pNode = pNode->next;
  }

  return closest_ms;
}

void* taosCacheTimedRefresh(void *handle) {
  SCacheObj* pCacheObj = handle;
  if (pCacheObj == NULL) {
    uDebug("object is destroyed. no refresh retry");
    return NULL;
  }

  while(1) {
    __cache_wr_lock(pCacheObj);

    int64_t now = taosGetTimestampMs();
    doCacheRefresh(pCacheObj, now, NULL);
    doSwipeTrash(pCacheObj);

    fd_set rfds;
    struct timeval tv   = {0};
    struct timeval *ptv = &tv;

    FD_ZERO(&rfds);
    FD_SET(pCacheObj->efd, &rfds);

    int64_t closest_ms = get_closest_ms(pCacheObj);
    now = taosGetTimestampMs();
    if (closest_ms == -1) {
      ptv = NULL;
      // block select call until wakeup
    } else {
      int64_t delta = closest_ms - now;
      if (delta > 0) {
        tv.tv_sec      = delta / 1000;
        tv.tv_usec     = delta % 1000 * 1000;
      }
    }

    if (pCacheObj->deleting) {
      uDebug("%s refresh threads quit", pCacheObj->name);
      __cache_unlock(pCacheObj);
      break;
    }

    __cache_unlock(pCacheObj);
    int n = select(pCacheObj->efd + 1, &rfds, NULL, NULL, ptv);
    __cache_wr_lock(pCacheObj);

    if (n == -1) {
      if (errno == EINTR) {
        __cache_unlock(pCacheObj);
        continue;
      }
      uError("internal logic error or corruption, select failed, we have no way to recover by now. cache: %s[%s]",
             pCacheObj->name, strerror(errno));
      assert(0);
    } else if (n==1) {
      int64_t v = 0;
      read(pCacheObj->efd, &v, sizeof(v)); // read to clear it
    }

    pCacheObj->statistics.refreshCount++;

    __cache_unlock(pCacheObj);
  }

  __cache_wr_lock(pCacheObj);
  int64_t now = taosGetTimestampMs();
  doCacheRefresh(pCacheObj, now, NULL);
  doSwipeTrash(pCacheObj);
  __cache_unlock(pCacheObj);

  return NULL;
}

void taosCacheRefresh(SCacheObj *pCacheObj, __cache_free_fn_t fp) {
  if (pCacheObj == NULL) {
    return;
  }

  __cache_wr_lock(pCacheObj);
  int64_t now = taosGetTimestampMs();
  doCacheRefresh(pCacheObj, now, fp);
  __cache_unlock(pCacheObj);
}

