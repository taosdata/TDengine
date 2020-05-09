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

#include "tcache.h"
#include "tlog.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "hashutil.h"

#define HASH_MAX_CAPACITY   (1024*1024*16)
#define HASH_VALUE_IN_TRASH (-1)
#define HASH_DEFAULT_LOAD_FACTOR (0.75)
#define HASH_INDEX(v, c) ((v) & ((c)-1))

/**
 * todo: refactor to extract the hash table out of cache structure
 */
typedef struct SCacheStatis {
  int64_t missCount;
  int64_t hitCount;
  int64_t totalAccess;
  int64_t refreshCount;
  int32_t numOfCollision;
  int32_t numOfResize;
  int64_t resizeTime;
} SCacheStatis;

typedef struct _cache_node_t {
  char *                key;  // null-terminated string
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
  uint32_t hashVal;   // the hash value of key, if hashVal == HASH_VALUE_IN_TRASH, this node is moved to trash
  uint32_t nodeSize;  // allocated size for current SDataNode
  char     data[];
} SDataNode;

typedef uint32_t (*_hashFunc)(const char *, uint32_t);

typedef struct {
  SDataNode **hashList;
  int         capacity;
  int         size;
  int64_t     totalSize;  // total allocated buffer in this hash table, SCacheObj is not included.
  int64_t     refreshTime;

  /*
   * to accommodate the old datanode which has the same key value of new one in hashList
   * when an new node is put into cache, if an existed one with the same key:
   * 1. if the old one does not be referenced, update it.
   * 2. otherwise, move the old one to pTrash, add the new one.
   *
   * when the node in pTrash does not be referenced, it will be release at the expired time
   */
  SDataNode *  pTrash;
  void *       tmrCtrl;
  void *       pTimer;
  SCacheStatis statistics;
  _hashFunc    hashFp;
  int          numOfElemsInTrash;  // number of element in trash
  int16_t      deleting;           // set the deleting flag to stop refreshing asap.

#if defined        LINUX
  pthread_rwlock_t lock;
#else
  pthread_mutex_t lock;
#endif

} SCacheObj;

static FORCE_INLINE void __cache_wr_lock(SCacheObj *pObj) {
#if defined LINUX
  pthread_rwlock_wrlock(&pObj->lock);
#else
  pthread_mutex_lock(&pObj->lock);
#endif
}

static FORCE_INLINE void __cache_rd_lock(SCacheObj *pObj) {
#if defined LINUX
  pthread_rwlock_rdlock(&pObj->lock);
#else
  pthread_mutex_lock(&pObj->lock);
#endif
}

static FORCE_INLINE void __cache_unlock(SCacheObj *pObj) {
#if defined LINUX
  pthread_rwlock_unlock(&pObj->lock);
#else
  pthread_mutex_unlock(&pObj->lock);
#endif
}

static FORCE_INLINE int32_t __cache_lock_init(SCacheObj *pObj) {
#if defined LINUX
  return pthread_rwlock_init(&pObj->lock, NULL);
#else
  return pthread_mutex_init(&pObj->lock, NULL);
#endif
}

static FORCE_INLINE void __cache_lock_destroy(SCacheObj *pObj) {
#if defined LINUX
  pthread_rwlock_destroy(&pObj->lock);
#else
  pthread_mutex_destroy(&pObj->lock);
#endif
}

static FORCE_INLINE int32_t taosHashTableLength(int32_t length) {
  int32_t trueLength = MIN(length, HASH_MAX_CAPACITY);

  int32_t i = 4;
  while (i < trueLength) i = (i << 1);
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
  pNewNode->addTime = (uint64_t)taosGetTimestampMs();
  pNewNode->time = pNewNode->addTime + lifespan;

  pNewNode->key = pNewNode->data + dataSize;
  strcpy(pNewNode->key, key);

  pNewNode->signature = (uint64_t)pNewNode;
  pNewNode->nodeSize = (uint32_t)totalSize;

  return pNewNode;
}

/**
 * hash key function
 *
 * @param key    key string
 * @param len    length of key
 * @return       hash value
 */
static FORCE_INLINE uint32_t taosHashKey(const char *key, uint32_t len) { return MurmurHash3_32(key, len); }

/**
 * add object node into trash, and this object is closed for referencing if it is add to trash
 * It will be removed until the pNode->refCount == 0
 * @param pObj    Cache object
 * @param pNode   Cache slot object
 */
static void taosAddToTrash(SCacheObj *pObj, SDataNode *pNode) {
  if (pNode->hashVal == HASH_VALUE_IN_TRASH) { /* node is already in trash */
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
static void taosClearCacheTrash(SCacheObj *pObj, bool force) {
  __cache_wr_lock(pObj);

  if (pObj->numOfElemsInTrash == 0) {
    if (pObj->pTrash != NULL) {
      pError("key:inconsistency data in cache, numOfElem in trash:%d", pObj->numOfElemsInTrash);
    }
    pObj->pTrash = NULL;

    __cache_unlock(pObj);
    return;
  }

  SDataNode *pNode = pObj->pTrash;

  while (pNode) {
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
  __cache_unlock(pObj);
}

/**
 * add data node into cache
 * @param pObj    cache object
 * @param pNode   Cache slot object
 */
static void taosAddNodeToHashTable(SCacheObj *pObj, SDataNode *pNode) {
  int32_t slotIndex = HASH_INDEX(pNode->hashVal, pObj->capacity);
  pNode->next = pObj->hashList[slotIndex];

  if (pObj->hashList[slotIndex] != NULL) {
    (pObj->hashList[slotIndex])->prev = pNode;
    pObj->statistics.numOfCollision++;
  }
  pObj->hashList[slotIndex] = pNode;

  pObj->size++;
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
  if (pNode->prev != NULL) {
    pNode->prev->next = pNext;
  } else { /* the node is in hashlist, remove it */
    pObj->hashList[HASH_INDEX(pNode->hashVal, pObj->capacity)] = pNext;
  }

  if (pNext != NULL) {
    pNext->prev = pNode->prev;
  }

  pObj->size--;
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
    pObj->hashList[HASH_INDEX(pNode->hashVal, pObj->capacity)] = pNode;
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
static SDataNode *taosGetNodeFromHashTable(SCacheObj *pObj, const char *key, uint32_t keyLen) {
  uint32_t hash = (*pObj->hashFp)(key, keyLen);

  int32_t    slot = HASH_INDEX(hash, pObj->capacity);
  SDataNode *pNode = pObj->hashList[slot];

  while (pNode) {
    if (strcmp(pNode->key, key) == 0) break;

    pNode = pNode->next;
  }

  if (pNode) {
    assert(HASH_INDEX(pNode->hashVal, pObj->capacity) == slot);
  }

  return pNode;
}

/**
 * resize the hash list if the threshold is reached
 *
 * @param pObj
 */
static void taosHashTableResize(SCacheObj *pObj) {
  if (pObj->size < pObj->capacity * HASH_DEFAULT_LOAD_FACTOR) {
    return;
  }

  // double the original capacity
  pObj->statistics.numOfResize++;
  SDataNode *pNode = NULL;
  SDataNode *pNext = NULL;

  int32_t newSize = pObj->capacity << 1;
  if (newSize > HASH_MAX_CAPACITY) {
    pTrace("current capacity:%d, maximum capacity:%d, no resize applied due to limitation is reached",
           pObj->capacity, HASH_MAX_CAPACITY);
    return;
  }

  int64_t     st = taosGetTimestampUs();
  SDataNode **pList = realloc(pObj->hashList, sizeof(SDataNode *) * newSize);
  if (pList == NULL) {
    pTrace("cache resize failed due to out of memory, capacity remain:%d", pObj->capacity);
    return;
  }

  pObj->hashList = pList;

  int32_t inc = newSize - pObj->capacity;
  memset(&pObj->hashList[pObj->capacity], 0, inc * sizeof(SDataNode *));

  pObj->capacity = newSize;

  for (int32_t i = 0; i < pObj->capacity; ++i) {
    pNode = pObj->hashList[i];

    while (pNode) {
      int32_t j = HASH_INDEX(pNode->hashVal, pObj->capacity);
      if (j == i) {  // this key resides in the same slot, no need to relocate it
        pNode = pNode->next;
      } else {
        pNext = pNode->next;

        // remove from current slot
        if (pNode->prev != NULL) {
          pNode->prev->next = pNode->next;
        } else {
          pObj->hashList[i] = pNode->next;
        }

        if (pNode->next != NULL) {
          (pNode->next)->prev = pNode->prev;
        }

        // added into new slot
        pNode->next = NULL;
        pNode->prev = NULL;

        pNode->next = pObj->hashList[j];

        if (pObj->hashList[j] != NULL) {
          (pObj->hashList[j])->prev = pNode;
        }
        pObj->hashList[j] = pNode;

        // continue
        pNode = pNext;
      }
    }
  }

  int64_t et = taosGetTimestampUs();
  pObj->statistics.resizeTime += (et - st);

  pTrace("cache resize completed, new capacity:%d, load factor:%f, elapsed time:%fms", pObj->capacity,
         ((double)pObj->size) / pObj->capacity, (et - st) / 1000.0);
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

  pTrace("key:%s is removed from cache,total:%d,size:%ldbytes", pNode->key, pObj->size, pObj->totalSize);
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

  // only a node is not referenced by any other object, in-place update it
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

    // update the timestamp information for updated key/value
    pNewNode->addTime = taosGetTimestampMs();
    pNewNode->time = pNewNode->addTime + keepTime;

    atomic_add_fetch_32(&pNewNode->refCount, 1);

    // the address of this node may be changed, so the prev and next element should update the corresponding pointer
    taosUpdateInHashTable(pObj, pNewNode);
  } else {
    int32_t hashVal = pNode->hashVal;
    taosCacheMoveNodeToTrash(pObj, pNode);

    pNewNode = taosCreateHashNode(key, keyLen, pData, dataSize, keepTime);
    if (pNewNode == NULL) {
      return NULL;
    }

    atomic_add_fetch_32(&pNewNode->refCount, 1);

    assert(hashVal == (*pObj->hashFp)(key, keyLen - 1));
    pNewNode->hashVal = hashVal;

    // add new element to hashtable
    taosAddNodeToHashTable(pObj, pNewNode);
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

  atomic_add_fetch_32(&pNode->refCount, 1);
  pNode->hashVal = (*pObj->hashFp)(key, keyLen - 1);
  taosAddNodeToHashTable(pObj, pNode);

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
  if (pObj == NULL || pObj->capacity == 0) return NULL;

  uint32_t keyLen = (uint32_t)strlen(key) + 1;

  __cache_wr_lock(pObj);

  SDataNode *pOldNode = taosGetNodeFromHashTable(pObj, key, keyLen - 1);

  if (pOldNode == NULL) {  // do add to cache
    // check if the threshold is reached
    taosHashTableResize(pObj);

    pNode = taosAddToCacheImpl(pObj, key, keyLen, pData, dataSize, keepTime * 1000L);
    if (NULL != pNode) {
      pTrace(
          "key:%s %p added into cache, slot:%d, addTime:%" PRIu64 ", expireTime:%" PRIu64 ", cache total:%d, "
          "size:%" PRId64 " bytes, collision:%d",
          pNode->key, pNode, HASH_INDEX(pNode->hashVal, pObj->capacity), pNode->addTime, pNode->time, pObj->size,
          pObj->totalSize, pObj->statistics.numOfCollision);
    } else {
      pError("key:%s failed to added into cache, out of memory", key);
    }
  } else {  // old data exists, update the node
    pNode = taosUpdateCacheImpl(pObj, pOldNode, key, keyLen, pData, dataSize, keepTime * 1000L);
    pTrace("key:%s %p exist in cache, updated", key, pNode);
  }

  __cache_unlock(pObj);

  return (pNode != NULL) ? pNode->data : NULL;
}

static FORCE_INLINE void taosDecRef(SDataNode *pNode) {
  if (pNode == NULL) {
    return;
  }

  if (pNode->refCount > 0) {
    atomic_sub_fetch_32(&pNode->refCount, 1);
    pTrace("key:%s is released by app.refcnt:%d", pNode->key, pNode->refCount);
  } else {
    /*
     * safety check.
     * app may false releases cached object twice, to decrease the refcount more than acquired
     */
    pError("key:%s is released by app more than referenced.refcnt:%d", pNode->key, pNode->refCount);
  }
}

/**
 * remove data in cache, the data will not be removed immediately.
 * if it is referenced by other object, it will be remain in cache
 * @param handle
 * @param data
 */
void taosRemoveDataFromCache(void *handle, void **data, bool _remove) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL || pObj->capacity == 0 || (*data) == NULL || (pObj->size + pObj->numOfElemsInTrash == 0)) return;

  size_t     offset = offsetof(SDataNode, data);
  SDataNode *pNode = (SDataNode *)((char *)(*data) - offset);

  if (pNode->signature != (uint64_t)pNode) {
    pError("key: %p release invalid cache data", pNode);
    return;
  }

  *data = NULL;

  if (_remove) {
    __cache_wr_lock(pObj);
    // pNode may be released immediately by other thread after the reference count of pNode is set to 0,
    // So we need to lock it in the first place.
    taosDecRef(pNode);
    taosCacheMoveNodeToTrash(pObj, pNode);

    __cache_unlock(pObj);
  } else {
    taosDecRef(pNode);
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
  if (pObj == NULL || pObj->capacity == 0) return NULL;

  uint32_t keyLen = (uint32_t)strlen(key);

  __cache_rd_lock(pObj);

  SDataNode *ptNode = taosGetNodeFromHashTable(handle, key, keyLen);
  if (ptNode != NULL) {
    atomic_add_fetch_32(&ptNode->refCount, 1);
  }

  __cache_unlock(pObj);

  if (ptNode != NULL) {
    atomic_add_fetch_32(&pObj->statistics.hitCount, 1);
    pTrace("key:%s is retrieved from cache,refcnt:%d", key, ptNode->refCount);
  } else {
    atomic_add_fetch_32(&pObj->statistics.missCount, 1);
    pTrace("key:%s not in cache,retrieved failed", key);
  }

  atomic_add_fetch_32(&pObj->statistics.totalAccess, 1);
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
  if (pObj == NULL || pObj->capacity == 0) return NULL;

  SDataNode *pNew = NULL;

  uint32_t keyLen = strlen(key) + 1;

  __cache_wr_lock(pObj);

  SDataNode *pNode = taosGetNodeFromHashTable(handle, key, keyLen - 1);

  if (pNode == NULL) {  // object has been released, do add operation
    pNew = taosAddToCacheImpl(pObj, key, keyLen, pData, size, duration * 1000L);
    pWarn("key:%s does not exist, update failed,do add to cache.total:%d,size:%ldbytes", key, pObj->size,
          pObj->totalSize);
  } else {
    pNew = taosUpdateCacheImpl(pObj, pNode, key, keyLen, pData, size, duration * 1000L);
    pTrace("key:%s updated.expireTime:%" PRIu64 ".refCnt:%d", key, pNode->time, pNode->refCount);
  }

  __cache_unlock(pObj);
  return (pNew != NULL) ? pNew->data : NULL;
}

static void doCleanUpDataCache(SCacheObj* pObj) {
  SDataNode *pNode, *pNext;

  __cache_wr_lock(pObj);

  if (pObj->hashList && pObj->size > 0) {
    for (int i = 0; i < pObj->capacity; ++i) {
      pNode = pObj->hashList[i];
      while (pNode) {
        pNext = pNode->next;
        free(pNode);
        pNode = pNext;
      }
    }

    tfree(pObj->hashList);
  }

  __cache_unlock(pObj);

  taosClearCacheTrash(pObj, true);
  __cache_lock_destroy(pObj);

  memset(pObj, 0, sizeof(SCacheObj));

  free(pObj);
}

/**
 * refresh cache to remove data in both hash list and trash, if any nodes' refcount == 0, every pObj->refreshTime
 * @param handle   Cache object handle
 */
void taosRefreshDataCache(void *handle, void *tmrId) {
  SDataNode *pNode, *pNext;
  SCacheObj *pObj = (SCacheObj *)handle;

  if (pObj == NULL || pObj->capacity <= 0) {
    pTrace("object is destroyed. no refresh retry");
    return;
  }

  if (pObj->deleting == 1) {
    doCleanUpDataCache(pObj);
    return;
  }

  uint64_t time = taosGetTimestampMs();
  uint32_t numOfCheck = 0;
  pObj->statistics.refreshCount++;

  int32_t num = pObj->size;

  for (int i = 0; i < pObj->capacity; ++i) {
    // in deleting process, quit refreshing immediately
    if (pObj->deleting == 1) {
      break;
    }

    __cache_wr_lock(pObj);
    pNode = pObj->hashList[i];

    while (pNode) {
      numOfCheck++;
      pNext = pNode->next;

      if (pNode->time <= time && pNode->refCount <= 0) {
        taosCacheReleaseNode(pObj, pNode);
      }
      pNode = pNext;
    }

    /* all data have been checked, not need to iterate further */
    if (numOfCheck == num || pObj->size <= 0) {
      __cache_unlock(pObj);
      break;
    }

    __cache_unlock(pObj);
  }

  if (pObj->deleting == 1) { // clean up resources and abort
    doCleanUpDataCache(pObj);
  } else {
    taosClearCacheTrash(pObj, false);
    taosTmrReset(taosRefreshDataCache, pObj->refreshTime, pObj, pObj->tmrCtrl, &pObj->pTimer);
  }
}

/**
 *
 * @param handle
 * @param tmrId
 */
void taosClearDataCache(void *handle) {
  SDataNode *pNode, *pNext;
  SCacheObj *pObj = (SCacheObj *)handle;

  int32_t capacity = pObj->capacity;

  for (int i = 0; i < capacity; ++i) {
    __cache_wr_lock(pObj);

    pNode = pObj->hashList[i];

    while (pNode) {
      pNext = pNode->next;
      taosCacheMoveNodeToTrash(pObj, pNode);
      pNode = pNext;
    }

    pObj->hashList[i] = NULL;

    __cache_unlock(pObj);
  }

  taosClearCacheTrash(pObj, false);
}

/**
 * @param capacity          maximum slots available for hash elements
 * @param tmrCtrl           timer ctrl
 * @param refreshTime       refresh operation interval time, the maximum survival time when one element is expired and
 *                          not referenced by other objects
 * @return
 */
void *taosInitDataCache(int capacity, void *tmrCtrl, int64_t refreshTime) {
  if (tmrCtrl == NULL || refreshTime <= 0 || capacity <= 0) {
    return NULL;
  }

  SCacheObj *pObj = (SCacheObj *)calloc(1, sizeof(SCacheObj));
  if (pObj == NULL) {
    pError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  // the max slots is not defined by user
  pObj->capacity = taosHashTableLength(capacity);
  assert((pObj->capacity & (pObj->capacity - 1)) == 0);

  pObj->hashFp = taosHashKey;
  pObj->refreshTime = refreshTime * 1000;

  pObj->hashList = (SDataNode **)calloc(1, sizeof(SDataNode *) * pObj->capacity);
  if (pObj->hashList == NULL) {
    free(pObj);
    pError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  pObj->tmrCtrl = tmrCtrl;
  taosTmrReset(taosRefreshDataCache, pObj->refreshTime, pObj, pObj->tmrCtrl, &pObj->pTimer);

  if (__cache_lock_init(pObj) != 0) {
    taosTmrStopA(&pObj->pTimer);
    free(pObj->hashList);
    free(pObj);

    pError("failed to init lock, reason:%s", strerror(errno));
    return NULL;
  }

  return (void *)pObj;
}

/**
 * release all allocated memory and destroy the cache object.
 *
 * This function only set the deleting flag, and the specific work of clean up cache is delegated to
 * taosRefreshDataCache function, which will executed every SCacheObj->refreshTime sec.
 *
 * If the value of SCacheObj->refreshTime is too large, the taosRefreshDataCache function may not be invoked
 * before the main thread terminated, in which case all allocated resources are simply recycled by OS.
 *
 * @param handle
 */
void taosCleanUpDataCache(void *handle) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL) {
    return;
  }

  pObj->deleting = 1;
}

void* taosGetDataFromExists(void* handle, void* data) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL || data == NULL) return NULL;
  
  size_t     offset = offsetof(SDataNode, data);
  SDataNode *ptNode = (SDataNode *)((char *)data - offset);
  
  if (ptNode->signature != (uint64_t) ptNode) {
    pError("key: %p the data from cache is invalid", ptNode);
    return NULL;
  }
  
  int32_t ref = atomic_add_fetch_32(&ptNode->refCount, 1);
  pTrace("%p add ref data in cache, refCnt:%d", data, ref)
  
  // the data if referenced by at least one object, so the reference count must be greater than the value of 2.
  assert(ref >= 2);
  return data;
}

void* taosTransferDataInCache(void* handle, void** data) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL || data == NULL) return NULL;
  
  size_t     offset = offsetof(SDataNode, data);
  SDataNode *ptNode = (SDataNode *)((char *)(*data) - offset);
  
  if (ptNode->signature != (uint64_t) ptNode) {
    pError("key: %p the data from cache is invalid", ptNode);
    return NULL;
  }
  
  assert(ptNode->refCount >= 1);
  
  char* d = *data;
  
  // clear its reference to old area
  *data = NULL;
  
  return d;
}
