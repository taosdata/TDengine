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

#ifndef TDENGINE_TCACHE_H
#define TDENGINE_TCACHE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tlockfree.h"
#include "hash.h"

#if defined(_TD_ARM_32) 
  #define TSDB_CACHE_PTR_KEY  TSDB_DATA_TYPE_INT
  #define TSDB_CACHE_PTR_TYPE int32_t
#else
  #define TSDB_CACHE_PTR_KEY  TSDB_DATA_TYPE_BIGINT  
  #define TSDB_CACHE_PTR_TYPE int64_t
#endif

typedef void (*__cache_free_fn_t)(void*);

typedef struct SCacheStatis {
  int64_t missCount;
  int64_t hitCount;
  int64_t totalAccess;
  int64_t refreshCount;
} SCacheStatis;

struct STrashElem;

typedef struct SCacheDataNode {
  uint64_t           addedTime;    // the added time when this element is added or updated into cache
  uint64_t           lifespan;     // life duration when this element should be remove from cache
  uint64_t           expireTime;   // expire time
  uint64_t           signature;
  struct STrashElem *pTNodeHeader; // point to trash node head
  uint16_t           keySize: 15;  // max key size: 32kb
  bool               inTrashcan: 1;// denote if it is in trash or not
  uint32_t           size;         // allocated size for current SCacheDataNode
  T_REF_DECLARE()
  char              *key;
  char               data[];
} SCacheDataNode;

typedef struct STrashElem {
  struct STrashElem *prev;
  struct STrashElem *next;
  SCacheDataNode    *pData;
} STrashElem;

/*
 * to accommodate the old data which has the same key value of new one in hashList
 * when an new node is put into cache, if an existed one with the same key:
 * 1. if the old one does not be referenced, update it.
 * 2. otherwise, move the old one to pTrash, addedTime the new one.
 *
 * when the node in pTrash does not be referenced, it will be release at the expired expiredTime
 */
typedef struct {
  int64_t         totalSize;          // total allocated buffer in this hash table, SCacheObj is not included.
  int64_t         refreshTime;
  STrashElem *    pTrash;
  char*           name;
  SCacheStatis    statistics;
  SHashObj *      pHashTable;
  __cache_free_fn_t freeFp;
  uint32_t        numOfElemsInTrash;  // number of element in trash
  uint8_t         deleting;           // set the deleting flag to stop refreshing ASAP.
  pthread_t       refreshWorker;
  bool            extendLifespan;     // auto extend life span when one item is accessed.
  int64_t         checkTick;          // tick used to record the check times of the refresh threads
#if defined(LINUX)
  pthread_rwlock_t lock;
#else
  pthread_mutex_t  lock;
#endif
} SCacheObj;

/**
 * initialize the cache object
 * @param keyType              key type
 * @param refreshTimeInSeconds refresh operation interval time, the maximum survival time when one element is expired
 *                             and not referenced by other objects
 * @param extendLifespan       auto extend lifespan, if accessed
 * @param fn                   free resource callback function
 * @return
 */
SCacheObj *taosCacheInit(int32_t keyType, int64_t refreshTimeInSeconds, bool extendLifespan, __cache_free_fn_t fn, const char *cacheName);

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
void *taosCachePut(SCacheObj *pCacheObj, const void *key, size_t keyLen, const void *pData, size_t dataSize, int durationMS);

/**
 * get data from cache
 * @param pCacheObj     cache object
 * @param key           key
 * @return              cached data or NULL
 */
void *taosCacheAcquireByKey(SCacheObj *pCacheObj, const void *key, size_t keyLen);

/**
 * Add one reference count for the exist data, and assign this data for a new owner.
 * The new owner needs to invoke the taosCacheRelease when it does not need this data anymore.
 * This procedure is a faster version of taosCacheAcquireByKey function, which avoids the sideeffect of the problem of
 * the data is moved to trash, and taosCacheAcquireByKey will fail to retrieve it again.
 *
 * @param handle
 * @param data
 * @return
 */
void *taosCacheAcquireByData(SCacheObj *pCacheObj, void *data);

/**
 * transfer the ownership of data in cache to another object without increasing reference count.
 * @param handle
 * @param data
 * @return
 */
void *taosCacheTransfer(SCacheObj *pCacheObj, void **data);

/**
 * remove data in cache, the data will not be removed immediately.
 * if it is referenced by other object, it will be remain in cache
 * @param handle    cache object
 * @param data      not the key, actually referenced data
 * @param _remove   force model, reduce the ref count and move the data into pTrash
 */
void taosCacheRelease(SCacheObj *pCacheObj, void **data, bool _remove);

/**
 *  move all data node into trash, clear node in trash can if it is not referenced by any clients
 * @param handle
 */
void taosCacheEmpty(SCacheObj *pCacheObj);

/**
 * release all allocated memory and destroy the cache object.
 *
 * This function only set the deleting flag, and the specific work of clean up cache is delegated to
 * taosCacheRefresh function, which will executed every SCacheObj->refreshTime sec.
 *
 * If the value of SCacheObj->refreshTime is too large, the taosCacheRefresh function may not be invoked
 * before the main thread terminated, in which case all allocated resources are simply recycled by OS.
 *
 * @param handle
 */
void taosCacheCleanup(SCacheObj *pCacheObj);

/**
 *
 * @param pCacheObj
 * @param fp
 * @return
 */
void taosCacheRefresh(SCacheObj *pCacheObj, __cache_free_fn_t fp);

/**
 * stop background refresh worker thread
 */
void taosStopCacheRefreshWorker();

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TCACHE_H
