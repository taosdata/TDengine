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
#include "tref.h"
#include "hash.h"

typedef struct SCacheStatis {
  int64_t missCount;
  int64_t hitCount;
  int64_t totalAccess;
  int64_t refreshCount;
  int32_t numOfCollision;
} SCacheStatis;

typedef struct SCacheDataNode {
  uint64_t addedTime;    // the added time when this element is added or updated into cache
  uint64_t expiredTime;  // expiredTime expiredTime when this element should be remove from cache
  uint64_t signature;
  uint32_t size;         // allocated size for current SCacheDataNode
  uint16_t keySize : 15;
  bool     inTrash : 1;  // denote if it is in trash or not
  T_REF_DECLARE()
  char *key;
  char  data[];
} SCacheDataNode;

typedef struct STrashElem {
  struct STrashElem *prev;
  struct STrashElem *next;
  SCacheDataNode *        pData;
} STrashElem;

typedef struct {
  int64_t totalSize;  // total allocated buffer in this hash table, SCacheObj is not included.
  int64_t refreshTime;
  
  /*
   * to accommodate the old datanode which has the same key value of new one in hashList
   * when an new node is put into cache, if an existed one with the same key:
   * 1. if the old one does not be referenced, update it.
   * 2. otherwise, move the old one to pTrash, addedTime the new one.
   *
   * when the node in pTrash does not be referenced, it will be release at the expired expiredTime
   */
  STrashElem * pTrash;
  void *       tmrCtrl;
  void *       pTimer;
  SCacheStatis statistics;
  SHashObj *   pHashTable;
  int          numOfElemsInTrash;  // number of element in trash
  int16_t      deleting;           // set the deleting flag to stop refreshing ASAP.
  T_REF_DECLARE()

#if defined(LINUX)
  pthread_rwlock_t lock;
#else
  pthread_mutex_t lock;
#endif

} SCacheObj;

/**
 *
 * @param maxSessions       maximum slots available for hash elements
 * @param tmrCtrl           timer ctrl
 * @param refreshTime       refresh operation interval time, the maximum survival time when one element is expired and
 *                          not referenced by other objects
 * @return
 */
SCacheObj *taosCacheInit(void *tmrCtrl, int64_t refreshTimeInSeconds);

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
void *taosCachePut(SCacheObj *pCacheObj, const char *key, const void *pData, size_t dataSize, int keepTimeInSeconds);

/**
 * get data from cache
 * @param pCacheObj     cache object
 * @param key           key
 * @return              cached data or NULL
 */
void *taosCacheAcquireByName(SCacheObj *pCacheObj, const char *key);

/**
 * Add one reference count for the exist data, and assign this data for a new owner.
 * The new owner needs to invoke the taosCacheRelease when it does not need this data anymore.
 * This procedure is a faster version of taosCacheAcquireByName function, which avoids the sideeffect of the problem of
 * the data is moved to trash, and taosCacheAcquireByName will fail to retrieve it again.
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
 * @param _remove   force model, reduce the ref count and move the data into
 * pTrash
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

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TCACHE_H
