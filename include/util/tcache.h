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

#ifndef _TD_UTIL_CACHE_H_
#define _TD_UTIL_CACHE_H_

#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_TD_ARM_32)
#define TSDB_CACHE_PTR_KEY  TSDB_DATA_TYPE_INT
#define TSDB_CACHE_PTR_TYPE int32_t
#else
#define TSDB_CACHE_PTR_KEY  TSDB_DATA_TYPE_BIGINT
#define TSDB_CACHE_PTR_TYPE int64_t
#endif

typedef void (*__cache_free_fn_t)(void *);
typedef void (*__cache_trav_fn_t)(void *, void *);

typedef struct SCacheStatis {
  int64_t missCount;
  int64_t hitCount;
  int64_t totalAccess;
  int64_t refreshCount;
} SCacheStatis;

typedef struct SCacheObj  SCacheObj;
typedef struct SCacheIter SCacheIter;
typedef struct STrashElem STrashElem;

/**
 * initialize the cache object
 * @param keyType              key type
 * @param refreshTimeInMs      refresh operation interval time, the maximum survival time when one element is expired
 *                             and not referenced by other objects
 * @param extendLifespan       auto extend lifespan, if accessed
 * @param fn                   free resource callback function
 * @return
 */
SCacheObj *taosCacheInit(int32_t keyType, int64_t refreshTimeInMs, bool extendLifespan, __cache_free_fn_t fn,
                         const char *cacheName);

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
void *taosCachePut(SCacheObj *pCacheObj, const void *key, size_t keyLen, const void *pData, size_t dataSize,
                   int32_t durationMS);

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
void *taosCacheTransferData(SCacheObj *pCacheObj, void **data);

/**
 * remove data in cache, the data will not be removed immediately.
 * if it is referenced by other object, it will be remain in cache
 * @param handle    cache object
 * @param data      not the key, actually referenced data
 * @param _remove   force model, reduce the ref count and move the data into pTrash
 */
void taosCacheRelease(SCacheObj *pCacheObj, void **data, bool _remove);

/**
 *
 * @param pCacheObj
 * @return
 */
size_t taosCacheGetNumOfObj(const SCacheObj *pCacheObj);

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
void taosCacheRefresh(SCacheObj *pCacheObj, __cache_trav_fn_t fp, void *param1);

/**
 * stop background refresh worker thread
 */
void taosStopCacheRefreshWorker();

SCacheIter *taosCacheCreateIter(const SCacheObj *pCacheObj);
bool        taosCacheIterNext(SCacheIter *pIter);
void       *taosCacheIterGetData(const SCacheIter *pIter, size_t *dataLen);
void       *taosCacheIterGetKey(const SCacheIter *pIter, size_t *keyLen);
void        taosCacheDestroyIter(SCacheIter *pIter);

void taosCacheTryExtendLifeSpan(SCacheObj *pCacheObj, void **data);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_CACHE_H_*/
