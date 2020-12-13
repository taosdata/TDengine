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

/* 
 * tcache is designed with the following features:
 * 1: put an item into cache, so it can be acquired or released later 
 * 2: if no apps hold the cached item, it will be removed from cache once its lifetime is expired
 * 3: if an item with the same key is already cached, the new cached item will overwirte the old one
 * 4: it is multi-thread safe, acuqire/release/remove/iterate actions can be excuted by different threads
 * 5: hash slots is dynamically resized to guarantee the hash efficiency

 * Limitation:
 * 1: maximum number of caches is pre-defined by TAOS_CACHE_MAX_OBJS, not dynamic. 
 * 2: if a cached item is already released/removed, if app still tries to release/remove it, it may crash
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

// for initialization. return cache ID, or -1 if an error occurred (in which case, terrno is set appropriately).
// keyType is the data types supported by TDengine and defined in taos.h, name is cache name for debug purpose
int32_t taosCacheInit(int32_t keyType, int64_t refreshTimeInSeconds, bool extendLifespan, __cache_free_fn_t fn, const char *name);

// for cleanup, cacheId is the value returned by taosCacheInit
void  taosCacheCleanup(int32_t cacheId);

// put an item into cache, return the pointer to cached item. The cached is also acquired automatically 
// durationMs specifies how long the cached data will be kept in memory after app releases it
void *taosCachePut(int32_t cacheId, const void *key, size_t keyLen, const void *pData, size_t dataSize, int durationMS);

// acquire the cached data via key
void *taosCacheAcquireByKey(int32_t cacheId, const void *key, size_t keyLen);

// acquire the cached data via pointer to cached data
void *taosCacheAcquireByData(void *data);

// release the cached data
void  taosCacheRelease(void *data);

// API to iterate all the cached items 
// return the first cached item if indata is NULL, or the next cached item. It means iteration is over if NULL is returned. 
void *taosCacheIterate(int32_t cacheId, void *indata);

// cancel the iteration. If app wants to stop the iteration, this API must be called, otherwise resource will be leaked
void  taosCacheCancelIterate(int32_t cacheId, void *indata);

// transfer the owner of cached item from one to another
void *taosCacheTransfer(void **data);

// execute function fp for each cached item
void  taosCacheRefresh(int32_t cacheId, __cache_free_fn_t fp);

// empty all the cached items
void  taosCacheEmpty(int32_t cacheId);

// returns number of cached items including the cached ones
int32_t taosCacheGetCount(int32_t cacheId);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TCACHE_H
