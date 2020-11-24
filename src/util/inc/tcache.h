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

int32_t taosCacheInit(int32_t keyType, int64_t refreshTimeInSeconds, bool extendLifespan, __cache_free_fn_t fn, const char *cacheName);
void  taosCacheCleanup(int32_t cacheId);
void *taosCachePut(int32_t cacheId, const void *key, size_t keyLen, const void *pData, size_t dataSize, int durationMS);
void *taosCacheAcquireByKey(int32_t cacheId, const void *key, size_t keyLen);
void *taosCacheAcquireByData(void *data);
void  taosCacheRelease(void *data);
void *taosCacheIterate(int32_t cacheId, void *indata);
void *taosCacheTransfer(void **data);
void  taosCacheRefresh(int32_t cacheId, __cache_free_fn_t fp);
void  taosCacheEmpty(int32_t cacheId);
int32_t taosCacheGetCount(int32_t cacheId);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TCACHE_H
