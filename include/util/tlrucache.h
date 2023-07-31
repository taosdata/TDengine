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

#ifndef _TD_UTIL_LRUCACHE_H_
#define _TD_UTIL_LRUCACHE_H_

#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SLRUCache SLRUCache;

typedef void (*_taos_lru_deleter_t)(const void *key, size_t keyLen, void *value, void *ud);
typedef int (*_taos_lru_functor_t)(const void *key, size_t keyLen, void *value, void *ud);

typedef struct LRUHandle LRUHandle;

typedef enum { TAOS_LRU_PRIORITY_HIGH, TAOS_LRU_PRIORITY_LOW } LRUPriority;

typedef enum {
  TAOS_LRU_STATUS_OK,
  TAOS_LRU_STATUS_FAIL,
  TAOS_LRU_STATUS_INCOMPLETE,
  TAOS_LRU_STATUS_OK_OVERWRITTEN
} LRUStatus;

SLRUCache *taosLRUCacheInit(size_t capacity, int numShardBits, double highPriPoolRatio);
void       taosLRUCacheCleanup(SLRUCache *cache);

LRUStatus  taosLRUCacheInsert(SLRUCache *cache, const void *key, size_t keyLen, void *value, size_t charge,
                              _taos_lru_deleter_t deleter, LRUHandle **handle, LRUPriority priority, void *ud);
LRUHandle *taosLRUCacheLookup(SLRUCache *cache, const void *key, size_t keyLen);
void       taosLRUCacheErase(SLRUCache *cache, const void *key, size_t keyLen);

void taosLRUCacheApply(SLRUCache *cache, _taos_lru_functor_t functor, void *ud);
void taosLRUCacheEraseUnrefEntries(SLRUCache *cache);

bool taosLRUCacheRef(SLRUCache *cache, LRUHandle *handle);
bool taosLRUCacheRelease(SLRUCache *cache, LRUHandle *handle, bool eraseIfLastRef);

void *taosLRUCacheValue(SLRUCache *cache, LRUHandle *handle);

size_t taosLRUCacheGetUsage(SLRUCache *cache);
size_t taosLRUCacheGetPinnedUsage(SLRUCache *cache);

int32_t taosLRUCacheGetElems(SLRUCache *cache);

void   taosLRUCacheSetCapacity(SLRUCache *cache, size_t capacity);
size_t taosLRUCacheGetCapacity(SLRUCache *cache);

void taosLRUCacheSetStrictCapacity(SLRUCache *cache, bool strict);
bool taosLRUCacheIsStrictCapacity(SLRUCache *cache);

#ifdef __cplusplus
}
#endif

#endif  // _TD_UTIL_LRUCACHE_H_
