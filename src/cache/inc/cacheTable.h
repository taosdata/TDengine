/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#ifndef TDENGINE_CACHE_HASHTABLE_H
#define TDENGINE_CACHE_HASHTABLE_H

#include <stdbool.h>
#include <stdint.h>
#include "cache.h"
#include "cacheMutex.h"
#include "cacheTypes.h"
#include "hashfunc.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cacheTableBucket {
  uint32_t hash;
  cacheItem* head;
  cacheMutex mutex;
} cacheTableBucket;

struct cacheTable { 
  cache_t* pCache;
  cacheMutex mutex;
  cacheTable* next;
  cacheTableOption option;

  _hash_fn_t hashFp;

  uint32_t capacity;

  cacheTableBucket* pBucket;
};

void cacheTableDestroy(cacheTable *pTable);

int cacheTablePut(cacheTable *pTable, cacheItem* pItem);
cacheItem* cacheTableGet(cacheTable* pTable, const char* key, uint8_t nkey);
void cacheTableRemove(cacheTable* pTable, const char* key, uint8_t nkey, bool freeItem);

cacheMutex* cacheGetTableBucketMutexByKey(cacheTable* pTable, const char* key, uint8_t nkey);

static int FORCE_INLINE cacheTableLockBucket(cacheTable* pTable, uint32_t hash) {
  return cacheMutexLock(&(pTable->pBucket[hash % pTable->capacity].mutex));
}

static int FORCE_INLINE cacheTableUnlockBucket(cacheTable* pTable, uint32_t hash) {
  return cacheMutexUnlock(&(pTable->pBucket[hash % pTable->capacity].mutex));
}

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_CACHE_HASHTABLE_H */
