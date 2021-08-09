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
#include "tlockfree.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cacheTableBucket {
  cacheItem* head;
} cacheTableBucket;

struct cacheTable { 
  cache_t* pCache;
  SRWLatch     latch;
  cacheMutex mutex;
  cacheTable* next;
  cacheTableOption option;

  _hash_fn_t hashFp;

  int hashPower;
  uint64_t nNum;

  cacheTableBucket* pBucket;

  cacheTableBucket* pOldBucket;

  bool expanding;             /* expanding flag */
  uint32_t expandIndex;       /* expand max index */
};

void cacheTableDestroy(cacheTable *pTable);

int cacheTablePut(cacheTable *pTable, cacheItem* pItem);
cacheItem* cacheTableGet(cacheTable* pTable, const void* key, uint8_t nkey);
void cacheTableRemove(cacheTable* pTable, const void* key, uint8_t nkey, bool freeItem);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_CACHE_HASHTABLE_H */
