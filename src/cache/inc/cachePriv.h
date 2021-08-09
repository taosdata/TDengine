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

#ifndef TDENGINE_CACHEINT_H
#define TDENGINE_CACHEINT_H

#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include "cache.h"
#include "cacheDefine.h"
#include "cacheLru.h"
#include "cacheMutex.h"
#include "cacheTypes.h"
#include "tlockfree.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cacheAllocMemoryCookie {
  void* buffer;
  struct cacheAllocMemoryCookie *next;
} cacheAllocMemoryCookie;

struct cache_t {
  SRWLatch     latch;

  cacheOption options;

  cacheSlabClass* slabs[MAX_NUMBER_OF_SLAB_CLASSES];    /* array of slab pointers */

  cacheSlabLruClass  lruArray[MAX_NUMBER_OF_SLAB_LRU];           /* LRU item list array */

  cacheTable* tableHead;                                /* cache table list */

  size_t alloced;                   /* allocated memory size */

  cacheItem*  neverExpireItemHead;  /* never expire items list head */

  cacheAllocMemoryCookie* cookieHead;

  cacheItem* chunkItemHead;

  cacheMutex* itemMutex;

  int powerLargest;  
};

#define hashsize(n) ((unsigned long int)1<<(n))
#define hashmask(n) (hashsize(n)-1)

void *allocMemory(cache_t *cache, size_t size, bool chunked);

cacheMutex* getItemMutexByIndex(cacheTable* pTable, uint32_t hash);

cacheMutex* getItemMutexByKey(cacheTable* pTable, const void* key, uint8_t nkey);

cacheMutex* getItemMutexByItem(cacheItem* pItem);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CACHEINT_H