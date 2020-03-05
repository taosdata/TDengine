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
#if !defined(_TD_TSDBCACHE_H_)
#define _TD_TSDBCACHE_H_

#include <stdint.h>

// #include "cache.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_DEFAULT_CACHE_BLOCK_SIZE 16*1024*1024 /* 16M */

typedef struct {
  int64_t skey;     // start key
  int64_t ekey;     // end key
  int32_t numOfRows; // numOfRows
} STableCacheInfo;

typedef struct _tsdb_cache_block {
  char *                    pData;
  STableCacheInfo *         pTableInfo;
  struct _tsdb_cache_block *prev;
  struct _tsdb_cache_block *next;
} STSDBCacheBlock;

// Use a doublely linked list to implement this
typedef struct STSDBCache {
  // Number of blocks the cache is allocated
  int32_t          numOfBlocks;
  STSDBCacheBlock *cacheList;
  void *           current;
} STsdbCache;

// ---- Operation on STSDBCacheBlock
#define TSDB_CACHE_BLOCK_DATA(pBlock) ((pBlock)->pData)
#define TSDB_CACHE_AVAIL_SPACE(pBlock) ((char *)((pBlock)->pTableInfo) - ((pBlock)->pData))
#define TSDB_TABLE_INFO_OF_CACHE(pBlock, tableId) ((pBlock)->pTableInfo)[tableId]
#define TSDB_NEXT_CACHE_BLOCK(pBlock) ((pBlock)->next)
#define TSDB_PREV_CACHE_BLOCK(pBlock) ((pBlock)->prev)

STsdbCache *tsdbCreateCache(int32_t numOfBlocks);
int32_t tsdbFreeCache(STsdbCache *pCache);
void *tsdbAllocFromCache(STsdbCache *pCache, int64_t bytes);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TSDBCACHE_H_
