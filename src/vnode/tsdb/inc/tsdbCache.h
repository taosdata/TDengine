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

#include "taosdef.h"
#include "tlist.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_DEFAULT_CACHE_BLOCK_SIZE 16 * 1024 * 1024 /* 16M */

typedef struct {
  int  blockId;
  int  offset;
  int  remain;
  int  padding;
  char data[];
} STsdbCacheBlock;

typedef struct {
  int64_t index;
  SList * memPool;
} STsdbCachePool;

typedef struct {
  TSKEY   keyFirst;
  TSKEY   keyLast;
  int64_t numOfPoints;
  SList * list;
} SCacheMem;

typedef struct {
  int              maxBytes;
  int              cacheBlockSize;
  STsdbCachePool   pool;
  STsdbCacheBlock *curBlock;
  SCacheMem *      mem;
  SCacheMem *      imem;
} STsdbCache;

STsdbCache *tsdbInitCache(int maxBytes, int cacheBlockSize);
void        tsdbFreeCache(STsdbCache *pCache);
void *      tsdbAllocFromCache(STsdbCache *pCache, int bytes, TSKEY key);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TSDBCACHE_H_
