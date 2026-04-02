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

#ifndef BSE_TABLE_CACHE_H
#define BSE_TABLE_CACHE_H

#include "bseTable.h"
#include "bseUtil.h"
#include "tlockfree.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*SCacheFreeElemFn)(void *p);
typedef struct {
  void            *pItem;
  SSeqRange        pKey;
  SListNode       *pNode;
  SCacheFreeElemFn freeFunc;
  T_REF_DECLARE()

} SCacheItem;

typedef struct {
  int32_t cap;
  int32_t size;
  void   *pCache;
} STableCache;

void freeCacheItem(SCacheItem *pItem);

void bseCacheRefItem(SCacheItem *pItem);
void bseCacheUnrefItem(SCacheItem *pItem);

typedef void (*CacheFreeFn)(void *p);
int32_t tableCacheOpen(int32_t cap, CacheFreeFn fn, STableCache **p);
int32_t tableCacheGet(STableCache *p, SSeqRange *key, SCacheItem **pReader);
int32_t tableCachePut(STableCache *, SSeqRange *key, STableReader *pReader);
int32_t tableCacheRemove(STableCache *p, SSeqRange *key);
void    tableCacheClose(STableCache *p);
int32_t tableCacheClear(STableCache *);
int32_t tableCacheResize(STableCache *p, int32_t newCap);

typedef struct {
  int32_t cap;
  int32_t size;
  void   *pCache;

} SBlockCache;

int32_t blockCacheOpen(int32_t cap, CacheFreeFn fn, SBlockCache **p);
int32_t blockCacheGet(SBlockCache *p, SSeqRange *key, void **pBlock);
int32_t blockCachePut(SBlockCache *p, SSeqRange *key, void *pBlock);
int32_t blockCacheRemove(SBlockCache *p, SSeqRange *key);
void    blockCacheClose(SBlockCache *p);
int32_t blockCacheClear(SBlockCache *p);
int32_t blockCacheResize(SBlockCache *p, int32_t newCap);

#ifdef __cplusplus
}
#endif

#endif
