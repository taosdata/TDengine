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

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int32_t        cap;
  int32_t        size;
  void          *pCache;
  TdThreadRwlock rwlock;
} STableCache;

typedef void (*CacheFreeFn)(void *p);
int32_t tableCacheOpen(int32_t cap, CacheFreeFn fn, STableCache **p);
int32_t tableCacheGet(STableCache *p, SSeqRange *key, STableReader **pReader);
int32_t tableCachePut(STableCache *pMgt, SSeqRange *key, STableReader *pReader);
int32_t tableCacheRemove(STableCache *pMgt, SSeqRange *key);
void    tableCacheClose(STableCache *p);

typedef struct {
  int32_t cap;
  int32_t size;
  void   *pCache;

  TdThreadRwlock rwlock;
} SBlockCache;

int32_t blockCacheOpen(int32_t cap, CacheFreeFn fn, SBlockCache **p);
int32_t blockCacheGet(SBlockCache *p, SSeqRange *key, SBlock **pBlock);
int32_t blockCachePut(SBlockCache *p, SSeqRange *key, SBlock *pBlock);
int32_t blockCacheRemove(SBlockCache *p, SSeqRange *key);
void    blockCacheClose(SBlockCache *p);

#ifdef __cplusplus
}
#endif

#endif
