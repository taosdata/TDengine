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
#ifndef __INDEX_CACHE_H__
#define __INDEX_CACHE_H__

#include "index.h"
#include "tlockfree.h"
// ----------------- row structure in skiplist ---------------------

/* A data row, the format is like below:
 * |<--totalLen-->|<-- fieldId-->|<-- value len--->|<-- value-->|<--version--->|<-- itermType -->|
 *
 */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct IndexCache {
  T_REF_DECLARE() 
  int cVersion; // 
} IndexCache;


// 
IndexCache *indexCacheCreate();

void indexCacheDestroy(IndexCache *cache);

int indexCachePut(IndexCache *cache, int32_t fieldId,  const char *fieldVale,  int32_t fvlen, uint64_t uid, int8_t operaType); 

int indexCacheGet(IndexCache *cache, uint64_t *rst);
int indexCacheSearch(IndexCache *cache, SIndexMultiTermQuery *query, SArray *result);

#ifdef __cplusplus
}
#endif



#endif
