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
 * content: |<--totalLen-->|<-- fieldid-->|<--field type -->|<-- value len--->|<-- value -->|<--  uid  -->|<--version--->|<-- itermType -->|
 * len    : |<--int32_t -->|<-- int16_t-->|<--  int16_t --->|<--- int32_t --->|<--valuelen->|<--uint64_t->|<-- int32_t-->|<--  int8_t  --->| 
 */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct IndexCache {
  T_REF_DECLARE() 
} IndexCache;


// 
IndexCache *indexCacheCreate();

void indexCacheDestroy(IndexCache *cache);

int indexCachePut(IndexCache *cache, int16_t fieldId, int16_t fieldType, const char *fieldValue,  int32_t fvLen, 
              uint32_t version, uint64_t uid, int8_t operType);

int indexCacheGet(IndexCache *cache, uint64_t *rst);
int indexCacheSearch(IndexCache *cache, SIndexMultiTermQuery *query, SArray *result);

#ifdef __cplusplus
}
#endif



#endif
