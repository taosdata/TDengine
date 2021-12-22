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
#include "indexInt.h"
#include "tlockfree.h"
#include "tskiplist.h"
// ----------------- key structure in skiplist ---------------------

/* A data row, the format is like below:
 * content: |<--totalLen-->|<-- fieldid-->|<--field type-->|<-- value len--->|
 *          |<-- value -->|<--uid -->|<--version--->|<-- itermType -->|
 *  len :   |<--int32_t -->|<-- int16_t-->|<--  int8_t --->|<--- int32_t --->|
 *          <--valuelen->|<--uint64_t->| *  <-- int32_t-->|<-- int8_t --->|
 */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct IndexCache {
  T_REF_DECLARE()
  SSkipList* skiplist;
} IndexCache;

//
IndexCache* indexCacheCreate();

void indexCacheDestroy(void* cache);

int indexCachePut(void* cache, SIndexTerm* term, int16_t colId, int32_t version, uint64_t uid);

// int indexCacheGet(void *cache, uint64_t *rst);
int indexCacheSearch(void* cache, SIndexTermQuery* query, int16_t colId, int32_t version, SArray* result, STermValueType* s);

#ifdef __cplusplus
}
#endif

#endif
