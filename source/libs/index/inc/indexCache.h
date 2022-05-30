/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com> *
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

#include "indexInt.h"
#include "indexUtil.h"
#include "tskiplist.h"

// ----------------- key structure in skiplist ---------------------

/* A data row, the format is like below
 * content: |<---colVal---->|<-- version--->|<-- uid--->|<-- colType --->|<--operaType--->|
 */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct MemTable {
  T_REF_DECLARE()
  SSkipList* mem;
  void*      pCache;
} MemTable;
typedef struct IndexCache {
  T_REF_DECLARE()
  MemTable *mem, *imm;
  SIndex*   index;
  char*     colName;
  int64_t   version;
  int64_t   occupiedMem;
  int8_t    type;
  uint64_t  suid;

  TdThreadMutex mtx;
  TdThreadCond  finished;
} IndexCache;

#define CACHE_VERSION(cache) atomic_load_64(&cache->version)

typedef struct CacheTerm {
  // key
  char*   colVal;
  int64_t version;
  // value
  uint64_t uid;
  int8_t   colType;

  SIndexOperOnColumn operaType;
} CacheTerm;
//

IndexCache* indexCacheCreate(SIndex* idx, uint64_t suid, const char* colName, int8_t type);

void indexCacheForceToMerge(void* cache);
void indexCacheDestroy(void* cache);
void indexCacheBroadcast(void* cache);
void indexCacheWait(void* cache);

Iterate* indexCacheIteratorCreate(IndexCache* cache);
void     indexCacheIteratorDestroy(Iterate* iiter);

int indexCachePut(void* cache, SIndexTerm* term, uint64_t uid);

// int indexCacheGet(void *cache, uint64_t *rst);
int indexCacheSearch(void* cache, SIndexTermQuery* query, SIdxTRslt* tr, STermValueType* s);

void indexCacheRef(IndexCache* cache);
void indexCacheUnRef(IndexCache* cache);

void indexCacheDebug(IndexCache* cache);

void indexCacheDestroyImm(IndexCache* cache);
#ifdef __cplusplus
}
#endif

#endif
