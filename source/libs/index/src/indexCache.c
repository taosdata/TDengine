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

#include "indexCache.h"
#include "indexComm.h"
#include "indexUtil.h"
#include "tcompare.h"
#include "tsched.h"

#define MAX_INDEX_KEY_LEN 256  // test only, change later

#define MEM_TERM_LIMIT     10 * 10000
#define MEM_THRESHOLD      128 * 1024 * 1024  // 8M
#define MEM_SIGNAL_QUIT    MEM_THRESHOLD * 5
#define MEM_ESTIMATE_RADIO 1.5

static void idxMemRef(MemTable* tbl);
static void idxMemUnRef(MemTable* tbl);

static void    idxCacheTermDestroy(CacheTerm* ct);
static int32_t idxCacheTermCompare(const void* l, const void* r);
static int32_t idxCacheJsonTermCompare(const void* l, const void* r);
static char*   idxCacheTermGet(const void* pData);

static MemTable* idxInternalCacheCreate(int8_t type);

static int32_t cacheSearchTerm(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchPrefix(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchSuffix(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchRegex(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchLessThan(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchLessEqual(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchGreaterThan(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchGreaterEqual(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchRange(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
/*comm compare func, used in (LE/LT/GE/GT compare)*/
static int32_t cacheSearchCompareFunc(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s, RangeType type);
static int32_t cacheSearchTerm_JSON(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchEqual_JSON(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchPrefix_JSON(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchSuffix_JSON(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchRegex_JSON(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchLessThan_JSON(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchLessEqual_JSON(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchGreaterThan_JSON(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchGreaterEqual_JSON(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);
static int32_t cacheSearchRange_JSON(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s);

static int32_t cacheSearchCompareFunc_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s,
                                           RangeType type);

static int32_t (*cacheSearch[][QUERY_MAX])(void* cache, SIndexTerm* ct, SIdxTRslt* tr, STermValueType* s) = {
    {cacheSearchTerm, cacheSearchPrefix, cacheSearchSuffix, cacheSearchRegex, cacheSearchLessThan, cacheSearchLessEqual,
     cacheSearchGreaterThan, cacheSearchGreaterEqual, cacheSearchRange},
    {cacheSearchEqual_JSON, cacheSearchPrefix_JSON, cacheSearchSuffix_JSON, cacheSearchRegex_JSON,
     cacheSearchLessThan_JSON, cacheSearchLessEqual_JSON, cacheSearchGreaterThan_JSON, cacheSearchGreaterEqual_JSON,
     cacheSearchRange_JSON}};

static void idxDoMergeWork(SSchedMsg* msg);
static bool idxCacheIteratorNext(Iterate* itera);

static int32_t cacheSearchTerm(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  if (cache == NULL) {
    return 0;
  }
  MemTable*   mem = cache;
  IndexCache* pCache = mem->pCache;

  CacheTerm* pCt = taosMemoryCalloc(1, sizeof(CacheTerm));
  pCt->colVal = term->colVal;
  pCt->version = atomic_load_64(&pCache->version);

  char* key = idxCacheTermGet(pCt);

  SSkipListIterator* iter = tSkipListCreateIterFromVal(mem->mem, key, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* node = tSkipListIterGet(iter);
    if (node == NULL) {
      break;
    }
    CacheTerm* c = (CacheTerm*)SL_GET_NODE_DATA(node);
    if (0 == strcmp(c->colVal, pCt->colVal) && strlen(pCt->colVal) == strlen(c->colVal)) {
      if (c->operaType == ADD_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->del, tr->add, c->uid)
        *s = kTypeValue;
      } else if (c->operaType == DEL_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->add, tr->del, c->uid)
      }
    } else {
      break;
    }
  }

  taosMemoryFree(pCt);
  tSkipListDestroyIter(iter);
  return 0;
}
static int32_t cacheSearchPrefix(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  // impl later
  return 0;
}
static int32_t cacheSearchSuffix(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  // impl later
  return 0;
}
static int32_t cacheSearchRegex(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  // impl later
  return 0;
}
static int32_t cacheSearchCompareFunc(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s, RangeType type) {
  if (cache == NULL) {
    return 0;
  }
  MemTable*   mem = cache;
  IndexCache* pCache = mem->pCache;

  _cache_range_compare cmpFn = idxGetCompare(type);

  CacheTerm* pCt = taosMemoryCalloc(1, sizeof(CacheTerm));
  pCt->colVal = term->colVal;
  pCt->colType = term->colType;
  pCt->version = atomic_load_64(&pCache->version);

  char* key = idxCacheTermGet(pCt);

  SSkipListIterator* iter = tSkipListCreateIter(mem->mem);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* node = tSkipListIterGet(iter);
    if (node == NULL) {
      break;
    }
    CacheTerm* c = (CacheTerm*)SL_GET_NODE_DATA(node);
    TExeCond   cond = cmpFn(c->colVal, pCt->colVal, pCt->colType);
    if (cond == MATCH) {
      if (c->operaType == ADD_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->del, tr->add, c->uid)
        // taosArrayPush(result, &c->uid);
        *s = kTypeValue;
      } else if (c->operaType == DEL_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->add, tr->del, c->uid)
      }
    } else if (cond == CONTINUE) {
      continue;
    } else if (cond == BREAK) {
      break;
    }
  }
  taosMemoryFree(pCt);
  tSkipListDestroyIter(iter);
  return TSDB_CODE_SUCCESS;
}
static int32_t cacheSearchLessThan(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return cacheSearchCompareFunc(cache, term, tr, s, LT);
}
static int32_t cacheSearchLessEqual(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return cacheSearchCompareFunc(cache, term, tr, s, LE);
}
static int32_t cacheSearchGreaterThan(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return cacheSearchCompareFunc(cache, term, tr, s, GT);
}
static int32_t cacheSearchGreaterEqual(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return cacheSearchCompareFunc(cache, term, tr, s, GE);
}

static int32_t cacheSearchTerm_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  if (cache == NULL) {
    return 0;
  }
  MemTable*   mem = cache;
  IndexCache* pCache = mem->pCache;

  CacheTerm* pCt = taosMemoryCalloc(1, sizeof(CacheTerm));
  pCt->colVal = term->colVal;
  pCt->version = atomic_load_64(&pCache->version);

  char* exBuf = NULL;
  if (IDX_TYPE_CONTAIN_EXTERN_TYPE(term->colType, TSDB_DATA_TYPE_JSON)) {
    exBuf = idxPackJsonData(term);
    pCt->colVal = exBuf;
  }
  char* key = idxCacheTermGet(pCt);

  SSkipListIterator* iter = tSkipListCreateIterFromVal(mem->mem, key, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* node = tSkipListIterGet(iter);
    if (node == NULL) {
      break;
    }
    CacheTerm* c = (CacheTerm*)SL_GET_NODE_DATA(node);

    if (0 == strcmp(c->colVal, pCt->colVal)) {
      if (c->operaType == ADD_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->del, tr->add, c->uid)
        *s = kTypeValue;
      } else if (c->operaType == DEL_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->add, tr->del, c->uid)
      }
    } else {
      break;
    }
  }

  taosMemoryFree(pCt);
  taosMemoryFree(exBuf);
  tSkipListDestroyIter(iter);
  return 0;

  return TSDB_CODE_SUCCESS;
}
static int32_t cacheSearchSuffix_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return TSDB_CODE_SUCCESS;
}
static int32_t cacheSearchRegex_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return TSDB_CODE_SUCCESS;
}
static int32_t cacheSearchEqual_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return cacheSearchCompareFunc_JSON(cache, term, tr, s, EQ);
}
static int32_t cacheSearchPrefix_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return cacheSearchCompareFunc_JSON(cache, term, tr, s, CONTAINS);
}
static int32_t cacheSearchLessThan_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return cacheSearchCompareFunc_JSON(cache, term, tr, s, LT);
}
static int32_t cacheSearchLessEqual_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return cacheSearchCompareFunc_JSON(cache, term, tr, s, LE);
}
static int32_t cacheSearchGreaterThan_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return cacheSearchCompareFunc_JSON(cache, term, tr, s, GT);
}
static int32_t cacheSearchGreaterEqual_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return cacheSearchCompareFunc_JSON(cache, term, tr, s, GE);
}
static int32_t cacheSearchContain_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return cacheSearchCompareFunc_JSON(cache, term, tr, s, CONTAINS);
}
static int32_t cacheSearchRange_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  return TSDB_CODE_SUCCESS;
}

static int32_t cacheSearchCompareFunc_JSON(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s,
                                           RangeType type) {
  if (cache == NULL) {
    return 0;
  }
  _cache_range_compare cmpFn = idxGetCompare(type);

  MemTable*   mem = cache;
  IndexCache* pCache = mem->pCache;

  CacheTerm* pCt = taosMemoryCalloc(1, sizeof(CacheTerm));
  pCt->colVal = term->colVal;
  pCt->version = atomic_load_64(&pCache->version);

  int8_t dType = IDX_TYPE_GET_TYPE(term->colType);
  int    skip = 0;
  char*  exBuf = NULL;
  if (type == CONTAINS) {
    SIndexTerm tm = {.suid = term->suid,
                     .operType = term->operType,
                     .colType = term->colType,
                     .colName = term->colVal,
                     .nColName = term->nColVal};
    exBuf = idxPackJsonDataPrefixNoType(&tm, &skip);
    pCt->colVal = exBuf;
  } else {
    exBuf = idxPackJsonDataPrefix(term, &skip);
    pCt->colVal = exBuf;
  }
  char* key = idxCacheTermGet(pCt);

  SSkipListIterator* iter = tSkipListCreateIterFromVal(mem->mem, key, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* node = tSkipListIterGet(iter);
    if (node == NULL) {
      break;
    }
    CacheTerm* c = (CacheTerm*)SL_GET_NODE_DATA(node);
    TExeCond   cond = CONTINUE;
    if (type == CONTAINS) {
      if (0 == strncmp(c->colVal, pCt->colVal, skip)) {
        cond = MATCH;
      }
    } else {
      if (0 != strncmp(c->colVal, pCt->colVal, skip - 1)) {
        break;
      } else if (0 != strncmp(c->colVal, pCt->colVal, skip)) {
        continue;
      } else {
        char* p = taosMemoryCalloc(1, strlen(c->colVal) + 1);
        memcpy(p, c->colVal, strlen(c->colVal));
        cond = cmpFn(p + skip, term->colVal, dType);
        taosMemoryFree(p);
      }
    }
    if (cond == MATCH) {
      if (c->operaType == ADD_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->del, tr->add, c->uid)
        *s = kTypeValue;
      } else if (c->operaType == DEL_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->add, tr->del, c->uid)
      }
    } else if (cond == CONTINUE) {
      continue;
    } else if (cond == BREAK) {
      break;
    }
  }

  taosMemoryFree(pCt);
  taosMemoryFree(exBuf);
  tSkipListDestroyIter(iter);

  return TSDB_CODE_SUCCESS;
}
static int32_t cacheSearchRange(void* cache, SIndexTerm* term, SIdxTRslt* tr, STermValueType* s) {
  // impl later
  return 0;
}
static IterateValue* idxCacheIteratorGetValue(Iterate* iter);

IndexCache* idxCacheCreate(SIndex* idx, uint64_t suid, const char* colName, int8_t type) {
  IndexCache* cache = taosMemoryCalloc(1, sizeof(IndexCache));
  if (cache == NULL) {
    indexError("failed to create index cache");
    return NULL;
  };

  cache->mem = idxInternalCacheCreate(type);
  cache->mem->pCache = cache;
  cache->colName = IDX_TYPE_CONTAIN_EXTERN_TYPE(type, TSDB_DATA_TYPE_JSON) ? taosStrdup(JSON_COLUMN) : taosStrdup(colName);
  cache->type = type;
  cache->index = idx;
  cache->version = 0;
  cache->suid = suid;
  cache->occupiedMem = 0;

  taosThreadMutexInit(&cache->mtx, NULL);
  taosThreadCondInit(&cache->finished, NULL);

  idxCacheRef(cache);
  if (idx != NULL) {
    idxAcquireRef(idx->refId);
  }
  return cache;
}
void idxCacheDebug(IndexCache* cache) {
  MemTable* tbl = NULL;

  taosThreadMutexLock(&cache->mtx);
  tbl = cache->mem;
  idxMemRef(tbl);
  taosThreadMutexUnlock(&cache->mtx);

  {
    SSkipList*         slt = tbl->mem;
    SSkipListIterator* iter = tSkipListCreateIter(slt);
    while (tSkipListIterNext(iter)) {
      SSkipListNode* node = tSkipListIterGet(iter);
      CacheTerm*     ct = (CacheTerm*)SL_GET_NODE_DATA(node);
      if (ct != NULL) {
        // TODO, add more debug info
        indexInfo("{colVal: %s, version: %" PRId64 "} \t", ct->colVal, ct->version);
      }
    }
    tSkipListDestroyIter(iter);

    idxMemUnRef(tbl);
  }

  {
    taosThreadMutexLock(&cache->mtx);
    tbl = cache->imm;
    idxMemRef(tbl);
    taosThreadMutexUnlock(&cache->mtx);
    if (tbl != NULL) {
      SSkipList*         slt = tbl->mem;
      SSkipListIterator* iter = tSkipListCreateIter(slt);
      while (tSkipListIterNext(iter)) {
        SSkipListNode* node = tSkipListIterGet(iter);
        CacheTerm*     ct = (CacheTerm*)SL_GET_NODE_DATA(node);
        if (ct != NULL) {
          // TODO, add more debug info
          indexInfo("{colVal: %s, version: %" PRId64 "} \t", ct->colVal, ct->version);
        }
      }
      tSkipListDestroyIter(iter);
    }

    idxMemUnRef(tbl);
  }
}

void idxCacheDestroySkiplist(SSkipList* slt) {
  SSkipListIterator* iter = tSkipListCreateIter(slt);
  while (iter != NULL && tSkipListIterNext(iter)) {
    SSkipListNode* node = tSkipListIterGet(iter);
    CacheTerm*     ct = (CacheTerm*)SL_GET_NODE_DATA(node);
    if (ct != NULL) {
      taosMemoryFree(ct->colVal);
      taosMemoryFree(ct);
    }
  }
  tSkipListDestroyIter(iter);
  tSkipListDestroy(slt);
}
void idxCacheBroadcast(void* cache) {
  IndexCache* pCache = cache;
  taosThreadCondBroadcast(&pCache->finished);
}
void idxCacheWait(void* cache) {
  IndexCache* pCache = cache;
  taosThreadCondWait(&pCache->finished, &pCache->mtx);
}
void idxCacheDestroyImm(IndexCache* cache) {
  if (cache == NULL) {
    return;
  }
  MemTable* tbl = NULL;
  taosThreadMutexLock(&cache->mtx);

  tbl = cache->imm;
  cache->imm = NULL;  // or throw int bg thread
  idxCacheBroadcast(cache);

  taosThreadMutexUnlock(&cache->mtx);

  idxMemUnRef(tbl);
  idxMemUnRef(tbl);
}
void idxCacheDestroy(void* cache) {
  IndexCache* pCache = cache;
  if (pCache == NULL) {
    return;
  }

  idxMemUnRef(pCache->mem);
  idxMemUnRef(pCache->imm);
  taosMemoryFree(pCache->colName);

  taosThreadMutexDestroy(&pCache->mtx);
  taosThreadCondDestroy(&pCache->finished);
  if (pCache->index != NULL) {
    idxReleaseRef(((SIndex*)pCache->index)->refId);
  }
  taosMemoryFree(pCache);
}

Iterate* idxCacheIteratorCreate(IndexCache* cache) {
  if (cache->imm == NULL) {
    return NULL;
  }
  Iterate* iter = taosMemoryCalloc(1, sizeof(Iterate));
  if (iter == NULL) {
    return NULL;
  }
  taosThreadMutexLock(&cache->mtx);

  idxMemRef(cache->imm);

  MemTable* tbl = cache->imm;
  iter->val.val = taosArrayInit(1, sizeof(uint64_t));
  iter->val.colVal = NULL;
  iter->iter = tbl != NULL ? tSkipListCreateIter(tbl->mem) : NULL;
  iter->next = idxCacheIteratorNext;
  iter->getValue = idxCacheIteratorGetValue;

  taosThreadMutexUnlock(&cache->mtx);

  return iter;
}
void idxCacheIteratorDestroy(Iterate* iter) {
  if (iter == NULL) {
    return;
  }
  tSkipListDestroyIter(iter->iter);
  iterateValueDestroy(&iter->val, true);
  taosMemoryFree(iter);
}

int idxCacheSchedToMerge(IndexCache* pCache, bool notify) {
  SSchedMsg schedMsg = {0};
  schedMsg.fp = idxDoMergeWork;
  schedMsg.ahandle = pCache;
  if (notify) {
    schedMsg.thandle = taosMemoryMalloc(1);
  }
  schedMsg.msg = NULL;
  idxAcquireRef(pCache->index->refId);
  taosScheduleTask(indexQhandle, &schedMsg);
  return 0;
}

static void idxCacheMakeRoomForWrite(IndexCache* cache) {
  while (true) {
    if (cache->occupiedMem * MEM_ESTIMATE_RADIO < MEM_THRESHOLD) {
      break;
    } else if (cache->imm != NULL) {
      // TODO: wake up by condition variable
      idxCacheWait(cache);
    } else {
      bool quit = cache->occupiedMem >= MEM_SIGNAL_QUIT ? true : false;

      idxCacheRef(cache);
      cache->imm = cache->mem;
      cache->mem = idxInternalCacheCreate(cache->type);

      cache->mem->pCache = cache;
      cache->occupiedMem = 0;
      if (quit == false) {
        atomic_store_32(&cache->merging, 1);
      }
      // 1. sched to merge
      // 2. unref cache in bgwork
      idxCacheSchedToMerge(cache, quit);
    }
  }
}
int idxCachePut(void* cache, SIndexTerm* term, uint64_t uid) {
  if (cache == NULL) {
    return -1;
  }
  bool hasJson = IDX_TYPE_CONTAIN_EXTERN_TYPE(term->colType, TSDB_DATA_TYPE_JSON);

  IndexCache* pCache = cache;
  idxCacheRef(pCache);
  // encode data
  CacheTerm* ct = taosMemoryCalloc(1, sizeof(CacheTerm));
  if (ct == NULL) {
    return -1;
  }
  // set up key
  ct->colType = term->colType;
  if (hasJson) {
    ct->colVal = idxPackJsonData(term);
  } else {
    ct->colVal = (char*)taosMemoryCalloc(1, sizeof(char) * (term->nColVal + 1));
    memcpy(ct->colVal, term->colVal, term->nColVal);
  }
  ct->version = atomic_add_fetch_64(&pCache->version, 1);
  // set value
  ct->uid = uid;
  ct->operaType = term->operType;
  // ugly code, refactor later
  int64_t estimate = sizeof(ct) + strlen(ct->colVal);

  taosThreadMutexLock(&pCache->mtx);
  pCache->occupiedMem += estimate;
  idxCacheMakeRoomForWrite(pCache);
  MemTable* tbl = pCache->mem;
  idxMemRef(tbl);
  tSkipListPut(tbl->mem, (char*)ct);
  idxMemUnRef(tbl);

  taosThreadMutexUnlock(&pCache->mtx);
  idxCacheUnRef(pCache);
  return 0;
}
void idxCacheForceToMerge(void* cache) {
  IndexCache* pCache = cache;

  idxCacheRef(pCache);
  taosThreadMutexLock(&pCache->mtx);

  indexInfo("%p is forced to merge into tfile", pCache);
  pCache->occupiedMem += MEM_SIGNAL_QUIT;
  idxCacheMakeRoomForWrite(pCache);

  taosThreadMutexUnlock(&pCache->mtx);
  idxCacheUnRef(pCache);
  return;
}
int idxCacheDel(void* cache, const char* fieldValue, int32_t fvlen, uint64_t uid, int8_t operType) {
  IndexCache* pCache = cache;
  return 0;
}

static int32_t idxQueryMem(MemTable* mem, SIndexTermQuery* query, SIdxTRslt* tr, STermValueType* s) {
  if (mem == NULL) {
    return 0;
  }

  SIndexTerm*     term = query->term;
  EIndexQueryType qtype = query->qType;

  if (IDX_TYPE_CONTAIN_EXTERN_TYPE(term->colType, TSDB_DATA_TYPE_JSON)) {
    return cacheSearch[1][qtype](mem, term, tr, s);
  } else {
    return cacheSearch[0][qtype](mem, term, tr, s);
  }
}
int idxCacheSearch(void* cache, SIndexTermQuery* query, SIdxTRslt* result, STermValueType* s) {
  if (cache == NULL) {
    return 0;
  }

  IndexCache* pCache = cache;

  MemTable *mem = NULL, *imm = NULL;
  taosThreadMutexLock(&pCache->mtx);
  mem = pCache->mem;
  imm = pCache->imm;
  idxMemRef(mem);
  idxMemRef(imm);
  taosThreadMutexUnlock(&pCache->mtx);

  int64_t st = taosGetTimestampUs();

  int ret = (mem && mem->mem) ? idxQueryMem(mem, query, result, s) : 0;
  if (ret == 0 && *s != kTypeDeletion) {
    // continue search in imm
    ret = (imm && imm->mem) ? idxQueryMem(imm, query, result, s) : 0;
  }

  idxMemUnRef(mem);
  idxMemUnRef(imm);
  indexInfo("cache search, time cost %" PRIu64 "us", taosGetTimestampUs() - st);

  return ret;
}

void idxCacheRef(IndexCache* cache) {
  if (cache == NULL) {
    return;
  }
  int ref = T_REF_INC(cache);
  UNUSED(ref);
}
void idxCacheUnRef(IndexCache* cache) {
  if (cache == NULL) {
    return;
  }
  int ref = T_REF_DEC(cache);
  if (ref == 0) {
    idxCacheDestroy(cache);
  }
}

void idxMemRef(MemTable* tbl) {
  if (tbl == NULL) {
    return;
  }
  int ref = T_REF_INC(tbl);
  UNUSED(ref);
}
void idxMemUnRef(MemTable* tbl) {
  if (tbl == NULL) {
    return;
  }
  int ref = T_REF_DEC(tbl);
  if (ref == 0) {
    SSkipList* slt = tbl->mem;
    idxCacheDestroySkiplist(slt);
    taosMemoryFree(tbl);
  }
}

static void idxCacheTermDestroy(CacheTerm* ct) {
  if (ct == NULL) {
    return;
  }
  taosMemoryFree(ct->colVal);
  taosMemoryFree(ct);
}
static char* idxCacheTermGet(const void* pData) {
  CacheTerm* p = (CacheTerm*)pData;
  return (char*)p;
}
static int32_t idxCacheTermCompare(const void* l, const void* r) {
  CacheTerm* lt = (CacheTerm*)l;
  CacheTerm* rt = (CacheTerm*)r;
  // compare colVal
  int32_t cmp = strcmp(lt->colVal, rt->colVal);
  if (cmp == 0) {
    if (rt->version == lt->version) {
      cmp = 0;
    } else {
      cmp = rt->version < lt->version ? -1 : 1;
    }
  }
  return cmp;
}

static int idxFindCh(char* a, char c) {
  char* p = a;
  while (*p != 0 && *p++ != c) {
  }
  return p - a;
}
static int idxCacheJsonTermCompareImpl(char* a, char* b) {
  // int alen = idxFindCh(a, '&');
  // int blen = idxFindCh(b, '&');

  // int cmp = strncmp(a, b, MIN(alen, blen));
  // if (cmp == 0) {
  //  cmp = alen - blen;
  //  if (cmp != 0) {
  //    return cmp;
  //  }
  //  cmp = *(a + alen) - *(b + blen);
  //  if (cmp != 0) {
  //    return cmp;
  //  }
  //  alen += 2;
  //  blen += 2;
  //  cmp = strcmp(a + alen, b + blen);
  //}
  return 0;
}
static int32_t idxCacheJsonTermCompare(const void* l, const void* r) {
  CacheTerm* lt = (CacheTerm*)l;
  CacheTerm* rt = (CacheTerm*)r;
  // compare colVal
  int32_t cmp = strcmp(lt->colVal, rt->colVal);
  if (cmp == 0) {
    return rt->version - lt->version;
  }
  return cmp;
}
static MemTable* idxInternalCacheCreate(int8_t type) {
  // int ttype = IDX_TYPE_CONTAIN_EXTERN_TYPE(type, TSDB_DATA_TYPE_JSON) ? TSDB_DATA_TYPE_BINARY :
  // TSDB_DATA_TYPE_BINARY;
  int ttype = TSDB_DATA_TYPE_BINARY;
  int32_t (*cmpFn)(const void* l, const void* r) =
      IDX_TYPE_CONTAIN_EXTERN_TYPE(type, TSDB_DATA_TYPE_JSON) ? idxCacheJsonTermCompare : idxCacheTermCompare;

  MemTable* tbl = taosMemoryCalloc(1, sizeof(MemTable));
  idxMemRef(tbl);
  // if (ttype == TSDB_DATA_TYPE_BINARY || ttype == TSDB_DATA_TYPE_NCHAR || ttype == TSDB_DATA_TYPE_GEOMETRY) {
  tbl->mem = tSkipListCreate(MAX_SKIP_LIST_LEVEL, ttype, MAX_INDEX_KEY_LEN, cmpFn, SL_ALLOW_DUP_KEY, idxCacheTermGet);
  //}
  return tbl;
}

static void idxDoMergeWork(SSchedMsg* msg) {
  IndexCache* pCache = msg->ahandle;
  SIndex*     sidx = (SIndex*)pCache->index;

  int quit = msg->thandle ? true : false;
  taosMemoryFree(msg->thandle);
  idxFlushCacheToTFile(sidx, pCache, quit);
}
static bool idxCacheIteratorNext(Iterate* itera) {
  SSkipListIterator* iter = itera->iter;
  if (iter == NULL) {
    return false;
  }
  IterateValue* iv = &itera->val;
  iterateValueDestroy(iv, false);

  bool next = tSkipListIterNext(iter);
  if (next) {
    SSkipListNode* node = tSkipListIterGet(iter);
    CacheTerm*     ct = (CacheTerm*)SL_GET_NODE_DATA(node);

    iv->type = ct->operaType;
    iv->ver = ct->version;
    iv->colVal = taosStrdup(ct->colVal);
    taosArrayPush(iv->val, &ct->uid);
  }
  return next;
}

static IterateValue* idxCacheIteratorGetValue(Iterate* iter) {
  // opt later
  return &iter->val;
}
