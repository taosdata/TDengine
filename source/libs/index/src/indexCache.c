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
#define MEM_THRESHOLD      1024 * 1024
#define MEM_ESTIMATE_RADIO 1.5

static void indexMemRef(MemTable* tbl);
static void indexMemUnRef(MemTable* tbl);

static void    indexCacheTermDestroy(CacheTerm* ct);
static int32_t indexCacheTermCompare(const void* l, const void* r);
static int32_t indexCacheJsonTermCompare(const void* l, const void* r);
static char*   indexCacheTermGet(const void* pData);

static MemTable* indexInternalCacheCreate(int8_t type);

static int32_t cacheSearchTerm(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchPrefix(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchSuffix(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchRegex(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchLessThan(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchLessEqual(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchGreaterThan(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchGreaterEqual(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchRange(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
/*comm func of compare, used in (LE/LT/GE/GT compare)*/
static int32_t cacheSearchCompareFunc(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s,
                                      RangeType type);
static int32_t cacheSearchTerm_JSON(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchPrefix_JSON(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchSuffix_JSON(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchRegex_JSON(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchLessThan_JSON(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchLessEqual_JSON(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchGreaterThan_JSON(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchGreaterEqual_JSON(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);
static int32_t cacheSearchRange_JSON(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s);

static int32_t cacheSearchCompareFunc_JSON(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s,
                                           RangeType type);

static int32_t (*cacheSearch[][QUERY_MAX])(void* cache, SIndexTerm* ct, SIdxTempResult* tr, STermValueType* s) = {
    {cacheSearchTerm, cacheSearchPrefix, cacheSearchSuffix, cacheSearchRegex, cacheSearchLessThan, cacheSearchLessEqual,
     cacheSearchGreaterThan, cacheSearchGreaterEqual, cacheSearchRange},
    {cacheSearchTerm_JSON, cacheSearchPrefix_JSON, cacheSearchSuffix_JSON, cacheSearchRegex_JSON,
     cacheSearchLessThan_JSON, cacheSearchLessEqual_JSON, cacheSearchGreaterThan_JSON, cacheSearchGreaterEqual_JSON,
     cacheSearchRange_JSON}};

static void doMergeWork(SSchedMsg* msg);
static bool indexCacheIteratorNext(Iterate* itera);

static int32_t cacheSearchTerm(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  if (cache == NULL) {
    return 0;
  }
  MemTable*   mem = cache;
  IndexCache* pCache = mem->pCache;

  CacheTerm* pCt = taosMemoryCalloc(1, sizeof(CacheTerm));
  pCt->colVal = term->colVal;
  pCt->version = atomic_load_32(&pCache->version);

  char* key = indexCacheTermGet(pCt);

  SSkipListIterator* iter = tSkipListCreateIterFromVal(mem->mem, key, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* node = tSkipListIterGet(iter);
    if (node == NULL) {
      break;
    }
    CacheTerm* c = (CacheTerm*)SL_GET_NODE_DATA(node);
    if (0 == strcmp(c->colVal, pCt->colVal)) {
      if (c->operaType == ADD_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->deled, tr->added, c->uid)
        // taosArrayPush(result, &c->uid);
        *s = kTypeValue;
      } else if (c->operaType == DEL_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->added, tr->deled, c->uid)
      }
    } else {
      break;
    }
  }

  taosMemoryFree(pCt);
  tSkipListDestroyIter(iter);
  return 0;
}
static int32_t cacheSearchPrefix(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  // impl later
  return 0;
}
static int32_t cacheSearchSuffix(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  // impl later
  return 0;
}
static int32_t cacheSearchRegex(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  // impl later
  return 0;
}
static int32_t cacheSearchCompareFunc(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s,
                                      RangeType type) {
  if (cache == NULL) {
    return 0;
  }

  _cache_range_compare cmpFn = indexGetCompare(type);

  MemTable*   mem = cache;
  IndexCache* pCache = mem->pCache;

  CacheTerm* pCt = taosMemoryCalloc(1, sizeof(CacheTerm));
  pCt->colVal = term->colVal;
  pCt->version = atomic_load_32(&pCache->version);

  char* key = indexCacheTermGet(pCt);

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
        INDEX_MERGE_ADD_DEL(tr->deled, tr->added, c->uid)
        // taosArrayPush(result, &c->uid);
        *s = kTypeValue;
      } else if (c->operaType == DEL_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->added, tr->deled, c->uid)
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
static int32_t cacheSearchLessThan(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return cacheSearchCompareFunc(cache, term, tr, s, LT);
}
static int32_t cacheSearchLessEqual(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return cacheSearchCompareFunc(cache, term, tr, s, LE);
}
static int32_t cacheSearchGreaterThan(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return cacheSearchCompareFunc(cache, term, tr, s, GT);
}
static int32_t cacheSearchGreaterEqual(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return cacheSearchCompareFunc(cache, term, tr, s, GE);
}

static int32_t cacheSearchTerm_JSON(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  if (cache == NULL) {
    return 0;
  }
  MemTable*   mem = cache;
  IndexCache* pCache = mem->pCache;

  CacheTerm* pCt = taosMemoryCalloc(1, sizeof(CacheTerm));
  pCt->colVal = term->colVal;
  pCt->version = atomic_load_32(&pCache->version);

  char* exBuf = NULL;
  if (INDEX_TYPE_CONTAIN_EXTERN_TYPE(term->colType, TSDB_DATA_TYPE_JSON)) {
    exBuf = indexPackJsonData(term);
    pCt->colVal = exBuf;
  }
  char* key = indexCacheTermGet(pCt);

  SSkipListIterator* iter = tSkipListCreateIterFromVal(mem->mem, key, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* node = tSkipListIterGet(iter);
    if (node == NULL) {
      break;
    }
    CacheTerm* c = (CacheTerm*)SL_GET_NODE_DATA(node);

    if (0 == strcmp(c->colVal, pCt->colVal)) {
      if (c->operaType == ADD_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->deled, tr->added, c->uid)
        // taosArrayPush(result, &c->uid);
        *s = kTypeValue;
      } else if (c->operaType == DEL_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->added, tr->deled, c->uid)
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
static int32_t cacheSearchPrefix_JSON(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return TSDB_CODE_SUCCESS;
}
static int32_t cacheSearchSuffix_JSON(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return TSDB_CODE_SUCCESS;
}
static int32_t cacheSearchRegex_JSON(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return TSDB_CODE_SUCCESS;
}
static int32_t cacheSearchLessThan_JSON(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return cacheSearchCompareFunc_JSON(cache, term, tr, s, LT);
}
static int32_t cacheSearchLessEqual_JSON(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return cacheSearchCompareFunc_JSON(cache, term, tr, s, LE);
}
static int32_t cacheSearchGreaterThan_JSON(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return cacheSearchCompareFunc_JSON(cache, term, tr, s, GT);
}
static int32_t cacheSearchGreaterEqual_JSON(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return cacheSearchCompareFunc_JSON(cache, term, tr, s, GE);
}
static int32_t cacheSearchRange_JSON(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  return TSDB_CODE_SUCCESS;
}

static int32_t cacheSearchCompareFunc_JSON(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s,
                                           RangeType type) {
  if (cache == NULL) {
    return 0;
  }
  _cache_range_compare cmpFn = indexGetCompare(type);

  MemTable*   mem = cache;
  IndexCache* pCache = mem->pCache;

  CacheTerm* pCt = taosMemoryCalloc(1, sizeof(CacheTerm));
  pCt->colVal = term->colVal;
  pCt->version = atomic_load_32(&pCache->version);

  int8_t dType = INDEX_TYPE_GET_TYPE(term->colType);
  int    skip = 0;
  char*  exBuf = NULL;

  if (INDEX_TYPE_CONTAIN_EXTERN_TYPE(term->colType, TSDB_DATA_TYPE_JSON)) {
    exBuf = indexPackJsonDataPrefix(term, &skip);
    pCt->colVal = exBuf;
  }
  char* key = indexCacheTermGet(pCt);

  // SSkipListIterator* iter = tSkipListCreateIter(mem->mem);
  SSkipListIterator* iter = tSkipListCreateIterFromVal(mem->mem, key, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* node = tSkipListIterGet(iter);
    if (node == NULL) {
      break;
    }
    CacheTerm* c = (CacheTerm*)SL_GET_NODE_DATA(node);
    // printf("json val: %s\n", c->colVal);
    if (0 != strncmp(c->colVal, pCt->colVal, skip)) {
      break;
    }

    TExeCond cond = cmpFn(c->colVal + skip, term->colVal, dType);
    if (cond == MATCH) {
      if (c->operaType == ADD_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->deled, tr->added, c->uid)
        // taosArrayPush(result, &c->uid);
        *s = kTypeValue;
      } else if (c->operaType == DEL_VALUE) {
        INDEX_MERGE_ADD_DEL(tr->added, tr->deled, c->uid)
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
static int32_t cacheSearchRange(void* cache, SIndexTerm* term, SIdxTempResult* tr, STermValueType* s) {
  // impl later
  return 0;
}
static IterateValue* indexCacheIteratorGetValue(Iterate* iter);

IndexCache* indexCacheCreate(SIndex* idx, uint64_t suid, const char* colName, int8_t type) {
  IndexCache* cache = taosMemoryCalloc(1, sizeof(IndexCache));
  if (cache == NULL) {
    indexError("failed to create index cache");
    return NULL;
  };

  cache->mem = indexInternalCacheCreate(type);
  cache->mem->pCache = cache;
  cache->colName = INDEX_TYPE_CONTAIN_EXTERN_TYPE(type, TSDB_DATA_TYPE_JSON) ? tstrdup(JSON_COLUMN) : tstrdup(colName);
  cache->type = type;
  cache->index = idx;
  cache->version = 0;
  cache->suid = suid;
  cache->occupiedMem = 0;

  taosThreadMutexInit(&cache->mtx, NULL);
  taosThreadCondInit(&cache->finished, NULL);

  indexCacheRef(cache);
  return cache;
}
void indexCacheDebug(IndexCache* cache) {
  MemTable* tbl = NULL;

  taosThreadMutexLock(&cache->mtx);
  tbl = cache->mem;
  indexMemRef(tbl);
  taosThreadMutexUnlock(&cache->mtx);

  {
    SSkipList*         slt = tbl->mem;
    SSkipListIterator* iter = tSkipListCreateIter(slt);
    while (tSkipListIterNext(iter)) {
      SSkipListNode* node = tSkipListIterGet(iter);
      CacheTerm*     ct = (CacheTerm*)SL_GET_NODE_DATA(node);
      if (ct != NULL) {
        // TODO, add more debug info
        indexInfo("{colVal: %s, version: %d} \t", ct->colVal, ct->version);
      }
    }
    tSkipListDestroyIter(iter);

    indexMemUnRef(tbl);
  }

  {
    taosThreadMutexLock(&cache->mtx);
    tbl = cache->imm;
    indexMemRef(tbl);
    taosThreadMutexUnlock(&cache->mtx);
    if (tbl != NULL) {
      SSkipList*         slt = tbl->mem;
      SSkipListIterator* iter = tSkipListCreateIter(slt);
      while (tSkipListIterNext(iter)) {
        SSkipListNode* node = tSkipListIterGet(iter);
        CacheTerm*     ct = (CacheTerm*)SL_GET_NODE_DATA(node);
        if (ct != NULL) {
          // TODO, add more debug info
          indexInfo("{colVal: %s, version: %d} \t", ct->colVal, ct->version);
        }
      }
      tSkipListDestroyIter(iter);
    }

    indexMemUnRef(tbl);
  }
}

void indexCacheDestroySkiplist(SSkipList* slt) {
  SSkipListIterator* iter = tSkipListCreateIter(slt);
  while (tSkipListIterNext(iter)) {
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
void indexCacheDestroyImm(IndexCache* cache) {
  if (cache == NULL) {
    return;
  }

  MemTable* tbl = NULL;
  taosThreadMutexLock(&cache->mtx);

  tbl = cache->imm;
  cache->imm = NULL;  // or throw int bg thread
  taosThreadCondBroadcast(&cache->finished);

  taosThreadMutexUnlock(&cache->mtx);

  indexMemUnRef(tbl);
  indexMemUnRef(tbl);
}
void indexCacheDestroy(void* cache) {
  IndexCache* pCache = cache;
  if (pCache == NULL) {
    return;
  }
  indexMemUnRef(pCache->mem);
  indexMemUnRef(pCache->imm);
  taosMemoryFree(pCache->colName);

  taosThreadMutexDestroy(&pCache->mtx);
  taosThreadCondDestroy(&pCache->finished);

  taosMemoryFree(pCache);
}

Iterate* indexCacheIteratorCreate(IndexCache* cache) {
  Iterate* iiter = taosMemoryCalloc(1, sizeof(Iterate));
  if (iiter == NULL) {
    return NULL;
  }

  taosThreadMutexLock(&cache->mtx);

  indexMemRef(cache->imm);

  MemTable* tbl = cache->imm;
  iiter->val.val = taosArrayInit(1, sizeof(uint64_t));
  iiter->val.colVal = NULL;
  iiter->iter = tbl != NULL ? tSkipListCreateIter(tbl->mem) : NULL;
  iiter->next = indexCacheIteratorNext;
  iiter->getValue = indexCacheIteratorGetValue;

  taosThreadMutexUnlock(&cache->mtx);

  return iiter;
}
void indexCacheIteratorDestroy(Iterate* iter) {
  if (iter == NULL) {
    return;
  }
  tSkipListDestroyIter(iter->iter);
  iterateValueDestroy(&iter->val, true);
  taosMemoryFree(iter);
}

int indexCacheSchedToMerge(IndexCache* pCache) {
  SSchedMsg schedMsg = {0};
  schedMsg.fp = doMergeWork;
  schedMsg.ahandle = pCache;
  schedMsg.thandle = NULL;
  schedMsg.msg = NULL;

  taosScheduleTask(indexQhandle, &schedMsg);

  return 0;
}

static void indexCacheMakeRoomForWrite(IndexCache* cache) {
  while (true) {
    if (cache->occupiedMem * MEM_ESTIMATE_RADIO < MEM_THRESHOLD) {
      break;
    } else if (cache->imm != NULL) {
      // TODO: wake up by condition variable
      taosThreadCondWait(&cache->finished, &cache->mtx);
    } else {
      indexCacheRef(cache);
      cache->imm = cache->mem;
      cache->mem = indexInternalCacheCreate(cache->type);
      cache->mem->pCache = cache;
      cache->occupiedMem = 0;
      // sched to merge
      // unref cache in bgwork
      indexCacheSchedToMerge(cache);
    }
  }
}
int indexCachePut(void* cache, SIndexTerm* term, uint64_t uid) {
  if (cache == NULL) {
    return -1;
  }
  bool hasJson = INDEX_TYPE_CONTAIN_EXTERN_TYPE(term->colType, TSDB_DATA_TYPE_JSON);

  IndexCache* pCache = cache;
  indexCacheRef(pCache);
  // encode data
  CacheTerm* ct = taosMemoryCalloc(1, sizeof(CacheTerm));
  if (cache == NULL) {
    return -1;
  }
  // set up key
  ct->colType = term->colType;
  if (hasJson) {
    ct->colVal = indexPackJsonData(term);
  } else {
    ct->colVal = (char*)taosMemoryCalloc(1, sizeof(char) * (term->nColVal + 1));
    memcpy(ct->colVal, term->colVal, term->nColVal);
  }
  ct->version = atomic_add_fetch_32(&pCache->version, 1);
  // set value
  ct->uid = uid;
  ct->operaType = term->operType;
  // ugly code, refactor later
  int64_t estimate = sizeof(ct) + strlen(ct->colVal);

  taosThreadMutexLock(&pCache->mtx);
  pCache->occupiedMem += estimate;
  indexCacheMakeRoomForWrite(pCache);
  MemTable* tbl = pCache->mem;
  indexMemRef(tbl);
  tSkipListPut(tbl->mem, (char*)ct);
  indexMemUnRef(tbl);

  taosThreadMutexUnlock(&pCache->mtx);

  indexCacheUnRef(pCache);
  return 0;
  // encode end
}
int indexCacheDel(void* cache, const char* fieldValue, int32_t fvlen, uint64_t uid, int8_t operType) {
  IndexCache* pCache = cache;
  return 0;
}

static int32_t indexQueryMem(MemTable* mem, SIndexTermQuery* query, SIdxTempResult* tr, STermValueType* s) {
  if (mem == NULL) {
    return 0;
  }

  SIndexTerm*     term = query->term;
  EIndexQueryType qtype = query->qType;

  if (INDEX_TYPE_CONTAIN_EXTERN_TYPE(term->colType, TSDB_DATA_TYPE_JSON)) {
    return cacheSearch[1][qtype](mem, term, tr, s);
  } else {
    return cacheSearch[0][qtype](mem, term, tr, s);
  }
}
int indexCacheSearch(void* cache, SIndexTermQuery* query, SIdxTempResult* result, STermValueType* s) {
  int64_t st = taosGetTimestampUs();
  if (cache == NULL) {
    return 0;
  }
  IndexCache* pCache = cache;

  MemTable *mem = NULL, *imm = NULL;
  taosThreadMutexLock(&pCache->mtx);
  mem = pCache->mem;
  imm = pCache->imm;
  indexMemRef(mem);
  indexMemRef(imm);
  taosThreadMutexUnlock(&pCache->mtx);

  int ret = indexQueryMem(mem, query, result, s);
  if (ret == 0 && *s != kTypeDeletion) {
    // continue search in imm
    ret = indexQueryMem(imm, query, result, s);
  }

  indexMemUnRef(mem);
  indexMemUnRef(imm);
  indexInfo("cache search, time cost %" PRIu64 "us", taosGetTimestampUs() - st);

  return ret;
}

void indexCacheRef(IndexCache* cache) {
  if (cache == NULL) {
    return;
  }
  int ref = T_REF_INC(cache);
  UNUSED(ref);
}
void indexCacheUnRef(IndexCache* cache) {
  if (cache == NULL) {
    return;
  }
  int ref = T_REF_DEC(cache);
  if (ref == 0) {
    indexCacheDestroy(cache);
  }
}

void indexMemRef(MemTable* tbl) {
  if (tbl == NULL) {
    return;
  }
  int ref = T_REF_INC(tbl);
  UNUSED(ref);
}
void indexMemUnRef(MemTable* tbl) {
  if (tbl == NULL) {
    return;
  }
  int ref = T_REF_DEC(tbl);
  if (ref == 0) {
    SSkipList* slt = tbl->mem;
    indexCacheDestroySkiplist(slt);
    taosMemoryFree(tbl);
  }
}

static void indexCacheTermDestroy(CacheTerm* ct) {
  if (ct == NULL) {
    return;
  }
  taosMemoryFree(ct->colVal);
  taosMemoryFree(ct);
}
static char* indexCacheTermGet(const void* pData) {
  CacheTerm* p = (CacheTerm*)pData;
  return (char*)p;
}
static int32_t indexCacheTermCompare(const void* l, const void* r) {
  CacheTerm* lt = (CacheTerm*)l;
  CacheTerm* rt = (CacheTerm*)r;
  // compare colVal
  int32_t cmp = strcmp(lt->colVal, rt->colVal);
  if (cmp == 0) {
    return rt->version - lt->version;
  }
  return cmp;
}

static int indexFindCh(char* a, char c) {
  char* p = a;
  while (*p != 0 && *p++ != c) {
  }
  return p - a;
}
static int indexCacheJsonTermCompareImpl(char* a, char* b) {
  // int alen = indexFindCh(a, '&');
  // int blen = indexFindCh(b, '&');

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
static int32_t indexCacheJsonTermCompare(const void* l, const void* r) {
  CacheTerm* lt = (CacheTerm*)l;
  CacheTerm* rt = (CacheTerm*)r;
  // compare colVal
  int32_t cmp = strcmp(lt->colVal, rt->colVal);
  if (cmp == 0) {
    return rt->version - lt->version;
  }
  return cmp;
}
static MemTable* indexInternalCacheCreate(int8_t type) {
  int ttype = INDEX_TYPE_CONTAIN_EXTERN_TYPE(type, TSDB_DATA_TYPE_JSON) ? TSDB_DATA_TYPE_BINARY : type;
  int32_t (*cmpFn)(const void* l, const void* r) =
      INDEX_TYPE_CONTAIN_EXTERN_TYPE(type, TSDB_DATA_TYPE_JSON) ? indexCacheJsonTermCompare : indexCacheTermCompare;

  MemTable* tbl = taosMemoryCalloc(1, sizeof(MemTable));
  indexMemRef(tbl);
  if (ttype == TSDB_DATA_TYPE_BINARY || ttype == TSDB_DATA_TYPE_NCHAR) {
    tbl->mem =
        tSkipListCreate(MAX_SKIP_LIST_LEVEL, ttype, MAX_INDEX_KEY_LEN, cmpFn, SL_ALLOW_DUP_KEY, indexCacheTermGet);
  }
  return tbl;
}

static void doMergeWork(SSchedMsg* msg) {
  IndexCache* pCache = msg->ahandle;
  SIndex*     sidx = (SIndex*)pCache->index;
  indexFlushCacheToTFile(sidx, pCache);
}
static bool indexCacheIteratorNext(Iterate* itera) {
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
    iv->colVal = tstrdup(ct->colVal);
    // printf("col Val: %s\n", iv->colVal);
    // iv->colType = cv->colType;

    taosArrayPush(iv->val, &ct->uid);
  }
  return next;
}

static IterateValue* indexCacheIteratorGetValue(Iterate* iter) {
  // opt later
  return &iter->val;
}
