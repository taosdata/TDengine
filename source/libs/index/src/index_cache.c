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

#include "index_cache.h"
#include "index_util.h"
#include "tcompare.h"
#include "tsched.h"

#define MAX_INDEX_KEY_LEN 256  // test only, change later

#define CACH_LIMIT 1000000
// ref index_cache.h:22
//#define CACHE_KEY_LEN(p) \
//  (sizeof(int32_t) + sizeof(uint16_t) + sizeof(p->colType) + sizeof(p->nColVal) + p->nColVal + sizeof(uint64_t) + sizeof(p->operType))

static void cacheTermDestroy(CacheTerm* ct) {
  if (ct == NULL) { return; }

  free(ct->colVal);
  free(ct);
}
static char* getIndexKey(const void* pData) {
  CacheTerm* p = (CacheTerm*)pData;
  return (char*)p;
}

static int32_t compareKey(const void* l, const void* r) {
  CacheTerm* lt = (CacheTerm*)l;
  CacheTerm* rt = (CacheTerm*)r;

  // compare colVal
  int i, j;
  for (i = 0, j = 0; i < lt->nColVal && j < rt->nColVal; i++, j++) {
    if (lt->colVal[i] == rt->colVal[j]) {
      continue;
    } else {
      return lt->colVal[i] < rt->colVal[j] ? -1 : 1;
    }
  }
  if (i < lt->nColVal) {
    return 1;
  } else if (j < rt->nColVal) {
    return -1;
  }
  // compare version
  return rt->version - lt->version;
}

static SSkipList* indexInternalCacheCreate(int8_t type) {
  if (type == TSDB_DATA_TYPE_BINARY) {
    return tSkipListCreate(MAX_SKIP_LIST_LEVEL, type, MAX_INDEX_KEY_LEN, compareKey, SL_ALLOW_DUP_KEY, getIndexKey);
  }
}

IndexCache* indexCacheCreate(SIndex* idx, const char* colName, int8_t type) {
  IndexCache* cache = calloc(1, sizeof(IndexCache));
  if (cache == NULL) {
    indexError("failed to create index cache");
    return NULL;
  };
  cache->mem = indexInternalCacheCreate(type);

  cache->colName = calloc(1, strlen(colName) + 1);
  memcpy(cache->colName, colName, strlen(colName));
  cache->type = type;
  cache->index = idx;
  cache->version = 0;

  indexCacheRef(cache);
  return cache;
}
void indexCacheDebug(IndexCache* cache) {
  SSkipListIterator* iter = tSkipListCreateIter(cache->mem);
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

void indexCacheDestroySkiplist(SSkipList* slt) {
  SSkipListIterator* iter = tSkipListCreateIter(slt);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* node = tSkipListIterGet(iter);
    CacheTerm*     ct = (CacheTerm*)SL_GET_NODE_DATA(node);
    if (ct != NULL) {}
  }
  tSkipListDestroyIter(iter);
}

void indexCacheDestroy(void* cache) {
  IndexCache* pCache = cache;
  if (pCache == NULL) { return; }
  tSkipListDestroy(pCache->mem);
  tSkipListDestroy(pCache->imm);
  free(pCache->colName);
  free(pCache);
}

static void doMergeWork(SSchedMsg* msg) {
  IndexCache* pCache = msg->ahandle;
  SIndex*     sidx = (SIndex*)pCache->index;
  indexFlushCacheTFile(sidx, pCache);
}
static bool indexCacheIteratorNext(Iterate* itera) {
  SSkipListIterator* iter = itera->iter;
  if (iter == NULL) { return false; }

  IterateValue* iv = &itera->val;
  iterateValueDestroy(iv, false);

  bool next = tSkipListIterNext(iter);
  if (next) {
    SSkipListNode* node = tSkipListIterGet(iter);
    CacheTerm*     ct = (CacheTerm*)SL_GET_NODE_DATA(node);

    iv->type = ct->operaType;
    iv->colVal = ct->colVal;

    taosArrayPush(iv->val, &ct->uid);
  }

  return next;
}

static IterateValue* indexCacheIteratorGetValue(Iterate* iter) {
  return &iter->val;
}
Iterate* indexCacheIteratorCreate(IndexCache* cache) {
  Iterate* iiter = calloc(1, sizeof(Iterate));
  if (iiter == NULL) { return NULL; }

  iiter->val.val = taosArrayInit(1, sizeof(uint64_t));
  iiter->iter = cache->imm != NULL ? tSkipListCreateIter(cache->imm) : NULL;
  iiter->next = indexCacheIteratorNext;
  iiter->getValue = indexCacheIteratorGetValue;

  return iiter;
}
void indexCacheIteratorDestroy(Iterate* iter) {
  if (iter == NULL) { return; }

  tSkipListDestroyIter(iter->iter);
  iterateValueDestroy(&iter->val, true);
  free(iter);
}

int indexCacheSchedToMerge(IndexCache* pCache) {
  SSchedMsg schedMsg = {0};
  schedMsg.fp = doMergeWork;
  schedMsg.ahandle = pCache;
  schedMsg.thandle = NULL;
  schedMsg.msg = NULL;

  taosScheduleTask(indexQhandle, &schedMsg);
}
int indexCachePut(void* cache, SIndexTerm* term, uint64_t uid) {
  if (cache == NULL) { return -1; }

  IndexCache* pCache = cache;
  indexCacheRef(pCache);
  // encode data
  CacheTerm* ct = calloc(1, sizeof(CacheTerm));
  if (cache == NULL) { return -1; }
  // set up key
  ct->colType = term->colType;
  ct->nColVal = term->nColVal;
  ct->colVal = (char*)calloc(1, sizeof(char) * (ct->nColVal + 1));
  memcpy(ct->colVal, term->colVal, ct->nColVal);
  ct->version = atomic_add_fetch_32(&pCache->version, 1);
  // set value
  ct->uid = uid;
  ct->operaType = term->operType;

  tSkipListPut(pCache->mem, (char*)ct);
  pCache->nTerm += 1;

  if (pCache->nTerm >= CACH_LIMIT) {
    pCache->nTerm = 0;

    while (pCache->imm != NULL) {
      // do nothong
    }

    pCache->imm = pCache->mem;
    pCache->mem = indexInternalCacheCreate(pCache->type);

    // sched to merge
    // unref cache int bgwork
    indexCacheSchedToMerge(pCache);
  }
  indexCacheUnRef(pCache);
  return 0;
  // encode end
}
int indexCacheDel(void* cache, const char* fieldValue, int32_t fvlen, uint64_t uid, int8_t operType) {
  IndexCache* pCache = cache;
  return 0;
}
int indexCacheSearch(void* cache, SIndexTermQuery* query, SArray* result, STermValueType* s) {
  if (cache == NULL) { return -1; }
  IndexCache*     pCache = cache;
  SIndexTerm*     term = query->term;
  EIndexQueryType qtype = query->qType;

  CacheTerm* ct = calloc(1, sizeof(CacheTerm));
  if (ct == NULL) { return -1; }
  ct->nColVal = term->nColVal;
  ct->colVal = calloc(1, sizeof(char) * (ct->nColVal + 1));
  memcpy(ct->colVal, term->colVal, ct->nColVal);
  ct->version = atomic_load_32(&pCache->version);

  char* key = getIndexKey(ct);
  // TODO handle multi situation later, and refactor
  SSkipListIterator* iter = tSkipListCreateIterFromVal(pCache->mem, key, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* node = tSkipListIterGet(iter);
    if (node != NULL) {
      CacheTerm* c = (CacheTerm*)SL_GET_NODE_DATA(node);
      if (c->operaType == ADD_VALUE || qtype == QUERY_TERM) {
        if (c->nColVal == ct->nColVal && strncmp(c->colVal, ct->colVal, c->nColVal) == 0) {
          taosArrayPush(result, &c->uid);
          *s = kTypeValue;
        } else {
          break;
        }
      } else if (c->operaType == DEL_VALUE) {
        // table is del, not need
        *s = kTypeDeletion;
        break;
      }
    }
  }
  tSkipListDestroyIter(iter);
  cacheTermDestroy(ct);
  // int32_t keyLen = CACHE_KEY_LEN(term);
  // char*   buf = calloc(1, keyLen);
  if (qtype == QUERY_TERM) {
    //
  } else if (qtype == QUERY_PREFIX) {
    //
  } else if (qtype == QUERY_SUFFIX) {
    //
  } else if (qtype == QUERY_REGEX) {
    //
  }
  return 0;
}

void indexCacheRef(IndexCache* cache) {
  int ref = T_REF_INC(cache);
  UNUSED(ref);
}
void indexCacheUnRef(IndexCache* cache) {
  int ref = T_REF_DEC(cache);
  if (ref == 0) { indexCacheDestroy(cache); }
}
