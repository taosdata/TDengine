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

#define MAX_INDEX_KEY_LEN 256  // test only, change later

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

  // compare colId
  if (lt->colId != rt->colId) { return lt->colId - rt->colId; }

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

  // char*      lp = (char*)l;
  // char*      rp = (char*)r;

  //// compare col id
  // int16_t lf, rf;  // cold id
  // memcpy(&lf, lp, sizeof(lf));
  // memcpy(&rf, rp, sizeof(rf));
  // if (lf != rf) { return lf < rf ? -1 : 1; }

  // lp += sizeof(lf);
  // rp += sizeof(rf);

  //// skip value len
  // int32_t lfl, rfl;
  // memcpy(&lfl, lp, sizeof(lfl));
  // memcpy(&rfl, rp, sizeof(rfl));
  // lp += sizeof(lfl);
  // rp += sizeof(rfl);

  //// compare value
  // int32_t i, j;
  // for (i = 0, j = 0; i < lfl && j < rfl; i++, j++) {
  //  if (lp[i] == rp[j]) {
  //    continue;
  //  } else {
  //    return lp[i] < rp[j] ? -1 : 1;
  //  }
  //}
  // if (i < lfl) {
  //  return 1;
  //} else if (j < rfl) {
  //  return -1;
  //}
  // lp += lfl;
  // rp += rfl;

  //// compare version, desc order
  // int32_t lv, rv;
  // memcpy(&lv, lp, sizeof(lv));
  // memcpy(&rv, rp, sizeof(rv));
  // if (lv != rv) { return lv < rv ? 1 : -1; }

  // return 0;
}
IndexCache* indexCacheCreate() {
  IndexCache* cache = calloc(1, sizeof(IndexCache));
  if (cache == NULL) {
    indexError("failed to create index cache");
    return NULL;
  }
  cache->skiplist =
      tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_BINARY, MAX_INDEX_KEY_LEN, compareKey, SL_ALLOW_DUP_KEY, getIndexKey);
  return cache;
}
void indexCacheDebug(IndexCache* cache) {
  SSkipListIterator* iter = tSkipListCreateIter(cache->skiplist);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* node = tSkipListIterGet(iter);
    CacheTerm*     ct = (CacheTerm*)SL_GET_NODE_DATA(node);
    if (ct != NULL) {
      // TODO, add more debug info
      indexInfo("{colId:%d, colVal: %s, version: %d} \t", ct->colId, ct->colVal, ct->version);
    }
  }
  tSkipListDestroyIter(iter);
}

void indexCacheDestroy(void* cache) {
  IndexCache* pCache = cache;
  if (pCache == NULL) { return; }
  tSkipListDestroy(pCache->skiplist);
  free(pCache);
}

int indexCachePut(void* cache, SIndexTerm* term, int16_t colId, int32_t version, uint64_t uid) {
  if (cache == NULL) { return -1; }

  IndexCache* pCache = cache;
  // encode data
  CacheTerm* ct = calloc(1, sizeof(CacheTerm));
  if (cache == NULL) { return -1; }
  // set up key
  ct->colId = colId;
  ct->colType = term->colType;
  ct->nColVal = term->nColVal;
  ct->colVal = (char*)calloc(1, sizeof(char) * (ct->nColVal + 1));
  memcpy(ct->colVal, term->colVal, ct->nColVal);
  ct->version = version;

  ct->uid = uid;
  ct->operaType = term->operType;

  tSkipListPut(pCache->skiplist, (char*)ct);
  return 0;
  // encode end
}
int indexCacheDel(void* cache, int32_t fieldId, const char* fieldValue, int32_t fvlen, uint64_t uid, int8_t operType) {
  IndexCache* pCache = cache;
  return 0;
}
int indexCacheSearch(void* cache, SIndexTermQuery* query, int16_t colId, int32_t version, SArray* result, STermValueType* s) {
  if (cache == NULL) { return -1; }
  IndexCache*     pCache = cache;
  SIndexTerm*     term = query->term;
  EIndexQueryType qtype = query->qType;

  CacheTerm* ct = calloc(1, sizeof(CacheTerm));
  if (ct == NULL) { return -1; }
  ct->colId = colId;
  ct->nColVal = term->nColVal;
  ct->colVal = calloc(1, sizeof(char) * (ct->nColVal + 1));
  memcpy(ct->colVal, term->colVal, ct->nColVal);
  ct->version = version;

  char* key = getIndexKey(ct);
  // TODO handle multi situation later, and refactor
  SSkipListIterator* iter = tSkipListCreateIterFromVal(pCache->skiplist, key, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
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
