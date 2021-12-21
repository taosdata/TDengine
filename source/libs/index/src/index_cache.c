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
#define CACHE_KEY_LEN(p)                                                                                          \
  (sizeof(int32_t) + sizeof(uint16_t) + sizeof(p->colType) + sizeof(p->nColVal) + p->nColVal + sizeof(uint64_t) + \
      sizeof(p->operType))

static char *  getIndexKey(const void *pData) { return NULL; }
static int32_t compareKey(const void *l, const void *r) {
  char *lp = (char *)l;
  char *rp = (char *)r;

  // skip total len, not compare
  int32_t ll, rl;  // len
  memcpy(&ll, lp, sizeof(int32_t));
  memcpy(&rl, rp, sizeof(int32_t));
  lp += sizeof(int32_t);
  rp += sizeof(int32_t);

  // compare field id
  int16_t lf, rf;  // field id
  memcpy(&lf, lp, sizeof(lf));
  memcpy(&rf, rp, sizeof(rf));
  if (lf != rf) { return lf < rf ? -1 : 1; }
  lp += sizeof(lf);
  rp += sizeof(rf);

  // compare field type
  int8_t lft, rft;
  memcpy(&lft, lp, sizeof(lft));
  memcpy(&rft, rp, sizeof(rft));
  lp += sizeof(lft);
  rp += sizeof(rft);
  assert(rft == rft);

  // skip value len
  int32_t lfl, rfl;
  memcpy(&lfl, lp, sizeof(lfl));
  memcpy(&rfl, rp, sizeof(rfl));
  lp += sizeof(lfl);
  rp += sizeof(rfl);

  // compare value
  int32_t i, j;
  for (i = 0, j = 0; i < lfl && j < rfl; i++, j++) {
    if (lp[i] == rp[j]) {
      continue;
    } else {
      return lp[i] < rp[j] ? -1 : 1;
    }
  }
  if (i < lfl) {
    return 1;
  } else if (j < rfl) {
    return -1;
  }
  lp += lfl;
  rp += rfl;

  // skip uid
  uint64_t lu, ru;
  memcpy(&lu, lp, sizeof(lu));
  memcpy(&ru, rp, sizeof(ru));
  lp += sizeof(lu);
  rp += sizeof(ru);

  // compare version, desc order
  int32_t lv, rv;
  memcpy(&lv, lp, sizeof(lv));
  memcpy(&rv, rp, sizeof(rv));
  if (lv != rv) { return lv > rv ? -1 : 1; }

  lp += sizeof(lv);
  rp += sizeof(rv);
  // not care item type

  return 0;
}
IndexCache *indexCacheCreate() {
  IndexCache *cache = calloc(1, sizeof(IndexCache));
  if (cache == NULL) {
    indexError("failed to create index cache");
    return NULL;
  }
  cache->skiplist = tSkipListCreate(
      MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_BINARY, MAX_INDEX_KEY_LEN, compareKey, SL_ALLOW_DUP_KEY, getIndexKey);
  return cache;
}

void indexCacheDestroy(void *cache) {
  IndexCache *pCache = cache;
  if (pCache == NULL) { return; }
  tSkipListDestroy(pCache->skiplist);
  free(pCache);
}

int indexCachePut(void *cache, SIndexTerm *term, int16_t colId, int32_t version, uint64_t uid) {
  if (cache == NULL) { return -1; }

  IndexCache *pCache = cache;
  // encode data
  int32_t total = CACHE_KEY_LEN(term);

  char *buf = calloc(1, total);
  char *p = buf;

  SERIALIZE_VAR_TO_BUF(p, total, int32_t);
  SERIALIZE_VAR_TO_BUF(p, colId, int16_t);

  SERIALIZE_MEM_TO_BUF(p, term, colType);
  SERIALIZE_MEM_TO_BUF(p, term, nColVal);
  SERIALIZE_STR_MEM_TO_BUF(p, term, colVal, term->nColVal);

  SERIALIZE_VAR_TO_BUF(p, version, int32_t);
  SERIALIZE_VAR_TO_BUF(p, uid, uint64_t);

  SERIALIZE_MEM_TO_BUF(p, term, operType);

  tSkipListPut(pCache->skiplist, (void *)buf);
  return 0;
  // encode end
}
int indexCacheDel(void *cache, int32_t fieldId, const char *fieldValue, int32_t fvlen, uint64_t uid, int8_t operType) {
  IndexCache *pCache = cache;
  return 0;
}
int indexCacheSearch(void *cache,
    SIndexTermQuery *      query,
    int16_t                colId,
    int32_t                version,
    SArray *               result,
    STermValueType *       s) {
  if (cache == NULL) { return -1; }
  IndexCache *    pCache = cache;
  SIndexTerm *    term = query->term;
  EIndexQueryType qtype = query->qType;

  int32_t keyLen = CACHE_KEY_LEN(term);

  char *buf = calloc(1, keyLen);
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
