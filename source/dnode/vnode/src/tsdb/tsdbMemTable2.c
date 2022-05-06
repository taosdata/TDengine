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

#include "tsdb.h"

typedef struct SMemTable       SMemTable;
typedef struct SMemData        SMemData;
typedef struct SMemSkipList    SMemSkipList;
typedef struct SMemSkipListCfg SMemSkipListCfg;

struct SMemTable {
  STsdb     *pTsdb;
  TSKEY      minKey;
  TSKEY      maxKey;
  int64_t    minVer;
  int64_t    maxVer;
  int64_t    nRows;
  int32_t    nHash;
  int32_t    nBucket;
  SMemData **pBuckets;
};

struct SMemSkipListCfg {
  int8_t  maxLevel;
  int32_t nKey;
  int32_t nData;
};

struct SMemSkipList {
  int8_t   level;
  uint32_t seed;
};

struct SMemData {
  SMemData    *pHashNext;
  tb_uid_t     suid;
  tb_uid_t     uid;
  TSKEY        minKey;
  TSKEY        maxKey;
  int64_t      minVer;
  int64_t      maxVer;
  int64_t      nRows;
  SMemSkipList sl;
};

// SMemTable
int32_t tsdbMemTableCreate2(STsdb *pTsdb, SMemTable **ppMemTb) {
  SMemTable *pMemTb = NULL;

  pMemTb = taosMemoryCalloc(1, sizeof(*pMemTb));
  if (pMemTb == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMemTb->pTsdb = pTsdb;
  pMemTb->minKey = TSKEY_MAX;
  pMemTb->maxKey = TSKEY_MIN;
  pMemTb->minVer = -1;
  pMemTb->maxVer = -1;
  pMemTb->nRows = 0;
  pMemTb->nHash = 0;
  pMemTb->nBucket = 1024;
  pMemTb->pBuckets = taosMemoryCalloc(pMemTb->nBucket, sizeof(*pMemTb->pBuckets));
  if (pMemTb->pBuckets == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  *ppMemTb = pMemTb;
  return 0;
}

int32_t tsdbMemTableDestroy2(STsdb *pTsdb, SMemTable *pMT) {
  // TODO
  return 0;
}

// SMemData

// SMemSkipList