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

typedef struct SMemTable        SMemTable;
typedef struct SMemData         SMemData;
typedef struct SMemSkipList     SMemSkipList;
typedef struct SMemSkipListNode SMemSkipListNode;

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

struct SMemSkipListNode {
  int8_t            level;
  SMemSkipListNode *forwards[];
};

struct SMemSkipList {
  uint32_t         seed;
  int8_t           level;
  int32_t          size;
  SMemSkipListNode pHead[];
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
    taosMemoryFree(pMemTb);
    return -1;
  }

  *ppMemTb = pMemTb;
  return 0;
}

int32_t tsdbMemTableDestroy2(STsdb *pTsdb, SMemTable *pMemTb) {
  if (pMemTb) {
    // loop to destroy the contents (todo)
    taosMemoryFree(pMemTb->pBuckets);
    taosMemoryFree(pMemTb);
  }
  return 0;
}

int32_t tsdbInsertData2(SMemTable *pMemTb, int64_t version, const SVSubmitBlk *pSubmitBlk) {
  SMemData         *pMemData;
  SVBufPool        *pPool = pMemTb->pTsdb->pVnode->inUse;
  int32_t           hash;
  int32_t           tlen;
  uint8_t           buf[16];
  int32_t           rSize;
  SMemSkipListNode *pSlNode;
  const STSRow     *pTSRow;

  // search hash
  hash = (pSubmitBlk->suid + pSubmitBlk->uid) % pMemTb->nBucket;
  for (pMemData = pMemTb->pBuckets[hash]; pMemData; pMemData = pMemData->pHashNext) {
    if (pMemData->suid == pSubmitBlk->suid && pMemData->uid == pSubmitBlk->uid) break;
  }

  // create pMemData if need
  if (pMemData == NULL) {
    pMemData = vnodeBufPoolMalloc(pPool, sizeof(*pMemData));
    if (pMemData == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    pMemData->pHashNext = NULL;
    pMemData->suid = pSubmitBlk->suid;
    pMemData->uid = pSubmitBlk->uid;
    pMemData->minKey = TSKEY_MAX;
    pMemData->maxKey = TSKEY_MIN;
    pMemData->minVer = -1;
    pMemData->maxVer = -1;
    pMemData->nRows = 0;
    pMemData->sl.level = 0;
    pMemData->sl.seed = taosRand();
    pMemData->sl.size = 0;

    // add to MemTable
    hash = (pMemData->suid + pMemData->uid) % pMemTb->nBucket;
    pMemData->pHashNext = pMemTb->pBuckets[hash];
    pMemTb->pBuckets[hash] = pMemData;
  }

  // loop to insert data to skiplist
  for (;;) {
    rSize = 0;
    pTSRow = NULL;
    if (pTSRow == NULL) break;

    // check the row (todo)

    // move the cursor to position to write (todo)

    // insert the row
    int8_t level = 1;
    tlen = 0;  // sizeof(int64_t) + tsdbPutLen(rSize) + rSize;
    pSlNode = vnodeBufPoolMalloc(pPool, tlen);
    if (pSlNode == NULL) {
      ASSERT(0);
    }
  }

  return 0;
}

static void tsdbEncodeRow(int64_t version, int32_t rSize, const STSRow *pRow) {}

static void tsdbDecodeRow(int64_t *version, int32_t *rSize, const STSRow **ppRow) {}

// SMemData

// SMemSkipList