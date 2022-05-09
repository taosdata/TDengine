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

typedef struct SMemTable          SMemTable;
typedef struct SMemData           SMemData;
typedef struct SMemSkipList       SMemSkipList;
typedef struct SMemSkipListNode   SMemSkipListNode;
typedef struct SMemSkipListCurosr SMemSkipListCurosr;

struct SMemTable {
  STsdb              *pTsdb;
  TSKEY               minKey;
  TSKEY               maxKey;
  int64_t             minVer;
  int64_t             maxVer;
  int64_t             nRows;
  int32_t             nHash;
  int32_t             nBucket;
  SMemData          **pBuckets;
  SMemSkipListCurosr *pSlc;
};

struct SMemSkipListNode {
  int8_t            level;
  SMemSkipListNode *forwards[];
};

struct SMemSkipList {
  uint32_t         seed;
  int8_t           maxLevel;
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

struct SMemSkipListCurosr {
  SMemSkipList     *pSl;
  SMemSkipListNode *forwards[];
};

typedef struct {
  int64_t       version;
  uint32_t      szRow;
  const STSRow *pRow;
} STsdbRow;

#define HASH_BUCKET(SUID, UID, NBUCKET) (TABS((SUID) + (UID)) % (NBUCKET))

#define SL_NODE_SIZE(l)        (sizeof(SMemSkipListNode) + sizeof(SMemSkipListNode *) * (l)*2)
#define SL_NODE_HALF_SIZE(l)   (sizeof(SMemSkipListNode) + sizeof(SMemSkipListNode *) * (l))
#define SL_NODE_FORWARD(n, l)  ((n)->forwards[l])
#define SL_NODE_BACKWARD(n, l) ((n)->forwards[(n)->level + (l)])
#define SL_NODE_DATA(n)        (&SL_NODE_BACKWARD(n, (n)->level))

#define SL_HEAD_NODE(sl)            ((sl)->pHead)
#define SL_TAIL_NODE(sl)            ((SMemSkipListNode *)&SL_NODE_FORWARD(SL_HEAD_NODE(sl), (sl)->maxLevel))
#define SL_HEAD_NODE_FORWARD(n, l)  SL_NODE_FORWARD(n, l)
#define SL_TAIL_NODE_BACKWARD(n, l) SL_NODE_FORWARD(n, l)

static int8_t  tsdbMemSkipListRandLevel(SMemSkipList *pSl);
static int32_t tsdbEncodeRow(SEncoder *pEncoder, const STsdbRow *pRow);
static int32_t tsdbDecodeRow(SDecoder *pDecoder, STsdbRow *pRow);
static int32_t tsdbMemSkipListCursorCreate(int8_t maxLevel, SMemSkipListCurosr **ppSlc);
static void    tsdbMemSkipListCursorDestroy(SMemSkipListCurosr *pSlc);
static void    tsdbMemSkipListCursorInit(SMemSkipListCurosr *pSlc, SMemSkipList *pSl);
static void    tsdbMemSkipListCursorPut(SMemSkipListCurosr *pSlc, SMemSkipListNode *pNode);

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
  if (tsdbMemSkipListCursorCreate(pTsdb->pVnode->config.tsdbCfg.slLevel, &pMemTb->pSlc) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pMemTb->pBuckets);
    taosMemoryFree(pMemTb);
  }

  *ppMemTb = pMemTb;
  return 0;
}

int32_t tsdbMemTableDestroy2(STsdb *pTsdb, SMemTable *pMemTb) {
  if (pMemTb) {
    // loop to destroy the contents (todo)
    tsdbMemSkipListCursorDestroy(pMemTb->pSlc);
    taosMemoryFree(pMemTb->pBuckets);
    taosMemoryFree(pMemTb);
  }
  return 0;
}

int32_t tsdbInsertData2(SMemTable *pMemTb, int64_t version, const SVSubmitBlk *pSubmitBlk) {
  SMemData  *pMemData;
  STsdb     *pTsdb = pMemTb->pTsdb;
  SVnode    *pVnode = pTsdb->pVnode;
  SVBufPool *pPool = pVnode->inUse;
  tb_uid_t   suid = pSubmitBlk->suid;
  tb_uid_t   uid = pSubmitBlk->uid;
  int32_t    iBucket;

  // search SMemData by hash
  iBucket = HASH_BUCKET(suid, uid, pMemTb->nBucket);
  for (pMemData = pMemTb->pBuckets[iBucket]; pMemData; pMemData = pMemData->pHashNext) {
    if (pMemData->suid == suid && pMemData->uid == uid) break;
  }

  // create pMemData if need
  if (pMemData == NULL) {
    int8_t            maxLevel = pVnode->config.tsdbCfg.slLevel;
    int32_t           tsize = sizeof(*pMemData) + SL_NODE_HALF_SIZE(maxLevel) * 2;
    SMemSkipListNode *pHead, *pTail;

    pMemData = vnodeBufPoolMalloc(pPool, tsize);
    if (pMemData == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    pMemData->pHashNext = NULL;
    pMemData->suid = suid;
    pMemData->uid = uid;
    pMemData->minKey = TSKEY_MAX;
    pMemData->maxKey = TSKEY_MIN;
    pMemData->minVer = -1;
    pMemData->maxVer = -1;
    pMemData->nRows = 0;
    pMemData->sl.seed = taosRand();
    pMemData->sl.maxLevel = maxLevel;
    pMemData->sl.level = 0;
    pMemData->sl.size = 0;
    pHead = SL_HEAD_NODE(&pMemData->sl);
    pTail = SL_TAIL_NODE(&pMemData->sl);
    pHead->level = maxLevel;
    pTail->level = maxLevel;
    for (int iLevel = 0; iLevel < maxLevel; iLevel++) {
      SL_HEAD_NODE_FORWARD(pHead, iLevel) = pTail;
      SL_TAIL_NODE_BACKWARD(pTail, iLevel) = pHead;
    }

    // add to hash
    if (pMemTb->nHash >= pMemTb->nBucket) {
      // rehash (todo)
    }
    iBucket = HASH_BUCKET(suid, uid, pMemTb->nBucket);
    pMemData->pHashNext = pMemTb->pBuckets[iBucket];
    pMemTb->pBuckets[iBucket] = pMemData;
    pMemTb->nHash++;

    // sort organize (todo)
  }

  // do insert data to SMemData
  STsdbRow tRow = {.version = version};
  SEncoder ec = {0};
  SDecoder dc = {0};

  tDecoderInit(&dc, pSubmitBlk->pData, pSubmitBlk->nData);
  tsdbMemSkipListCursorInit(pMemTb->pSlc, &pMemData->sl);
  for (;;) {
    if (tDecodeIsEnd(&dc)) break;

    if (tDecodeBinary(&dc, (const uint8_t **)&tRow.pRow, &tRow.szRow) < 0) {
      terrno = TSDB_CODE_INVALID_MSG;
      return -1;
    }

    // move cursor

    // encode row
    int8_t  level = tsdbMemSkipListRandLevel(&pMemData->sl);
    int32_t tsize;
    int32_t ret;
    tEncodeSize(tsdbEncodeRow, &tRow, tsize, ret);
    SMemSkipListNode *pNode = vnodeBufPoolMalloc(pPool, tsize + SL_NODE_SIZE(level));
    if (pNode == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    pNode->level = level;
    tEncoderInit(&ec, (uint8_t *)SL_NODE_DATA(pNode), tsize);
    ret = tsdbEncodeRow(&ec, &tRow);
    ASSERT(ret == 0);
    tEncoderClear(&ec);

    // put the node
    tsdbMemSkipListCursorPut(pMemTb->pSlc, pNode);

    // update status
    if (tRow.pRow->ts < pMemData->minKey) pMemData->minKey = tRow.pRow->ts;
    if (tRow.pRow->ts > pMemData->maxKey) pMemData->maxKey = tRow.pRow->ts;
  }
  tDecoderClear(&dc);

  // update status
  if (pMemData->minVer == -1) pMemData->minVer = version;
  if (pMemData->maxVer == -1 || pMemData->maxVer < version) pMemData->maxVer = version;

  if (pMemTb->minKey < pMemData->minKey) pMemTb->minKey = pMemData->minKey;
  if (pMemTb->maxKey < pMemData->maxKey) pMemTb->maxKey = pMemData->maxKey;
  if (pMemTb->minVer == -1) pMemTb->minVer = version;
  if (pMemTb->maxVer == -1 || pMemTb->maxVer < version) pMemTb->maxVer = version;

  return 0;
}

static FORCE_INLINE int8_t tsdbMemSkipListRandLevel(SMemSkipList *pSl) {
  int8_t         level = 1;
  int8_t         tlevel;
  const uint32_t factor = 4;

  if (pSl->size) {
    tlevel = TMIN(pSl->maxLevel, pSl->level + 1);
    while ((taosRandR(&pSl->seed) % factor) == 0 && level < tlevel) {
      level++;
    }
  }

  return level;
}

static FORCE_INLINE int32_t tsdbEncodeRow(SEncoder *pEncoder, const STsdbRow *pRow) {
  if (tEncodeI64(pEncoder, pRow->version) < 0) return -1;
  if (tEncodeBinary(pEncoder, (const uint8_t *)pRow->pRow, pRow->szRow) < 0) return -1;
  return 0;
}

static FORCE_INLINE int32_t tsdbDecodeRow(SDecoder *pDecoder, STsdbRow *pRow) {
  if (tDecodeI64(pDecoder, &pRow->version) < 0) return -1;
  if (tDecodeBinary(pDecoder, (const uint8_t **)&pRow->pRow, &pRow->szRow) < 0) return -1;
  return 0;
}

static int32_t tsdbMemSkipListCursorCreate(int8_t maxLevel, SMemSkipListCurosr **ppSlc) {
  *ppSlc = (SMemSkipListCurosr *)taosMemoryCalloc(1, sizeof(**ppSlc) + sizeof(SMemSkipListNode *) * maxLevel);
  if (*ppSlc == NULL) {
    return -1;
  }
  return 0;
}

static void tsdbMemSkipListCursorDestroy(SMemSkipListCurosr *pSlc) { taosMemoryFree(pSlc); }

static void tsdbMemSkipListCursorInit(SMemSkipListCurosr *pSlc, SMemSkipList *pSl) {
  SMemSkipListNode *pHead = SL_HEAD_NODE(pSl);
  pSlc->pSl = pSl;
  for (int8_t iLevel = 0; iLevel < pSl->maxLevel; iLevel++) {
    pSlc->forwards[iLevel] = pHead;
  }
}

static void tsdbMemSkipListCursorPut(SMemSkipListCurosr *pSlc, SMemSkipListNode *pNode) {
  SMemSkipList     *pSl = pSlc->pSl;
  SMemSkipListNode *pNodeNext;

  for (int8_t iLevel = 0; iLevel < pNode->level; iLevel++) {
    // todo

    ASSERT(0);
  }

  if (pSl->level < pNode->level) {
    pSl->level = pNode->level;
  }

  pSl->size += 1;
}