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

typedef struct SMemData         SMemData;
typedef struct SMemSkipList     SMemSkipList;
typedef struct SMemSkipListNode SMemSkipListNode;

struct SMemSkipListNode {
  int8_t            level;
  SMemSkipListNode *forwards[0];
};

struct SMemSkipList {
  uint32_t          seed;
  int32_t           size;
  int8_t            maxLevel;
  int8_t            level;
  SMemSkipListNode *pHead;
  SMemSkipListNode *pTail;
};

struct SMemData {
  tb_uid_t     suid;
  tb_uid_t     uid;
  TSDBKEY      minKey;
  TSDBKEY      maxKey;
  SDelOp      *delOpHead;
  SDelOp      *delOpTail;
  SMemSkipList sl;
};

struct SMemTable {
  STsdb  *pTsdb;
  int32_t nRef;
  TSDBKEY minKey;
  TSDBKEY maxKey;
  int64_t nRows;
  SArray *pArray;  // SArray<SMemData>
};

#define SL_NODE_SIZE(l)        (sizeof(SMemSkipListNode) + sizeof(SMemSkipListNode *) * (l)*2)
#define SL_NODE_HALF_SIZE(l)   (sizeof(SMemSkipListNode) + sizeof(SMemSkipListNode *) * (l))
#define SL_NODE_FORWARD(n, l)  ((n)->forwards[l])
#define SL_NODE_BACKWARD(n, l) ((n)->forwards[(n)->level + (l)])
#define SL_NODE_DATA(n)        (&SL_NODE_BACKWARD(n, (n)->level))

#define SL_HEAD_FORWARD(sl, l)  SL_NODE_FORWARD((sl)->pHead, l)
#define SL_TAIL_BACKWARD(sl, l) SL_NODE_FORWARD((sl)->pTail, l)

static int32_t tsdbGetOrCreateMemData(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid, SMemData **ppMemData);
static int     memDataPCmprFn(const void *p1, const void *p2);
static int32_t tPutTSDBRow(uint8_t *p, TSDBROW *pRow);
static int32_t tGetTSDBRow(uint8_t *p, TSDBROW *pRow);
static int8_t  tsdbMemSkipListRandLevel(SMemSkipList *pSl);

// SMemTable ==============================================
int32_t tsdbMemTableCreate2(STsdb *pTsdb, SMemTable **ppMemTable) {
  int32_t    code = 0;
  SMemTable *pMemTable = NULL;

  pMemTable = (SMemTable *)taosMemoryCalloc(1, sizeof(*pMemTable));
  if (pMemTable == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pMemTable->pTsdb = pTsdb;
  pMemTable->nRef = 1;
  pMemTable->minKey = (TSDBKEY){.version = INT64_MAX, .ts = TSKEY_MAX};
  pMemTable->maxKey = (TSDBKEY){.version = -1, .ts = TSKEY_MIN};
  pMemTable->nRows = 0;
  pMemTable->pArray = taosArrayInit(512, sizeof(SMemData *));
  if (pMemTable->pArray == NULL) {
    taosMemoryFree(pMemTable);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  *ppMemTable = pMemTable;
  return code;

_err:
  *ppMemTable = NULL;
  return code;
}

void tsdbMemTableDestroy2(SMemTable *pMemTable) {
  taosArrayDestroyEx(pMemTable->pArray, NULL /*TODO*/);
  taosMemoryFree(pMemTable);
}

int32_t tsdbInsertTableData2(STsdb *pTsdb, int64_t version, SVSubmitBlk *pSubmitBlk) {
  int32_t    code = 0;
  SMemTable *pMemTable = (SMemTable *)pTsdb->mem;  // TODO
  SMemData  *pMemData;
  TSDBROW    row = {.version = version};

  ASSERT(pMemTable);

  {
    // check if table exists (todo)
  }

  code = tsdbGetOrCreateMemData(pMemTable, pSubmitBlk->suid, pSubmitBlk->uid, &pMemData);
  if (code) {
    tsdbError("vgId:%d failed to create/get table data since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
    goto _err;
  }

  // do insert
  int32_t           nt;
  uint8_t          *pt;
  int32_t           n = 0;
  uint8_t          *p = pSubmitBlk->pData;
  SVBufPool        *pPool = pTsdb->pVnode->inUse;
  int8_t            level;
  SMemSkipListNode *pNode;
  while (n < pSubmitBlk->nData) {
    nt = tGetTSRow(p + n, &row.tsRow);
    n += nt;

    ASSERT(n <= pSubmitBlk->nData);

    // build the node
    level = tsdbMemSkipListRandLevel(&pMemData->sl);
    pNode = (SMemSkipListNode *)vnodeBufPoolMalloc(pPool, SL_NODE_SIZE(level) + nt + sizeof(version));
    if (pNode == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    pNode->level = level;
    tPutTSDBRow((uint8_t *)SL_NODE_DATA(pNode), &row);

    // put the node (todo)

    // set info
    if (tsdbKeyCmprFn(&row, &pMemData->minKey) < 0) pMemData->minKey = *(TSDBKEY *)&row;
    if (tsdbKeyCmprFn(&row, &pMemData->maxKey) > 0) pMemData->maxKey = *(TSDBKEY *)&row;
  }

  if (tsdbKeyCmprFn(&pMemTable->minKey, &pMemData->minKey) < 0) pMemTable->minKey = pMemData->minKey;
  if (tsdbKeyCmprFn(&pMemTable->maxKey, &pMemData->maxKey) > 0) pMemTable->maxKey = pMemData->maxKey;

  return code;

_err:
  return code;
}

int32_t tsdbDeleteTableData2(STsdb *pTsdb, int64_t version, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey) {
  int32_t    code = 0;
  SMemTable *pMemTable = (SMemTable *)pTsdb->mem;  // TODO
  SMemData  *pMemData;
  SVBufPool *pPool = pTsdb->pVnode->inUse;

  ASSERT(pMemTable);

  {
    // check if table exists (todo)
  }

  code = tsdbGetOrCreateMemData(pMemTable, suid, uid, &pMemData);
  if (code) {
    goto _err;
  }

  // do delete
  SDelOp *pDelOp = (SDelOp *)vnodeBufPoolMalloc(pPool, sizeof(*pDelOp));
  if (pDelOp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pDelOp->version = version;
  pDelOp->sKey = sKey;
  pDelOp->eKey = eKey;
  pDelOp->pNext = NULL;
  if (pMemData->delOpHead == NULL) {
    ASSERT(pMemData->delOpTail == NULL);
    pMemData->delOpHead = pMemData->delOpTail = pDelOp;
  } else {
    pMemData->delOpTail->pNext = pDelOp;
    pMemData->delOpTail = pDelOp;
  }

  {
    // update the state of pMemTable, pMemData, last and lastrow (todo)
  }

  tsdbDebug("vgId:%d delete data from table suid:%" PRId64 " uid:%" PRId64 " sKey:%" PRId64 " eKey:%" PRId64
            " since %s",
            TD_VID(pTsdb->pVnode), suid, uid, sKey, eKey, tstrerror(code));
  return code;

_err:
  tsdbError("vgId:%d failed to delete data from table suid:%" PRId64 " uid:%" PRId64 " sKey:%" PRId64 " eKey:%" PRId64
            " since %s",
            TD_VID(pTsdb->pVnode), suid, uid, sKey, eKey, tstrerror(code));
  return code;
}

static int32_t tsdbGetOrCreateMemData(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid, SMemData **ppMemData) {
  int32_t    code = 0;
  int32_t    idx = 0;
  SMemData  *pMemDataT = &(SMemData){.suid = suid, .uid = uid};
  SMemData  *pMemData = NULL;
  SVBufPool *pPool = pMemTable->pTsdb->pVnode->inUse;
  int8_t     maxLevel = pMemTable->pTsdb->pVnode->config.tsdbCfg.slLevel;

  // get
  idx = taosArraySearchIdx(pMemTable->pArray, &pMemDataT, memDataPCmprFn, TD_GE);
  if (idx >= 0) {
    pMemData = (SMemData *)taosArrayGet(pMemTable->pArray, idx);
    if (memDataPCmprFn(&pMemDataT, &pMemData) == 0) goto _exit;
  }

  // create
  pMemData = vnodeBufPoolMalloc(pPool, sizeof(*pMemData) + SL_NODE_HALF_SIZE(maxLevel) * 2);
  if (pMemData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pMemData->suid = suid;
  pMemData->uid = uid;
  pMemData->minKey = (TSDBKEY){.version = INT64_MAX, .ts = TSKEY_MAX};
  pMemData->maxKey = (TSDBKEY){.version = -1, .ts = TSKEY_MIN};
  pMemData->delOpHead = pMemData->delOpTail = NULL;
  pMemData->sl.seed = taosRand();
  pMemData->sl.size = 0;
  pMemData->sl.maxLevel = maxLevel;
  pMemData->sl.level = 0;
  pMemData->sl.pHead = (SMemSkipListNode *)&pMemData[1];
  pMemData->sl.pTail = (SMemSkipListNode *)POINTER_SHIFT(pMemData->sl.pHead, SL_NODE_HALF_SIZE(maxLevel));

  for (int8_t iLevel = 0; iLevel < pMemData->sl.maxLevel; iLevel++) {
    SL_HEAD_FORWARD(&pMemData->sl, iLevel) = pMemData->sl.pTail;
    SL_TAIL_BACKWARD(&pMemData->sl, iLevel) = pMemData->sl.pHead;
  }

  if (idx < 0) idx = 0;
  if (taosArrayInsert(pMemTable->pArray, idx, &pMemData) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

_exit:
  *ppMemData = pMemData;
  return code;

_err:
  *ppMemData = NULL;
  return code;
}

static int memDataPCmprFn(const void *p1, const void *p2) {
  SMemData *pMemData1 = *(SMemData **)p1;
  SMemData *pMemData2 = *(SMemData **)p2;

  if (pMemData1->suid < pMemData2->suid) {
    return -1;
  } else if (pMemData1->suid > pMemData2->suid) {
    return 1;
  }

  if (pMemData1->uid < pMemData2->uid) {
    return -1;
  } else if (pMemData1->uid > pMemData2->uid) {
    return 1;
  }

  return 0;
}

static int32_t tPutTSDBRow(uint8_t *p, TSDBROW *pRow) {
  int32_t n = 0;

  n += tPutI64(p ? p + n : p, pRow->version);
  n += tPutTSRow(p ? p + n : p, &pRow->tsRow);

  return n;
}

static int32_t tGetTSDBRow(uint8_t *p, TSDBROW *pRow) {
  int32_t n = 0;

  n += tGetI64(p + n, &pRow->version);
  n += tGetTSRow(p + n, &pRow->tsRow);

  return n;
}

static FORCE_INLINE int8_t tsdbMemSkipListRandLevel(SMemSkipList *pSl) {
  int8_t         level = 1;
  int8_t         tlevel = TMIN(pSl->maxLevel, pSl->level + 1);
  const uint32_t factor = 4;

  while ((taosRandR(&pSl->seed) % factor) == 0 && level < tlevel) {
    level++;
  }

  return level;
}

#if 0  //====================================================================================

#define SL_MAX_LEVEL 5

struct SMemSkipListCurosr {
  SMemSkipList     *pSl;
  SMemSkipListNode *pNodes[SL_MAX_LEVEL];
};

typedef struct {
  int64_t       version;
  uint32_t      szRow;
  const STSRow *pRow;
} STsdbRow;

#define HASH_BUCKET(SUID, UID, NBUCKET) (TABS((SUID) + (UID)) % (NBUCKET))

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
static int32_t tsdbMemSkipListCursorMoveTo(SMemSkipListCurosr *pSlc, int64_t version, TSKEY ts, int32_t flags);
static void    tsdbMemSkipListCursorMoveToFirst(SMemSkipListCurosr *pSlc);
static void    tsdbMemSkipListCursorMoveToLast(SMemSkipListCurosr *pSlc);
static int32_t tsdbMemSkipListCursorMoveToNext(SMemSkipListCurosr *pSlc);
static int32_t tsdbMemSkipListCursorMoveToPrev(SMemSkipListCurosr *pSlc);
static SMemSkipListNode *tsdbMemSkipListNodeCreate(SVBufPool *pPool, SMemSkipList *pSl, const STsdbRow *pTRow);

// SMemTable ========================
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
  SMemSkipListNode *forwards[SL_MAX_LEVEL];
  SMemSkipListNode *pNode;
  int32_t           iRow;
  STsdbRow          tRow = {.version = version};
  SEncoder          ec = {0};
  SDecoder          dc = {0};

  tDecoderInit(&dc, pSubmitBlk->pData, pSubmitBlk->nData);
  tsdbMemSkipListCursorInit(pMemTb->pSlc, &pMemData->sl);
  for (iRow = 0;; iRow++) {
    if (tDecodeIsEnd(&dc)) break;

    // decode row
    if (tDecodeBinary(&dc, (uint8_t **)&tRow.pRow, &tRow.szRow) < 0) {
      terrno = TSDB_CODE_INVALID_MSG;
      return -1;
    }

    // move cursor
    tsdbMemSkipListCursorMoveTo(pMemTb->pSlc, version, tRow.pRow->ts, 0);

    // encode row
    pNode = tsdbMemSkipListNodeCreate(pPool, &pMemData->sl, &tRow);
    if (pNode == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

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

static FORCE_INLINE int32_t tsdbEncodeRow(SEncoder *pEncoder, const STsdbRow *pRow) {
  if (tEncodeI64(pEncoder, pRow->version) < 0) return -1;
  if (tEncodeBinary(pEncoder, (const uint8_t *)pRow->pRow, pRow->szRow) < 0) return -1;
  return 0;
}

static FORCE_INLINE int32_t tsdbDecodeRow(SDecoder *pDecoder, STsdbRow *pRow) {
  if (tDecodeI64(pDecoder, &pRow->version) < 0) return -1;
  if (tDecodeBinary(pDecoder, (uint8_t **)&pRow->pRow, &pRow->szRow) < 0) return -1;
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
  // for (int8_t iLevel = 0; iLevel < pSl->maxLevel; iLevel++) {
  //   pSlc->forwards[iLevel] = pHead;
  // }
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

static int32_t tsdbMemSkipListCursorMoveTo(SMemSkipListCurosr *pSlc, int64_t version, TSKEY ts, int32_t flags) {
  SMemSkipListNode **pForwards = NULL;
  SMemSkipList      *pSl = pSlc->pSl;
  int8_t             maxLevel = pSl->maxLevel;
  SMemSkipListNode  *pHead = SL_HEAD_NODE(pSl);
  SMemSkipListNode  *pTail = SL_TAIL_NODE(pSl);

  if (pSl->size == 0) {
    for (int8_t iLevel = 0; iLevel < pSl->maxLevel; iLevel++) {
      pForwards[iLevel] = pHead;
    }
  }

  return 0;
}

static void tsdbMemSkipListCursorMoveToFirst(SMemSkipListCurosr *pSlc) {
  SMemSkipList     *pSl = pSlc->pSl;
  SMemSkipListNode *pHead = SL_HEAD_NODE(pSl);

  for (int8_t iLevel = 0; iLevel < pSl->maxLevel; iLevel++) {
    pSlc->pNodes[iLevel] = pHead;
  }

  tsdbMemSkipListCursorMoveToNext(pSlc);
}

static void tsdbMemSkipListCursorMoveToLast(SMemSkipListCurosr *pSlc) {
  SMemSkipList     *pSl = pSlc->pSl;
  SMemSkipListNode *pTail = SL_TAIL_NODE(pSl);

  for (int8_t iLevel = 0; iLevel < pSl->maxLevel; iLevel++) {
    pSlc->pNodes[iLevel] = pTail;
  }

  tsdbMemSkipListCursorMoveToPrev(pSlc);
}

static int32_t tsdbMemSkipListCursorMoveToNext(SMemSkipListCurosr *pSlc) {
  // TODO
  return 0;
}

static int32_t tsdbMemSkipListCursorMoveToPrev(SMemSkipListCurosr *pSlc) {
  // TODO
  return 0;
}

static SMemSkipListNode *tsdbMemSkipListNodeCreate(SVBufPool *pPool, SMemSkipList *pSl, const STsdbRow *pTRow) {
  int32_t           tsize;
  int32_t           ret;
  int8_t            level = tsdbMemSkipListRandLevel(pSl);
  SMemSkipListNode *pNode = NULL;
  SEncoder          ec = {0};

  tEncodeSize(tsdbEncodeRow, pTRow, tsize, ret);
  pNode = vnodeBufPoolMalloc(pPool, tsize + SL_NODE_SIZE(level));
  if (pNode) {
    pNode->level = level;
    tEncoderInit(&ec, (uint8_t *)SL_NODE_DATA(pNode), tsize);
    tsdbEncodeRow(&ec, pTRow);
    tEncoderClear(&ec);
  }

  return pNode;
}
#endif