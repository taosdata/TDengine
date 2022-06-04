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

#define SL_MAX_LEVEL 5

#define SL_NODE_SIZE(l)        (sizeof(SMemSkipListNode) + sizeof(SMemSkipListNode *) * (l)*2)
#define SL_NODE_FORWARD(n, l)  ((n)->forwards[l])
#define SL_NODE_BACKWARD(n, l) ((n)->forwards[(n)->level + (l)])
#define SL_NODE_DATA(n)        (&SL_NODE_BACKWARD(n, (n)->level))

static int32_t tsdbGetOrCreateMemData(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid, SMemData **ppMemData);
static int     memDataPCmprFn(const void *p1, const void *p2);
static int32_t tPutTSDBRow(uint8_t *p, TSDBROW *pRow);
static int32_t tGetTSDBRow(uint8_t *p, TSDBROW *pRow);
static int8_t  tsdbMemSkipListRandLevel(SMemSkipList *pSl);
static void    memDataMovePosTo(SMemData *pMemData, SMemSkipListNode **pos, TSDBROW *pRow, int8_t forward);
static int32_t memDataDoPut(SMemData *pMemData, SMemSkipListNode **pos, TSDBROW *pRow, int8_t forward);
static int32_t tsdbInsertTableDataImpl(SMemTable *pMemTable, SMemData *pMemData, int64_t version,
                                       SVSubmitBlk *pSubmitBlk);

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
  ASSERT(pSubmitBlk->nData > 0);

  {
    // check if table exists (todo)
  }

  code = tsdbGetOrCreateMemData(pMemTable, pSubmitBlk->suid, pSubmitBlk->uid, &pMemData);
  if (code) {
    tsdbError("vgId:%d, failed to create/get table data since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
    goto _err;
  }

  // do insert
  code = tsdbInsertTableDataImpl(pMemTable, pMemData, version, pSubmitBlk);
  if (code) {
    goto _err;
  }

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

  tsdbDebug("vgId:%d, delete data from table suid:%" PRId64 " uid:%" PRId64 " sKey:%" PRId64 " eKey:%" PRId64
            " since %s",
            TD_VID(pTsdb->pVnode), suid, uid, sKey, eKey, tstrerror(code));
  return code;

_err:
  tsdbError("vgId:%d, failed to delete data from table suid:%" PRId64 " uid:%" PRId64 " sKey:%" PRId64 " eKey:%" PRId64
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
  pMemData = vnodeBufPoolMalloc(pPool, sizeof(*pMemData) + SL_NODE_SIZE(maxLevel) * 2);
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
  pMemData->sl.pTail = (SMemSkipListNode *)POINTER_SHIFT(pMemData->sl.pHead, SL_NODE_SIZE(maxLevel));
  pMemData->sl.pHead->level = maxLevel;
  pMemData->sl.pTail->level = maxLevel;

  for (int8_t iLevel = 0; iLevel < pMemData->sl.maxLevel; iLevel++) {
    SL_NODE_FORWARD(pMemData->sl.pHead, iLevel) = pMemData->sl.pTail;
    SL_NODE_BACKWARD(pMemData->sl.pHead, iLevel) = NULL;
    SL_NODE_BACKWARD(pMemData->sl.pTail, iLevel) = pMemData->sl.pHead;
    SL_NODE_FORWARD(pMemData->sl.pTail, iLevel) = NULL;
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

static int32_t tsdbInsertTableDataImpl(SMemTable *pMemTable, SMemData *pMemData, int64_t version,
                                       SVSubmitBlk *pSubmitBlk) {
  int32_t  code = 0;
  int32_t  n = 0;
  uint8_t *p = pSubmitBlk->pData;
  int32_t  nRow = 0;
  TSDBROW  row = {.version = version};

  SMemSkipListNode *pos[SL_MAX_LEVEL];

  ASSERT(pSubmitBlk->nData);

  // backward put first data
  n += tGetTSRow(p + n, &row.tsRow);
  ASSERT(n <= pSubmitBlk->nData);

  memDataMovePosTo(pMemData, pos, &row, 0);
  code = memDataDoPut(pMemData, pos, &row, 0);
  if (code) {
    goto _exit;
  }
  nRow++;

  // forward put rest
  while (n < pSubmitBlk->nData) {
    n += tGetTSRow(p + n, &row.tsRow);
    ASSERT(n <= pSubmitBlk->nData);

    // TODO
    memDataMovePosTo(pMemData, pos, &row, 1);
    code = memDataDoPut(pMemData, pos, &row, 1);
    if (code) {
      goto _exit;
    }

    nRow++;
  }

_exit:
  return code;
}

static void memDataMovePosTo(SMemData *pMemData, SMemSkipListNode **pos, TSDBROW *pRow, int8_t forward) {
  SMemSkipListNode *px;
  SMemSkipListNode *pn;
  TSDBKEY          *pKey;
  int               c;

  if (forward) {
    px = pMemData->sl.pHead;
    for (int8_t iLevel = pMemData->sl.maxLevel - 1; iLevel >= 0; iLevel--) {
      pn = SL_NODE_FORWARD(px, iLevel);
      while (pn != pMemData->sl.pTail) {
        pKey = (TSDBKEY *)SL_NODE_DATA(pn);

        c = tsdbKeyCmprFn(pKey, pRow);
        if (pKey >= 0) {
          break;
        } else {
          px = pn;
          pn = SL_NODE_FORWARD(px, iLevel);
        }
      }
      pos[iLevel] = px;
    }
  } else {
    px = pMemData->sl.pTail;
    for (int8_t iLevel = pMemData->sl.maxLevel - 1; iLevel >= 0; iLevel--) {
      if (iLevel < pMemData->sl.level) {
        pn = SL_NODE_BACKWARD(px, iLevel);
        while (pn != pMemData->sl.pHead) {
          pKey = (TSDBKEY *)SL_NODE_DATA(pn);

          c = tsdbKeyCmprFn(pKey, pRow);
          if (c <= 0) {
            break;
          } else {
            px = pn;
            pn = SL_NODE_BACKWARD(px, iLevel);
          }
        }
      }
      pos[iLevel] = px;
    }
  }
}

static int32_t memDataDoPut(SMemData *pMemData, SMemSkipListNode **pos, TSDBROW *pRow, int8_t forward) {
  int32_t code = 0;
  // TODO
  return code;
}