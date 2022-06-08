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

typedef struct {
  tb_uid_t  uid;
  STSchema *pTSchema;
} SSkmInfo;

#define SL_MAX_LEVEL 5

#define SL_NODE_SIZE(l)        (sizeof(SMemSkipListNode) + sizeof(SMemSkipListNode *) * (l)*2)
#define SL_NODE_FORWARD(n, l)  ((n)->forwards[l])
#define SL_NODE_BACKWARD(n, l) ((n)->forwards[(n)->level + (l)])
#define SL_NODE_DATA(n)        (&SL_NODE_BACKWARD(n, (n)->level))

#define SL_MOVE_BACKWARD 0x1
#define SL_MOVE_FROM_POS 0x2

static int32_t tsdbGetOrCreateMemData(SMemTable2 *pMemTable, tb_uid_t suid, tb_uid_t uid, SMemData **ppMemData);
static int     memDataPCmprFn(const void *p1, const void *p2);
static int32_t tPutTSDBRow(uint8_t *p, TSDBROW *pRow);
static int32_t tGetTSDBRow(uint8_t *p, TSDBROW *pRow);
static int8_t  tsdbMemSkipListRandLevel(SMemSkipList *pSl);
static int32_t tsdbInsertTableDataImpl(SMemTable2 *pMemTable, SMemData *pMemData, int64_t version,
                                       SVSubmitBlk *pSubmitBlk);
static void    memDataMovePosTo(SMemData *pMemData, SMemSkipListNode **pos, TSDBKEY *pKey, int32_t flags);

// SMemTable ==============================================
int32_t tsdbMemTableCreate2(STsdb *pTsdb, SMemTable2 **ppMemTable) {
  int32_t     code = 0;
  SMemTable2 *pMemTable = NULL;

  pMemTable = (SMemTable2 *)taosMemoryCalloc(1, sizeof(*pMemTable));
  if (pMemTable == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pMemTable->pTsdb = pTsdb;
  pMemTable->nRef = 1;
  pMemTable->minKey = (TSDBKEY){.version = INT64_MAX, .ts = TSKEY_MAX};
  pMemTable->maxKey = (TSDBKEY){.version = -1, .ts = TSKEY_MIN};
  pMemTable->nRows = 0;
  pMemTable->nDelOp = 0;
  pMemTable->aMemData = taosArrayInit(512, sizeof(SMemData *));
  if (pMemTable->aMemData == NULL) {
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

void tsdbMemTableDestroy2(SMemTable2 *pMemTable) {
  taosArrayDestroyEx(pMemTable->aMemData, NULL /*TODO*/);
  taosMemoryFree(pMemTable);
}

int32_t tsdbInsertTableData2(STsdb *pTsdb, int64_t version, SVSubmitBlk *pSubmitBlk) {
  int32_t     code = 0;
  SMemTable2 *pMemTable = (SMemTable2 *)pTsdb->mem;  // TODO
  SMemData   *pMemData;
  TSDBROW     row = {.version = version};

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
  int32_t     code = 0;
  SMemTable2 *pMemTable = (SMemTable2 *)pTsdb->mem;  // TODO
  SMemData   *pMemData;
  SVBufPool  *pPool = pTsdb->pVnode->inUse;

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

  pMemTable->nDelOp++;

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

void tsdbMemDataIterOpen(SMemData *pMemData, TSDBKEY *pKey, int8_t backward, SMemDataIter *pIter) {
  SMemSkipListNode *pos[SL_MAX_LEVEL];

  pIter->pMemData = pMemData;
  pIter->backward = backward;
  pIter->pRow = NULL;
  if (pKey == NULL) {
    // create from head or tail
    if (backward) {
      pIter->pNode = SL_NODE_BACKWARD(pMemData->sl.pTail, 0);
    } else {
      pIter->pNode = SL_NODE_FORWARD(pMemData->sl.pHead, 0);
    }
  } else {
    // create from a key
    if (backward) {
      memDataMovePosTo(pMemData, pos, pKey, SL_MOVE_BACKWARD);
      pIter->pNode = SL_NODE_BACKWARD(pos[0], 0);
    } else {
      memDataMovePosTo(pMemData, pos, pKey, 0);
      pIter->pNode = SL_NODE_FORWARD(pos[0], 0);
    }
  }
}

bool tsdbMemDataIterNext(SMemDataIter *pIter) {
  SMemSkipListNode *pHead = pIter->pMemData->sl.pHead;
  SMemSkipListNode *pTail = pIter->pMemData->sl.pTail;

  pIter->pRow = NULL;
  if (pIter->backward) {
    ASSERT(pIter->pNode != pTail);

    if (pIter->pNode == pHead) {
      return false;
    }

    pIter->pNode = SL_NODE_BACKWARD(pIter->pNode, 0);
    if (pIter->pNode == pHead) {
      return false;
    }
  } else {
    ASSERT(pIter->pNode != pHead);

    if (pIter->pNode == pTail) {
      return false;
    }

    pIter->pNode = SL_NODE_FORWARD(pIter->pNode, 0);
    if (pIter->pNode == pTail) {
      return false;
    }
  }

  return true;
}

void tsdbMemDataIterGet(SMemDataIter *pIter, TSDBROW **ppRow) {
  if (pIter->pRow) {
    *ppRow = pIter->pRow;
  } else {
    SMemSkipListNode *pHead = pIter->pMemData->sl.pHead;
    SMemSkipListNode *pTail = pIter->pMemData->sl.pTail;

    if (pIter->backward) {
      ASSERT(pIter->pNode != pTail);

      if (pIter->pNode == pHead) {
        *ppRow = NULL;
      } else {
        tGetTSDBRow((uint8_t *)SL_NODE_DATA(pIter->pNode), &pIter->row);
        *ppRow = &pIter->row;
      }
    } else {
      ASSERT(pIter->pNode != pHead);

      if (pIter->pNode == pTail) {
        *ppRow = NULL;
      } else {
        tGetTSDBRow((uint8_t *)SL_NODE_DATA(pIter->pNode), &pIter->row);
        *ppRow = &pIter->row;
      }
    }
  }
}

static int32_t tsdbGetOrCreateMemData(SMemTable2 *pMemTable, tb_uid_t suid, tb_uid_t uid, SMemData **ppMemData) {
  int32_t    code = 0;
  int32_t    idx = 0;
  SMemData  *pMemDataT = &(SMemData){.suid = suid, .uid = uid};
  SMemData  *pMemData = NULL;
  SVBufPool *pPool = pMemTable->pTsdb->pVnode->inUse;
  int8_t     maxLevel = pMemTable->pTsdb->pVnode->config.tsdbCfg.slLevel;

  // get
  idx = taosArraySearchIdx(pMemTable->aMemData, &pMemDataT, memDataPCmprFn, TD_GE);
  if (idx >= 0) {
    pMemData = (SMemData *)taosArrayGet(pMemTable->aMemData, idx);
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
  if (taosArrayInsert(pMemTable->aMemData, idx, &pMemData) == NULL) {
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

static void memDataMovePosTo(SMemData *pMemData, SMemSkipListNode **pos, TSDBKEY *pKey, int32_t flags) {
  SMemSkipListNode *px;
  SMemSkipListNode *pn;
  TSDBKEY          *pTKey;
  int               c;
  int               backward = flags & SL_MOVE_BACKWARD;
  int               fromPos = flags & SL_MOVE_FROM_POS;

  if (backward) {
    px = pMemData->sl.pTail;

    for (int8_t iLevel = pMemData->sl.maxLevel - 1; iLevel >= pMemData->sl.level; iLevel--) {
      pos[iLevel] = px;
    }

    if (pMemData->sl.level) {
      if (fromPos) px = pos[pMemData->sl.level - 1];

      for (int8_t iLevel = pMemData->sl.level - 1; iLevel >= 0; iLevel--) {
        pn = SL_NODE_BACKWARD(px, iLevel);
        while (pn != pMemData->sl.pHead) {
          pTKey = (TSDBKEY *)SL_NODE_DATA(pn);

          c = tsdbKeyCmprFn(pTKey, pKey);
          if (c <= 0) {
            break;
          } else {
            px = pn;
            pn = SL_NODE_BACKWARD(px, iLevel);
          }
        }

        pos[iLevel] = px;
      }
    }
  } else {
    px = pMemData->sl.pHead;

    for (int8_t iLevel = pMemData->sl.maxLevel - 1; iLevel >= pMemData->sl.level; iLevel--) {
      pos[iLevel] = px;
    }

    if (pMemData->sl.level) {
      if (fromPos) px = pos[pMemData->sl.level - 1];

      for (int8_t iLevel = pMemData->sl.level - 1; iLevel >= 0; iLevel--) {
        pn = SL_NODE_FORWARD(px, iLevel);
        while (pn != pMemData->sl.pHead) {
          pTKey = (TSDBKEY *)SL_NODE_DATA(pn);

          c = tsdbKeyCmprFn(pTKey, pKey);
          if (c >= 0) {
            break;
          } else {
            px = pn;
            pn = SL_NODE_FORWARD(px, iLevel);
          }
        }

        pos[iLevel] = px;
      }
    }
  }
}

static int32_t memDataDoPut(SMemTable2 *pMemTable, SMemData *pMemData, SMemSkipListNode **pos, TSDBROW *pRow,
                            int8_t forward) {
  int32_t           code = 0;
  int8_t            level;
  SMemSkipListNode *pNode;
  SVBufPool        *pPool = pMemTable->pTsdb->pVnode->inUse;

  // node
  level = tsdbMemSkipListRandLevel(&pMemData->sl);
  pNode = (SMemSkipListNode *)vnodeBufPoolMalloc(pPool, SL_NODE_SIZE(level) + tPutTSDBRow(NULL, pRow));
  if (pNode == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  pNode->level = level;
  for (int8_t iLevel = 0; iLevel < level; iLevel++) {
    SL_NODE_FORWARD(pNode, iLevel) = NULL;
    SL_NODE_BACKWARD(pNode, iLevel) = NULL;
  }

  tPutTSDBRow((uint8_t *)SL_NODE_DATA(pNode), pRow);

  // put
  for (int8_t iLevel = 0; iLevel < pNode->level; iLevel++) {
    SMemSkipListNode *px = pos[iLevel];

    if (forward) {
      SMemSkipListNode *pNext = SL_NODE_FORWARD(px, iLevel);

      SL_NODE_FORWARD(pNode, iLevel) = pNext;
      SL_NODE_BACKWARD(pNode, iLevel) = px;

      SL_NODE_BACKWARD(pNext, iLevel) = pNode;
      SL_NODE_FORWARD(px, iLevel) = pNode;
    } else {
      SMemSkipListNode *pPrev = SL_NODE_BACKWARD(px, iLevel);

      SL_NODE_FORWARD(pNode, iLevel) = px;
      SL_NODE_BACKWARD(pNode, iLevel) = pPrev;

      SL_NODE_FORWARD(pPrev, iLevel) = pNode;
      SL_NODE_BACKWARD(px, iLevel) = pNode;
    }
  }

  pMemData->sl.size++;
  if (pMemData->sl.level < pNode->level) {
    pMemData->sl.level = pNode->level;
  }

_exit:
  return code;
}

static int32_t tsdbInsertTableDataImpl(SMemTable2 *pMemTable, SMemData *pMemData, int64_t version,
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

  memDataMovePosTo(pMemData, pos, &(TSDBKEY){.version = version, .ts = row.tsRow.ts}, SL_MOVE_BACKWARD);
  code = memDataDoPut(pMemTable, pMemData, pos, &row, 0);
  if (code) {
    goto _exit;
  }
  nRow++;

  if (tsdbKeyCmprFn((TSDBKEY *)&row, &pMemData->minKey) < 0) {
    pMemData->minKey = *(TSDBKEY *)&row;
  }
  if (tsdbKeyCmprFn((TSDBKEY *)&row, &pMemTable->minKey) < 0) {
    pMemTable->minKey = *(TSDBKEY *)&row;
  }

  // forward put rest
  for (int8_t iLevel = 0; iLevel < pMemData->sl.maxLevel; iLevel++) {
    pos[iLevel] = SL_NODE_BACKWARD(pos[iLevel], iLevel);
  }
  while (n < pSubmitBlk->nData) {
    n += tGetTSRow(p + n, &row.tsRow);
    ASSERT(n <= pSubmitBlk->nData);

    memDataMovePosTo(pMemData, pos, &(TSDBKEY){.version = version, .ts = row.tsRow.ts}, SL_MOVE_FROM_POS);
    code = memDataDoPut(pMemTable, pMemData, pos, &row, 1);
    if (code) {
      goto _exit;
    }

    nRow++;
  }

  if (tsdbKeyCmprFn((TSDBKEY *)&row, &pMemData->maxKey) > 0) {
    pMemData->maxKey = *(TSDBKEY *)&row;
  }
  if (tsdbKeyCmprFn((TSDBKEY *)&row, &pMemTable->maxKey) > 0) {
    pMemTable->maxKey = *(TSDBKEY *)&row;
  }
  pMemTable->nRows += nRow;

_exit:
  return code;
}
