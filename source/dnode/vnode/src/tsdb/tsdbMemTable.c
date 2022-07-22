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

#define SL_MAX_LEVEL 5

#define SL_NODE_SIZE(l)        (sizeof(SMemSkipListNode) + sizeof(SMemSkipListNode *) * (l)*2)
#define SL_NODE_FORWARD(n, l)  ((n)->forwards[l])
#define SL_NODE_BACKWARD(n, l) ((n)->forwards[(n)->level + (l)])
#define SL_NODE_DATA(n)        (&SL_NODE_BACKWARD(n, (n)->level))

#define SL_MOVE_BACKWARD 0x1
#define SL_MOVE_FROM_POS 0x2

static void    tbDataMovePosTo(STbData *pTbData, SMemSkipListNode **pos, TSDBKEY *pKey, int32_t flags);
static int32_t tsdbGetOrCreateTbData(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid, STbData **ppTbData);
static int32_t tsdbInsertTableDataImpl(SMemTable *pMemTable, STbData *pTbData, int64_t version,
                                       SSubmitMsgIter *pMsgIter, SSubmitBlk *pBlock, SSubmitBlkRsp *pRsp);

int32_t tsdbMemTableCreate(STsdb *pTsdb, SMemTable **ppMemTable) {
  int32_t    code = 0;
  SMemTable *pMemTable = NULL;

  pMemTable = (SMemTable *)taosMemoryCalloc(1, sizeof(*pMemTable));
  if (pMemTable == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  taosInitRWLatch(&pMemTable->latch);
  pMemTable->pTsdb = pTsdb;
  pMemTable->pPool = pTsdb->pVnode->inUse;
  pMemTable->nRef = 1;
  pMemTable->minKey = TSKEY_MAX;
  pMemTable->maxKey = TSKEY_MIN;
  pMemTable->minVersion = VERSION_MAX;
  pMemTable->maxVersion = VERSION_MIN;
  pMemTable->nRow = 0;
  pMemTable->nDel = 0;
  pMemTable->aTbData = taosArrayInit(128, sizeof(STbData *));
  if (pMemTable->aTbData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pMemTable);
    goto _err;
  }
  vnodeBufPoolRef(pMemTable->pPool);

  *ppMemTable = pMemTable;
  return code;

_err:
  *ppMemTable = NULL;
  return code;
}

void tsdbMemTableDestroy(SMemTable *pMemTable) {
  if (pMemTable) {
    vnodeBufPoolUnRef(pMemTable->pPool);
    taosArrayDestroy(pMemTable->aTbData);
    taosMemoryFree(pMemTable);
  }
}

static int32_t tbDataPCmprFn(const void *p1, const void *p2) {
  STbData *pTbData1 = *(STbData **)p1;
  STbData *pTbData2 = *(STbData **)p2;

  if (pTbData1->suid < pTbData2->suid) {
    return -1;
  } else if (pTbData1->suid > pTbData2->suid) {
    return 1;
  }

  if (pTbData1->uid < pTbData2->uid) {
    return -1;
  } else if (pTbData1->uid > pTbData2->uid) {
    return 1;
  }

  return 0;
}
void tsdbGetTbDataFromMemTable(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid, STbData **ppTbData) {
  STbData *pTbData = &(STbData){.suid = suid, .uid = uid};

  taosRLockLatch(&pMemTable->latch);
  void *p = taosArraySearch(pMemTable->aTbData, &pTbData, tbDataPCmprFn, TD_EQ);
  taosRUnLockLatch(&pMemTable->latch);

  *ppTbData = p ? *(STbData **)p : NULL;
}

int32_t tsdbInsertTableData(STsdb *pTsdb, int64_t version, SSubmitMsgIter *pMsgIter, SSubmitBlk *pBlock,
                            SSubmitBlkRsp *pRsp) {
  int32_t    code = 0;
  SMemTable *pMemTable = pTsdb->mem;
  STbData   *pTbData = NULL;
  tb_uid_t   suid = pMsgIter->suid;
  tb_uid_t   uid = pMsgIter->uid;
  int32_t    sverNew;

  // check if table exists (todo: refact)
  SMetaReader mr = {0};
  // SMetaEntry  me = {0};
  metaReaderInit(&mr, pTsdb->pVnode->pMeta, 0);
  if (metaGetTableEntryByUid(&mr, pMsgIter->uid) < 0) {
    metaReaderClear(&mr);
    code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    goto _err;
  }
  if (pRsp->tblFName) strcat(pRsp->tblFName, mr.me.name);

  if (mr.me.type == TSDB_NORMAL_TABLE) {
    sverNew = mr.me.ntbEntry.schemaRow.version;
  } else {
    tDecoderClear(&mr.coder);

    metaGetTableEntryByUid(&mr, mr.me.ctbEntry.suid);
    sverNew = mr.me.stbEntry.schemaRow.version;
  }
  metaReaderClear(&mr);
  pRsp->sver = sverNew;

  // create/get STbData to op
  code = tsdbGetOrCreateTbData(pMemTable, suid, uid, &pTbData);
  if (code) {
    goto _err;
  }

  // do insert impl
  code = tsdbInsertTableDataImpl(pMemTable, pTbData, version, pMsgIter, pBlock, pRsp);
  if (code) {
    goto _err;
  }

  return code;

_err:
  return code;
}

int32_t tsdbDeleteTableData(STsdb *pTsdb, int64_t version, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey) {
  int32_t    code = 0;
  SMemTable *pMemTable = pTsdb->mem;
  STbData   *pTbData = NULL;
  SVBufPool *pPool = pTsdb->pVnode->inUse;
  TSDBKEY    lastKey = {.version = version, .ts = eKey};

  // check if table exists (todo)

  code = tsdbGetOrCreateTbData(pMemTable, suid, uid, &pTbData);
  if (code) {
    goto _err;
  }

  // do delete
  SDelData *pDelData = (SDelData *)vnodeBufPoolMalloc(pPool, sizeof(*pDelData));
  if (pDelData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pDelData->version = version;
  pDelData->sKey = sKey;
  pDelData->eKey = eKey;
  pDelData->pNext = NULL;
  if (pTbData->pHead == NULL) {
    ASSERT(pTbData->pTail == NULL);
    pTbData->pHead = pTbData->pTail = pDelData;
  } else {
    pTbData->pTail->pNext = pDelData;
    pTbData->pTail = pDelData;
  }

  // update the state of pMemTable and other (todo)

  pMemTable->minVersion = TMIN(pMemTable->minVersion, version);
  pMemTable->maxVersion = TMAX(pMemTable->maxVersion, version);
  pMemTable->nDel++;

  if (TSDB_CACHE_LAST_ROW(pMemTable->pTsdb->pVnode->config) && tsdbKeyCmprFn(&lastKey, &pTbData->maxKey) >= 0) {
    tsdbCacheDeleteLastrow(pTsdb->lruCache, pTbData->uid, eKey);
  }

  if (TSDB_CACHE_LAST(pMemTable->pTsdb->pVnode->config)) {
    tsdbCacheDeleteLast(pTsdb->lruCache, pTbData->uid, eKey);
  }

  tsdbError("vgId:%d, delete data from table suid:%" PRId64 " uid:%" PRId64 " skey:%" PRId64 " eKey:%" PRId64
            " since %s",
            TD_VID(pTsdb->pVnode), suid, uid, sKey, eKey, tstrerror(code));
  return code;

_err:
  tsdbError("vgId:%d, failed to delete data from table suid:%" PRId64 " uid:%" PRId64 " skey:%" PRId64 " eKey:%" PRId64
            " since %s",
            TD_VID(pTsdb->pVnode), suid, uid, sKey, eKey, tstrerror(code));
  return code;
}

int32_t tsdbTbDataIterCreate(STbData *pTbData, TSDBKEY *pFrom, int8_t backward, STbDataIter **ppIter) {
  int32_t code = 0;

  (*ppIter) = (STbDataIter *)taosMemoryCalloc(1, sizeof(STbDataIter));
  if ((*ppIter) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  tsdbTbDataIterOpen(pTbData, pFrom, backward, *ppIter);

_exit:
  return code;
}

void *tsdbTbDataIterDestroy(STbDataIter *pIter) {
  if (pIter) {
    taosMemoryFree(pIter);
  }

  return NULL;
}

void tsdbTbDataIterOpen(STbData *pTbData, TSDBKEY *pFrom, int8_t backward, STbDataIter *pIter) {
  SMemSkipListNode *pos[SL_MAX_LEVEL];
  SMemSkipListNode *pHead;
  SMemSkipListNode *pTail;

  pHead = pTbData->sl.pHead;
  pTail = pTbData->sl.pTail;
  pIter->pTbData = pTbData;
  pIter->backward = backward;
  pIter->pRow = NULL;
  pIter->row.type = 0;
  if (pFrom == NULL) {
    // create from head or tail
    if (backward) {
      pIter->pNode = SL_NODE_BACKWARD(pTbData->sl.pTail, 0);
    } else {
      pIter->pNode = SL_NODE_FORWARD(pTbData->sl.pHead, 0);
    }
  } else {
    // create from a key
    if (backward) {
      tbDataMovePosTo(pTbData, pos, pFrom, SL_MOVE_BACKWARD);
      pIter->pNode = SL_NODE_BACKWARD(pos[0], 0);
    } else {
      tbDataMovePosTo(pTbData, pos, pFrom, 0);
      pIter->pNode = SL_NODE_FORWARD(pos[0], 0);
    }
  }
}

bool tsdbTbDataIterNext(STbDataIter *pIter) {
  SMemSkipListNode *pHead = pIter->pTbData->sl.pHead;
  SMemSkipListNode *pTail = pIter->pTbData->sl.pTail;

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

TSDBROW *tsdbTbDataIterGet(STbDataIter *pIter) {
  // we add here for commit usage
  if (pIter == NULL) return NULL;

  if (pIter->pRow) {
    goto _exit;
  }

  if (pIter->backward) {
    if (pIter->pNode == pIter->pTbData->sl.pHead) {
      goto _exit;
    }
  } else {
    if (pIter->pNode == pIter->pTbData->sl.pTail) {
      goto _exit;
    }
  }

  tGetTSDBRow((uint8_t *)SL_NODE_DATA(pIter->pNode), &pIter->row);
  pIter->pRow = &pIter->row;

_exit:
  return pIter->pRow;
}

static int32_t tsdbGetOrCreateTbData(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid, STbData **ppTbData) {
  int32_t  code = 0;
  int32_t  idx = 0;
  STbData *pTbData = NULL;
  STbData *pTbDataT = &(STbData){.suid = suid, .uid = uid};

  // get
  idx = taosArraySearchIdx(pMemTable->aTbData, &pTbDataT, tbDataPCmprFn, TD_GE);
  if (idx >= 0) {
    pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, idx);
    if (tbDataPCmprFn(&pTbDataT, &pTbData) == 0) goto _exit;
  }

  // create
  SVBufPool *pPool = pMemTable->pTsdb->pVnode->inUse;
  int8_t     maxLevel = pMemTable->pTsdb->pVnode->config.tsdbCfg.slLevel;

  pTbData = vnodeBufPoolMalloc(pPool, sizeof(*pTbData) + SL_NODE_SIZE(maxLevel) * 2);
  if (pTbData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pTbData->suid = suid;
  pTbData->uid = uid;
  pTbData->minKey = TSKEY_MAX;
  pTbData->maxKey = TSKEY_MIN;
  pTbData->minVersion = VERSION_MAX;
  pTbData->maxVersion = VERSION_MIN;
  pTbData->maxSkmVer = -1;
  pTbData->pHead = NULL;
  pTbData->pTail = NULL;
  pTbData->sl.seed = taosRand();
  pTbData->sl.size = 0;
  pTbData->sl.maxLevel = maxLevel;
  pTbData->sl.level = 0;
  pTbData->sl.pHead = (SMemSkipListNode *)&pTbData[1];
  pTbData->sl.pTail = (SMemSkipListNode *)POINTER_SHIFT(pTbData->sl.pHead, SL_NODE_SIZE(maxLevel));
  pTbData->sl.pHead->level = maxLevel;
  pTbData->sl.pTail->level = maxLevel;
  for (int8_t iLevel = 0; iLevel < maxLevel; iLevel++) {
    SL_NODE_FORWARD(pTbData->sl.pHead, iLevel) = pTbData->sl.pTail;
    SL_NODE_BACKWARD(pTbData->sl.pTail, iLevel) = pTbData->sl.pHead;

    SL_NODE_BACKWARD(pTbData->sl.pHead, iLevel) = NULL;
    SL_NODE_FORWARD(pTbData->sl.pTail, iLevel) = NULL;
  }

  void *p;
  if (idx < 0) {
    idx = taosArrayGetSize(pMemTable->aTbData);
  }

  taosWLockLatch(&pMemTable->latch);
  p = taosArrayInsert(pMemTable->aTbData, idx, &pTbData);
  taosWUnLockLatch(&pMemTable->latch);

  if (p == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

_exit:
  *ppTbData = pTbData;
  return code;

_err:
  *ppTbData = NULL;
  return code;
}

static void tbDataMovePosTo(STbData *pTbData, SMemSkipListNode **pos, TSDBKEY *pKey, int32_t flags) {
  SMemSkipListNode *px;
  SMemSkipListNode *pn;
  TSDBKEY          *pTKey;
  int               c;
  int               backward = flags & SL_MOVE_BACKWARD;
  int               fromPos = flags & SL_MOVE_FROM_POS;

  if (backward) {
    px = pTbData->sl.pTail;

    for (int8_t iLevel = pTbData->sl.maxLevel - 1; iLevel >= pTbData->sl.level; iLevel--) {
      pos[iLevel] = px;
    }

    if (pTbData->sl.level) {
      if (fromPos) px = pos[pTbData->sl.level - 1];

      for (int8_t iLevel = pTbData->sl.level - 1; iLevel >= 0; iLevel--) {
        pn = SL_NODE_BACKWARD(px, iLevel);
        while (pn != pTbData->sl.pHead) {
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
    px = pTbData->sl.pHead;

    for (int8_t iLevel = pTbData->sl.maxLevel - 1; iLevel >= pTbData->sl.level; iLevel--) {
      pos[iLevel] = px;
    }

    if (pTbData->sl.level) {
      if (fromPos) px = pos[pTbData->sl.level - 1];

      for (int8_t iLevel = pTbData->sl.level - 1; iLevel >= 0; iLevel--) {
        pn = SL_NODE_FORWARD(px, iLevel);
        while (pn != pTbData->sl.pTail) {
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

static FORCE_INLINE int8_t tsdbMemSkipListRandLevel(SMemSkipList *pSl) {
  int8_t         level = 1;
  int8_t         tlevel = TMIN(pSl->maxLevel, pSl->level + 1);
  const uint32_t factor = 4;

  while ((taosRandR(&pSl->seed) % factor) == 0 && level < tlevel) {
    level++;
  }

  return level;
}
static int32_t tbDataDoPut(SMemTable *pMemTable, STbData *pTbData, SMemSkipListNode **pos, TSDBROW *pRow,
                           int8_t forward) {
  int32_t           code = 0;
  int8_t            level;
  SMemSkipListNode *pNode;
  SVBufPool        *pPool = pMemTable->pTsdb->pVnode->inUse;

  // node
  level = tsdbMemSkipListRandLevel(&pTbData->sl);
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

  pTbData->sl.size++;
  if (pTbData->sl.level < pNode->level) {
    pTbData->sl.level = pNode->level;
  }

_exit:
  return code;
}

static int32_t tsdbInsertTableDataImpl(SMemTable *pMemTable, STbData *pTbData, int64_t version,
                                       SSubmitMsgIter *pMsgIter, SSubmitBlk *pBlock, SSubmitBlkRsp *pRsp) {
  int32_t           code = 0;
  SSubmitBlkIter    blkIter = {0};
  TSDBKEY           key = {.version = version};
  SMemSkipListNode *pos[SL_MAX_LEVEL];
  TSDBROW           row = tsdbRowFromTSRow(version, NULL);
  int32_t           nRow = 0;
  STSRow           *pLastRow = NULL;

  tInitSubmitBlkIter(pMsgIter, pBlock, &blkIter);

  // backward put first data
  row.pTSRow = tGetSubmitBlkNext(&blkIter);
  key.ts = row.pTSRow->ts;
  nRow++;
  tbDataMovePosTo(pTbData, pos, &key, SL_MOVE_BACKWARD);
  code = tbDataDoPut(pMemTable, pTbData, pos, &row, 0);
  if (code) {
    goto _err;
  }

  pTbData->minKey = TMIN(pTbData->minKey, key.ts);

  pLastRow = row.pTSRow;

  // forward put rest data
  row.pTSRow = tGetSubmitBlkNext(&blkIter);
  if (row.pTSRow) {
    for (int8_t iLevel = 0; iLevel < pTbData->sl.maxLevel; iLevel++) {
      pos[iLevel] = SL_NODE_BACKWARD(pos[iLevel], iLevel);
    }
    do {
      key.ts = row.pTSRow->ts;
      nRow++;
      tbDataMovePosTo(pTbData, pos, &key, SL_MOVE_FROM_POS);
      code = tbDataDoPut(pMemTable, pTbData, pos, &row, 1);
      if (code) {
        goto _err;
      }

      pLastRow = row.pTSRow;

      row.pTSRow = tGetSubmitBlkNext(&blkIter);
    } while (row.pTSRow);
  }

  if (key.ts >= pTbData->maxKey) {
    if (key.ts > pTbData->maxKey) {
      pTbData->maxKey = key.ts;
    }

    if (TSDB_CACHE_LAST_ROW(pMemTable->pTsdb->pVnode->config) && pLastRow != NULL) {
      tsdbCacheInsertLastrow(pMemTable->pTsdb->lruCache, pMemTable->pTsdb, pTbData->uid, pLastRow, true);
    }
  }

  if (TSDB_CACHE_LAST(pMemTable->pTsdb->pVnode->config)) {
    tsdbCacheInsertLast(pMemTable->pTsdb->lruCache, pTbData->uid, pLastRow, pMemTable->pTsdb);
  }

  pTbData->minVersion = TMIN(pTbData->minVersion, version);
  pTbData->maxVersion = TMAX(pTbData->maxVersion, version);
  pTbData->maxSkmVer = TMAX(pTbData->maxSkmVer, pMsgIter->sversion);

  // SMemTable
  pMemTable->minKey = TMIN(pMemTable->minKey, pTbData->minKey);
  pMemTable->maxKey = TMAX(pMemTable->maxKey, pTbData->maxKey);
  pMemTable->minVersion = TMIN(pMemTable->minVersion, pTbData->minVersion);
  pMemTable->maxVersion = TMAX(pMemTable->maxVersion, pTbData->maxVersion);
  pMemTable->nRow += nRow;

  pRsp->numOfRows = nRow;
  pRsp->affectedRows = nRow;

  return code;

_err:
  return code;
}

int32_t tsdbGetNRowsInTbData(STbData *pTbData) { return pTbData->sl.size; }

void tsdbRefMemTable(SMemTable *pMemTable) {
  int32_t nRef = atomic_fetch_add_32(&pMemTable->nRef, 1);
  ASSERT(nRef > 0);
}

void tsdbUnrefMemTable(SMemTable *pMemTable) {
  int32_t nRef = atomic_sub_fetch_32(&pMemTable->nRef, 1);
  if (nRef == 0) {
    tsdbMemTableDestroy(pMemTable);
  }
}
