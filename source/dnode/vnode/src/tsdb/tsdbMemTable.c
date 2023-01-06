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

#define MEM_MIN_HASH 1024
#define SL_MAX_LEVEL 5

// sizeof(SMemSkipListNode) + sizeof(SMemSkipListNode *) * (l) * 2
#define SL_NODE_SIZE(l)               (sizeof(SMemSkipListNode) + ((l) << 4))
#define SL_NODE_FORWARD(n, l)         ((n)->forwards[l])
#define SL_NODE_BACKWARD(n, l)        ((n)->forwards[(n)->level + (l)])
#define SL_GET_NODE_FORWARD(n, l)     ((SMemSkipListNode *)atomic_load_ptr(&SL_NODE_FORWARD(n, l)))
#define SL_GET_NODE_BACKWARD(n, l)    ((SMemSkipListNode *)atomic_load_ptr(&SL_NODE_BACKWARD(n, l)))
#define SL_SET_NODE_FORWARD(n, l, p)  atomic_store_ptr(&SL_NODE_FORWARD(n, l), p)
#define SL_SET_NODE_BACKWARD(n, l, p) atomic_store_ptr(&SL_NODE_BACKWARD(n, l), p)

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
  pMemTable->nRow = 0;
  pMemTable->nDel = 0;
  pMemTable->nTbData = 0;
  pMemTable->nBucket = MEM_MIN_HASH;
  pMemTable->aBucket = (STbData **)taosMemoryCalloc(pMemTable->nBucket, sizeof(STbData *));
  if (pMemTable->aBucket == NULL) {
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
    taosMemoryFree(pMemTable->aBucket);
    taosMemoryFree(pMemTable);
  }
}

static FORCE_INLINE STbData *tsdbGetTbDataFromMemTableImpl(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid) {
  STbData *pTbData = pMemTable->aBucket[TABS(uid) % pMemTable->nBucket];

  while (pTbData) {
    if (pTbData->uid == uid) break;
    pTbData = pTbData->next;
  }

  return pTbData;
}

STbData *tsdbGetTbDataFromMemTable(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid) {
  STbData *pTbData;

  taosRLockLatch(&pMemTable->latch);
  pTbData = tsdbGetTbDataFromMemTableImpl(pMemTable, suid, uid);
  taosRUnLockLatch(&pMemTable->latch);

  return pTbData;
}

int32_t tsdbInsertTableData(STsdb *pTsdb, int64_t version, SSubmitMsgIter *pMsgIter, SSubmitBlk *pBlock,
                            SSubmitBlkRsp *pRsp) {
  int32_t    code = 0;
  SMemTable *pMemTable = pTsdb->mem;
  STbData   *pTbData = NULL;
  tb_uid_t   suid = pMsgIter->suid;
  tb_uid_t   uid = pMsgIter->uid;

  SMetaInfo info;
  code = metaGetInfo(pTsdb->pVnode->pMeta, uid, &info, NULL);
  if (code) {
    code = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    goto _err;
  }
  if (info.suid != suid) {
    code = TSDB_CODE_INVALID_MSG;
    goto _err;
  }
  if (info.suid) {
    metaGetInfo(pTsdb->pVnode->pMeta, info.suid, &info, NULL);
  }
  if (pMsgIter->sversion != info.skmVer) {
    tsdbError("vgId:%d, req sver:%d, skmVer:%d suid:%" PRId64 " uid:%" PRId64, TD_VID(pTsdb->pVnode),
              pMsgIter->sversion, info.skmVer, suid, uid);
    code = TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
    goto _err;
  }

  pRsp->sver = info.skmVer;

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
  terrno = code;
  return code;
}

int32_t tsdbDeleteTableData(STsdb *pTsdb, int64_t version, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey) {
  int32_t    code = 0;
  SMemTable *pMemTable = pTsdb->mem;
  STbData   *pTbData = NULL;
  SVBufPool *pPool = pTsdb->pVnode->inUse;
  TSDBKEY    lastKey = {.version = version, .ts = eKey};

  // check if table exists
  SMetaInfo info;
  code = metaGetInfo(pTsdb->pVnode->pMeta, uid, &info, NULL);
  if (code) {
    code = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    goto _err;
  }
  if (info.suid != suid) {
    code = TSDB_CODE_INVALID_MSG;
    goto _err;
  }

  code = tsdbGetOrCreateTbData(pMemTable, suid, uid, &pTbData);
  if (code) {
    goto _err;
  }

  ASSERT(pPool != NULL);
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

  pMemTable->nDel++;

  if (TSDB_CACHE_LAST_ROW(pMemTable->pTsdb->pVnode->config) && tsdbKeyCmprFn(&lastKey, &pTbData->maxKey) >= 0) {
    tsdbCacheDeleteLastrow(pTsdb->lruCache, pTbData->uid, eKey);
  }

  if (TSDB_CACHE_LAST(pMemTable->pTsdb->pVnode->config)) {
    tsdbCacheDeleteLast(pTsdb->lruCache, pTbData->uid, eKey);
  }

  tsdbInfo("vgId:%d, delete data from table suid:%" PRId64 " uid:%" PRId64 " skey:%" PRId64 " eKey:%" PRId64
           " at version %" PRId64 " since %s",
           TD_VID(pTsdb->pVnode), suid, uid, sKey, eKey, version, tstrerror(code));
  return code;

_err:
  tsdbError("vgId:%d, failed to delete data from table suid:%" PRId64 " uid:%" PRId64 " skey:%" PRId64 " eKey:%" PRId64
            " at version %" PRId64 " since %s",
            TD_VID(pTsdb->pVnode), suid, uid, sKey, eKey, version, tstrerror(code));
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
      pIter->pNode = SL_GET_NODE_BACKWARD(pTbData->sl.pTail, 0);
    } else {
      pIter->pNode = SL_GET_NODE_FORWARD(pTbData->sl.pHead, 0);
    }
  } else {
    // create from a key
    if (backward) {
      tbDataMovePosTo(pTbData, pos, pFrom, SL_MOVE_BACKWARD);
      pIter->pNode = SL_GET_NODE_BACKWARD(pos[0], 0);
    } else {
      tbDataMovePosTo(pTbData, pos, pFrom, 0);
      pIter->pNode = SL_GET_NODE_FORWARD(pos[0], 0);
    }
  }
}

bool tsdbTbDataIterNext(STbDataIter *pIter) {
  pIter->pRow = NULL;
  if (pIter->backward) {
    ASSERT(pIter->pNode != pIter->pTbData->sl.pTail);

    if (pIter->pNode == pIter->pTbData->sl.pHead) {
      return false;
    }

    pIter->pNode = SL_GET_NODE_BACKWARD(pIter->pNode, 0);
    if (pIter->pNode == pIter->pTbData->sl.pHead) {
      return false;
    }
  } else {
    ASSERT(pIter->pNode != pIter->pTbData->sl.pHead);

    if (pIter->pNode == pIter->pTbData->sl.pTail) {
      return false;
    }

    pIter->pNode = SL_GET_NODE_FORWARD(pIter->pNode, 0);
    if (pIter->pNode == pIter->pTbData->sl.pTail) {
      return false;
    }
  }

  return true;
}

static int32_t tsdbMemTableRehash(SMemTable *pMemTable) {
  int32_t code = 0;

  int32_t   nBucket = pMemTable->nBucket * 2;
  STbData **aBucket = (STbData **)taosMemoryCalloc(nBucket, sizeof(STbData *));
  if (aBucket == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  for (int32_t iBucket = 0; iBucket < pMemTable->nBucket; iBucket++) {
    STbData *pTbData = pMemTable->aBucket[iBucket];

    while (pTbData) {
      STbData *pNext = pTbData->next;

      int32_t idx = TABS(pTbData->uid) % nBucket;
      pTbData->next = aBucket[idx];
      aBucket[idx] = pTbData;

      pTbData = pNext;
    }
  }

  taosMemoryFree(pMemTable->aBucket);
  pMemTable->nBucket = nBucket;
  pMemTable->aBucket = aBucket;

_exit:
  return code;
}

static int32_t tsdbGetOrCreateTbData(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid, STbData **ppTbData) {
  int32_t code = 0;

  // get
  STbData *pTbData = tsdbGetTbDataFromMemTableImpl(pMemTable, suid, uid);
  if (pTbData) goto _exit;

  // create
  SVBufPool *pPool = pMemTable->pTsdb->pVnode->inUse;
  int8_t     maxLevel = pMemTable->pTsdb->pVnode->config.tsdbCfg.slLevel;

  ASSERT(pPool != NULL);
  pTbData = vnodeBufPoolMallocAligned(pPool, sizeof(*pTbData) + SL_NODE_SIZE(maxLevel) * 2);
  if (pTbData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pTbData->suid = suid;
  pTbData->uid = uid;
  pTbData->minKey = TSKEY_MAX;
  pTbData->maxKey = TSKEY_MIN;
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

  taosWLockLatch(&pMemTable->latch);

  if (pMemTable->nTbData >= pMemTable->nBucket) {
    code = tsdbMemTableRehash(pMemTable);
    if (code) {
      taosWUnLockLatch(&pMemTable->latch);
      goto _err;
    }
  }

  int32_t idx = TABS(uid) % pMemTable->nBucket;
  pTbData->next = pMemTable->aBucket[idx];
  pMemTable->aBucket[idx] = pTbData;
  pMemTable->nTbData++;

  taosWUnLockLatch(&pMemTable->latch);

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
  TSDBKEY           tKey = {0};
  int32_t           backward = flags & SL_MOVE_BACKWARD;
  int32_t           fromPos = flags & SL_MOVE_FROM_POS;

  if (backward) {
    px = pTbData->sl.pTail;

    if (!fromPos) {
      for (int8_t iLevel = pTbData->sl.level; iLevel < pTbData->sl.maxLevel; iLevel++) {
        pos[iLevel] = px;
      }
    }

    if (pTbData->sl.level) {
      if (fromPos) px = pos[pTbData->sl.level - 1];

      for (int8_t iLevel = pTbData->sl.level - 1; iLevel >= 0; iLevel--) {
        pn = SL_GET_NODE_BACKWARD(px, iLevel);
        while (pn != pTbData->sl.pHead) {
          tKey.version = pn->version;
          tKey.ts = pn->pTSRow->ts;

          int32_t c = tsdbKeyCmprFn(&tKey, pKey);
          if (c <= 0) {
            break;
          } else {
            px = pn;
            pn = SL_GET_NODE_BACKWARD(px, iLevel);
          }
        }

        pos[iLevel] = px;
      }
    }
  } else {
    px = pTbData->sl.pHead;

    if (!fromPos) {
      for (int8_t iLevel = pTbData->sl.level; iLevel < pTbData->sl.maxLevel; iLevel++) {
        pos[iLevel] = px;
      }
    }

    if (pTbData->sl.level) {
      if (fromPos) px = pos[pTbData->sl.level - 1];

      for (int8_t iLevel = pTbData->sl.level - 1; iLevel >= 0; iLevel--) {
        pn = SL_GET_NODE_FORWARD(px, iLevel);
        while (pn != pTbData->sl.pTail) {
          tKey.version = pn->version;
          tKey.ts = pn->pTSRow->ts;

          int32_t c = tsdbKeyCmprFn(&tKey, pKey);
          if (c >= 0) {
            break;
          } else {
            px = pn;
            pn = SL_GET_NODE_FORWARD(px, iLevel);
          }
        }

        pos[iLevel] = px;
      }
    }
  }
}

static FORCE_INLINE int8_t tsdbMemSkipListRandLevel(SMemSkipList *pSl) {
  int8_t level = 1;
  int8_t tlevel = TMIN(pSl->maxLevel, pSl->level + 1);

  while ((taosRandR(&pSl->seed) & 0x3) == 0 && level < tlevel) {
    level++;
  }

  return level;
}
static int32_t tbDataDoPut(SMemTable *pMemTable, STbData *pTbData, SMemSkipListNode **pos, int64_t version,
                           STSRow *pRow, int8_t forward) {
  int32_t           code = 0;
  int8_t            level;
  SMemSkipListNode *pNode;
  SVBufPool        *pPool = pMemTable->pTsdb->pVnode->inUse;
  int64_t           nSize;

  // create node
  level = tsdbMemSkipListRandLevel(&pTbData->sl);
  nSize = SL_NODE_SIZE(level);
  pNode = (SMemSkipListNode *)vnodeBufPoolMallocAligned(pPool, nSize + pRow->len);
  if (pNode == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  pNode->level = level;
  pNode->version = version;
  pNode->pTSRow = (STSRow *)((char *)pNode + nSize);
  memcpy(pNode->pTSRow, pRow, pRow->len);

  // set node
  if (forward) {
    for (int8_t iLevel = 0; iLevel < level; iLevel++) {
      SL_NODE_FORWARD(pNode, iLevel) = SL_NODE_FORWARD(pos[iLevel], iLevel);
      SL_NODE_BACKWARD(pNode, iLevel) = pos[iLevel];
    }
  } else {
    for (int8_t iLevel = 0; iLevel < level; iLevel++) {
      SL_NODE_FORWARD(pNode, iLevel) = pos[iLevel];
      SL_NODE_BACKWARD(pNode, iLevel) = SL_NODE_BACKWARD(pos[iLevel], iLevel);
    }
  }

  // set forward and backward
  if (forward) {
    for (int8_t iLevel = level - 1; iLevel >= 0; iLevel--) {
      SMemSkipListNode *pNext = pos[iLevel]->forwards[iLevel];

      SL_SET_NODE_FORWARD(pos[iLevel], iLevel, pNode);
      SL_SET_NODE_BACKWARD(pNext, iLevel, pNode);

      pos[iLevel] = pNode;
    }
  } else {
    for (int8_t iLevel = level - 1; iLevel >= 0; iLevel--) {
      SMemSkipListNode *pPrev = pos[iLevel]->forwards[pos[iLevel]->level + iLevel];

      SL_SET_NODE_FORWARD(pPrev, iLevel, pNode);
      SL_SET_NODE_BACKWARD(pos[iLevel], iLevel, pNode);

      pos[iLevel] = pNode;
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
  if (row.pTSRow == NULL) return code;

  key.ts = row.pTSRow->ts;
  nRow++;
  tbDataMovePosTo(pTbData, pos, &key, SL_MOVE_BACKWARD);
  code = tbDataDoPut(pMemTable, pTbData, pos, version, row.pTSRow, 0);
  if (code) {
    goto _err;
  }

  pTbData->minKey = TMIN(pTbData->minKey, key.ts);

  pLastRow = row.pTSRow;

  // forward put rest data
  row.pTSRow = tGetSubmitBlkNext(&blkIter);
  if (row.pTSRow) {
    for (int8_t iLevel = pos[0]->level; iLevel < pTbData->sl.maxLevel; iLevel++) {
      pos[iLevel] = SL_NODE_BACKWARD(pos[iLevel], iLevel);
    }
    do {
      key.ts = row.pTSRow->ts;
      nRow++;
      if (SL_NODE_FORWARD(pos[0], 0) != pTbData->sl.pTail) {
        tbDataMovePosTo(pTbData, pos, &key, SL_MOVE_FROM_POS);
      }
      code = tbDataDoPut(pMemTable, pTbData, pos, version, row.pTSRow, 1);
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

  // SMemTable
  pMemTable->minKey = TMIN(pMemTable->minKey, pTbData->minKey);
  pMemTable->maxKey = TMAX(pMemTable->maxKey, pTbData->maxKey);
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

static FORCE_INLINE int32_t tbDataPCmprFn(const void *p1, const void *p2) {
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

SArray *tsdbMemTableGetTbDataArray(SMemTable *pMemTable) {
  SArray *aTbDataP = taosArrayInit(pMemTable->nTbData, sizeof(STbData *));
  if (aTbDataP == NULL) goto _exit;

  for (int32_t iBucket = 0; iBucket < pMemTable->nBucket; iBucket++) {
    STbData *pTbData = pMemTable->aBucket[iBucket];

    while (pTbData) {
      taosArrayPush(aTbDataP, &pTbData);
      pTbData = pTbData->next;
    }
  }

  taosArraySort(aTbDataP, tbDataPCmprFn);

_exit:
  return aTbDataP;
}
