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
#include "util/tsimplehash.h"

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
static int32_t tsdbInsertRowDataToTable(SMemTable *pMemTable, STbData *pTbData, int64_t version,
                                        SSubmitTbData *pSubmitTbData, int32_t *affectedRows);
static int32_t tsdbInsertColDataToTable(SMemTable *pMemTable, STbData *pTbData, int64_t version,
                                        SSubmitTbData *pSubmitTbData, int32_t *affectedRows);

static int32_t tTbDataCmprFn(const SRBTreeNode *n1, const SRBTreeNode *n2) {
  STbData *tbData1 = TCONTAINER_OF(n1, STbData, rbtn);
  STbData *tbData2 = TCONTAINER_OF(n2, STbData, rbtn);
  if (tbData1->suid < tbData2->suid) return -1;
  if (tbData1->suid > tbData2->suid) return 1;
  if (tbData1->uid < tbData2->uid) return -1;
  if (tbData1->uid > tbData2->uid) return 1;
  return 0;
}

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
  pMemTable->minVer = VERSION_MAX;
  pMemTable->maxVer = VERSION_MIN;
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
  tRBTreeCreate(pMemTable->tbDataTree, tTbDataCmprFn);

  *ppMemTable = pMemTable;
  return code;

_err:
  *ppMemTable = NULL;
  return code;
}

void tsdbMemTableDestroy(SMemTable *pMemTable, bool proactive) {
  if (pMemTable) {
    vnodeBufPoolUnRef(pMemTable->pPool, proactive);
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

int32_t tsdbInsertTableData(STsdb *pTsdb, int64_t version, SSubmitTbData *pSubmitTbData, int32_t *affectedRows) {
  int32_t    code = 0;
  SMemTable *pMemTable = pTsdb->mem;
  STbData   *pTbData = NULL;
  tb_uid_t   suid = pSubmitTbData->suid;
  tb_uid_t   uid = pSubmitTbData->uid;

  // create/get STbData to op
  code = tsdbGetOrCreateTbData(pMemTable, suid, uid, &pTbData);
  if (code) {
    goto _err;
  }

  // do insert impl
  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    code = tsdbInsertColDataToTable(pMemTable, pTbData, version, pSubmitTbData, affectedRows);
  } else {
    code = tsdbInsertRowDataToTable(pMemTable, pTbData, version, pSubmitTbData, affectedRows);
  }
  if (code) goto _err;

  // update
  pMemTable->minVer = TMIN(pMemTable->minVer, version);
  pMemTable->maxVer = TMAX(pMemTable->maxVer, version);

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
  taosWLockLatch(&pTbData->lock);
  if (pTbData->pHead == NULL) {
    ASSERT(pTbData->pTail == NULL);
    pTbData->pHead = pTbData->pTail = pDelData;
  } else {
    pTbData->pTail->pNext = pDelData;
    pTbData->pTail = pDelData;
  }
  taosWUnLockLatch(&pTbData->lock);

  pMemTable->nDel++;
  pMemTable->minVer = TMIN(pMemTable->minVer, version);
  pMemTable->maxVer = TMAX(pMemTable->maxVer, version);
  /*
  if (TSDB_CACHE_LAST_ROW(pMemTable->pTsdb->pVnode->config) && tsdbKeyCmprFn(&lastKey, &pTbData->maxKey) >= 0) {
    tsdbCacheDeleteLastrow(pTsdb->lruCache, pTbData->uid, eKey);
  }

  if (TSDB_CACHE_LAST(pMemTable->pTsdb->pVnode->config)) {
    tsdbCacheDeleteLast(pTsdb->lruCache, pTbData->uid, eKey);
  }
  */
  // if (eKey >= pTbData->maxKey && sKey <= pTbData->maxKey) {
  tsdbCacheDel(pTsdb, suid, uid, sKey, eKey);
  //}

  tsdbTrace("vgId:%d, delete data from table suid:%" PRId64 " uid:%" PRId64 " skey:%" PRId64 " eKey:%" PRId64
            " at version %" PRId64,
            TD_VID(pTsdb->pVnode), suid, uid, sKey, eKey, version);
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

int64_t tsdbCountTbDataRows(STbData *pTbData) {
  SMemSkipListNode *pNode = pTbData->sl.pHead;
  int64_t           rowsNum = 0;

  while (NULL != pNode) {
    pNode = SL_GET_NODE_FORWARD(pNode, 0);
    if (pNode == pTbData->sl.pTail) {
      return rowsNum;
    }

    rowsNum++;
  }

  return rowsNum;
}

void tsdbMemTableCountRows(SMemTable *pMemTable, SSHashObj *pTableMap, int64_t *rowsNum) {
  taosRLockLatch(&pMemTable->latch);
  for (int32_t i = 0; i < pMemTable->nBucket; ++i) {
    STbData *pTbData = pMemTable->aBucket[i];
    while (pTbData) {
      void *p = tSimpleHashGet(pTableMap, &pTbData->uid, sizeof(pTbData->uid));
      if (p == NULL) {
        pTbData = pTbData->next;
        continue;
      }

      *rowsNum += tsdbCountTbDataRows(pTbData);
      pTbData = pTbData->next;
    }
  }
  taosRUnLockLatch(&pMemTable->latch);
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
  taosInitRWLatch(&pTbData->lock);

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

  tRBTreePut(pMemTable->tbDataTree, pTbData->rbtn);

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
          if (pn->flag == TSDBROW_ROW_FMT) {
            tKey.version = pn->version;
            tKey.ts = ((SRow *)pn->pData)->ts;
          } else if (pn->flag == TSDBROW_COL_FMT) {
            tKey.version = ((SBlockData *)pn->pData)->aVersion[pn->iRow];
            tKey.ts = ((SBlockData *)pn->pData)->aTSKEY[pn->iRow];
          }

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
          if (pn->flag == TSDBROW_ROW_FMT) {
            tKey.version = pn->version;
            tKey.ts = ((SRow *)pn->pData)->ts;
          } else if (pn->flag == TSDBROW_COL_FMT) {
            tKey.version = ((SBlockData *)pn->pData)->aVersion[pn->iRow];
            tKey.ts = ((SBlockData *)pn->pData)->aTSKEY[pn->iRow];
          }

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
static int32_t tbDataDoPut(SMemTable *pMemTable, STbData *pTbData, SMemSkipListNode **pos, TSDBROW *pRow,
                           int8_t forward) {
  int32_t           code = 0;
  int8_t            level;
  SMemSkipListNode *pNode = NULL;
  SVBufPool        *pPool = pMemTable->pTsdb->pVnode->inUse;
  int64_t           nSize;

  // create node
  level = tsdbMemSkipListRandLevel(&pTbData->sl);
  nSize = SL_NODE_SIZE(level);
  if (pRow->type == TSDBROW_ROW_FMT) {
    pNode = (SMemSkipListNode *)vnodeBufPoolMallocAligned(pPool, nSize + pRow->pTSRow->len);
  } else if (pRow->type == TSDBROW_COL_FMT) {
    pNode = (SMemSkipListNode *)vnodeBufPoolMallocAligned(pPool, nSize);
  } else {
    ASSERT(0);
  }
  if (pNode == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  pNode->level = level;
  pNode->flag = pRow->type;
  if (pRow->type == TSDBROW_ROW_FMT) {
    pNode->version = pRow->version;
    pNode->pData = (char *)pNode + nSize;
    memcpy(pNode->pData, pRow->pTSRow, pRow->pTSRow->len);
  } else if (pRow->type == TSDBROW_COL_FMT) {
    pNode->iRow = pRow->iRow;
    pNode->pData = pRow->pBlockData;
  } else {
    ASSERT(0);
  }

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

static int32_t tsdbInsertColDataToTable(SMemTable *pMemTable, STbData *pTbData, int64_t version,
                                        SSubmitTbData *pSubmitTbData, int32_t *affectedRows) {
  int32_t code = 0;

  SVBufPool *pPool = pMemTable->pTsdb->pVnode->inUse;
  int32_t    nColData = TARRAY_SIZE(pSubmitTbData->aCol);
  SColData  *aColData = (SColData *)TARRAY_DATA(pSubmitTbData->aCol);

  ASSERT(aColData[0].cid == PRIMARYKEY_TIMESTAMP_COL_ID);
  ASSERT(aColData[0].type == TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT(aColData[0].flag == HAS_VALUE);

  // copy and construct block data
  SBlockData *pBlockData = vnodeBufPoolMalloc(pPool, sizeof(*pBlockData));
  if (pBlockData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  pBlockData->suid = pTbData->suid;
  pBlockData->uid = pTbData->uid;
  pBlockData->nRow = aColData[0].nVal;
  pBlockData->aUid = NULL;
  pBlockData->aVersion = vnodeBufPoolMalloc(pPool, aColData[0].nData);
  if (pBlockData->aVersion == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  for (int32_t i = 0; i < pBlockData->nRow; i++) {  // todo: here can be optimized
    pBlockData->aVersion[i] = version;
  }

  pBlockData->aTSKEY = vnodeBufPoolMalloc(pPool, aColData[0].nData);
  if (pBlockData->aTSKEY == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  memcpy(pBlockData->aTSKEY, aColData[0].pData, aColData[0].nData);

  pBlockData->nColData = nColData - 1;
  pBlockData->aColData = vnodeBufPoolMalloc(pPool, sizeof(SColData) * pBlockData->nColData);
  if (pBlockData->aColData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  for (int32_t iColData = 0; iColData < pBlockData->nColData; ++iColData) {
    code = tColDataCopy(&aColData[iColData + 1], &pBlockData->aColData[iColData], (xMallocFn)vnodeBufPoolMalloc, pPool);
    if (code) goto _exit;
  }

  // loop to add each row to the skiplist
  SMemSkipListNode *pos[SL_MAX_LEVEL];
  TSDBROW           tRow = tsdbRowFromBlockData(pBlockData, 0);
  TSDBKEY           key = {.version = version, .ts = pBlockData->aTSKEY[0]};
  TSDBROW           lRow;  // last row

  // first row
  tbDataMovePosTo(pTbData, pos, &key, SL_MOVE_BACKWARD);
  if ((code = tbDataDoPut(pMemTable, pTbData, pos, &tRow, 0))) goto _exit;
  pTbData->minKey = TMIN(pTbData->minKey, key.ts);
  lRow = tRow;

  // remain row
  ++tRow.iRow;
  if (tRow.iRow < pBlockData->nRow) {
    for (int8_t iLevel = pos[0]->level; iLevel < pTbData->sl.maxLevel; iLevel++) {
      pos[iLevel] = SL_NODE_BACKWARD(pos[iLevel], iLevel);
    }

    while (tRow.iRow < pBlockData->nRow) {
      key.ts = pBlockData->aTSKEY[tRow.iRow];

      if (SL_NODE_FORWARD(pos[0], 0) != pTbData->sl.pTail) {
        tbDataMovePosTo(pTbData, pos, &key, SL_MOVE_FROM_POS);
      }

      if ((code = tbDataDoPut(pMemTable, pTbData, pos, &tRow, 1))) goto _exit;
      lRow = tRow;

      ++tRow.iRow;
    }
  }

  if (key.ts >= pTbData->maxKey) {
    pTbData->maxKey = key.ts;
  }

  if (!TSDB_CACHE_NO(pMemTable->pTsdb->pVnode->config)) {
    tsdbCacheUpdate(pMemTable->pTsdb, pTbData->suid, pTbData->uid, &lRow);
  }

  // SMemTable
  pMemTable->minKey = TMIN(pMemTable->minKey, pTbData->minKey);
  pMemTable->maxKey = TMAX(pMemTable->maxKey, pTbData->maxKey);
  pMemTable->nRow += pBlockData->nRow;

  if (affectedRows) *affectedRows = pBlockData->nRow;

_exit:
  return code;
}

static int32_t tsdbInsertRowDataToTable(SMemTable *pMemTable, STbData *pTbData, int64_t version,
                                        SSubmitTbData *pSubmitTbData, int32_t *affectedRows) {
  int32_t code = 0;

  int32_t           nRow = TARRAY_SIZE(pSubmitTbData->aRowP);
  SRow            **aRow = (SRow **)TARRAY_DATA(pSubmitTbData->aRowP);
  TSDBKEY           key = {.version = version};
  SMemSkipListNode *pos[SL_MAX_LEVEL];
  TSDBROW           tRow = {.type = TSDBROW_ROW_FMT, .version = version};
  int32_t           iRow = 0;
  TSDBROW           lRow;

  // backward put first data
  tRow.pTSRow = aRow[iRow++];
  key.ts = tRow.pTSRow->ts;
  tbDataMovePosTo(pTbData, pos, &key, SL_MOVE_BACKWARD);
  code = tbDataDoPut(pMemTable, pTbData, pos, &tRow, 0);
  if (code) goto _exit;
  lRow = tRow;

  pTbData->minKey = TMIN(pTbData->minKey, key.ts);

  // forward put rest data
  if (iRow < nRow) {
    for (int8_t iLevel = pos[0]->level; iLevel < pTbData->sl.maxLevel; iLevel++) {
      pos[iLevel] = SL_NODE_BACKWARD(pos[iLevel], iLevel);
    }

    while (iRow < nRow) {
      tRow.pTSRow = aRow[iRow];
      key.ts = tRow.pTSRow->ts;

      if (SL_NODE_FORWARD(pos[0], 0) != pTbData->sl.pTail) {
        tbDataMovePosTo(pTbData, pos, &key, SL_MOVE_FROM_POS);
      }

      code = tbDataDoPut(pMemTable, pTbData, pos, &tRow, 1);
      if (code) goto _exit;

      lRow = tRow;

      iRow++;
    }
  }

  if (key.ts >= pTbData->maxKey) {
    pTbData->maxKey = key.ts;
  }
  if (!TSDB_CACHE_NO(pMemTable->pTsdb->pVnode->config)) {
    tsdbCacheUpdate(pMemTable->pTsdb, pTbData->suid, pTbData->uid, &lRow);
  }

  // SMemTable
  pMemTable->minKey = TMIN(pMemTable->minKey, pTbData->minKey);
  pMemTable->maxKey = TMAX(pMemTable->maxKey, pTbData->maxKey);
  pMemTable->nRow += nRow;

  if (affectedRows) *affectedRows = nRow;

_exit:
  return code;
}

int32_t tsdbGetNRowsInTbData(STbData *pTbData) { return pTbData->sl.size; }

int32_t tsdbRefMemTable(SMemTable *pMemTable, SQueryNode *pQNode) {
  int32_t code = 0;

  int32_t nRef = atomic_fetch_add_32(&pMemTable->nRef, 1);
  ASSERT(nRef > 0);

  vnodeBufPoolRegisterQuery(pMemTable->pPool, pQNode);

_exit:
  return code;
}

int32_t tsdbUnrefMemTable(SMemTable *pMemTable, SQueryNode *pNode, bool proactive) {
  int32_t code = 0;

  if (pNode) {
    vnodeBufPoolDeregisterQuery(pMemTable->pPool, pNode, proactive);
  }

  if (atomic_sub_fetch_32(&pMemTable->nRef, 1) == 0) {
    tsdbMemTableDestroy(pMemTable, proactive);
  }

  return code;
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
