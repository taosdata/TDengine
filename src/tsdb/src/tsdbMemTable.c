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

#include "tdataformat.h"
#include "tfunctional.h"
#include "tsdbint.h"
#include "tskiplist.h"
#include "tsdbRowMergeBuf.h"

#define TSDB_DATA_SKIPLIST_LEVEL 5
#define TSDB_MAX_INSERT_BATCH 512

typedef struct {
  int32_t  totalLen;
  int32_t  len;
  SMemRow  row;
} SSubmitBlkIter;

typedef struct {
  int32_t totalLen;
  int32_t len;
  void *  pMsg;
} SSubmitMsgIter;

static SMemTable *  tsdbNewMemTable(STsdbRepo *pRepo);
static void         tsdbFreeMemTable(SMemTable *pMemTable);
static STableData*  tsdbNewTableData(STsdbCfg *pCfg, STable *pTable);
static void         tsdbFreeTableData(STableData *pTableData);
static char *       tsdbGetTsTupleKey(const void *data);
static int          tsdbAdjustMemMaxTables(SMemTable *pMemTable, int maxTables);
static int          tsdbAppendTableRowToCols(STable *pTable, SDataCols *pCols, STSchema **ppSchema, SMemRow row);
static int          tsdbInitSubmitBlkIter(SSubmitBlk *pBlock, SSubmitBlkIter *pIter);
static SMemRow      tsdbGetSubmitBlkNext(SSubmitBlkIter *pIter);
static int          tsdbScanAndConvertSubmitMsg(STsdbRepo *pRepo, SSubmitMsg *pMsg);
static int          tsdbInsertDataToTable(STsdbRepo *pRepo, SSubmitBlk *pBlock, int32_t *affectedrows);
static int          tsdbInitSubmitMsgIter(SSubmitMsg *pMsg, SSubmitMsgIter *pIter);
static int          tsdbGetSubmitMsgNext(SSubmitMsgIter *pIter, SSubmitBlk **pPBlock);
static int          tsdbCheckTableSchema(STsdbRepo *pRepo, SSubmitBlk *pBlock, STable *pTable);
static int          tsdbUpdateTableLatestInfo(STsdbRepo *pRepo, STable *pTable, SMemRow row);

static FORCE_INLINE int tsdbCheckRowRange(STsdbRepo *pRepo, STable *pTable, SMemRow row, TSKEY minKey, TSKEY maxKey,
                                          TSKEY now);

int32_t tsdbInsertData(STsdbRepo *repo, SSubmitMsg *pMsg, SShellSubmitRspMsg *pRsp) {
  STsdbRepo *    pRepo = repo;
  SSubmitMsgIter msgIter = {0};
  SSubmitBlk *   pBlock = NULL;
  int32_t        affectedrows = 0;

  if (tsdbScanAndConvertSubmitMsg(pRepo, pMsg) < 0) {
    if (terrno != TSDB_CODE_TDB_TABLE_RECONFIGURE) {
      tsdbError("vgId:%d failed to insert data since %s", REPO_ID(pRepo), tstrerror(terrno));
    }
    return -1;
  }

  tsdbInitSubmitMsgIter(pMsg, &msgIter);
  while (true) {
    tsdbGetSubmitMsgNext(&msgIter, &pBlock);
    if (pBlock == NULL) break;
    if (tsdbInsertDataToTable(pRepo, pBlock, &affectedrows) < 0) {
      return -1;
    }
  }

  if (pRsp != NULL) pRsp->affectedRows = htonl(affectedrows);

  if (tsdbCheckCommit(pRepo) < 0) return -1;
  return 0;
}

// ---------------- INTERNAL FUNCTIONS ----------------
int tsdbRefMemTable(STsdbRepo *pRepo, SMemTable *pMemTable) {
  if (pMemTable == NULL) return 0;
  int ref = T_REF_INC(pMemTable);
	tsdbDebug("vgId:%d ref memtable %p ref %d", REPO_ID(pRepo), pMemTable, ref);
  return 0;
}

// Need to lock the repository
int tsdbUnRefMemTable(STsdbRepo *pRepo, SMemTable *pMemTable) {
  if (pMemTable == NULL) return 0;

	int ref = T_REF_DEC(pMemTable);
	tsdbDebug("vgId:%d unref memtable %p ref %d", REPO_ID(pRepo), pMemTable, ref);
  if (ref == 0) {
    STsdbBufPool *pBufPool = pRepo->pPool;

    SListNode *pNode = NULL;
    bool addNew = false;
    if (tsdbLockRepo(pRepo) < 0) return -1;
    while ((pNode = tdListPopHead(pMemTable->bufBlockList)) != NULL) {
      if (pBufPool->nRecycleBlocks > 0) {
        tsdbRecycleBufferBlock(pBufPool, pNode, false);
        pBufPool->nRecycleBlocks -= 1;
      } else {
       if(pBufPool->nElasticBlocks > 0 && listNEles(pBufPool->bufBlockList) > 2) {
          tsdbRecycleBufferBlock(pBufPool, pNode, true);
        } else {
          tdListAppendNode(pBufPool->bufBlockList, pNode);
          addNew = true;
        }
      }      
    }
    if (addNew) {
      int code = pthread_cond_signal(&pBufPool->poolNotEmpty);
      if (code != 0) {
        if (tsdbUnlockRepo(pRepo) < 0) return -1;
        tsdbError("vgId:%d failed to signal pool not empty since %s", REPO_ID(pRepo), strerror(code));
        terrno = TAOS_SYSTEM_ERROR(code);
        return -1;
      }
    }

    if (tsdbUnlockRepo(pRepo) < 0) return -1;

    for (int i = 0; i < pMemTable->maxTables; i++) {
      if (pMemTable->tData[i] != NULL) {
        tsdbFreeTableData(pMemTable->tData[i]);
      }
    }

    tdListDiscard(pMemTable->actList);
    tdListDiscard(pMemTable->bufBlockList);
    tsdbFreeMemTable(pMemTable);
  }
  return 0;
}

int tsdbTakeMemSnapshot(STsdbRepo *pRepo, SMemSnapshot *pSnapshot, SArray *pATable) {
  memset(pSnapshot, 0, sizeof(*pSnapshot));

  if (tsdbLockRepo(pRepo) < 0) return -1;

  pSnapshot->omem = pRepo->mem;
  pSnapshot->imem = pRepo->imem;
  tsdbRefMemTable(pRepo, pRepo->mem);
  tsdbRefMemTable(pRepo, pRepo->imem);

  if (tsdbUnlockRepo(pRepo) < 0) return -1;

  if (pSnapshot->omem) {
    taosRLockLatch(&(pSnapshot->omem->latch));

    pSnapshot->mem = &(pSnapshot->mtable);

    pSnapshot->mem->tData = (STableData **)calloc(pSnapshot->omem->maxTables, sizeof(STableData *));
    if (pSnapshot->mem->tData == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      taosRUnLockLatch(&(pSnapshot->omem->latch));
      tsdbUnRefMemTable(pRepo, pSnapshot->omem);
      tsdbUnRefMemTable(pRepo, pSnapshot->imem);
      pSnapshot->mem = NULL;
      pSnapshot->imem = NULL;
      pSnapshot->omem = NULL;
      return -1;
    }

    pSnapshot->mem->keyFirst = pSnapshot->omem->keyFirst;
    pSnapshot->mem->keyLast = pSnapshot->omem->keyLast;
    pSnapshot->mem->numOfRows = pSnapshot->omem->numOfRows;
    pSnapshot->mem->maxTables = pSnapshot->omem->maxTables;

    for (size_t i = 0; i < taosArrayGetSize(pATable); i++) {
      STable *    pTable = *(STable **)taosArrayGet(pATable, i);
      int32_t     tid = TABLE_TID(pTable);
      STableData *pTableData = (tid < pSnapshot->omem->maxTables) ? pSnapshot->omem->tData[tid] : NULL;

      if ((pTableData == NULL) || (TABLE_UID(pTable) != pTableData->uid)) continue;

      pSnapshot->mem->tData[tid] = pTableData;
      T_REF_INC(pTableData);
    }

    taosRUnLockLatch(&(pSnapshot->omem->latch));
  }

  tsdbDebug("vgId:%d take memory snapshot, pMem %p pIMem %p", REPO_ID(pRepo), pSnapshot->omem, pSnapshot->imem);
  return 0;
}

void tsdbUnTakeMemSnapShot(STsdbRepo *pRepo, SMemSnapshot *pSnapshot) {
  tsdbDebug("vgId:%d untake memory snapshot, pMem %p pIMem %p", REPO_ID(pRepo), pSnapshot->omem, pSnapshot->imem);

  if (pSnapshot->mem) {
    ASSERT(pSnapshot->omem != NULL);

    for (size_t i = 0; i < pSnapshot->mem->maxTables; i++) {
      STableData *pTableData = pSnapshot->mem->tData[i];
      if (pTableData) {
        tsdbFreeTableData(pTableData);
      }
    }
    tfree(pSnapshot->mem->tData);

    tsdbUnRefMemTable(pRepo, pSnapshot->omem);
  }

  tsdbUnRefMemTable(pRepo, pSnapshot->imem);

  pSnapshot->mem = NULL;
  pSnapshot->imem = NULL;
  pSnapshot->omem = NULL;
}

void *tsdbAllocBytes(STsdbRepo *pRepo, int bytes) {
  STsdbCfg *     pCfg = &pRepo->config;
  STsdbBufBlock *pBufBlock = NULL;
  void *         ptr = NULL;

  // Either allocate from buffer blocks or from SYSTEM memory pool
  if (pRepo->mem == NULL) {
    SMemTable *pMemTable = tsdbNewMemTable(pRepo);
    if (pMemTable == NULL) return NULL;
    pRepo->mem = pMemTable;
  }

  ASSERT(pRepo->mem != NULL);

  pBufBlock = tsdbGetCurrBufBlock(pRepo);
  if ((pRepo->mem->extraBuffList != NULL) ||
      ((listNEles(pRepo->mem->bufBlockList) >= pCfg->totalBlocks / 3) && (pBufBlock->remain < bytes))) {
    // allocate from SYSTEM buffer pool
    if (pRepo->mem->extraBuffList == NULL) {
      pRepo->mem->extraBuffList = tdListNew(0);
      if (pRepo->mem->extraBuffList == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return NULL;
      }
    }

    ASSERT(pRepo->mem->extraBuffList != NULL);
    SListNode *pNode = (SListNode *)malloc(sizeof(SListNode) + bytes);
    if (pNode == NULL) {
      if (listNEles(pRepo->mem->extraBuffList) == 0) {
        tdListFree(pRepo->mem->extraBuffList);
        pRepo->mem->extraBuffList = NULL;
      }
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return NULL;
    }

    pNode->next = pNode->prev = NULL;
    tdListAppendNode(pRepo->mem->extraBuffList, pNode);
    ptr = (void *)(pNode->data);
    tsdbTrace("vgId:%d allocate %d bytes from SYSTEM buffer block", REPO_ID(pRepo), bytes);
  } else {  // allocate from TSDB buffer pool
    if (pBufBlock == NULL || pBufBlock->remain < bytes) {
      ASSERT(listNEles(pRepo->mem->bufBlockList) < pCfg->totalBlocks / 3);
      if (tsdbLockRepo(pRepo) < 0) return NULL;
      SListNode *pNode = tsdbAllocBufBlockFromPool(pRepo);
      tdListAppendNode(pRepo->mem->bufBlockList, pNode);
      if (tsdbUnlockRepo(pRepo) < 0) return NULL;
      pBufBlock = tsdbGetCurrBufBlock(pRepo);
    }

    ASSERT(pBufBlock->remain >= bytes);
    ptr = POINTER_SHIFT(pBufBlock->data, pBufBlock->offset);
    pBufBlock->offset += bytes;
    pBufBlock->remain -= bytes;
    tsdbTrace("vgId:%d allocate %d bytes from TSDB buffer block, nBlocks %d offset %d remain %d", REPO_ID(pRepo), bytes,
              listNEles(pRepo->mem->bufBlockList), pBufBlock->offset, pBufBlock->remain);
  }

  return ptr;
}

int tsdbSyncCommitConfig(STsdbRepo* pRepo) {
  ASSERT(pRepo->config_changed == true);
  tsem_wait(&(pRepo->readyToCommit));

  if (pRepo->code != TSDB_CODE_SUCCESS) {
    tsdbWarn("vgId:%d try to commit config when TSDB not in good state: %s", REPO_ID(pRepo), tstrerror(terrno));
  }

  if (tsdbLockRepo(pRepo) < 0) return -1;
  tsdbScheduleCommit(pRepo, COMMIT_CONFIG_REQ);
  if (tsdbUnlockRepo(pRepo) < 0) return -1;

  tsem_wait(&(pRepo->readyToCommit));
  tsem_post(&(pRepo->readyToCommit));

  if (pRepo->code != TSDB_CODE_SUCCESS) {
    terrno = pRepo->code;
    return -1;
  }

  terrno = TSDB_CODE_SUCCESS;
  return 0;
}

int tsdbAsyncCommit(STsdbRepo *pRepo) {
  tsem_wait(&(pRepo->readyToCommit));

  ASSERT(pRepo->imem == NULL);
  if (pRepo->mem == NULL) {
    tsem_post(&(pRepo->readyToCommit));
    return 0;
  }

  if (pRepo->code != TSDB_CODE_SUCCESS) {
    tsdbWarn("vgId:%d try to commit when TSDB not in good state: %s", REPO_ID(pRepo), tstrerror(terrno));
  }

  if (pRepo->appH.notifyStatus) pRepo->appH.notifyStatus(pRepo->appH.appH, TSDB_STATUS_COMMIT_START, TSDB_CODE_SUCCESS);
  if (tsdbLockRepo(pRepo) < 0) return -1;
  pRepo->imem = pRepo->mem;
  pRepo->mem = NULL;
  tsdbScheduleCommit(pRepo, COMMIT_REQ);
  if (tsdbUnlockRepo(pRepo) < 0) return -1;

  return 0;
}

int tsdbSyncCommit(STsdbRepo *repo) {
  STsdbRepo *pRepo = repo;

  tsdbAsyncCommit(pRepo);
  tsem_wait(&(pRepo->readyToCommit));
  tsem_post(&(pRepo->readyToCommit));

  if (pRepo->code != TSDB_CODE_SUCCESS) {
    terrno = pRepo->code;
    return -1;
  } else {
    terrno = TSDB_CODE_SUCCESS;
    return 0;
  }
}

/**
 * This is an important function to load data or try to load data from memory skiplist iterator.
 * 
 * This function load memory data until:
 * 1. iterator ends
 * 2. data key exceeds maxKey
 * 3. rowsIncreased = rowsInserted - rowsDeleteSucceed >= maxRowsToRead
 * 4. operations in pCols not exceeds its max capacity if pCols is given
 * 
 * The function tries to procceed AS MUCH AS POSSIBLE.
 */
int tsdbLoadDataFromCache(STable *pTable, SSkipListIterator *pIter, TSKEY maxKey, int maxRowsToRead, SDataCols *pCols,
                          TKEY *filterKeys, int nFilterKeys, bool keepDup, SMergeInfo *pMergeInfo) {
  ASSERT(maxRowsToRead > 0 && nFilterKeys >= 0);
  if (pIter == NULL) return 0;
  STSchema * pSchema = NULL;
  TSKEY      rowKey = 0;
  TSKEY      fKey = 0;
  bool       isRowDel = false;
  int        filterIter = 0;
  SMemRow    row = NULL;
  SMergeInfo mInfo;

  if (pMergeInfo == NULL) pMergeInfo = &mInfo;

  memset(pMergeInfo, 0, sizeof(*pMergeInfo));
  pMergeInfo->keyFirst = INT64_MAX;
  pMergeInfo->keyLast = INT64_MIN;
  if (pCols) tdResetDataCols(pCols);

  row = tsdbNextIterRow(pIter);
  if (row == NULL || memRowKey(row) > maxKey) {
    rowKey = INT64_MAX;
    isRowDel = false;
  } else {
    rowKey = memRowKey(row);
    isRowDel = memRowDeleted(row);
  }

  if (filterIter >= nFilterKeys) {
    fKey = INT64_MAX;
  } else {
    fKey = tdGetKey(filterKeys[filterIter]);
  }

  while (true) {
    if (fKey == INT64_MAX && rowKey == INT64_MAX) break;

    if (fKey < rowKey) {
      pMergeInfo->keyFirst = MIN(pMergeInfo->keyFirst, fKey);
      pMergeInfo->keyLast = MAX(pMergeInfo->keyLast, fKey);

      filterIter++;
      if (filterIter >= nFilterKeys) {
        fKey = INT64_MAX;
      } else {
        fKey = tdGetKey(filterKeys[filterIter]);
      }
    } else if (fKey > rowKey) {
      if (isRowDel) {
        pMergeInfo->rowsDeleteFailed++;
      } else {
        if (pMergeInfo->rowsInserted - pMergeInfo->rowsDeleteSucceed >= maxRowsToRead) break;
        if (pCols && pMergeInfo->nOperations >= pCols->maxPoints) break;
        pMergeInfo->rowsInserted++;
        pMergeInfo->nOperations++;
        pMergeInfo->keyFirst = MIN(pMergeInfo->keyFirst, rowKey);
        pMergeInfo->keyLast = MAX(pMergeInfo->keyLast, rowKey);
        tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row);
      }

      tSkipListIterNext(pIter);
      row = tsdbNextIterRow(pIter);
      if (row == NULL || memRowKey(row) > maxKey) {
        rowKey = INT64_MAX;
        isRowDel = false;
      } else {
        rowKey = memRowKey(row);
        isRowDel = memRowDeleted(row);
      }
    } else {
      if (isRowDel) {
        ASSERT(!keepDup);
        if (pCols && pMergeInfo->nOperations >= pCols->maxPoints) break;
        pMergeInfo->rowsDeleteSucceed++;
        pMergeInfo->nOperations++;
        tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row);
      } else {
        if (keepDup) {
          if (pCols && pMergeInfo->nOperations >= pCols->maxPoints) break;
          pMergeInfo->rowsUpdated++;
          pMergeInfo->nOperations++;
          pMergeInfo->keyFirst = MIN(pMergeInfo->keyFirst, rowKey);
          pMergeInfo->keyLast = MAX(pMergeInfo->keyLast, rowKey);
          tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row);
        } else {
          pMergeInfo->keyFirst = MIN(pMergeInfo->keyFirst, fKey);
          pMergeInfo->keyLast = MAX(pMergeInfo->keyLast, fKey);
        }
      }

      tSkipListIterNext(pIter);
      row = tsdbNextIterRow(pIter);
      if (row == NULL || memRowKey(row) > maxKey) {
        rowKey = INT64_MAX;
        isRowDel = false;
      } else {
        rowKey = memRowKey(row);
        isRowDel = memRowDeleted(row);
      }

      filterIter++;
      if (filterIter >= nFilterKeys) {
        fKey = INT64_MAX;
      } else {
        fKey = tdGetKey(filterKeys[filterIter]);
      }
    }
  }

  return 0;
}

// ---------------- LOCAL FUNCTIONS ----------------
static SMemTable* tsdbNewMemTable(STsdbRepo *pRepo) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  SMemTable *pMemTable = (SMemTable *)calloc(1, sizeof(*pMemTable));
  if (pMemTable == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pMemTable->keyFirst = INT64_MAX;
  pMemTable->keyLast = 0;
  pMemTable->numOfRows = 0;

  pMemTable->maxTables = pMeta->maxTables;
  pMemTable->tData = (STableData **)calloc(pMemTable->maxTables, sizeof(STableData *));
  if (pMemTable->tData == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pMemTable->actList = tdListNew(0);
  if (pMemTable->actList == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pMemTable->bufBlockList = tdListNew(sizeof(STsdbBufBlock*));
  if (pMemTable->bufBlockList == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  T_REF_INC(pMemTable);

  return pMemTable;

_err:
  tsdbFreeMemTable(pMemTable);
  return NULL;
}

static void tsdbFreeMemTable(SMemTable* pMemTable) {
  if (pMemTable) {
    ASSERT((pMemTable->bufBlockList == NULL) ? true : (listNEles(pMemTable->bufBlockList) == 0));
    ASSERT((pMemTable->actList == NULL) ? true : (listNEles(pMemTable->actList) == 0));

    tdListFree(pMemTable->extraBuffList);
    tdListFree(pMemTable->bufBlockList);
    tdListFree(pMemTable->actList);
    tfree(pMemTable->tData);
    free(pMemTable);
  }
}

static STableData *tsdbNewTableData(STsdbCfg *pCfg, STable *pTable) {
  STableData *pTableData = (STableData *)calloc(1, sizeof(*pTableData));
  if (pTableData == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  pTableData->uid = TABLE_UID(pTable);
  pTableData->keyFirst = INT64_MAX;
  pTableData->keyLast = 0;
  pTableData->numOfRows = 0;

  uint8_t skipListCreateFlags;
  if(pCfg->update == TD_ROW_DISCARD_UPDATE)
    skipListCreateFlags = SL_DISCARD_DUP_KEY;
  else
    skipListCreateFlags = SL_UPDATE_DUP_KEY;

  pTableData->pData =
      tSkipListCreate(TSDB_DATA_SKIPLIST_LEVEL, TSDB_DATA_TYPE_TIMESTAMP, TYPE_BYTES[TSDB_DATA_TYPE_TIMESTAMP],
                      tkeyComparFn, skipListCreateFlags, tsdbGetTsTupleKey);
  if (pTableData->pData == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    free(pTableData);
    return NULL;
  }

  T_REF_INC(pTableData);

  return pTableData;
}

static void tsdbFreeTableData(STableData *pTableData) {
  if (pTableData) {
    int32_t ref = T_REF_DEC(pTableData);
    if (ref == 0) {
      tSkipListDestroy(pTableData->pData);
      free(pTableData);
    }
  }
}

static char *tsdbGetTsTupleKey(const void *data) { return memRowKeys((SMemRow)data); }

static int tsdbAdjustMemMaxTables(SMemTable *pMemTable, int maxTables) {
  ASSERT(pMemTable->maxTables < maxTables);

  STableData **pTableData = (STableData **)calloc(maxTables, sizeof(STableData *));
  if (pTableData == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }
  memcpy((void *)pTableData, (void *)pMemTable->tData, sizeof(STableData *) * pMemTable->maxTables);

  STableData **tData = pMemTable->tData;

  taosWLockLatch(&(pMemTable->latch));
  pMemTable->maxTables = maxTables;
  pMemTable->tData = pTableData;
  taosWUnLockLatch(&(pMemTable->latch));

  tfree(tData);

  return 0;
}

static int tsdbAppendTableRowToCols(STable *pTable, SDataCols *pCols, STSchema **ppSchema, SMemRow row) {
  if (pCols) {
    if (*ppSchema == NULL || schemaVersion(*ppSchema) != memRowVersion(row)) {
      *ppSchema = tsdbGetTableSchemaImpl(pTable, false, false, memRowVersion(row), (int8_t)memRowType(row));
      if (*ppSchema == NULL) {
        ASSERT(false);
        return -1;
      }
    }

    tdAppendMemRowToDataCol(row, *ppSchema, pCols, true, 0);
  }

  return 0;
}

static int tsdbInitSubmitBlkIter(SSubmitBlk *pBlock, SSubmitBlkIter *pIter) {
  if (pBlock->dataLen <= 0) return -1;
  pIter->totalLen = pBlock->dataLen;
  pIter->len = 0;
  pIter->row = (SMemRow)(pBlock->data + pBlock->schemaLen);
  return 0;
}

static SMemRow tsdbGetSubmitBlkNext(SSubmitBlkIter *pIter) {
  SMemRow row = pIter->row;  // firstly, get current row
  if (row == NULL) return NULL;

  pIter->len += memRowTLen(row);
  if (pIter->len >= pIter->totalLen) {  // reach the end
    pIter->row = NULL;
  } else {
    pIter->row = (char *)row + memRowTLen(row);  // secondly, move to next row
  }

  return row;
}

static FORCE_INLINE int tsdbCheckRowRange(STsdbRepo *pRepo, STable *pTable, SMemRow row, TSKEY minKey, TSKEY maxKey,
                                          TSKEY now) {
  TSKEY rowKey = memRowKey(row);
  if (rowKey < minKey || rowKey > maxKey) {
    tsdbError("vgId:%d table %s tid %d uid %" PRIu64 " timestamp is out of range! now %" PRId64 " minKey %" PRId64
              " maxKey %" PRId64 " row key %" PRId64,
              REPO_ID(pRepo), TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TABLE_UID(pTable), now, minKey, maxKey,
              rowKey);
    terrno = TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE;
    return -1;
  }

  return 0;
}

static int tsdbScanAndConvertSubmitMsg(STsdbRepo *pRepo, SSubmitMsg *pMsg) {
  ASSERT(pMsg != NULL);
  STsdbMeta *    pMeta = pRepo->tsdbMeta;
  SSubmitMsgIter msgIter = {0};
  SSubmitBlk *   pBlock = NULL;
  SSubmitBlkIter blkIter = {0};
  SMemRow        row = NULL;
  TSKEY          now = taosGetTimestamp(pRepo->config.precision);
  TSKEY          minKey = now - tsTickPerDay[pRepo->config.precision] * pRepo->config.keep;
  TSKEY          maxKey = now + tsTickPerDay[pRepo->config.precision] * pRepo->config.daysPerFile;

  terrno = TSDB_CODE_SUCCESS;
  pMsg->length = htonl(pMsg->length);
  pMsg->numOfBlocks = htonl(pMsg->numOfBlocks);

  if (tsdbInitSubmitMsgIter(pMsg, &msgIter) < 0) return -1;
  while (true) {
    if (tsdbGetSubmitMsgNext(&msgIter, &pBlock) < 0) return -1;
    if (pBlock == NULL) break;

    pBlock->uid = htobe64(pBlock->uid);
    pBlock->tid = htonl(pBlock->tid);
    pBlock->sversion = htonl(pBlock->sversion);
    pBlock->dataLen = htonl(pBlock->dataLen);
    pBlock->schemaLen = htonl(pBlock->schemaLen);
    pBlock->numOfRows = htons(pBlock->numOfRows);

    if (pBlock->tid <= 0 || pBlock->tid >= pMeta->maxTables) {
      tsdbError("vgId:%d failed to get table to insert data, uid %" PRIu64 " tid %d", REPO_ID(pRepo), pBlock->uid,
                pBlock->tid);
      terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
      return -1;
    }

    STable *pTable = pMeta->tables[pBlock->tid];
    if (pTable == NULL || TABLE_UID(pTable) != pBlock->uid) {
      tsdbError("vgId:%d failed to get table to insert data, uid %" PRIu64 " tid %d", REPO_ID(pRepo), pBlock->uid,
                pBlock->tid);
      terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
      return -1;
    }

    if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
      tsdbError("vgId:%d invalid action trying to insert a super table %s", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable));
      terrno = TSDB_CODE_TDB_INVALID_ACTION;
      return -1;
    }

    // Check schema version and update schema if needed
    if (tsdbCheckTableSchema(pRepo, pBlock, pTable) < 0) {
      if (terrno == TSDB_CODE_TDB_TABLE_RECONFIGURE) {
        continue;
      } else {
        return -1;
      }
    }

    tsdbInitSubmitBlkIter(pBlock, &blkIter);
    while ((row = tsdbGetSubmitBlkNext(&blkIter)) != NULL) {
      if (tsdbCheckRowRange(pRepo, pTable, row, minKey, maxKey, now) < 0) {
        return -1;
      }
    }
  }

  if (terrno != TSDB_CODE_SUCCESS) return -1;
  return 0;
}

//row1 has higher priority
static SMemRow tsdbInsertDupKeyMerge(SMemRow row1, SMemRow row2, STsdbRepo* pRepo,
                                     STSchema **ppSchema1, STSchema **ppSchema2,
                                     STable* pTable, int32_t* pPoints, SMemRow* pLastRow) {
  
  //for compatiblity, duplicate key inserted when update=0 should be also calculated as affected rows!
  if(row1 == NULL && row2 == NULL && pRepo->config.update == TD_ROW_DISCARD_UPDATE) {
    (*pPoints)++;
    return NULL;
  }

  if(row2 == NULL || pRepo->config.update != TD_ROW_PARTIAL_UPDATE) {
    void* pMem = tsdbAllocBytes(pRepo, memRowTLen(row1));
    if(pMem == NULL) return NULL;
    memRowCpy(pMem, row1);
    (*pPoints)++;
    *pLastRow = pMem;
    return pMem;
  }

  STSchema *pSchema1 = *ppSchema1;
  STSchema *pSchema2 = *ppSchema2;
  SMergeBuf * pBuf = &pRepo->mergeBuf;
  int dv1 = memRowVersion(row1);
  int dv2 = memRowVersion(row2);
  if(pSchema1 == NULL || schemaVersion(pSchema1) != dv1) {
    if(pSchema2 != NULL && schemaVersion(pSchema2) == dv1) {
      *ppSchema1 = pSchema2;
    } else {
      *ppSchema1 = tsdbGetTableSchemaImpl(pTable, false, false, memRowVersion(row1), (int8_t)memRowType(row1));
    }
    pSchema1 = *ppSchema1;
  }

  if(pSchema2 == NULL || schemaVersion(pSchema2) != dv2) {
    if(schemaVersion(pSchema1) == dv2) {
      pSchema2 = pSchema1;
    } else {
      *ppSchema2 = tsdbGetTableSchemaImpl(pTable, false, false, memRowVersion(row2), (int8_t)memRowType(row2));
      pSchema2 = *ppSchema2;
    }
  }

  SMemRow tmp = tsdbMergeTwoRows(pBuf, row1, row2, pSchema1, pSchema2);

  void* pMem = tsdbAllocBytes(pRepo, memRowTLen(tmp));
  if(pMem == NULL) return NULL;
  memRowCpy(pMem, tmp);

  (*pPoints)++;
  *pLastRow = pMem;
  return pMem;
}

static void* tsdbInsertDupKeyMergePacked(void** args) {
  return tsdbInsertDupKeyMerge(args[0], args[1], args[2], (STSchema**)&args[3], (STSchema**)&args[4], args[5], args[6], args[7]);
}

static void tsdbSetupSkipListHookFns(SSkipList* pSkipList, STsdbRepo *pRepo, STable *pTable, int32_t* pPoints, SMemRow* pLastRow) {

  if(pSkipList->insertHandleFn == NULL) {
    tGenericSavedFunc *dupHandleSavedFunc = genericSavedFuncInit((GenericVaFunc)&tsdbInsertDupKeyMergePacked, 9);
    dupHandleSavedFunc->args[2] = pRepo;
    dupHandleSavedFunc->args[3] = NULL;
    dupHandleSavedFunc->args[4] = NULL;
    dupHandleSavedFunc->args[5] = pTable;
    pSkipList->insertHandleFn = dupHandleSavedFunc;
  }
  pSkipList->insertHandleFn->args[6] = pPoints;
  pSkipList->insertHandleFn->args[7] = pLastRow;
}

static int tsdbInsertDataToTable(STsdbRepo* pRepo, SSubmitBlk* pBlock, int32_t *pAffectedRows) {

  STsdbMeta       *pMeta = pRepo->tsdbMeta;
  int32_t          points = 0;
  STable          *pTable = NULL;
  SSubmitBlkIter   blkIter = {0};
  SMemTable       *pMemTable = NULL;
  STableData      *pTableData = NULL;
  STsdbCfg        *pCfg = &(pRepo->config);

  tsdbInitSubmitBlkIter(pBlock, &blkIter);
  if(blkIter.row == NULL) return 0;
  TSKEY firstRowKey = memRowKey(blkIter.row);

  tsdbAllocBytes(pRepo, 0);
  pMemTable = pRepo->mem;

  ASSERT(pMemTable != NULL);
  ASSERT(pBlock->tid < pMeta->maxTables);

  pTable = pMeta->tables[pBlock->tid];

  ASSERT(pTable != NULL && TABLE_UID(pTable) == pBlock->uid);


  if (TABLE_TID(pTable) >= pMemTable->maxTables) {
    if (tsdbAdjustMemMaxTables(pMemTable, pMeta->maxTables) < 0) {
      return -1;
    }
  }
  pTableData = pMemTable->tData[TABLE_TID(pTable)];

  if (pTableData == NULL || pTableData->uid != TABLE_UID(pTable)) {
    if (pTableData != NULL) {
      taosWLockLatch(&(pMemTable->latch));
      pMemTable->tData[TABLE_TID(pTable)] = NULL;
      tsdbFreeTableData(pTableData);
      taosWUnLockLatch(&(pMemTable->latch));
    }

    pTableData = tsdbNewTableData(pCfg, pTable);
    if (pTableData == NULL) {
      tsdbError("vgId:%d failed to insert data to table %s uid %" PRId64 " tid %d since %s", REPO_ID(pRepo),
                TABLE_CHAR_NAME(pTable), TABLE_UID(pTable), TABLE_TID(pTable), tstrerror(terrno));
      return -1;
    }

    pRepo->mem->tData[TABLE_TID(pTable)] = pTableData;
  }

  ASSERT((pTableData != NULL) && pTableData->uid == TABLE_UID(pTable));

  SMemRow lastRow = NULL;
  int64_t osize = SL_SIZE(pTableData->pData);
  tsdbSetupSkipListHookFns(pTableData->pData, pRepo, pTable, &points, &lastRow);
  tSkipListPutBatchByIter(pTableData->pData, &blkIter, (iter_next_fn_t)tsdbGetSubmitBlkNext);
  int64_t dsize = SL_SIZE(pTableData->pData) - osize;
  (*pAffectedRows) += points;


  if(lastRow != NULL) {
    TSKEY lastRowKey = memRowKey(lastRow);
    if (pMemTable->keyFirst > firstRowKey) pMemTable->keyFirst = firstRowKey;
    pMemTable->numOfRows += dsize;

    if (pTableData->keyFirst > firstRowKey) pTableData->keyFirst = firstRowKey;
    pTableData->numOfRows += dsize;
    if (pMemTable->keyLast < lastRowKey) pMemTable->keyLast = lastRowKey;
    if (pTableData->keyLast < lastRowKey) pTableData->keyLast = lastRowKey;
    if (tsdbUpdateTableLatestInfo(pRepo, pTable, lastRow) < 0) {
      return -1;
    }
  }

  STSchema *pSchema = tsdbGetTableSchemaByVersion(pTable, pBlock->sversion, -1);
  pRepo->stat.pointsWritten += points * schemaNCols(pSchema);
  pRepo->stat.totalStorage += points * schemaVLen(pSchema);

  return 0;
}


static int tsdbInitSubmitMsgIter(SSubmitMsg *pMsg, SSubmitMsgIter *pIter) {
  if (pMsg == NULL) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    return -1;
  }

  pIter->totalLen = pMsg->length;
  pIter->len = 0;
  pIter->pMsg = pMsg;
  if (pMsg->length <= TSDB_SUBMIT_MSG_HEAD_SIZE) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    return -1;
  }

  return 0;
}

static int tsdbGetSubmitMsgNext(SSubmitMsgIter *pIter, SSubmitBlk **pPBlock) {
  if (pIter->len == 0) {
    pIter->len += TSDB_SUBMIT_MSG_HEAD_SIZE;
  } else {
    SSubmitBlk *pSubmitBlk = (SSubmitBlk *)POINTER_SHIFT(pIter->pMsg, pIter->len);
    pIter->len += (sizeof(SSubmitBlk) + pSubmitBlk->dataLen + pSubmitBlk->schemaLen);
  }

  if (pIter->len > pIter->totalLen) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    *pPBlock = NULL;
    return -1;
  }

  *pPBlock = (pIter->len == pIter->totalLen) ? NULL : (SSubmitBlk *)POINTER_SHIFT(pIter->pMsg, pIter->len);

  return 0;
}

static int tsdbCheckTableSchema(STsdbRepo *pRepo, SSubmitBlk *pBlock, STable *pTable) {
  ASSERT(pTable != NULL);

  STSchema *pSchema = tsdbGetTableSchemaImpl(pTable, false, false, -1, -1);
  int       sversion = schemaVersion(pSchema);

  if (pBlock->sversion == sversion) {
    return 0;
  } else {
    if (TABLE_TYPE(pTable) == TSDB_STREAM_TABLE) {  // stream table is not allowed to change schema
      terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
      return -1;
    }
  }

  if (pBlock->sversion > sversion) {  // may need to update table schema
    if (pBlock->schemaLen > 0) {
      tsdbDebug(
          "vgId:%d table %s tid %d uid %" PRIu64 " schema version %d is out of data, client version %d, update...",
          REPO_ID(pRepo), TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TABLE_UID(pTable), sversion, pBlock->sversion);
      ASSERT(pBlock->schemaLen % sizeof(STColumn) == 0);
      int       numOfCols = pBlock->schemaLen / sizeof(STColumn);
      STColumn *pTCol = (STColumn *)pBlock->data;

      STSchemaBuilder schemaBuilder = {0};
      if (tdInitTSchemaBuilder(&schemaBuilder, pBlock->sversion) < 0) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        tsdbError("vgId:%d failed to update schema of table %s since %s", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable),
                  tstrerror(terrno));
        return -1;
      }

      for (int i = 0; i < numOfCols; i++) {
        if (tdAddColToSchema(&schemaBuilder, pTCol[i].type, htons(pTCol[i].colId), htons(pTCol[i].bytes)) < 0) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          tsdbError("vgId:%d failed to update schema of table %s since %s", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable),
                    tstrerror(terrno));
          tdDestroyTSchemaBuilder(&schemaBuilder);
          return -1;
        }
      }

      STSchema *pNSchema = tdGetSchemaFromBuilder(&schemaBuilder);
      if (pNSchema == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        tdDestroyTSchemaBuilder(&schemaBuilder);
        return -1;
      }

      tdDestroyTSchemaBuilder(&schemaBuilder);
      tsdbUpdateTableSchema(pRepo, pTable, pNSchema, true);
    } else {
      tsdbDebug(
          "vgId:%d table %s tid %d uid %" PRIu64 " schema version %d is out of data, client version %d, reconfigure...",
          REPO_ID(pRepo), TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TABLE_UID(pTable), sversion, pBlock->sversion);
      terrno = TSDB_CODE_TDB_TABLE_RECONFIGURE;
      return -1;
    }
  } else {
    ASSERT(pBlock->sversion >= 0);
    if (tsdbGetTableSchemaImpl(pTable, false, false, pBlock->sversion, -1) == NULL) {
      tsdbError("vgId:%d invalid submit schema version %d to table %s tid %d from client", REPO_ID(pRepo),
                pBlock->sversion, TABLE_CHAR_NAME(pTable), TABLE_TID(pTable));
      terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
      return -1;
    }
  }

  return 0;
}


static void updateTableLatestColumn(STsdbRepo *pRepo, STable *pTable, SMemRow row) {
  tsdbDebug("vgId:%d updateTableLatestColumn, %s row version:%d", REPO_ID(pRepo), pTable->name->data,
            memRowVersion(row));

  STSchema* pSchema = tsdbGetTableLatestSchema(pTable);
  if (tsdbUpdateLastColSchema(pTable, pSchema) < 0) {
    return;
  }

  pSchema = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row), (int8_t)memRowType(row));
  if (pSchema == NULL) {
    return;
  }

  SDataCol *pLatestCols = pTable->lastCols;
  int32_t kvIdx = 0;

  for (int16_t j = 0; j < schemaNCols(pSchema); j++) {
    STColumn *pTCol = schemaColAt(pSchema, j);
    // ignore not exist colId
    int16_t idx = tsdbGetLastColumnsIndexByColId(pTable, pTCol->colId);
    if (idx == -1) {
      continue;
    }

    void *value = NULL;

    value = tdGetMemRowDataOfColEx(row, pTCol->colId, (int8_t)pTCol->type,
                                   TD_DATA_ROW_HEAD_SIZE + pSchema->columns[j].offset, &kvIdx);

    if ((value == NULL) || isNull(value, pTCol->type)) {
      continue;
    }
    // lock
    TSDB_WLOCK_TABLE(pTable); 
    SDataCol *pDataCol = &(pLatestCols[idx]);
    if (pDataCol->pData == NULL) {
      pDataCol->pData = malloc(pTCol->bytes);
      pDataCol->bytes = pTCol->bytes;
    } else if (pDataCol->bytes < pTCol->bytes) {
      pDataCol->pData = realloc(pDataCol->pData, pTCol->bytes);
      pDataCol->bytes = pTCol->bytes;
    }
    // the actual value size
    uint16_t bytes = IS_VAR_DATA_TYPE(pTCol->type) ? varDataTLen(value) : pTCol->bytes;
    // the actual data size CANNOT larger than column size
    assert(pTCol->bytes >= bytes);
    memcpy(pDataCol->pData, value, bytes);
    //tsdbInfo("updateTableLatestColumn vgId:%d cache column %d for %d,%s", REPO_ID(pRepo), j, pDataCol->bytes, (char*)pDataCol->pData);
    pDataCol->ts = memRowKey(row);
    // unlock
    TSDB_WUNLOCK_TABLE(pTable); 
  }
}

static int tsdbUpdateTableLatestInfo(STsdbRepo *pRepo, STable *pTable, SMemRow row) {
  STsdbCfg *pCfg = &pRepo->config;

  // if cacheLastRow config has been reset, free the lastRow
  if (!pCfg->cacheLastRow && pTable->lastRow != NULL) {
    SMemRow cachedLastRow = pTable->lastRow;
    TSDB_WLOCK_TABLE(pTable);
    pTable->lastRow = NULL;
    TSDB_WUNLOCK_TABLE(pTable);
    taosTZfree(cachedLastRow);
  }

  if (tsdbGetTableLastKeyImpl(pTable) <= memRowKey(row)) {
    if (CACHE_LAST_ROW(pCfg) || pTable->lastRow != NULL) {
      SMemRow nrow = pTable->lastRow;
      if (taosTSizeof(nrow) < memRowTLen(row)) {
        SMemRow orow = nrow;
        nrow = taosTMalloc(memRowTLen(row));
        if (nrow == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          return -1;
        }

        memRowCpy(nrow, row);
        TSDB_WLOCK_TABLE(pTable);
        pTable->lastKey = memRowKey(row);
        pTable->lastRow = nrow;
        TSDB_WUNLOCK_TABLE(pTable);
        taosTZfree(orow);
      } else {
        TSDB_WLOCK_TABLE(pTable);
        pTable->lastKey = memRowKey(row);
        memRowCpy(nrow, row);
        TSDB_WUNLOCK_TABLE(pTable);
      }
    } else {
      pTable->lastKey = memRowKey(row);
    }

    if (CACHE_LAST_NULL_COLUMN(pCfg)) {
      updateTableLatestColumn(pRepo, pTable, row);
    }
  }

  pTable->cacheLastConfigVersion = pRepo->cacheLastConfigVersion;

  return 0;
}
