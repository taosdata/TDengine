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
#include "tsdbMain.h"

#define TSDB_DATA_SKIPLIST_LEVEL 5

static SMemTable * tsdbNewMemTable(STsdbRepo *pRepo);
static void        tsdbFreeMemTable(SMemTable *pMemTable);
static STableData *tsdbNewTableData(STsdbCfg *pCfg, STable *pTable);
static void        tsdbFreeTableData(STableData *pTableData);
static char *      tsdbGetTsTupleKey(const void *data);
static int          tsdbAdjustMemMaxTables(SMemTable *pMemTable, int maxTables);
static int          tsdbAppendTableRowToCols(STable *pTable, SDataCols *pCols, STSchema **ppSchema, SDataRow row);
static int          tsdbInitSubmitBlkIter(SSubmitBlk *pBlock, SSubmitBlkIter *pIter);
static SDataRow     tsdbGetSubmitBlkNext(SSubmitBlkIter *pIter);
static int          tsdbScanAndConvertSubmitMsg(STsdbRepo *pRepo, SSubmitMsg *pMsg);
static int          tsdbInsertDataToTable(STsdbRepo *pRepo, SSubmitBlk *pBlock, int32_t *affectedrows);
static int          tsdbCopyRowToMem(STsdbRepo *pRepo, SDataRow row, STable *pTable, void **ppRow);
static int          tsdbInitSubmitMsgIter(SSubmitMsg *pMsg, SSubmitMsgIter *pIter);
static int          tsdbGetSubmitMsgNext(SSubmitMsgIter *pIter, SSubmitBlk **pPBlock);
static int          tsdbCheckTableSchema(STsdbRepo *pRepo, SSubmitBlk *pBlock, STable *pTable);
static int          tsdbInsertDataToTableImpl(STsdbRepo *pRepo, STable *pTable, void **rows, int rowCounter);
static void         tsdbFreeRows(STsdbRepo *pRepo, void **rows, int rowCounter);

static FORCE_INLINE int tsdbCheckRowRange(STsdbRepo *pRepo, STable *pTable, SDataRow row, TSKEY minKey, TSKEY maxKey,
                                          TSKEY now);

int32_t tsdbInsertData(TSDB_REPO_T *repo, SSubmitMsg *pMsg, SShellSubmitRspMsg *pRsp) {
  STsdbRepo *    pRepo = (STsdbRepo *)repo;
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
    if (tsdbLockRepo(pRepo) < 0) return -1;
    while ((pNode = tdListPopHead(pMemTable->bufBlockList)) != NULL) {
      tdListAppendNode(pBufPool->bufBlockList, pNode);
    }
    int code = pthread_cond_signal(&pBufPool->poolNotEmpty);
    if (code != 0) {
      if (tsdbUnlockRepo(pRepo) < 0) return -1;
      tsdbError("vgId:%d failed to signal pool not empty since %s", REPO_ID(pRepo), strerror(code));
      terrno = TAOS_SYSTEM_ERROR(code);
      return -1;
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

int tsdbTakeMemSnapshot(STsdbRepo *pRepo, SMemTable **pMem, SMemTable **pIMem) {
  if (tsdbLockRepo(pRepo) < 0) return -1;

  *pMem = pRepo->mem;
  *pIMem = pRepo->imem;
  tsdbRefMemTable(pRepo, *pMem);
  tsdbRefMemTable(pRepo, *pIMem);

  if (tsdbUnlockRepo(pRepo) < 0) return -1;

  if (*pMem != NULL) taosRLockLatch(&((*pMem)->latch));

  tsdbDebug("vgId:%d take memory snapshot, pMem %p pIMem %p", REPO_ID(pRepo), *pMem, *pIMem);
  return 0;
}

void tsdbUnTakeMemSnapShot(STsdbRepo *pRepo, SMemTable *pMem, SMemTable *pIMem) {
  tsdbDebug("vgId:%d untake memory snapshot, pMem %p pIMem %p", REPO_ID(pRepo), pMem, pIMem);

  if (pMem != NULL) {
    taosRUnLockLatch(&(pMem->latch));
    tsdbUnRefMemTable(pRepo, pMem);
  }

  if (pIMem != NULL) {
    tsdbUnRefMemTable(pRepo, pIMem);
  }
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

int tsdbAsyncCommit(STsdbRepo *pRepo) {
  if (pRepo->mem == NULL) return 0;

  SMemTable *pIMem = pRepo->imem;

  sem_wait(&(pRepo->readyToCommit));

  if (pRepo->code != TSDB_CODE_SUCCESS) {
    tsdbWarn("vgId:%d try to commit when TSDB not in good state: %s", REPO_ID(pRepo), tstrerror(terrno));
  }

  if (pRepo->appH.notifyStatus) pRepo->appH.notifyStatus(pRepo->appH.appH, TSDB_STATUS_COMMIT_START, TSDB_CODE_SUCCESS);
  if (tsdbLockRepo(pRepo) < 0) return -1;
  pRepo->imem = pRepo->mem;
  pRepo->mem = NULL;
  tsdbScheduleCommit(pRepo);
  if (tsdbUnlockRepo(pRepo) < 0) return -1;

  if (tsdbUnRefMemTable(pRepo, pIMem) < 0) return -1;

  return 0;
}

int tsdbSyncCommit(TSDB_REPO_T *repo) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  tsdbAsyncCommit(pRepo);
  sem_wait(&(pRepo->readyToCommit));
  sem_post(&(pRepo->readyToCommit));

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
 * The function tries to procceed AS MUSH AS POSSIBLE.
 */
int tsdbLoadDataFromCache(STable *pTable, SSkipListIterator *pIter, TSKEY maxKey, int maxRowsToRead, SDataCols *pCols,
                          TKEY *filterKeys, int nFilterKeys, bool keepDup, SMergeInfo *pMergeInfo) {
  ASSERT(maxRowsToRead > 0 && nFilterKeys >= 0 && pMergeInfo != NULL);
  if (pIter == NULL) return 0;
  STSchema *pSchema = NULL;
  TSKEY     rowKey = 0;
  TSKEY     fKey = 0;
  bool      isRowDel = false;
  int       filterIter = 0;
  SDataRow  row = NULL;
  SMergeInfo mInfo;

  if (pMergeInfo == NULL) pMergeInfo = &mInfo;

  memset(pMergeInfo, 0, sizeof(*pMergeInfo));
  pMergeInfo->keyFirst = INT64_MAX;
  pMergeInfo->keyLast = INT64_MIN;
  if (pCols) tdResetDataCols(pCols);

  row = tsdbNextIterRow(pIter);
  if (row == NULL || dataRowKey(row) > maxKey) {
    rowKey = INT64_MAX;
    isRowDel = false;
  } else {
    rowKey = dataRowKey(row);
    isRowDel = dataRowDeleted(row);
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
      if (row == NULL || dataRowKey(row) > maxKey) {
        rowKey = INT64_MAX;
        isRowDel = false;
      } else {
        rowKey = dataRowKey(row);
        isRowDel = dataRowDeleted(row);
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
      if (row == NULL || dataRowKey(row) > maxKey) {
        rowKey = INT64_MAX;
        isRowDel = false;
      } else {
        rowKey = dataRowKey(row);
        isRowDel = dataRowDeleted(row);
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
    goto _err;
  }

  pTableData->uid = TABLE_UID(pTable);
  pTableData->keyFirst = INT64_MAX;
  pTableData->keyLast = 0;
  pTableData->numOfRows = 0;

  pTableData->pData =
      tSkipListCreate(TSDB_DATA_SKIPLIST_LEVEL, TSDB_DATA_TYPE_TIMESTAMP, TYPE_BYTES[TSDB_DATA_TYPE_TIMESTAMP],
                      tkeyComparFn, pCfg->update ? SL_UPDATE_DUP_KEY : SL_DISCARD_DUP_KEY, tsdbGetTsTupleKey);
  if (pTableData->pData == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  return pTableData;

_err:
  tsdbFreeTableData(pTableData);
  return NULL;
}

static void tsdbFreeTableData(STableData *pTableData) {
  if (pTableData) {
    tSkipListDestroy(pTableData->pData);
    free(pTableData);
  }
}

static char *tsdbGetTsTupleKey(const void *data) { return dataRowTuple((SDataRow)data); }

void tsdbGetFidKeyRange(int daysPerFile, int8_t precision, int fileId, TSKEY *minKey, TSKEY *maxKey) {
  *minKey = fileId * daysPerFile * tsMsPerDay[precision];
  *maxKey = *minKey + daysPerFile * tsMsPerDay[precision] - 1;
}

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

static int tsdbAppendTableRowToCols(STable *pTable, SDataCols *pCols, STSchema **ppSchema, SDataRow row) {
  if (pCols) {
    if (*ppSchema == NULL || schemaVersion(*ppSchema) != dataRowVersion(row)) {
      *ppSchema = tsdbGetTableSchemaImpl(pTable, false, false, dataRowVersion(row));
      if (*ppSchema == NULL) {
        ASSERT(false);
        return -1;
      }
    }

    tdAppendDataRowToDataCol(row, *ppSchema, pCols);
  }

  return 0;
}

static int tsdbInitSubmitBlkIter(SSubmitBlk *pBlock, SSubmitBlkIter *pIter) {
  if (pBlock->dataLen <= 0) return -1;
  pIter->totalLen = pBlock->dataLen;
  pIter->len = 0;
  pIter->row = (SDataRow)(pBlock->data+pBlock->schemaLen);
  return 0;
}

static SDataRow tsdbGetSubmitBlkNext(SSubmitBlkIter *pIter) {
  SDataRow row = pIter->row;
  if (row == NULL) return NULL;

  pIter->len += dataRowLen(row);
  if (pIter->len >= pIter->totalLen) {
    pIter->row = NULL;
  } else {
    pIter->row = (char *)row + dataRowLen(row);
  }

  return row;
}

static FORCE_INLINE int tsdbCheckRowRange(STsdbRepo *pRepo, STable *pTable, SDataRow row, TSKEY minKey, TSKEY maxKey,
                                          TSKEY now) {
  if (dataRowKey(row) < minKey || dataRowKey(row) > maxKey) {
    tsdbError("vgId:%d table %s tid %d uid %" PRIu64 " timestamp is out of range! now %" PRId64 " minKey %" PRId64
              " maxKey %" PRId64 " row key %" PRId64,
              REPO_ID(pRepo), TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TABLE_UID(pTable), now, minKey, maxKey,
              dataRowKey(row));
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
  SDataRow       row = NULL;
  TSKEY          now = taosGetTimestamp(pRepo->config.precision);
  TSKEY          minKey = now - tsMsPerDay[pRepo->config.precision] * pRepo->config.keep;
  TSKEY          maxKey = now + tsMsPerDay[pRepo->config.precision] * pRepo->config.daysPerFile;

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

static int tsdbInsertDataToTable(STsdbRepo *pRepo, SSubmitBlk *pBlock, int32_t *affectedrows) {
  STsdbMeta *    pMeta = pRepo->tsdbMeta;
  int64_t        points = 0;
  STable *       pTable = NULL;
  SSubmitBlkIter blkIter = {0};
  SDataRow       row = NULL;
  void **        rows = NULL;
  int            rowCounter = 0;

  ASSERT(pBlock->tid < pMeta->maxTables);
  pTable = pMeta->tables[pBlock->tid];
  ASSERT(pTable != NULL && TABLE_UID(pTable) == pBlock->uid);

  rows = (void **)calloc(pBlock->numOfRows, sizeof(void *));
  if (rows == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  tsdbInitSubmitBlkIter(pBlock, &blkIter);
  while ((row = tsdbGetSubmitBlkNext(&blkIter)) != NULL) {
    if (tsdbCopyRowToMem(pRepo, row, pTable, &(rows[rowCounter])) < 0) {
      tsdbFreeRows(pRepo, rows, rowCounter);
      goto _err;
    }

    (*affectedrows)++;
    points++;

    if (rows[rowCounter] != NULL) {
      rowCounter++;
    }
  }

  if (tsdbInsertDataToTableImpl(pRepo, pTable, rows, rowCounter) < 0) {
    goto _err;
  }

  STSchema *pSchema = tsdbGetTableSchemaByVersion(pTable, pBlock->sversion);
  pRepo->stat.pointsWritten += points * schemaNCols(pSchema);
  pRepo->stat.totalStorage += points * schemaVLen(pSchema);

  free(rows);
  return 0;

_err:
  free(rows);
  return -1;
}

static int tsdbCopyRowToMem(STsdbRepo *pRepo, SDataRow row, STable *pTable, void **ppRow) {
  STsdbCfg *  pCfg = &pRepo->config;
  TKEY        tkey = dataRowTKey(row);
  TSKEY       key = dataRowKey(row);
  bool        isRowDelete = TKEY_IS_DELETED(tkey);

  if (isRowDelete) {
    if (!pCfg->update) {
      tsdbWarn("vgId:%d vnode is not allowed to update but try to delete a data row", REPO_ID(pRepo));
      terrno = TSDB_CODE_TDB_INVALID_ACTION;
      return -1;
    }

    if (key > TABLE_LASTKEY(pTable)) {
      tsdbTrace("vgId:%d skip to delete row key %" PRId64 " which is larger than table lastKey %" PRId64,
                REPO_ID(pRepo), key, TABLE_LASTKEY(pTable));
      return 0;
    }
  }

  void *pRow = tsdbAllocBytes(pRepo, dataRowLen(row));
  if (pRow == NULL) {
    tsdbError("vgId:%d failed to insert row with key %" PRId64 " to table %s while allocate %d bytes since %s",
              REPO_ID(pRepo), key, TABLE_CHAR_NAME(pTable), dataRowLen(row), tstrerror(terrno));
    return -1;
  }

  dataRowCpy(pRow, row);
  ppRow[0] = pRow;

  tsdbTrace("vgId:%d a row is %s table %s tid %d uid %" PRIu64 " key %" PRIu64, REPO_ID(pRepo),
            isRowDelete ? "deleted from" : "updated in", TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TABLE_UID(pTable),
            key);

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

  STSchema *pSchema = tsdbGetTableSchemaImpl(pTable, false, false, -1);
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
    if (tsdbGetTableSchemaImpl(pTable, false, false, pBlock->sversion) == NULL) {
      tsdbError("vgId:%d invalid submit schema version %d to table %s tid %d from client", REPO_ID(pRepo),
                pBlock->sversion, TABLE_CHAR_NAME(pTable), TABLE_TID(pTable));
    }
    terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
    return -1;
  }

  return 0;
}

static int tsdbInsertDataToTableImpl(STsdbRepo *pRepo, STable *pTable, void **rows, int rowCounter) {
  if (rowCounter < 1) return 0;

  SMemTable * pMemTable = NULL;
  STableData *pTableData = NULL;
  STsdbMeta * pMeta = pRepo->tsdbMeta;
  STsdbCfg *  pCfg = &(pRepo->config);

  ASSERT(pRepo->mem != NULL);
  pMemTable = pRepo->mem;

  if (TABLE_TID(pTable) >= pMemTable->maxTables) {
    if (tsdbAdjustMemMaxTables(pMemTable, pMeta->maxTables) < 0) {
      tsdbFreeRows(pRepo, rows, rowCounter);
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
      tsdbFreeRows(pRepo, rows, rowCounter);
      return -1;
    }

    pRepo->mem->tData[TABLE_TID(pTable)] = pTableData;
  }

  ASSERT((pTableData != NULL) && pTableData->uid == TABLE_UID(pTable));

  int64_t osize = SL_SIZE(pTableData->pData);
  tSkipListPutBatch(pTableData->pData, rows, rowCounter);
  int64_t dsize = SL_SIZE(pTableData->pData) - osize;

  if (pMemTable->keyFirst > dataRowKey(rows[0])) pMemTable->keyFirst = dataRowKey(rows[0]);
  if (pMemTable->keyLast < dataRowKey(rows[rowCounter - 1])) pMemTable->keyLast = dataRowKey(rows[rowCounter - 1]);
  pMemTable->numOfRows += dsize;

  if (pTableData->keyFirst > dataRowKey(rows[0])) pTableData->keyFirst = dataRowKey(rows[0]);
  if (pTableData->keyLast < dataRowKey(rows[rowCounter - 1])) pTableData->keyLast = dataRowKey(rows[rowCounter - 1]);
  pTableData->numOfRows += dsize;

  // TODO: impl delete row thing
  if (TABLE_LASTKEY(pTable) < dataRowKey(rows[rowCounter-1])) TABLE_LASTKEY(pTable) = dataRowKey(rows[rowCounter-1]);

  return 0;
}

static void tsdbFreeRows(STsdbRepo *pRepo, void **rows, int rowCounter) {
  ASSERT(pRepo->mem != NULL);
  STsdbBufPool *pBufPool = pRepo->pPool;

  for (int i = rowCounter - 1; i >= 0; --i) {
    SDataRow row = (SDataRow)rows[i];
    int      bytes = (int)dataRowLen(row);

    if (pRepo->mem->extraBuffList == NULL) {
      STsdbBufBlock *pBufBlock = tsdbGetCurrBufBlock(pRepo);
      ASSERT(pBufBlock != NULL && pBufBlock->offset >= bytes);

      pBufBlock->offset -= bytes;
      pBufBlock->remain += bytes;
      ASSERT(row == POINTER_SHIFT(pBufBlock->data, pBufBlock->offset));
      tsdbTrace("vgId:%d free %d bytes to TSDB buffer pool, nBlocks %d offset %d remain %d", REPO_ID(pRepo), bytes,
                listNEles(pRepo->mem->bufBlockList), pBufBlock->offset, pBufBlock->remain);

      if (pBufBlock->offset == 0) {  // return the block to buffer pool
        tsdbLockRepo(pRepo);
        SListNode *pNode = tdListPopTail(pRepo->mem->bufBlockList);
        tdListPrependNode(pBufPool->bufBlockList, pNode);
        tsdbUnlockRepo(pRepo);
      }
    } else {
      ASSERT(listNEles(pRepo->mem->extraBuffList) > 0);
      SListNode *pNode = tdListPopTail(pRepo->mem->extraBuffList);
      ASSERT(row == pNode->data);
      free(pNode);
      tsdbTrace("vgId:%d free %d bytes to SYSTEM buffer pool", REPO_ID(pRepo), bytes);

      if (listNEles(pRepo->mem->extraBuffList) == 0) {
        tdListFree(pRepo->mem->extraBuffList);
        pRepo->mem->extraBuffList = NULL;
      }
    }
  }
}