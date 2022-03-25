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

#include "tsdbDef.h"

static int      tsdbScanAndConvertSubmitMsg(STsdb *pTsdb, SSubmitReq *pMsg);
static int      tsdbMemTableInsertTbData(STsdb *pRepo, SSubmitBlk *pBlock, int32_t *pAffectedRows);
static STbData *tsdbNewTbData(tb_uid_t uid);
static void     tsdbFreeTbData(STbData *pTbData);
static char *   tsdbGetTsTupleKey(const void *data);
static int      tsdbTbDataComp(const void *arg1, const void *arg2);
static char *   tsdbTbDataGetUid(const void *arg);
static int      tsdbAppendTableRowToCols(STable *pTable, SDataCols *pCols, STSchema **ppSchema, STSRow *row);

STsdbMemTable *tsdbNewMemTable(STsdb *pTsdb) {
  STsdbMemTable *pMemTable = (STsdbMemTable *)taosMemoryCalloc(1, sizeof(*pMemTable));
  if (pMemTable == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  T_REF_INIT_VAL(pMemTable, 1);
  taosInitRWLatch(&(pMemTable->latch));
  pMemTable->keyMax = TSKEY_MIN;
  pMemTable->keyMin = TSKEY_MAX;
  pMemTable->nRow = 0;
  pMemTable->pMA = pTsdb->pmaf->create(pTsdb->pmaf);
  if (pMemTable->pMA == NULL) {
    taosMemoryFree(pMemTable);
    return NULL;
  }

  // Initialize the container
  pMemTable->pSlIdx =
      tSkipListCreate(5, TSDB_DATA_TYPE_BIGINT, sizeof(tb_uid_t), tsdbTbDataComp, SL_DISCARD_DUP_KEY, tsdbTbDataGetUid);
  if (pMemTable->pSlIdx == NULL) {
    pTsdb->pmaf->destroy(pTsdb->pmaf, pMemTable->pMA);
    taosMemoryFree(pMemTable);
    return NULL;
  }

  pMemTable->pHashIdx = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pMemTable->pHashIdx == NULL) {
    pTsdb->pmaf->destroy(pTsdb->pmaf, pMemTable->pMA);
    tSkipListDestroy(pMemTable->pSlIdx);
    taosMemoryFree(pMemTable);
    return NULL;
  }

  return pMemTable;
}

void tsdbFreeMemTable(STsdb *pTsdb, STsdbMemTable *pMemTable) {
  if (pMemTable) {
    taosHashCleanup(pMemTable->pHashIdx);
    tSkipListDestroy(pMemTable->pSlIdx);
    if (pMemTable->pMA) {
      pTsdb->pmaf->destroy(pTsdb->pmaf, pMemTable->pMA);
    }
    taosMemoryFree(pMemTable);
  }
}

int tsdbMemTableInsert(STsdb *pTsdb, STsdbMemTable *pMemTable, SSubmitReq *pMsg, SSubmitRsp *pRsp) {
  SSubmitBlk *   pBlock = NULL;
  SSubmitMsgIter msgIter = {0};
  int32_t        affectedrows = 0, numOfRows = 0;

  if (tsdbScanAndConvertSubmitMsg(pTsdb, pMsg) < 0) {
    if (terrno != TSDB_CODE_TDB_TABLE_RECONFIGURE) {
      tsdbError("vgId:%d failed to insert data since %s", REPO_ID(pTsdb), tstrerror(terrno));
    }
    return -1;
  }

  tInitSubmitMsgIter(pMsg, &msgIter);
  while (true) {
    tGetSubmitMsgNext(&msgIter, &pBlock);
    if (pBlock == NULL) break;
    if (tsdbMemTableInsertTbData(pTsdb, pBlock, &affectedrows) < 0) {
      return -1;
    }

    numOfRows += pBlock->numOfRows;
  }

  if (pRsp != NULL) {
    pRsp->affectedRows = htonl(affectedrows);
    pRsp->numOfRows = htonl(numOfRows);
  }

  return 0;
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
  STSRow *   row = NULL;
  SMergeInfo mInfo;

  if (pMergeInfo == NULL) pMergeInfo = &mInfo;

  memset(pMergeInfo, 0, sizeof(*pMergeInfo));
  pMergeInfo->keyFirst = INT64_MAX;
  pMergeInfo->keyLast = INT64_MIN;
  if (pCols) tdResetDataCols(pCols);

  row = tsdbNextIterRow(pIter);
  if (row == NULL || TD_ROW_KEY(row) > maxKey) {
    rowKey = INT64_MAX;
    isRowDel = false;
  } else {
    rowKey = TD_ROW_KEY(row);
    isRowDel = TD_ROW_IS_DELETED(row);
  }

  if (filterIter >= nFilterKeys) {
    fKey = INT64_MAX;
  } else {
    fKey = tdGetKey(filterKeys[filterIter]);
  }

  while (true) {
    if (fKey == INT64_MAX && rowKey == INT64_MAX) break;

    if (fKey < rowKey) {
      pMergeInfo->keyFirst = TMIN(pMergeInfo->keyFirst, fKey);
      pMergeInfo->keyLast = TMAX(pMergeInfo->keyLast, fKey);

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
        pMergeInfo->keyFirst = TMIN(pMergeInfo->keyFirst, rowKey);
        pMergeInfo->keyLast = TMAX(pMergeInfo->keyLast, rowKey);
        tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row);
      }

      tSkipListIterNext(pIter);
      row = tsdbNextIterRow(pIter);
      if (row == NULL || TD_ROW_KEY(row) > maxKey) {
        rowKey = INT64_MAX;
        isRowDel = false;
      } else {
        rowKey = TD_ROW_KEY(row);
        isRowDel = TD_ROW_IS_DELETED(row);
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
          pMergeInfo->keyFirst = TMIN(pMergeInfo->keyFirst, rowKey);
          pMergeInfo->keyLast = TMAX(pMergeInfo->keyLast, rowKey);
          tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row);
        } else {
          pMergeInfo->keyFirst = TMIN(pMergeInfo->keyFirst, fKey);
          pMergeInfo->keyLast = TMAX(pMergeInfo->keyLast, fKey);
        }
      }

      tSkipListIterNext(pIter);
      row = tsdbNextIterRow(pIter);
      if (row == NULL || TD_ROW_KEY(row) > maxKey) {
        rowKey = INT64_MAX;
        isRowDel = false;
      } else {
        rowKey = TD_ROW_KEY(row);
        isRowDel = TD_ROW_IS_DELETED(row);
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

static int tsdbScanAndConvertSubmitMsg(STsdb *pTsdb, SSubmitReq *pMsg) {
  ASSERT(pMsg != NULL);
  // STsdbMeta *    pMeta = pTsdb->tsdbMeta;
  SSubmitMsgIter msgIter = {0};
  SSubmitBlk *   pBlock = NULL;
  SSubmitBlkIter blkIter = {0};
  STSRow *       row = NULL;
  TSKEY          now = taosGetTimestamp(pTsdb->config.precision);
  TSKEY          minKey = now - tsTickPerDay[pTsdb->config.precision] * pTsdb->config.keep;
  TSKEY          maxKey = now + tsTickPerDay[pTsdb->config.precision] * pTsdb->config.daysPerFile;

  terrno = TSDB_CODE_SUCCESS;
  pMsg->length = htonl(pMsg->length);
  pMsg->numOfBlocks = htonl(pMsg->numOfBlocks);

  if (tInitSubmitMsgIter(pMsg, &msgIter) < 0) return -1;
  while (true) {
    if (tGetSubmitMsgNext(&msgIter, &pBlock) < 0) return -1;
    if (pBlock == NULL) break;

    pBlock->uid = htobe64(pBlock->uid);
    pBlock->suid = htobe64(pBlock->suid);
    pBlock->sversion = htonl(pBlock->sversion);
    pBlock->dataLen = htonl(pBlock->dataLen);
    pBlock->schemaLen = htonl(pBlock->schemaLen);
    pBlock->numOfRows = htons(pBlock->numOfRows);

#if 0
    if (pBlock->tid <= 0 || pBlock->tid >= pMeta->maxTables) {
      tsdbError("vgId:%d failed to get table to insert data, uid %" PRIu64 " tid %d", REPO_ID(pTsdb), pBlock->uid,
                pBlock->tid);
      terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
      return -1;
    }

    STable *pTable = pMeta->tables[pBlock->tid];
    if (pTable == NULL || TABLE_UID(pTable) != pBlock->uid) {
      tsdbError("vgId:%d failed to get table to insert data, uid %" PRIu64 " tid %d", REPO_ID(pTsdb), pBlock->uid,
                pBlock->tid);
      terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
      return -1;
    }

    if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
      tsdbError("vgId:%d invalid action trying to insert a super table %s", REPO_ID(pTsdb), TABLE_CHAR_NAME(pTable));
      terrno = TSDB_CODE_TDB_INVALID_ACTION;
      return -1;
    }

    // Check schema version and update schema if needed
    if (tsdbCheckTableSchema(pTsdb, pBlock, pTable) < 0) {
      if (terrno == TSDB_CODE_TDB_TABLE_RECONFIGURE) {
        continue;
      } else {
        return -1;
      }
    }

    tsdbInitSubmitBlkIter(pBlock, &blkIter);
    while ((row = tsdbGetSubmitBlkNext(&blkIter)) != NULL) {
      if (tsdbCheckRowRange(pTsdb, pTable, row, minKey, maxKey, now) < 0) {
        return -1;
      }
    }
#endif
  }

  if (terrno != TSDB_CODE_SUCCESS) return -1;
  return 0;
}

static int tsdbMemTableInsertTbData(STsdb *pTsdb, SSubmitBlk *pBlock, int32_t *pAffectedRows) {
  // STsdbMeta       *pMeta = pRepo->tsdbMeta;
  // int32_t          points = 0;
  // STable          *pTable = NULL;
  SSubmitBlkIter blkIter = {0};
  STsdbMemTable *pMemTable = pTsdb->mem;
  void *         tptr;
  STbData *      pTbData;
  STSRow *       row;
  TSKEY          keyMin;
  TSKEY          keyMax;

  // SMemTable       *pMemTable = NULL;
  // STableData      *pTableData = NULL;
  // STsdbCfg        *pCfg = &(pRepo->config);

  tptr = taosHashGet(pMemTable->pHashIdx, &(pBlock->uid), sizeof(pBlock->uid));
  if (tptr == NULL) {
    pTbData = tsdbNewTbData(pBlock->uid);
    if (pTbData == NULL) {
      return -1;
    }

    // Put into hash
    taosHashPut(pMemTable->pHashIdx, &(pBlock->uid), sizeof(pBlock->uid), &(pTbData), sizeof(pTbData));

    // Put into skiplist
    tSkipListPut(pMemTable->pSlIdx, pTbData);
  } else {
    pTbData = *(STbData **)tptr;
  }

  tInitSubmitBlkIter(pBlock, &blkIter);
  if (blkIter.row == NULL) return 0;
  keyMin = TD_ROW_KEY(blkIter.row);

  tSkipListPutBatchByIter(pTbData->pData, &blkIter, (iter_next_fn_t)tGetSubmitBlkNext);

  // Set statistics
  keyMax = TD_ROW_KEY(blkIter.row);

  pTbData->nrows += pBlock->numOfRows;
  if (pTbData->keyMin > keyMin) pTbData->keyMin = keyMin;
  if (pTbData->keyMax < keyMax) pTbData->keyMax = keyMax;

  pMemTable->nRow += pBlock->numOfRows;
  if (pMemTable->keyMin > keyMin) pMemTable->keyMin = keyMin;
  if (pMemTable->keyMax < keyMax) pMemTable->keyMax = keyMax;

  // STSRow* lastRow = NULL;
  // int64_t osize = SL_SIZE(pTableData->pData);
  // tsdbSetupSkipListHookFns(pTableData->pData, pRepo, pTable, &points, &lastRow);
  // tSkipListPutBatchByIter(pTableData->pData, &blkIter, (iter_next_fn_t)tsdbGetSubmitBlkNext);
  // int64_t dsize = SL_SIZE(pTableData->pData) - osize;
  // (*pAffectedRows) += points;

  // if(lastRow != NULL) {
  //   TSKEY lastRowKey = TD_ROW_KEY(lastRow);
  //   if (pMemTable->keyFirst > firstRowKey) pMemTable->keyFirst = firstRowKey;
  //   pMemTable->numOfRows += dsize;

  //   if (pTableData->keyFirst > firstRowKey) pTableData->keyFirst = firstRowKey;
  //   pTableData->numOfRows += dsize;
  //   if (pMemTable->keyLast < lastRowKey) pMemTable->keyLast = lastRowKey;
  //   if (pTableData->keyLast < lastRowKey) pTableData->keyLast = lastRowKey;
  //   if (tsdbUpdateTableLatestInfo(pRepo, pTable, lastRow) < 0) {
  //     return -1;
  //   }
  // }

  // STSchema *pSchema = tsdbGetTableSchemaByVersion(pTable, pBlock->sversion, -1);
  // pRepo->stat.pointsWritten += points * schemaNCols(pSchema);
  // pRepo->stat.totalStorage += points * schemaVLen(pSchema);

  return 0;
}

static STbData *tsdbNewTbData(tb_uid_t uid) {
  STbData *pTbData = (STbData *)taosMemoryCalloc(1, sizeof(*pTbData));
  if (pTbData == NULL) {
    return NULL;
  }

  pTbData->uid = uid;
  pTbData->keyMin = TSKEY_MAX;
  pTbData->keyMax = TSKEY_MIN;
  pTbData->nrows = 0;

  // uint8_t skipListCreateFlags;
  // if (pCfg->update == TD_ROW_DISCARD_UPDATE)
  //   skipListCreateFlags = SL_DISCARD_DUP_KEY;
  // else
  //   skipListCreateFlags = SL_UPDATE_DUP_KEY;

  // pTableData->pData =
  //     tSkipListCreate(TSDB_DATA_SKIPLIST_LEVEL, TSDB_DATA_TYPE_TIMESTAMP, TYPE_BYTES[TSDB_DATA_TYPE_TIMESTAMP],
  //                     tkeyComparFn, skipListCreateFlags, tsdbGetTsTupleKey);
  // if (pTableData->pData == NULL) {
  //   terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
  //   taosMemoryFree(pTableData);
  //   return NULL;
  // }

  pTbData->pData = tSkipListCreate(5, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), tkeyComparFn, SL_DISCARD_DUP_KEY,
                                   tsdbGetTsTupleKey);
  if (pTbData->pData == NULL) {
    taosMemoryFree(pTbData);
    return NULL;
  }

  return pTbData;
}

static void tsdbFreeTbData(STbData *pTbData) {
  if (pTbData) {
    tSkipListDestroy(pTbData->pData);
    taosMemoryFree(pTbData);
  }
}

static char *tsdbGetTsTupleKey(const void *data) { return (char *)TD_ROW_KEY_ADDR((STSRow *)data); }

static int tsdbTbDataComp(const void *arg1, const void *arg2) {
  STbData *pTbData1 = (STbData *)arg1;
  STbData *pTbData2 = (STbData *)arg2;

  if (pTbData1->uid > pTbData2->uid) {
    return 1;
  } else if (pTbData1->uid == pTbData2->uid) {
    return 0;
  } else {
    return -1;
  }
}

static char *tsdbTbDataGetUid(const void *arg) {
  STbData *pTbData = (STbData *)arg;
  return (char *)(&(pTbData->uid));
}
static int tsdbAppendTableRowToCols(STable *pTable, SDataCols *pCols, STSchema **ppSchema, STSRow *row) {
  if (pCols) {
    if (*ppSchema == NULL || schemaVersion(*ppSchema) != TD_ROW_SVER(row)) {
      *ppSchema = tsdbGetTableSchemaImpl(pTable, false, false, TD_ROW_SVER(row));
      if (*ppSchema == NULL) {
        ASSERT(false);
        return -1;
      }
    }

    tdAppendSTSRowToDataCol(row, *ppSchema, pCols, true);
  }

  return 0;
}

/* ------------------------ REFACTORING ------------------------ */
#if 0
int tsdbInsertDataToMemTable(STsdbMemTable *pMemTable, SSubmitReq *pMsg) {
  SMemAllocator *pMA = pMemTable->pMA;
  STbData *      pTbData = (STbData *)TD_MA_MALLOC(pMA, sizeof(*pTbData));
  if (pTbData == NULL) {
    // TODO
  }

  TD_SLIST_PUSH(&(pMemTable->list), pTbData);

  return 0;
}

#include "tdataformat.h"
#include "tfunctional.h"
#include "tsdbRowMergeBuf.h"
#include "tsdbint.h"
#include "tskiplist.h"

#define TSDB_DATA_SKIPLIST_LEVEL 5
#define TSDB_MAX_INSERT_BATCH 512

typedef struct {
  int32_t  totalLen;
  int32_t  len;
  STSRow*  row;
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
static int          tsdbAdjustMemMaxTables(SMemTable *pMemTable, int maxTables);
static int          tsdbAppendTableRowToCols(STable *pTable, SDataCols *pCols, STSchema **ppSchema, STSRow* row);
static int          tsdbInitSubmitBlkIter(SSubmitBlk *pBlock, SSubmitBlkIter *pIter);
static STSRow*      tsdbGetSubmitBlkNext(SSubmitBlkIter *pIter);
static int          tsdbScanAndConvertSubmitMsg(STsdbRepo *pRepo, SSubmitReq *pMsg);
static int          tsdbInsertDataToTable(STsdbRepo *pRepo, SSubmitBlk *pBlock, int32_t *affectedrows);
static int          tsdbInitSubmitMsgIter(SSubmitReq *pMsg, SSubmitMsgIter *pIter);
static int          tsdbGetSubmitMsgNext(SSubmitMsgIter *pIter, SSubmitBlk **pPBlock);
static int          tsdbCheckTableSchema(STsdbRepo *pRepo, SSubmitBlk *pBlock, STable *pTable);
static int          tsdbUpdateTableLatestInfo(STsdbRepo *pRepo, STable *pTable, STSRow* row);

static FORCE_INLINE int tsdbCheckRowRange(STsdbRepo *pRepo, STable *pTable, STSRow* row, TSKEY minKey, TSKEY maxKey,
                                          TSKEY now);


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
      int code = taosThreadCondSignal(&pBufPool->poolNotEmpty);
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

    pSnapshot->mem->tData = (STableData **)taosMemoryCalloc(pSnapshot->omem->maxTables, sizeof(STableData *));
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
    taosMemoryFreeClear(pSnapshot->mem->tData);

    tsdbUnRefMemTable(pRepo, pSnapshot->omem);
  }

  tsdbUnRefMemTable(pRepo, pSnapshot->imem);

  pSnapshot->mem = NULL;
  pSnapshot->imem = NULL;
  pSnapshot->omem = NULL;
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
  STSRow*    row = NULL;
  SMergeInfo mInfo;

  if (pMergeInfo == NULL) pMergeInfo = &mInfo;

  memset(pMergeInfo, 0, sizeof(*pMergeInfo));
  pMergeInfo->keyFirst = INT64_MAX;
  pMergeInfo->keyLast = INT64_MIN;
  if (pCols) tdResetDataCols(pCols);

  row = tsdbNextIterRow(pIter);
  if (row == NULL || TD_ROW_KEY(row) > maxKey) {
    rowKey = INT64_MAX;
    isRowDel = false;
  } else {
    rowKey = TD_ROW_KEY(row);
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
      pMergeInfo->keyFirst = TMIN(pMergeInfo->keyFirst, fKey);
      pMergeInfo->keyLast = TMAX(pMergeInfo->keyLast, fKey);

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
        pMergeInfo->keyFirst = TMIN(pMergeInfo->keyFirst, rowKey);
        pMergeInfo->keyLast = TMAX(pMergeInfo->keyLast, rowKey);
        tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row);
      }

      tSkipListIterNext(pIter);
      row = tsdbNextIterRow(pIter);
      if (row == NULL || TD_ROW_KEY(row) > maxKey) {
        rowKey = INT64_MAX;
        isRowDel = false;
      } else {
        rowKey = TD_ROW_KEY(row);
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
          pMergeInfo->keyFirst = TMIN(pMergeInfo->keyFirst, rowKey);
          pMergeInfo->keyLast = TMAX(pMergeInfo->keyLast, rowKey);
          tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row);
        } else {
          pMergeInfo->keyFirst = TMIN(pMergeInfo->keyFirst, fKey);
          pMergeInfo->keyLast = TMAX(pMergeInfo->keyLast, fKey);
        }
      }

      tSkipListIterNext(pIter);
      row = tsdbNextIterRow(pIter);
      if (row == NULL || TD_ROW_KEY(row) > maxKey) {
        rowKey = INT64_MAX;
        isRowDel = false;
      } else {
        rowKey = TD_ROW_KEY(row);
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

static FORCE_INLINE int tsdbCheckRowRange(STsdbRepo *pRepo, STable *pTable, STSRow* row, TSKEY minKey, TSKEY maxKey,
                                          TSKEY now) {
  TSKEY rowKey = TD_ROW_KEY(row);
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


//row1 has higher priority
static STSRow* tsdbInsertDupKeyMerge(STSRow* row1, STSRow* row2, STsdbRepo* pRepo,
                                     STSchema **ppSchema1, STSchema **ppSchema2,
                                     STable* pTable, int32_t* pPoints, STSRow** pLastRow) {

  //for compatiblity, duplicate key inserted when update=0 should be also calculated as affected rows!
  if(row1 == NULL && row2 == NULL && pRepo->config.update == TD_ROW_DISCARD_UPDATE) {
    (*pPoints)++;
    return NULL;
  }

  tsdbTrace("vgId:%d a row is %s table %s tid %d uid %" PRIu64 " key %" PRIu64, REPO_ID(pRepo),
            "updated in", TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TABLE_UID(pTable),
            TD_ROW_KEY(row1));

  if(row2 == NULL || pRepo->config.update != TD_ROW_PARTIAL_UPDATE) {
    void* pMem = tsdbAllocBytes(pRepo, TD_ROW_LEN(row1));
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

  STSRow* tmp = tsdbMergeTwoRows(pBuf, row1, row2, pSchema1, pSchema2);

  void* pMem = tsdbAllocBytes(pRepo, TD_ROW_LEN(tmp));
  if(pMem == NULL) return NULL;
  memRowCpy(pMem, tmp);

  (*pPoints)++;
  *pLastRow = pMem;
  return pMem;
}

static void* tsdbInsertDupKeyMergePacked(void** args) {
  return tsdbInsertDupKeyMerge(args[0], args[1], args[2], (STSchema**)&args[3], (STSchema**)&args[4], args[5], args[6], args[7]);
}

static void tsdbSetupSkipListHookFns(SSkipList* pSkipList, STsdbRepo *pRepo, STable *pTable, int32_t* pPoints, STSRow** pLastRow) {

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

static void updateTableLatestColumn(STsdbRepo *pRepo, STable *pTable, STSRow* row) {
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
      pDataCol->pData = taosMemoryMalloc(pTCol->bytes);
      pDataCol->bytes = pTCol->bytes;
    } else if (pDataCol->bytes < pTCol->bytes) {
      pDataCol->pData = taosMemoryRealloc(pDataCol->pData, pTCol->bytes);
      pDataCol->bytes = pTCol->bytes;
    }
    // the actual value size
    uint16_t bytes = IS_VAR_DATA_TYPE(pTCol->type) ? varDataTLen(value) : pTCol->bytes;
    // the actual data size CANNOT larger than column size
    assert(pTCol->bytes >= bytes);
    memcpy(pDataCol->pData, value, bytes);
    //tsdbInfo("updateTableLatestColumn vgId:%d cache column %d for %d,%s", REPO_ID(pRepo), j, pDataCol->bytes, (char*)pDataCol->pData);
    pDataCol->ts = TD_ROW_KEY(row);
    // unlock
    TSDB_WUNLOCK_TABLE(pTable); 
  }
}

static int tsdbUpdateTableLatestInfo(STsdbRepo *pRepo, STable *pTable, STSRow* row) {
  STsdbCfg *pCfg = &pRepo->config;

  // if cacheLastRow config has been reset, free the lastRow
  if (!pCfg->cacheLastRow && pTable->lastRow != NULL) {
    STSRow* cachedLastRow = pTable->lastRow;
    TSDB_WLOCK_TABLE(pTable);
    pTable->lastRow = NULL;
    TSDB_WUNLOCK_TABLE(pTable);
    taosTZfree(cachedLastRow);
  }

  if (tsdbGetTableLastKeyImpl(pTable) <= TD_ROW_KEY(row)) {
    if (CACHE_LAST_ROW(pCfg) || pTable->lastRow != NULL) {
      STSRow* nrow = pTable->lastRow;
      if (taosTSizeof(nrow) < TD_ROW_LEN(row)) {
        STSRow* orow = nrow;
        nrow = taosTMalloc(TD_ROW_LEN(row));
        if (nrow == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          return -1;
        }

        memRowCpy(nrow, row);
        TSDB_WLOCK_TABLE(pTable);
        pTable->lastKey = TD_ROW_KEY(row);
        pTable->lastRow = nrow;
        TSDB_WUNLOCK_TABLE(pTable);
        taosTZfree(orow);
      } else {
        TSDB_WLOCK_TABLE(pTable);
        pTable->lastKey = TD_ROW_KEY(row);
        memRowCpy(nrow, row);
        TSDB_WUNLOCK_TABLE(pTable);
      }
    } else {
      pTable->lastKey = TD_ROW_KEY(row);
    }

    if (CACHE_LAST_NULL_COLUMN(pCfg)) {
      updateTableLatestColumn(pRepo, pTable, row);
    }
  }

  pTable->cacheLastConfigVersion = pRepo->cacheLastConfigVersion;

  return 0;
}

#endif