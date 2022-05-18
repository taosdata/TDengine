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

static STbData *tsdbNewTbData(tb_uid_t uid);
static void     tsdbFreeTbData(STbData *pTbData);
static char    *tsdbGetTsTupleKey(const void *data);
static int      tsdbTbDataComp(const void *arg1, const void *arg2);
static char    *tsdbTbDataGetUid(const void *arg);
static int tsdbAppendTableRowToCols(STable *pTable, SDataCols *pCols, STSchema **ppSchema, STSRow *row, bool merge);

int tsdbMemTableCreate(STsdb *pTsdb, STsdbMemTable **ppMemTable) {
  STsdbMemTable *pMemTable;
  SVnode        *pVnode;

  *ppMemTable = NULL;
  pVnode = pTsdb->pVnode;

  // alloc handle
  pMemTable = (STsdbMemTable *)taosMemoryCalloc(1, sizeof(*pMemTable));
  if (pMemTable == NULL) {
    return -1;
  }

  pMemTable->pPool = pTsdb->pVnode->inUse;
  T_REF_INIT_VAL(pMemTable, 1);
  taosInitRWLatch(&pMemTable->latch);
  pMemTable->keyMin = TSKEY_MAX;
  pMemTable->keyMax = TSKEY_MIN;
  pMemTable->nRow = 0;
  pMemTable->pSlIdx = tSkipListCreate(pVnode->config.tsdbCfg.slLevel, TSDB_DATA_TYPE_BIGINT, sizeof(tb_uid_t),
                                      tsdbTbDataComp, SL_DISCARD_DUP_KEY, tsdbTbDataGetUid);
  if (pMemTable->pSlIdx == NULL) {
    taosMemoryFree(pMemTable);
    return -1;
  }

  pMemTable->pHashIdx = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pMemTable->pHashIdx == NULL) {
    tSkipListDestroy(pMemTable->pSlIdx);
    taosMemoryFree(pMemTable);
    return -1;
  }

  *ppMemTable = pMemTable;
  return 0;
}

void tsdbMemTableDestroy(STsdb *pTsdb, STsdbMemTable *pMemTable) {
  if (pMemTable) {
    taosHashCleanup(pMemTable->pHashIdx);
    SSkipListIterator *pIter = tSkipListCreateIter(pMemTable->pSlIdx);
    SSkipListNode     *pNode = NULL;
    STbData           *pTbData = NULL;
    for (;;) {
      if (!tSkipListIterNext(pIter)) break;
      pNode = tSkipListIterGet(pIter);
      pTbData = (STbData *)pNode->pData;
      tsdbFreeTbData(pTbData);
    }
    tSkipListDestroyIter(pIter);
    tSkipListDestroy(pMemTable->pSlIdx);
    taosMemoryFree(pMemTable);
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
  STSchema *pSchema = NULL;
  TSKEY     rowKey = 0;
  TSKEY     fKey = 0;
  // only fetch lastKey from mem data as file data not used in this function actually
  TSKEY      lastKey = TSKEY_INITIAL_VAL;
  bool       isRowDel = false;
  int        filterIter = 0;
  STSRow    *row = NULL;
  SMergeInfo mInfo;

  // TODO: support Multi-Version(the rows with the same TS keys in memory can't be merged if its version refered by
  // query handle)

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
  // 1. fkey - no dup since merged up to maxVersion of each query handle by tsdbLoadBlockDataCols
  // 2. rowKey - would dup since Multi-Version supported
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
#if 0
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
#endif
#if 1
    } else if (fKey > rowKey) {
      if (isRowDel) {
        // TODO: support delete function
        pMergeInfo->rowsDeleteFailed++;
      } else {
        if (pMergeInfo->rowsInserted - pMergeInfo->rowsDeleteSucceed >= maxRowsToRead) break;
        if (pCols && pMergeInfo->nOperations >= pCols->maxPoints) break;

        if (lastKey != rowKey) {
          pMergeInfo->rowsInserted++;
          pMergeInfo->nOperations++;
          pMergeInfo->keyFirst = TMIN(pMergeInfo->keyFirst, rowKey);
          pMergeInfo->keyLast = TMAX(pMergeInfo->keyLast, rowKey);
          if (pCols) {
            if (lastKey != TSKEY_INITIAL_VAL) {
              ++pCols->numOfRows;
            }
            tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row, false);
          }
          lastKey = rowKey;
        } else {
          if (keepDup) {
            tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row, true);
          } else {
            // discard
          }
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
    } else {           // fkey == rowKey
      if (isRowDel) {  // TODO: support delete function(How to stands for delete in file? rowVersion = -1?)
        ASSERT(!keepDup);
        if (pCols && pMergeInfo->nOperations >= pCols->maxPoints) break;
        pMergeInfo->rowsDeleteSucceed++;
        pMergeInfo->nOperations++;
        tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row, false);
      } else {
        if (keepDup) {
          if (pCols && pMergeInfo->nOperations >= pCols->maxPoints) break;
          if (lastKey != rowKey) {
            pMergeInfo->rowsUpdated++;
            pMergeInfo->nOperations++;
            pMergeInfo->keyFirst = TMIN(pMergeInfo->keyFirst, rowKey);
            pMergeInfo->keyLast = TMAX(pMergeInfo->keyLast, rowKey);
            if (pCols) {
              if (lastKey != TSKEY_INITIAL_VAL) {
                ++pCols->numOfRows;
              }
              tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row, false);
            }
            lastKey = rowKey;
          } else {
            tsdbAppendTableRowToCols(pTable, pCols, &pSchema, row, true);
          }
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
#endif
  }
  if (pCols && (lastKey != TSKEY_INITIAL_VAL)) {
    ++pCols->numOfRows;
  }

  return 0;
}

int tsdbInsertTableData(STsdb *pTsdb, SSubmitMsgIter *pMsgIter, SSubmitBlk *pBlock, SSubmitBlkRsp *pRsp) {
  SSubmitBlkIter blkIter = {0};
  STsdbMemTable *pMemTable = pTsdb->mem;
  void          *tptr;
  STbData       *pTbData;
  STSRow        *row;
  TSKEY          keyMin;
  TSKEY          keyMax;
  SSubmitBlk    *pBlkCopy;
  int64_t        sverNew;

  // check if table exists
  SMetaReader mr = {0};
  SMetaEntry  me = {0};
  metaReaderInit(&mr, pTsdb->pVnode->pMeta, 0);
  if (metaGetTableEntryByUid(&mr, pMsgIter->uid) < 0) {
    metaReaderClear(&mr);
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    return -1;
  }
  if (mr.me.type == TSDB_NORMAL_TABLE) {
    sverNew = mr.me.ntbEntry.schema.sver;
  } else {
    metaGetTableEntryByUid(&mr, mr.me.ctbEntry.suid);
    sverNew = mr.me.stbEntry.schema.sver;
  }
  metaReaderClear(&mr);

  // create container is nedd
  tptr = taosHashGet(pMemTable->pHashIdx, &(pMsgIter->uid), sizeof(pMsgIter->uid));
  if (tptr == NULL) {
    pTbData = tsdbNewTbData(pMsgIter->uid);
    if (pTbData == NULL) {
      return -1;
    }

    // Put into hash
    taosHashPut(pMemTable->pHashIdx, &(pMsgIter->uid), sizeof(pMsgIter->uid), &(pTbData), sizeof(pTbData));

    // Put into skiplist
    tSkipListPut(pMemTable->pSlIdx, pTbData);
  } else {
    pTbData = *(STbData **)tptr;
  }

  // copy data to buffer pool
  int32_t tlen = pMsgIter->dataLen + pMsgIter->schemaLen + sizeof(*pBlock);
  pBlkCopy = (SSubmitBlk *)vnodeBufPoolMalloc(pTsdb->mem->pPool, tlen);
  memcpy(pBlkCopy, pBlock, tlen);

  tInitSubmitBlkIter(pMsgIter, pBlkCopy, &blkIter);
  if (blkIter.row == NULL) return 0;
  keyMin = TD_ROW_KEY(blkIter.row);

  tSkipListPutBatchByIter(pTbData->pData, &blkIter, (iter_next_fn_t)tGetSubmitBlkNext);

#ifdef TD_DEBUG_PRINT_ROW
  printf("!!! %s:%d table %" PRIi64 " has %d rows in skiplist\n\n", __func__, __LINE__, pTbData->uid,
         SL_SIZE(pTbData->pData));
#endif

  // Set statistics
  keyMax = TD_ROW_KEY(blkIter.row);

  pTbData->nrows += pMsgIter->numOfRows;
  if (pTbData->keyMin > keyMin) pTbData->keyMin = keyMin;
  if (pTbData->keyMax < keyMax) pTbData->keyMax = keyMax;

  pMemTable->nRow += pMsgIter->numOfRows;
  if (pMemTable->keyMin > keyMin) pMemTable->keyMin = keyMin;
  if (pMemTable->keyMax < keyMax) pMemTable->keyMax = keyMax;

  pRsp->numOfRows = pMsgIter->numOfRows;
  pRsp->affectedRows = pMsgIter->numOfRows;
  pRsp->sver = sverNew;

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
#if 0
  pTbData->pData = tSkipListCreate(5, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), tkeyComparFn, SL_DISCARD_DUP_KEY,
                                   tsdbGetTsTupleKey);
#endif
  pTbData->pData =
      tSkipListCreate(5, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), tkeyComparFn, SL_ALLOW_DUP_KEY, tsdbGetTsTupleKey);
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
static int tsdbAppendTableRowToCols(STable *pTable, SDataCols *pCols, STSchema **ppSchema, STSRow *row, bool merge) {
  if (pCols) {
    if (*ppSchema == NULL || schemaVersion(*ppSchema) != TD_ROW_SVER(row)) {
      *ppSchema = tsdbGetTableSchemaImpl(pTable, false, false, TD_ROW_SVER(row));
      if (*ppSchema == NULL) {
        ASSERT(false);
        return -1;
      }
    }

    tdAppendSTSRowToDataCol(row, *ppSchema, pCols, merge);
  }

  return 0;
}