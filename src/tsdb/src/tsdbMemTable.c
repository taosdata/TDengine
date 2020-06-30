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

typedef struct {
  STable *           pTable;
  SSkipListIterator *pIter;
} SCommitIter;

static FORCE_INLINE STsdbBufBlock *tsdbGetCurrBufBlock(STsdbRepo *pRepo);

static void        tsdbFreeBytes(STsdbRepo *pRepo, void *ptr, int bytes);
static SMemTable * tsdbNewMemTable(STsdbCfg *pCfg);
static void        tsdbFreeMemTable(SMemTable *pMemTable);
static STableData *tsdbNewTableData(STsdbCfg *pCfg, STable *pTable);
static void        tsdbFreeTableData(STableData *pTableData);
static char *      tsdbGetTsTupleKey(const void *data);
static void *      tsdbCommitData(void *arg);
static int         tsdbCommitMeta(STsdbRepo *pRepo);
static void        tsdbEndCommit(STsdbRepo *pRepo);
static TSKEY       tsdbNextIterKey(SCommitIter *pIter);
static int         tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey);
static int  tsdbCommitToFile(STsdbRepo *pRepo, int fid, SCommitIter *iters, SRWHelper *pHelper, SDataCols *pDataCols);
static void tsdbGetFidKeyRange(int daysPerFile, int8_t precision, int fileId, TSKEY *minKey, TSKEY *maxKey);
static SCommitIter *tsdbCreateTableIters(STsdbRepo *pRepo);
static void         tsdbDestroyTableIters(SCommitIter *iters, int maxTables);
static int          tsdbReadRowsFromCache(STsdbMeta *pMeta, STable *pTable, SSkipListIterator *pIter, TSKEY maxKey,
                                          int maxRowsToRead, SDataCols *pCols);

// ---------------- INTERNAL FUNCTIONS ----------------
int tsdbInsertRowToMem(STsdbRepo *pRepo, SDataRow row, STable *pTable) {
  STsdbCfg *  pCfg = &pRepo->config;
  int32_t     level = 0;
  int32_t     headSize = 0;
  TSKEY       key = dataRowKey(row);
  SMemTable * pMemTable = pRepo->mem;
  STableData *pTableData = NULL;
  SSkipList * pSList = NULL;
  int         bytes = 0;

  if (pMemTable != NULL && pMemTable->tData[TABLE_TID(pTable)] != NULL &&
      pMemTable->tData[TABLE_TID(pTable)]->uid == TABLE_UID(pTable)) {
    pTableData = pMemTable->tData[TABLE_TID(pTable)];
    pSList = pTableData->pData;
  }

  tSkipListNewNodeInfo(pSList, &level, &headSize);

  bytes = headSize + dataRowLen(row);
  SSkipListNode *pNode = tsdbAllocBytes(pRepo, bytes);
  if (pNode == NULL) {
    tsdbError("vgId:%d failed to insert row with key %" PRId64 " to table %s while allocate %d bytes since %s",
              REPO_ID(pRepo), key, TABLE_CHAR_NAME(pTable), bytes, tstrerror(terrno));
    return -1;
  }
  pNode->level = level;
  dataRowCpy(SL_GET_NODE_DATA(pNode), row);

  // Operations above may change pRepo->mem, retake those values
  ASSERT(pRepo->mem != NULL);
  pMemTable = pRepo->mem;
  pTableData = pMemTable->tData[TABLE_TID(pTable)];

  if (pTableData == NULL || pTableData->uid != TABLE_UID(pTable)) {
    if (pTableData != NULL) {  // destroy the table skiplist (may have race condition problem)
      pMemTable->tData[TABLE_TID(pTable)] = NULL;
      tsdbFreeTableData(pTableData);
    }
    pTableData = tsdbNewTableData(pCfg, pTable);
    if (pTableData == NULL) {
      tsdbError("vgId:%d failed to insert row with key %" PRId64
                " to table %s while create new table data object since %s",
                REPO_ID(pRepo), key, TABLE_CHAR_NAME(pTable), tstrerror(terrno));
      tsdbFreeBytes(pRepo, (void *)pNode, bytes);
      return -1;
    }

    pRepo->mem->tData[TABLE_TID(pTable)] = pTableData;
  }

  ASSERT((pTableData != NULL) && pTableData->uid == TABLE_UID(pTable));

  if (tSkipListPut(pTableData->pData, pNode) == NULL) {
    tsdbFreeBytes(pRepo, (void *)pNode, bytes);
  } else {
    if (TABLE_LASTKEY(pTable) < key) TABLE_LASTKEY(pTable) = key;
    if (pMemTable->keyFirst > key) pMemTable->keyFirst = key;
    if (pMemTable->keyLast < key) pMemTable->keyLast = key;
    pMemTable->numOfRows++;

    if (pTableData->keyFirst > key) pTableData->keyFirst = key;
    if (pTableData->keyLast < key) pTableData->keyLast = key;
    pTableData->numOfRows++;

    ASSERT(pTableData->numOfRows == tSkipListGetSize(pTableData->pData));
  }

  tsdbTrace("vgId:%d a row is inserted to table %s tid %d uid %" PRIu64 " key %" PRIu64, REPO_ID(pRepo),
            TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TABLE_UID(pTable), key);

  return 0;
}

int tsdbRefMemTable(STsdbRepo *pRepo, SMemTable *pMemTable) {
  if (pMemTable == NULL) return 0;
  T_REF_INC(pMemTable);
  return 0;
}

// Need to lock the repository
int tsdbUnRefMemTable(STsdbRepo *pRepo, SMemTable *pMemTable) {
  if (pMemTable == NULL) return 0;

  if (T_REF_DEC(pMemTable) == 0) {
    STsdbCfg *    pCfg = &pRepo->config;
    STsdbBufPool *pBufPool = pRepo->pPool;

    SListNode *pNode = NULL;
    if (tsdbLockRepo(pRepo) < 0) return -1;
    while ((pNode = tdListPopHead(pMemTable->bufBlockList)) != NULL) {
      tdListAppendNode(pBufPool->bufBlockList, pNode);
    }
    int code = pthread_cond_signal(&pBufPool->poolNotEmpty);
    if (code != 0) {
      tsdbUnlockRepo(pRepo);
      tsdbError("vgId:%d failed to signal pool not empty since %s", REPO_ID(pRepo), strerror(code));
      terrno = TAOS_SYSTEM_ERROR(code);
      return -1;
    }
    if (tsdbUnlockRepo(pRepo) < 0) return -1;

    for (int i = 0; i < pCfg->maxTables; i++) {
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

  return 0;
}

void *tsdbAllocBytes(STsdbRepo *pRepo, int bytes) {
  STsdbCfg *     pCfg = &pRepo->config;
  STsdbBufBlock *pBufBlock = tsdbGetCurrBufBlock(pRepo);

  if (pBufBlock != NULL && pBufBlock->remain < bytes) {
    if (listNEles(pRepo->mem->bufBlockList) >= pCfg->totalBlocks / 2) {  // need to commit mem
      if (tsdbAsyncCommit(pRepo) < 0) return NULL;
    } else {
      if (tsdbLockRepo(pRepo) < 0) return NULL;
      SListNode *pNode = tsdbAllocBufBlockFromPool(pRepo);
      tdListAppendNode(pRepo->mem->bufBlockList, pNode);
      if (tsdbUnlockRepo(pRepo) < 0) return NULL;
    }
  }

  if (pRepo->mem == NULL) {
    SMemTable *pMemTable = tsdbNewMemTable(&pRepo->config);
    if (pMemTable == NULL) return NULL;

    if (tsdbLockRepo(pRepo) < 0) {
      tsdbFreeMemTable(pMemTable);
      return NULL;
    }

    SListNode *pNode = tsdbAllocBufBlockFromPool(pRepo);
    tdListAppendNode(pMemTable->bufBlockList, pNode);
    pRepo->mem = pMemTable;

    if (tsdbUnlockRepo(pRepo) < 0) return NULL;
  }

  pBufBlock = tsdbGetCurrBufBlock(pRepo);
  ASSERT(pBufBlock->remain >= bytes);
  void *ptr = POINTER_SHIFT(pBufBlock->data, pBufBlock->offset);
  pBufBlock->offset += bytes;
  pBufBlock->remain -= bytes;

  return ptr;
}

int tsdbAsyncCommit(STsdbRepo *pRepo) {
  SMemTable *pIMem = pRepo->imem;
  int        code = 0;

  if (pIMem != NULL) {
    ASSERT(pRepo->commit);
    code = pthread_join(pRepo->commitThread, NULL);
    if (code != 0) {
      tsdbError("vgId:%d failed to thread join since %s", REPO_ID(pRepo), strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
    pRepo->commit = 0;
  }

  ASSERT(pRepo->commit == 0);
  if (pRepo->mem != NULL) {
    if (pRepo->appH.notifyStatus) pRepo->appH.notifyStatus(pRepo->appH.appH, TSDB_STATUS_COMMIT_START);
    if (tsdbLockRepo(pRepo) < 0) return -1;
    pRepo->imem = pRepo->mem;
    pRepo->mem = NULL;
    pRepo->commit = 1;
    code = pthread_create(&pRepo->commitThread, NULL, tsdbCommitData, (void *)pRepo);
    if (code != 0) {
      tsdbError("vgId:%d failed to create commit thread since %s", REPO_ID(pRepo), strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(code);
      tsdbUnlockRepo(pRepo);
      return -1;
    }
    if (tsdbUnlockRepo(pRepo) < 0) return -1;
  }

  if (pIMem && tsdbUnRefMemTable(pRepo, pIMem) < 0) return -1;

  return 0;
}

// ---------------- LOCAL FUNCTIONS ----------------
static FORCE_INLINE STsdbBufBlock *tsdbGetCurrBufBlock(STsdbRepo *pRepo) {
  ASSERT(pRepo != NULL);
  if (pRepo->mem == NULL) return NULL;

  SListNode *pNode = listTail(pRepo->mem->bufBlockList);
  if (pNode == NULL) return NULL;

  STsdbBufBlock *pBufBlock = NULL;
  tdListNodeGetData(pRepo->mem->bufBlockList, pNode, (void *)(&pBufBlock));

  return pBufBlock;
}

static void tsdbFreeBytes(STsdbRepo *pRepo, void *ptr, int bytes) {
  STsdbBufBlock *pBufBlock = tsdbGetCurrBufBlock(pRepo);
  ASSERT(pBufBlock != NULL);
  pBufBlock->offset -= bytes;
  pBufBlock->remain += bytes;
  ASSERT(ptr == POINTER_SHIFT(pBufBlock->data, pBufBlock->offset));
}

static SMemTable* tsdbNewMemTable(STsdbCfg* pCfg) {
  SMemTable *pMemTable = (SMemTable *)calloc(1, sizeof(*pMemTable));
  if (pMemTable == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pMemTable->keyFirst = INT64_MAX;
  pMemTable->keyLast = 0;
  pMemTable->numOfRows = 0;

  pMemTable->tData = (STableData**)calloc(pCfg->maxTables, sizeof(STableData*));
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

  pTableData->pData = tSkipListCreate(TSDB_DATA_SKIPLIST_LEVEL, TSDB_DATA_TYPE_TIMESTAMP,
                                      TYPE_BYTES[TSDB_DATA_TYPE_TIMESTAMP], 0, 0, 0, tsdbGetTsTupleKey);
  if (pTableData->pData == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  // TODO: operation here should not be here, remove it
  pTableData->pData->level = 1;

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

static char *tsdbGetTsTupleKey(const void *data) { return dataRowTuple(data); }

static void *tsdbCommitData(void *arg) {
  STsdbRepo *  pRepo = (STsdbRepo *)arg;
  SMemTable *  pMem = pRepo->imem;
  STsdbCfg *   pCfg = &pRepo->config;
  SDataCols *  pDataCols = NULL;
  STsdbMeta *  pMeta = pRepo->tsdbMeta;
  SCommitIter *iters = NULL;
  SRWHelper    whelper = {0};
  ASSERT(pRepo->commit == 1);
  ASSERT(pMem != NULL);

  tsdbInfo("vgId:%d start to commit! keyFirst %" PRId64 " keyLast %" PRId64 " numOfRows %" PRId64, REPO_ID(pRepo),
            pMem->keyFirst, pMem->keyLast, pMem->numOfRows);

  // Create the iterator to read from cache
  if (pMem->numOfRows > 0) {
    iters = tsdbCreateTableIters(pRepo);
    if (iters == NULL) {
      tsdbError("vgId:%d failed to create commit iterator since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _exit;
    }

    if (tsdbInitWriteHelper(&whelper, pRepo) < 0) {
      tsdbError("vgId:%d failed to init write helper since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _exit;
    }

    if ((pDataCols = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pCfg->maxRowsPerFileBlock)) == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      tsdbError("vgId:%d failed to init data cols with maxRowBytes %d maxCols %d maxRowsPerFileBlock %d since %s",
                REPO_ID(pRepo), pMeta->maxCols, pMeta->maxRowBytes, pCfg->maxRowsPerFileBlock, tstrerror(terrno));
      goto _exit;
    }

    int sfid = TSDB_KEY_FILEID(pMem->keyFirst, pCfg->daysPerFile, pCfg->precision);
    int efid = TSDB_KEY_FILEID(pMem->keyLast, pCfg->daysPerFile, pCfg->precision);

    // Loop to commit to each file
    for (int fid = sfid; fid <= efid; fid++) {
      if (tsdbCommitToFile(pRepo, fid, iters, &whelper, pDataCols) < 0) {
        tsdbError("vgId:%d failed to commit to file %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
        goto _exit;
      }
    }
  }

  // Commit to update meta file
  if (tsdbCommitMeta(pRepo) < 0) {
    tsdbError("vgId:%d failed to commit data while committing meta data since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _exit;
  }

  tsdbFitRetention(pRepo);

_exit:
  tdFreeDataCols(pDataCols);
  tsdbDestroyTableIters(iters, pCfg->maxTables);
  tsdbDestroyHelper(&whelper);
  tsdbEndCommit(pRepo);
  tsdbInfo("vgId:%d commit over", pRepo->config.tsdbId);

  return NULL;
}

static int tsdbCommitMeta(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  SActObj *  pAct = NULL;
  SActCont * pCont = NULL;

  if (listNEles(pMem->actList) > 0) {
    if (tdKVStoreStartCommit(pMeta->pStore) < 0) {
      tsdbError("vgId:%d failed to commit data while start commit meta since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _err;
    }

    SListNode *pNode = NULL;

    while ((pNode = tdListPopHead(pMem->actList)) != NULL) {
      pAct = (SActObj *)pNode->data;
      if (pAct->act == TSDB_UPDATE_META) {
        pCont = (SActCont *)POINTER_SHIFT(pAct, sizeof(SActObj));
        if (tdUpdateKVStoreRecord(pMeta->pStore, pAct->uid, (void *)(pCont->cont), pCont->len) < 0) {
          tsdbError("vgId:%d failed to update meta with uid %" PRIu64 " since %s", REPO_ID(pRepo), pAct->uid,
                    tstrerror(terrno));
          tdKVStoreEndCommit(pMeta->pStore);
          goto _err;
        }
      } else if (pAct->act == TSDB_DROP_META) {
        if (tdDropKVStoreRecord(pMeta->pStore, pAct->uid) < 0) {
          tsdbError("vgId:%d failed to drop meta with uid %" PRIu64 " since %s", REPO_ID(pRepo), pAct->uid,
                    tstrerror(terrno));
          tdKVStoreEndCommit(pMeta->pStore);
          goto _err;
        }
      } else {
        ASSERT(false);
      }
    }

    if (tdKVStoreEndCommit(pMeta->pStore) < 0) {
      tsdbError("vgId:%d failed to commit data while end commit meta since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _err;
    }
  }

  return 0;

_err:
  return -1;
}

static void tsdbEndCommit(STsdbRepo *pRepo) {
  ASSERT(pRepo->commit == 1);
  if (pRepo->appH.notifyStatus) pRepo->appH.notifyStatus(pRepo->appH.appH, TSDB_STATUS_COMMIT_OVER);
}

static TSKEY tsdbNextIterKey(SCommitIter *pIter) {
  if (pIter == NULL) return -1;

  SSkipListNode *node = tSkipListIterGet(pIter->pIter);
  if (node == NULL) return -1;

  SDataRow row = SL_GET_NODE_DATA(node);
  return dataRowKey(row);
}

static int tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey) {
  for (int i = 0; i < nIters; i++) {
    TSKEY nextKey = tsdbNextIterKey(iters + i);
    if (nextKey > 0 && (nextKey >= minKey && nextKey <= maxKey)) return 1;
  }
  return 0;
}

static void tsdbGetFidKeyRange(int daysPerFile, int8_t precision, int fileId, TSKEY *minKey, TSKEY *maxKey) {
  *minKey = fileId * daysPerFile * tsMsPerDay[precision];
  *maxKey = *minKey + daysPerFile * tsMsPerDay[precision] - 1;
}

static int tsdbCommitToFile(STsdbRepo *pRepo, int fid, SCommitIter *iters, SRWHelper *pHelper, SDataCols *pDataCols) {
  char *      dataDir = NULL;
  STsdbMeta * pMeta = pRepo->tsdbMeta;
  STsdbCfg *  pCfg = &pRepo->config;
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pGroup = NULL;

  TSKEY minKey = 0, maxKey = 0;
  tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, fid, &minKey, &maxKey);

  // Check if there are data to commit to this file
  int hasDataToCommit = tsdbHasDataToCommit(iters, pCfg->maxTables, minKey, maxKey);
  if (!hasDataToCommit) {
    tsdbDebug("vgId:%d no data to commit to file %d", REPO_ID(pRepo), fid);
    return 0;
  }

  // Create and open files for commit
  dataDir = tsdbGetDataDirName(pRepo->rootDir);
  if (dataDir == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if ((pGroup = tsdbCreateFGroupIfNeed(pRepo, dataDir, fid, pCfg->maxTables)) == NULL) {
    tsdbError("vgId:%d failed to create file group %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
    goto _err;
  }

  // Open files for write/read
  if (tsdbSetAndOpenHelperFile(pHelper, pGroup) < 0) {
    tsdbError("vgId:%d failed to set helper file since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  // Loop to commit data in each table
  for (int tid = 1; tid < pCfg->maxTables; tid++) {
    SCommitIter *pIter = iters + tid;
    if (pIter->pTable == NULL) continue;

    tsdbSetHelperTable(pHelper, pIter->pTable, pRepo);

    if (pIter->pIter != NULL) {
      tdInitDataCols(pDataCols, tsdbGetTableSchema(pIter->pTable));

      int maxRowsToRead = pCfg->maxRowsPerFileBlock * 4 / 5;
      int nLoop = 0;
      while (true) {
        int rowsRead = tsdbReadRowsFromCache(pMeta, pIter->pTable, pIter->pIter, maxKey, maxRowsToRead, pDataCols);
        ASSERT(rowsRead >= 0);
        if (pDataCols->numOfRows == 0) break;
        nLoop++;

        ASSERT(dataColsKeyFirst(pDataCols) >= minKey && dataColsKeyFirst(pDataCols) <= maxKey);
        ASSERT(dataColsKeyLast(pDataCols) >= minKey && dataColsKeyLast(pDataCols) <= maxKey);

        int rowsWritten = tsdbWriteDataBlock(pHelper, pDataCols);
        ASSERT(rowsWritten != 0);
        if (rowsWritten < 0) {
          tsdbError("vgId:%d failed to write data block to table %s tid %d uid %" PRIu64 " since %s", REPO_ID(pRepo),
                    TABLE_CHAR_NAME(pIter->pTable), TABLE_TID(pIter->pTable), TABLE_UID(pIter->pTable),
                    tstrerror(terrno));
          goto _err;
        }
        ASSERT(rowsWritten <= pDataCols->numOfRows);

        tdPopDataColsPoints(pDataCols, rowsWritten);
        maxRowsToRead = pCfg->maxRowsPerFileBlock * 4 / 5 - pDataCols->numOfRows;
      }

      ASSERT(pDataCols->numOfRows == 0);
    }

    // Move the last block to the new .l file if neccessary
    if (tsdbMoveLastBlockIfNeccessary(pHelper) < 0) {
      tsdbError("vgId:%d, failed to move last block, since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _err;
    }

    // Write the SCompBlock part
    if (tsdbWriteCompInfo(pHelper) < 0) {
      tsdbError("vgId:%d, failed to write compInfo part since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _err;
    }
  }

  if (tsdbWriteCompIdx(pHelper) < 0) {
    tsdbError("vgId:%d failed to write compIdx part to file %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
    goto _err;
  }

  tfree(dataDir);
  tsdbCloseHelperFile(pHelper, 0);

  pthread_rwlock_wrlock(&(pFileH->fhlock));
  pGroup->files[TSDB_FILE_TYPE_HEAD] = pHelper->files.headF;
  pGroup->files[TSDB_FILE_TYPE_DATA] = pHelper->files.dataF;
  pGroup->files[TSDB_FILE_TYPE_LAST] = pHelper->files.lastF;
  pthread_rwlock_unlock(&(pFileH->fhlock));

  return 0;

_err:
  tfree(dataDir);
  tsdbCloseHelperFile(pHelper, 1);
  return -1;
}

static SCommitIter *tsdbCreateTableIters(STsdbRepo *pRepo) {
  STsdbCfg * pCfg = &(pRepo->config);
  SMemTable *pMem = pRepo->imem;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  SCommitIter *iters = (SCommitIter *)calloc(pCfg->maxTables, sizeof(SCommitIter));
  if (iters == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  if (tsdbRLockRepoMeta(pRepo) < 0) goto _err;

  // reference all tables
  for (int i = 0; i < pCfg->maxTables; i++) { 
    if (pMeta->tables[i] != NULL) {
      tsdbRefTable(pMeta->tables[i]);
      iters[i].pTable = pMeta->tables[i];
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) goto _err;

  for (int i = 0; i < pCfg->maxTables; i++) {
    if ((iters[i].pTable != NULL) && (pMem->tData[i] != NULL) && (TABLE_UID(iters[i].pTable) == pMem->tData[i]->uid)) {
      if ((iters[i].pIter = tSkipListCreateIter(pMem->tData[i]->pData)) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }

      if (!tSkipListIterNext(iters[i].pIter)) {
        terrno = TSDB_CODE_TDB_NO_TABLE_DATA_IN_MEM;
        goto _err;
      }
    }
  }

  return iters;

_err:
  tsdbDestroyTableIters(iters, pCfg->maxTables);
  return NULL;
}

static void tsdbDestroyTableIters(SCommitIter *iters, int maxTables) {
  if (iters == NULL) return;

  for (int i = 1; i < maxTables; i++) {
    if (iters[i].pTable != NULL) {
      tsdbUnRefTable(iters[i].pTable);
      tSkipListDestroyIter(iters[i].pIter);
    }
  }

  free(iters);
}

static int tsdbReadRowsFromCache(STsdbMeta *pMeta, STable *pTable, SSkipListIterator *pIter, TSKEY maxKey, int maxRowsToRead, SDataCols *pCols) {
  ASSERT(maxRowsToRead > 0);
  if (pIter == NULL) return 0;
  STSchema *pSchema = NULL;

  int numOfRows = 0;

  do {
    if (numOfRows >= maxRowsToRead) break;

    SSkipListNode *node = tSkipListIterGet(pIter);
    if (node == NULL) break;

    SDataRow row = SL_GET_NODE_DATA(node);
    if (dataRowKey(row) > maxKey) break;

    if (pSchema == NULL || schemaVersion(pSchema) != dataRowVersion(row)) {
      pSchema = tsdbGetTableSchemaByVersion(pTable, dataRowVersion(row));
      if (pSchema == NULL) {
        // TODO: deal with the error here
        ASSERT(false);
      }
    }

    tdAppendDataRowToDataCol(row, pSchema, pCols);
    numOfRows++;
  } while (tSkipListIterNext(pIter));

  return numOfRows;
}