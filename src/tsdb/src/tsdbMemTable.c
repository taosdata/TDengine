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

static void        tsdbFreeBytes(STsdbRepo *pRepo, void *ptr, int bytes);
static SMemTable * tsdbNewMemTable(STsdbRepo *pRepo);
static void        tsdbFreeMemTable(SMemTable *pMemTable);
static STableData *tsdbNewTableData(STsdbCfg *pCfg, STable *pTable);
static void        tsdbFreeTableData(STableData *pTableData);
static char *      tsdbGetTsTupleKey(const void *data);
static void *      tsdbCommitData(void *arg);
static int         tsdbCommitMeta(STsdbRepo *pRepo);
static void        tsdbEndCommit(STsdbRepo *pRepo);
static int         tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey);
static int tsdbCommitToFile(STsdbRepo *pRepo, int fid, SCommitIter *iters, SRWHelper *pHelper, SDataCols *pDataCols);
static SCommitIter *tsdbCreateCommitIters(STsdbRepo *pRepo);
static void         tsdbDestroyCommitIters(SCommitIter *iters, int maxTables);
static int          tsdbAdjustMemMaxTables(SMemTable *pMemTable, int maxTables);

// ---------------- INTERNAL FUNCTIONS ----------------
int tsdbInsertRowToMem(STsdbRepo *pRepo, SDataRow row, STable *pTable) {
  STsdbCfg *  pCfg = &pRepo->config;
  STsdbMeta * pMeta = pRepo->tsdbMeta;
  int32_t     level = 0;
  int32_t     headSize = 0;
  TSKEY       key = dataRowKey(row);
  SMemTable * pMemTable = pRepo->mem;
  STableData *pTableData = NULL;
  SSkipList * pSList = NULL;

  if (pMemTable != NULL && TABLE_TID(pTable) < pMemTable->maxTables && pMemTable->tData[TABLE_TID(pTable)] != NULL &&
      pMemTable->tData[TABLE_TID(pTable)]->uid == TABLE_UID(pTable)) {
    pTableData = pMemTable->tData[TABLE_TID(pTable)];
    pSList = pTableData->pData;
  }

  tSkipListNewNodeInfo(pSList, &level, &headSize);

  SSkipListNode *pNode = (SSkipListNode *)malloc(headSize + sizeof(SDataRow *));
  if (pNode == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  void *pRow = tsdbAllocBytes(pRepo, dataRowLen(row));
  if (pRow == NULL) {
    tsdbError("vgId:%d failed to insert row with key %" PRId64 " to table %s while allocate %d bytes since %s",
              REPO_ID(pRepo), key, TABLE_CHAR_NAME(pTable), dataRowLen(row), tstrerror(terrno));
    free(pNode);
    return -1;
  }

  pNode->level = level;
  dataRowCpy(pRow, row);
  *(SDataRow *)SL_GET_NODE_DATA(pNode) = pRow;

  // Operations above may change pRepo->mem, retake those values
  ASSERT(pRepo->mem != NULL);
  pMemTable = pRepo->mem;

  if (TABLE_TID(pTable) >= pMemTable->maxTables) {
    if (tsdbAdjustMemMaxTables(pMemTable, pMeta->maxTables) < 0) {
      tsdbFreeBytes(pRepo, pRow, dataRowLen(row));
      free(pNode);
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
      tsdbError("vgId:%d failed to insert row with key %" PRId64
                " to table %s while create new table data object since %s",
                REPO_ID(pRepo), key, TABLE_CHAR_NAME(pTable), tstrerror(terrno));
      tsdbFreeBytes(pRepo, (void *)pRow, dataRowLen(row));
      free(pNode);
      return -1;
    }

    pRepo->mem->tData[TABLE_TID(pTable)] = pTableData;
  }

  ASSERT((pTableData != NULL) && pTableData->uid == TABLE_UID(pTable));

  if (tSkipListPut(pTableData->pData, pNode) == NULL) {
    tsdbFreeBytes(pRepo, (void *)pRow, dataRowLen(row));
    free(pNode);
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
      tsdbUnlockRepo(pRepo);
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
  if (pMem != NULL) {
    taosRUnLockLatch(&(pMem->latch));
    tsdbUnRefMemTable(pRepo, pMem);
  }

  if (pIMem != NULL) {
    tsdbUnRefMemTable(pRepo, pIMem);
  }

  tsdbDebug("vgId:%d untake memory snapshot, pMem %p pIMem %p", REPO_ID(pRepo), pMem, pIMem);
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
  SMemTable *pIMem = pRepo->imem;
  int        code = 0;

  if (pIMem != NULL) {
    ASSERT(pRepo->commit);
    tsdbDebug("vgId:%d waiting for the commit thread", REPO_ID(pRepo));
    code = pthread_join(pRepo->commitThread, NULL);
    tsdbDebug("vgId:%d commit thread is finished", REPO_ID(pRepo));
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

int tsdbLoadDataFromCache(STable *pTable, SSkipListIterator *pIter, TSKEY maxKey, int maxRowsToRead, SDataCols *pCols,
                          TSKEY *filterKeys, int nFilterKeys) {
  ASSERT(maxRowsToRead > 0 && nFilterKeys >= 0);
  if (pIter == NULL) return 0;
  STSchema *pSchema = NULL;
  int       numOfRows = 0;
  TSKEY     keyNext = 0;
  int       filterIter = 0;

  if (nFilterKeys != 0) { // for filter purpose
    ASSERT(filterKeys != NULL);
    keyNext = tsdbNextIterKey(pIter);
    if (keyNext < 0 || keyNext > maxKey) return numOfRows;
    void *ptr = taosbsearch((void *)(&keyNext), (void *)filterKeys, nFilterKeys, sizeof(TSKEY), compTSKEY, TD_GE);
    filterIter = (ptr == NULL) ? nFilterKeys : (int)((POINTER_DISTANCE(ptr, filterKeys) / sizeof(TSKEY)));
  }

  do {
    SDataRow row = tsdbNextIterRow(pIter);
    if (row == NULL) break;

    keyNext = dataRowKey(row);
    if (keyNext > maxKey) break;

    bool keyFiltered = false;
    if (nFilterKeys != 0) {
      while (true) {
        if (filterIter >= nFilterKeys) break;
        if (keyNext == filterKeys[filterIter]) {
          keyFiltered = true;
          filterIter++;
          break;
        } else if (keyNext < filterKeys[filterIter]) {
          break;
        } else {
          filterIter++;
        }
      }
    }

    if (!keyFiltered) {
      if (numOfRows >= maxRowsToRead) break;
      if (pCols) {
        if (pSchema == NULL || schemaVersion(pSchema) != dataRowVersion(row)) {
          pSchema = tsdbGetTableSchemaImpl(pTable, false, false, dataRowVersion(row));
          if (pSchema == NULL) {
            ASSERT(0);
          }
        }

        tdAppendDataRowToDataCol(row, pSchema, pCols);
      }
      numOfRows++;
    }
  } while (tSkipListIterNext(pIter));

  return numOfRows;
}

// ---------------- LOCAL FUNCTIONS ----------------
static void tsdbFreeBytes(STsdbRepo *pRepo, void *ptr, int bytes) {
  ASSERT(pRepo->mem != NULL);
  if (pRepo->mem->extraBuffList == NULL) {
    STsdbBufBlock *pBufBlock = tsdbGetCurrBufBlock(pRepo);
    ASSERT(pBufBlock != NULL);
    pBufBlock->offset -= bytes;
    pBufBlock->remain += bytes;
    ASSERT(ptr == POINTER_SHIFT(pBufBlock->data, pBufBlock->offset));
    tsdbTrace("vgId:%d free %d bytes to TSDB buffer pool, nBlocks %d offset %d remain %d", REPO_ID(pRepo), bytes,
              listNEles(pRepo->mem->bufBlockList), pBufBlock->offset, pBufBlock->remain);
  } else {
    SListNode *pNode = (SListNode *)POINTER_SHIFT(ptr, -(int)(sizeof(SListNode)));
    ASSERT(listTail(pRepo->mem->extraBuffList) == pNode);
    tdListPopNode(pRepo->mem->extraBuffList, pNode);
    free(pNode);
    tsdbTrace("vgId:%d free %d bytes to SYSTEM buffer pool", REPO_ID(pRepo), bytes);
  }
}

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
    taosTFree(pMemTable->tData);
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
                                      TYPE_BYTES[TSDB_DATA_TYPE_TIMESTAMP], 0, 0, 1, tsdbGetTsTupleKey);
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

static char *tsdbGetTsTupleKey(const void *data) { return dataRowTuple(*(SDataRow *)data); }

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
    iters = tsdbCreateCommitIters(pRepo);
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

    int sfid = (int)(TSDB_KEY_FILEID(pMem->keyFirst, pCfg->daysPerFile, pCfg->precision));
    int efid = (int)(TSDB_KEY_FILEID(pMem->keyLast, pCfg->daysPerFile, pCfg->precision));

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
  tsdbDestroyCommitIters(iters, pMem->maxTables);
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

static int tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey) {
  for (int i = 0; i < nIters; i++) {
    TSKEY nextKey = tsdbNextIterKey((iters + i)->pIter);
    if (nextKey > 0 && (nextKey >= minKey && nextKey <= maxKey)) return 1;
  }
  return 0;
}

void tsdbGetFidKeyRange(int daysPerFile, int8_t precision, int fileId, TSKEY *minKey, TSKEY *maxKey) {
  *minKey = fileId * daysPerFile * tsMsPerDay[precision];
  *maxKey = *minKey + daysPerFile * tsMsPerDay[precision] - 1;
}

static int tsdbCommitToFile(STsdbRepo *pRepo, int fid, SCommitIter *iters, SRWHelper *pHelper, SDataCols *pDataCols) {
  char *      dataDir = NULL;
  STsdbCfg *  pCfg = &pRepo->config;
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pGroup = NULL;
  SMemTable * pMem = pRepo->imem;
  bool        newLast = false;

  TSKEY minKey = 0, maxKey = 0;
  tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, fid, &minKey, &maxKey);

  // Check if there are data to commit to this file
  int hasDataToCommit = tsdbHasDataToCommit(iters, pMem->maxTables, minKey, maxKey);
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

  if ((pGroup = tsdbCreateFGroupIfNeed(pRepo, dataDir, fid)) == NULL) {
    tsdbError("vgId:%d failed to create file group %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
    goto _err;
  }

  // Open files for write/read
  if (tsdbSetAndOpenHelperFile(pHelper, pGroup) < 0) {
    tsdbError("vgId:%d failed to set helper file since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  newLast = TSDB_NLAST_FILE_OPENED(pHelper);

  if (tsdbLoadCompIdx(pHelper, NULL) < 0) {
    tsdbError("vgId:%d failed to load SCompIdx part since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  // Loop to commit data in each table
  for (int tid = 1; tid < pMem->maxTables; tid++) {
    SCommitIter *pIter = iters + tid;
    if (pIter->pTable == NULL) continue;

    taosRLockLatch(&(pIter->pTable->latch));

    if (tsdbSetHelperTable(pHelper, pIter->pTable, pRepo) < 0) goto _err;

    if (pIter->pIter != NULL) {
      if (tdInitDataCols(pDataCols, tsdbGetTableSchemaImpl(pIter->pTable, false, false, -1)) < 0) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }

      if (tsdbCommitTableData(pHelper, pIter, pDataCols, maxKey) < 0) {
        taosRUnLockLatch(&(pIter->pTable->latch));
        tsdbError("vgId:%d failed to write data of table %s tid %d uid %" PRIu64 " since %s", REPO_ID(pRepo),
                  TABLE_CHAR_NAME(pIter->pTable), TABLE_TID(pIter->pTable), TABLE_UID(pIter->pTable),
                  tstrerror(terrno));
        goto _err;
      }
    }

    taosRUnLockLatch(&(pIter->pTable->latch));

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

  taosTFree(dataDir);
  tsdbCloseHelperFile(pHelper, 0, pGroup);

  pthread_rwlock_wrlock(&(pFileH->fhlock));

  (void)rename(helperNewHeadF(pHelper)->fname, helperHeadF(pHelper)->fname);
  pGroup->files[TSDB_FILE_TYPE_HEAD].info = helperNewHeadF(pHelper)->info;

  if (newLast) {
    (void)rename(helperNewLastF(pHelper)->fname, helperLastF(pHelper)->fname);
    pGroup->files[TSDB_FILE_TYPE_LAST].info = helperNewLastF(pHelper)->info;
  } else {
    pGroup->files[TSDB_FILE_TYPE_LAST].info = helperLastF(pHelper)->info;
  }

  pGroup->files[TSDB_FILE_TYPE_DATA].info = helperDataF(pHelper)->info;

  pthread_rwlock_unlock(&(pFileH->fhlock));

  return 0;

_err:
  taosTFree(dataDir);
  tsdbCloseHelperFile(pHelper, 1, NULL);
  return -1;
}

static SCommitIter *tsdbCreateCommitIters(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  SCommitIter *iters = (SCommitIter *)calloc(pMem->maxTables, sizeof(SCommitIter));
  if (iters == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  if (tsdbRLockRepoMeta(pRepo) < 0) goto _err;

  // reference all tables
  for (int i = 0; i < pMem->maxTables; i++) {
    if (pMeta->tables[i] != NULL) {
      tsdbRefTable(pMeta->tables[i]);
      iters[i].pTable = pMeta->tables[i];
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) goto _err;

  for (int i = 0; i < pMem->maxTables; i++) {
    if ((iters[i].pTable != NULL) && (pMem->tData[i] != NULL) && (TABLE_UID(iters[i].pTable) == pMem->tData[i]->uid)) {
      if ((iters[i].pIter = tSkipListCreateIter(pMem->tData[i]->pData)) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }

      tSkipListIterNext(iters[i].pIter);
    }
  }

  return iters;

_err:
  tsdbDestroyCommitIters(iters, pMem->maxTables);
  return NULL;
}

static void tsdbDestroyCommitIters(SCommitIter *iters, int maxTables) {
  if (iters == NULL) return;

  for (int i = 1; i < maxTables; i++) {
    if (iters[i].pTable != NULL) {
      tsdbUnRefTable(iters[i].pTable);
      tSkipListDestroyIter(iters[i].pIter);
    }
  }

  free(iters);
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

  taosTFree(tData);

  return 0;
}