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

// ---------------- INTERNAL FUNCTIONS ----------------
int tsdbInsertRowToMem(STsdbRepo *pRepo, SDataRow row, STable *pTable) {
  STsdbCfg *  pCfg = &pRepo->config;
  int32_t     level = 0;
  int32_t     headSize = 0;
  TSKEY       key = dataRowKey(row);
  SMemTable * pMemTable = pRepo->mem;
  STableData *pTableData = NULL;
  int         bytes = 0;

  if (pMemTable != NULL && pMemTable->tData[TABLE_TID(pTable)] != NULL &&
      pMemTable->tData[TABLE_TID(pTable)]->uid == TALBE_UID(pTable)) {
    pTableData = pMemTable->tData[TABLE_TID(pTable)];
  }

  tSkipListNewNodeInfo(pTableData, &level, &headSize);

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

  if (pTableData == NULL || pTableData->uid != TALBE_UID(pTable)) {
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

  ASSERT(pTableData != NULL) && pTableData->uid == TALBE_UID(pTable);

  if (tSkipListPut(pTableData->pData, pNode) == NULL) {
    tsdbFreeBytes(pRepo, (void *)pNode, bytes);
  } else {
    if (pMemTable->keyFirst > key) pMemTable->keyFirst = key;
    if (pMemTable->keyLast < key) pMemTable->keyLast = key;
    pMemTable->numOfRows++;

    if (pTableData->keyFirst > key) pTableData->keyFirst = key;
    if (pTableData->keyLast < key) pTableData->keyLast = key;
    pTableData->numOfRows++;

    ASSERT(pTableData->numOfRows == tSkipListGetSize(pTableData->pData));
  }

  tsdbTrace("vgId:%d a row is inserted to table %s tid %d uid %" PRIu64 " key %" PRIu64, REPO_ID(pRepo),
            TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TALBE_UID(pTable), key);

  return 0;
}

int tsdbRefMemTable(STsdbRepo *pRepo, SMemTable *pMemTable) {
  ASSERT(IS_REPO_LOCKED(pRepo));
  ASSERT(pMemTable != NULL);
  T_REF_INC(pMemTable);
}

// Need to lock the repository
int tsdbUnRefMemTable(STsdbRepo *pRepo, SMemTable *pMemTable) {
  ASSERT(pMemTable != NULL);

  if (T_REF_DEC(pMemTable) == 0) {
    STsdbCfg *    pCfg = &pRepo->config;
    STsdbBufPool *pBufPool = pRepo->pPool;

    SListNode *pNode = NULL;
    // TODO: check the correctness of this code part
    if (tsdbLockRepo(pRepo) < 0) return -1;
    while ((pNode = tdListPopHead(pMemTable->bufBlockList)) != NULL) {
      tdListAppendNode(pBufPool->bufBlockList, pNode);
      if (pthread_cond_signal(&pBufPool->poolNotEmpty) != 0) {
        // TODO
      }
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

// ---------------- LOCAL FUNCTIONS ----------------
static FORCE_INLINE STsdbBufBlock *tsdbGetCurrBufBlock(STsdbRepo *pRepo) {
  ASSERT(pRepo != NULL);
  if (pRepo->mem == NULL) return NULL;

  SListNode *pNode = listTail(pRepo->mem);
  if (pNode == NULL) return NULL;

  STsdbBufBlock *pBufBlock = NULL;
  tdListNodeGetData(pMemTable->bufBlockList, pNode, (void *)(&pBufBlock));

  return pBufBlock;
}

static void *tsdbAllocBytes(STsdbRepo *pRepo, int bytes) {
  STsdbCfg *     pCfg = &pRepo->config;
  STsdbBufBlock *pBufBlock = tsdbGetCurrBufBlock(pRepo);
  int            code = 0;

  if (pBufBlock != NULL && pBufBlock->remain < bytes) {
    if (listNEles(pRepo->mem) >= pCfg->totalBlocks / 2) {  // need to commit mem
      if (pRepo->imem) {
        code = pthread_join(pRepo->commitThread, NULL);
        if (code != 0) {
          tsdbError("vgId:%d failed to thread join since %s", REPO_ID(pRepo), strerror(errno));
          terrno = TAOS_SYSTEM_ERROR(errno);
          return NULL;
        }
      }

      ASSERT(pRepo->commit == 0);
      SMemTable *pImem = pRepo->imem;

      if (tsdbLockRepo(pRepo) < 0) return NULL;
      pRepo->imem = pRepo->mem;
      pRepo->mem = NULL;
      pRepo->commit = 1;
      code = pthread_create(&pRepo->commitThread, NULL, tsdbCommitData, (void *)pRepo);
      if (code != 0) {
        tsdbError("vgId:%d failed to create commit thread since %s", REPO_ID(pRepo), strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(code);
        tsdbUnlockRepo(pRepo);
        return NULL;
      }
      if (tsdbUnlockRepo(pRepo) < 0) return NULL;

      if (pImem && tsdbUnRefMemTable(pRepo, pImem) < 0) return NULL;
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

  pTableData->uid = TALBE_UID(pTable);
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