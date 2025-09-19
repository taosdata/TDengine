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
#include "tsdbCacheInt.h"

// Row-based cache serialization functions
static int32_t tsdbRowCacheSerialize(SLastRow *pLastRow, char **value, size_t *size) {
  if (!pLastRow) {
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // Calculate total size needed
  *size =
      sizeof(STsdbRowKey) + sizeof(int8_t) + sizeof(int32_t) + sizeof(int32_t);  // rowKey + dirty + numCols + rowLen
  int32_t rowLen = 0;
  if (pLastRow->pRow) {
    rowLen = TD_ROW_LEN(pLastRow->pRow);
    *size += rowLen;  // Row data size
  }
  if (pLastRow->numCols > 0) {
    *size += pLastRow->numCols * sizeof(SColCacheStatus);  // Column status array size
  }

  *value = taosMemoryMalloc(*size);
  if (NULL == *value) {
    TAOS_RETURN(terrno);
  }

  int32_t offset = 0;

  // Serialize row key
  memcpy(*value + offset, &pLastRow->rowKey, sizeof(STsdbRowKey));
  offset += sizeof(STsdbRowKey);

  // Serialize dirty flag
  *((int8_t *)(*value + offset)) = pLastRow->dirty;
  offset += sizeof(int8_t);

  // Serialize number of columns
  *((int32_t *)(*value + offset)) = pLastRow->numCols;
  offset += sizeof(int32_t);

  // Serialize row length
  *((int32_t *)(*value + offset)) = rowLen;
  offset += sizeof(int32_t);

  // Serialize row data (only if pRow is not NULL)
  if (pLastRow->pRow && rowLen > 0) {
    memcpy(*value + offset, pLastRow->pRow, rowLen);
    offset += rowLen;
  }

  // Serialize column cache statuses
  if (pLastRow->numCols > 0 && pLastRow->colStatus) {
    memcpy(*value + offset, pLastRow->colStatus, pLastRow->numCols * sizeof(SColCacheStatus));
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t tsdbRowCacheDeserialize(char const *value, size_t size, SLastRow **ppLastRow) {
  if (!value || size == 0) {
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  SLastRow *pLastRow = taosMemoryCalloc(1, sizeof(SLastRow));
  if (NULL == pLastRow) {
    TAOS_RETURN(terrno);
  }

  int32_t offset = 0;

  // Deserialize row key
  if (offset + sizeof(STsdbRowKey) > size) {
    taosMemoryFreeClear(pLastRow);
    TAOS_RETURN(TSDB_CODE_INVALID_DATA_FMT);
  }
  memcpy(&pLastRow->rowKey, value + offset, sizeof(STsdbRowKey));
  offset += sizeof(STsdbRowKey);

  // Deserialize dirty flag
  if (offset + sizeof(int8_t) > size) {
    taosMemoryFreeClear(pLastRow);
    TAOS_RETURN(TSDB_CODE_INVALID_DATA_FMT);
  }
  pLastRow->dirty = *((int8_t *)(value + offset));
  offset += sizeof(int8_t);

  // Deserialize number of columns
  if (offset + sizeof(int32_t) > size) {
    taosMemoryFreeClear(pLastRow);
    TAOS_RETURN(TSDB_CODE_INVALID_DATA_FMT);
  }
  pLastRow->numCols = *((int32_t *)(value + offset));
  offset += sizeof(int32_t);

  // Deserialize row length
  if (offset + sizeof(int32_t) > size) {
    taosMemoryFreeClear(pLastRow);
    TAOS_RETURN(TSDB_CODE_INVALID_DATA_FMT);
  }
  int32_t rowLen = *((int32_t *)(value + offset));
  offset += sizeof(int32_t);

  // Deserialize row data (only if rowLen > 0)
  if (rowLen > 0) {
    if (offset + rowLen > size) {
      taosMemoryFreeClear(pLastRow);
      TAOS_RETURN(TSDB_CODE_INVALID_DATA_FMT);
    }

    pLastRow->pRow = taosMemoryMalloc(rowLen);
    if (!pLastRow->pRow) {
      taosMemoryFreeClear(pLastRow);
      TAOS_RETURN(terrno);
    }
    memcpy(pLastRow->pRow, value + offset, rowLen);
  } else {
    // pRow is NULL when rowLen is 0
    pLastRow->pRow = NULL;
  }

  // Deserialize column cache statuses
  if (pLastRow->numCols > 0) {
    size_t colStatusSize = pLastRow->numCols * sizeof(SColCacheStatus);
    if (offset + colStatusSize > size) {
      if (pLastRow->pRow) {
        taosMemoryFree(pLastRow->pRow);
      }
      taosMemoryFreeClear(pLastRow);
      TAOS_RETURN(TSDB_CODE_INVALID_DATA_FMT);
    }

    pLastRow->colStatus = taosMemoryMalloc(colStatusSize);
    if (!pLastRow->colStatus) {
      if (pLastRow->pRow) {
        taosMemoryFree(pLastRow->pRow);
      }
      taosMemoryFreeClear(pLastRow);
      TAOS_RETURN(terrno);
    }
    memcpy(pLastRow->colStatus, value + offset, colStatusSize);
  } else {
    pLastRow->colStatus = NULL;
  }

  *ppLastRow = pLastRow;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static void tsdbRowCacheFreeItem(void *pItem) {
  SLastRow *pRow = (SLastRow *)pItem;
  if (pRow) {
    if (pRow->pRow) {
      taosMemoryFree(pRow->pRow);
    }
    if (pRow->colStatus) {
      taosMemoryFree(pRow->colStatus);
    }
  }
}

// Row-based cache RocksDB operations
int32_t tsdbRowCachePutToRocksdb(STsdb *pTsdb, SLastRowKey *pLastRowKey, SLastRow *pLastRow) {
  int32_t code = 0;
#ifdef USE_ROCKSDB
  char  *rocks_value = NULL;
  size_t vlen = 0;

  code = tsdbRowCacheSerialize(pLastRow, &rocks_value, &vlen);
  if (code) {
    tsdbError("tsdb/rowcache/putrocks: vgId:%d, serialize failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
    TAOS_RETURN(code);
  }

  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  (void)taosThreadMutexLock(&pTsdb->rCache.writeBatchMutex);
  rocksdb_writebatch_put(wb, (char *)pLastRowKey, ROCKS_ROW_KEY_LEN, rocks_value, vlen);
  (void)taosThreadMutexUnlock(&pTsdb->rCache.writeBatchMutex);

  taosMemoryFree(rocks_value);
#endif
  TAOS_RETURN(code);
}

static void tsdbRowCacheDeleter(const void *key, size_t klen, void *value, void *ud) {
  SLastRow *pLastRow = (SLastRow *)value;

  if (pLastRow->dirty) {
    STsdb  *pTsdb = (STsdb *)ud;
    int32_t code = tsdbRowCachePutToRocksdb(pTsdb, (SLastRowKey *)key, pLastRow);
    if (code) {
      tsdbTrace("tsdb/rowcache: vgId:%d, flush cache %s failed at line %d.", TD_VID(pTsdb->pVnode), __func__, __LINE__);
    }
  }

  tsdbRowCacheFreeItem(pLastRow);
  taosMemoryFree(value);
}

static void tsdbRowCacheOverWriter(const void *key, size_t klen, void *value, void *ud) {
  SLastRow *pLastRow = (SLastRow *)value;
  pLastRow->dirty = 0;
}

int32_t tsdbRowCachePutToLRU(STsdb *pTsdb, SLastRowKey *pLastRowKey, SLastRow *pLastRow, int8_t dirty) {
  int32_t code = 0, lino = 0;

  SLastRow *pLRULastRow = taosMemoryCalloc(1, sizeof(SLastRow));
  if (!pLRULastRow) {
    return terrno;
  }

  *pLRULastRow = *pLastRow;
  pLRULastRow->dirty = dirty;

  // Allocate memory for row data
  if (pLastRow->pRow) {
    int32_t rowLen = TD_ROW_LEN(pLastRow->pRow);
    pLRULastRow->pRow = taosMemoryMalloc(rowLen);
    if (!pLRULastRow->pRow) {
      taosMemoryFree(pLRULastRow);
      TAOS_RETURN(terrno);
    }
    memcpy(pLRULastRow->pRow, pLastRow->pRow, rowLen);
  }

  // Allocate memory for colStatus data
  if (pLastRow->colStatus && pLastRow->numCols > 0) {
    pLRULastRow->colStatus = taosMemoryMalloc(pLastRow->numCols * sizeof(SColCacheStatus));
    if (!pLRULastRow->colStatus) {
      if (pLRULastRow->pRow) taosMemoryFree(pLRULastRow->pRow);
      taosMemoryFree(pLRULastRow);
      TAOS_RETURN(terrno);
    }
    memcpy(pLRULastRow->colStatus, pLastRow->colStatus, pLastRow->numCols * sizeof(SColCacheStatus));
  }

  size_t charge = sizeof(SLastRow) + (pLRULastRow->pRow ? TD_ROW_LEN(pLRULastRow->pRow) : 0);

  LRUStatus status =
      taosLRUCacheInsert(pTsdb->lruCache, pLastRowKey, ROCKS_ROW_KEY_LEN, pLRULastRow, charge, tsdbRowCacheDeleter,
                         tsdbRowCacheOverWriter, NULL, TAOS_LRU_PRIORITY_LOW, pTsdb);
  if (TAOS_LRU_STATUS_OK != status && TAOS_LRU_STATUS_OK_OVERWRITTEN != status) {
    tsdbError("vgId:%d, %s failed at line %d status %d.", TD_VID(pTsdb->pVnode), __func__, __LINE__, status);
    code = TSDB_CODE_FAILED;
    pLRULastRow = NULL;
  }

_exit:
  if (TSDB_CODE_SUCCESS != code) {
    if (pLRULastRow) {
      tsdbRowCacheFreeItem(pLRULastRow);
      taosMemoryFree(pLRULastRow);
    }
    tsdbError("tsdb/rowcache/putlru: vgId:%d, failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino,
              tstrerror(code));
  }

  TAOS_RETURN(code);
}

// Row-based cache deleter and overwriter for LRU

// Row-based cache flush dirty function
int tsdbRowCacheFlushDirty(const void *key, size_t klen, void *value, void *ud) {
  SLastRow *pLastRow = (SLastRow *)value;

  if (pLastRow->dirty) {
    STsdb *pTsdb = (STsdb *)ud;

    int32_t code = tsdbRowCachePutToRocksdb(pTsdb, (SLastRowKey *)key, pLastRow);
    if (code) {
      tsdbError("tsdb/rowcache: vgId:%d, flush dirty lru failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
      return code;
    }

    pLastRow->dirty = 0;
    rocksMayWrite(pTsdb, false);
  }

  return 0;
}

// Helper functions for column cache status management
static int32_t tsdbLastRowInitColStatus(SLastRow *pLastRow, STSchema *pTSchema) {
  if (!pLastRow || !pTSchema) {
    return TSDB_CODE_INVALID_PARA;
  }

  pLastRow->numCols = pTSchema->numOfCols;
  pLastRow->colStatus = taosMemoryCalloc(pLastRow->numCols, sizeof(SColCacheStatus));
  if (!pLastRow->colStatus) {
    return terrno;
  }

  for (int32_t i = 0; i < pLastRow->numCols; i++) {
    pLastRow->colStatus[i].cid = pTSchema->columns[i].colId;
    pLastRow->colStatus[i].status = TSDB_LAST_CACHE_VALID;
    pLastRow->colStatus[i].lastTs = TSKEY_MIN;  // Initialize to minimum timestamp
  }

  return TSDB_CODE_SUCCESS;
}

static SColCacheStatus *tsdbLastRowGetLastColValColStatus(SLastRow *pLastRow, int16_t cid) {
  if (!pLastRow || !pLastRow->colStatus) {
    return NULL;
  }

  for (int32_t i = 0; i < pLastRow->numCols; i++) {
    if (pLastRow->colStatus[i].cid == cid) {
      return &pLastRow->colStatus[i];
    }
  }

  return NULL;
}

static int32_t tsdbLastRowSetColStatus(SLastRow *pLastRow, int16_t cid, ELastCacheStatus status) {
  if (!pLastRow || !pLastRow->colStatus) {
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t i = 0; i < pLastRow->numCols; i++) {
    if (pLastRow->colStatus[i].cid == cid) {
      pLastRow->colStatus[i].status = status;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_NOT_FOUND;
}

int32_t tsdbLastRowInvalidateCol(SLastRow *pLastRow, int16_t cid) {
  return tsdbLastRowSetColStatus(pLastRow, cid, TSDB_LAST_CACHE_NO_CACHE);
}

// Create table cache for row-based cache (both LRU and RocksDB)
int32_t tsdbRowCacheCreateTable(STsdb *pTsdb, int64_t uid, STSchema *pTSchema) {
  int32_t code = 0;

  // Create empty row cache entries for both LAST_ROW and LAST flags
  SLastRowKey rowKeys[2] = {{.lflag = LFLAG_LAST_ROW, .uid = uid}, {.lflag = LFLAG_LAST, .uid = uid}};

  for (int i = 0; i < 2; i++) {
    // Create array with all columns to build row
    SArray *colVals = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
    if (!colVals) {
      continue;
    }

    // Add all columns as empty values
    for (int j = 0; j < pTSchema->numOfCols; j++) {
      SColVal emptyCol = COL_VAL_NONE(pTSchema->columns[j].colId, pTSchema->columns[j].type);
      taosArrayPush(colVals, &emptyCol);
    }

    // Build SRow from the column values
    SRow             *pRow = NULL;
    SRowBuildScanInfo scanInfo = {0};
    code = tRowBuild(colVals, pTSchema, &pRow, &scanInfo);

    taosArrayDestroy(colVals);

    if (code == 0 && pRow) {
      // Create row cache entry with the built row
      SRowKey     emptyRowKey = {.ts = TSKEY_MIN, .numOfPKs = 0};
      STsdbRowKey rowKey = {.key = emptyRowKey};
      SLastRow    newRow = {.rowKey = rowKey, .pRow = pRow, .dirty = 1, .numCols = 0, .colStatus = NULL};

      // Initialize column status for the new row
      code = tsdbLastRowInitColStatus(&newRow, pTSchema);
      if (code == 0) {
        // Write to RocksDB first
#ifdef USE_ROCKSDB
        rocksMayWrite(pTsdb, true);
        code = tsdbRowCachePutToRocksdb(pTsdb, &rowKeys[i], &newRow);
        if (code != 0) {
          tsdbError("vgId:%d, %s put to rocksdb failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                    tstrerror(code));
        }
#endif

        // Write to LRU cache
        if (code == 0) {
          code = tsdbRowCachePutToLRU(pTsdb, &rowKeys[i], &newRow, 1);
        }

        // Always cleanup our allocated memory since LRU cache makes its own copy
        if (newRow.colStatus) taosMemoryFree(newRow.colStatus);
        if (pRow) taosMemoryFree(pRow);
      } else {
        // Cleanup on error
        if (pRow) taosMemoryFree(pRow);
      }
    }
  }

  TAOS_RETURN(code);
}

// Delete table cache for row-based cache (both LRU and RocksDB)
int32_t tsdbRowCacheDeleteTable(STsdb *pTsdb, int64_t uid) {
  int32_t code = 0;

  // Delete row cache entries for both LAST_ROW and LAST flags
  SLastRowKey rowKeys[2] = {{.lflag = LFLAG_LAST_ROW, .uid = uid}, {.lflag = LFLAG_LAST, .uid = uid}};

  for (int i = 0; i < 2; i++) {
#ifdef USE_ROCKSDB
    char  *rowKey = (char *)&rowKeys[i];
    size_t rowKeySize = ROCKS_ROW_KEY_LEN;
    rocksMayWrite(pTsdb, true);
    rocksdb_writebatch_delete(pTsdb->rCache.writebatch, rowKey, rowKeySize);
#endif
    // Handle LRU cache
    LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, &rowKeys[i], ROCKS_ROW_KEY_LEN);
    if (h) {
      tsdbLRUCacheRelease(pTsdb->lruCache, h, true);
      taosLRUCacheErase(pTsdb->lruCache, &rowKeys[i], ROCKS_ROW_KEY_LEN);
    }
  }

  TAOS_RETURN(code);
}

// Row-based cache update functions
int32_t tsdbRowCacheUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, STsdbRowKey *pRowKey, SRow *pRow, int8_t lflag) {
  int32_t    code = 0, lino = 0;
  SLRUCache *pCache = pTsdb->lruCache;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  SLastRowKey key = {.uid = uid, .lflag = lflag};
  LRUHandle  *h = taosLRUCacheLookup(pCache, &key, ROCKS_ROW_KEY_LEN);

  if (h) {
    SLastRow *pLastRow = (SLastRow *)taosLRUCacheValue(pCache, h);
    // Always check for row cache updates - individual column status will be handled separately
    int32_t cmp_res = tRowKeyCompare(&pLastRow->rowKey.key, &pRowKey->key);
    if (cmp_res < 0 || (cmp_res == 0 && pRow != NULL)) {
      SLastRow newLastRow = {.rowKey = *pRowKey, .pRow = pRow, .dirty = 1, .numCols = 0, .colStatus = NULL};

      // Initialize column status array for the new row
      STSchema *pTSchema = NULL;
      if (metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, -1, &pTSchema) == 0 && pTSchema) {
        tsdbLastRowInitColStatus(&newLastRow, pTSchema);
        taosMemoryFree(pTSchema);
      }
      code = tsdbRowCachePutToLRU(pTsdb, &key, &newLastRow, 1);
      if (newLastRow.colStatus) {
        taosMemoryFree(newLastRow.colStatus);
      }
    }
    tsdbLRUCacheRelease(pCache, h, false);
  } else {
    // Load from RocksDB and update
    char   *keys_list[1];
    size_t  keys_list_sizes[1];
    char  **values_list = NULL;
    size_t *values_list_sizes = NULL;

    keys_list[0] = (char *)&key;
    keys_list_sizes[0] = ROCKS_ROW_KEY_LEN;

    rocksMayWrite(pTsdb, true);
    code = tsdbCacheGetValuesFromRocks(pTsdb, 1, (const char *const *)keys_list, keys_list_sizes, &values_list,
                                       &values_list_sizes);

    if (code == TSDB_CODE_SUCCESS && values_list && values_list[0]) {
      SLastRow *pLastRow = NULL;
      code = tsdbRowCacheDeserialize(values_list[0], values_list_sizes[0], &pLastRow);
      if (code == TSDB_CODE_SUCCESS && pLastRow) {
        // Always check for row cache updates - individual column status will be handled separately
        int32_t cmp_res = tRowKeyCompare(&pLastRow->rowKey.key, &pRowKey->key);
        if (cmp_res < 0 || (cmp_res == 0 && pRow != NULL)) {
          SLastRow lastRowTmp = {.rowKey = *pRowKey, .pRow = pRow, .dirty = 0, .numCols = 0, .colStatus = NULL};

          // Initialize column status array for the new row
          STSchema *pTSchema = NULL;
          if (metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, -1, &pTSchema) == 0 && pTSchema) {
            tsdbLastRowInitColStatus(&lastRowTmp, pTSchema);
            taosMemoryFree(pTSchema);
          }

          code = tsdbRowCachePutToRocksdb(pTsdb, &key, &lastRowTmp);
          if (code == TSDB_CODE_SUCCESS) {
            code = tsdbRowCachePutToLRU(pTsdb, &key, &lastRowTmp, 0);
          }
          if (lastRowTmp.colStatus) {
            taosMemoryFree(lastRowTmp.colStatus);
          }
        }
        if (pLastRow->pRow) {
          taosMemoryFree(pLastRow->pRow);
        }
        if (pLastRow->colStatus) {
          taosMemoryFree(pLastRow->colStatus);
        }
        taosMemoryFree(pLastRow);
      }
    }

    if (values_list) {
#ifdef USE_ROCKSDB
      if (values_list[0]) {
        rocksdb_free(values_list[0]);
      }
#endif
      taosMemoryFree(values_list);
    }
    if (values_list_sizes) {
      taosMemoryFree(values_list_sizes);
    }

    rocksMayWrite(pTsdb, false);
  }

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  if (code) {
    tsdbError("tsdb/rowcache: vgId:%d, update failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino,
              tstrerror(code));
  }

  TAOS_RETURN(code);
}

int32_t tsdbRowCacheRowFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, int64_t version, int32_t nRow,
                                    SRow **aRow) {
  int32_t code = 0, lino = 0;

  if (!aRow || nRow <= 0) {
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // Update last_row cache with the last row
  TSDBROW     lRow = {.type = TSDBROW_ROW_FMT, .pTSRow = aRow[nRow - 1], .version = version};
  STsdbRowKey tsdbRowKey = {0};
  tsdbRowGetKey(&lRow, &tsdbRowKey);

  TAOS_CHECK_GOTO(tsdbRowCacheUpdate(pTsdb, suid, uid, &tsdbRowKey, aRow[nRow - 1], LFLAG_LAST_ROW), &lino, _exit);

  // Find the most recent row with non-null values for last cache
  for (int32_t iRow = nRow - 1; iRow >= 0; --iRow) {
    TSDBROW     tRow = {.type = TSDBROW_ROW_FMT, .pTSRow = aRow[iRow], .version = version};
    STsdbRowKey tRowKey = {0};
    tsdbRowGetKey(&tRow, &tRowKey);

    // Check if this row has any non-null values
    STSchema *pTSchema = NULL;
    int32_t   sver = TSDBROW_SVERSION(&tRow);
    TAOS_CHECK_GOTO(tsdbUpdateSkm(pTsdb, suid, uid, sver), &lino, _exit);
    pTSchema = pTsdb->rCache.pTSchema;

    STSDBRowIter iter = {0};
    TAOS_CHECK_GOTO(tsdbRowIterOpen(&iter, &tRow, pTSchema), &lino, _exit);

    bool hasValue = false;
    for (SColVal *pColVal = tsdbRowIterNext(&iter); pColVal; pColVal = tsdbRowIterNext(&iter)) {
      if (COL_VAL_IS_VALUE(pColVal)) {
        hasValue = true;
        break;
      }
    }
    tsdbRowClose(&iter);

    if (hasValue) {
      TAOS_CHECK_GOTO(tsdbRowCacheUpdate(pTsdb, suid, uid, &tRowKey, aRow[iRow], LFLAG_LAST), &lino, _exit);
      break;  // Found the most recent row with values, stop here
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
  }

  TAOS_RETURN(code);
}

int32_t tsdbRowCacheColFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SBlockData *pBlockData) {
  int32_t code = 0, lino = 0;

  if (!pBlockData || pBlockData->nRow <= 0) {
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // Update last_row cache with the last row
  TSDBROW     lRow = tsdbRowFromBlockData(pBlockData, pBlockData->nRow - 1);
  STsdbRowKey tsdbRowKey = {0};
  tsdbRowGetKey(&lRow, &tsdbRowKey);

  // For block data, we need to convert to row format
  // Since we're working with block data, we can't directly get SRow
  // For now, we'll skip row cache update for block data format
  // TODO: Implement proper block data to row conversion if needed
  TAOS_CHECK_GOTO(TSDB_CODE_SUCCESS, &lino, _exit);  // Placeholder - skip for now

  // TODO: Implement proper block data processing for row-based cache
  // For now, skip the last cache update for block data

_exit:

  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
  }

  TAOS_RETURN(code);
}

// Update LAST cache with proper column-level timestamp tracking
static int32_t tsdbRowCacheUpdateLastCols(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SArray *updCtxArray,
                                          STSchema *pTSchema) {
  int32_t    code = 0, lino = 0;
  SLRUCache *pCache = pTsdb->lruCache;

  if (!updCtxArray || !pTSchema) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  // Get the LAST cache entry
  SLastRowKey key = {.uid = uid, .lflag = LFLAG_LAST};
  LRUHandle  *h = taosLRUCacheLookup(pCache, &key, ROCKS_ROW_KEY_LEN);
  SLastRow   *pLastRow = NULL;
  bool        newEntry = false;

  if (h) {
    pLastRow = (SLastRow *)taosLRUCacheValue(pCache, h);
  } else {
    // Try to load from RocksDB
    char   *keys_list[1];
    size_t  keys_list_sizes[1];
    char  **values_list = NULL;
    size_t *values_list_sizes = NULL;

    keys_list[0] = (char *)&key;
    keys_list_sizes[0] = ROCKS_ROW_KEY_LEN;

    rocksMayWrite(pTsdb, true);
    code = tsdbCacheGetValuesFromRocks(pTsdb, 1, (const char *const *)keys_list, keys_list_sizes, &values_list,
                                       &values_list_sizes);

    if (code == TSDB_CODE_SUCCESS && values_list && values_list[0]) {
      code = tsdbRowCacheDeserialize(values_list[0], values_list_sizes[0], &pLastRow);
    }
    // Cleanup RocksDB values
    if (values_list) {
#ifdef USE_ROCKSDB
      for (int i = 0; i < 1; ++i) {
        if (values_list[i]) {
          rocksdb_free(values_list[i]);
        }
      }
#endif
      taosMemoryFree(values_list);
    }
    if (values_list_sizes) {
      taosMemoryFree(values_list_sizes);
    }

    if (!pLastRow) {
      // Create new entry
      pLastRow = taosMemoryCalloc(1, sizeof(SLastRow));
      if (!pLastRow) {
        (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
        TAOS_RETURN(terrno);
      }
      pLastRow->rowKey.key.ts = TSKEY_MIN;
      pLastRow->dirty = 1;
      newEntry = true;

      // Initialize column status array
      tsdbLastRowInitColStatus(pLastRow, pTSchema);
    }
  }

  // Initialize lastColVals array with current values from pLastRow or NULL
  SArray *lastColVals = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
  if (!lastColVals) {
    code = terrno;
    goto _cleanup;
  }

  // Initialize array with existing values from pLastRow or NULL if no existing row
  for (int j = 0; j < pTSchema->numOfCols; j++) {
    STColumn *pCol = &pTSchema->columns[j];
    SColVal   colVal = COL_VAL_NULL(pCol->colId, pCol->type);

    if (pLastRow->pRow) {
      tRowGetLastColVal(pLastRow->pRow, pTSchema, pCol->colId, &colVal);
    }

    if (!taosArrayPush(lastColVals, &colVal)) {
      code = terrno;
      taosArrayDestroy(lastColVals);
      goto _cleanup;
    }
  }

  // Process each LAST update context and update the array as needed
  int  num_keys = TARRAY_SIZE(updCtxArray);
  bool hasUpdates = false;

  for (int i = 0; i < num_keys; ++i) {
    SLastUpdateCtx *updCtx = &((SLastUpdateCtx *)TARRAY_DATA(updCtxArray))[i];

    if (updCtx->lflag != LFLAG_LAST || !COL_VAL_IS_VALUE(&updCtx->colVal)) {
      continue;
    }

    // Find the column status for this column ID
    SColCacheStatus *colStatus = NULL;
    int              schemaColIdx = -1;
    for (int j = 0; j < pLastRow->numCols; j++) {
      if (pLastRow->colStatus[j].cid == updCtx->colVal.cid) {
        colStatus = &pLastRow->colStatus[j];
        break;
      }
    }

    // Find the corresponding column in schema
    for (int k = 0; k < pTSchema->numOfCols; k++) {
      if (pTSchema->columns[k].colId == updCtx->colVal.cid) {
        schemaColIdx = k;
        break;
      }
    }

    // If column status or schema column not found, skip
    if (!colStatus || schemaColIdx == -1) {
      continue;
    }

    // Check if this is a newer value for this column
    if (updCtx->tsdbRowKey.key.ts > colStatus->lastTs) {
      colStatus->lastTs = updCtx->tsdbRowKey.key.ts;
      colStatus->status = TSDB_LAST_CACHE_VALID;

      // Update the value in lastColVals array
      SColVal *pColVal = taosArrayGet(lastColVals, schemaColIdx);
      *pColVal = updCtx->colVal;
      hasUpdates = true;
    }
  }

  if (hasUpdates) {
    // Build the new row
    SRow             *pNewRow = NULL;
    SRowBuildScanInfo scanInfo = {0};

    code = tRowBuild(lastColVals, pTSchema, &pNewRow, &scanInfo);
    taosArrayDestroy(lastColVals);
    lastColVals = NULL;

    if (code == TSDB_CODE_SUCCESS && pNewRow) {
      // Free the old row if it exists
      if (pLastRow->pRow) {
        taosMemoryFree(pLastRow->pRow);
      }
      pLastRow->pRow = pNewRow;
      pLastRow->dirty = 1;

      // Update the row key timestamp to the latest timestamp among all columns
      TSKEY maxTs = TSKEY_MIN;
      for (int j = 0; j < pLastRow->numCols; j++) {
        if (pLastRow->colStatus[j].lastTs > maxTs) {
          maxTs = pLastRow->colStatus[j].lastTs;
        }
      }
      pLastRow->rowKey.key.ts = maxTs;
    }
  }

  // Update cache
  if (hasUpdates) {
    if (newEntry) {
      code = tsdbRowCachePutToLRU(pTsdb, &key, pLastRow, 1);
    } else {
      pLastRow->dirty = 1;
    }
  }

_cleanup:
  if (lastColVals) {
    taosArrayDestroy(lastColVals);
  }
  if (h) {
    tsdbLRUCacheRelease(pCache, h, false);
  } else if (newEntry && pLastRow) {
    if (pLastRow->colStatus) {
      taosMemoryFree(pLastRow->colStatus);
    }
    if (pLastRow->pRow) {
      taosMemoryFree(pLastRow->pRow);
    }
    taosMemoryFree(pLastRow);
  }

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  if (code) {
    tsdbError("tsdb/rowcache: vgId:%d, update last cols failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino,
              tstrerror(code));
  }

  TAOS_RETURN(code);
}

// Row cache update from context array function
int32_t tsdbRowCacheUpdateFromCtxArray(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SArray *updCtxArray) {
  int32_t code = 0, lino = 0;

  if (!updCtxArray || TARRAY_SIZE(updCtxArray) == 0) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  // Group updates by lflag and timestamp to find the most recent entry for each type
  STsdbRowKey lastRowKey = {.key.ts = TSKEY_MIN};
  STsdbRowKey lastKey = {.key.ts = TSKEY_MIN};
  bool        hasLastRow = false;
  bool        hasLast = false;

  // Arrays to collect column values for row building
  SArray   *lastRowColVals = NULL;
  SArray   *lastColVals = NULL;
  STSchema *pTSchema = NULL;

  // Find the most recent timestamp for each cache type and collect column values
  int num_keys = TARRAY_SIZE(updCtxArray);
  for (int i = 0; i < num_keys; ++i) {
    SLastUpdateCtx *updCtx = &((SLastUpdateCtx *)TARRAY_DATA(updCtxArray))[i];

    if (updCtx->lflag == LFLAG_LAST_ROW) {
      if (tRowKeyCompare(&lastRowKey.key, &updCtx->tsdbRowKey.key) < 0) {
        lastRowKey = updCtx->tsdbRowKey;
        hasLastRow = true;
      }
    } else if (updCtx->lflag == LFLAG_LAST && COL_VAL_IS_VALUE(&updCtx->colVal)) {
      if (tRowKeyCompare(&lastKey.key, &updCtx->tsdbRowKey.key) < 0) {
        lastKey = updCtx->tsdbRowKey;
        hasLast = true;
      }
    }
  }

  // Get table schema needed for row building
  if (hasLastRow || hasLast) {
    // Use the version from the most recent update
    STsdbRowKey *pMostRecentKey = (hasLastRow && hasLast)
                                      ? (tRowKeyCompare(&lastRowKey.key, &lastKey.key) >= 0 ? &lastRowKey : &lastKey)
                                      : (hasLastRow ? &lastRowKey : &lastKey);

    TAOS_CHECK_GOTO(metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, -1, &pTSchema), &lino, _exit);
  }

  // Process LAST_ROW updates
  if (hasLastRow && pTSchema) {
    lastRowColVals = taosArrayInit(16, sizeof(SColVal));
    if (!lastRowColVals) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }

    // Collect all column values with the latest timestamp for LAST_ROW
    for (int i = 0; i < num_keys; ++i) {
      SLastUpdateCtx *updCtx = &((SLastUpdateCtx *)TARRAY_DATA(updCtxArray))[i];
      if (updCtx->lflag == LFLAG_LAST_ROW && tRowKeyCompare(&updCtx->tsdbRowKey.key, &lastRowKey.key) == 0) {
        if (!taosArrayPush(lastRowColVals, &updCtx->colVal)) {
          TAOS_CHECK_GOTO(terrno, &lino, _exit);
        }
      }
    }

    // Build SRow and update cache
    if (TARRAY_SIZE(lastRowColVals) > 0) {
      SRow             *pRow = NULL;
      SRowBuildScanInfo scanInfo = {0};

      TAOS_CHECK_GOTO(tRowBuild(lastRowColVals, pTSchema, &pRow, &scanInfo), &lino, _exit);
      if (pRow) {
        code = tsdbRowCacheUpdate(pTsdb, suid, uid, &lastRowKey, pRow, LFLAG_LAST_ROW);
        // Free pRow since tsdbRowCacheUpdate makes a copy
        taosMemoryFree(pRow);
        pRow = NULL;
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
    }
  }

  // Process LAST updates - handle each column's timestamp independently
  if (hasLast && pTSchema) {
    code = tsdbRowCacheUpdateLastCols(pTsdb, suid, uid, updCtxArray, pTSchema);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

_exit:
  // Cleanup
  if (lastRowColVals) {
    taosArrayDestroy(lastRowColVals);
  }
  if (lastColVals) {
    taosArrayDestroy(lastColVals);
  }
  if (pTSchema) {
    taosMemoryFree(pTSchema);
  }

  if (code) {
    tsdbError("tsdb/rowcache: vgId:%d, update from ctx array failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino,
              tstrerror(code));
  }

  TAOS_RETURN(code);
}

int tsdbRowCacheCmp(void *state, const char *a, size_t alen, const char *b, size_t blen) {
  (void)state;
  (void)alen;
  (void)blen;

  SLastRowKey *lhs = (SLastRowKey *)a;
  SLastRowKey *rhs = (SLastRowKey *)b;

  if (lhs->uid < rhs->uid) {
    return -1;
  } else if (lhs->uid > rhs->uid) {
    return 1;
  }

  if ((lhs->lflag & LFLAG_LAST) < (rhs->lflag & LFLAG_LAST)) {
    return -1;
  } else if ((lhs->lflag & LFLAG_LAST) > (rhs->lflag & LFLAG_LAST)) {
    return 1;
  }

  return 0;
}
static void tsdbFreeLastRow(SLastRow *pLastRow) {
  if (pLastRow == NULL) {
    return;
  }
  if (pLastRow->pRow) {
    taosMemoryFree(pLastRow->pRow);
  }
  if (pLastRow->colStatus) {
    taosMemoryFree(pLastRow->colStatus);
  }
  taosMemoryFree(pLastRow);
}

int32_t tsdbRowCacheNewTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type, int8_t lflag,
                                   SSchemaWrapper *pOldSchemaWrapper, SSchemaWrapper *pNewSchemaWrapper) {
  int32_t   code = 0, lino = 0;
  STSchema *pOldTSchema = NULL, *pNewTSchema = NULL;
  SArray   *colVals = NULL;
  char    **values_list = NULL;
  size_t   *values_list_sizes = NULL;

  // Schema wrappers are required for column operations
  if (pOldSchemaWrapper == NULL || pNewSchemaWrapper == NULL) {
    tsdbError("vgId:%d, %s failed: schema wrappers are required for column operations", TD_VID(pTsdb->pVnode),
              __func__);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // Build new row with the added column
  code = metaBuildTSchemaFromSchemaWrapper(pOldSchemaWrapper, &pOldTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  code = metaBuildTSchemaFromSchemaWrapper(pNewSchemaWrapper, &pNewTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  tsdbInfo("vgId:%d, %s old schema version: %d, new schema version: %d", TD_VID(pTsdb->pVnode), __func__,
           pOldTSchema->version, pNewTSchema->version);

  // Schema-based column addition logic for existing tables
  // For row-based cache, handle the specific lflag passed from caller
  SLastRowKey rowKey = {.lflag = lflag, .uid = uid};

  // 1. Handle RocksDB row cache
#ifdef USE_ROCKSDB
  char  *rowKeyPtr = (char *)&rowKey;
  size_t rowKeySize = ROCKS_ROW_KEY_LEN;

  // Get row from RocksDB
  rocksMayWrite(pTsdb, true);
  code = tsdbCacheGetValuesFromRocks(pTsdb, 1, (const char *const *)&rowKeyPtr, &rowKeySize, &values_list,
                                     &values_list_sizes);
  if (code != TSDB_CODE_SUCCESS) {
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  if (values_list && values_list[0] != NULL) {
    SLastRow *pLastRow = NULL;
    code = tsdbRowCacheDeserialize(values_list[0], values_list_sizes[0], &pLastRow);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbFreeLastRow(pLastRow);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }
    if (pLastRow && pLastRow->pRow) {
      // Extract existing column values
      colVals = taosArrayInit(pNewTSchema->numOfCols, sizeof(SColVal));
      if (colVals == NULL) {
        tsdbFreeLastRow(pLastRow);
        code = TSDB_CODE_OUT_OF_MEMORY;
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }

      // Copy existing columns
      for (int j = 0; j < pOldTSchema->numOfCols; j++) {
        SColVal colVal = {0};
        code = tRowGetLastColVal(pLastRow->pRow, pOldTSchema, j, &colVal);
        if (code != TSDB_CODE_SUCCESS) {
          tsdbFreeLastRow(pLastRow);
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
        if (!taosArrayPush(colVals, &colVal)) {
          tsdbFreeLastRow(pLastRow);
          code = terrno;
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
      }

      // Add the new column with empty value
      SColVal newColVal = COL_VAL_NONE(cid, col_type);
      if (!taosArrayPush(colVals, &newColVal)) {
        tsdbFreeLastRow(pLastRow);
        code = terrno;
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }

      // Build new row
      SRow             *pNewRow = NULL;
      SRowBuildScanInfo scanInfo = {0};

      code = tRowBuild(colVals, pNewTSchema, &pNewRow, &scanInfo);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbFreeLastRow(pLastRow);
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }

      // Update the cached row - extend existing colStatus
      SLastRow newLastRow = {
          .rowKey = pLastRow->rowKey, .pRow = pNewRow, .dirty = 1, .numCols = pLastRow->numCols + 1, .colStatus = NULL};

      // Extend colStatus array - allocate new memory instead of realloc to avoid double free
      if (pLastRow->colStatus) {
        newLastRow.colStatus = taosMemoryMalloc(newLastRow.numCols * sizeof(SColCacheStatus));
        if (newLastRow.colStatus == NULL) {
          taosMemoryFreeClear(pNewRow);
          tsdbFreeLastRow(pLastRow);
          code = TSDB_CODE_OUT_OF_MEMORY;
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
        // Copy existing status entries
        memcpy(newLastRow.colStatus, pLastRow->colStatus, pLastRow->numCols * sizeof(SColCacheStatus));
        // Add new column status
        newLastRow.colStatus[newLastRow.numCols - 1] = (SColCacheStatus){.cid = cid, .status = TSDB_LAST_CACHE_VALID};
      } else {
        // Initialize colStatus for the first time
        code = tsdbLastRowInitColStatus(&newLastRow, pNewTSchema);
        if (code != TSDB_CODE_SUCCESS) {
          taosMemoryFreeClear(pNewRow);
          tsdbFreeLastRow(pLastRow);
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
      }

      code = tsdbRowCachePutToRocksdb(pTsdb, &rowKey, &newLastRow);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFreeClear(pNewRow);
        taosMemoryFree(newLastRow.colStatus);
        tsdbFreeLastRow(pLastRow);
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }

      // Clean up - free the new colStatus we allocated and the new row
      if (newLastRow.colStatus && newLastRow.colStatus != pLastRow->colStatus) {
        taosMemoryFree(newLastRow.colStatus);
      }

      taosMemoryFreeClear(pNewRow);
      taosArrayDestroy(colVals);
      colVals = NULL;
    }

    tsdbFreeLastRow(pLastRow);
  }

#endif

  // 2. Handle LRU row cache - correct approach: get, decode, modify, re-encode
  LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, &rowKey, ROCKS_ROW_KEY_LEN);
  if (h) {
    SLastRow *pLastRow = (SLastRow *)taosLRUCacheValue(pTsdb->lruCache, h);
    if (pLastRow && pLastRow->pRow) {
      // Decode existing row data into column values
      colVals = taosArrayInit(pNewTSchema->numOfCols, sizeof(SColVal));
      if (colVals == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }

      // Extract existing columns from current row
      for (int j = 0; j < pOldTSchema->numOfCols; j++) {
        SColVal colVal = {0};
        code = tRowGetLastColVal(pLastRow->pRow, pOldTSchema, j, &colVal);
        if (code == TSDB_CODE_SUCCESS) {
          taosArrayPush(colVals, &colVal);
        } else if (code != TSDB_CODE_SUCCESS) {
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
      }

      // Add the new empty column
      SColVal newColVal = COL_VAL_NONE(cid, col_type);
      if (!taosArrayPush(colVals, &newColVal)) {
        code = terrno;
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }

      // Re-encode into new row with updated schema
      SRow             *pNewRow = NULL;
      SRowBuildScanInfo scanInfo = {0};
      code = tRowBuild(colVals, pNewTSchema, &pNewRow, &scanInfo);
      if (code != TSDB_CODE_SUCCESS) {
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }

      // Create updated row structure
      SLastRow newLastRow = {
          .rowKey = pLastRow->rowKey, .pRow = pNewRow, .dirty = 1, .numCols = pLastRow->numCols + 1, .colStatus = NULL};

      // Handle column status array - allocate new memory to avoid double free
      if (pLastRow->colStatus) {
        newLastRow.colStatus = taosMemoryMalloc(newLastRow.numCols * sizeof(SColCacheStatus));
        if (newLastRow.colStatus == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
        // Copy existing status entries
        memcpy(newLastRow.colStatus, pLastRow->colStatus, pLastRow->numCols * sizeof(SColCacheStatus));
        // Add new column status
        newLastRow.colStatus[newLastRow.numCols - 1] = (SColCacheStatus){.cid = cid, .status = TSDB_LAST_CACHE_VALID};

      } else {
        // Initialize colStatus for the first time
        code = tsdbLastRowInitColStatus(&newLastRow, pNewTSchema);
        if (code != TSDB_CODE_SUCCESS) {
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
      }

      if (code == TSDB_CODE_SUCCESS && newLastRow.colStatus == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
      // Put the updated row back to LRU cache - cache will make its own copy
      code = tsdbRowCachePutToLRU(pTsdb, &rowKey, &newLastRow, 1);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(newLastRow.colStatus);
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
      // Always clean up our allocated memory since LRU cache makes its own copy
      if (newLastRow.colStatus && newLastRow.colStatus != pLastRow->colStatus) {
        taosMemoryFree(newLastRow.colStatus);
      }
      if (pNewRow == NULL) {
        taosMemoryFree(pNewRow);
      }
    }
    taosArrayDestroy(colVals);
  }

  tsdbLRUCacheRelease(pTsdb->lruCache, h, false);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s.", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }

  rocksdb_free(values_list[0]);
  taosMemFree(values_list);
  taosMemFree(values_list_sizes);
  taosMemoryFree(pNewTSchema);
  taosMemoryFree(pOldTSchema);
  TAOS_RETURN(code);
}

int32_t tsdbRowCacheDropTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t lflag,
                                    SSchemaWrapper *pOldSchemaWrapper, SSchemaWrapper *pNewSchemaWrapper,
                                    bool hasPrimaryKey) {
  int32_t   code = 0, lino = 0;
  char    **values_list = NULL;
  size_t   *values_list_sizes = NULL;
  STSchema *pTOldSchema = NULL, *pNewTSchema = NULL;
  SArray   *colVals = NULL;

  // Schema wrappers are required for column operations
  if (pOldSchemaWrapper == NULL || pNewSchemaWrapper == NULL) {
    tsdbError("vgId:%d, %s failed: schema wrappers are required for column operations", TD_VID(pTsdb->pVnode),
              __func__);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  code = metaBuildTSchemaFromSchemaWrapper(pOldSchemaWrapper, &pTOldSchema);
  if (code != TSDB_CODE_SUCCESS) {
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }
  code = metaBuildTSchemaFromSchemaWrapper(pNewSchemaWrapper, &pNewTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  // For row-based cache, handle the specific lflag passed from caller
  SLastRowKey rowKey = {.lflag = lflag, .uid = uid};

  // 1. Handle RocksDB row cache
#ifdef USE_ROCKSDB
  char  *rowKeyPtr = (char *)&rowKey;
  size_t rowKeySize = ROCKS_ROW_KEY_LEN;

  // Get row from RocksDB
  rocksMayWrite(pTsdb, true);
  code = tsdbCacheGetValuesFromRocks(pTsdb, 1, (const char *const *)&rowKeyPtr, &rowKeySize, &values_list,
                                     &values_list_sizes);
  if (code != TSDB_CODE_SUCCESS) {
    TAOS_RETURN(code);
  }

  if (values_list && values_list[0] != NULL) {
    SLastRow *pLastRow = NULL;
    code = tsdbRowCacheDeserialize(values_list[0], values_list_sizes[0], &pLastRow);
    if (code != TSDB_CODE_SUCCESS) {
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }
    if (pLastRow && pLastRow->pRow) {
      // Check if column exists in the row
      bool columnExists = false;
      for (int j = 0; j < pTOldSchema->numOfCols; j++) {
        if (pTOldSchema->columns[j].colId == cid) {
          columnExists = true;
          break;
        }
      }

      if (columnExists) {
        // Build new row without the dropped column
        colVals = taosArrayInit(pTOldSchema->numOfCols - 1, sizeof(SColVal));
        if (!colVals) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          tsdbFreeLastRow(pLastRow);
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }

        // Extract all columns except the one to be dropped
        for (int j = 0; j < pTOldSchema->numOfCols; j++) {
          if (pTOldSchema->columns[j].colId != cid) {
            SColVal colVal = {0};
            code = tRowGetLastColVal(pLastRow->pRow, pTOldSchema, j, &colVal);
            if (code != TSDB_CODE_SUCCESS) {
              tsdbFreeLastRow(pLastRow);
              TAOS_CHECK_GOTO(code, &lino, _exit);
            }
            if (!taosArrayPush(colVals, &colVal)) {
              tsdbFreeLastRow(pLastRow);
              TAOS_CHECK_GOTO(code, &lino, _exit);
            }
          }
        }

        // Build new row
        SRow             *pNewRow = NULL;
        SRowBuildScanInfo scanInfo = {0};
        code = tRowBuild(colVals, pNewTSchema, &pNewRow, &scanInfo);
        if (code == TSDB_CODE_SUCCESS && pNewRow) {
          // Update the cached row
          SLastRow newLastRow = {.rowKey = pLastRow->rowKey,
                                 .pRow = pNewRow,
                                 .dirty = 1,
                                 .numCols = pLastRow->numCols,
                                 .colStatus = pLastRow->colStatus};

          // Reuse original column status and remove the dropped column
          if (newLastRow.colStatus) {
            // Update column status to remove the dropped column
            for (int k = 0; k < newLastRow.numCols; k++) {
              if (newLastRow.colStatus[k].cid == cid) {
                // Remove this column status entry
                memmove(&newLastRow.colStatus[k], &newLastRow.colStatus[k + 1],
                        (newLastRow.numCols - k - 1) * sizeof(SColCacheStatus));
                newLastRow.numCols--;
                break;
              }
            }
          }

          // Serialize and write back to RocksDB
          code = tsdbRowCachePutToRocksdb(pTsdb, &rowKey, &newLastRow);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFree(pNewRow);
            tsdbFreeLastRow(pLastRow);
            TAOS_CHECK_GOTO(code, &lino, _exit);
          }
          if (pNewRow) {
            taosMemoryFree(pNewRow);
          }
        } else {
          taosMemoryFree(pNewRow);
          tsdbFreeLastRow(pLastRow);
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
        taosArrayDestroy(colVals);

      } else {
        // Column doesn't exist in row, just delete the column status if present
        bool statusUpdated = false;
        for (int k = 0; k < pLastRow->numCols; k++) {
          if (pLastRow->colStatus && pLastRow->colStatus[k].cid == cid) {
            // Remove this column status entry
            memmove(&pLastRow->colStatus[k], &pLastRow->colStatus[k + 1],
                    (pLastRow->numCols - k - 1) * sizeof(SColCacheStatus));
            pLastRow->numCols--;
            statusUpdated = true;
            break;
          }
        }

        if (statusUpdated) {
          pLastRow->dirty = 1;
          code = tsdbRowCachePutToRocksdb(pTsdb, &rowKey, pLastRow);
        }
      }
    }

    // Clean up
    tsdbFreeLastRow(pLastRow);
  }

#endif

  // 2. Handle LRU row cache
  LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, &rowKey, ROCKS_ROW_KEY_LEN);
  if (h) {
    SLastRow *pLastRow = (SLastRow *)taosLRUCacheValue(pTsdb->lruCache, h);
    if (pLastRow && pLastRow->pRow) {
      // Check if the row contains the column to be dropped
      // Check if column exists in the row
      bool columnExists = false;
      for (int j = 0; j < pTOldSchema->numOfCols; j++) {
        if (pTOldSchema->columns[j].colId == cid) {
          columnExists = true;
          break;
        }
      }

      if (columnExists) {
        // Build new row without the dropped column
        SArray *colVals = taosArrayInit(pTOldSchema->numOfCols - 1, sizeof(SColVal));
        if (colVals) {
          // Extract all columns except the one to be dropped
          for (int j = 0; j < pTOldSchema->numOfCols; j++) {
            if (pTOldSchema->columns[j].colId != cid) {
              SColVal colVal = {0};
              code = tRowGetLastColVal(pLastRow->pRow, pTOldSchema, j, &colVal);
              if (code != TSDB_CODE_SUCCESS) {
                taosMemoryFree(colVals);
                tsdbFreeLastRow(pLastRow);
                TAOS_CHECK_GOTO(code, &lino, _exit);
              }
              if (!taosArrayPush(colVals, &colVal)) {
                taosMemoryFree(colVals);
                tsdbFreeLastRow(pLastRow);
                TAOS_CHECK_GOTO(code, &lino, _exit);
              }
            }
          }
          // Build new row
          SRow             *pNewRow = NULL;
          SRowBuildScanInfo scanInfo = {0};
          code = tRowBuild(colVals, pNewTSchema, &pNewRow, &scanInfo);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFree(colVals);
            tsdbFreeLastRow(pLastRow);
            TAOS_CHECK_GOTO(code, &lino, _exit);
          }
          if (pNewRow) {
            // Update the cached row
            SLastRow newLastRow = {.rowKey = pLastRow->rowKey,
                                   .pRow = pNewRow,
                                   .dirty = 1,
                                   .numCols = pLastRow->numCols,
                                   .colStatus = pLastRow->colStatus};

            // Reuse original column status and remove the dropped column
            if (newLastRow.colStatus) {
              // Update column status to remove the dropped column
              for (int k = 0; k < newLastRow.numCols; k++) {
                if (newLastRow.colStatus[k].cid == cid) {
                  // Remove this column status entry
                  memmove(&newLastRow.colStatus[k], &newLastRow.colStatus[k + 1],
                          (newLastRow.numCols - k - 1) * sizeof(SColCacheStatus));
                  newLastRow.numCols--;
                  break;
                }
              }
            }

            // Update LRU cache
            code = tsdbRowCachePutToLRU(pTsdb, &rowKey, &newLastRow, 1);
            if (code != TSDB_CODE_SUCCESS) {
              taosMemoryFree(pNewRow);
              tsdbFreeLastRow(pLastRow);
              TAOS_CHECK_GOTO(code, &lino, _exit);
            }
          }

          taosArrayDestroy(colVals);
        }
      } else {
        // Column doesn't exist in row, just delete the column status if present
        bool statusUpdated = false;
        for (int k = 0; k < pLastRow->numCols; k++) {
          if (pLastRow->colStatus && pLastRow->colStatus[k].cid == cid) {
            // Remove this column status entry
            memmove(&pLastRow->colStatus[k], &pLastRow->colStatus[k + 1],
                    (pLastRow->numCols - k - 1) * sizeof(SColCacheStatus));
            pLastRow->numCols--;
            statusUpdated = true;
            break;
          }
        }

        if (statusUpdated) {
          pLastRow->dirty = 1;
          // The row is already in LRU, just mark it as dirty
        }
      }
    }
    tsdbLRUCacheRelease(pTsdb->lruCache, h, false);
  }
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s.", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  taosMemoryFree(pTOldSchema);
  taosMemoryFree(pNewTSchema);
  rocksdb_free(values_list[0]);
  if (values_list) {
    taosMemoryFree(values_list);
  }
  if (values_list_sizes) {
    taosMemoryFree(values_list_sizes);
  }
  TAOS_RETURN(code);
}

int32_t tsdbCacheGetBatchFromRowLru(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SCacheRowsReader *pr,
                                    int8_t ltype) {
  int32_t    code = 0, lino = 0;
  SLRUCache *pCache = pTsdb->lruCache;
  SArray    *pCidList = pr->pCidList;
  int        numKeys = TARRAY_SIZE(pCidList);
  SArray    *remainCols = NULL;  // Track columns that need to be loaded from subsequent layers

  STSchema *pTSchema = NULL;
  code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, pr->info.suid, uid, -1, &pTSchema);
  if (code != 0) {
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  // Step 1: Try to get from row-based LRU cache
  SLastRowKey rowKey = {.lflag = ltype, .uid = uid};
  LRUHandle  *h = taosLRUCacheLookup(pCache, &rowKey, ROCKS_ROW_KEY_LEN);
  SLastRow   *pLastRow = h ? (SLastRow *)taosLRUCacheValue(pCache, h) : NULL;

  // Initialize remainCols with all requested columns
  remainCols = taosArrayInit(numKeys, sizeof(SIdxKey));
  if (!remainCols) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  for (int i = 0; i < numKeys; ++i) {
    int16_t  cid = ((int16_t *)TARRAY_DATA(pCidList))[i];
    SLastKey key = {.lflag = ltype, .uid = uid, .cid = cid};
    SIdxKey  idxKey = {.idx = i, .key = key};
    taosArrayPush(remainCols, &idxKey);
  }

  if (h && pLastRow) {
    // Found valid row in LRU cache, check each column and remove valid ones from remainCols

    // Create a new array to store columns that are still invalid after LRU check
    SArray *newRemainCols = taosArrayInit(numKeys, sizeof(SIdxKey));
    if (!newRemainCols) {
      tsdbLRUCacheRelease(pCache, h, false);
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }

    // Check each column in remainCols
    for (int i = 0; i < TARRAY_SIZE(remainCols); ++i) {
      SIdxKey  *idxKey = (SIdxKey *)taosArrayGet(remainCols, i);
      int16_t   cid = idxKey->key.cid;
      STColumn *pCol = NULL;
      int       colIndex = -1;

      // Find column in schema
      for (int j = 0; j < pTSchema->numOfCols; ++j) {
        if (pTSchema->columns[j].colId == cid) {
          pCol = &pTSchema->columns[j];
          colIndex = j;
          break;
        }
      }

      // Check column-specific cache status
      SColCacheStatus *colStatus = tsdbLastRowGetLastColValColStatus(pLastRow, cid);

      if (colStatus == NULL || colStatus->status == TSDB_LAST_CACHE_NO_CACHE) {
        // Column is invalid in LRU cache, keep it in remainCols for RocksDB/TSDB loading
        int16_t  cid = ((int16_t *)TARRAY_DATA(pCidList))[i];
        SLastCol noneCol = {.rowKey.ts = TSKEY_MIN,
                            .colVal = COL_VAL_NONE(cid, pr->pSchema->columns[pr->pSlotIds[i]].type),
                            .cacheStatus = TSDB_LAST_CACHE_NO_CACHE};

        if (taosArrayPush(pLastArray, &noneCol) == NULL) {
          code = terrno;
          taosArrayDestroy(newRemainCols);
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
        taosArrayPush(newRemainCols, idxKey);
        continue;
      }

      // Column is valid in LRU cache, extract it and add to result
      SLastCol lastCol;
      lastCol.rowKey = pLastRow->rowKey.key;
      lastCol.rowKey.ts = colStatus->lastTs;

      lastCol.cacheStatus = colStatus->status;

      if (pCol && colIndex >= 0) {
        // Extract column value from row
        SColVal colVal;
        code = tRowGetLastColVal(pLastRow->pRow, pTSchema, colIndex, &colVal);
        if (code == 0) {
          lastCol.colVal = colVal;
          code = tsdbCacheReallocSLastCol(&lastCol, NULL);
          if (code != 0) {
            tsdbLRUCacheRelease(pCache, h, false);
            taosArrayDestroy(newRemainCols);
            TAOS_CHECK_GOTO(code, &lino, _exit);
          }

        } else {
          // Column not found in row, set as null
          lastCol.colVal = COL_VAL_NONE(cid, pCol->type);
        }
      } else {
        // Column not found in schema, set as null
        lastCol.colVal = COL_VAL_NONE(cid, TSDB_DATA_TYPE_NULL);
      }

      // Add valid column to result array
      if (taosArrayPush(pLastArray, &lastCol) == NULL) {
        tsdbLRUCacheRelease(pCache, h, false);
        taosArrayDestroy(newRemainCols);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
    }

    tsdbLRUCacheRelease(pCache, h, false);

    // Replace remainCols with newRemainCols (contains only invalid columns)
    taosArrayDestroy(remainCols);
    remainCols = newRemainCols;
  } else {
    // Row not found in LRU cache, all columns remain in remainCols for RocksDB/TSDB loading
    if (h) {
      tsdbLRUCacheRelease(pCache, h, false);
    }

    // Fill with NONE values first
    for (int i = 0; i < numKeys; ++i) {
      int16_t  cid = ((int16_t *)TARRAY_DATA(pCidList))[i];
      SLastCol noneCol = {.rowKey.ts = TSKEY_MIN,
                          .colVal = COL_VAL_NONE(cid, pr->pSchema->columns[pr->pSlotIds[i]].type),
                          .cacheStatus = TSDB_LAST_CACHE_NO_CACHE};

      if (taosArrayPush(pLastArray, &noneCol) == NULL) {
        code = terrno;
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
    }
  }

  // Step 2: If remainCols > 0, try to load from RocksDB
  if (remainCols && TARRAY_SIZE(remainCols) > 0) {
    (void)taosThreadMutexLock(&pTsdb->lruMutex);

    // Double check LRU cache after acquiring mutex
    h = taosLRUCacheLookup(pCache, &rowKey, ROCKS_ROW_KEY_LEN);
    pLastRow = h ? (SLastRow *)taosLRUCacheValue(pCache, h) : NULL;

    if (h && pLastRow && pLastRow->pRow) {
      SArray *newRemainCols = taosArrayInit(TARRAY_SIZE(remainCols), sizeof(SIdxKey));
      if (newRemainCols) {
        // Re-check each column in remainCols
        for (int i = 0; i < TARRAY_SIZE(remainCols); ++i) {
          SIdxKey  *idxKey = (SIdxKey *)taosArrayGet(remainCols, i);
          int16_t   cid = idxKey->key.cid;
          STColumn *pCol = NULL;
          int       colIndex = -1;

          // Find column in schema
          for (int j = 0; j < pTSchema->numOfCols; ++j) {
            if (pTSchema->columns[j].colId == cid) {
              pCol = &pTSchema->columns[j];
              colIndex = j;
              break;
            }
          }

          // Check if this column is still invalid in the cached row
          SColCacheStatus *colStatus = tsdbLastRowGetLastColValColStatus(pLastRow, cid);
          if (colStatus == NULL || colStatus->status == TSDB_LAST_CACHE_NO_CACHE) {
            // Column is still invalid, keep it in remainCols
            taosArrayPush(newRemainCols, idxKey);
            continue;
          }

          // Column is now valid, extract from cached row
          SLastCol lastCol;
          lastCol.rowKey = pLastRow->rowKey.key;
          lastCol.rowKey.ts = colStatus->lastTs;
          lastCol.cacheStatus = colStatus->status;

          if (pCol && colIndex >= 0) {
            SColVal colVal;
            code = tRowGetLastColVal(pLastRow->pRow, pTSchema, colIndex, &colVal);
            if (code == 0) {
              lastCol.colVal = colVal;
              code = tsdbCacheReallocSLastCol(&lastCol, NULL);
              if (code != 0) {
                tsdbLRUCacheRelease(pCache, h, false);
                taosArrayDestroy(newRemainCols);
                (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
                TAOS_CHECK_GOTO(code, &lino, _exit);
              }
            } else {
              lastCol.colVal = COL_VAL_NONE(cid, pCol->type);
            }
          } else {
            lastCol.colVal = COL_VAL_NONE(cid, TSDB_DATA_TYPE_NULL);
          }

          // Add to result array
          taosArraySet(pLastArray, idxKey->idx, &lastCol);
        }

        // Update remainCols with only invalid columns
        taosArrayDestroy(remainCols);
        remainCols = newRemainCols;
      }
      tsdbLRUCacheRelease(pCache, h, false);
    } else {
      // Not in LRU, try RocksDB
      if (h) {
        tsdbLRUCacheRelease(pCache, h, false);
      }

      rocksMayWrite(pTsdb, true);  // flush writebatch cache

      // Load from RocksDB
      const char *keys_list[1] = {(const char *)&rowKey};
      size_t      keys_list_sizes[1] = {ROCKS_ROW_KEY_LEN};
      char      **values_list = NULL;
      size_t     *values_list_sizes = NULL;

      code = tsdbCacheGetValuesFromRocks(pTsdb, 1, keys_list, keys_list_sizes, &values_list, &values_list_sizes);
      if (code) {
        TAOS_RETURN(code);
      }
      if (values_list && values_list[0]) {
        // Found in RocksDB, deserialize and extract columns
        SLastRow *pRocksRow = NULL;
        code = tsdbRowCacheDeserialize(values_list[0], values_list_sizes[0], &pRocksRow);
        if (code == 0 && pRocksRow) {
          // Put back to LRU
          code = tsdbRowCachePutToLRU(pTsdb, &rowKey, pRocksRow, 0);

          if (pRocksRow->pRow) {
            // Extract only the invalid columns from RocksDB row
            SArray *newRemainCols = taosArrayInit(TARRAY_SIZE(remainCols), sizeof(SIdxKey));
            if (newRemainCols) {
              // Check each column in remainCols
              for (int i = 0; i < TARRAY_SIZE(remainCols); ++i) {
                SIdxKey  *idxKey = (SIdxKey *)taosArrayGet(remainCols, i);
                int16_t   cid = idxKey->key.cid;
                STColumn *pCol = NULL;
                int       colIndex = -1;

                // Find column in schema
                for (int j = 0; j < pTSchema->numOfCols; ++j) {
                  if (pTSchema->columns[j].colId == cid) {
                    pCol = &pTSchema->columns[j];
                    colIndex = j;
                    break;
                  }
                }

                // Check column status in RocksDB row
                SColCacheStatus *colStatus = tsdbLastRowGetLastColValColStatus(pRocksRow, cid);
                if (colStatus == NULL || colStatus->status == TSDB_LAST_CACHE_NO_CACHE) {
                  // Column is still invalid in RocksDB, keep it for TSDB loading
                  taosArrayPush(newRemainCols, idxKey);
                  continue;
                }

                // Column is valid in RocksDB, extract it
                SLastCol lastCol;
                lastCol.rowKey = pRocksRow->rowKey.key;
                lastCol.rowKey.ts = colStatus->lastTs;
                lastCol.cacheStatus = colStatus->status;

                if (pCol && colIndex >= 0) {
                  SColVal colVal;
                  code = tRowGetLastColVal(pRocksRow->pRow, pTSchema, colIndex, &colVal);
                  if (code == 0) {
                    lastCol.colVal = colVal;
                    code = tsdbCacheReallocSLastCol(&lastCol, NULL);
                    if (code != 0) {
                      taosArrayDestroy(newRemainCols);
                      (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
                      TAOS_CHECK_GOTO(code, &lino, _exit);
                    }
                  } else {
                    lastCol.colVal = COL_VAL_NONE(cid, pCol->type);
                  }
                } else {
                  lastCol.colVal = COL_VAL_NONE(cid, TSDB_DATA_TYPE_NULL);
                }

                // Add to result array
                taosArraySet(pLastArray, idxKey->idx, &lastCol);
              }

              // Update remainCols with only invalid columns
              taosArrayDestroy(remainCols);
              remainCols = newRemainCols;
            }
          }

          // Free the deserialized row
          if (pRocksRow->pRow) {
            taosMemoryFree(pRocksRow->pRow);
          }
          taosMemoryFree(pRocksRow);
        }
      } else {
        // Not in RocksDB either, remainCols still contains columns that need TSDB loading
        // These will be processed in Step 3 below
      }

      // Clean up RocksDB values
      if (values_list) {
#ifdef USE_ROCKSDB
        for (int i = 0; i < 1; ++i) {
          if (values_list[i]) {
            rocksdb_free(values_list[i]);
          }
        }
#endif
        taosMemoryFree(values_list);
      }
      if (values_list_sizes) {
        taosMemoryFree(values_list_sizes);
      }
    }

    (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
  }

  // Step 3: If remainCols still > 0, load from TSDB and update last cache in lru&rocksdb.
  if (remainCols && TARRAY_SIZE(remainCols) > 0) {
    code = tsdbCacheLoadFromRaw(pTsdb, uid, pLastArray, remainCols, pr, ltype);
    if (code) {
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }

    SArray *colVals = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
    if (!colVals) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }

    for (int j = 0; j < pTSchema->numOfCols; j++) {
      SColVal colVal = {0};
      if (pLastRow) {
        code = tRowGetLastColVal(pLastRow->pRow, pTSchema, j, &colVal);
        if (code != TSDB_CODE_SUCCESS) {
          taosArrayDestroy(colVals);
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
      } else {
        colVal = COL_VAL_NONE(pTSchema->columns[j].colId, pTSchema->columns[j].type);
      }

      if (!taosArrayPush(colVals, &colVal)) {
        taosArrayDestroy(colVals);
        code = terrno;
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
    }
    SColCacheStatus *colStatus = taosMemoryMalloc(pTSchema->numOfCols * sizeof(SColCacheStatus));
    if (!colStatus) {
      taosArrayDestroy(colVals);
      code = TSDB_CODE_OUT_OF_MEMORY;
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }
    if (pLastRow != NULL && pLastRow->colStatus != NULL) {
      memcpy(colStatus, pLastRow->colStatus, pLastRow->numCols * sizeof(SColCacheStatus));
    } else {
      for (int32_t i = 0; i < pTSchema->numOfCols; i++) {
        colStatus[i].cid = pTSchema->columns[i].colId;
        colStatus[i].status = TSDB_LAST_CACHE_NO_CACHE;
        colStatus[i].lastTs = TSKEY_MIN;  // Initialize to minimum timestamp
      }
    }

    int32_t nums = TARRAY_SIZE(remainCols);
    for (int i = 0; i < nums; i++) {
      SIdxKey  *idxKey = taosArrayGet(remainCols, i);
      int16_t   cid = idxKey->key.cid;
      SLastCol *lastCol = (SLastCol *)taosArrayGet(pLastArray, idxKey->idx);
      for (int j = 0; j < pTSchema->numOfCols; j++) {
        if (pTSchema->columns[j].colId == cid) {
          taosArraySet(colVals, j, &lastCol->colVal);
          break;
        }
      }
      for (int j = 0; j < pTSchema->numOfCols; j++) {
        if (colStatus[j].cid == cid) {
          colStatus[j].status = TSDB_LAST_CACHE_VALID;
          colStatus[j].lastTs = lastCol->rowKey.ts;
          break;
        }
      }
    }

    // build new SRow.
    SRow             *pNewRow = NULL;
    SRowBuildScanInfo pScanInfo = {0};
    code = tRowBuild(colVals, pTSchema, &pNewRow, &pScanInfo);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(colStatus);
      taosArrayDestroy(colVals);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }

    if (!pNewRow) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      taosMemoryFree(colStatus);
      taosArrayDestroy(colVals);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }

    // Update the cached row
    SRowKey     emptyRowKey = {.ts = TSKEY_MIN, .numOfPKs = 0};
    STsdbRowKey stsdbRowKey = {.key = emptyRowKey};

    SLastRow newLastRow = {
        .rowKey = stsdbRowKey, .pRow = pNewRow, .dirty = 1, .numCols = pTSchema->numOfCols, .colStatus = colStatus};

    // Update rocksdb cache
    code = tsdbRowCachePutToRocksdb(pTsdb, &rowKey, &newLastRow);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pNewRow);
      taosArrayDestroy(colVals);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }

    // Update LRU cache
    code = tsdbRowCachePutToLRU(pTsdb, &rowKey, &newLastRow, 1);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pNewRow);
      taosArrayDestroy(colVals);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }

    taosMemoryFree(colStatus);
    taosMemoryFree(pNewRow);
    taosArrayDestroy(colVals);
  }

  // Add memory query logic if tsUpdateCacheBatch is enabled
  if (tsUpdateCacheBatch) {
    // Create and populate keyArray for compatibility with tsdbCacheGetBatchFromMem
    SArray *keyArray = taosArrayInit(16, sizeof(SLastKey));
    if (!keyArray) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }

    // Fill keyArray with keys for all columns being queried
    SArray *pCidList = pr->pCidList;
    int     numKeys = TARRAY_SIZE(pCidList);
    for (int i = 0; i < numKeys; ++i) {
      int16_t cid = ((int16_t *)TARRAY_DATA(pCidList))[i];

      SLastKey key = {.lflag = ltype, .uid = uid, .cid = cid};
      // Handle function type for last_row vs last
      int32_t funcType = FUNCTION_TYPE_CACHE_LAST;
      if (pr->pFuncTypeList != NULL && taosArrayGetSize(pr->pFuncTypeList) > i) {
        funcType = ((int32_t *)TARRAY_DATA(pr->pFuncTypeList))[i];
      }
      if (((pr->type & CACHESCAN_RETRIEVE_LAST) == CACHESCAN_RETRIEVE_LAST) &&
          FUNCTION_TYPE_CACHE_LAST_ROW == funcType) {
        int8_t tempType = CACHESCAN_RETRIEVE_LAST_ROW | (pr->type ^ CACHESCAN_RETRIEVE_LAST);
        key.lflag = (tempType & CACHESCAN_RETRIEVE_LAST) >> 3;
      }

      if (!taosArrayPush(keyArray, &key)) {
        taosArrayDestroy(keyArray);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
    }

    // Use the existing tsdbCacheGetBatchFromMem function which works for both row and col cache
    code = tsdbCacheGetBatchFromMem(pTsdb, uid, pLastArray, pr, keyArray);
    taosArrayDestroy(keyArray);
    if (code) {
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }
  }

_exit:
  // Clean up remainCols if it exists
  if (remainCols) {
    taosArrayDestroy(remainCols);
  }
  taosMemFree(pTSchema);

  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

int32_t tsdbRowCacheDel(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey) {
  int32_t   code = 0, lino = 0;
  STSchema *pTSchema = NULL;
  int       sver = -1;
  int       numKeys = 0;
  SArray   *remainKeys = NULL;

  TAOS_CHECK_RETURN(metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, sver, &pTSchema));

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  // For row-based cache, we only need to handle LAST_ROW and LAST row keys
  for (int8_t lflag = LFLAG_LAST_ROW; lflag <= LFLAG_LAST; ++lflag) {
    SLastRowKey rowKey = {.lflag = lflag, .uid = uid};
    LRUHandle  *h = taosLRUCacheLookup(pTsdb->lruCache, &rowKey, ROCKS_ROW_KEY_LEN);

    if (h) {
      SLastRow *pLastRow = (SLastRow *)taosLRUCacheValue(pTsdb->lruCache, h);
      // Check if the row timestamp falls within the deletion range
      if (pLastRow && pLastRow->rowKey.key.ts <= eKey && pLastRow->rowKey.key.ts >= sKey) {
        // Mark all columns in this row as invalid/no-cache
        bool needUpdate = false;
        for (int32_t i = 0; i < pLastRow->numCols; i++) {
          if (pLastRow->colStatus[i].status != TSDB_LAST_CACHE_NO_CACHE) {
            pLastRow->colStatus[i].status = TSDB_LAST_CACHE_NO_CACHE;
            pLastRow->colStatus[i].lastTs = TSKEY_MIN;
            needUpdate = true;
          }
        }

        if (needUpdate) {
          pLastRow->dirty = 1;
          // Put the updated row back to cache
          code = tsdbRowCachePutToLRU(pTsdb, &rowKey, pLastRow, 1);
        }
      }
      tsdbLRUCacheRelease(pTsdb->lruCache, h, false);
      TAOS_CHECK_EXIT(code);
    } else {
      // Row not in LRU cache, add to list for RocksDB lookup
      if (!remainKeys) {
        remainKeys = taosArrayInit(2, sizeof(SLastRowKey));
      }
      if (!taosArrayPush(remainKeys, &rowKey)) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  if (remainKeys) {
    numKeys = TARRAY_SIZE(remainKeys);
  }

  char  **keys_list = taosMemoryCalloc(numKeys, sizeof(char *));
  size_t *keys_list_sizes = taosMemoryCalloc(numKeys, sizeof(size_t));
  char  **values_list = NULL;
  size_t *values_list_sizes = NULL;

  if (!keys_list || !keys_list_sizes) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }
  const size_t klen = ROCKS_ROW_KEY_LEN;

  for (int i = 0; i < numKeys; ++i) {
    char *key = taosMemoryCalloc(1, sizeof(SLastRowKey));
    if (!key) {
      code = terrno;
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }
    SLastRowKey *pRowKey = taosArrayGet(remainKeys, i);

    ((SLastRowKey *)key)[0] = *pRowKey;

    keys_list[i] = key;
    keys_list_sizes[i] = klen;
  }

  rocksMayWrite(pTsdb, true);  // flush writebatch cache

  TAOS_CHECK_GOTO(tsdbCacheGetValuesFromRocks(pTsdb, numKeys, (const char *const *)keys_list, keys_list_sizes,
                                              &values_list, &values_list_sizes),
                  NULL, _exit);

  // Process rows from RocksDB
  for (int i = 0; i < numKeys; ++i) {
    SLastRow *pLastRow = NULL;
    if (values_list[i] != NULL) {
      code = tsdbRowCacheDeserialize(values_list[i], values_list_sizes[i], &pLastRow);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbError("vgId:%d, %s deserialize failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
    }

    SLastRowKey *pRowKey = taosArrayGet(remainKeys, i);
    if (NULL != pLastRow && (pLastRow->rowKey.key.ts <= eKey && pLastRow->rowKey.key.ts >= sKey)) {
      // Mark all columns in this row as invalid/no-cache
      bool needUpdate = false;
      for (int32_t j = 0; j < pLastRow->numCols; j++) {
        if (pLastRow->colStatus[j].status != TSDB_LAST_CACHE_NO_CACHE) {
          pLastRow->colStatus[j].status = TSDB_LAST_CACHE_NO_CACHE;
          pLastRow->colStatus[j].lastTs = TSKEY_MIN;
          needUpdate = true;
        }
      }

      if (needUpdate) {
        pLastRow->dirty = 1;

        // Update both RocksDB and LRU cache
        if ((code = tsdbRowCachePutToRocksdb(pTsdb, pRowKey, pLastRow)) != TSDB_CODE_SUCCESS) {
          taosMemoryFreeClear(pLastRow);
          tsdbError("tsdb/cache/del: vgId:%d, put to rocks failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
        if ((code = tsdbRowCachePutToLRU(pTsdb, pRowKey, pLastRow, 0)) != TSDB_CODE_SUCCESS) {
          taosMemoryFreeClear(pLastRow);
          tsdbError("tsdb/cache/del: vgId:%d, put to lru failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
      }
    }

    if (pLastRow == NULL) {
      tsdbDebug("tsdb/cache/del: vgId:%d, no cache found for uid:%" PRId64 ", lflag:%d.", TD_VID(pTsdb->pVnode),
                pRowKey->uid, pRowKey->lflag);
    }

    taosMemoryFreeClear(pLastRow);
  }

  rocksMayWrite(pTsdb, false);

_exit:
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  for (int i = 0; i < numKeys; ++i) {
    taosMemoryFree(keys_list[i]);
  }
  taosMemoryFree(keys_list);
  taosMemoryFree(keys_list_sizes);
  if (values_list) {
#if USE_ROCKSDB
    for (int i = 0; i < numKeys; ++i) {
      rocksdb_free(values_list[i]);
    }
#endif
    taosMemoryFree(values_list);
  }
  taosMemoryFree(values_list_sizes);
  taosArrayDestroy(remainKeys);
  taosMemoryFree(pTSchema);

  TAOS_RETURN(code);
}