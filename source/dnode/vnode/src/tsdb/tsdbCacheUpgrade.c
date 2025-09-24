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

#include "meta.h"
#include "tdb.h"
#include "tsdbCacheInt.h"

// Function to check if cache upgrade is needed
bool tsdbNeedCacheUpgrade(SVnode *pVnode) {
  if (!pVnode) {
    return false;
  }

  // If cache format is not set (0) or explicitly set to colCache (0),
  // and we want to use rowCache, then upgrade is needed
  return (pVnode->config.cacheFormat == TSDB_CACHE_FORMAT_COL);
}

// Function to set cache format in vnode config and persist it
int32_t tsdbSetCacheFormat(SVnode *pVnode, int8_t cacheFormat) {
  if (!pVnode) {
    return TSDB_CODE_INVALID_PARA;
  }

  pVnode->config.cacheFormat = cacheFormat;

  // Persist the configuration to disk
  // Note: This will be handled by the vnode's config persistence mechanism
  // The actual persistence happens when vnode config is saved

  return TSDB_CODE_SUCCESS;
}

// Function to iterate through all tables in a vnode
typedef int32_t (*TableIteratorCallback)(SVnode *pVnode, tb_uid_t uid, tb_uid_t suid, STSchema *pTSchema,
                                         void *pUserData);

int32_t tsdbIterateAllTables(SVnode *pVnode, TableIteratorCallback callback, void *pUserData) {
  int32_t code = 0, lino = 0;

  if (!pVnode || !pVnode->pMeta || !callback) {
    return TSDB_CODE_INVALID_PARA;
  }

  SMeta *pMeta = pVnode->pMeta;

  // Use TDB cursor to traverse all tables
  TBC *pCur;
  code = tdbTbcOpen(pMeta->pTbDb, &pCur, NULL);
  if (code < 0) {
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  code = tdbTbcMoveToFirst(pCur);
  if (code) {
    tdbTbcClose(pCur);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  void *pData = NULL, *pKey = NULL;
  int   nData = 0, nKey = 0;

  while (1) {
    int32_t ret = tdbTbcNext(pCur, &pKey, &nKey, &pData, &nData);
    if (ret < 0) {
      break;
    }

    SMetaEntry me = {0};
    SDecoder   dc = {0};
    tDecoderInit(&dc, pData, nData);
    code = metaDecodeEntry(&dc, &me);
    if (code < 0) {
      tDecoderClear(&dc);
      TAOS_CHECK_GOTO(code, &lino, _exit);
      break;
    }

    // Process child tables and normal tables (skip super tables)
    if (me.type == TSDB_CHILD_TABLE || me.type == TSDB_NORMAL_TABLE) {
      STSchema *pTSchema = NULL;
      tb_uid_t  suid = 0;

      if (me.type == TSDB_CHILD_TABLE) {
        suid = me.ctbEntry.suid;
      }

      // Get table schema
      if (metaGetTbTSchemaEx(pMeta, suid, me.uid, -1, &pTSchema) == 0) {
        code = callback(pVnode, me.uid, suid, pTSchema, pUserData);
        taosMemoryFree(pTSchema);

        if (code != TSDB_CODE_SUCCESS) {
          tDecoderClear(&dc);
          break;
        }
      }
    }

    tDecoderClear(&dc);
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, failed at %s:%d to iterate all tables since %s", TD_VID(pVnode), __FILE__, lino,
              tstrerror(code));
  }

  tdbFree(pData);
  tdbFree(pKey);
  tdbTbcClose(pCur);
  return code;
}

// Callback structure for cache upgrade
typedef struct {
  STsdb                 *pTsdb;
  rocksdb_t             *pColDB;
  rocksdb_readoptions_t *pColReadoptions;
} SCacheUpgradeCtx;

// Function to get values from column cache RocksDB in batch
static int32_t tsdbCacheGetValuesFromColRocks(STsdb *pTsdb, rocksdb_t *pColDB, rocksdb_readoptions_t *pColReadoptions,
                                               size_t numKeys, const char *const *ppKeysList, size_t *pKeysListSizes,
                                               char ***pppValuesList, size_t **ppValuesListSizes) {
#ifdef USE_ROCKSDB
  if (!pColDB || !pColReadoptions) {
    return TSDB_CODE_INVALID_PARA;
  }

  char **valuesList = taosMemoryCalloc(numKeys, sizeof(char *));
  if (!valuesList) return terrno;
  
  size_t *valuesListSizes = taosMemoryCalloc(numKeys, sizeof(size_t));
  if (!valuesListSizes) {
    taosMemoryFreeClear(valuesList);
    return terrno;
  }
  
  char **errs = taosMemoryCalloc(numKeys, sizeof(char *));
  if (!errs) {
    taosMemoryFreeClear(valuesList);
    taosMemoryFreeClear(valuesListSizes);
    return terrno;
  }
  
  rocksdb_multi_get(pColDB, pColReadoptions, numKeys, ppKeysList, pKeysListSizes, valuesList,
                    valuesListSizes, errs);
  
  for (size_t i = 0; i < numKeys; ++i) {
    rocksdb_free(errs[i]);
  }
  taosMemoryFreeClear(errs);

  *pppValuesList = valuesList;
  *ppValuesListSizes = valuesListSizes;
#endif
  return TSDB_CODE_SUCCESS;
}

// Function to query column cache entries for a table with specific flag
int32_t tsdbQueryColCacheForTable(STsdb *pTsdb, tb_uid_t uid, STSchema *pTSchema, int8_t lflag, SArray **ppColArray,
                                  SArray **ppColStatus, rocksdb_t *pColDB, rocksdb_readoptions_t *pColReadoptions) {
  int32_t code = 0, lino = 0;

  if (!pTsdb || !pTSchema) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_PARA, &lino, _exit);
  }

  // Initialize column array and status array
  *ppColArray = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
  if (!*ppColArray) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  *ppColStatus = taosArrayInit(pTSchema->numOfCols, sizeof(SColCacheStatus));
  if (!*ppColStatus) {
    taosArrayDestroy(*ppColArray);
    *ppColArray = NULL;
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  // Prepare batch keys for RocksDB query
  int     numKeys = pTSchema->numOfCols;
  char  **keys_list = taosMemoryMalloc(numKeys * sizeof(char *));
  size_t *keys_list_sizes = taosMemoryMalloc(numKeys * sizeof(size_t));
  char   *key_list = taosMemoryMalloc(numKeys * ROCKS_KEY_LEN);
  
  if (!keys_list || !keys_list_sizes || !key_list) {
    taosMemoryFree(keys_list);
    taosMemoryFree(keys_list_sizes);
    taosMemoryFree(key_list);
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  // Build batch keys
  for (int32_t i = 0; i < numKeys; ++i) {
    int16_t cid = pTSchema->columns[i].colId;
    SLastKey key = {.uid = uid, .cid = cid, .lflag = lflag};
    
    memcpy(key_list + i * ROCKS_KEY_LEN, &key, ROCKS_KEY_LEN);
    keys_list[i] = key_list + i * ROCKS_KEY_LEN;
    keys_list_sizes[i] = ROCKS_KEY_LEN;
  }

  // Batch query from column cache RocksDB
  char  **values_list = NULL;
  size_t *values_list_sizes = NULL;
  
#ifdef USE_ROCKSDB
  if (pColDB && pColReadoptions) {
    code = tsdbCacheGetValuesFromColRocks(pTsdb, pColDB, pColReadoptions, numKeys, 
                                          (const char *const *)keys_list, keys_list_sizes, 
                                          &values_list, &values_list_sizes);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(key_list);
      taosMemoryFree(keys_list);
      taosMemoryFree(keys_list_sizes);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }
  }
#endif

  // Process results for each column
  for (int32_t i = 0; i < numKeys; i++) {
    int16_t cid = pTSchema->columns[i].colId;
    int8_t  colType = pTSchema->columns[i].type;
    bool    found = false;
    
    // Check if we got a value from RocksDB
    if (values_list && values_list[i] && values_list_sizes[i] > 0) {
      SLastCol *pLastCol = NULL;
      int32_t   deserCode = tsdbCacheDeserialize(values_list[i], values_list_sizes[i], &pLastCol);
      
      if (deserCode == TSDB_CODE_SUCCESS && pLastCol && pLastCol->cacheStatus != TSDB_LAST_CACHE_NO_CACHE) {
        // Add valid column value from RocksDB
        if (!taosArrayPush(*ppColArray, &pLastCol->colVal)) {
          taosMemoryFreeClear(pLastCol);
          code = terrno;
          goto _cleanup;
        }

        // Set column status as valid
        SColCacheStatus colStatus = {.cid = cid, .status = TSDB_LAST_CACHE_VALID, .lastTs = pLastCol->rowKey.ts};
        if (!taosArrayPush(*ppColStatus, &colStatus)) {
          taosMemoryFreeClear(pLastCol);
          code = terrno;
          goto _cleanup;
        }
        found = true;
      }
      taosMemoryFreeClear(pLastCol);
    }

    if (!found) {
      // Initialize NONE column value for columns without cache
      SColVal noneColVal = COL_VAL_NONE(cid, colType);
      if (!taosArrayPush(*ppColArray, &noneColVal)) {
        code = terrno;
        goto _cleanup;
      }

      // Set column status as no cache
      SColCacheStatus colStatus = {.cid = cid, .status = TSDB_LAST_CACHE_NO_CACHE, .lastTs = TSKEY_MIN};
      if (!taosArrayPush(*ppColStatus, &colStatus)) {
        code = terrno;
        goto _cleanup;
      }
    }
  }

_cleanup:
  // Cleanup batch query resources
  taosMemoryFree(key_list);
  taosMemoryFree(keys_list);
  taosMemoryFree(keys_list_sizes);
  
  if (values_list) {
#ifdef USE_ROCKSDB
    for (int i = 0; i < numKeys; ++i) {
      rocksdb_free(values_list[i]);
    }
#endif
    taosMemoryFree(values_list);
  }
  if (values_list_sizes) {
    taosMemoryFree(values_list_sizes);
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, failed at %s:%d to query column cache for table uid:%" PRId64 " lflag:%d since %s",
              TD_VID(pTsdb->pVnode), __FILE__, lino, uid, lflag, tstrerror(code));
    taosArrayDestroy(*ppColArray);
    taosArrayDestroy(*ppColStatus);
    *ppColArray = NULL;
    *ppColStatus = NULL;
  }

  return code;
}

// Function to convert column cache to row cache
int32_t tsdbConvertColCacheToRowCache(STsdb *pTsdb, tb_uid_t uid, tb_uid_t suid, STSchema *pTSchema, int8_t lflag,
                                      SArray *pColArray, SArray *pColStatus) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!pTsdb || !pTSchema || !pColArray || !pColStatus) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t colArraySize = taosArrayGetSize(pColArray);
  if (colArraySize != pTSchema->numOfCols) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Find the latest timestamp from valid columns
  STsdbRowKey rowKey = {.key.ts = TSKEY_MIN, .key.numOfPKs = 0};

  for (int32_t i = 0; i < colArraySize; i++) {
    SColCacheStatus *pStatus = taosArrayGet(pColStatus, i);
    if (pStatus && pStatus->status == TSDB_LAST_CACHE_VALID) {
      if (pStatus->lastTs > rowKey.key.ts) {
        rowKey.key.ts = pStatus->lastTs;
      }
    }
  }

  // Build row from column values
  SRow             *pRow = NULL;
  SRowBuildScanInfo sinfo = {0};

  code = tRowBuild(pColArray, pTSchema, &pRow, &sinfo);
  if (code == TSDB_CODE_SUCCESS && pRow) {
    // Create SLastRow structure
    SLastRow lastRow = {
        .rowKey = rowKey,
        .pRow = pRow,
        .dirty = 1,
        .numCols = pTSchema->numOfCols,
        .colStatus = (SColCacheStatus *)TARRAY_DATA(pColStatus),
    };

    // Write to row cache
    SLastRowKey rowCacheKey = {.uid = uid, .lflag = lflag};
    code = tsdbRowCachePutToRocksdb(pTsdb, &rowCacheKey, &lastRow);

    taosMemoryFree(pRow);
  }

  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, failed at %s:%d to convert column cache to row cache for table uid:%" PRId64
              " lflag:%d since %s",
              TD_VID(pTsdb->pVnode), __FILE__, __LINE__, uid, lflag, tstrerror(code));
  }

  return code;
}

// Callback function for upgrading each table's cache
int32_t tsdbUpgradeTableCacheCallback(SVnode *pVnode, tb_uid_t uid, tb_uid_t suid, STSchema *pTSchema,
                                      void *pUserData) {
  int32_t           code = 0;
  SCacheUpgradeCtx *pCtx = (SCacheUpgradeCtx *)pUserData;

  if (!pCtx || !pCtx->pTsdb) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Process both LAST_ROW and LAST cache types
  for (int8_t lflag = LFLAG_LAST_ROW; lflag <= LFLAG_LAST; lflag++) {
    SArray *pColArray = NULL;
    SArray *pColStatus = NULL;

    // Query column cache for this table with specific flag
    code = tsdbQueryColCacheForTable(pCtx->pTsdb, uid, pTSchema, lflag, &pColArray, &pColStatus, pCtx->pColDB,
                                     pCtx->pColReadoptions);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbError("vgId:%d, failed to query column cache for table uid:%" PRId64 " lflag:%d since %s", TD_VID(pVnode),
                uid, lflag, tstrerror(code));
      continue;  // Continue with next flag
    }

    // Convert column cache to row cache
    code = tsdbConvertColCacheToRowCache(pCtx->pTsdb, uid, suid, pTSchema, lflag, pColArray, pColStatus);
    if (code == TSDB_CODE_SUCCESS) {
      tsdbDebug("vgId:%d, successfully converted cache for table uid:%" PRId64 " lflag:%d", TD_VID(pVnode), uid, lflag);
    } else {
      tsdbError("vgId:%d, failed to convert column cache for table uid:%" PRId64 " lflag:%d since %s", TD_VID(pVnode),
                uid, lflag, tstrerror(code));
    }

    // Cleanup arrays
    taosArrayDestroy(pColArray);
    taosArrayDestroy(pColStatus);
  }
  // Continue processing even if this table failed
  return TSDB_CODE_SUCCESS;
}

// Main cache upgrade function
int32_t tsdbUpgradeCache(STsdb *pTsdb) {
  int32_t code = 0;

  if (!pTsdb || !pTsdb->pVnode) {
    return TSDB_CODE_INVALID_PARA;
  }
  SVnode *pVnode = pTsdb->pVnode;

  // Check if upgrade is needed
  if (!tsdbNeedCacheUpgrade(pVnode)) {
    tsdbInfo("vgId:%d, cache upgrade not needed, already using row cache format", TD_VID(pVnode));
    return TSDB_CODE_SUCCESS;
  }

  tsdbInfo("vgId:%d, starting cache upgrade from column format to row format", TD_VID(pVnode));

  // Open column cache RocksDB for reading old data
  rocksdb_t             *pColDB = NULL;
  rocksdb_options_t     *pColOptions = NULL;
  rocksdb_readoptions_t *pColReadoptions = NULL;

  code = tsdbOpenColCacheRocksDB(pTsdb, &pColDB, &pColOptions, &pColReadoptions);
  if (code != TSDB_CODE_SUCCESS) {
    if (code == TSDB_CODE_NOT_FOUND) {
      tsdbInfo("vgId:%d, no column cache RocksDB found, nothing to upgrade", TD_VID(pVnode));
      return TSDB_CODE_SUCCESS;
    } else {
      tsdbError("vgId:%d, failed to open column cache RocksDB for upgrade since %s", TD_VID(pVnode), tstrerror(code));
      return code;
    }
  }

  SCacheUpgradeCtx ctx = {.pTsdb = pTsdb, .pColDB = pColDB, .pColReadoptions = pColReadoptions};

  // Check row cache RocksDB health before upgrade
  if (pTsdb->rCache.db == NULL) {
    tsdbError("vgId:%d, row cache RocksDB is not available, cannot perform cache upgrade", TD_VID(pVnode));
    tsdbCloseColCacheRocksDB(pColDB, pColOptions, pColReadoptions);
    return TSDB_CODE_TDB_INIT_FAILED;
  }

  // Enable RocksDB write batching
  rocksMayWrite(pTsdb, true);

  // Iterate through all tables and upgrade their cache
  code = tsdbIterateAllTables(pVnode, tsdbUpgradeTableCacheCallback, &ctx);

  if (code == TSDB_CODE_SUCCESS) {
    // Flush any pending RocksDB writes
    rocksMayWrite(pTsdb, false);

    // Update cache format in configuration
    code = tsdbSetCacheFormat(pVnode, TSDB_CACHE_FORMAT_ROW);
    if (code == TSDB_CODE_SUCCESS) {
      // Close column cache RocksDB before deletion
      tsdbCloseColCacheRocksDB(pColDB, pColOptions, pColReadoptions);
      pColDB = NULL;
      pColOptions = NULL;
      pColReadoptions = NULL;

      // Delete the column cache RocksDB directory
      int32_t deleteCode = tsdbDeleteColCacheRocksDB(pTsdb);
      if (deleteCode == TSDB_CODE_SUCCESS) {
        tsdbInfo("vgId:%d, cache upgrade completed successfully and old column cache deleted", TD_VID(pVnode));
      } else {
        tsdbWarn("vgId:%d, cache upgrade completed but failed to delete old column cache since %s", TD_VID(pVnode),
                 tstrerror(deleteCode));
      }
    } else {
      tsdbError("vgId:%d, failed to update cache format configuration since %s", TD_VID(pVnode), tstrerror(code));
    }
  } else {
    tsdbError("vgId:%d, cache upgrade failed since %s", TD_VID(pVnode), tstrerror(code));
  }

  // Cleanup resources
  if (pColDB) {
    tsdbCloseColCacheRocksDB(pColDB, pColOptions, pColReadoptions);
  }

  return code;
}

