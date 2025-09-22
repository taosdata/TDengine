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
#include "meta.h"
#include "tdb.h"

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
typedef int32_t (*TableIteratorCallback)(SVnode *pVnode, tb_uid_t uid, tb_uid_t suid, 
                                        STSchema *pTSchema, void *pUserData);

int32_t tsdbIterateAllTables(SVnode *pVnode, TableIteratorCallback callback, void *pUserData) {
  int32_t code = 0;
  
  if (!pVnode || !pVnode->pMeta || !callback) {
    return TSDB_CODE_INVALID_PARA;
  }
  
  SMeta *pMeta = pVnode->pMeta;
  
  // Use TDB cursor to traverse all tables
  TBC *pCur;
  code = tdbTbcOpen(pMeta->pTbDb, &pCur, NULL);
  if (code < 0) {
    return code;
  }
  
  code = tdbTbcMoveToFirst(pCur);
  if (code) {
    tdbTbcClose(pCur);
    return code;
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
      break;
    }
    
    // Process child tables and normal tables (skip super tables)
    if (me.type == TSDB_CHILD_TABLE || me.type == TSDB_NORMAL_TABLE) {
      STSchema *pTSchema = NULL;
      tb_uid_t suid = 0;
      
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
  
  tdbFree(pData);
  tdbFree(pKey);
  tdbTbcClose(pCur);
  
  return code;
}

// Callback structure for cache upgrade
typedef struct {
  STsdb *pTsdb;
} SCacheUpgradeCtx;

// Function to query column cache entries for a table with specific flag
int32_t tsdbQueryColCacheForTable(STsdb *pTsdb, tb_uid_t uid, STSchema *pTSchema, 
                                  int8_t lflag, SArray **ppColArray, SArray **ppColStatus) {
  int32_t code = 0;
  int32_t lino = 0;
  
  if (!pTsdb || !pTSchema) {
    return TSDB_CODE_INVALID_PARA;
  }
  
  // Initialize column array and status array
  *ppColArray = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
  if (!*ppColArray) {
    return terrno;
  }
  
  *ppColStatus = taosArrayInit(pTSchema->numOfCols, sizeof(SColCacheStatus));
  if (!*ppColStatus) {
    taosArrayDestroy(*ppColArray);
    *ppColArray = NULL;
    return terrno;
  }
  
  // Query cache entries for each column with the specified flag
  for (int32_t i = 0; i < pTSchema->numOfCols; i++) {
    int16_t cid = pTSchema->columns[i].colId;
    int8_t colType = pTSchema->columns[i].type;
    
    SLastKey key = {.uid = uid, .cid = cid, .lflag = lflag};
    SLastCol *pLastCol = NULL;
    bool found = false;
    
    // First check LRU cache
    LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, &key, ROCKS_KEY_LEN);
    if (h) {
      pLastCol = (SLastCol *)taosLRUCacheValue(pTsdb->lruCache, h);
      if (pLastCol && pLastCol->cacheStatus != TSDB_LAST_CACHE_NO_CACHE) {
        // Add valid column value
        if (!taosArrayPush(*ppColArray, &pLastCol->colVal)) {
          tsdbLRUCacheRelease(pTsdb->lruCache, h, false);
          TAOS_CHECK_GOTO(terrno, &lino, _exit);
        }
        
        // Set column status as valid
        SColCacheStatus colStatus = {
          .cid = cid,
          .status = TSDB_LAST_CACHE_VALID,
          .lastTs = pLastCol->rowKey.ts
        };
        if (!taosArrayPush(*ppColStatus, &colStatus)) {
          tsdbLRUCacheRelease(pTsdb->lruCache, h, false);
          TAOS_CHECK_GOTO(terrno, &lino, _exit);
        }
        found = true;
      }
      tsdbLRUCacheRelease(pTsdb->lruCache, h, false);
    }
    
    if (!found) {
      // Query from RocksDB if not found in LRU
#ifdef USE_ROCKSDB
      char *pKey = (char *)&key;
      size_t keyLen = ROCKS_KEY_LEN;
      char *value = NULL;
      size_t valueLen = 0;
      
      char *err = NULL;
      value = rocksdb_get(pTsdb->rCache.db, pTsdb->rCache.readoptions, 
                         pKey, keyLen, &valueLen, &err);
      if (err) {
        rocksdb_free(err);
      }
      
      if (value && valueLen > 0) {
        // Deserialize RocksDB value
        SLastCol *pLastCol = NULL;
        int32_t deserCode = tsdbCacheDeserialize(value, valueLen, &pLastCol);
        if (deserCode == TSDB_CODE_SUCCESS && pLastCol && 
            pLastCol->cacheStatus != TSDB_LAST_CACHE_NO_CACHE) {
          // Add valid column value from RocksDB
          if (!taosArrayPush(*ppColArray, &pLastCol->colVal)) {
            taosMemoryFreeClear(pLastCol);
            rocksdb_free(value);
            TAOS_CHECK_GOTO(terrno, &lino, _exit);
          }
          
          // Set column status as valid
          SColCacheStatus colStatus = {
            .cid = cid,
            .status = TSDB_LAST_CACHE_VALID,
            .lastTs = pLastCol->rowKey.ts
          };
          if (!taosArrayPush(*ppColStatus, &colStatus)) {
            taosMemoryFreeClear(pLastCol);
            rocksdb_free(value);
            TAOS_CHECK_GOTO(terrno, &lino, _exit);
          }
          found = true;
        }
        taosMemoryFreeClear(pLastCol);
        rocksdb_free(value);
      }
#endif
    }
    
    if (!found) {
      // Initialize NONE column value for columns without cache
      SColVal noneColVal = COL_VAL_NONE(cid, colType);
      if (!taosArrayPush(*ppColArray, &noneColVal)) {
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      
      // Set column status as no cache
      SColCacheStatus colStatus = {
        .cid = cid,
        .status = TSDB_LAST_CACHE_NO_CACHE,
        .lastTs = TSKEY_MIN
      };
      if (!taosArrayPush(*ppColStatus, &colStatus)) {
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
    }
  }
  
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, failed at %s:%d to query column cache for table uid:%" PRId64 " lflag:%d since %s", 
              TD_VID(pTsdb->pVnode), __FILE__, __LINE__, uid, lflag, tstrerror(code));
    taosArrayDestroy(*ppColArray);
    taosArrayDestroy(*ppColStatus);
    *ppColArray = NULL;
    *ppColStatus = NULL;
  }
  
  return code;
}

// Function to convert column cache to row cache
int32_t tsdbConvertColCacheToRowCache(STsdb *pTsdb, tb_uid_t uid, tb_uid_t suid, 
                                     STSchema *pTSchema, int8_t lflag, 
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
  SRow *pRow = NULL;
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
    code = tsdbRowCachePutToLRU(pTsdb, &rowCacheKey, &lastRow, 1);
    if (code == TSDB_CODE_SUCCESS) {
      code = tsdbRowCachePutToRocksdb(pTsdb, &rowCacheKey, &lastRow);
    }
    
    taosMemoryFree(pRow);
  }
  
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, failed at %s:%d to convert column cache to row cache for table uid:%" PRId64 " lflag:%d since %s", 
              TD_VID(pTsdb->pVnode), __FILE__, __LINE__, uid, lflag, tstrerror(code));
  }
  
  return code;
}

// Function to delete old column cache entries
int32_t tsdbDeleteColCacheForTable(STsdb *pTsdb, tb_uid_t uid, STSchema *pTSchema) {
  int32_t code = 0;
  
  if (!pTsdb || !pTSchema) {
    return TSDB_CODE_INVALID_PARA;
  }
  
  // Delete column cache entries for all columns
  for (int32_t i = 0; i < pTSchema->numOfCols; i++) {
    int16_t cid = pTSchema->columns[i].colId;
    
    for (int8_t lflag = LFLAG_LAST_ROW; lflag <= LFLAG_LAST; lflag++) {
      SLastKey key = {.uid = uid, .cid = cid, .lflag = lflag};
      
      // Delete from LRU cache
      LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, &key, ROCKS_KEY_LEN);
      if (h) {
        tsdbLRUCacheRelease(pTsdb->lruCache, h, true);
        taosLRUCacheErase(pTsdb->lruCache, &key, ROCKS_KEY_LEN);
      }
      
      // Delete from RocksDB
#ifdef USE_ROCKSDB
      rocksdb_writebatch_delete(pTsdb->rCache.writebatch, (char *)&key, ROCKS_KEY_LEN);
#endif
    }
  }
  
  return code;
}

// Callback function for upgrading each table's cache
int32_t tsdbUpgradeTableCacheCallback(SVnode *pVnode, tb_uid_t uid, tb_uid_t suid, 
                                     STSchema *pTSchema, void *pUserData) {
  int32_t code = 0;
  SCacheUpgradeCtx *pCtx = (SCacheUpgradeCtx *)pUserData;
  
  if (!pCtx || !pCtx->pTsdb) {
    return TSDB_CODE_INVALID_PARA;
  }
  
  // Process both LAST_ROW and LAST cache types
  for (int8_t lflag = LFLAG_LAST_ROW; lflag <= LFLAG_LAST; lflag++) {
    SArray *pColArray = NULL;
    SArray *pColStatus = NULL;
    
    // Query column cache for this table with specific flag
    code = tsdbQueryColCacheForTable(pCtx->pTsdb, uid, pTSchema, lflag, &pColArray, &pColStatus);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbError("vgId:%d, failed to query column cache for table uid:%" PRId64 " lflag:%d since %s", 
                TD_VID(pVnode), uid, lflag, tstrerror(code));
      continue;  // Continue with next flag
    }
    
    // Convert column cache to row cache
    code = tsdbConvertColCacheToRowCache(pCtx->pTsdb, uid, suid, pTSchema, lflag, pColArray, pColStatus);
    if (code == TSDB_CODE_SUCCESS) {
      tsdbDebug("vgId:%d, successfully converted cache for table uid:%" PRId64 " lflag:%d", 
                TD_VID(pVnode), uid, lflag);
    } else {
      tsdbError("vgId:%d, failed to convert column cache for table uid:%" PRId64 " lflag:%d since %s", 
                TD_VID(pVnode), uid, lflag, tstrerror(code));
    }
    
    // Cleanup arrays
    taosArrayDestroy(pColArray);
    taosArrayDestroy(pColStatus);
  }
  
  // Delete old column cache entries for this table
  code = tsdbDeleteColCacheForTable(pCtx->pTsdb, uid, pTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, failed to delete old column cache for table uid:%" PRId64 " since %s", 
              TD_VID(pVnode), uid, tstrerror(code));
  }
  
  // Continue processing even if this table failed
  return TSDB_CODE_SUCCESS;
}

// Main cache upgrade function
int32_t tsdbUpgradeCache(SVnode *pVnode) {
  int32_t code = 0;
  
  if (!pVnode || !pVnode->pTsdb) {
    return TSDB_CODE_INVALID_PARA;
  }
  
  // Check if upgrade is needed
  if (!tsdbNeedCacheUpgrade(pVnode)) {
    tsdbInfo("vgId:%d, cache upgrade not needed, already using row cache format", TD_VID(pVnode));
    return TSDB_CODE_SUCCESS;
  }
  
  tsdbInfo("vgId:%d, starting cache upgrade from column format to row format", TD_VID(pVnode));
  
  STsdb *pTsdb = pVnode->pTsdb;
  SCacheUpgradeCtx ctx = {
    .pTsdb = pTsdb
  };
  
  // Lock the LRU cache during upgrade
  (void)taosThreadMutexLock(&pTsdb->lruMutex);
  
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
      tsdbInfo("vgId:%d, cache upgrade completed successfully", TD_VID(pVnode));
    } else {
      tsdbError("vgId:%d, failed to update cache format configuration since %s", 
                TD_VID(pVnode), tstrerror(code));
    }
  } else {
    tsdbError("vgId:%d, cache upgrade failed since %s", TD_VID(pVnode), tstrerror(code));
  }
  
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
  
  return code;
}
