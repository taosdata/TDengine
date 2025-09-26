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

int32_t tsdbCacheCommit(STsdb *pTsdb) {
  int32_t code = 0;

  // 0, tsdbCacheUpdateFromIMem if updateCacheBatch
  // flush dirty data of lru into rocks
  // 4, and update when writing if !updateCacheBatch
  // 5, merge cache & mem if updateCacheBatch

  if (tsUpdateCacheBatch) {
    code = tsdbCacheUpdateFromIMem(pTsdb);
    if (code) {
      tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));

      TAOS_RETURN(code);
    }
  }

  char      *err = NULL;
  SLRUCache *pCache = pTsdb->lruCache;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

#if TSDB_CACHE_ROW_BASED
  taosLRUCacheApply(pCache, tsdbRowCacheFlushDirty, pTsdb);
#else
  taosLRUCacheApply(pCache, tsdbCacheFlushDirty, pTsdb);
#endif

#ifdef USE_ROCKSDB
  rocksMayWrite(pTsdb, true);
  rocksdb_flush(pTsdb->rCache.db, pTsdb->rCache.flushoptions, &err);
#endif
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
#ifdef USE_ROCKSDB
  if (NULL != err) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, err);
    rocksdb_free(err);
    code = TSDB_CODE_FAILED;
  }
#endif
  TAOS_RETURN(code);
}

void tsdbCacheFreeSLastColItem(void *pItem) {
  SLastCol *pCol = (SLastCol *)pItem;
  for (int i = 0; i < pCol->rowKey.numOfPKs; i++) {
    if (IS_VAR_DATA_TYPE(pCol->rowKey.pks[i].type)) {
      taosMemoryFree(pCol->rowKey.pks[i].pData);
    }
  }
  if ((IS_VAR_DATA_TYPE(pCol->colVal.value.type) || pCol->colVal.value.type == TSDB_DATA_TYPE_DECIMAL) &&
      pCol->colVal.value.pData) {
    taosMemoryFree(pCol->colVal.value.pData);
  }
}

static int32_t tsdbCacheNewTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type, int8_t lflag,
                                       SSchemaWrapper *pOldSchemaWrapper, SSchemaWrapper *pNewSchemaWrapper) {
#if TSDB_CACHE_ROW_BASED
  return tsdbRowCacheNewTableColumn(pTsdb, uid, cid, col_type, lflag, pOldSchemaWrapper, pNewSchemaWrapper);
#else
  return tsdbColCacheNewTableColumn(pTsdb, uid, cid, col_type, lflag, pOldSchemaWrapper, pNewSchemaWrapper);
#endif
}

int32_t tsdbCacheCommitNoLock(STsdb *pTsdb) {
  int32_t code = 0;
  char   *err = NULL;

  SLRUCache *pCache = pTsdb->lruCache;
  // rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;

#if TSDB_CACHE_ROW_BASED
  taosLRUCacheApply(pCache, tsdbRowCacheFlushDirty, pTsdb);
#else
  taosLRUCacheApply(pCache, tsdbCacheFlushDirty, pTsdb);
#endif
#ifdef USE_ROCKSDB
  rocksMayWrite(pTsdb, true);
  rocksdb_flush(pTsdb->rCache.db, pTsdb->rCache.flushoptions, &err);
  if (NULL != err) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, err);
    rocksdb_free(err);
    code = TSDB_CODE_FAILED;
  }
#endif
  TAOS_RETURN(code);
}

static int32_t tsdbCacheDropTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t lflag,
                                        SSchemaWrapper *pOldSchemaWrapper, SSchemaWrapper *pNewSchemaWrapper,
                                        bool hasPrimaryKey) {
#if TSDB_CACHE_ROW_BASED
  return tsdbRowCacheDropTableColumn(pTsdb, uid, cid, lflag, pOldSchemaWrapper, pNewSchemaWrapper, hasPrimaryKey);
#else
  return tsdbColCacheDropTableColumn(pTsdb, uid, cid, lflag, pOldSchemaWrapper, pNewSchemaWrapper, hasPrimaryKey);
#endif
}

int32_t tsdbCacheNewTable(STsdb *pTsdb, tb_uid_t uid, tb_uid_t suid, const SSchemaWrapper *pSchemaRow) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  if (suid < 0) {
    // For normal table, create schema from pSchemaRow
    if (pSchemaRow->nCols > 0) {
      // Build STSchema from SSchemaWrapper for consistency
      STSchema *pTSchema = NULL;
      code = metaBuildTSchemaFromSchemaWrapper((SSchemaWrapper *)pSchemaRow, &pTSchema);
      if (code == TSDB_CODE_SUCCESS && pTSchema) {
#if TSDB_CACHE_ROW_BASED
        code = tsdbRowCacheCreateTable(pTsdb, uid, pTSchema);
#else
        code = tsdbColCacheCreateTable(pTsdb, uid, pTSchema);
#endif
        if (code != TSDB_CODE_SUCCESS) {
          tsdbTrace("vgId:%d, %s create table cache failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__,
                    __LINE__, tstrerror(code));
        }
        taosMemoryFree(pTSchema);
      }
    }
  } else {
    STSchema *pTSchema = NULL;
    code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, -1, &pTSchema);
    if (code != TSDB_CODE_SUCCESS) {
      (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

      TAOS_RETURN(code);
    }

    // For super table, create table cache using the schema
    if (pTSchema->numOfCols > 0) {
#if TSDB_CACHE_ROW_BASED
      code = tsdbRowCacheCreateTable(pTsdb, uid, pTSchema);
#else
      code = tsdbColCacheCreateTable(pTsdb, uid, pTSchema);
#endif
      if (code != TSDB_CODE_SUCCESS) {
        tsdbTrace("vgId:%d, %s create table cache failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__,
                  __LINE__, tstrerror(code));
      }
    }

    taosMemoryFree(pTSchema);
  }

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheDropTable(STsdb *pTsdb, tb_uid_t uid, tb_uid_t suid, SSchemaWrapper *pSchemaRow) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  code = tsdbCacheCommitNoLock(pTsdb);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s commit with no lock failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }

  if (pSchemaRow != NULL) {
    // For normal table, build schema and delete
    if (pSchemaRow->nCols > 0) {
      STSchema *pTSchema = NULL;
      code = metaBuildTSchemaFromSchemaWrapper(pSchemaRow, &pTSchema);
      if (code == TSDB_CODE_SUCCESS && pTSchema) {
#if TSDB_CACHE_ROW_BASED
        code = tsdbRowCacheDeleteTable(pTsdb, uid);
#else
        code = tsdbColCacheDeleteTable(pTsdb, uid, pTSchema);
#endif
        if (code != TSDB_CODE_SUCCESS) {
          tsdbTrace("vgId:%d, %s delete table cache failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__,
                    __LINE__, tstrerror(code));
        }
        taosMemoryFree(pTSchema);
      }
    }
  } else {
    STSchema *pTSchema = NULL;
    code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, -1, &pTSchema);
    if (code != TSDB_CODE_SUCCESS) {
      (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

      TAOS_RETURN(code);
    }

    // For super table, delete using schema
    if (pTSchema->numOfCols > 0) {
#if TSDB_CACHE_ROW_BASED
      code = tsdbRowCacheDeleteTable(pTsdb, uid);
#else
      code = tsdbColCacheDeleteTable(pTsdb, uid, pTSchema);
#endif
      if (code != TSDB_CODE_SUCCESS) {
        tsdbTrace("vgId:%d, %s delete table cache failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__,
                  __LINE__, tstrerror(code));
      }
    }

    taosMemoryFree(pTSchema);
  }

  rocksMayWrite(pTsdb, false);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheDropSubTables(STsdb *pTsdb, SArray *uids, tb_uid_t suid) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  code = tsdbCacheCommitNoLock(pTsdb);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s commit with no lock failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }

  STSchema *pTSchema = NULL;
  code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, suid, -1, &pTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

    TAOS_RETURN(code);
  }

  for (int i = 0; i < TARRAY_SIZE(uids); ++i) {
    int64_t uid = ((tb_uid_t *)TARRAY_DATA(uids))[i];

    // For each subtable, delete using appropriate cache method
    if (pTSchema->numOfCols > 0) {
#if TSDB_CACHE_ROW_BASED
      code = tsdbRowCacheDeleteTable(pTsdb, uid);
#else
      code = tsdbColCacheDeleteTable(pTsdb, uid, pTSchema);
#endif
      if (code != TSDB_CODE_SUCCESS) {
        tsdbTrace("vgId:%d, %s delete table cache failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__,
                  __LINE__, tstrerror(code));
      }
    }
  }

  taosMemoryFree(pTSchema);

  rocksMayWrite(pTsdb, false);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheNewNTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type, SSchemaWrapper *pOldSchemaWrapper, SSchemaWrapper *pNewSchemaWrapper) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 0, pOldSchemaWrapper, pNewSchemaWrapper);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }
  code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 1, pOldSchemaWrapper, pNewSchemaWrapper);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }
  // rocksMayWrite(pTsdb, true, false, false);
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheDropNTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, SSchemaWrapper *pOldSchemaWrapper,
                                  SSchemaWrapper *pNewSchemaWrapper, bool hasPrimayKey) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  code = tsdbCacheCommitNoLock(pTsdb);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s commit with no lock failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }

  code = tsdbCacheDropTableColumn(pTsdb, uid, cid, 0, pOldSchemaWrapper, pNewSchemaWrapper, hasPrimayKey);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s drop table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }
  code = tsdbCacheDropTableColumn(pTsdb, uid, cid, 1, pOldSchemaWrapper, pNewSchemaWrapper, hasPrimayKey);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s drop table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }

  rocksMayWrite(pTsdb, false);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheNewSTableColumn(STsdb *pTsdb, SArray *uids, int16_t cid, int8_t col_type, SSchemaWrapper *pOldSchemaWrapper, SSchemaWrapper *pNewSchemaWrapper) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  for (int i = 0; i < TARRAY_SIZE(uids); ++i) {
    tb_uid_t uid = ((tb_uid_t *)TARRAY_DATA(uids))[i];

    code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 0, pOldSchemaWrapper, pNewSchemaWrapper);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                tstrerror(code));
    }
    code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 1, pOldSchemaWrapper, pNewSchemaWrapper);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                tstrerror(code));
    }
  }

  // rocksMayWrite(pTsdb, true, false, false);
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
  TAOS_RETURN(code);
}

int32_t tsdbCacheDropSTableColumn(STsdb *pTsdb, SArray *uids, int16_t cid, SSchemaWrapper *pOldSchemaWrapper,
                                  SSchemaWrapper *pNewSchemaWrapper, bool hasPrimayKey) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  code = tsdbCacheCommitNoLock(pTsdb);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s commit with no lock failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }

  for (int i = 0; i < TARRAY_SIZE(uids); ++i) {
    int64_t uid = ((tb_uid_t *)TARRAY_DATA(uids))[i];

    code = tsdbCacheDropTableColumn(pTsdb, uid, cid, 0, pOldSchemaWrapper, pNewSchemaWrapper, hasPrimayKey);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbTrace("vgId:%d, %s drop table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                tstrerror(code));
    }
    code = tsdbCacheDropTableColumn(pTsdb, uid, cid, 1, pOldSchemaWrapper, pNewSchemaWrapper, hasPrimayKey);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbTrace("vgId:%d, %s drop table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                tstrerror(code));
    }
  }

  rocksMayWrite(pTsdb, false);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

static void tsdbCacheUpdateLastColToNone(SLastCol *pLastCol, ELastCacheStatus cacheStatus) {
  // update rowkey
  pLastCol->rowKey.ts = TSKEY_MIN;
  for (int8_t i = 0; i < pLastCol->rowKey.numOfPKs; i++) {
    SValue *pPKValue = &pLastCol->rowKey.pks[i];
    if (IS_VAR_DATA_TYPE(pPKValue->type) && pPKValue->nData > 0) {
      taosMemoryFreeClear(pPKValue->pData);
      pPKValue->nData = 0;
    } else {
      valueClearDatum(pPKValue, pPKValue->type);
    }
  }
  pLastCol->rowKey.numOfPKs = 0;

  // update colval
  if (IS_VAR_DATA_TYPE(pLastCol->colVal.value.type) && pLastCol->colVal.value.nData > 0) {
    taosMemoryFreeClear(pLastCol->colVal.value.pData);
    pLastCol->colVal.value.nData = 0;
  } else {
    valueClearDatum(&pLastCol->colVal.value, pLastCol->colVal.value.type);
  }

  pLastCol->colVal = COL_VAL_NONE(pLastCol->colVal.cid, pLastCol->colVal.value.type);
  pLastCol->dirty = 1;
  pLastCol->cacheStatus = cacheStatus;
}

int32_t tsdbCacheRowFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, int64_t version, int32_t nRow,
                                 SRow **aRow) {
#if TSDB_CACHE_ROW_BASED
  return tsdbRowCacheRowFormatUpdate(pTsdb, suid, uid, version, nRow, aRow);
#else

  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
  }

  tsdbRowClose(&iter);
  tSimpleHashCleanup(iColHash);
  taosArrayClear(ctxArray);

  TAOS_RETURN(code);
#endif
}

int32_t tsdbCacheColFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SBlockData *pBlockData) {
#if TSDB_CACHE_ROW_BASED
  return tsdbRowCacheColFormatUpdate(pTsdb, suid, uid, pBlockData);
#else
  return tsdbColCacheRowFormatUpdate(pTsdb, suid, uid, pBlockData);
#endif
}

int32_t tsdbCacheGetBatch(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SCacheRowsReader *pr, int8_t ltype) {
  int32_t code = 0;
  int32_t lino = 0;

#if TSDB_CACHE_ROW_BASED
  // Use row-based cache implementation
  TAOS_CHECK_EXIT(tsdbCacheGetBatchFromRowLru(pTsdb, uid, pLastArray, pr, ltype));
#else
  // Use original column-based cache implementation
  SArray *keyArray = taosArrayInit(16, sizeof(SLastKey));
  if (!keyArray) {
    TAOS_CHECK_EXIT(terrno);
  }

  TAOS_CHECK_EXIT(tsdbCacheGetBatchFromLru(pTsdb, uid, pLastArray, pr, ltype, keyArray));

  if (tsUpdateCacheBatch) {
    TAOS_CHECK_EXIT(tsdbCacheGetBatchFromMem(pTsdb, uid, pLastArray, pr, keyArray));
  }

  if (keyArray) {
    taosArrayDestroy(keyArray);
  }
#endif

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

int32_t tsdbCacheDel(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey) {
#if TSDB_CACHE_ROW_BASED
  return tsdbRowCacheDel(pTsdb, suid, uid, sKey, eKey);
#else
  return tsdbColCacheDel(pTsdb, suid, uid, sKey, eKey);
#endif
}

void tsdbCacheRelease(SLRUCache *pCache, LRUHandle *h) { tsdbLRUCacheRelease(pCache, h, false); }

void tsdbCacheSetCapacity(SVnode *pVnode, size_t capacity) {
  taosLRUCacheSetCapacity(pVnode->pTsdb->lruCache, capacity);
}

#ifdef BUILD_NO_CALL
size_t tsdbCacheGetCapacity(SVnode *pVnode) { return taosLRUCacheGetCapacity(pVnode->pTsdb->lruCache); }
#endif

size_t tsdbCacheGetUsage(SVnode *pVnode) {
  size_t usage = 0;
  if (pVnode->pTsdb != NULL) {
    usage = taosLRUCacheGetUsage(pVnode->pTsdb->lruCache);
  }

  return usage;
}

int32_t tsdbCacheGetElems(SVnode *pVnode) {
  int32_t elems = 0;
  if (pVnode->pTsdb != NULL) {
    elems = taosLRUCacheGetElems(pVnode->pTsdb->lruCache);
  }

  return elems;
}

#ifdef USE_SHARED_STORAGE
// block cache
static void getBCacheKey(int32_t fid, int64_t commitID, int64_t blkno, char *key, int *len) {
  struct {
    int32_t fid;
    int64_t commitID;
    int64_t blkno;
  } bKey = {0};

  bKey.fid = fid;
  bKey.commitID = commitID;
  bKey.blkno = blkno;

  *len = sizeof(bKey);
  memcpy(key, &bKey, *len);
}

static int32_t tsdbCacheLoadBlockSs(STsdbFD *pFD, uint8_t **ppBlock) {
  int32_t code = 0;

  int64_t block_size = tsSsBlockSize * pFD->szPage;
  int64_t block_offset = (pFD->blkno - 1) * block_size;

  char *buf = taosMemoryMalloc(block_size);
  if (buf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // TODO: pFD->objName is not initialized, but this function is never called.
  code = tssReadFileFromDefault(pFD->objName, block_offset, buf, &block_size);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(buf);
    goto _exit;
  }
  *ppBlock = (uint8_t *)buf;

_exit:
  return code;
}

static void deleteBCache(const void *key, size_t keyLen, void *value, void *ud) {
  (void)ud;
  uint8_t *pBlock = (uint8_t *)value;

  taosMemoryFree(pBlock);
}

int32_t tsdbCacheGetBlockSs(SLRUCache *pCache, STsdbFD *pFD, LRUHandle **handle) {
  int32_t code = 0;
  char    key[128] = {0};
  int     keyLen = 0;

  getBCacheKey(pFD->fid, pFD->cid, pFD->blkno, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (!h) {
    STsdb *pTsdb = pFD->pTsdb;
    (void)taosThreadMutexLock(&pTsdb->bMutex);

    h = taosLRUCacheLookup(pCache, key, keyLen);
    if (!h) {
      uint8_t *pBlock = NULL;
      code = tsdbCacheLoadBlockSs(pFD, &pBlock);
      //  if table's empty or error, return code of -1
      if (code != TSDB_CODE_SUCCESS || pBlock == NULL) {
        (void)taosThreadMutexUnlock(&pTsdb->bMutex);

        *handle = NULL;
        if (code == TSDB_CODE_SUCCESS && !pBlock) {
          code = TSDB_CODE_OUT_OF_MEMORY;
        }

        TAOS_RETURN(code);
      }

      size_t              charge = tsSsBlockSize * pFD->szPage;
      _taos_lru_deleter_t deleter = deleteBCache;
      LRUStatus           status =
          taosLRUCacheInsert(pCache, key, keyLen, pBlock, charge, deleter, NULL, &h, TAOS_LRU_PRIORITY_LOW, NULL);
      if (status != TAOS_LRU_STATUS_OK) {
        // code = -1;
      }
    }

    (void)taosThreadMutexUnlock(&pTsdb->bMutex);
  }

  *handle = h;

  TAOS_RETURN(code);
}

int32_t tsdbCacheGetPageSs(SLRUCache *pCache, STsdbFD *pFD, int64_t pgno, LRUHandle **handle) {
  if (!tsSsEnabled) {
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }

  int32_t code = 0;
  char    key[128] = {0};
  int     keyLen = 0;

  getBCacheKey(pFD->fid, pFD->cid, pgno, key, &keyLen);
  *handle = taosLRUCacheLookup(pCache, key, keyLen);

  return code;
}

void tsdbCacheSetPageSs(SLRUCache *pCache, STsdbFD *pFD, int64_t pgno, uint8_t *pPage) {
  if (!tsSsEnabled) {
    return;
  }

  char       key[128] = {0};
  int        keyLen = 0;
  LRUHandle *handle = NULL;

  getBCacheKey(pFD->fid, pFD->cid, pgno, key, &keyLen);
  (void)taosThreadMutexLock(&pFD->pTsdb->pgMutex);
  handle = taosLRUCacheLookup(pFD->pTsdb->pgCache, key, keyLen);
  if (!handle) {
    size_t              charge = pFD->szPage;
    _taos_lru_deleter_t deleter = deleteBCache;
    uint8_t            *pPg = taosMemoryMalloc(charge);
    if (!pPg) {
      (void)taosThreadMutexUnlock(&pFD->pTsdb->pgMutex);

      return;  // ignore error with ss cache and leave error untouched
    }
    memcpy(pPg, pPage, charge);

    LRUStatus status =
        taosLRUCacheInsert(pCache, key, keyLen, pPg, charge, deleter, NULL, &handle, TAOS_LRU_PRIORITY_LOW, NULL);
    if (status != TAOS_LRU_STATUS_OK) {
      // ignore cache updating if not ok
      // code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  (void)taosThreadMutexUnlock(&pFD->pTsdb->pgMutex);

  tsdbCacheRelease(pFD->pTsdb->pgCache, handle);
}
#endif
