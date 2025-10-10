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

#ifdef USE_SHARED_STORAGE

static int32_t tsdbOpenBCache(STsdb *pTsdb) {
  int32_t code = 0, lino = 0;
  int32_t szPage = pTsdb->pVnode->config.tsdbPageSize;
  int64_t szBlock = tsSsBlockSize <= 1024 ? 1024 : tsSsBlockSize;

  SLRUCache *pCache = taosLRUCacheInit((int64_t)tsSsBlockCacheSize * szBlock * szPage, 0, .5);
  if (pCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err);
  }

  taosLRUCacheSetStrictCapacity(pCache, false);

  (void)taosThreadMutexInit(&pTsdb->bMutex, NULL);

  pTsdb->bCache = pCache;

_err:
  if (code) {
    tsdbError("tsdb/bcache: vgId:%d, %s failed at line %d since %s.", TD_VID(pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }

  TAOS_RETURN(code);
}

static void tsdbCloseBCache(STsdb *pTsdb) {
  SLRUCache *pCache = pTsdb->bCache;
  if (pCache) {
    int32_t elems = taosLRUCacheGetElems(pCache);
    tsdbTrace("vgId:%d, elems: %d", TD_VID(pTsdb->pVnode), elems);
    taosLRUCacheEraseUnrefEntries(pCache);
    elems = taosLRUCacheGetElems(pCache);
    tsdbTrace("vgId:%d, elems: %d", TD_VID(pTsdb->pVnode), elems);

    taosLRUCacheCleanup(pCache);

    (void)taosThreadMutexDestroy(&pTsdb->bMutex);
  }
}

static int32_t tsdbOpenPgCache(STsdb *pTsdb) {
  int32_t code = 0, lino = 0;
  int32_t szPage = pTsdb->pVnode->config.tsdbPageSize;

  SLRUCache *pCache = taosLRUCacheInit((int64_t)tsSsPageCacheSize * szPage, 0, .5);
  if (pCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err);
  }

  taosLRUCacheSetStrictCapacity(pCache, false);

  (void)taosThreadMutexInit(&pTsdb->pgMutex, NULL);

  pTsdb->pgCache = pCache;

_err:
  if (code) {
    tsdbError("tsdb/pgcache: vgId:%d, open failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static void tsdbClosePgCache(STsdb *pTsdb) {
  SLRUCache *pCache = pTsdb->pgCache;
  if (pCache) {
    int32_t elems = taosLRUCacheGetElems(pCache);
    tsdbTrace("vgId:%d, elems: %d", TD_VID(pTsdb->pVnode), elems);
    taosLRUCacheEraseUnrefEntries(pCache);
    elems = taosLRUCacheGetElems(pCache);
    tsdbTrace("vgId:%d, elems: %d", TD_VID(pTsdb->pVnode), elems);

    taosLRUCacheCleanup(pCache);

    (void)taosThreadMutexDestroy(&pTsdb->bMutex);
  }
}

#endif  // USE_SHARED_STORAGE

static const char *myCmpName(void *state) {
  (void)state;
  return "myCmp";
}

static void myCmpDestroy(void *state) { (void)state; }

static int myCmp(void *state, const char *a, size_t alen, const char *b, size_t blen) {
#if TSDB_CACHE_ROW_BASED
  return tsdbRowCacheCmp(state, a, alen, b, blen);
#else
  return tsdbColCacheCmp(state, a, alen, b, blen);
#endif
}

static void tsdbCloseRocksCache(STsdb *pTsdb) {
#ifdef USE_ROCKSDB
  rocksdb_close(pTsdb->rCache.db);
  (void)taosThreadMutexDestroy(&pTsdb->rCache.writeBatchMutex);
  rocksdb_flushoptions_destroy(pTsdb->rCache.flushoptions);
  rocksdb_writebatch_destroy(pTsdb->rCache.writebatch);
  rocksdb_readoptions_destroy(pTsdb->rCache.readoptions);
  rocksdb_writeoptions_destroy(pTsdb->rCache.writeoptions);
  rocksdb_options_destroy(pTsdb->rCache.options);
  rocksdb_block_based_options_destroy(pTsdb->rCache.tableoptions);
  rocksdb_comparator_destroy(pTsdb->rCache.my_comparator);
  taosMemoryFree(pTsdb->rCache.pTSchema);
  taosArrayDestroy(pTsdb->rCache.ctxArray);
#endif
}

static void tsdbGetRocksPath(STsdb *pTsdb, char *path, bool isRowCache) {
  SVnode *pVnode = pTsdb->pVnode;
  vnodeGetPrimaryPath(pVnode, false, path, TSDB_FILENAME_LEN);

  int32_t offset = strlen(path);
  if (isRowCache) {
    snprintf(path + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s%srow_cache.rdb", TD_DIRSEP, pTsdb->name, TD_DIRSEP);
  } else {
    snprintf(path + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s%scache.rdb", TD_DIRSEP, pTsdb->name, TD_DIRSEP);
  }
}

// Open column cache RocksDB for upgrade purposes
int32_t tsdbOpenColCacheRocksDB(STsdb *pTsdb, rocksdb_t **ppColDB, rocksdb_options_t **ppOptions,
                                rocksdb_readoptions_t **ppReadoptions,rocksdb_comparator_t **ppColCmp) {
  int32_t code = 0, lino = 0;
#ifdef USE_ROCKSDB
  // Create column cache comparator
  *ppColCmp = rocksdb_comparator_create(
      NULL, myCmpDestroy, (int (*)(void *, const char *, size_t, const char *, size_t))tsdbColCacheCmp, myCmpName);
  if (NULL == *ppColCmp) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  rocksdb_options_t *options = rocksdb_options_create();
  if (NULL == options) {
    rocksdb_comparator_destroy(*ppColCmp);
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  rocksdb_options_set_create_if_missing(options, 0);  // Don't create if missing for upgrade
  rocksdb_options_set_comparator(options, *ppColCmp);
  rocksdb_options_set_info_log_level(options, 2);  // WARN_LEVEL

  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  if (NULL == readoptions) {
    rocksdb_options_destroy(options);
    rocksdb_comparator_destroy(*ppColCmp);
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  char *err = NULL;
  char  cachePath[TSDB_FILENAME_LEN] = {0};

  // Open column cache database
  tsdbGetRocksPath(pTsdb, cachePath, false);  // false for column cache

  rocksdb_t *colDB = rocksdb_open(options, cachePath, &err);
  if (NULL == colDB) {
    tsdbWarn("vgId:%d, failed to open column cache RocksDB: %s", TD_VID(pTsdb->pVnode), err ? err : "unknown");
    if (err) rocksdb_free(err);
    rocksdb_readoptions_destroy(readoptions);
    rocksdb_options_destroy(options);
    rocksdb_comparator_destroy(*ppColCmp);
    TAOS_RETURN(TSDB_CODE_NOT_FOUND);  // Column cache DB doesn't exist, which is fine
  }

  *ppColDB = colDB;
  *ppOptions = options;
  *ppReadoptions = readoptions;

  tsdbInfo("vgId:%d, successfully opened column cache RocksDB for upgrade", TD_VID(pTsdb->pVnode));
#endif
  TAOS_RETURN(code);
}

// Close column cache RocksDB
void tsdbCloseColCacheRocksDB(rocksdb_t *colDB, rocksdb_options_t *options, rocksdb_readoptions_t *readoptions, rocksdb_comparator_t *colCmp) {
#ifdef USE_ROCKSDB
  if (colDB) {
    rocksdb_close(colDB);
  }
  if (readoptions) {
    rocksdb_readoptions_destroy(readoptions);
  }
  if (options) {
    rocksdb_options_destroy(options);
  }
  if (colCmp) {
    rocksdb_comparator_destroy(colCmp);
  }
#endif
}

// Delete column cache RocksDB directory
int32_t tsdbDeleteColCacheRocksDB(STsdb *pTsdb) {
  char cachePath[TSDB_FILENAME_LEN] = {0};
  tsdbGetRocksPath(pTsdb, cachePath, false);  // false for column cache

  // Remove the directory
  taosRemoveDir(cachePath);

  // Check if directory still exists
  if (taosCheckExistFile(cachePath)) {
    tsdbWarn("vgId:%d, failed to delete column cache RocksDB: %s", TD_VID(pTsdb->pVnode), cachePath);
    return TSDB_CODE_FAILED;
  } else {
    tsdbInfo("vgId:%d, successfully deleted column cache RocksDB: %s", TD_VID(pTsdb->pVnode), cachePath);
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t tsdbOpenRocksCache(STsdb *pTsdb) {
  int32_t code = 0, lino = 0;
#ifdef USE_ROCKSDB
  rocksdb_comparator_t *cmp = rocksdb_comparator_create(NULL, myCmpDestroy, myCmp, myCmpName);
  if (NULL == cmp) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  rocksdb_block_based_table_options_t *tableoptions = rocksdb_block_based_options_create();
  pTsdb->rCache.tableoptions = tableoptions;

  rocksdb_options_t *options = rocksdb_options_create();
  if (NULL == options) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err);
  }

  rocksdb_options_set_create_if_missing(options, 1);
  rocksdb_options_set_comparator(options, cmp);
  rocksdb_options_set_block_based_table_factory(options, tableoptions);
  rocksdb_options_set_info_log_level(options, 2);  // WARN_LEVEL
  // rocksdb_options_set_inplace_update_support(options, 1);
  // rocksdb_options_set_allow_concurrent_memtable_write(options, 0);

  rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
  if (NULL == writeoptions) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err2);
  }
  rocksdb_writeoptions_disable_WAL(writeoptions, 1);

  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  if (NULL == readoptions) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err3);
  }

  char *err = NULL;
  char  cachePath[TSDB_FILENAME_LEN] = {0};

  // Always open row cache database for current operations
  tsdbGetRocksPath(pTsdb, cachePath, true);  // true for row cache

  rocksdb_t *db = rocksdb_open(options, cachePath, &err);
  if (NULL == db) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, err);
    rocksdb_free(err);

    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err4);
  }

  rocksdb_flushoptions_t *flushoptions = rocksdb_flushoptions_create();
  if (NULL == flushoptions) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err5);
  }

  rocksdb_writebatch_t *writebatch = rocksdb_writebatch_create();

  TAOS_CHECK_GOTO(taosThreadMutexInit(&pTsdb->rCache.writeBatchMutex, NULL), &lino, _err6);

  pTsdb->rCache.writebatch = writebatch;
  pTsdb->rCache.my_comparator = cmp;
  pTsdb->rCache.options = options;
  pTsdb->rCache.writeoptions = writeoptions;
  pTsdb->rCache.readoptions = readoptions;
  pTsdb->rCache.flushoptions = flushoptions;
  pTsdb->rCache.db = db;
  pTsdb->rCache.sver = -1;
  pTsdb->rCache.suid = -1;
  pTsdb->rCache.uid = -1;
  pTsdb->rCache.pTSchema = NULL;
  pTsdb->rCache.ctxArray = taosArrayInit(16, sizeof(SLastUpdateCtx));
  if (!pTsdb->rCache.ctxArray) {
    TAOS_CHECK_GOTO(terrno, &lino, _err7);
  }

  TAOS_RETURN(code);

_err7:
  (void)taosThreadMutexDestroy(&pTsdb->rCache.writeBatchMutex);
_err6:
  rocksdb_writebatch_destroy(writebatch);
_err5:
  rocksdb_close(pTsdb->rCache.db);
_err4:
  rocksdb_readoptions_destroy(readoptions);
_err3:
  rocksdb_writeoptions_destroy(writeoptions);
_err2:
  rocksdb_options_destroy(options);
  rocksdb_block_based_options_destroy(tableoptions);
_err:
  rocksdb_comparator_destroy(cmp);
#endif
  TAOS_RETURN(code);
}

int32_t tsdbOpenCache(STsdb *pTsdb) {
  int32_t code = 0, lino = 0;
  size_t  cfgCapacity = (size_t)pTsdb->pVnode->config.cacheLastSize * 1024 * 1024;
  SVnode *pVnode = pTsdb->pVnode;

  SLRUCache *pCache = taosLRUCacheInit(cfgCapacity, 0, .5);
  if (pCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err);
  }

#ifdef USE_SHARED_STORAGE
  if (tsSsEnabled) {
    TAOS_CHECK_GOTO(tsdbOpenBCache(pTsdb), &lino, _err);
    TAOS_CHECK_GOTO(tsdbOpenPgCache(pTsdb), &lino, _err);
  }
#endif

  TAOS_CHECK_GOTO(tsdbOpenRocksCache(pTsdb), &lino, _err);

  taosLRUCacheSetStrictCapacity(pCache, false);

  (void)taosThreadMutexInit(&pTsdb->lruMutex, NULL);
  // Check and perform cache upgrade if needed
  if (tsdbNeedCacheUpgrade(pVnode)) {
    vInfo("vgId:%d, start to upgrade cache from column format to row format", TD_VID(pVnode));

    code = tsdbUpgradeCache(pTsdb);
    if (code != TSDB_CODE_SUCCESS) {
      vError("vgId:%d, failed to upgrade cache since %s", TD_VID(pVnode), tstrerror(code));
      TAOS_CHECK_GOTO(code, &lino, _err);
    } else {
      vInfo("vgId:%d, cache upgrade completed successfully", TD_VID(pVnode));
    }
  }
_err:
  if (code) {
    tsdbError("tsdb/cache: vgId:%d, open failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino, tstrerror(code));
  }

  pTsdb->lruCache = pCache;

  TAOS_RETURN(code);
}

void tsdbCloseCache(STsdb *pTsdb) {
  SLRUCache *pCache = pTsdb->lruCache;
  if (pCache) {
    taosLRUCacheEraseUnrefEntries(pCache);

    taosLRUCacheCleanup(pCache);

    (void)taosThreadMutexDestroy(&pTsdb->lruMutex);
  }

#ifdef USE_SHARED_STORAGE
  if (tsSsEnabled) {
    tsdbCloseBCache(pTsdb);
    tsdbClosePgCache(pTsdb);
  }
#endif

  tsdbCloseRocksCache(pTsdb);
}

void rocksMayWrite(STsdb *pTsdb, bool force) {
#ifdef USE_ROCKSDB
  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;

  int count = rocksdb_writebatch_count(wb);
  if ((force && count > 0) || count >= ROCKS_BATCH_SIZE) {
    char *err = NULL;

    rocksdb_write(pTsdb->rCache.db, pTsdb->rCache.writeoptions, wb, &err);
    if (NULL != err) {
      tsdbError("vgId:%d, %s failed at line %d, count: %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, count,
                err);
      rocksdb_free(err);
    }

    rocksdb_writebatch_clear(wb);
  }
#endif
}

int32_t tsdbUpdateSkm(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, int32_t sver) {
  SRocksCache *pRCache = &pTsdb->rCache;
  if (pRCache->pTSchema && sver == pRCache->sver) {
    if (suid > 0 && suid == pRCache->suid) {
      return 0;
    }
    if (suid == 0 && uid == pRCache->uid) {
      return 0;
    }
  }

  pRCache->suid = suid;
  pRCache->uid = uid;
  pRCache->sver = sver;
  tDestroyTSchema(pRCache->pTSchema);
  return metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, sver, &pRCache->pTSchema);
}

int32_t tsdbCacheGetValuesFromRocks(STsdb *pTsdb, size_t numKeys, const char *const *ppKeysList, size_t *pKeysListSizes,
                                    char ***pppValuesList, size_t **ppValuesListSizes) {
#ifdef USE_ROCKSDB
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
  rocksdb_multi_get(pTsdb->rCache.db, pTsdb->rCache.readoptions, numKeys, ppKeysList, pKeysListSizes, valuesList,
                    valuesListSizes, errs);
  for (size_t i = 0; i < numKeys; ++i) {
    rocksdb_free(errs[i]);
  }
  taosMemoryFreeClear(errs);

  *pppValuesList = valuesList;
  *ppValuesListSizes = valuesListSizes;
#endif
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t tsdbCacheUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SArray *updCtxArray) {
  if (!updCtxArray || TARRAY_SIZE(updCtxArray) == 0) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

#if TSDB_CACHE_ROW_BASED
  // For row-based cache, we need to handle this differently
  // Group the updates by lflag and process as complete rows
  return tsdbRowCacheUpdateFromCtxArray(pTsdb, suid, uid, updCtxArray);
#else
  return tsdbColCacheUpdateFromCtxArray(pTsdb, suid, uid, updCtxArray);
#endif
}

static void getTableCacheKey(tb_uid_t uid, int cacheType, char *key, int *len) {
  if (cacheType == 0) {  // last_row
    *(uint64_t *)key = (uint64_t)uid;
  } else {  // last
    *(uint64_t *)key = ((uint64_t)uid) | 0x8000000000000000;
  }

  *len = sizeof(uint64_t);
}

static tb_uid_t getTableSuidByUid(tb_uid_t uid, STsdb *pTsdb) {
  tb_uid_t suid = 0;

  SMetaReader mr = {0};
  metaReaderDoInit(&mr, pTsdb->pVnode->pMeta, META_READER_LOCK);
  if (metaReaderGetTableEntryByUidCache(&mr, uid) < 0) {
    metaReaderClear(&mr);  // table not esist
    return 0;
  }

  if (mr.me.type == TSDB_CHILD_TABLE) {
    suid = mr.me.ctbEntry.suid;
  } else if (mr.me.type == TSDB_NORMAL_TABLE) {
    suid = 0;
  } else {
    suid = 0;
  }

  metaReaderClear(&mr);

  return suid;
}

static int32_t getTableDelDataFromDelIdx(SDelFReader *pDelReader, SDelIdx *pDelIdx, SArray *aDelData) {
  int32_t code = 0;

  if (pDelIdx) {
    code = tsdbReadDelDatav1(pDelReader, pDelIdx, aDelData, INT64_MAX);
  }

  TAOS_RETURN(code);
}

static int32_t getTableDelDataFromTbData(STbData *pTbData, SArray *aDelData) {
  int32_t   code = 0;
  SDelData *pDelData = pTbData ? pTbData->pHead : NULL;

  for (; pDelData; pDelData = pDelData->pNext) {
    if (!taosArrayPush(aDelData, pDelData)) {
      TAOS_RETURN(terrno);
    }
  }

  TAOS_RETURN(code);
}

static uint64_t *getUidList(SCacheRowsReader *pReader) {
  if (!pReader->uidList) {
    int32_t numOfTables = pReader->numOfTables;

    pReader->uidList = taosMemoryMalloc(numOfTables * sizeof(uint64_t));
    if (!pReader->uidList) {
      return NULL;
    }

    for (int32_t i = 0; i < numOfTables; ++i) {
      uint64_t uid = pReader->pTableList[i].uid;
      pReader->uidList[i] = uid;
    }

    taosSort(pReader->uidList, numOfTables, sizeof(uint64_t), uidComparFunc);
  }

  return pReader->uidList;
}

static void freeTableInfoFunc(void *param) {
  void **p = (void **)param;
  taosMemoryFreeClear(*p);
}

static STableLoadInfo *getTableLoadInfo(SCacheRowsReader *pReader, uint64_t uid) {
  if (!pReader->pTableMap) {
    pReader->pTableMap = tSimpleHashInit(pReader->numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    if (!pReader->pTableMap) {
      return NULL;
    }

    tSimpleHashSetFreeFp(pReader->pTableMap, freeTableInfoFunc);
  }

  STableLoadInfo  *pInfo = NULL;
  STableLoadInfo **ppInfo = tSimpleHashGet(pReader->pTableMap, &uid, sizeof(uid));
  if (!ppInfo) {
    pInfo = taosMemoryCalloc(1, sizeof(STableLoadInfo));
    if (pInfo) {
      if (tSimpleHashPut(pReader->pTableMap, &uid, sizeof(uint64_t), &pInfo, POINTER_BYTES)) {
        return NULL;
      }
    }

    return pInfo;
  }

  return *ppInfo;
}

static int32_t loadTombFromBlk(const TTombBlkArray *pTombBlkArray, SCacheRowsReader *pReader, void *pFileReader,
                               bool isFile) {
  int32_t   code = 0;
  int32_t   numOfTables = pReader->numOfTables;
  int64_t   suid = pReader->info.suid;
  uint64_t *uidList = getUidList(pReader);

  if (!uidList) {
    TAOS_RETURN(terrno);
  }

  for (int i = 0, j = 0; i < pTombBlkArray->size && j < numOfTables; ++i) {
    STombBlk *pTombBlk = &pTombBlkArray->data[i];
    if (pTombBlk->maxTbid.suid < suid || (pTombBlk->maxTbid.suid == suid && pTombBlk->maxTbid.uid < uidList[0])) {
      continue;
    }

    if (pTombBlk->minTbid.suid > suid ||
        (pTombBlk->minTbid.suid == suid && pTombBlk->minTbid.uid > uidList[numOfTables - 1])) {
      break;
    }

    STombBlock block = {0};
    code = isFile ? tsdbDataFileReadTombBlock(pFileReader, &pTombBlkArray->data[i], &block)
                  : tsdbSttFileReadTombBlock(pFileReader, &pTombBlkArray->data[i], &block);
    if (code != TSDB_CODE_SUCCESS) {
      TAOS_RETURN(code);
    }

    uint64_t        uid = uidList[j];
    STableLoadInfo *pInfo = getTableLoadInfo(pReader, uid);
    if (!pInfo) {
      tTombBlockDestroy(&block);
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }

    if (pInfo->pTombData == NULL) {
      pInfo->pTombData = taosArrayInit(4, sizeof(SDelData));
    }

    STombRecord record = {0};
    bool        finished = false;
    for (int32_t k = 0; k < TOMB_BLOCK_SIZE(&block); ++k) {
      code = tTombBlockGet(&block, k, &record);
      if (code != TSDB_CODE_SUCCESS) {
        finished = true;
        break;
      }

      if (record.suid < suid) {
        continue;
      }
      if (record.suid > suid) {
        finished = true;
        break;
      }

      bool newTable = false;
      if (uid < record.uid) {
        while (j < numOfTables && uidList[j] < record.uid) {
          ++j;
          newTable = true;
        }

        if (j >= numOfTables) {
          finished = true;
          break;
        }

        uid = uidList[j];
      }

      if (record.uid < uid) {
        continue;
      }

      if (newTable) {
        pInfo = getTableLoadInfo(pReader, uid);
        if (!pInfo) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          finished = true;
          break;
        }
        if (pInfo->pTombData == NULL) {
          pInfo->pTombData = taosArrayInit(4, sizeof(SDelData));
          if (!pInfo->pTombData) {
            code = terrno;
            finished = true;
            break;
          }
        }
      }

      if (record.version <= pReader->info.verRange.maxVer) {
        /*tsdbError("tomb xx load/cache: vgId:%d fid:%d record %" PRId64 "~%" PRId64 "~%" PRId64 " tomb records",
          TD_VID(pReader->pTsdb->pVnode), pReader->pCurFileSet->fid, record.skey, record.ekey, uid);*/

        SDelData delData = {.version = record.version, .sKey = record.skey, .eKey = record.ekey};
        if (!taosArrayPush(pInfo->pTombData, &delData)) {
          TAOS_RETURN(terrno);
        }
      }
    }

    tTombBlockDestroy(&block);

    if (finished) {
      TAOS_RETURN(code);
    }
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t loadDataTomb(SCacheRowsReader *pReader, SDataFileReader *pFileReader) {
  const TTombBlkArray *pBlkArray = NULL;

  TAOS_CHECK_RETURN(tsdbDataFileReadTombBlk(pFileReader, &pBlkArray));

  TAOS_RETURN(loadTombFromBlk(pBlkArray, pReader, pFileReader, true));
}

static int32_t loadSttTomb(STsdbReader *pTsdbReader, SSttFileReader *pSttFileReader, SSttBlockLoadInfo *pLoadInfo) {
  SCacheRowsReader    *pReader = (SCacheRowsReader *)pTsdbReader;
  const TTombBlkArray *pBlkArray = NULL;

  TAOS_CHECK_RETURN(tsdbSttFileReadTombBlk(pSttFileReader, &pBlkArray));

  TAOS_RETURN(loadTombFromBlk(pBlkArray, pReader, pSttFileReader, false));
}

typedef struct {
  SMergeTree  mergeTree;
  SMergeTree *pMergeTree;
} SFSLastIter;

static int32_t lastIterOpen(SFSLastIter *iter, STFileSet *pFileSet, STsdb *pTsdb, STSchema *pTSchema, tb_uid_t suid,
                            tb_uid_t uid, SCacheRowsReader *pr, int64_t lastTs, int16_t *aCols, int nCols) {
  int32_t code = 0;
  destroySttBlockReader(pr->pLDataIterArray, NULL);
  pr->pLDataIterArray = taosArrayInit(4, POINTER_BYTES);
  if (pr->pLDataIterArray == NULL) return terrno;

  SMergeTreeConf conf = {
      .uid = uid,
      .suid = suid,
      .pTsdb = pTsdb,
      .timewindow = (STimeWindow){.skey = lastTs, .ekey = TSKEY_MAX},
      .verRange = (SVersionRange){.minVer = 0, .maxVer = INT64_MAX},
      .strictTimeRange = false,
      .pSchema = pTSchema,
      .pCurrentFileset = pFileSet,
      .backward = 1,
      .pSttFileBlockIterArray = pr->pLDataIterArray,
      .pCols = aCols,
      .numOfCols = nCols,
      .loadTombFn = loadSttTomb,
      .pReader = pr,
      .idstr = pr->idstr,
      .pCurRowKey = &pr->rowKey,
  };

  TAOS_CHECK_RETURN(tMergeTreeOpen2(&iter->mergeTree, &conf, NULL));

  iter->pMergeTree = &iter->mergeTree;

  TAOS_RETURN(code);
}

static int32_t lastIterClose(SFSLastIter **iter) {
  int32_t code = 0;

  if ((*iter)->pMergeTree) {
    tMergeTreeClose((*iter)->pMergeTree);
    (*iter)->pMergeTree = NULL;
  }

  *iter = NULL;

  TAOS_RETURN(code);
}

static int32_t lastIterNext(SFSLastIter *iter, TSDBROW **ppRow) {
  bool hasVal = false;
  *ppRow = NULL;

  int32_t code = tMergeTreeNext(iter->pMergeTree, &hasVal);
  if (code != 0) {
    return code;
  }

  if (!hasVal) {
    *ppRow = NULL;
    TAOS_RETURN(code);
  }

  *ppRow = tMergeTreeGetRow(iter->pMergeTree);
  TAOS_RETURN(code);
}

typedef enum SFSNEXTROWSTATES {
  SFSNEXTROW_FS,
  SFSNEXTROW_FILESET,
  SFSNEXTROW_INDEXLIST,
  SFSNEXTROW_BRINBLOCK,
  SFSNEXTROW_BRINRECORD,
  SFSNEXTROW_BLOCKDATA,
  SFSNEXTROW_BLOCKROW,
  SFSNEXTROW_NEXTSTTROW
} SFSNEXTROWSTATES;

struct CacheNextRowIter;

typedef struct SFSNextRowIter {
  SFSNEXTROWSTATES         state;         // [input]
  SBlockIdx               *pBlockIdxExp;  // [input]
  STSchema                *pTSchema;      // [input]
  tb_uid_t                 suid;
  tb_uid_t                 uid;
  int32_t                  iFileSet;
  STFileSet               *pFileSet;
  TFileSetArray           *aDFileSet;
  SArray                  *pIndexList;
  int32_t                  iBrinIndex;
  SBrinBlock               brinBlock;
  SBrinBlock              *pBrinBlock;
  int32_t                  iBrinRecord;
  SBrinRecord              brinRecord;
  SBlockData               blockData;
  SBlockData              *pBlockData;
  int32_t                  nRow;
  int32_t                  iRow;
  TSDBROW                  row;
  int64_t                  lastTs;
  SFSLastIter              lastIter;
  SFSLastIter             *pLastIter;
  int8_t                   lastEmpty;
  TSDBROW                 *pLastRow;
  SRow                    *pTSRow;
  SRowMerger               rowMerger;
  SCacheRowsReader        *pr;
  struct CacheNextRowIter *pRowIter;
} SFSNextRowIter;

static void clearLastFileSet(SFSNextRowIter *state);

static int32_t getNextRowFromFS(void *iter, TSDBROW **ppRow, bool *pIgnoreEarlierTs, bool isLast, int16_t *aCols,
                                int nCols) {
  int32_t         code = 0, lino = 0;
  SFSNextRowIter *state = (SFSNextRowIter *)iter;
  STsdb          *pTsdb = state->pr->pTsdb;

  if (SFSNEXTROW_FS == state->state) {
    state->iFileSet = TARRAY2_SIZE(state->aDFileSet);

    state->state = SFSNEXTROW_FILESET;
  }

  if (SFSNEXTROW_FILESET == state->state) {
  _next_fileset:
    clearLastFileSet(state);

    if (--state->iFileSet < 0) {
      *ppRow = NULL;

      TAOS_RETURN(code);
    } else {
      state->pFileSet = TARRAY2_GET(state->aDFileSet, state->iFileSet);
    }

    STFileObj **pFileObj = state->pFileSet->farr;
    if (pFileObj[0] != NULL || pFileObj[3] != NULL) {
      if (state->pFileSet != state->pr->pCurFileSet) {
        SDataFileReaderConfig conf = {.tsdb = pTsdb, .szPage = pTsdb->pVnode->config.tsdbPageSize};
        const char           *filesName[4] = {0};
        if (pFileObj[0] != NULL) {
          conf.files[0].file = *pFileObj[0]->f;
          conf.files[0].exist = true;
          filesName[0] = pFileObj[0]->fname;

          conf.files[1].file = *pFileObj[1]->f;
          conf.files[1].exist = true;
          filesName[1] = pFileObj[1]->fname;

          conf.files[2].file = *pFileObj[2]->f;
          conf.files[2].exist = true;
          filesName[2] = pFileObj[2]->fname;
        }

        if (pFileObj[3] != NULL) {
          conf.files[3].exist = true;
          conf.files[3].file = *pFileObj[3]->f;
          filesName[3] = pFileObj[3]->fname;
        }

        TAOS_CHECK_GOTO(tsdbDataFileReaderOpen(filesName, &conf, &state->pr->pFileReader), &lino, _err);

        state->pr->pCurFileSet = state->pFileSet;

        code = loadDataTomb(state->pr, state->pr->pFileReader);
        if (code != TSDB_CODE_SUCCESS) {
          tsdbError("vgId:%d, %s load tomb failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                    tstrerror(code));
          TAOS_CHECK_GOTO(code, &lino, _err);
        }

        TAOS_CHECK_GOTO(tsdbDataFileReadBrinBlk(state->pr->pFileReader, &state->pr->pBlkArray), &lino, _err);
      }

      if (!state->pIndexList) {
        state->pIndexList = taosArrayInit(1, sizeof(SBrinBlk));
        if (!state->pIndexList) {
          TAOS_CHECK_GOTO(terrno, &lino, _err);
        }
      } else {
        taosArrayClear(state->pIndexList);
      }

      const TBrinBlkArray *pBlkArray = state->pr->pBlkArray;

      for (int i = TARRAY2_SIZE(pBlkArray) - 1; i >= 0; --i) {
        SBrinBlk *pBrinBlk = &pBlkArray->data[i];
        if (state->suid >= pBrinBlk->minTbid.suid && state->suid <= pBrinBlk->maxTbid.suid) {
          if (state->uid >= pBrinBlk->minTbid.uid && state->uid <= pBrinBlk->maxTbid.uid) {
            if (!taosArrayPush(state->pIndexList, pBrinBlk)) {
              TAOS_CHECK_GOTO(terrno, &lino, _err);
            }
          }
        } else if (state->suid > pBrinBlk->maxTbid.suid ||
                   (state->suid == pBrinBlk->maxTbid.suid && state->uid > pBrinBlk->maxTbid.uid)) {
          break;
        }
      }

      int indexSize = TARRAY_SIZE(state->pIndexList);
      if (indexSize <= 0) {
        goto _check_stt_data;
      }

      state->state = SFSNEXTROW_INDEXLIST;
      state->iBrinIndex = 1;
    }

  _check_stt_data:
    if (state->pFileSet != state->pr->pCurFileSet) {
      state->pr->pCurFileSet = state->pFileSet;
    }

    TAOS_CHECK_GOTO(lastIterOpen(&state->lastIter, state->pFileSet, pTsdb, state->pTSchema, state->suid, state->uid,
                                 state->pr, state->lastTs, aCols, nCols),
                    &lino, _err);

    TAOS_CHECK_GOTO(lastIterNext(&state->lastIter, &state->pLastRow), &lino, _err);

    if (!state->pLastRow) {
      state->lastEmpty = 1;

      if (SFSNEXTROW_INDEXLIST != state->state) {
        clearLastFileSet(state);
        goto _next_fileset;
      }
    } else {
      state->lastEmpty = 0;

      if (SFSNEXTROW_INDEXLIST != state->state) {
        state->state = SFSNEXTROW_NEXTSTTROW;

        *ppRow = state->pLastRow;
        state->pLastRow = NULL;

        TAOS_RETURN(code);
      }
    }

    state->pLastIter = &state->lastIter;
  }

  if (SFSNEXTROW_NEXTSTTROW == state->state) {
    TAOS_CHECK_GOTO(lastIterNext(&state->lastIter, &state->pLastRow), &lino, _err);

    if (!state->pLastRow) {
      if (state->pLastIter) {
        code = lastIterClose(&state->pLastIter);
        if (code != TSDB_CODE_SUCCESS) {
          tsdbError("vgId:%d, %s close last iter failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                    tstrerror(code));
          TAOS_RETURN(code);
        }
      }

      clearLastFileSet(state);
      state->state = SFSNEXTROW_FILESET;
      goto _next_fileset;
    } else {
      *ppRow = state->pLastRow;
      state->pLastRow = NULL;

      TAOS_RETURN(code);
    }
  }

  if (SFSNEXTROW_INDEXLIST == state->state) {
    SBrinBlk *pBrinBlk = NULL;
  _next_brinindex:
    if (--state->iBrinIndex < 0) {
      if (state->pLastRow) {
        state->state = SFSNEXTROW_NEXTSTTROW;
        *ppRow = state->pLastRow;
        state->pLastRow = NULL;
        return code;
      }

      clearLastFileSet(state);
      goto _next_fileset;
    } else {
      pBrinBlk = taosArrayGet(state->pIndexList, state->iBrinIndex);
    }

    if (!state->pBrinBlock) {
      state->pBrinBlock = &state->brinBlock;
    } else {
      tBrinBlockClear(&state->brinBlock);
    }

    TAOS_CHECK_GOTO(tsdbDataFileReadBrinBlock(state->pr->pFileReader, pBrinBlk, &state->brinBlock), &lino, _err);

    state->iBrinRecord = state->brinBlock.numOfRecords - 1;
    state->state = SFSNEXTROW_BRINBLOCK;
  }

  if (SFSNEXTROW_BRINBLOCK == state->state) {
  _next_brinrecord:
    if (state->iBrinRecord < 0) {  // empty brin block, goto _next_brinindex
      tBrinBlockClear(&state->brinBlock);
      goto _next_brinindex;
    }

    TAOS_CHECK_GOTO(tBrinBlockGet(&state->brinBlock, state->iBrinRecord, &state->brinRecord), &lino, _err);

    SBrinRecord *pRecord = &state->brinRecord;
    if (pRecord->uid != state->uid) {
      // TODO: goto next brin block early
      --state->iBrinRecord;
      goto _next_brinrecord;
    }

    state->state = SFSNEXTROW_BRINRECORD;
  }

  if (SFSNEXTROW_BRINRECORD == state->state) {
    SBrinRecord *pRecord = &state->brinRecord;

    if (!state->pBlockData) {
      state->pBlockData = &state->blockData;

      TAOS_CHECK_GOTO(tBlockDataCreate(&state->blockData), &lino, _err);
    } else {
      tBlockDataReset(state->pBlockData);
    }

    if (aCols[0] == PRIMARYKEY_TIMESTAMP_COL_ID) {
      --nCols;
      ++aCols;
    }

    TAOS_CHECK_GOTO(tsdbDataFileReadBlockDataByColumn(state->pr->pFileReader, pRecord, state->pBlockData,
                                                      state->pTSchema, aCols, nCols),
                    &lino, _err);

    state->nRow = state->blockData.nRow;
    state->iRow = state->nRow - 1;

    state->state = SFSNEXTROW_BLOCKROW;
  }

  if (SFSNEXTROW_BLOCKROW == state->state) {
    if (state->iRow < 0) {
      --state->iBrinRecord;
      goto _next_brinrecord;
    }

    state->row = tsdbRowFromBlockData(state->pBlockData, state->iRow);
    if (!state->pLastIter) {
      *ppRow = &state->row;
      --state->iRow;
      return code;
    }

    if (!state->pLastRow) {
      // get next row from fslast and process with fs row, --state->Row if select fs row
      TAOS_CHECK_GOTO(lastIterNext(&state->lastIter, &state->pLastRow), &lino, _err);
    }

    if (!state->pLastRow) {
      if (state->pLastIter) {
        code = lastIterClose(&state->pLastIter);
        if (code != TSDB_CODE_SUCCESS) {
          tsdbError("vgId:%d, %s close last iter failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                    tstrerror(code));
          TAOS_RETURN(code);
        }
      }

      *ppRow = &state->row;
      --state->iRow;
      return code;
    }

    // process state->pLastRow & state->row
    TSKEY rowTs = TSDBROW_TS(&state->row);
    TSKEY lastRowTs = TSDBROW_TS(state->pLastRow);
    if (lastRowTs > rowTs) {
      *ppRow = state->pLastRow;
      state->pLastRow = NULL;

      TAOS_RETURN(code);
    } else if (lastRowTs < rowTs) {
      *ppRow = &state->row;
      --state->iRow;

      TAOS_RETURN(code);
    } else {
      // TODO: merge rows and *ppRow = mergedRow
      SRowMerger *pMerger = &state->rowMerger;
      code = tsdbRowMergerInit(pMerger, state->pTSchema);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbError("vgId:%d, %s init row merger failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
        TAOS_RETURN(code);
      }

      TAOS_CHECK_GOTO(tsdbRowMergerAdd(pMerger, &state->row, state->pTSchema), &lino, _err);
      TAOS_CHECK_GOTO(tsdbRowMergerAdd(pMerger, state->pLastRow, state->pTSchema), &lino, _err);

      if (state->pTSRow) {
        taosMemoryFree(state->pTSRow);
        state->pTSRow = NULL;
      }

      TAOS_CHECK_GOTO(tsdbRowMergerGetRow(pMerger, &state->pTSRow), &lino, _err);

      state->row = tsdbRowFromTSRow(TSDBROW_VERSION(&state->row), state->pTSRow);
      *ppRow = &state->row;
      --state->iRow;

      tsdbRowMergerClear(pMerger);

      TAOS_RETURN(code);
    }
  }

_err:
  clearLastFileSet(state);

  *ppRow = NULL;

  if (code) {
    tsdbError("tsdb/cache: vgId:%d, %s failed at line %d since %s.", TD_VID(pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }

  TAOS_RETURN(code);
}

typedef enum SMEMNEXTROWSTATES {
  SMEMNEXTROW_ENTER,
  SMEMNEXTROW_NEXT,
} SMEMNEXTROWSTATES;

typedef struct SMemNextRowIter {
  SMEMNEXTROWSTATES state;
  STbData          *pMem;  // [input]
  STbDataIter       iter;  // mem buffer skip list iterator
  int64_t           lastTs;
} SMemNextRowIter;

static int32_t getNextRowFromMem(void *iter, TSDBROW **ppRow, bool *pIgnoreEarlierTs, bool isLast, int16_t *aCols,
                                 int nCols) {
  SMemNextRowIter *state = (SMemNextRowIter *)iter;
  int32_t          code = 0;
  *pIgnoreEarlierTs = false;
  switch (state->state) {
    case SMEMNEXTROW_ENTER: {
      if (state->pMem != NULL) {
        /*
        if (state->pMem->maxKey <= state->lastTs) {
          *ppRow = NULL;
          *pIgnoreEarlierTs = true;

          TAOS_RETURN(code);
        }
        */
        tsdbTbDataIterOpen(state->pMem, NULL, 1, &state->iter);

        TSDBROW *pMemRow = tsdbTbDataIterGet(&state->iter);
        if (pMemRow) {
          *ppRow = pMemRow;
          state->state = SMEMNEXTROW_NEXT;

          TAOS_RETURN(code);
        }
      }

      *ppRow = NULL;

      TAOS_RETURN(code);
    }
    case SMEMNEXTROW_NEXT:
      if (tsdbTbDataIterNext(&state->iter)) {
        *ppRow = tsdbTbDataIterGet(&state->iter);

        TAOS_RETURN(code);
      } else {
        *ppRow = NULL;

        TAOS_RETURN(code);
      }
    default:
      break;
  }

_err:
  *ppRow = NULL;

  TAOS_RETURN(code);
}

typedef int32_t (*_next_row_fn_t)(void *iter, TSDBROW **ppRow, bool *pIgnoreEarlierTs, bool isLast, int16_t *aCols,
                                  int nCols);
typedef int32_t (*_next_row_clear_fn_t)(void *iter);

typedef struct {
  TSDBROW             *pRow;
  bool                 stop;
  bool                 next;
  bool                 ignoreEarlierTs;
  void                *iter;
  _next_row_fn_t       nextRowFn;
  _next_row_clear_fn_t nextRowClearFn;
} TsdbNextRowState;

typedef struct {
  SArray           *pMemDelData;
  SArray           *pSkyline;
  int64_t           iSkyline;
  SBlockIdx         idx;
  SMemNextRowIter   memState;
  SMemNextRowIter   imemState;
  TSDBROW           memRow, imemRow;
  TsdbNextRowState  input[2];
  SCacheRowsReader *pr;
  STsdb            *pTsdb;
} MemNextRowIter;

static int32_t memRowIterOpen(MemNextRowIter *pIter, tb_uid_t uid, STsdb *pTsdb, STSchema *pTSchema, tb_uid_t suid,
                              STsdbReadSnap *pReadSnap, SCacheRowsReader *pr) {
  int32_t code = 0, lino = 0;

  STbData *pMem = NULL;
  if (pReadSnap->pMem) {
    pMem = tsdbGetTbDataFromMemTable(pReadSnap->pMem, suid, uid);
  }

  STbData *pIMem = NULL;
  if (pReadSnap->pIMem) {
    pIMem = tsdbGetTbDataFromMemTable(pReadSnap->pIMem, suid, uid);
  }

  pIter->pTsdb = pTsdb;

  pIter->pMemDelData = NULL;

  TAOS_CHECK_GOTO(loadMemTombData(&pIter->pMemDelData, pMem, pIMem, pr->info.verRange.maxVer), &lino, _exit);

  pIter->idx = (SBlockIdx){.suid = suid, .uid = uid};

  pIter->input[0] = (TsdbNextRowState){&pIter->memRow, true, false, false, &pIter->memState, getNextRowFromMem, NULL};
  pIter->input[1] = (TsdbNextRowState){&pIter->imemRow, true, false, false, &pIter->imemState, getNextRowFromMem, NULL};

  if (pMem) {
    pIter->memState.pMem = pMem;
    pIter->memState.state = SMEMNEXTROW_ENTER;
    pIter->input[0].stop = false;
    pIter->input[0].next = true;
  }

  if (pIMem) {
    pIter->imemState.pMem = pIMem;
    pIter->imemState.state = SMEMNEXTROW_ENTER;
    pIter->input[1].stop = false;
    pIter->input[1].next = true;
  }

  pIter->pr = pr;

_exit:
  if (code) {
    tsdbError("tsdb/cache: %s failed at line %d since %s.", __func__, lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static void memRowIterClose(MemNextRowIter *pIter) {
  for (int i = 0; i < 2; ++i) {
    if (pIter->input[i].nextRowClearFn) {
      (void)pIter->input[i].nextRowClearFn(pIter->input[i].iter);
    }
  }

  if (pIter->pSkyline) {
    taosArrayDestroy(pIter->pSkyline);
  }

  if (pIter->pMemDelData) {
    taosArrayDestroy(pIter->pMemDelData);
  }
}

static bool tsdbKeyDeleted(TSDBKEY *key, SArray *pSkyline, int64_t *iSkyline) {
  bool deleted = false;
  while (*iSkyline > 0) {
    TSDBKEY *pItemBack = (TSDBKEY *)taosArrayGet(pSkyline, *iSkyline);
    TSDBKEY *pItemFront = (TSDBKEY *)taosArrayGet(pSkyline, *iSkyline - 1);

    if (key->ts > pItemBack->ts) {
      return false;
    } else if (key->ts >= pItemFront->ts && key->ts <= pItemBack->ts) {
      if (key->version <= pItemFront->version || (key->ts == pItemBack->ts && key->version <= pItemBack->version)) {
        // if (key->version <= pItemFront->version || key->version <= pItemBack->version) {
        return true;
      } else {
        if (*iSkyline > 1) {
          --*iSkyline;
        } else {
          return false;
        }
      }
    } else {
      if (*iSkyline > 1) {
        --*iSkyline;
      } else {
        return false;
      }
    }
  }

  return deleted;
}

// Get next non-deleted row from imem
static TSDBROW *tsdbImemGetNextRow(STbDataIter *pTbIter, SArray *pSkyline, int64_t *piSkyline) {
  int32_t code = 0;

  if (tsdbTbDataIterNext(pTbIter)) {
    TSDBROW *pMemRow = tsdbTbDataIterGet(pTbIter);
    TSDBKEY  rowKey = TSDBROW_KEY(pMemRow);
    bool     deleted = tsdbKeyDeleted(&rowKey, pSkyline, piSkyline);
    if (!deleted) {
      return pMemRow;
    }
  }

  return NULL;
}

// Get first non-deleted row from imem
static TSDBROW *tsdbImemGetFirstRow(SMemTable *imem, STbData *pIMem, STbDataIter *pTbIter, SArray *pSkyline,
                                    int64_t *piSkyline) {
  int32_t code = 0;

  tsdbTbDataIterOpen(pIMem, NULL, 1, pTbIter);
  TSDBROW *pMemRow = tsdbTbDataIterGet(pTbIter);
  if (pMemRow) {
    // if non deleted, return the found row.
    TSDBKEY rowKey = TSDBROW_KEY(pMemRow);
    bool    deleted = tsdbKeyDeleted(&rowKey, pSkyline, piSkyline);
    if (!deleted) {
      return pMemRow;
    }
  } else {
    return NULL;
  }

  // continue to find the non-deleted first row from imem, using get next row
  return tsdbImemGetNextRow(pTbIter, pSkyline, piSkyline);
}

void tsdbCacheInvalidateSchema(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, int32_t sver) {
  SRocksCache *pRCache = &pTsdb->rCache;
  if (!pRCache->pTSchema || sver <= pTsdb->rCache.sver) return;

  if (suid > 0 && suid == pRCache->suid) {
    pRCache->sver = -1;
    pRCache->suid = -1;
  }
  if (suid == 0 && uid == pRCache->uid) {
    pRCache->sver = -1;
    pRCache->uid = -1;
  }
}

static TSDBROW *memRowIterGet(MemNextRowIter *pIter, bool isLast, int16_t *aCols, int nCols) {
  int32_t code = 0, lino = 0;

  for (;;) {
    for (int i = 0; i < 2; ++i) {
      if (pIter->input[i].next && !pIter->input[i].stop) {
        TAOS_CHECK_GOTO(pIter->input[i].nextRowFn(pIter->input[i].iter, &pIter->input[i].pRow,
                                                  &pIter->input[i].ignoreEarlierTs, isLast, aCols, nCols),
                        &lino, _exit);

        if (pIter->input[i].pRow == NULL) {
          pIter->input[i].stop = true;
          pIter->input[i].next = false;
        }
      }
    }

    if (pIter->input[0].stop && pIter->input[1].stop) {
      return NULL;
    }

    TSDBROW *max[2] = {0};
    int      iMax[2] = {-1, -1};
    int      nMax = 0;
    SRowKey  maxKey = {.ts = TSKEY_MIN};

    for (int i = 0; i < 2; ++i) {
      if (!pIter->input[i].stop && pIter->input[i].pRow != NULL) {
        STsdbRowKey tsdbRowKey = {0};
        tsdbRowGetKey(pIter->input[i].pRow, &tsdbRowKey);

        // merging & deduplicating on client side
        int c = tRowKeyCompare(&maxKey, &tsdbRowKey.key);
        if (c <= 0) {
          if (c < 0) {
            nMax = 0;
            maxKey = tsdbRowKey.key;
          }

          iMax[nMax] = i;
          max[nMax++] = pIter->input[i].pRow;
        }
        pIter->input[i].next = false;
      }
    }

    TSDBROW *merge[2] = {0};
    int      iMerge[2] = {-1, -1};
    int      nMerge = 0;
    for (int i = 0; i < nMax; ++i) {
      TSDBKEY maxKey1 = TSDBROW_KEY(max[i]);

      if (!pIter->pSkyline) {
        pIter->pSkyline = taosArrayInit(32, sizeof(TSDBKEY));
        TSDB_CHECK_NULL(pIter->pSkyline, code, lino, _exit, terrno);

        uint64_t        uid = pIter->idx.uid;
        STableLoadInfo *pInfo = getTableLoadInfo(pIter->pr, uid);
        TSDB_CHECK_NULL(pInfo, code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);

        if (pInfo->pTombData == NULL) {
          pInfo->pTombData = taosArrayInit(4, sizeof(SDelData));
          TSDB_CHECK_NULL(pInfo->pTombData, code, lino, _exit, terrno);
        }

        if (!taosArrayAddAll(pInfo->pTombData, pIter->pMemDelData)) {
          TAOS_CHECK_GOTO(terrno, &lino, _exit);
        }

        size_t delSize = TARRAY_SIZE(pInfo->pTombData);
        if (delSize > 0) {
          code = tsdbBuildDeleteSkyline(pInfo->pTombData, 0, (int32_t)(delSize - 1), pIter->pSkyline);
          TAOS_CHECK_GOTO(code, &lino, _exit);
        }
        pIter->iSkyline = taosArrayGetSize(pIter->pSkyline) - 1;
      }

      bool deleted = tsdbKeyDeleted(&maxKey1, pIter->pSkyline, &pIter->iSkyline);
      if (!deleted) {
        iMerge[nMerge] = iMax[i];
        merge[nMerge++] = max[i];
      }

      pIter->input[iMax[i]].next = deleted;
    }

    if (nMerge > 0) {
      pIter->input[iMerge[0]].next = true;

      return merge[0];
    }
  }

_exit:
  if (code) {
    tsdbError("tsdb/cache: %s failed at line %d since %s.", __func__, lino, tstrerror(code));
  }

  return NULL;
}

typedef struct CacheNextRowIter {
  SArray           *pMemDelData;
  SArray           *pSkyline;
  int64_t           iSkyline;
  SBlockIdx         idx;
  SMemNextRowIter   memState;
  SMemNextRowIter   imemState;
  SFSNextRowIter    fsState;
  TSDBROW           memRow, imemRow, fsLastRow, fsRow;
  TsdbNextRowState  input[3];
  SCacheRowsReader *pr;
  STsdb            *pTsdb;
} CacheNextRowIter;

int32_t clearNextRowFromFS(void *iter) {
  int32_t code = 0;

  SFSNextRowIter *state = (SFSNextRowIter *)iter;
  if (!state) {
    TAOS_RETURN(code);
  }

  if (state->pLastIter) {
    code = lastIterClose(&state->pLastIter);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbError("%s close last iter failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      TAOS_RETURN(code);
    }
  }

  if (state->pBlockData) {
    tBlockDataDestroy(state->pBlockData);
    state->pBlockData = NULL;
  }

  if (state->pBrinBlock) {
    tBrinBlockDestroy(state->pBrinBlock);
    state->pBrinBlock = NULL;
  }

  if (state->pIndexList) {
    taosArrayDestroy(state->pIndexList);
    state->pIndexList = NULL;
  }

  if (state->pTSRow) {
    taosMemoryFree(state->pTSRow);
    state->pTSRow = NULL;
  }

  if (state->pRowIter->pSkyline) {
    taosArrayDestroy(state->pRowIter->pSkyline);
    state->pRowIter->pSkyline = NULL;
  }

  TAOS_RETURN(code);
}

static void clearLastFileSet(SFSNextRowIter *state) {
  if (state->pLastIter) {
    int code = lastIterClose(&state->pLastIter);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbError("%s close last iter failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      return;
    }
  }

  if (state->pBlockData) {
    tBlockDataDestroy(state->pBlockData);
    state->pBlockData = NULL;
  }

  if (state->pr->pFileReader) {
    tsdbDataFileReaderClose(&state->pr->pFileReader);
    state->pr->pFileReader = NULL;

    state->pr->pCurFileSet = NULL;
  }

  if (state->pTSRow) {
    taosMemoryFree(state->pTSRow);
    state->pTSRow = NULL;
  }

  if (state->pRowIter->pSkyline) {
    taosArrayDestroy(state->pRowIter->pSkyline);
    state->pRowIter->pSkyline = NULL;

    void   *pe = NULL;
    int32_t iter = 0;
    while ((pe = tSimpleHashIterate(state->pr->pTableMap, pe, &iter)) != NULL) {
      STableLoadInfo *pInfo = *(STableLoadInfo **)pe;
      taosArrayDestroy(pInfo->pTombData);
      pInfo->pTombData = NULL;
    }
  }
}

static int32_t nextRowIterOpen(CacheNextRowIter *pIter, tb_uid_t uid, STsdb *pTsdb, STSchema *pTSchema, tb_uid_t suid,
                               SArray *pLDataIterArray, STsdbReadSnap *pReadSnap, int64_t lastTs,
                               SCacheRowsReader *pr) {
  int32_t code = 0, lino = 0;

  STbData *pMem = NULL;
  if (pReadSnap->pMem) {
    pMem = tsdbGetTbDataFromMemTable(pReadSnap->pMem, suid, uid);
  }

  STbData *pIMem = NULL;
  if (pReadSnap->pIMem) {
    pIMem = tsdbGetTbDataFromMemTable(pReadSnap->pIMem, suid, uid);
  }

  pIter->pTsdb = pTsdb;

  pIter->pMemDelData = NULL;

  TAOS_CHECK_GOTO(loadMemTombData(&pIter->pMemDelData, pMem, pIMem, pr->info.verRange.maxVer), &lino, _err);

  pIter->idx = (SBlockIdx){.suid = suid, .uid = uid};

  pIter->fsState.pRowIter = pIter;
  pIter->fsState.state = SFSNEXTROW_FS;
  pIter->fsState.aDFileSet = pReadSnap->pfSetArray;
  pIter->fsState.pBlockIdxExp = &pIter->idx;
  pIter->fsState.pTSchema = pTSchema;
  pIter->fsState.suid = suid;
  pIter->fsState.uid = uid;
  pIter->fsState.lastTs = lastTs;
  pIter->fsState.pr = pr;

  pIter->input[0] = (TsdbNextRowState){&pIter->memRow, true, false, false, &pIter->memState, getNextRowFromMem, NULL};
  pIter->input[1] = (TsdbNextRowState){&pIter->imemRow, true, false, false, &pIter->imemState, getNextRowFromMem, NULL};
  pIter->input[2] =
      (TsdbNextRowState){&pIter->fsRow, false, true, false, &pIter->fsState, getNextRowFromFS, clearNextRowFromFS};

  if (pMem) {
    pIter->memState.pMem = pMem;
    pIter->memState.state = SMEMNEXTROW_ENTER;
    pIter->memState.lastTs = lastTs;
    pIter->input[0].stop = false;
    pIter->input[0].next = true;
  }

  if (pIMem) {
    pIter->imemState.pMem = pIMem;
    pIter->imemState.state = SMEMNEXTROW_ENTER;
    pIter->imemState.lastTs = lastTs;
    pIter->input[1].stop = false;
    pIter->input[1].next = true;
  }

  pIter->pr = pr;

_err:
  TAOS_RETURN(code);
}

static void nextRowIterClose(CacheNextRowIter *pIter) {
  for (int i = 0; i < 3; ++i) {
    if (pIter->input[i].nextRowClearFn) {
      (void)pIter->input[i].nextRowClearFn(pIter->input[i].iter);
    }
  }

  if (pIter->pSkyline) {
    taosArrayDestroy(pIter->pSkyline);
  }

  if (pIter->pMemDelData) {
    taosArrayDestroy(pIter->pMemDelData);
  }
}

// iterate next row non deleted backward ts, version (from high to low)
static int32_t nextRowIterGet(CacheNextRowIter *pIter, TSDBROW **ppRow, bool *pIgnoreEarlierTs, bool isLast,
                              int16_t *aCols, int nCols) {
  int32_t code = 0, lino = 0;

  for (;;) {
    for (int i = 0; i < 3; ++i) {
      if (pIter->input[i].next && !pIter->input[i].stop) {
        TAOS_CHECK_GOTO(pIter->input[i].nextRowFn(pIter->input[i].iter, &pIter->input[i].pRow,
                                                  &pIter->input[i].ignoreEarlierTs, isLast, aCols, nCols),
                        &lino, _err);

        if (pIter->input[i].pRow == NULL) {
          pIter->input[i].stop = true;
          pIter->input[i].next = false;
        }
      }
    }

    if (pIter->input[0].stop && pIter->input[1].stop && pIter->input[2].stop) {
      *ppRow = NULL;
      *pIgnoreEarlierTs =
          (pIter->input[0].ignoreEarlierTs || pIter->input[1].ignoreEarlierTs || pIter->input[2].ignoreEarlierTs);

      TAOS_RETURN(code);
    }

    // select maxpoint(s) from mem, imem, fs and last
    TSDBROW *max[4] = {0};
    int      iMax[4] = {-1, -1, -1, -1};
    int      nMax = 0;
    SRowKey  maxKey = {.ts = TSKEY_MIN};

    for (int i = 0; i < 3; ++i) {
      if (!pIter->input[i].stop && pIter->input[i].pRow != NULL) {
        STsdbRowKey tsdbRowKey = {0};
        tsdbRowGetKey(pIter->input[i].pRow, &tsdbRowKey);

        // merging & deduplicating on client side
        int c = tRowKeyCompare(&maxKey, &tsdbRowKey.key);
        if (c <= 0) {
          if (c < 0) {
            nMax = 0;
            maxKey = tsdbRowKey.key;
          }

          iMax[nMax] = i;
          max[nMax++] = pIter->input[i].pRow;
        }
        pIter->input[i].next = false;
      }
    }

    // delete detection
    TSDBROW *merge[4] = {0};
    int      iMerge[4] = {-1, -1, -1, -1};
    int      nMerge = 0;
    for (int i = 0; i < nMax; ++i) {
      TSDBKEY maxKey1 = TSDBROW_KEY(max[i]);

      if (!pIter->pSkyline) {
        pIter->pSkyline = taosArrayInit(32, sizeof(TSDBKEY));
        TSDB_CHECK_NULL(pIter->pSkyline, code, lino, _err, terrno);

        uint64_t        uid = pIter->idx.uid;
        STableLoadInfo *pInfo = getTableLoadInfo(pIter->pr, uid);
        TSDB_CHECK_NULL(pInfo, code, lino, _err, TSDB_CODE_OUT_OF_MEMORY);

        if (pInfo->pTombData == NULL) {
          pInfo->pTombData = taosArrayInit(4, sizeof(SDelData));
          TSDB_CHECK_NULL(pInfo->pTombData, code, lino, _err, terrno);
        }

        if (!taosArrayAddAll(pInfo->pTombData, pIter->pMemDelData)) {
          TAOS_CHECK_GOTO(terrno, &lino, _err);
        }

        size_t delSize = TARRAY_SIZE(pInfo->pTombData);
        if (delSize > 0) {
          code = tsdbBuildDeleteSkyline(pInfo->pTombData, 0, (int32_t)(delSize - 1), pIter->pSkyline);
          TAOS_CHECK_GOTO(code, &lino, _err);
        }
        pIter->iSkyline = taosArrayGetSize(pIter->pSkyline) - 1;
      }

      bool deleted = tsdbKeyDeleted(&maxKey1, pIter->pSkyline, &pIter->iSkyline);
      if (!deleted) {
        iMerge[nMerge] = iMax[i];
        merge[nMerge++] = max[i];
      }

      pIter->input[iMax[i]].next = deleted;
    }

    if (nMerge > 0) {
      pIter->input[iMerge[0]].next = true;

      *ppRow = merge[0];

      TAOS_RETURN(code);
    }
  }

_err:
  if (code) {
    tsdbError("tsdb/cache: %s failed at line %d since %s.", __func__, lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t initLastColArrayPartial(STSchema *pTSchema, SArray **ppColArray, int16_t *slotIds, int nCols) {
  SArray *pColArray = taosArrayInit(nCols, sizeof(SLastCol));
  if (NULL == pColArray) {
    TAOS_RETURN(terrno);
  }

  for (int32_t i = 0; i < nCols; ++i) {
    int16_t  slotId = slotIds[i];
    SLastCol col = {.rowKey.ts = 0,
                    .colVal = COL_VAL_NULL(pTSchema->columns[slotId].colId, pTSchema->columns[slotId].type)};
    if (!taosArrayPush(pColArray, &col)) {
      TAOS_RETURN(terrno);
    }
  }
  *ppColArray = pColArray;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cloneTSchema(STSchema *pSrc, STSchema **ppDst) {
  int32_t len = sizeof(STSchema) + sizeof(STColumn) * pSrc->numOfCols;
  *ppDst = taosMemoryMalloc(len);
  if (NULL == *ppDst) {
    TAOS_RETURN(terrno);
  }
  memcpy(*ppDst, pSrc, len);

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t updateTSchema(int32_t sversion, SCacheRowsReader *pReader, uint64_t uid) {
  if (NULL == pReader->pCurrSchema && sversion == pReader->pSchema->version) {
    TAOS_RETURN(cloneTSchema(pReader->pSchema, &pReader->pCurrSchema));
  }

  if (NULL != pReader->pCurrSchema && sversion == pReader->pCurrSchema->version) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  taosMemoryFreeClear(pReader->pCurrSchema);
  TAOS_RETURN(
      metaGetTbTSchemaEx(pReader->pTsdb->pVnode->pMeta, pReader->info.suid, uid, sversion, &pReader->pCurrSchema));
}

static int32_t mergeLastCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                            int nCols, int16_t *slotIds) {
  int32_t   code = 0, lino = 0;
  STSchema *pTSchema = pr->pSchema;  // metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1, 1);
  int16_t   nLastCol = nCols;
  int16_t   noneCol = 0;
  bool      setNoneCol = false;
  bool      hasRow = false;
  bool      ignoreEarlierTs = false;
  SArray   *pColArray = NULL;
  SColVal  *pColVal = &(SColVal){0};

  TAOS_CHECK_RETURN(initLastColArrayPartial(pTSchema, &pColArray, slotIds, nCols));

  SArray *aColArray = taosArrayInit(nCols, sizeof(int16_t));
  if (NULL == aColArray) {
    taosArrayDestroy(pColArray);

    TAOS_RETURN(terrno);
  }

  for (int i = 0; i < nCols; ++i) {
    if (!taosArrayPush(aColArray, &aCols[i])) {
      taosArrayDestroy(pColArray);

      TAOS_RETURN(terrno);
    }
  }

  STsdbRowKey lastRowKey = {.key.ts = TSKEY_MAX};

  // inverse iterator
  CacheNextRowIter iter = {0};
  code =
      nextRowIterOpen(&iter, uid, pTsdb, pTSchema, pr->info.suid, pr->pLDataIterArray, pr->pReadSnap, pr->lastTs, pr);
  TAOS_CHECK_GOTO(code, &lino, _err);

  do {
    TSDBROW *pRow = NULL;
    code = nextRowIterGet(&iter, &pRow, &ignoreEarlierTs, true, TARRAY_DATA(aColArray), TARRAY_SIZE(aColArray));

    if (!pRow) {
      break;
    }

    hasRow = true;

    int32_t sversion = TSDBROW_SVERSION(pRow);
    if (sversion != -1) {
      TAOS_CHECK_GOTO(updateTSchema(sversion, pr, uid), &lino, _err);

      pTSchema = pr->pCurrSchema;
    }
    // int16_t nCol = pTSchema->numOfCols;

    STsdbRowKey rowKey = {0};
    tsdbRowGetKey(pRow, &rowKey);

    if (lastRowKey.key.ts == TSKEY_MAX) {  // first time
      lastRowKey = rowKey;

      for (int16_t iCol = noneCol; iCol < nCols; ++iCol) {
        if (iCol >= nLastCol) {
          break;
        }
        SLastCol *pCol = taosArrayGet(pColArray, iCol);
        if (slotIds[iCol] > pTSchema->numOfCols - 1) {
          if (!setNoneCol) {
            noneCol = iCol;
            setNoneCol = true;
          }
          continue;
        }
        if (pCol->colVal.cid != pTSchema->columns[slotIds[iCol]].colId) {
          continue;
        }
        if (slotIds[iCol] == 0) {
          STColumn *pTColumn = &pTSchema->columns[0];
          SValue    val = {.type = pTColumn->type};
          VALUE_SET_TRIVIAL_DATUM(&val, rowKey.key.ts);
          *pColVal = COL_VAL_VALUE(pTColumn->colId, val);

          SLastCol colTmp = {.rowKey = rowKey.key, .colVal = *pColVal, .cacheStatus = TSDB_LAST_CACHE_VALID};
          TAOS_CHECK_GOTO(tsdbCacheReallocSLastCol(&colTmp, NULL), &lino, _err);

          taosArraySet(pColArray, 0, &colTmp);
          continue;
        }
        tsdbRowGetColVal(pRow, pTSchema, slotIds[iCol], pColVal);

        *pCol = (SLastCol){.rowKey = rowKey.key, .colVal = *pColVal, .cacheStatus = TSDB_LAST_CACHE_VALID};
        TAOS_CHECK_GOTO(tsdbCacheReallocSLastCol(pCol, NULL), &lino, _err);

        if (!COL_VAL_IS_VALUE(pColVal)) {
          if (!setNoneCol) {
            noneCol = iCol;
            setNoneCol = true;
          }
        } else {
          int32_t aColIndex = taosArraySearchIdx(aColArray, &pColVal->cid, compareInt16Val, TD_EQ);
          if (aColIndex >= 0) {
            taosArrayRemove(aColArray, aColIndex);
          }
        }
      }
      if (!setNoneCol) {
        // done, goto return pColArray
        break;
      } else {
        continue;
      }
    }

    // merge into pColArray
    setNoneCol = false;
    for (int16_t iCol = noneCol; iCol < nCols; ++iCol) {
      if (iCol >= nLastCol) {
        break;
      }
      // high version's column value
      if (slotIds[iCol] > pTSchema->numOfCols - 1) {
        continue;
      }

      SLastCol *lastColVal = (SLastCol *)taosArrayGet(pColArray, iCol);
      if (lastColVal->colVal.cid != pTSchema->columns[slotIds[iCol]].colId) {
        continue;
      }
      SColVal *tColVal = &lastColVal->colVal;
      if (COL_VAL_IS_VALUE(tColVal)) continue;

      tsdbRowGetColVal(pRow, pTSchema, slotIds[iCol], pColVal);
      if (COL_VAL_IS_VALUE(pColVal)) {
        SLastCol lastCol = {.rowKey = rowKey.key, .colVal = *pColVal, .cacheStatus = TSDB_LAST_CACHE_VALID};
        TAOS_CHECK_GOTO(tsdbCacheReallocSLastCol(&lastCol, NULL), &lino, _err);

        tsdbCacheFreeSLastColItem(lastColVal);
        taosArraySet(pColArray, iCol, &lastCol);
        int32_t aColIndex = taosArraySearchIdx(aColArray, &lastCol.colVal.cid, compareInt16Val, TD_EQ);
        if (aColIndex >= 0) {
          taosArrayRemove(aColArray, aColIndex);
        }
      } else if (!COL_VAL_IS_VALUE(pColVal) && !setNoneCol) {
        noneCol = iCol;
        setNoneCol = true;
      }
    }
  } while (setNoneCol);

  if (!hasRow) {
    if (ignoreEarlierTs) {
      taosArrayDestroy(pColArray);
      pColArray = NULL;
    } else {
      taosArrayClear(pColArray);
    }
  }
  *ppLastArray = pColArray;

  nextRowIterClose(&iter);
  taosArrayDestroy(aColArray);

  TAOS_RETURN(code);

_err:
  nextRowIterClose(&iter);
  // taosMemoryFreeClear(pTSchema);
  *ppLastArray = NULL;
  taosArrayDestroyEx(pColArray, tsdbCacheFreeSLastColItem);
  taosArrayDestroy(aColArray);

  if (code) {
    tsdbError("tsdb/cache: vgId:%d, %s failed at line %d since %s.", TD_VID(pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t mergeLastRowCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                               int nCols, int16_t *slotIds) {
  int32_t   code = 0, lino = 0;
  STSchema *pTSchema = pr->pSchema;  // metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1, 1);
  int16_t   nLastCol = nCols;
  int16_t   noneCol = 0;
  bool      setNoneCol = false;
  bool      hasRow = false;
  bool      ignoreEarlierTs = false;
  SArray   *pColArray = NULL;
  SColVal  *pColVal = &(SColVal){0};

  TAOS_CHECK_RETURN(initLastColArrayPartial(pTSchema, &pColArray, slotIds, nCols));

  SArray *aColArray = taosArrayInit(nCols, sizeof(int16_t));
  if (NULL == aColArray) {
    taosArrayDestroy(pColArray);

    TAOS_RETURN(terrno);
  }

  for (int i = 0; i < nCols; ++i) {
    if (!taosArrayPush(aColArray, &aCols[i])) {
      taosArrayDestroy(pColArray);

      TAOS_RETURN(terrno);
    }
  }

  // inverse iterator
  CacheNextRowIter iter = {0};
  code =
      nextRowIterOpen(&iter, uid, pTsdb, pTSchema, pr->info.suid, pr->pLDataIterArray, pr->pReadSnap, pr->lastTs, pr);
  TAOS_CHECK_GOTO(code, &lino, _err);

  do {
    TSDBROW *pRow = NULL;
    code = nextRowIterGet(&iter, &pRow, &ignoreEarlierTs, false, TARRAY_DATA(aColArray), TARRAY_SIZE(aColArray));

    if (!pRow) {
      break;
    }

    hasRow = true;

    int32_t sversion = TSDBROW_SVERSION(pRow);
    if (sversion != -1) {
      TAOS_CHECK_GOTO(updateTSchema(sversion, pr, uid), &lino, _err);

      pTSchema = pr->pCurrSchema;
    }
    // int16_t nCol = pTSchema->numOfCols;

    STsdbRowKey rowKey = {0};
    tsdbRowGetKey(pRow, &rowKey);

    for (int16_t iCol = noneCol; iCol < nCols; ++iCol) {
      if (iCol >= nLastCol) {
        break;
      }
      SLastCol *pCol = taosArrayGet(pColArray, iCol);
      if (slotIds[iCol] > pTSchema->numOfCols - 1) {
        continue;
      }
      if (pCol->colVal.cid != pTSchema->columns[slotIds[iCol]].colId) {
        continue;
      }
      if (slotIds[iCol] == 0) {
        STColumn *pTColumn = &pTSchema->columns[0];
        SValue    val = {.type = pTColumn->type};
        VALUE_SET_TRIVIAL_DATUM(&val, rowKey.key.ts);
        *pColVal = COL_VAL_VALUE(pTColumn->colId, val);

        SLastCol colTmp = {.rowKey = rowKey.key, .colVal = *pColVal, .cacheStatus = TSDB_LAST_CACHE_VALID};
        TAOS_CHECK_GOTO(tsdbCacheReallocSLastCol(&colTmp, NULL), &lino, _err);

        taosArraySet(pColArray, 0, &colTmp);
        continue;
      }
      tsdbRowGetColVal(pRow, pTSchema, slotIds[iCol], pColVal);

      *pCol = (SLastCol){.rowKey = rowKey.key, .colVal = *pColVal, .cacheStatus = TSDB_LAST_CACHE_VALID};
      TAOS_CHECK_GOTO(tsdbCacheReallocSLastCol(pCol, NULL), &lino, _err);

      int32_t aColIndex = taosArraySearchIdx(aColArray, &pColVal->cid, compareInt16Val, TD_EQ);
      if (aColIndex >= 0) {
        taosArrayRemove(aColArray, aColIndex);
      }
    }

    break;
  } while (1);

  if (!hasRow) {
    if (ignoreEarlierTs) {
      taosArrayDestroy(pColArray);
      pColArray = NULL;
    } else {
      taosArrayClear(pColArray);
    }
  }
  *ppLastArray = pColArray;

  nextRowIterClose(&iter);
  taosArrayDestroy(aColArray);

  TAOS_RETURN(code);

_err:
  nextRowIterClose(&iter);

  *ppLastArray = NULL;
  taosArrayDestroyEx(pColArray, tsdbCacheFreeSLastColItem);
  taosArrayDestroy(aColArray);

  if (code) {
    tsdbError("tsdb/cache: vgId:%d, %s failed at line %d since %s.", TD_VID(pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }

  TAOS_RETURN(code);
}

int32_t tsdbLoadFromImem(SMemTable *imem, int64_t suid, int64_t uid) {
  int32_t      code = 0;
  int32_t      lino = 0;
  STsdb       *pTsdb = imem->pTsdb;
  SArray      *pMemDelData = NULL;
  SArray      *pSkyline = NULL;
  int64_t      iSkyline = 0;
  STbDataIter  tbIter = {0};
  TSDBROW     *pMemRow = NULL;
  STSchema    *pTSchema = NULL;
  SSHashObj   *iColHash = NULL;
  int32_t      sver;
  int32_t      nCol;
  SArray      *ctxArray = pTsdb->rCache.ctxArray;
  STsdbRowKey  tsdbRowKey = {0};
  STSDBRowIter iter = {0};

  STbData *pIMem = tsdbGetTbDataFromMemTable(imem, suid, uid);

  // load imem tomb data and build skyline
  TAOS_CHECK_GOTO(loadMemTombData(&pMemDelData, NULL, pIMem, INT64_MAX), &lino, _exit);

  // tsdbBuildDeleteSkyline
  size_t delSize = TARRAY_SIZE(pMemDelData);
  if (delSize > 0) {
    pSkyline = taosArrayInit(32, sizeof(TSDBKEY));
    if (!pSkyline) {
      TAOS_CHECK_EXIT(terrno);
    }

    TAOS_CHECK_EXIT(tsdbBuildDeleteSkyline(pMemDelData, 0, (int32_t)(delSize - 1), pSkyline));
    iSkyline = taosArrayGetSize(pSkyline) - 1;
  }

  pMemRow = tsdbImemGetFirstRow(imem, pIMem, &tbIter, pSkyline, &iSkyline);
  if (!pMemRow) {
    goto _exit;
  }

  // iter first row to last_row/last col values to ctxArray, and mark last null col ids
  sver = TSDBROW_SVERSION(pMemRow);
  TAOS_CHECK_GOTO(tsdbUpdateSkm(pTsdb, suid, uid, sver), &lino, _exit);
  pTSchema = pTsdb->rCache.pTSchema;
  nCol = pTSchema->numOfCols;

  tsdbRowGetKey(pMemRow, &tsdbRowKey);

  TAOS_CHECK_EXIT(tsdbRowIterOpen(&iter, pMemRow, pTSchema));

  int32_t iCol = 0;
  for (SColVal *pColVal = tsdbRowIterNext(&iter); pColVal && iCol < nCol; pColVal = tsdbRowIterNext(&iter), iCol++) {
    SLastUpdateCtx updateCtx = {.lflag = LFLAG_LAST_ROW, .tsdbRowKey = tsdbRowKey, .colVal = *pColVal};
    if (!taosArrayPush(ctxArray, &updateCtx)) {
      TAOS_CHECK_EXIT(terrno);
    }

    if (COL_VAL_IS_VALUE(pColVal)) {
      updateCtx.lflag = LFLAG_LAST;
      if (!taosArrayPush(ctxArray, &updateCtx)) {
        TAOS_CHECK_EXIT(terrno);
      }
    } else {
      if (!iColHash) {
        iColHash = tSimpleHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
        if (iColHash == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
      }

      if (tSimpleHashPut(iColHash, &pColVal->cid, sizeof(pColVal->cid), &pColVal->cid, sizeof(pColVal->cid))) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }
  tsdbRowClose(&iter);

  // continue to get next row to fill null last col values
  pMemRow = tsdbImemGetNextRow(&tbIter, pSkyline, &iSkyline);
  while (pMemRow) {
    if (tSimpleHashGetSize(iColHash) == 0) {
      break;
    }

    sver = TSDBROW_SVERSION(pMemRow);
    TAOS_CHECK_EXIT(tsdbUpdateSkm(pTsdb, suid, uid, sver));
    pTSchema = pTsdb->rCache.pTSchema;

    STsdbRowKey tsdbRowKey = {0};
    tsdbRowGetKey(pMemRow, &tsdbRowKey);

    TAOS_CHECK_EXIT(tsdbRowIterOpen(&iter, pMemRow, pTSchema));

    int32_t iCol = 0;
    for (SColVal *pColVal = tsdbRowIterNext(&iter); pColVal && iCol < nCol; pColVal = tsdbRowIterNext(&iter), iCol++) {
      if (tSimpleHashGet(iColHash, &pColVal->cid, sizeof(pColVal->cid)) && COL_VAL_IS_VALUE(pColVal)) {
        SLastUpdateCtx updateCtx = {.lflag = LFLAG_LAST, .tsdbRowKey = tsdbRowKey, .colVal = *pColVal};
        if (!taosArrayPush(ctxArray, &updateCtx)) {
          TAOS_CHECK_EXIT(terrno);
        }

        TAOS_CHECK_EXIT(tSimpleHashRemove(iColHash, &pColVal->cid, sizeof(pColVal->cid)));
      }
    }
    tsdbRowClose(&iter);

    pMemRow = tsdbImemGetNextRow(&tbIter, pSkyline, &iSkyline);
  }

  TAOS_CHECK_GOTO(tsdbCacheUpdate(pTsdb, suid, uid, ctxArray), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));

    tsdbRowClose(&iter);
  }

  taosArrayClear(ctxArray);
  // destroy any allocated resource
  tSimpleHashCleanup(iColHash);
  if (pMemDelData) {
    taosArrayDestroy(pMemDelData);
  }
  if (pSkyline) {
    taosArrayDestroy(pSkyline);
  }

  TAOS_RETURN(code);
}

int32_t tsdbCacheUpdateFromIMem(STsdb *pTsdb) {
  if (!pTsdb) return 0;
  if (!pTsdb->imem) return 0;

  int32_t    code = 0;
  int32_t    lino = 0;
  SMemTable *imem = pTsdb->imem;
  int32_t    nTbData = imem->nTbData;
  int64_t    nRow = imem->nRow;
  int64_t    nDel = imem->nDel;

  if (nRow == 0 || nTbData == 0) return 0;

  TAOS_CHECK_EXIT(tsdbMemTableSaveToCache(imem, tsdbLoadFromImem));

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done, nRow:%" PRId64 " nDel:%" PRId64, TD_VID(pTsdb->pVnode), __func__, nRow, nDel);
  }

  TAOS_RETURN(code);
}

int32_t tsdbCacheLoadFromRaw(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SArray *remainCols, SCacheRowsReader *pr,
                             int8_t ltype) {
  int32_t code = 0, lino = 0;
  // rocksdb_writebatch_t *wb = NULL;
  SArray *pTmpColArray = NULL;
  bool    extraTS = false;

  SIdxKey *idxKey = taosArrayGet(remainCols, 0);
  if (idxKey->key.cid != PRIMARYKEY_TIMESTAMP_COL_ID) {
    // ignore 'ts' loaded from cache and load it from tsdb
    // SLastCol *pLastCol = taosArrayGet(pLastArray, 0);
    // tsdbCacheUpdateLastColToNone(pLastCol, TSDB_LAST_CACHE_NO_CACHE);

    SLastKey *key = &(SLastKey){.lflag = ltype, .uid = uid, .cid = PRIMARYKEY_TIMESTAMP_COL_ID};
    if (!taosArrayInsert(remainCols, 0, &(SIdxKey){0, *key})) {
      TAOS_RETURN(terrno);
    }

    extraTS = true;
  }

  int      num_keys = TARRAY_SIZE(remainCols);
  int16_t *slotIds = taosMemoryMalloc(num_keys * sizeof(int16_t));

  int16_t *lastColIds = NULL, *lastSlotIds = NULL, *lastrowColIds = NULL, *lastrowSlotIds = NULL;
  lastColIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  lastSlotIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  lastrowColIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  lastrowSlotIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  SArray *lastTmpColArray = NULL, *lastTmpIndexArray = NULL, *lastrowTmpColArray = NULL, *lastrowTmpIndexArray = NULL;

  int lastIndex = 0;
  int lastrowIndex = 0;

  if (!slotIds || !lastColIds || !lastSlotIds || !lastrowColIds || !lastrowSlotIds) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int i = 0; i < num_keys; ++i) {
    SIdxKey *idxKey = taosArrayGet(remainCols, i);
    if (extraTS && !i) {
      slotIds[i] = 0;
    } else {
      slotIds[i] = pr->pSlotIds[idxKey->idx];
    }

    if (IS_LAST_KEY(idxKey->key)) {
      if (NULL == lastTmpIndexArray) {
        lastTmpIndexArray = taosArrayInit(num_keys, sizeof(int32_t));
        if (!lastTmpIndexArray) {
          TAOS_CHECK_EXIT(terrno);
        }
      }
      if (!taosArrayPush(lastTmpIndexArray, &(i))) {
        TAOS_CHECK_EXIT(terrno);
      }
      lastColIds[lastIndex] = idxKey->key.cid;
      if (extraTS && !i) {
        lastSlotIds[lastIndex] = 0;
      } else {
        lastSlotIds[lastIndex] = pr->pSlotIds[idxKey->idx];
      }
      lastIndex++;
    } else {
      if (NULL == lastrowTmpIndexArray) {
        lastrowTmpIndexArray = taosArrayInit(num_keys, sizeof(int32_t));
        if (!lastrowTmpIndexArray) {
          TAOS_CHECK_EXIT(terrno);
        }
      }
      if (!taosArrayPush(lastrowTmpIndexArray, &(i))) {
        TAOS_CHECK_EXIT(terrno);
      }
      lastrowColIds[lastrowIndex] = idxKey->key.cid;
      if (extraTS && !i) {
        lastrowSlotIds[lastrowIndex] = 0;
      } else {
        lastrowSlotIds[lastrowIndex] = pr->pSlotIds[idxKey->idx];
      }
      lastrowIndex++;
    }
  }

  pTmpColArray = taosArrayInit(lastIndex + lastrowIndex, sizeof(SLastCol));
  if (!pTmpColArray) {
    TAOS_CHECK_EXIT(terrno);
  }

  if (lastTmpIndexArray != NULL) {
    TAOS_CHECK_EXIT(mergeLastCid(uid, pTsdb, &lastTmpColArray, pr, lastColIds, lastIndex, lastSlotIds));
    for (int i = 0; i < taosArrayGetSize(lastTmpColArray); i++) {
      if (!taosArrayInsert(pTmpColArray, *(int32_t *)taosArrayGet(lastTmpIndexArray, i),
                           taosArrayGet(lastTmpColArray, i))) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  if (lastrowTmpIndexArray != NULL) {
    TAOS_CHECK_EXIT(mergeLastRowCid(uid, pTsdb, &lastrowTmpColArray, pr, lastrowColIds, lastrowIndex, lastrowSlotIds));
    for (int i = 0; i < taosArrayGetSize(lastrowTmpColArray); i++) {
      if (!taosArrayInsert(pTmpColArray, *(int32_t *)taosArrayGet(lastrowTmpIndexArray, i),
                           taosArrayGet(lastrowTmpColArray, i))) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  SLRUCache *pCache = pTsdb->lruCache;
  for (int i = 0; i < num_keys; ++i) {
    SIdxKey  *idxKey = taosArrayGet(remainCols, i);
    SLastCol *pLastCol = NULL;

    if (pTmpColArray && TARRAY_SIZE(pTmpColArray) >= i + 1) {
      pLastCol = taosArrayGet(pTmpColArray, i);
    }

    // still null, then make up a none col value
    SLastCol noneCol = {.rowKey.ts = TSKEY_MIN,
                        .colVal = COL_VAL_NONE(idxKey->key.cid, pr->pSchema->columns[slotIds[i]].type),
                        .cacheStatus = TSDB_LAST_CACHE_VALID};
    if (!pLastCol) {
      pLastCol = &noneCol;
    }

    if (!extraTS || i > 0) {
      taosArraySet(pLastArray, idxKey->idx, pLastCol);
    }

    // taosArrayRemove(remainCols, i);

    if (/*!pTmpColArray*/ lastTmpIndexArray && !lastTmpColArray) {
      continue;
    }
    if (/*!pTmpColArray*/ lastrowTmpIndexArray && !lastrowTmpColArray) {
      continue;
    }
#ifndef TSDB_CACHE_ROW_BASED
    // store result back to rocks cache
    code = tsdbCachePutToRocksdb(pTsdb, &idxKey->key, pLastCol);
    if (code) {
      tsdbError("vgId:%d, %s failed at line %d since %s.", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
      TAOS_CHECK_EXIT(code);
    }
    code = tsdbCachePutToLRU(pTsdb, &idxKey->key, pLastCol, 0);
    if (code) {
      tsdbError("vgId:%d, %s failed at line %d since %s.", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
      TAOS_CHECK_EXIT(code);
    }
#endif

    if (extraTS && i == 0) {
      tsdbCacheFreeSLastColItem(pLastCol);
    }
  }

  rocksMayWrite(pTsdb, false);

_exit:
  taosArrayDestroy(lastrowTmpIndexArray);
  taosArrayDestroy(lastrowTmpColArray);
  taosArrayDestroy(lastTmpIndexArray);
  taosArrayDestroy(lastTmpColArray);

  taosMemoryFree(lastColIds);
  taosMemoryFree(lastSlotIds);
  taosMemoryFree(lastrowColIds);
  taosMemoryFree(lastrowSlotIds);

  taosArrayDestroy(pTmpColArray);

  taosMemoryFree(slotIds);

  TAOS_RETURN(code);
}

int32_t tsdbCacheGetBatchFromMem(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SCacheRowsReader *pr,
                                 SArray *keyArray) {
  int32_t        code = 0;
  int32_t        lino = 0;
  STSchema      *pTSchema = pr->pSchema;
  SLRUCache     *pCache = pTsdb->lruCache;
  SArray        *pCidList = pr->pCidList;
  int            numKeys = TARRAY_SIZE(pCidList);
  MemNextRowIter iter = {0};
  SSHashObj     *iColHash = NULL;
  STSDBRowIter   rowIter = {0};

  // 1, get from mem, imem filtered with delete info
  TAOS_CHECK_EXIT(memRowIterOpen(&iter, uid, pTsdb, pTSchema, pr->info.suid, pr->pReadSnap, pr));

  TSDBROW *pRow = memRowIterGet(&iter, false, NULL, 0);
  if (!pRow) {
    goto _exit;
  }

  int32_t sversion = TSDBROW_SVERSION(pRow);
  if (sversion != -1) {
    TAOS_CHECK_EXIT(updateTSchema(sversion, pr, uid));

    pTSchema = pr->pCurrSchema;
  }
  int32_t nCol = pTSchema->numOfCols;

  STsdbRowKey rowKey = {0};
  tsdbRowGetKey(pRow, &rowKey);

  TAOS_CHECK_EXIT(tsdbRowIterOpen(&rowIter, pRow, pTSchema));

  int32_t iCol = 0, jCol = 0, jnCol = TARRAY_SIZE(pLastArray);
  for (SColVal *pColVal = tsdbRowIterNext(&rowIter); pColVal && iCol < nCol && jCol < jnCol;) {
    SLastCol *pTargetCol = &((SLastCol *)TARRAY_DATA(pLastArray))[jCol];
    if (pColVal->cid < pTargetCol->colVal.cid) {
      pColVal = tsdbRowIterNext(&rowIter), ++iCol;

      continue;
    }
    if (pColVal->cid > pTargetCol->colVal.cid) {
      break;
    }

    int32_t cmp_res = tRowKeyCompare(&pTargetCol->rowKey, &rowKey.key);
    if (!IS_LAST_KEY(((SLastKey *)TARRAY_DATA(keyArray))[jCol])) {
      if (cmp_res < 0 || (cmp_res == 0 && !COL_VAL_IS_NONE(pColVal))) {
        SLastCol lastCol = {.rowKey = rowKey.key, .colVal = *pColVal, .dirty = 1, .cacheStatus = TSDB_LAST_CACHE_VALID};
        TAOS_CHECK_EXIT(tsdbCacheReallocSLastCol(&lastCol, NULL));

        tsdbCacheFreeSLastColItem(pTargetCol);
        taosArraySet(pLastArray, jCol, &lastCol);
      }
    } else {
      if (COL_VAL_IS_VALUE(pColVal)) {
        if (cmp_res <= 0) {
          SLastCol lastCol = {
              .rowKey = rowKey.key, .colVal = *pColVal, .dirty = 1, .cacheStatus = TSDB_LAST_CACHE_VALID};
          TAOS_CHECK_EXIT(tsdbCacheReallocSLastCol(&lastCol, NULL));

          tsdbCacheFreeSLastColItem(pTargetCol);
          taosArraySet(pLastArray, jCol, &lastCol);
        }
      } else {
        if (!iColHash) {
          iColHash = tSimpleHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
          if (iColHash == NULL) {
            TAOS_CHECK_EXIT(terrno);
          }
        }

        if (tSimpleHashPut(iColHash, &pColVal->cid, sizeof(pColVal->cid), &jCol, sizeof(jCol))) {
          TAOS_CHECK_EXIT(terrno);
        }
      }
    }

    ++jCol;

    if (jCol < jnCol && ((SLastCol *)TARRAY_DATA(pLastArray))[jCol].colVal.cid > pColVal->cid) {
      pColVal = tsdbRowIterNext(&rowIter), ++iCol;
    }
  }
  tsdbRowClose(&rowIter);

  if (iColHash && tSimpleHashGetSize(iColHash) > 0) {
    pRow = memRowIterGet(&iter, false, NULL, 0);
    while (pRow) {
      if (tSimpleHashGetSize(iColHash) == 0) {
        break;
      }

      sversion = TSDBROW_SVERSION(pRow);
      if (sversion != -1) {
        TAOS_CHECK_EXIT(updateTSchema(sversion, pr, uid));

        pTSchema = pr->pCurrSchema;
      }
      nCol = pTSchema->numOfCols;

      STsdbRowKey tsdbRowKey = {0};
      tsdbRowGetKey(pRow, &tsdbRowKey);

      TAOS_CHECK_EXIT(tsdbRowIterOpen(&rowIter, pRow, pTSchema));

      iCol = 0;
      for (SColVal *pColVal = tsdbRowIterNext(&rowIter); pColVal && iCol < nCol;
           pColVal = tsdbRowIterNext(&rowIter), iCol++) {
        int32_t *pjCol = tSimpleHashGet(iColHash, &pColVal->cid, sizeof(pColVal->cid));
        if (pjCol && COL_VAL_IS_VALUE(pColVal)) {
          SLastCol *pTargetCol = &((SLastCol *)TARRAY_DATA(pLastArray))[*pjCol];

          int32_t cmp_res = tRowKeyCompare(&pTargetCol->rowKey, &tsdbRowKey.key);
          if (cmp_res <= 0) {
            SLastCol lastCol = {
                .rowKey = tsdbRowKey.key, .colVal = *pColVal, .dirty = 1, .cacheStatus = TSDB_LAST_CACHE_VALID};
            TAOS_CHECK_EXIT(tsdbCacheReallocSLastCol(&lastCol, NULL));

            tsdbCacheFreeSLastColItem(pTargetCol);
            taosArraySet(pLastArray, *pjCol, &lastCol);
          }

          TAOS_CHECK_EXIT(tSimpleHashRemove(iColHash, &pColVal->cid, sizeof(pColVal->cid)));
        }
      }
      tsdbRowClose(&rowIter);

      pRow = memRowIterGet(&iter, false, NULL, 0);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));

    tsdbRowClose(&rowIter);
  }

  tSimpleHashCleanup(iColHash);

  memRowIterClose(&iter);

  TAOS_RETURN(code);
}

void tsdbLRUCacheRelease(SLRUCache* cache, LRUHandle* handle, bool eraseIfLastRef) {
    if (!taosLRUCacheRelease(cache, handle, eraseIfLastRef)) {
      tsdbTrace(" release lru cache failed");
    }
  }