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
#include "cos.h"
#include "functionMgt.h"
#include "tsdb.h"
#include "tsdbDataFileRW.h"
#include "tsdbReadUtil.h"
#include "vnd.h"

#define ROCKS_BATCH_SIZE (4096)

#if 0
static int32_t tsdbOpenBICache(STsdb *pTsdb) {
  int32_t    code = 0;
  SLRUCache *pCache = taosLRUCacheInit(10 * 1024 * 1024, 0, .5);
  if (pCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  taosLRUCacheSetStrictCapacity(pCache, false);

  taosThreadMutexInit(&pTsdb->biMutex, NULL);

_err:
  pTsdb->biCache = pCache;
  return code;
}

static void tsdbCloseBICache(STsdb *pTsdb) {
  SLRUCache *pCache = pTsdb->biCache;
  if (pCache) {
    int32_t elems = taosLRUCacheGetElems(pCache);
    tsdbTrace("vgId:%d, elems: %d", TD_VID(pTsdb->pVnode), elems);
    taosLRUCacheEraseUnrefEntries(pCache);
    elems = taosLRUCacheGetElems(pCache);
    tsdbTrace("vgId:%d, elems: %d", TD_VID(pTsdb->pVnode), elems);

    taosLRUCacheCleanup(pCache);

    taosThreadMutexDestroy(&pTsdb->biMutex);
  }
}
#endif

static int32_t tsdbOpenBCache(STsdb *pTsdb) {
  int32_t    code = 0;
  int32_t    szPage = pTsdb->pVnode->config.tsdbPageSize;
  int64_t    szBlock = tsS3BlockSize <= 1024 ? 1024 : tsS3BlockSize;
  SLRUCache *pCache = taosLRUCacheInit((int64_t)tsS3BlockCacheSize * szBlock * szPage, 0, .5);
  if (pCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  taosLRUCacheSetStrictCapacity(pCache, false);

  taosThreadMutexInit(&pTsdb->bMutex, NULL);

  pTsdb->bCache = pCache;

_err:
  return code;
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

    taosThreadMutexDestroy(&pTsdb->bMutex);
  }
}

static int32_t tsdbOpenPgCache(STsdb *pTsdb) {
  int32_t code = 0;
  // SLRUCache *pCache = taosLRUCacheInit(10 * 1024 * 1024, 0, .5);
  int32_t szPage = pTsdb->pVnode->config.tsdbPageSize;

  SLRUCache *pCache = taosLRUCacheInit((int64_t)tsS3PageCacheSize * szPage, 0, .5);
  if (pCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  taosLRUCacheSetStrictCapacity(pCache, false);

  taosThreadMutexInit(&pTsdb->pgMutex, NULL);

_err:
  pTsdb->pgCache = pCache;
  return code;
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

    taosThreadMutexDestroy(&pTsdb->bMutex);
  }
}

#define ROCKS_KEY_LEN (sizeof(tb_uid_t) + sizeof(int16_t) + sizeof(int8_t))

typedef struct {
  tb_uid_t uid;
  int16_t  cid;
  int8_t   ltype;
} SLastKey;

static void tsdbGetRocksPath(STsdb *pTsdb, char *path) {
  SVnode *pVnode = pTsdb->pVnode;
  vnodeGetPrimaryDir(pTsdb->path, pVnode->diskPrimary, pVnode->pTfs, path, TSDB_FILENAME_LEN);

  int32_t offset = strlen(path);
  snprintf(path + offset, TSDB_FILENAME_LEN - offset - 1, "%scache.rdb", TD_DIRSEP);
}

static const char *myCmpName(void *state) {
  (void)state;
  return "myCmp";
}

static void myCmpDestroy(void *state) { (void)state; }

static int myCmp(void *state, const char *a, size_t alen, const char *b, size_t blen) {
  (void)state;
  (void)alen;
  (void)blen;
  SLastKey *lhs = (SLastKey *)a;
  SLastKey *rhs = (SLastKey *)b;

  if (lhs->uid < rhs->uid) {
    return -1;
  } else if (lhs->uid > rhs->uid) {
    return 1;
  }

  if (lhs->cid < rhs->cid) {
    return -1;
  } else if (lhs->cid > rhs->cid) {
    return 1;
  }

  if (lhs->ltype < rhs->ltype) {
    return -1;
  } else if (lhs->ltype > rhs->ltype) {
    return 1;
  }

  return 0;
}

static int32_t tsdbOpenRocksCache(STsdb *pTsdb) {
  int32_t code = 0;

  rocksdb_comparator_t *cmp = rocksdb_comparator_create(NULL, myCmpDestroy, myCmp, myCmpName);
  if (NULL == cmp) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  rocksdb_cache_t *cache = rocksdb_cache_create_lru(5 * 1024 * 1024);
  pTsdb->rCache.blockcache = cache;

  rocksdb_block_based_table_options_t *tableoptions = rocksdb_block_based_options_create();
  pTsdb->rCache.tableoptions = tableoptions;

  rocksdb_options_t *options = rocksdb_options_create();
  if (NULL == options) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  rocksdb_options_set_create_if_missing(options, 1);
  rocksdb_options_set_comparator(options, cmp);
  rocksdb_block_based_options_set_block_cache(tableoptions, cache);
  rocksdb_options_set_block_based_table_factory(options, tableoptions);
  rocksdb_options_set_info_log_level(options, 2);  // WARN_LEVEL
  // rocksdb_options_set_inplace_update_support(options, 1);
  // rocksdb_options_set_allow_concurrent_memtable_write(options, 0);

  rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
  if (NULL == writeoptions) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err2;
  }
  rocksdb_writeoptions_disable_WAL(writeoptions, 1);

  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  if (NULL == readoptions) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err3;
  }

  char *err = NULL;
  char  cachePath[TSDB_FILENAME_LEN] = {0};
  tsdbGetRocksPath(pTsdb, cachePath);

  rocksdb_t *db = rocksdb_open(options, cachePath, &err);
  if (NULL == db) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, err);
    rocksdb_free(err);

    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err4;
  }

  rocksdb_flushoptions_t *flushoptions = rocksdb_flushoptions_create();
  if (NULL == flushoptions) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err5;
  }

  rocksdb_writebatch_t *writebatch = rocksdb_writebatch_create();
  rocksdb_writebatch_t *rwritebatch = rocksdb_writebatch_create();

  pTsdb->rCache.writebatch = writebatch;
  pTsdb->rCache.rwritebatch = rwritebatch;
  pTsdb->rCache.my_comparator = cmp;
  pTsdb->rCache.options = options;
  pTsdb->rCache.writeoptions = writeoptions;
  pTsdb->rCache.readoptions = readoptions;
  pTsdb->rCache.flushoptions = flushoptions;
  pTsdb->rCache.db = db;

  taosThreadMutexInit(&pTsdb->rCache.rMutex, NULL);

  pTsdb->rCache.pTSchema = NULL;

  return code;

_err5:
  rocksdb_close(pTsdb->rCache.db);
_err4:
  rocksdb_readoptions_destroy(readoptions);
_err3:
  rocksdb_writeoptions_destroy(writeoptions);
_err2:
  rocksdb_options_destroy(options);
  rocksdb_block_based_options_destroy(tableoptions);
  rocksdb_cache_destroy(cache);
_err:
  rocksdb_comparator_destroy(cmp);
  return code;
}

static void tsdbCloseRocksCache(STsdb *pTsdb) {
  rocksdb_close(pTsdb->rCache.db);
  rocksdb_flushoptions_destroy(pTsdb->rCache.flushoptions);
  rocksdb_writebatch_destroy(pTsdb->rCache.writebatch);
  rocksdb_writebatch_destroy(pTsdb->rCache.rwritebatch);
  rocksdb_readoptions_destroy(pTsdb->rCache.readoptions);
  rocksdb_writeoptions_destroy(pTsdb->rCache.writeoptions);
  rocksdb_options_destroy(pTsdb->rCache.options);
  rocksdb_block_based_options_destroy(pTsdb->rCache.tableoptions);
  rocksdb_cache_destroy(pTsdb->rCache.blockcache);
  rocksdb_comparator_destroy(pTsdb->rCache.my_comparator);
  taosThreadMutexDestroy(&pTsdb->rCache.rMutex);
  taosMemoryFree(pTsdb->rCache.pTSchema);
}

static void rocksMayWrite(STsdb *pTsdb, bool force, bool read, bool lock) {
  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  if (read) {
    if (lock) {
      taosThreadMutexLock(&pTsdb->lruMutex);
    }
    wb = pTsdb->rCache.rwritebatch;
  } else {
    if (lock) {
      taosThreadMutexLock(&pTsdb->rCache.rMutex);
    }
  }

  int count = rocksdb_writebatch_count(wb);
  if ((force && count > 0) || count >= ROCKS_BATCH_SIZE) {
    char *err = NULL;

    rocksdb_write(pTsdb->rCache.db, pTsdb->rCache.writeoptions, wb, &err);
    if (NULL != err) {
      tsdbError("vgId:%d, %s failed at line %d, count: %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, count,
                err);
      rocksdb_free(err);
      // pTsdb->flushState.flush_count = 0;
    }

    rocksdb_writebatch_clear(wb);
  }

  if (lock) {
    if (read) {
      taosThreadMutexUnlock(&pTsdb->lruMutex);
    } else {
      taosThreadMutexUnlock(&pTsdb->rCache.rMutex);
    }
  }
}

static SLastCol *tsdbCacheDeserialize(char const *value) {
  if (!value) {
    return NULL;
  }

  SLastCol *pLastCol = (SLastCol *)value;
  SColVal  *pColVal = &pLastCol->colVal;
  if (IS_VAR_DATA_TYPE(pColVal->type)) {
    if (pColVal->value.nData > 0) {
      pColVal->value.pData = (char *)value + sizeof(*pLastCol);
    } else {
      pColVal->value.pData = NULL;
    }
  }

  return pLastCol;
}

static void tsdbCacheSerialize(SLastCol *pLastCol, char **value, size_t *size) {
  SColVal *pColVal = &pLastCol->colVal;
  size_t   length = sizeof(*pLastCol);
  if (IS_VAR_DATA_TYPE(pColVal->type)) {
    length += pColVal->value.nData;
  }
  *value = taosMemoryMalloc(length);

  *(SLastCol *)(*value) = *pLastCol;
  if (IS_VAR_DATA_TYPE(pColVal->type)) {
    uint8_t *pVal = pColVal->value.pData;
    SColVal *pDColVal = &((SLastCol *)(*value))->colVal;
    pDColVal->value.pData = *value + sizeof(*pLastCol);
    if (pColVal->value.nData > 0) {
      memcpy(pDColVal->value.pData, pVal, pColVal->value.nData);
    } else {
      pDColVal->value.pData = NULL;
    }
  }
  *size = length;
}

static void tsdbCachePutBatch(SLastCol *pLastCol, const void *key, size_t klen, SCacheFlushState *state) {
  STsdb                *pTsdb = state->pTsdb;
  SRocksCache          *rCache = &pTsdb->rCache;
  rocksdb_writebatch_t *wb = rCache->writebatch;
  char                 *rocks_value = NULL;
  size_t                vlen = 0;

  tsdbCacheSerialize(pLastCol, &rocks_value, &vlen);

  taosThreadMutexLock(&rCache->rMutex);

  rocksdb_writebatch_put(wb, (char *)key, klen, rocks_value, vlen);

  taosMemoryFree(rocks_value);

  if (++state->flush_count >= ROCKS_BATCH_SIZE) {
    char *err = NULL;

    rocksdb_write(rCache->db, rCache->writeoptions, wb, &err);
    if (NULL != err) {
      tsdbError("vgId:%d, %s failed at line %d, count: %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                state->flush_count, err);
      rocksdb_free(err);
    }

    rocksdb_writebatch_clear(wb);

    state->flush_count = 0;
  }

  taosThreadMutexUnlock(&rCache->rMutex);
}

int tsdbCacheFlushDirty(const void *key, size_t klen, void *value, void *ud) {
  SLastCol *pLastCol = (SLastCol *)value;

  if (pLastCol->dirty) {
    tsdbCachePutBatch(pLastCol, key, klen, (SCacheFlushState *)ud);

    pLastCol->dirty = 0;
  }

  return 0;
}

int32_t tsdbCacheCommit(STsdb *pTsdb) {
  int32_t code = 0;
  char   *err = NULL;

  SLRUCache            *pCache = pTsdb->lruCache;
  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;

  taosThreadMutexLock(&pTsdb->lruMutex);

  taosLRUCacheApply(pCache, tsdbCacheFlushDirty, &pTsdb->flushState);

  rocksMayWrite(pTsdb, true, false, false);
  rocksMayWrite(pTsdb, true, true, false);
  rocksdb_flush(pTsdb->rCache.db, pTsdb->rCache.flushoptions, &err);

  taosThreadMutexUnlock(&pTsdb->lruMutex);

  if (NULL != err) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, err);
    rocksdb_free(err);
    code = -1;
  }

  return code;
}

static void reallocVarData(SColVal *pColVal) {
  if (IS_VAR_DATA_TYPE(pColVal->type)) {
    uint8_t *pVal = pColVal->value.pData;
    if (pColVal->value.nData > 0) {
      pColVal->value.pData = taosMemoryMalloc(pColVal->value.nData);
      memcpy(pColVal->value.pData, pVal, pColVal->value.nData);
    } else {
      pColVal->value.pData = NULL;
    }
  }
}

static void tsdbCacheDeleter(const void *key, size_t klen, void *value, void *ud) {
  SLastCol *pLastCol = (SLastCol *)value;

  if (pLastCol->dirty) {
    tsdbCachePutBatch(pLastCol, key, klen, (SCacheFlushState *)ud);
  }

  if (IS_VAR_DATA_TYPE(pLastCol->colVal.type) /* && pLastCol->colVal.value.nData > 0*/) {
    taosMemoryFree(pLastCol->colVal.value.pData);
  }

  taosMemoryFree(value);
}

static int32_t tsdbCacheNewTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type, int8_t ltype) {
  int32_t code = 0;

  SLRUCache            *pCache = pTsdb->lruCache;
  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  SLastCol              noneCol = {.ts = TSKEY_MIN, .colVal = COL_VAL_NONE(cid, col_type), .dirty = 1};
  SLastCol             *pLastCol = &noneCol;

  SLastCol *pTmpLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
  *pTmpLastCol = *pLastCol;
  pLastCol = pTmpLastCol;

  reallocVarData(&pLastCol->colVal);
  size_t charge = sizeof(*pLastCol);
  if (IS_VAR_DATA_TYPE(pLastCol->colVal.type)) {
    charge += pLastCol->colVal.value.nData;
  }

  SLastKey *pLastKey = &(SLastKey){.ltype = ltype, .uid = uid, .cid = cid};
  LRUStatus status = taosLRUCacheInsert(pCache, pLastKey, ROCKS_KEY_LEN, pLastCol, charge, tsdbCacheDeleter, NULL,
                                        TAOS_LRU_PRIORITY_LOW, &pTsdb->flushState);
  if (status != TAOS_LRU_STATUS_OK) {
    code = -1;
  }
  /*
  // store result back to rocks cache
  char  *value = NULL;
  size_t vlen = 0;
  tsdbCacheSerialize(pLastCol, &value, &vlen);

  SLastKey *key = pLastKey;
  size_t    klen = ROCKS_KEY_LEN;
  rocksdb_writebatch_put(wb, (char *)key, klen, value, vlen);
  taosMemoryFree(value);
  */
  return code;
}

int32_t tsdbCacheCommitNoLock(STsdb *pTsdb) {
  int32_t code = 0;
  char   *err = NULL;

  SLRUCache            *pCache = pTsdb->lruCache;
  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;

  taosLRUCacheApply(pCache, tsdbCacheFlushDirty, &pTsdb->flushState);

  rocksMayWrite(pTsdb, true, false, false);
  rocksMayWrite(pTsdb, true, true, false);
  rocksdb_flush(pTsdb->rCache.db, pTsdb->rCache.flushoptions, &err);

  if (NULL != err) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, err);
    rocksdb_free(err);
    code = -1;
  }

  return code;
}

static int32_t tsdbCacheDropTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type, int8_t ltype) {
  int32_t code = 0;

  // build keys & multi get from rocks
  char       **keys_list = taosMemoryCalloc(2, sizeof(char *));
  size_t      *keys_list_sizes = taosMemoryCalloc(2, sizeof(size_t));
  const size_t klen = ROCKS_KEY_LEN;

  char *keys = taosMemoryCalloc(2, sizeof(SLastKey));
  ((SLastKey *)keys)[0] = (SLastKey){.ltype = 1, .uid = uid, .cid = cid};
  ((SLastKey *)keys)[1] = (SLastKey){.ltype = 0, .uid = uid, .cid = cid};

  keys_list[0] = keys;
  keys_list[1] = keys + sizeof(SLastKey);
  keys_list_sizes[0] = klen;
  keys_list_sizes[1] = klen;

  char  **values_list = taosMemoryCalloc(2, sizeof(char *));
  size_t *values_list_sizes = taosMemoryCalloc(2, sizeof(size_t));
  char  **errs = taosMemoryCalloc(2, sizeof(char *));

  // rocksMayWrite(pTsdb, true, false, false);
  rocksdb_multi_get(pTsdb->rCache.db, pTsdb->rCache.readoptions, 2, (const char *const *)keys_list, keys_list_sizes,
                    values_list, values_list_sizes, errs);

  for (int i = 0; i < 2; ++i) {
    if (errs[i]) {
      rocksdb_free(errs[i]);
    }
  }
  taosMemoryFree(errs);

  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  {
    SLastCol *pLastCol = tsdbCacheDeserialize(values_list[0]);
    if (NULL != pLastCol) {
      rocksdb_writebatch_delete(wb, keys_list[0], klen);
    }
    pLastCol = tsdbCacheDeserialize(values_list[1]);
    if (NULL != pLastCol) {
      rocksdb_writebatch_delete(wb, keys_list[1], klen);
    }

    rocksdb_free(values_list[0]);
    rocksdb_free(values_list[1]);

    bool       erase = false;
    LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, keys_list[0], klen);
    if (h) {
      SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pTsdb->lruCache, h);
      erase = true;

      taosLRUCacheRelease(pTsdb->lruCache, h, erase);
    }
    if (erase) {
      taosLRUCacheErase(pTsdb->lruCache, keys_list[0], klen);
    }

    erase = false;
    h = taosLRUCacheLookup(pTsdb->lruCache, keys_list[1], klen);
    if (h) {
      SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pTsdb->lruCache, h);
      erase = true;

      taosLRUCacheRelease(pTsdb->lruCache, h, erase);
    }
    if (erase) {
      taosLRUCacheErase(pTsdb->lruCache, keys_list[1], klen);
    }
  }

  taosMemoryFree(keys_list[0]);

  taosMemoryFree(keys_list);
  taosMemoryFree(keys_list_sizes);
  taosMemoryFree(values_list);
  taosMemoryFree(values_list_sizes);

  return code;
}

int32_t tsdbCacheNewTable(STsdb *pTsdb, tb_uid_t uid, tb_uid_t suid, SSchemaWrapper *pSchemaRow) {
  int32_t code = 0;

  taosThreadMutexLock(&pTsdb->lruMutex);

  if (suid < 0) {
    int nCols = pSchemaRow->nCols;
    for (int i = 0; i < nCols; ++i) {
      int16_t cid = pSchemaRow->pSchema[i].colId;
      int8_t  col_type = pSchemaRow->pSchema[i].type;

      (void)tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 0);
      (void)tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 1);
    }
  } else {
    STSchema *pTSchema = NULL;
    code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, -1, &pTSchema);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return -1;
    }

    int nCols = pTSchema->numOfCols;
    for (int i = 0; i < nCols; ++i) {
      int16_t cid = pTSchema->columns[i].colId;
      int8_t  col_type = pTSchema->columns[i].type;

      (void)tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 0);
      (void)tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 1);
    }

    taosMemoryFree(pTSchema);
  }

  taosThreadMutexUnlock(&pTsdb->lruMutex);

  return code;
}

int32_t tsdbCacheDropTable(STsdb *pTsdb, tb_uid_t uid, tb_uid_t suid, SSchemaWrapper *pSchemaRow) {
  int32_t code = 0;

  taosThreadMutexLock(&pTsdb->lruMutex);

  (void)tsdbCacheCommitNoLock(pTsdb);

  if (suid < 0) {
    int nCols = pSchemaRow->nCols;
    for (int i = 0; i < nCols; ++i) {
      int16_t cid = pSchemaRow->pSchema[i].colId;
      int8_t  col_type = pSchemaRow->pSchema[i].type;

      (void)tsdbCacheDropTableColumn(pTsdb, uid, cid, col_type, 0);
      (void)tsdbCacheDropTableColumn(pTsdb, uid, cid, col_type, 1);
    }
  } else {
    STSchema *pTSchema = NULL;
    code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, -1, &pTSchema);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return -1;
    }

    int nCols = pTSchema->numOfCols;
    for (int i = 0; i < nCols; ++i) {
      int16_t cid = pTSchema->columns[i].colId;
      int8_t  col_type = pTSchema->columns[i].type;

      (void)tsdbCacheDropTableColumn(pTsdb, uid, cid, col_type, 0);
      (void)tsdbCacheDropTableColumn(pTsdb, uid, cid, col_type, 1);
    }

    taosMemoryFree(pTSchema);
  }

  rocksMayWrite(pTsdb, true, false, false);

  taosThreadMutexUnlock(&pTsdb->lruMutex);

  return code;
}

int32_t tsdbCacheDropSubTables(STsdb *pTsdb, SArray *uids, tb_uid_t suid) {
  int32_t code = 0;

  taosThreadMutexLock(&pTsdb->lruMutex);

  (void)tsdbCacheCommitNoLock(pTsdb);

  STSchema *pTSchema = NULL;
  code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, suid, -1, &pTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return -1;
  }
  for (int i = 0; i < TARRAY_SIZE(uids); ++i) {
    int64_t uid = ((tb_uid_t *)TARRAY_DATA(uids))[i];

    int nCols = pTSchema->numOfCols;
    for (int i = 0; i < nCols; ++i) {
      int16_t cid = pTSchema->columns[i].colId;
      int8_t  col_type = pTSchema->columns[i].type;

      (void)tsdbCacheDropTableColumn(pTsdb, uid, cid, col_type, 0);
      (void)tsdbCacheDropTableColumn(pTsdb, uid, cid, col_type, 1);
    }
  }

  taosMemoryFree(pTSchema);

  rocksMayWrite(pTsdb, true, false, false);

  taosThreadMutexUnlock(&pTsdb->lruMutex);

  return code;
}

int32_t tsdbCacheNewNTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type) {
  int32_t code = 0;

  taosThreadMutexLock(&pTsdb->lruMutex);

  (void)tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 0);
  (void)tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 1);

  // rocksMayWrite(pTsdb, true, false, false);
  taosThreadMutexUnlock(&pTsdb->lruMutex);
  //(void)tsdbCacheCommit(pTsdb);

  return code;
}

int32_t tsdbCacheDropNTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type) {
  int32_t code = 0;

  taosThreadMutexLock(&pTsdb->lruMutex);

  (void)tsdbCacheCommitNoLock(pTsdb);

  (void)tsdbCacheDropTableColumn(pTsdb, uid, cid, col_type, 0);
  (void)tsdbCacheDropTableColumn(pTsdb, uid, cid, col_type, 1);

  rocksMayWrite(pTsdb, true, false, true);

  taosThreadMutexUnlock(&pTsdb->lruMutex);

  return code;
}

int32_t tsdbCacheNewSTableColumn(STsdb *pTsdb, SArray *uids, int16_t cid, int8_t col_type) {
  int32_t code = 0;

  taosThreadMutexLock(&pTsdb->lruMutex);

  for (int i = 0; i < TARRAY_SIZE(uids); ++i) {
    tb_uid_t uid = ((tb_uid_t *)TARRAY_DATA(uids))[i];

    (void)tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 0);
    (void)tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 1);
  }

  // rocksMayWrite(pTsdb, true, false, false);
  taosThreadMutexUnlock(&pTsdb->lruMutex);
  //(void)tsdbCacheCommit(pTsdb);

  return code;
}

int32_t tsdbCacheDropSTableColumn(STsdb *pTsdb, SArray *uids, int16_t cid, int8_t col_type) {
  int32_t code = 0;

  taosThreadMutexLock(&pTsdb->lruMutex);

  (void)tsdbCacheCommitNoLock(pTsdb);

  for (int i = 0; i < TARRAY_SIZE(uids); ++i) {
    int64_t uid = ((tb_uid_t *)TARRAY_DATA(uids))[i];

    (void)tsdbCacheDropTableColumn(pTsdb, uid, cid, col_type, 0);
    (void)tsdbCacheDropTableColumn(pTsdb, uid, cid, col_type, 1);
  }

  rocksMayWrite(pTsdb, true, false, true);

  taosThreadMutexUnlock(&pTsdb->lruMutex);

  return code;
}

static SLastCol *tsdbCacheLookup(STsdb *pTsdb, tb_uid_t uid, int16_t cid, int8_t ltype) {
  SLastCol *pLastCol = NULL;

  char     *err = NULL;
  size_t    vlen = 0;
  SLastKey *key = &(SLastKey){.ltype = ltype, .uid = uid, .cid = cid};
  size_t    klen = ROCKS_KEY_LEN;
  char     *value = NULL;
  value = rocksdb_get(pTsdb->rCache.db, pTsdb->rCache.readoptions, (char *)key, klen, &vlen, &err);
  if (NULL != err) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, err);
    rocksdb_free(err);
  }

  pLastCol = tsdbCacheDeserialize(value);

  return pLastCol;
}

typedef struct {
  int      idx;
  SLastKey key;
} SIdxKey;

int32_t tsdbCacheUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, TSDBROW *pRow) {
  int32_t code = 0;

  // 1, fetch schema
  STSchema *pTSchema = NULL;
  int32_t   sver = TSDBROW_SVERSION(pRow);

  code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, sver, &pTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return -1;
  }

  // 2, iterate col values into array
  SArray *aColVal = taosArrayInit(32, sizeof(SColVal));

  STSDBRowIter iter = {0};
  tsdbRowIterOpen(&iter, pRow, pTSchema);

  for (SColVal *pColVal = tsdbRowIterNext(&iter); pColVal; pColVal = tsdbRowIterNext(&iter)) {
    taosArrayPush(aColVal, pColVal);
  }

  tsdbRowClose(&iter);

  // 3, build keys & multi get from rocks
  int        num_keys = TARRAY_SIZE(aColVal);
  TSKEY      keyTs = TSDBROW_TS(pRow);
  SArray    *remainCols = NULL;
  SLRUCache *pCache = pTsdb->lruCache;

  taosThreadMutexLock(&pTsdb->lruMutex);
  for (int i = 0; i < num_keys; ++i) {
    SColVal *pColVal = (SColVal *)taosArrayGet(aColVal, i);
    int16_t  cid = pColVal->cid;

    SLastKey  *key = &(SLastKey){.ltype = 0, .uid = uid, .cid = cid};
    size_t     klen = ROCKS_KEY_LEN;
    LRUHandle *h = taosLRUCacheLookup(pCache, key, klen);
    if (h) {
      SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pCache, h);

      if (pLastCol->ts <= keyTs) {
        uint8_t *pVal = NULL;
        int      nData = pLastCol->colVal.value.nData;
        if (IS_VAR_DATA_TYPE(pColVal->type)) {
          pVal = pLastCol->colVal.value.pData;
        }
        pLastCol->ts = keyTs;
        pLastCol->colVal = *pColVal;
        if (IS_VAR_DATA_TYPE(pColVal->type)) {
          if (nData < pColVal->value.nData) {
            taosMemoryFree(pVal);
            pLastCol->colVal.value.pData = taosMemoryCalloc(1, pColVal->value.nData);
          } else {
            pLastCol->colVal.value.pData = pVal;
          }
          if (pColVal->value.nData) {
            memcpy(pLastCol->colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
          }
        }

        if (!pLastCol->dirty) {
          pLastCol->dirty = 1;
        }
      }

      taosLRUCacheRelease(pCache, h, false);
    } else {
      if (!remainCols) {
        remainCols = taosArrayInit(num_keys * 2, sizeof(SIdxKey));
      }
      taosArrayPush(remainCols, &(SIdxKey){i, *key});
    }

    if (COL_VAL_IS_VALUE(pColVal)) {
      key->ltype = 1;
      LRUHandle *h = taosLRUCacheLookup(pCache, key, klen);
      if (h) {
        SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pCache, h);

        if (pLastCol->ts <= keyTs) {
          uint8_t *pVal = NULL;
          int      nData = pLastCol->colVal.value.nData;
          if (IS_VAR_DATA_TYPE(pColVal->type)) {
            pVal = pLastCol->colVal.value.pData;
          }
          pLastCol->ts = keyTs;
          pLastCol->colVal = *pColVal;
          if (IS_VAR_DATA_TYPE(pColVal->type)) {
            if (nData < pColVal->value.nData) {
              taosMemoryFree(pVal);
              pLastCol->colVal.value.pData = taosMemoryCalloc(1, pColVal->value.nData);
            } else {
              pLastCol->colVal.value.pData = pVal;
            }
            if (pColVal->value.nData) {
              memcpy(pLastCol->colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
            }
          }

          if (!pLastCol->dirty) {
            pLastCol->dirty = 1;
          }
        }

        taosLRUCacheRelease(pCache, h, false);
      } else {
        if (!remainCols) {
          remainCols = taosArrayInit(num_keys * 2, sizeof(SIdxKey));
        }
        taosArrayPush(remainCols, &(SIdxKey){i, *key});
      }
    }
  }

  if (remainCols) {
    num_keys = TARRAY_SIZE(remainCols);
  }
  if (remainCols && num_keys > 0) {
    char  **keys_list = taosMemoryCalloc(num_keys, sizeof(char *));
    size_t *keys_list_sizes = taosMemoryCalloc(num_keys, sizeof(size_t));
    for (int i = 0; i < num_keys; ++i) {
      SIdxKey *idxKey = &((SIdxKey *)TARRAY_DATA(remainCols))[i];

      keys_list[i] = (char *)&idxKey->key;
      keys_list_sizes[i] = ROCKS_KEY_LEN;
    }
    char  **values_list = taosMemoryCalloc(num_keys, sizeof(char *));
    size_t *values_list_sizes = taosMemoryCalloc(num_keys, sizeof(size_t));
    char  **errs = taosMemoryCalloc(num_keys, sizeof(char *));
    rocksdb_multi_get(pTsdb->rCache.db, pTsdb->rCache.readoptions, num_keys, (const char *const *)keys_list,
                      keys_list_sizes, values_list, values_list_sizes, errs);
    for (int i = 0; i < num_keys; ++i) {
      rocksdb_free(errs[i]);
    }
    taosMemoryFree(errs);
    taosMemoryFree(keys_list);
    taosMemoryFree(keys_list_sizes);
    taosMemoryFree(values_list_sizes);

    rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
    for (int i = 0; i < num_keys; ++i) {
      SIdxKey *idxKey = &((SIdxKey *)TARRAY_DATA(remainCols))[i];
      SColVal *pColVal = (SColVal *)TARRAY_DATA(aColVal) + idxKey->idx;
      // SColVal *pColVal = (SColVal *)taosArrayGet(aColVal, idxKey->idx);

      SLastCol *pLastCol = tsdbCacheDeserialize(values_list[i]);

      if (idxKey->key.ltype == 0) {
        if (NULL == pLastCol || pLastCol->ts <= keyTs) {
          char  *value = NULL;
          size_t vlen = 0;
          tsdbCacheSerialize(&(SLastCol){.ts = keyTs, .colVal = *pColVal}, &value, &vlen);
          // SLastKey key = (SLastKey){.ltype = 0, .uid = uid, .cid = pColVal->cid};
          taosThreadMutexLock(&pTsdb->rCache.rMutex);

          rocksdb_writebatch_put(wb, (char *)&idxKey->key, ROCKS_KEY_LEN, value, vlen);

          taosThreadMutexUnlock(&pTsdb->rCache.rMutex);

          pLastCol = (SLastCol *)value;
          SLastCol *pTmpLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
          *pTmpLastCol = *pLastCol;
          pLastCol = pTmpLastCol;

          reallocVarData(&pLastCol->colVal);
          size_t charge = sizeof(*pLastCol);
          if (IS_VAR_DATA_TYPE(pLastCol->colVal.type)) {
            charge += pLastCol->colVal.value.nData;
          }

          LRUStatus status = taosLRUCacheInsert(pTsdb->lruCache, &idxKey->key, ROCKS_KEY_LEN, pLastCol, charge,
                                                tsdbCacheDeleter, NULL, TAOS_LRU_PRIORITY_LOW, &pTsdb->flushState);
          if (status != TAOS_LRU_STATUS_OK) {
            code = -1;
          }

          taosMemoryFree(value);
        }
      } else {
        if (COL_VAL_IS_VALUE(pColVal)) {
          if (NULL == pLastCol || pLastCol->ts <= keyTs) {
            char  *value = NULL;
            size_t vlen = 0;
            tsdbCacheSerialize(&(SLastCol){.ts = keyTs, .colVal = *pColVal}, &value, &vlen);
            // SLastKey key = (SLastKey){.ltype = 1, .uid = uid, .cid = pColVal->cid};
            taosThreadMutexLock(&pTsdb->rCache.rMutex);

            rocksdb_writebatch_put(wb, (char *)&idxKey->key, ROCKS_KEY_LEN, value, vlen);

            taosThreadMutexUnlock(&pTsdb->rCache.rMutex);

            pLastCol = (SLastCol *)value;
            SLastCol *pTmpLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
            *pTmpLastCol = *pLastCol;
            pLastCol = pTmpLastCol;

            reallocVarData(&pLastCol->colVal);
            size_t charge = sizeof(*pLastCol);
            if (IS_VAR_DATA_TYPE(pLastCol->colVal.type)) {
              charge += pLastCol->colVal.value.nData;
            }

            LRUStatus status = taosLRUCacheInsert(pTsdb->lruCache, &idxKey->key, ROCKS_KEY_LEN, pLastCol, charge,
                                                  tsdbCacheDeleter, NULL, TAOS_LRU_PRIORITY_LOW, &pTsdb->flushState);
            if (status != TAOS_LRU_STATUS_OK) {
              code = -1;
            }

            taosMemoryFree(value);
          }
        }
      }

      rocksdb_free(values_list[i]);
    }

    rocksMayWrite(pTsdb, true, false, true);

    taosMemoryFree(values_list);

    taosArrayDestroy(remainCols);
  }

  taosThreadMutexUnlock(&pTsdb->lruMutex);

_exit:
  taosArrayDestroy(aColVal);
  taosMemoryFree(pTSchema);
  return code;
}

static int32_t mergeLastCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                            int nCols, int16_t *slotIds);

static int32_t mergeLastRowCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                               int nCols, int16_t *slotIds);
#ifdef BUILD_NO_CALL
int32_t tsdbCacheGetSlow(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SCacheRowsReader *pr, int8_t ltype) {
  rocksdb_writebatch_t *wb = NULL;
  int32_t               code = 0;

  SArray *pCidList = pr->pCidList;
  int     num_keys = TARRAY_SIZE(pCidList);

  char  **keys_list = taosMemoryMalloc(num_keys * sizeof(char *));
  size_t *keys_list_sizes = taosMemoryMalloc(num_keys * sizeof(size_t));
  char   *key_list = taosMemoryMalloc(num_keys * ROCKS_KEY_LEN);
  for (int i = 0; i < num_keys; ++i) {
    int16_t cid = *(int16_t *)taosArrayGet(pCidList, i);

    memcpy(key_list + i * ROCKS_KEY_LEN, &(SLastKey){.ltype = ltype, .uid = uid, .cid = cid}, ROCKS_KEY_LEN);
    keys_list[i] = key_list + i * ROCKS_KEY_LEN;
    keys_list_sizes[i] = ROCKS_KEY_LEN;
  }

  char  **values_list = taosMemoryCalloc(num_keys, sizeof(char *));
  size_t *values_list_sizes = taosMemoryCalloc(num_keys, sizeof(size_t));
  char  **errs = taosMemoryMalloc(num_keys * sizeof(char *));
  rocksdb_multi_get(pTsdb->rCache.db, pTsdb->rCache.readoptions, num_keys, (const char *const *)keys_list,
                    keys_list_sizes, values_list, values_list_sizes, errs);
  for (int i = 0; i < num_keys; ++i) {
    if (errs[i]) {
      rocksdb_free(errs[i]);
    }
  }
  taosMemoryFree(key_list);
  taosMemoryFree(keys_list);
  taosMemoryFree(keys_list_sizes);
  taosMemoryFree(errs);

  for (int i = 0; i < num_keys; ++i) {
    bool      freeCol = true;
    SArray   *pTmpColArray = NULL;
    SLastCol *pLastCol = tsdbCacheDeserialize(values_list[i]);
    int16_t   cid = *(int16_t *)taosArrayGet(pCidList, i);
    SLastCol  noneCol = {.ts = TSKEY_MIN, .colVal = COL_VAL_NONE(cid, pr->pSchema->columns[pr->pSlotIds[i]].type)};
    if (pLastCol) {
      reallocVarData(&pLastCol->colVal);
    } else {
      taosThreadMutexLock(&pTsdb->rCache.rMutex);

      pLastCol = tsdbCacheLookup(pTsdb, uid, cid, ltype);
      if (!pLastCol) {
        // recalc: load from tsdb
        int16_t aCols[1] = {cid};
        int16_t slotIds[1] = {pr->pSlotIds[i]};
        pTmpColArray = NULL;

        if (ltype) {
          mergeLastCid(uid, pTsdb, &pTmpColArray, pr, aCols, 1, slotIds);
        } else {
          mergeLastRowCid(uid, pTsdb, &pTmpColArray, pr, aCols, 1, slotIds);
        }

        if (pTmpColArray && TARRAY_SIZE(pTmpColArray) >= 1) {
          pLastCol = taosArrayGet(pTmpColArray, 0);
          freeCol = false;
        }

        // still null, then make up a none col value
        if (!pLastCol) {
          pLastCol = &noneCol;
          freeCol = false;
        }

        // store result back to rocks cache
        wb = pTsdb->rCache.rwritebatch;
        char  *value = NULL;
        size_t vlen = 0;
        tsdbCacheSerialize(pLastCol, &value, &vlen);

        SLastKey *key = &(SLastKey){.ltype = ltype, .uid = uid, .cid = pLastCol->colVal.cid};
        size_t    klen = ROCKS_KEY_LEN;
        rocksdb_writebatch_put(wb, (char *)key, klen, value, vlen);

        taosMemoryFree(value);
      } else {
        reallocVarData(&pLastCol->colVal);
      }

      if (wb) {
        rocksMayWrite(pTsdb, false, true, false);
      }

      taosThreadMutexUnlock(&pTsdb->rCache.rMutex);
    }

    taosArrayPush(pLastArray, pLastCol);
    taosArrayDestroy(pTmpColArray);
    if (freeCol) {
      taosMemoryFree(pLastCol);
    }
  }
  taosMemoryFree(values_list);
  taosMemoryFree(values_list_sizes);

  return code;
}

static SLastCol *tsdbCacheLoadCol(STsdb *pTsdb, SCacheRowsReader *pr, int16_t slotid, tb_uid_t uid, int16_t cid,
                                  int8_t ltype) {
  SLastCol *pLastCol = tsdbCacheLookup(pTsdb, uid, cid, ltype);
  if (!pLastCol) {
    rocksdb_writebatch_t *wb = NULL;

    taosThreadMutexLock(&pTsdb->rCache.rMutex);
    pLastCol = tsdbCacheLookup(pTsdb, uid, cid, ltype);
    if (!pLastCol) {
      // recalc: load from tsdb
      int16_t aCols[1] = {cid};
      int16_t slotIds[1] = {slotid};
      SArray *pTmpColArray = NULL;

      if (ltype) {
        mergeLastCid(uid, pTsdb, &pTmpColArray, pr, aCols, 1, slotIds);
      } else {
        mergeLastRowCid(uid, pTsdb, &pTmpColArray, pr, aCols, 1, slotIds);
      }

      if (pTmpColArray && TARRAY_SIZE(pTmpColArray) >= 1) {
        pLastCol = taosArrayGet(pTmpColArray, 0);
      }

      // still null, then make up a none col value
      SLastCol noneCol = {.ts = TSKEY_MIN, .colVal = COL_VAL_NONE(cid, pr->pSchema->columns[slotid].type)};
      if (!pLastCol) {
        pLastCol = &noneCol;
      }

      // store result back to rocks cache
      wb = pTsdb->rCache.rwritebatch;
      char  *value = NULL;
      size_t vlen = 0;
      tsdbCacheSerialize(pLastCol, &value, &vlen);

      SLastKey *key = &(SLastKey){.ltype = ltype, .uid = uid, .cid = pLastCol->colVal.cid};
      size_t    klen = ROCKS_KEY_LEN;
      rocksdb_writebatch_put(wb, (char *)key, klen, value, vlen);
      taosMemoryFree(value);

      SLastCol *pTmpLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
      *pTmpLastCol = *pLastCol;
      pLastCol = pTmpLastCol;

      taosArrayDestroy(pTmpColArray);
    }

    if (wb) {
      rocksMayWrite(pTsdb, false, true, false);
    }

    taosThreadMutexUnlock(&pTsdb->rCache.rMutex);
  }

  return pLastCol;
}
#endif

static int32_t tsdbCacheLoadFromRaw(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SArray *remainCols,
                                    SCacheRowsReader *pr, int8_t ltype) {
  int32_t               code = 0;
  rocksdb_writebatch_t *wb = NULL;
  SArray               *pTmpColArray = NULL;

  SIdxKey *idxKey = taosArrayGet(remainCols, 0);
  if (idxKey->key.cid != PRIMARYKEY_TIMESTAMP_COL_ID) {
    SLastKey *key = &(SLastKey){.ltype = ltype, .uid = uid, .cid = PRIMARYKEY_TIMESTAMP_COL_ID};

    taosArrayInsert(remainCols, 0, &(SIdxKey){0, *key});
  }

  int      num_keys = TARRAY_SIZE(remainCols);
  int16_t *slotIds = taosMemoryMalloc(num_keys * sizeof(int16_t));

  int16_t *lastColIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  int16_t *lastSlotIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  int16_t *lastrowColIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  int16_t *lastrowSlotIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  SArray* lastTmpColArray = NULL;
  SArray* lastTmpIndexArray = NULL;
  SArray* lastrowTmpColArray = NULL;
  SArray* lastrowTmpIndexArray = NULL;

  int lastIndex = 0;
  int lastrowIndex = 0;

  for (int i = 0; i < num_keys; ++i) {
    SIdxKey *idxKey = taosArrayGet(remainCols, i);
    slotIds[i] = pr->pSlotIds[idxKey->idx];
    if (idxKey->key.ltype == CACHESCAN_RETRIEVE_LAST >> 3) {
      if(NULL == lastTmpIndexArray) {
        lastTmpIndexArray = taosArrayInit(num_keys, sizeof(int32_t));
      }
      taosArrayPush(lastTmpIndexArray, &(i));
      lastColIds[lastIndex] = idxKey->key.cid;
      lastSlotIds[lastIndex] = pr->pSlotIds[idxKey->idx];
      lastIndex++;
    } else {
      if(NULL == lastrowTmpIndexArray) {
        lastrowTmpIndexArray = taosArrayInit(num_keys, sizeof(int32_t));
      }
      taosArrayPush(lastrowTmpIndexArray, &(i));
      lastrowColIds[lastrowIndex] = idxKey->key.cid;
      lastrowSlotIds[lastrowIndex] = pr->pSlotIds[idxKey->idx];
      lastrowIndex++;
    }
  }

  pTmpColArray = taosArrayInit(lastIndex + lastrowIndex, sizeof(SLastCol));

  if(lastTmpIndexArray != NULL) {
    mergeLastCid(uid, pTsdb, &lastTmpColArray, pr, lastColIds, lastIndex, lastSlotIds);
    for(int i = 0; i < taosArrayGetSize(lastTmpColArray); i++) {
      taosArrayInsert(pTmpColArray, *(int32_t*)taosArrayGet(lastTmpIndexArray, i), taosArrayGet(lastTmpColArray, i));
    }
  }

  if(lastrowTmpIndexArray != NULL) {
    mergeLastRowCid(uid, pTsdb, &lastrowTmpColArray, pr, lastrowColIds, lastrowIndex, lastrowSlotIds);
    for(int i = 0; i < taosArrayGetSize(lastrowTmpColArray); i++) {
      taosArrayInsert(pTmpColArray, *(int32_t*)taosArrayGet(lastrowTmpIndexArray, i), taosArrayGet(lastrowTmpColArray, i));
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
    SLastCol noneCol = {.ts = TSKEY_MIN,
                        .colVal = COL_VAL_NONE(idxKey->key.cid, pr->pSchema->columns[slotIds[i]].type)};
    if (!pLastCol) {
      pLastCol = &noneCol;
      reallocVarData(&pLastCol->colVal);
    }

    taosArraySet(pLastArray, idxKey->idx, pLastCol);
    // taosArrayRemove(remainCols, i);

    if (!pTmpColArray) {
      continue;
    }

    SLastCol *pTmpLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
    *pTmpLastCol = *pLastCol;
    pLastCol = pTmpLastCol;

    reallocVarData(&pLastCol->colVal);
    size_t charge = sizeof(*pLastCol);
    if (IS_VAR_DATA_TYPE(pLastCol->colVal.type)) {
      charge += pLastCol->colVal.value.nData;
    }

    LRUStatus status = taosLRUCacheInsert(pCache, &idxKey->key, ROCKS_KEY_LEN, pLastCol, charge, tsdbCacheDeleter, NULL,
                                          TAOS_LRU_PRIORITY_LOW, &pTsdb->flushState);
    if (status != TAOS_LRU_STATUS_OK) {
      code = -1;
    }

    // store result back to rocks cache
    wb = pTsdb->rCache.rwritebatch;
    char  *value = NULL;
    size_t vlen = 0;
    tsdbCacheSerialize(pLastCol, &value, &vlen);

    SLastKey *key = &idxKey->key;
    size_t    klen = ROCKS_KEY_LEN;
    rocksdb_writebatch_put(wb, (char *)key, klen, value, vlen);
    taosMemoryFree(value);
  }

  if (wb) {
    rocksMayWrite(pTsdb, false, true, false);
  }

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

  return code;
}

static int32_t tsdbCacheLoadFromRocks(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SArray *remainCols,
                                      SCacheRowsReader *pr, int8_t ltype) {
  int32_t code = 0;
  int     num_keys = TARRAY_SIZE(remainCols);
  char  **keys_list = taosMemoryMalloc(num_keys * sizeof(char *));
  size_t *keys_list_sizes = taosMemoryMalloc(num_keys * sizeof(size_t));
  char   *key_list = taosMemoryMalloc(num_keys * ROCKS_KEY_LEN);
  for (int i = 0; i < num_keys; ++i) {
    int16_t cid = *(int16_t *)taosArrayGet(remainCols, i);

    memcpy(key_list + i * ROCKS_KEY_LEN, &((SIdxKey *)taosArrayGet(remainCols, i))->key, ROCKS_KEY_LEN);
    keys_list[i] = key_list + i * ROCKS_KEY_LEN;
    keys_list_sizes[i] = ROCKS_KEY_LEN;
  }

  char  **values_list = taosMemoryCalloc(num_keys, sizeof(char *));
  size_t *values_list_sizes = taosMemoryCalloc(num_keys, sizeof(size_t));
  char  **errs = taosMemoryMalloc(num_keys * sizeof(char *));
  rocksdb_multi_get(pTsdb->rCache.db, pTsdb->rCache.readoptions, num_keys, (const char *const *)keys_list,
                    keys_list_sizes, values_list, values_list_sizes, errs);
  for (int i = 0; i < num_keys; ++i) {
    if (errs[i]) {
      rocksdb_free(errs[i]);
    }
  }
  taosMemoryFree(key_list);
  taosMemoryFree(keys_list);
  taosMemoryFree(keys_list_sizes);
  taosMemoryFree(errs);

  SLRUCache *pCache = pTsdb->lruCache;
  for (int i = 0, j = 0; i < num_keys && j < TARRAY_SIZE(remainCols); ++i) {
    SLastCol *pLastCol = tsdbCacheDeserialize(values_list[i]);
    SIdxKey  *idxKey = &((SIdxKey *)TARRAY_DATA(remainCols))[j];
    if (pLastCol) {
      SLastCol *pTmpLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
      *pTmpLastCol = *pLastCol;
      pLastCol = pTmpLastCol;

      reallocVarData(&pLastCol->colVal);
      size_t charge = sizeof(*pLastCol);
      if (IS_VAR_DATA_TYPE(pLastCol->colVal.type)) {
        charge += pLastCol->colVal.value.nData;
      }

      LRUStatus status = taosLRUCacheInsert(pCache, &idxKey->key, ROCKS_KEY_LEN, pLastCol, charge, tsdbCacheDeleter,
                                            NULL, TAOS_LRU_PRIORITY_LOW, &pTsdb->flushState);
      if (status != TAOS_LRU_STATUS_OK) {
        code = -1;
      }

      SLastCol lastCol = *pLastCol;
      reallocVarData(&lastCol.colVal);
      taosArraySet(pLastArray, idxKey->idx, &lastCol);
      taosArrayRemove(remainCols, j);

      taosMemoryFree(values_list[i]);
    } else {
      ++j;
    }
  }

  taosMemoryFree(values_list);
  taosMemoryFree(values_list_sizes);

  if (TARRAY_SIZE(remainCols) > 0) {
    // tsdbTrace("tsdb/cache: vgId: %d, load %" PRId64 " from raw", TD_VID(pTsdb->pVnode), uid);
    code = tsdbCacheLoadFromRaw(pTsdb, uid, pLastArray, remainCols, pr, ltype);
  }

  return code;
}

int32_t tsdbCacheGetBatch(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SCacheRowsReader *pr, int8_t ltype) {
  int32_t    code = 0;
  SArray    *remainCols = NULL;
  SLRUCache *pCache = pTsdb->lruCache;
  SArray    *pCidList = pr->pCidList;
  int        num_keys = TARRAY_SIZE(pCidList);

  for (int i = 0; i < num_keys; ++i) {
    int16_t cid = ((int16_t *)TARRAY_DATA(pCidList))[i];

    SLastKey *key = &(SLastKey){.ltype = ltype, .uid = uid, .cid = cid};
    // for select last_row, last case
    int32_t funcType = FUNCTION_TYPE_CACHE_LAST;
    if (pr->pFuncTypeList != NULL && taosArrayGetSize(pr->pFuncTypeList) > i) {
      funcType = ((int32_t *)TARRAY_DATA(pr->pFuncTypeList))[i];
    }
    if (((pr->type & CACHESCAN_RETRIEVE_LAST) == CACHESCAN_RETRIEVE_LAST) && FUNCTION_TYPE_CACHE_LAST_ROW == funcType) {
      int8_t tempType = CACHESCAN_RETRIEVE_LAST_ROW | (pr->type ^ CACHESCAN_RETRIEVE_LAST);
      key->ltype = (tempType & CACHESCAN_RETRIEVE_LAST) >> 3;
    }

    LRUHandle *h = taosLRUCacheLookup(pCache, key, ROCKS_KEY_LEN);
    if (h) {
      SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pCache, h);

      SLastCol lastCol = *pLastCol;
      reallocVarData(&lastCol.colVal);
      taosArrayPush(pLastArray, &lastCol);

      taosLRUCacheRelease(pCache, h, false);
    } else {
      SLastCol noneCol = {.ts = TSKEY_MIN, .colVal = COL_VAL_NONE(cid, pr->pSchema->columns[pr->pSlotIds[i]].type)};

      taosArrayPush(pLastArray, &noneCol);

      if (!remainCols) {
        remainCols = taosArrayInit(num_keys, sizeof(SIdxKey));
      }
      taosArrayPush(remainCols, &(SIdxKey){i, *key});
    }
  }

  if (remainCols && TARRAY_SIZE(remainCols) > 0) {
    taosThreadMutexLock(&pTsdb->lruMutex);
    for (int i = 0; i < TARRAY_SIZE(remainCols);) {
      SIdxKey   *idxKey = &((SIdxKey *)TARRAY_DATA(remainCols))[i];
      LRUHandle *h = taosLRUCacheLookup(pCache, &idxKey->key, ROCKS_KEY_LEN);
      if (h) {
        SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pCache, h);

        SLastCol lastCol = *pLastCol;
        reallocVarData(&lastCol.colVal);
        taosArraySet(pLastArray, idxKey->idx, &lastCol);

        taosLRUCacheRelease(pCache, h, false);

        taosArrayRemove(remainCols, i);
      } else {
        ++i;
      }
    }

    // tsdbTrace("tsdb/cache: vgId: %d, load %" PRId64 " from rocks", TD_VID(pTsdb->pVnode), uid);
    code = tsdbCacheLoadFromRocks(pTsdb, uid, pLastArray, remainCols, pr, ltype);

    taosThreadMutexUnlock(&pTsdb->lruMutex);

    if (remainCols) {
      taosArrayDestroy(remainCols);
    }
  }

  return code;
}

int32_t tsdbCacheDel(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey) {
  int32_t code = 0;
  // fetch schema
  STSchema *pTSchema = NULL;
  int       sver = -1;
  code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, sver, &pTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return -1;
  }

  // build keys & multi get from rocks
  int          num_keys = pTSchema->numOfCols;
  char       **keys_list = taosMemoryCalloc(num_keys * 2, sizeof(char *));
  size_t      *keys_list_sizes = taosMemoryCalloc(num_keys * 2, sizeof(size_t));
  const size_t klen = ROCKS_KEY_LEN;
  for (int i = 0; i < num_keys; ++i) {
    int16_t cid = pTSchema->columns[i].colId;

    char *keys = taosMemoryCalloc(2, sizeof(SLastKey));
    ((SLastKey *)keys)[0] = (SLastKey){.ltype = 1, .uid = uid, .cid = cid};
    ((SLastKey *)keys)[1] = (SLastKey){.ltype = 0, .uid = uid, .cid = cid};

    keys_list[i] = keys;
    keys_list[num_keys + i] = keys + sizeof(SLastKey);
    keys_list_sizes[i] = klen;
    keys_list_sizes[num_keys + i] = klen;
  }
  char  **values_list = taosMemoryCalloc(num_keys * 2, sizeof(char *));
  size_t *values_list_sizes = taosMemoryCalloc(num_keys * 2, sizeof(size_t));
  char  **errs = taosMemoryCalloc(num_keys * 2, sizeof(char *));

  (void)tsdbCacheCommit(pTsdb);

  taosThreadMutexLock(&pTsdb->lruMutex);

  taosThreadMutexLock(&pTsdb->rCache.rMutex);
  // rocksMayWrite(pTsdb, true, false, false);
  rocksdb_multi_get(pTsdb->rCache.db, pTsdb->rCache.readoptions, num_keys * 2, (const char *const *)keys_list,
                    keys_list_sizes, values_list, values_list_sizes, errs);
  taosThreadMutexUnlock(&pTsdb->rCache.rMutex);

  for (int i = 0; i < num_keys * 2; ++i) {
    if (errs[i]) {
      rocksdb_free(errs[i]);
    }
  }
  taosMemoryFree(errs);

  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  for (int i = 0; i < num_keys; ++i) {
    SLastCol *pLastCol = tsdbCacheDeserialize(values_list[i]);
    taosThreadMutexLock(&pTsdb->rCache.rMutex);
    if (NULL != pLastCol && (pLastCol->ts <= eKey && pLastCol->ts >= sKey)) {
      rocksdb_writebatch_delete(wb, keys_list[i], klen);
    }
    pLastCol = tsdbCacheDeserialize(values_list[i + num_keys]);
    if (NULL != pLastCol && (pLastCol->ts <= eKey && pLastCol->ts >= sKey)) {
      rocksdb_writebatch_delete(wb, keys_list[num_keys + i], klen);
    }
    taosThreadMutexUnlock(&pTsdb->rCache.rMutex);

    rocksdb_free(values_list[i]);
    rocksdb_free(values_list[i + num_keys]);

    // taosThreadMutexLock(&pTsdb->lruMutex);

    bool       erase = false;
    LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, keys_list[i], klen);
    if (h) {
      SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pTsdb->lruCache, h);
      if (pLastCol->dirty) {
        pLastCol->dirty = 0;
      }
      if (pLastCol->ts <= eKey && pLastCol->ts >= sKey) {
        erase = true;
      }
      taosLRUCacheRelease(pTsdb->lruCache, h, erase);
    }
    if (erase) {
      taosLRUCacheErase(pTsdb->lruCache, keys_list[i], klen);
    }

    erase = false;
    h = taosLRUCacheLookup(pTsdb->lruCache, keys_list[num_keys + i], klen);
    if (h) {
      SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pTsdb->lruCache, h);
      if (pLastCol->dirty) {
        pLastCol->dirty = 0;
      }
      if (pLastCol->ts <= eKey && pLastCol->ts >= sKey) {
        erase = true;
      }
      taosLRUCacheRelease(pTsdb->lruCache, h, erase);
    }
    if (erase) {
      taosLRUCacheErase(pTsdb->lruCache, keys_list[num_keys + i], klen);
    }
    // taosThreadMutexUnlock(&pTsdb->lruMutex);
  }
  for (int i = 0; i < num_keys; ++i) {
    taosMemoryFree(keys_list[i]);
  }
  taosMemoryFree(keys_list);
  taosMemoryFree(keys_list_sizes);
  taosMemoryFree(values_list);
  taosMemoryFree(values_list_sizes);

  rocksMayWrite(pTsdb, true, false, true);

  taosThreadMutexUnlock(&pTsdb->lruMutex);

_exit:
  taosMemoryFree(pTSchema);

  return code;
}

int32_t tsdbOpenCache(STsdb *pTsdb) {
  int32_t    code = 0;
  SLRUCache *pCache = NULL;
  size_t     cfgCapacity = pTsdb->pVnode->config.cacheLastSize * 1024 * 1024;

  pCache = taosLRUCacheInit(cfgCapacity, 0, .5);
  if (pCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

#if 0
  code = tsdbOpenBICache(pTsdb);
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
#endif

  code = tsdbOpenBCache(pTsdb);
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  code = tsdbOpenPgCache(pTsdb);
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  code = tsdbOpenRocksCache(pTsdb);
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  taosLRUCacheSetStrictCapacity(pCache, false);

  taosThreadMutexInit(&pTsdb->lruMutex, NULL);

  pTsdb->flushState.pTsdb = pTsdb;
  pTsdb->flushState.flush_count = 0;

_err:
  pTsdb->lruCache = pCache;
  return code;
}

void tsdbCloseCache(STsdb *pTsdb) {
  SLRUCache *pCache = pTsdb->lruCache;
  if (pCache) {
    taosLRUCacheEraseUnrefEntries(pCache);

    taosLRUCacheCleanup(pCache);

    taosThreadMutexDestroy(&pTsdb->lruMutex);
  }

#if 0
  tsdbCloseBICache(pTsdb);
#endif
  tsdbCloseBCache(pTsdb);
  tsdbClosePgCache(pTsdb);
  tsdbCloseRocksCache(pTsdb);
}

static void getTableCacheKey(tb_uid_t uid, int cacheType, char *key, int *len) {
  if (cacheType == 0) {  // last_row
    *(uint64_t *)key = (uint64_t)uid;
  } else {  // last
    *(uint64_t *)key = ((uint64_t)uid) | 0x8000000000000000;
  }

  *len = sizeof(uint64_t);
}

#ifdef BUILD_NO_CALL
static void deleteTableCacheLast(const void *key, size_t keyLen, void *value, void *ud) {
  (void)ud;
  SArray *pLastArray = (SArray *)value;
  int16_t nCol = taosArrayGetSize(pLastArray);
  for (int16_t iCol = 0; iCol < nCol; ++iCol) {
    SLastCol *pLastCol = (SLastCol *)taosArrayGet(pLastArray, iCol);
    if (IS_VAR_DATA_TYPE(pLastCol->colVal.type) && pLastCol->colVal.value.nData > 0) {
      taosMemoryFree(pLastCol->colVal.value.pData);
    }
  }

  taosArrayDestroy(value);
}

int32_t tsdbCacheDeleteLastrow(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey) {
  int32_t code = 0;

  char key[32] = {0};
  int  keyLen = 0;

  // getTableCacheKey(uid, "lr", key, &keyLen);
  getTableCacheKey(uid, 0, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    SArray *pLast = (SArray *)taosLRUCacheValue(pCache, h);
    bool    invalidate = false;
    int16_t nCol = taosArrayGetSize(pLast);

    for (int16_t iCol = 0; iCol < nCol; ++iCol) {
      SLastCol *tTsVal = (SLastCol *)taosArrayGet(pLast, iCol);
      if (eKey >= tTsVal->ts) {
        invalidate = true;
        break;
      }
    }

    taosLRUCacheRelease(pCache, h, invalidate);
    if (invalidate) {
      taosLRUCacheErase(pCache, key, keyLen);
    }
  }

  return code;
}

int32_t tsdbCacheDeleteLast(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey) {
  int32_t code = 0;

  char key[32] = {0};
  int  keyLen = 0;

  // getTableCacheKey(uid, "l", key, &keyLen);
  getTableCacheKey(uid, 1, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    SArray *pLast = (SArray *)taosLRUCacheValue(pCache, h);
    bool    invalidate = false;
    int16_t nCol = taosArrayGetSize(pLast);

    for (int16_t iCol = 0; iCol < nCol; ++iCol) {
      SLastCol *tTsVal = (SLastCol *)taosArrayGet(pLast, iCol);
      if (eKey >= tTsVal->ts) {
        invalidate = true;
        break;
      }
    }

    taosLRUCacheRelease(pCache, h, invalidate);
    if (invalidate) {
      taosLRUCacheErase(pCache, key, keyLen);
    }
  }

  return code;
}

int32_t tsdbCacheInsertLastrow(SLRUCache *pCache, STsdb *pTsdb, tb_uid_t uid, TSDBROW *row, bool dup) {
  int32_t code = 0;
  STSRow *cacheRow = NULL;
  char    key[32] = {0};
  int     keyLen = 0;

  // getTableCacheKey(uid, "lr", key, &keyLen);
  getTableCacheKey(uid, 0, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    STSchema *pTSchema = metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1, 1);
    TSKEY     keyTs = TSDBROW_TS(row);
    bool      invalidate = false;

    SArray *pLast = (SArray *)taosLRUCacheValue(pCache, h);
    int16_t nCol = taosArrayGetSize(pLast);
    int16_t iCol = 0;

    if (nCol <= 0) {
      nCol = pTSchema->numOfCols;

      STColumn *pTColumn = &pTSchema->columns[0];
      SColVal   tColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.val = keyTs});
      if (taosArrayPush(pLast, &(SLastCol){.ts = keyTs, .colVal = tColVal}) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _invalidate;
      }

      for (iCol = 1; iCol < nCol; ++iCol) {
        SColVal colVal = {0};
        tsdbRowGetColVal(row, pTSchema, iCol, &colVal);

        SLastCol lastCol = {.ts = keyTs, .colVal = colVal};
        if (IS_VAR_DATA_TYPE(colVal.type) && colVal.value.nData > 0) {
          lastCol.colVal.value.pData = taosMemoryMalloc(colVal.value.nData);
          if (lastCol.colVal.value.pData == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _invalidate;
          }
          memcpy(lastCol.colVal.value.pData, colVal.value.pData, colVal.value.nData);
        }

        if (taosArrayPush(pLast, &lastCol) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _invalidate;
        }
      }

      goto _invalidate;
    }

    if (nCol != pTSchema->numOfCols) {
      invalidate = true;
      goto _invalidate;
    }

    SLastCol *tTsVal = (SLastCol *)taosArrayGet(pLast, iCol);
    if (keyTs > tTsVal->ts) {
      STColumn *pTColumn = &pTSchema->columns[0];
      SColVal   tColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.val = keyTs});

      taosArraySet(pLast, iCol, &(SLastCol){.ts = keyTs, .colVal = tColVal});
    }

    for (++iCol; iCol < nCol; ++iCol) {
      SLastCol *tTsVal1 = (SLastCol *)taosArrayGet(pLast, iCol);
      if (keyTs >= tTsVal1->ts) {
        SColVal *tColVal = &tTsVal1->colVal;

        SColVal colVal = {0};
        tsdbRowGetColVal(row, pTSchema, iCol, &colVal);

        if (colVal.cid != tColVal->cid) {
          invalidate = true;
          goto _invalidate;
        }

        if (!COL_VAL_IS_NONE(&colVal)) {
          if (keyTs == tTsVal1->ts && !COL_VAL_IS_NONE(tColVal)) {
            invalidate = true;

            break;
          } else {  // new inserting key is greater than cached, update cached entry
            SLastCol lastCol = {.ts = keyTs, .colVal = colVal};
            if (IS_VAR_DATA_TYPE(colVal.type) && colVal.value.nData > 0) {
              SLastCol *pLastCol = (SLastCol *)taosArrayGet(pLast, iCol);
              if (pLastCol->colVal.value.nData > 0 && NULL != pLastCol->colVal.value.pData)
                taosMemoryFree(pLastCol->colVal.value.pData);

              lastCol.colVal.value.pData = taosMemoryMalloc(colVal.value.nData);
              if (lastCol.colVal.value.pData == NULL) {
                terrno = TSDB_CODE_OUT_OF_MEMORY;
                code = TSDB_CODE_OUT_OF_MEMORY;
                goto _invalidate;
              }
              memcpy(lastCol.colVal.value.pData, colVal.value.pData, colVal.value.nData);
            }

            taosArraySet(pLast, iCol, &lastCol);
          }
        }
      }
    }

  _invalidate:
    taosMemoryFreeClear(pTSchema);

    taosLRUCacheRelease(pCache, h, invalidate);
    if (invalidate) {
      taosLRUCacheErase(pCache, key, keyLen);
    }
  }

  return code;
}

int32_t tsdbCacheInsertLast(SLRUCache *pCache, tb_uid_t uid, TSDBROW *row, STsdb *pTsdb) {
  int32_t code = 0;
  STSRow *cacheRow = NULL;
  char    key[32] = {0};
  int     keyLen = 0;

  // getTableCacheKey(uid, "l", key, &keyLen);
  getTableCacheKey(uid, 1, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    STSchema *pTSchema = metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1, 1);
    TSKEY     keyTs = TSDBROW_TS(row);
    bool      invalidate = false;

    SArray *pLast = (SArray *)taosLRUCacheValue(pCache, h);
    int16_t nCol = taosArrayGetSize(pLast);
    int16_t iCol = 0;

    if (nCol <= 0) {
      nCol = pTSchema->numOfCols;

      STColumn *pTColumn = &pTSchema->columns[0];
      SColVal   tColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.val = keyTs});
      if (taosArrayPush(pLast, &(SLastCol){.ts = keyTs, .colVal = tColVal}) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _invalidate;
      }

      for (iCol = 1; iCol < nCol; ++iCol) {
        SColVal colVal = {0};
        tsdbRowGetColVal(row, pTSchema, iCol, &colVal);

        SLastCol lastCol = {.ts = keyTs, .colVal = colVal};
        if (IS_VAR_DATA_TYPE(colVal.type) && colVal.value.nData > 0) {
          lastCol.colVal.value.pData = taosMemoryMalloc(colVal.value.nData);
          if (lastCol.colVal.value.pData == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _invalidate;
          }
          memcpy(lastCol.colVal.value.pData, colVal.value.pData, colVal.value.nData);
        }

        if (taosArrayPush(pLast, &lastCol) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _invalidate;
        }
      }

      goto _invalidate;
    }

    if (nCol != pTSchema->numOfCols) {
      invalidate = true;
      goto _invalidate;
    }

    SLastCol *tTsVal = (SLastCol *)taosArrayGet(pLast, iCol);
    if (keyTs > tTsVal->ts) {
      STColumn *pTColumn = &pTSchema->columns[0];
      SColVal   tColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.val = keyTs});

      taosArraySet(pLast, iCol, &(SLastCol){.ts = keyTs, .colVal = tColVal});
    }

    for (++iCol; iCol < nCol; ++iCol) {
      SLastCol *tTsVal1 = (SLastCol *)taosArrayGet(pLast, iCol);
      if (keyTs >= tTsVal1->ts) {
        SColVal *tColVal = &tTsVal1->colVal;

        SColVal colVal = {0};
        tsdbRowGetColVal(row, pTSchema, iCol, &colVal);

        if (colVal.cid != tColVal->cid) {
          invalidate = true;
          goto _invalidate;
        }

        if (COL_VAL_IS_VALUE(&colVal)) {
          if (keyTs == tTsVal1->ts && COL_VAL_IS_VALUE(tColVal)) {
            invalidate = true;

            break;
          } else {
            SLastCol lastCol = {.ts = keyTs, .colVal = colVal};
            if (IS_VAR_DATA_TYPE(colVal.type) && colVal.value.nData > 0) {
              SLastCol *pLastCol = (SLastCol *)taosArrayGet(pLast, iCol);
              if (pLastCol->colVal.value.nData > 0 && NULL != pLastCol->colVal.value.pData)
                taosMemoryFree(pLastCol->colVal.value.pData);

              lastCol.colVal.value.pData = taosMemoryMalloc(colVal.value.nData);
              if (lastCol.colVal.value.pData == NULL) {
                terrno = TSDB_CODE_OUT_OF_MEMORY;
                code = TSDB_CODE_OUT_OF_MEMORY;
                goto _invalidate;
              }
              memcpy(lastCol.colVal.value.pData, colVal.value.pData, colVal.value.nData);
            }

            taosArraySet(pLast, iCol, &lastCol);
          }
        }
      }
    }

  _invalidate:
    taosMemoryFreeClear(pTSchema);

    taosLRUCacheRelease(pCache, h, invalidate);
    if (invalidate) {
      taosLRUCacheErase(pCache, key, keyLen);
    }
  }

  return code;
}
#endif

static tb_uid_t getTableSuidByUid(tb_uid_t uid, STsdb *pTsdb) {
  tb_uid_t suid = 0;

  SMetaReader mr = {0};
  metaReaderDoInit(&mr, pTsdb->pVnode->pMeta, 0);
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

  return code;
}

static int32_t getTableDelDataFromTbData(STbData *pTbData, SArray *aDelData) {
  int32_t   code = 0;
  SDelData *pDelData = pTbData ? pTbData->pHead : NULL;

  for (; pDelData; pDelData = pDelData->pNext) {
    taosArrayPush(aDelData, pDelData);
  }

  return code;
}

#ifdef BUILD_NO_CALL
static int32_t getTableDelData(STbData *pMem, STbData *pIMem, SDelFReader *pDelReader, SDelIdx *pDelIdx,
                               SArray *aDelData) {
  int32_t code = 0;

  if (pDelIdx) {
    code = getTableDelDataFromDelIdx(pDelReader, pDelIdx, aDelData);
    if (code) goto _err;
  }

  if (pMem) {
    code = getTableDelDataFromTbData(pMem, aDelData);
    if (code) goto _err;
  }

  if (pIMem) {
    code = getTableDelDataFromTbData(pIMem, aDelData);
    if (code) goto _err;
  }

_err:
  return code;
}

static int32_t getTableDelSkyline(STbData *pMem, STbData *pIMem, SDelFReader *pDelReader, SDelIdx *pDelIdx,
                                  SArray *aSkyline) {
  int32_t code = 0;
  SArray *aDelData = NULL;

  aDelData = taosArrayInit(32, sizeof(SDelData));
  code = getTableDelData(pMem, pIMem, pDelReader, pDelIdx, aDelData);
  if (code) goto _err;

  size_t nDelData = taosArrayGetSize(aDelData);
  if (nDelData > 0) {
    code = tsdbBuildDeleteSkyline(aDelData, 0, (int32_t)(nDelData - 1), aSkyline);
    if (code) goto _err;
  }

_err:
  if (aDelData) {
    taosArrayDestroy(aDelData);
  }
  return code;
}
#endif

static void freeTableInfoFunc(void *param) {
  void **p = (void **)param;
  taosMemoryFreeClear(*p);
}

static STableLoadInfo *getTableLoadInfo(SCacheRowsReader *pReader, uint64_t uid) {
  if (!pReader->pTableMap) {
    pReader->pTableMap = tSimpleHashInit(pReader->numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));

    tSimpleHashSetFreeFp(pReader->pTableMap, freeTableInfoFunc);
  }

  STableLoadInfo  *pInfo = NULL;
  STableLoadInfo **ppInfo = tSimpleHashGet(pReader->pTableMap, &uid, sizeof(uid));
  if (!ppInfo) {
    pInfo = taosMemoryCalloc(1, sizeof(STableLoadInfo));
    tSimpleHashPut(pReader->pTableMap, &uid, sizeof(uint64_t), &pInfo, POINTER_BYTES);

    return pInfo;
  }

  return *ppInfo;
}

static uint64_t *getUidList(SCacheRowsReader *pReader) {
  if (!pReader->uidList) {
    int32_t numOfTables = pReader->numOfTables;

    pReader->uidList = taosMemoryMalloc(numOfTables * sizeof(uint64_t));

    for (int32_t i = 0; i < numOfTables; ++i) {
      uint64_t uid = pReader->pTableList[i].uid;
      pReader->uidList[i] = uid;
    }

    taosSort(pReader->uidList, numOfTables, sizeof(uint64_t), uidComparFunc);
  }

  return pReader->uidList;
}

static int32_t loadTombFromBlk(const TTombBlkArray *pTombBlkArray, SCacheRowsReader *pReader, void *pFileReader,
                               bool isFile) {
  int32_t   code = 0;
  uint64_t *uidList = getUidList(pReader);
  int32_t   numOfTables = pReader->numOfTables;
  int64_t   suid = pReader->info.suid;

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
      return code;
    }

    uint64_t        uid = uidList[j];
    STableLoadInfo *pInfo = getTableLoadInfo(pReader, uid);
    if (pInfo->pTombData == NULL) {
      pInfo->pTombData = taosArrayInit(4, sizeof(SDelData));
    }

    STombRecord record = {0};
    bool        finished = false;
    for (int32_t k = 0; k < TARRAY2_SIZE(block.suid); ++k) {
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
        if (pInfo->pTombData == NULL) {
          pInfo->pTombData = taosArrayInit(4, sizeof(SDelData));
        }
      }

      if (record.version <= pReader->info.verRange.maxVer) {
        /*tsdbError("tomb xx load/cache: vgId:%d fid:%d record %" PRId64 "~%" PRId64 "~%" PRId64 " tomb records",
          TD_VID(pReader->pTsdb->pVnode), pReader->pCurFileSet->fid, record.skey, record.ekey, uid);*/

        SDelData delData = {.version = record.version, .sKey = record.skey, .eKey = record.ekey};
        taosArrayPush(pInfo->pTombData, &delData);
      }
    }

    tTombBlockDestroy(&block);

    if (finished) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t loadDataTomb(SCacheRowsReader *pReader, SDataFileReader *pFileReader) {
  int32_t code = 0;

  const TTombBlkArray *pBlkArray = NULL;
  code = tsdbDataFileReadTombBlk(pFileReader, &pBlkArray);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return loadTombFromBlk(pBlkArray, pReader, pFileReader, true);
}

static int32_t loadSttTomb(STsdbReader *pTsdbReader, SSttFileReader *pSttFileReader, SSttBlockLoadInfo *pLoadInfo) {
  int32_t code = 0;

  SCacheRowsReader *pReader = (SCacheRowsReader *)pTsdbReader;

  const TTombBlkArray *pBlkArray = NULL;
  code = tsdbSttFileReadTombBlk(pSttFileReader, &pBlkArray);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return loadTombFromBlk(pBlkArray, pReader, pSttFileReader, false);
}

typedef struct {
  SMergeTree  mergeTree;
  SMergeTree *pMergeTree;
} SFSLastIter;

static int32_t lastIterOpen(SFSLastIter *iter, STFileSet *pFileSet, STsdb *pTsdb, STSchema *pTSchema, tb_uid_t suid,
                            tb_uid_t uid, SCacheRowsReader *pr, int64_t lastTs, int16_t *aCols, int nCols) {
  int32_t code = 0;
  pr->pLDataIterArray = destroySttBlockReader(pr->pLDataIterArray, NULL);
  pr->pLDataIterArray = taosArrayInit(4, POINTER_BYTES);

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
  };

  code = tMergeTreeOpen2(&iter->mergeTree, &conf, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    return -1;
  }

  iter->pMergeTree = &iter->mergeTree;

  return code;
}

static int32_t lastIterClose(SFSLastIter **iter) {
  int32_t code = 0;

  if ((*iter)->pMergeTree) {
    tMergeTreeClose((*iter)->pMergeTree);
    (*iter)->pMergeTree = NULL;
  }

  *iter = NULL;

  return code;
}

static int32_t lastIterNext(SFSLastIter *iter, TSDBROW **ppRow) {
  int32_t code = 0;

  bool hasVal = tMergeTreeNext(iter->pMergeTree);
  if (!hasVal) {
    *ppRow = NULL;
    return code;
  }

  *ppRow = tMergeTreeGetRow(iter->pMergeTree);

  return code;
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
  SFSNextRowIter *state = (SFSNextRowIter *)iter;
  int32_t         code = 0;
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
      return code;
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

        code = tsdbDataFileReaderOpen(filesName, &conf, &state->pr->pFileReader);
        if (code != TSDB_CODE_SUCCESS) {
          goto _err;
        }

        state->pr->pCurFileSet = state->pFileSet;

        loadDataTomb(state->pr, state->pr->pFileReader);

        int32_t code = tsdbDataFileReadBrinBlk(state->pr->pFileReader, &state->pr->pBlkArray);
        if (code != TSDB_CODE_SUCCESS) {
          goto _err;
        }
      }

      if (!state->pIndexList) {
        state->pIndexList = taosArrayInit(1, sizeof(SBrinBlk));
      } else {
        taosArrayClear(state->pIndexList);
      }

      const TBrinBlkArray *pBlkArray = state->pr->pBlkArray;

      for (int i = TARRAY2_SIZE(pBlkArray) - 1; i >= 0; --i) {
        SBrinBlk *pBrinBlk = &pBlkArray->data[i];
        if (state->suid >= pBrinBlk->minTbid.suid && state->suid <= pBrinBlk->maxTbid.suid) {
          if (state->uid >= pBrinBlk->minTbid.uid && state->uid <= pBrinBlk->maxTbid.uid) {
            taosArrayPush(state->pIndexList, pBrinBlk);
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
      state->iBrinIndex = indexSize;
    }

  _check_stt_data:
    if (state->pFileSet != state->pr->pCurFileSet) {
      state->pr->pCurFileSet = state->pFileSet;
    }

    code = lastIterOpen(&state->lastIter, state->pFileSet, pTsdb, state->pTSchema, state->suid, state->uid, state->pr,
                        state->lastTs, aCols, nCols);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    code = lastIterNext(&state->lastIter, &state->pLastRow);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

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
        return code;
      }
    }

    state->pLastIter = &state->lastIter;
  }

  if (SFSNEXTROW_NEXTSTTROW == state->state) {
    code = lastIterNext(&state->lastIter, &state->pLastRow);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    if (!state->pLastRow) {
      if (state->pLastIter) {
        lastIterClose(&state->pLastIter);
      }

      clearLastFileSet(state);
      state->state = SFSNEXTROW_FILESET;
      goto _next_fileset;
    } else {
      *ppRow = state->pLastRow;
      state->pLastRow = NULL;
      return code;
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
    code = tsdbDataFileReadBrinBlock(state->pr->pFileReader, pBrinBlk, &state->brinBlock);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    state->iBrinRecord = BRIN_BLOCK_SIZE(&state->brinBlock) - 1;
    state->state = SFSNEXTROW_BRINBLOCK;
  }

  if (SFSNEXTROW_BRINBLOCK == state->state) {
  _next_brinrecord:
    if (state->iBrinRecord < 0) {  // empty brin block, goto _next_brinindex
      tBrinBlockClear(&state->brinBlock);
      goto _next_brinindex;
    }
    code = tBrinBlockGet(&state->brinBlock, state->iBrinRecord, &state->brinRecord);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

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
      code = tBlockDataCreate(&state->blockData);
      if (code) goto _err;
    } else {
      tBlockDataReset(state->pBlockData);
    }

    if (aCols[0] == PRIMARYKEY_TIMESTAMP_COL_ID) {
      --nCols;
      ++aCols;
    }
    code = tsdbDataFileReadBlockDataByColumn(state->pr->pFileReader, pRecord, state->pBlockData, state->pTSchema, aCols,
                                             nCols);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

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
      code = lastIterNext(&state->lastIter, &state->pLastRow);
      if (code != TSDB_CODE_SUCCESS) {
        goto _err;
      }
    }

    if (!state->pLastRow) {
      if (state->pLastIter) {
        lastIterClose(&state->pLastIter);
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
      return code;
    } else if (lastRowTs < rowTs) {
      *ppRow = &state->row;
      --state->iRow;
      return code;
    } else {
      // TODO: merge rows and *ppRow = mergedRow
      SRowMerger *pMerger = &state->rowMerger;
      tsdbRowMergerInit(pMerger, state->pTSchema);

      code = tsdbRowMergerAdd(pMerger, &state->row, state->pTSchema);
      if (code != TSDB_CODE_SUCCESS) {
        goto _err;
      }
      code = tsdbRowMergerAdd(pMerger, state->pLastRow, state->pTSchema);
      if (code != TSDB_CODE_SUCCESS) {
        goto _err;
      }

      if (state->pTSRow) {
        taosMemoryFree(state->pTSRow);
        state->pTSRow = NULL;
      }

      code = tsdbRowMergerGetRow(pMerger, &state->pTSRow);
      if (code != TSDB_CODE_SUCCESS) {
        goto _err;
      }

      state->row = tsdbRowFromTSRow(TSDBROW_VERSION(&state->row), state->pTSRow);
      *ppRow = &state->row;
      --state->iRow;

      tsdbRowMergerClear(pMerger);

      return code;
    }
  }

_err:
  clearLastFileSet(state);

  *ppRow = NULL;

  return code;
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
        if (state->pMem->maxKey <= state->lastTs) {
          *ppRow = NULL;
          *pIgnoreEarlierTs = true;
          return code;
        }
        tsdbTbDataIterOpen(state->pMem, NULL, 1, &state->iter);

        TSDBROW *pMemRow = tsdbTbDataIterGet(&state->iter);
        if (pMemRow) {
          *ppRow = pMemRow;
          state->state = SMEMNEXTROW_NEXT;

          return code;
        }
      }

      *ppRow = NULL;

      return code;
    }
    case SMEMNEXTROW_NEXT:
      if (tsdbTbDataIterNext(&state->iter)) {
        *ppRow = tsdbTbDataIterGet(&state->iter);

        return code;
      } else {
        *ppRow = NULL;

        return code;
      }
    default:
      ASSERT(0);
      break;
  }

_err:
  *ppRow = NULL;
  return code;
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
    return code;
  }

  if (state->pLastIter) {
    lastIterClose(&state->pLastIter);
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

  return code;
}

static void clearLastFileSet(SFSNextRowIter *state) {
  if (state->pLastIter) {
    lastIterClose(&state->pLastIter);
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
      pInfo->pTombData = taosArrayDestroy(pInfo->pTombData);
    }
  }
}

static int32_t nextRowIterOpen(CacheNextRowIter *pIter, tb_uid_t uid, STsdb *pTsdb, STSchema *pTSchema, tb_uid_t suid,
                               SArray *pLDataIterArray, STsdbReadSnap *pReadSnap, int64_t lastTs,
                               SCacheRowsReader *pr) {
  int code = 0;

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

  loadMemTombData(&pIter->pMemDelData, pMem, pIMem, pr->info.verRange.maxVer);

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
  return code;
}

static int32_t nextRowIterClose(CacheNextRowIter *pIter) {
  int code = 0;

  for (int i = 0; i < 3; ++i) {
    if (pIter->input[i].nextRowClearFn) {
      pIter->input[i].nextRowClearFn(pIter->input[i].iter);
    }
  }

  if (pIter->pSkyline) {
    taosArrayDestroy(pIter->pSkyline);
  }

  if (pIter->pMemDelData) {
    taosArrayDestroy(pIter->pMemDelData);
  }

_err:
  return code;
}

// iterate next row non deleted backward ts, version (from high to low)
static int32_t nextRowIterGet(CacheNextRowIter *pIter, TSDBROW **ppRow, bool *pIgnoreEarlierTs, bool isLast,
                              int16_t *aCols, int nCols) {
  int code = 0;
  for (;;) {
    for (int i = 0; i < 3; ++i) {
      if (pIter->input[i].next && !pIter->input[i].stop) {
        code = pIter->input[i].nextRowFn(pIter->input[i].iter, &pIter->input[i].pRow, &pIter->input[i].ignoreEarlierTs,
                                         isLast, aCols, nCols);
        if (code) goto _err;

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
      return code;
    }

    // select maxpoint(s) from mem, imem, fs and last
    TSDBROW *max[4] = {0};
    int      iMax[4] = {-1, -1, -1, -1};
    int      nMax = 0;
    TSKEY    maxKey = TSKEY_MIN;

    for (int i = 0; i < 3; ++i) {
      if (!pIter->input[i].stop && pIter->input[i].pRow != NULL) {
        TSDBKEY key = TSDBROW_KEY(pIter->input[i].pRow);

        // merging & deduplicating on client side
        if (maxKey <= key.ts) {
          if (maxKey < key.ts) {
            nMax = 0;
            maxKey = key.ts;
          }

          iMax[nMax] = i;
          max[nMax++] = pIter->input[i].pRow;
        } else {
          pIter->input[i].next = false;
        }
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

        uint64_t        uid = pIter->idx.uid;
        STableLoadInfo *pInfo = getTableLoadInfo(pIter->pr, uid);
        if (pInfo->pTombData == NULL) {
          pInfo->pTombData = taosArrayInit(4, sizeof(SDelData));
        }

        taosArrayAddAll(pInfo->pTombData, pIter->pMemDelData);

        size_t delSize = TARRAY_SIZE(pInfo->pTombData);
        if (delSize > 0) {
          code = tsdbBuildDeleteSkyline(pInfo->pTombData, 0, (int32_t)(delSize - 1), pIter->pSkyline);
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
      return code;
    }
  }

_err:
  return code;
}

#ifdef BUILD_NO_CALL
static int32_t initLastColArray(STSchema *pTSchema, SArray **ppColArray) {
  SArray *pColArray = taosArrayInit(pTSchema->numOfCols, sizeof(SLastCol));
  if (NULL == pColArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < pTSchema->numOfCols; ++i) {
    SLastCol col = {.ts = 0, .colVal = COL_VAL_NULL(pTSchema->columns[i].colId, pTSchema->columns[i].type)};
    taosArrayPush(pColArray, &col);
  }
  *ppColArray = pColArray;
  return TSDB_CODE_SUCCESS;
}
#endif

static int32_t initLastColArrayPartial(STSchema *pTSchema, SArray **ppColArray, int16_t *slotIds, int nCols) {
  SArray *pColArray = taosArrayInit(nCols, sizeof(SLastCol));
  if (NULL == pColArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < nCols; ++i) {
    int16_t  slotId = slotIds[i];
    SLastCol col = {.ts = 0, .colVal = COL_VAL_NULL(pTSchema->columns[slotId].colId, pTSchema->columns[slotId].type)};
    taosArrayPush(pColArray, &col);
  }
  *ppColArray = pColArray;
  return TSDB_CODE_SUCCESS;
}

static int32_t cloneTSchema(STSchema *pSrc, STSchema **ppDst) {
  int32_t len = sizeof(STSchema) + sizeof(STColumn) * pSrc->numOfCols;
  *ppDst = taosMemoryMalloc(len);
  if (NULL == *ppDst) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memcpy(*ppDst, pSrc, len);
  return TSDB_CODE_SUCCESS;
}

static int32_t updateTSchema(int32_t sversion, SCacheRowsReader *pReader, uint64_t uid) {
  if (NULL == pReader->pCurrSchema && sversion == pReader->pSchema->version) {
    return cloneTSchema(pReader->pSchema, &pReader->pCurrSchema);
  }

  if (NULL != pReader->pCurrSchema && sversion == pReader->pCurrSchema->version) {
    return TSDB_CODE_SUCCESS;
  }

  taosMemoryFreeClear(pReader->pCurrSchema);
  return metaGetTbTSchemaEx(pReader->pTsdb->pVnode->pMeta, pReader->info.suid, uid, sversion, &pReader->pCurrSchema);
}

static int32_t mergeLastCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                            int nCols, int16_t *slotIds) {
  STSchema *pTSchema = pr->pSchema;  // metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1, 1);
  int16_t   nLastCol = nCols;
  int16_t   noneCol = 0;
  bool      setNoneCol = false;
  bool      hasRow = false;
  bool      ignoreEarlierTs = false;
  SArray   *pColArray = NULL;
  SColVal  *pColVal = &(SColVal){0};

  int32_t code = initLastColArrayPartial(pTSchema, &pColArray, slotIds, nCols);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  SArray *aColArray = taosArrayInit(nCols, sizeof(int16_t));
  if (NULL == aColArray) {
    taosArrayDestroy(pColArray);

    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int i = 0; i < nCols; ++i) {
    taosArrayPush(aColArray, &aCols[i]);
  }

  TSKEY lastRowTs = TSKEY_MAX;

  CacheNextRowIter iter = {0};
  nextRowIterOpen(&iter, uid, pTsdb, pTSchema, pr->info.suid, pr->pLDataIterArray, pr->pReadSnap, pr->lastTs, pr);

  do {
    TSDBROW *pRow = NULL;
    nextRowIterGet(&iter, &pRow, &ignoreEarlierTs, true, TARRAY_DATA(aColArray), TARRAY_SIZE(aColArray));

    if (!pRow) {
      break;
    }

    hasRow = true;

    int32_t sversion = TSDBROW_SVERSION(pRow);
    if (sversion != -1) {
      code = updateTSchema(sversion, pr, uid);
      if (TSDB_CODE_SUCCESS != code) {
        goto _err;
      }
      pTSchema = pr->pCurrSchema;
    }
    // int16_t nCol = pTSchema->numOfCols;

    TSKEY rowTs = TSDBROW_TS(pRow);

    if (lastRowTs == TSKEY_MAX) {
      lastRowTs = rowTs;

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

          *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.val = rowTs});
          taosArraySet(pColArray, 0, &(SLastCol){.ts = rowTs, .colVal = *pColVal});
          continue;
        }
        tsdbRowGetColVal(pRow, pTSchema, slotIds[iCol], pColVal);

        *pCol = (SLastCol){.ts = rowTs, .colVal = *pColVal};
        if (IS_VAR_DATA_TYPE(pColVal->type) /*&& pColVal->value.nData > 0*/) {
          if (pColVal->value.nData > 0) {
            pCol->colVal.value.pData = taosMemoryMalloc(pCol->colVal.value.nData);
            if (pCol->colVal.value.pData == NULL) {
              terrno = TSDB_CODE_OUT_OF_MEMORY;
              code = TSDB_CODE_OUT_OF_MEMORY;
              goto _err;
            }
            memcpy(pCol->colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
          } else {
            pCol->colVal.value.pData = NULL;
          }
        }

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
      SLastCol *lastColVal = (SLastCol *)taosArrayGet(pColArray, iCol);
      if (lastColVal->colVal.cid != pTSchema->columns[slotIds[iCol]].colId) {
        continue;
      }
      SColVal *tColVal = &lastColVal->colVal;

      tsdbRowGetColVal(pRow, pTSchema, slotIds[iCol], pColVal);
      if (!COL_VAL_IS_VALUE(tColVal) && COL_VAL_IS_VALUE(pColVal)) {
        SLastCol lastCol = {.ts = rowTs, .colVal = *pColVal};
        if (IS_VAR_DATA_TYPE(pColVal->type) /* && pColVal->value.nData > 0 */) {
          SLastCol *pLastCol = (SLastCol *)taosArrayGet(pColArray, iCol);
          taosMemoryFree(pLastCol->colVal.value.pData);

          if (pColVal->value.nData > 0) {
            lastCol.colVal.value.pData = taosMemoryMalloc(lastCol.colVal.value.nData);
            if (lastCol.colVal.value.pData == NULL) {
              terrno = TSDB_CODE_OUT_OF_MEMORY;
              code = TSDB_CODE_OUT_OF_MEMORY;
              goto _err;
            }
            memcpy(lastCol.colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
          } else {
            lastCol.colVal.value.pData = NULL;
          }
        }

        taosArraySet(pColArray, iCol, &lastCol);
        int32_t aColIndex = taosArraySearchIdx(aColArray, &lastCol.colVal.cid, compareInt16Val, TD_EQ);
        if (aColIndex >= 0) {
          taosArrayRemove(aColArray, aColIndex);
        }
      } else if (!COL_VAL_IS_VALUE(tColVal) && !COL_VAL_IS_VALUE(pColVal) && !setNoneCol) {
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

  return code;

_err:
  nextRowIterClose(&iter);
  // taosMemoryFreeClear(pTSchema);
  *ppLastArray = NULL;
  taosArrayDestroy(pColArray);
  taosArrayDestroy(aColArray);
  return code;
}

static int32_t mergeLastRowCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                               int nCols, int16_t *slotIds) {
  STSchema *pTSchema = pr->pSchema;  // metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1, 1);
  int16_t   nLastCol = nCols;
  int16_t   noneCol = 0;
  bool      setNoneCol = false;
  bool      hasRow = false;
  bool      ignoreEarlierTs = false;
  SArray   *pColArray = NULL;
  SColVal  *pColVal = &(SColVal){0};

  int32_t code = initLastColArrayPartial(pTSchema, &pColArray, slotIds, nCols);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  SArray *aColArray = taosArrayInit(nCols, sizeof(int16_t));
  if (NULL == aColArray) {
    taosArrayDestroy(pColArray);

    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int i = 0; i < nCols; ++i) {
    taosArrayPush(aColArray, &aCols[i]);
  }

  TSKEY lastRowTs = TSKEY_MAX;

  CacheNextRowIter iter = {0};
  nextRowIterOpen(&iter, uid, pTsdb, pTSchema, pr->info.suid, pr->pLDataIterArray, pr->pReadSnap, pr->lastTs, pr);

  do {
    TSDBROW *pRow = NULL;
    nextRowIterGet(&iter, &pRow, &ignoreEarlierTs, false, TARRAY_DATA(aColArray), TARRAY_SIZE(aColArray));

    if (!pRow) {
      break;
    }

    hasRow = true;

    int32_t sversion = TSDBROW_SVERSION(pRow);
    if (sversion != -1) {
      code = updateTSchema(sversion, pr, uid);
      if (TSDB_CODE_SUCCESS != code) {
        goto _err;
      }
      pTSchema = pr->pCurrSchema;
    }
    // int16_t nCol = pTSchema->numOfCols;

    TSKEY rowTs = TSDBROW_TS(pRow);

    lastRowTs = rowTs;

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

        *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.val = rowTs});
        taosArraySet(pColArray, 0, &(SLastCol){.ts = rowTs, .colVal = *pColVal});
        continue;
      }
      tsdbRowGetColVal(pRow, pTSchema, slotIds[iCol], pColVal);

      *pCol = (SLastCol){.ts = rowTs, .colVal = *pColVal};
      if (IS_VAR_DATA_TYPE(pColVal->type) /*&& pColVal->value.nData > 0*/) {
        if (pColVal->value.nData > 0) {
          pCol->colVal.value.pData = taosMemoryMalloc(pCol->colVal.value.nData);
          if (pCol->colVal.value.pData == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _err;
          }
          if (pColVal->value.nData > 0) {
            memcpy(pCol->colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
          }
        } else {
          pCol->colVal.value.pData = NULL;
        }
      }

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

  return code;

_err:
  nextRowIterClose(&iter);

  *ppLastArray = NULL;
  taosArrayDestroy(pColArray);
  taosArrayDestroy(aColArray);
  return code;
}

int32_t tsdbCacheRelease(SLRUCache *pCache, LRUHandle *h) {
  int32_t code = 0;

  taosLRUCacheRelease(pCache, h, false);

  return code;
}

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

#if 0
static void getBICacheKey(int32_t fid, int64_t commitID, char *key, int *len) {
  struct {
    int32_t fid;
    int64_t commitID;
  } biKey = {0};

  biKey.fid = fid;
  biKey.commitID = commitID;

  *len = sizeof(biKey);
  memcpy(key, &biKey, *len);
}

static int32_t tsdbCacheLoadBlockIdx(SDataFReader *pFileReader, SArray **aBlockIdx) {
  SArray *pArray = taosArrayInit(8, sizeof(SBlockIdx));
  int32_t code = tsdbReadBlockIdx(pFileReader, pArray);

  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pArray);
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  *aBlockIdx = pArray;

  return code;
}

static void deleteBICache(const void *key, size_t keyLen, void *value, void *ud) {
  (void)ud;
  SArray *pArray = (SArray *)value;

  taosArrayDestroy(pArray);
}

int32_t tsdbCacheGetBlockIdx(SLRUCache *pCache, SDataFReader *pFileReader, LRUHandle **handle) {
  int32_t code = 0;
  char    key[128] = {0};
  int     keyLen = 0;

  getBICacheKey(pFileReader->pSet->fid, pFileReader->pSet->pHeadF->commitID, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (!h) {
    STsdb *pTsdb = pFileReader->pTsdb;
    taosThreadMutexLock(&pTsdb->biMutex);

    h = taosLRUCacheLookup(pCache, key, keyLen);
    if (!h) {
      SArray *pArray = NULL;
      code = tsdbCacheLoadBlockIdx(pFileReader, &pArray);
      //  if table's empty or error, return code of -1
      if (code != TSDB_CODE_SUCCESS || pArray == NULL) {
        taosThreadMutexUnlock(&pTsdb->biMutex);

        *handle = NULL;
        return 0;
      }

      size_t              charge = pArray->capacity * pArray->elemSize + sizeof(*pArray);
      _taos_lru_deleter_t deleter = deleteBICache;
      LRUStatus           status =
          taosLRUCacheInsert(pCache, key, keyLen, pArray, charge, deleter, &h, TAOS_LRU_PRIORITY_LOW, NULL);
      if (status != TAOS_LRU_STATUS_OK) {
        code = -1;
      }
    }

    taosThreadMutexUnlock(&pTsdb->biMutex);
  }

  tsdbTrace("bi cache:%p, ref", pCache);
  *handle = h;

  return code;
}

int32_t tsdbBICacheRelease(SLRUCache *pCache, LRUHandle *h) {
  int32_t code = 0;

  taosLRUCacheRelease(pCache, h, false);
  tsdbTrace("bi cache:%p, release", pCache);

  return code;
}
#endif

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

static int32_t tsdbCacheLoadBlockS3(STsdbFD *pFD, uint8_t **ppBlock) {
  int32_t code = 0;
  /*
  uint8_t *pBlock = taosMemoryCalloc(1, tsS3BlockSize * pFD->szPage);
  if (pBlock == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  */
  int64_t block_offset = (pFD->blkno - 1) * tsS3BlockSize * pFD->szPage;
  code = s3GetObjectBlock(pFD->objName, block_offset, tsS3BlockSize * pFD->szPage, 0, ppBlock);
  if (code != TSDB_CODE_SUCCESS) {
    // taosMemoryFree(pBlock);
    // code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  //*ppBlock = pBlock;

  tsdbTrace("block:%p load from s3", *ppBlock);

_exit:
  return code;
}

static void deleteBCache(const void *key, size_t keyLen, void *value, void *ud) {
  (void)ud;
  uint8_t *pBlock = (uint8_t *)value;

  taosMemoryFree(pBlock);
}

int32_t tsdbCacheGetBlockS3(SLRUCache *pCache, STsdbFD *pFD, LRUHandle **handle) {
  int32_t code = 0;
  char    key[128] = {0};
  int     keyLen = 0;

  getBCacheKey(pFD->fid, pFD->cid, pFD->blkno, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (!h) {
    STsdb *pTsdb = pFD->pTsdb;
    taosThreadMutexLock(&pTsdb->bMutex);

    h = taosLRUCacheLookup(pCache, key, keyLen);
    if (!h) {
      uint8_t *pBlock = NULL;
      code = tsdbCacheLoadBlockS3(pFD, &pBlock);
      //  if table's empty or error, return code of -1
      if (code != TSDB_CODE_SUCCESS || pBlock == NULL) {
        taosThreadMutexUnlock(&pTsdb->bMutex);

        *handle = NULL;
        if (code == TSDB_CODE_SUCCESS && !pBlock) {
          code = TSDB_CODE_OUT_OF_MEMORY;
        }
        return code;
      }

      size_t              charge = tsS3BlockSize * pFD->szPage;
      _taos_lru_deleter_t deleter = deleteBCache;
      LRUStatus           status =
          taosLRUCacheInsert(pCache, key, keyLen, pBlock, charge, deleter, &h, TAOS_LRU_PRIORITY_LOW, NULL);
      if (status != TAOS_LRU_STATUS_OK) {
        code = -1;
      }
    }

    taosThreadMutexUnlock(&pTsdb->bMutex);
  }

  *handle = h;

  return code;
}

int32_t tsdbCacheGetPageS3(SLRUCache *pCache, STsdbFD *pFD, int64_t pgno, LRUHandle **handle) {
  int32_t code = 0;
  char    key[128] = {0};
  int     keyLen = 0;

  getBCacheKey(pFD->fid, pFD->cid, pgno, key, &keyLen);
  *handle = taosLRUCacheLookup(pCache, key, keyLen);

  return code;
}

int32_t tsdbCacheSetPageS3(SLRUCache *pCache, STsdbFD *pFD, int64_t pgno, uint8_t *pPage) {
  int32_t    code = 0;
  char       key[128] = {0};
  int        keyLen = 0;
  LRUHandle *handle = NULL;

  getBCacheKey(pFD->fid, pFD->cid, pgno, key, &keyLen);
  taosThreadMutexLock(&pFD->pTsdb->pgMutex);
  handle = taosLRUCacheLookup(pFD->pTsdb->pgCache, key, keyLen);
  if (!handle) {
    size_t              charge = pFD->szPage;
    _taos_lru_deleter_t deleter = deleteBCache;
    uint8_t            *pPg = taosMemoryMalloc(charge);
    memcpy(pPg, pPage, charge);

    LRUStatus status =
        taosLRUCacheInsert(pCache, key, keyLen, pPg, charge, deleter, &handle, TAOS_LRU_PRIORITY_LOW, NULL);
    if (status != TAOS_LRU_STATUS_OK) {
      // ignore cache updating if not ok
      // code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  taosThreadMutexUnlock(&pFD->pTsdb->pgMutex);

  tsdbCacheRelease(pFD->pTsdb->pgCache, handle);

  return code;
}
