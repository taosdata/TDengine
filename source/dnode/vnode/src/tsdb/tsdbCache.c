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

#define ROCKS_KEY_LEN (sizeof(tb_uid_t) + sizeof(int16_t) + sizeof(int8_t))

typedef struct {
  tb_uid_t uid;
  int16_t  cid;
  int8_t   ltype;
} SLastKey;

static void tsdbGetRocksPath(STsdb *pTsdb, char *path) {
  SVnode *pVnode = pTsdb->pVnode;
  if (pVnode->pTfs) {
    if (path) {
      snprintf(path, TSDB_FILENAME_LEN, "%s%s%s%scache.rdb", tfsGetPrimaryPath(pTsdb->pVnode->pTfs), TD_DIRSEP,
               pTsdb->path, TD_DIRSEP);
    }
  } else {
    if (path) {
      snprintf(path, TSDB_FILENAME_LEN, "%s%scache.rdb", pTsdb->path, TD_DIRSEP);
    }
  }
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
  rocksdb_writebatch_t *wb = NULL;
  if (read) {
    if (lock) {
      taosThreadMutexLock(&pTsdb->lruMutex);
    }
    wb = pTsdb->rCache.rwritebatch;
  } else {
    if (lock) {
      taosThreadMutexLock(&pTsdb->rCache.rMutex);
    }
    wb = pTsdb->rCache.writebatch;
  }
  int count = rocksdb_writebatch_count(wb);
  if ((force && count > 0) || count >= 1024) {
    char *err = NULL;
    rocksdb_write(pTsdb->rCache.db, pTsdb->rCache.writeoptions, wb, &err);
    if (NULL != err) {
      tsdbError("vgId:%d, %s failed at line %d, count: %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, count,
                err);
      rocksdb_free(err);
    }

    rocksdb_writebatch_clear(wb);
  }
  if (read) {
    if (lock) taosThreadMutexUnlock(&pTsdb->lruMutex);
  } else {
    if (lock) taosThreadMutexUnlock(&pTsdb->rCache.rMutex);
  }
}

int32_t tsdbCacheCommit(STsdb *pTsdb) {
  int32_t code = 0;
  char   *err = NULL;

  rocksMayWrite(pTsdb, true, false, true);
  rocksMayWrite(pTsdb, true, true, true);
  rocksdb_flush(pTsdb->rCache.db, pTsdb->rCache.flushoptions, &err);
  if (NULL != err) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, err);
    rocksdb_free(err);
    code = -1;
  }

  return code;
}

SLastCol *tsdbCacheDeserialize(char const *value) {
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

void tsdbCacheSerialize(SLastCol *pLastCol, char **value, size_t *size) {
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

static void reallocVarData(SColVal *pColVal) {
  if (IS_VAR_DATA_TYPE(pColVal->type)) {
    uint8_t *pVal = pColVal->value.pData;
    pColVal->value.pData = taosMemoryMalloc(pColVal->value.nData);
    if (pColVal->value.nData) {
      memcpy(pColVal->value.pData, pVal, pColVal->value.nData);
    }
  }
}

static void tsdbCacheDeleter(const void *key, size_t keyLen, void *value) {
  SLastCol *pLastCol = (SLastCol *)value;

  // TODO: add dirty flag to SLastCol
  if (pLastCol->dirty) {
    // TODO: queue into dirty list, free it after save to backstore
  } else {
    if (IS_VAR_DATA_TYPE(pLastCol->colVal.type) /* && pLastCol->colVal.value.nData > 0*/) {
      taosMemoryFree(pLastCol->colVal.value.pData);
    }

    taosMemoryFree(value);
  }
}

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
  int     num_keys = TARRAY_SIZE(aColVal);
  char  **keys_list = taosMemoryCalloc(num_keys * 2, sizeof(char *));
  size_t *keys_list_sizes = taosMemoryCalloc(num_keys * 2, sizeof(size_t));
  char   *key_list = taosMemoryMalloc(num_keys * ROCKS_KEY_LEN * 2);
  for (int i = 0; i < num_keys; ++i) {
    SColVal *pColVal = (SColVal *)taosArrayGet(aColVal, i);
    int16_t  cid = pColVal->cid;

    memcpy(key_list + i * ROCKS_KEY_LEN, &(SLastKey){.ltype = 1, .uid = uid, .cid = cid}, ROCKS_KEY_LEN);
    memcpy(key_list + i * ROCKS_KEY_LEN + num_keys * ROCKS_KEY_LEN, &(SLastKey){.ltype = 0, .uid = uid, .cid = cid},
           ROCKS_KEY_LEN);
    keys_list[i] = key_list + i * ROCKS_KEY_LEN;
    keys_list[num_keys + i] = key_list + i * ROCKS_KEY_LEN + num_keys * ROCKS_KEY_LEN;
    keys_list_sizes[i] = ROCKS_KEY_LEN;
    keys_list_sizes[num_keys + i] = ROCKS_KEY_LEN;
  }
  char  **values_list = taosMemoryCalloc(num_keys * 2, sizeof(char *));
  size_t *values_list_sizes = taosMemoryCalloc(num_keys * 2, sizeof(size_t));
  char  **errs = taosMemoryCalloc(num_keys * 2, sizeof(char *));
  taosThreadMutexLock(&pTsdb->rCache.rMutex);
  rocksdb_multi_get(pTsdb->rCache.db, pTsdb->rCache.readoptions, num_keys * 2, (const char *const *)keys_list,
                    keys_list_sizes, values_list, values_list_sizes, errs);
  for (int i = 0; i < num_keys * 2; ++i) {
    rocksdb_free(errs[i]);
  }
  taosMemoryFree(key_list);
  taosMemoryFree(keys_list);
  taosMemoryFree(keys_list_sizes);
  taosMemoryFree(errs);

  TSKEY                 keyTs = TSDBROW_TS(pRow);
  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  for (int i = 0; i < num_keys; ++i) {
    SColVal *pColVal = (SColVal *)taosArrayGet(aColVal, i);

    // if (!COL_VAL_IS_NONE(pColVal)) {
    SLastCol *pLastCol = tsdbCacheDeserialize(values_list[i + num_keys]);

    if (NULL == pLastCol || pLastCol->ts <= keyTs) {
      char  *value = NULL;
      size_t vlen = 0;
      tsdbCacheSerialize(&(SLastCol){.ts = keyTs, .colVal = *pColVal}, &value, &vlen);
      SLastKey key = (SLastKey){.ltype = 0, .uid = uid, .cid = pColVal->cid};
      size_t   klen = ROCKS_KEY_LEN;
      rocksdb_writebatch_put(wb, (char *)&key, klen, value, vlen);

      pLastCol = (SLastCol *)value;
      SLastCol *pTmpLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
      *pTmpLastCol = *pLastCol;
      pLastCol = pTmpLastCol;

      reallocVarData(&pLastCol->colVal);
      size_t charge = sizeof(*pLastCol);
      if (IS_VAR_DATA_TYPE(pLastCol->colVal.type)) {
        charge += pLastCol->colVal.value.nData;
      }

      LRUStatus status = taosLRUCacheInsert(pTsdb->lruCache, &key, ROCKS_KEY_LEN, pLastCol, charge, tsdbCacheDeleter,
                                            NULL, TAOS_LRU_PRIORITY_LOW);
      if (status != TAOS_LRU_STATUS_OK) {
        code = -1;
      }

      taosMemoryFree(value);
    }

    if (COL_VAL_IS_VALUE(pColVal)) {
      SLastCol *pLastCol = tsdbCacheDeserialize(values_list[i]);

      if (NULL == pLastCol || pLastCol->ts <= keyTs) {
        char  *value = NULL;
        size_t vlen = 0;
        tsdbCacheSerialize(&(SLastCol){.ts = keyTs, .colVal = *pColVal}, &value, &vlen);
        SLastKey key = (SLastKey){.ltype = 1, .uid = uid, .cid = pColVal->cid};

        rocksdb_writebatch_put(wb, (char *)&key, ROCKS_KEY_LEN, value, vlen);

        pLastCol = (SLastCol *)value;
        SLastCol *pTmpLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
        *pTmpLastCol = *pLastCol;
        pLastCol = pTmpLastCol;

        reallocVarData(&pLastCol->colVal);
        size_t charge = sizeof(*pLastCol);
        if (IS_VAR_DATA_TYPE(pLastCol->colVal.type)) {
          charge += pLastCol->colVal.value.nData;
        }

        LRUStatus status = taosLRUCacheInsert(pTsdb->lruCache, &key, ROCKS_KEY_LEN, pLastCol, charge, tsdbCacheDeleter,
                                              NULL, TAOS_LRU_PRIORITY_LOW);
        if (status != TAOS_LRU_STATUS_OK) {
          code = -1;
        }

        taosMemoryFree(value);
      }
    }
    //}

    rocksdb_free(values_list[i]);
    rocksdb_free(values_list[i + num_keys]);
  }
  taosMemoryFree(values_list);
  taosMemoryFree(values_list_sizes);

  rocksMayWrite(pTsdb, true, false, false);
  taosThreadMutexUnlock(&pTsdb->rCache.rMutex);

_exit:
  taosArrayDestroy(aColVal);
  taosMemoryFree(pTSchema);
  return code;
}

static int32_t mergeLastCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                            int nCols, int16_t *slotIds);

static int32_t mergeLastRowCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                               int nCols, int16_t *slotIds);
#if 1
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
#endif

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

typedef struct {
  int      idx;
  SLastKey key;
} SIdxKey;

static int32_t tsdbCacheLoadFromRaw(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SArray *remainCols,
                                    SCacheRowsReader *pr, int8_t ltype) {
  int32_t               code = 0;
  rocksdb_writebatch_t *wb = NULL;
  SArray               *pTmpColArray = NULL;
  int                   num_keys = TARRAY_SIZE(remainCols);
  int16_t              *aCols = taosMemoryMalloc(num_keys * sizeof(int16_t));
  int16_t              *slotIds = taosMemoryMalloc(num_keys * sizeof(int16_t));

  for (int i = 0; i < num_keys; ++i) {
    SIdxKey *idxKey = taosArrayGet(remainCols, i);
    aCols[i] = idxKey->key.cid;
    slotIds[i] = pr->pSlotIds[idxKey->idx];
  }

  if (ltype) {
    mergeLastCid(uid, pTsdb, &pTmpColArray, pr, aCols, num_keys, slotIds);
  } else {
    mergeLastRowCid(uid, pTsdb, &pTmpColArray, pr, aCols, num_keys, slotIds);
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
                                          TAOS_LRU_PRIORITY_LOW);
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

  taosArrayDestroy(pTmpColArray);

  taosMemoryFree(aCols);
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
                                            NULL, TAOS_LRU_PRIORITY_LOW);
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

    LRUHandle *h = taosLRUCacheLookup(pCache, key, ROCKS_KEY_LEN);
    if (h) {
      SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pCache, h);

      SLastCol lastCol = *pLastCol;
      reallocVarData(&lastCol.colVal);
      taosArrayPush(pLastArray, &lastCol);

      if (h) {
        taosLRUCacheRelease(pCache, h, false);
      }
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

        if (h) {
          taosLRUCacheRelease(pCache, h, false);
        }

        taosArrayRemove(remainCols, i);
      } else {
        ++i;
      }
    }

    code = tsdbCacheLoadFromRocks(pTsdb, uid, pLastArray, remainCols, pr, ltype);

    taosThreadMutexUnlock(&pTsdb->lruMutex);
  }

  if (remainCols) {
    taosArrayDestroy(remainCols);
  }

  return code;
}

int32_t tsdbCacheGet(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SCacheRowsReader *pr, int8_t ltype) {
  int32_t    code = 0;
  SLRUCache *pCache = pTsdb->lruCache;
  SArray    *pCidList = pr->pCidList;
  int        num_keys = TARRAY_SIZE(pCidList);

  for (int i = 0; i < num_keys; ++i) {
    SLastCol *pLastCol = NULL;
    int16_t   cid = *(int16_t *)taosArrayGet(pCidList, i);

    SLastKey  *key = &(SLastKey){.ltype = ltype, .uid = uid, .cid = cid};
    LRUHandle *h = taosLRUCacheLookup(pCache, key, ROCKS_KEY_LEN);
    if (!h) {
      taosThreadMutexLock(&pTsdb->lruMutex);
      h = taosLRUCacheLookup(pCache, key, ROCKS_KEY_LEN);
      if (!h) {
        pLastCol = tsdbCacheLoadCol(pTsdb, pr, pr->pSlotIds[i], uid, cid, ltype);

        size_t charge = sizeof(*pLastCol);
        if (IS_VAR_DATA_TYPE(pLastCol->colVal.type)) {
          charge += pLastCol->colVal.value.nData;
        }

        LRUStatus status = taosLRUCacheInsert(pCache, key, ROCKS_KEY_LEN, pLastCol, charge, tsdbCacheDeleter, &h,
                                              TAOS_LRU_PRIORITY_LOW);
        if (status != TAOS_LRU_STATUS_OK) {
          code = -1;
        }
      }

      taosThreadMutexUnlock(&pTsdb->lruMutex);
    }

    pLastCol = (SLastCol *)taosLRUCacheValue(pCache, h);

    SLastCol lastCol = *pLastCol;
    reallocVarData(&lastCol.colVal);

    if (h) {
      taosLRUCacheRelease(pCache, h, false);
    }

    taosArrayPush(pLastArray, &lastCol);
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
  taosThreadMutexLock(&pTsdb->rCache.rMutex);
  rocksMayWrite(pTsdb, true, false, false);
  rocksdb_multi_get(pTsdb->rCache.db, pTsdb->rCache.readoptions, num_keys * 2, (const char *const *)keys_list,
                    keys_list_sizes, values_list, values_list_sizes, errs);
  for (int i = 0; i < num_keys * 2; ++i) {
    if (errs[i]) {
      rocksdb_free(errs[i]);
    }
  }
  taosMemoryFree(errs);

  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  for (int i = 0; i < num_keys; ++i) {
    SLastCol *pLastCol = tsdbCacheDeserialize(values_list[i]);
    if (NULL != pLastCol && (pLastCol->ts <= eKey && pLastCol->ts >= sKey)) {
      rocksdb_writebatch_delete(wb, keys_list[i], klen);
    }
    taosLRUCacheErase(pTsdb->lruCache, keys_list[i], klen);

    pLastCol = tsdbCacheDeserialize(values_list[i + num_keys]);
    if (NULL != pLastCol && (pLastCol->ts <= eKey && pLastCol->ts >= sKey)) {
      rocksdb_writebatch_delete(wb, keys_list[num_keys + i], klen);
    }
    taosLRUCacheErase(pTsdb->lruCache, keys_list[num_keys + i], klen);

    rocksdb_free(values_list[i]);
    rocksdb_free(values_list[i + num_keys]);
  }
  for (int i = 0; i < num_keys; ++i) {
    taosMemoryFree(keys_list[i]);
  }
  taosMemoryFree(keys_list);
  taosMemoryFree(keys_list_sizes);
  taosMemoryFree(values_list);
  taosMemoryFree(values_list_sizes);

  rocksMayWrite(pTsdb, true, false, false);
  taosThreadMutexUnlock(&pTsdb->rCache.rMutex);

_exit:
  taosMemoryFree(pTSchema);

  return code;
}

int32_t tsdbOpenCache(STsdb *pTsdb) {
  int32_t    code = 0;
  SLRUCache *pCache = NULL;
  size_t     cfgCapacity = pTsdb->pVnode->config.cacheLastSize * 1024 * 1024;

  pCache = taosLRUCacheInit(cfgCapacity, 1, .5);
  if (pCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  code = tsdbOpenBICache(pTsdb);
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

  tsdbCloseBICache(pTsdb);
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

static void deleteTableCacheLast(const void *key, size_t keyLen, void *value) {
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
/*
int32_t tsdbCacheDelete(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

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

    if (invalidate) {
      taosLRUCacheRelease(pCache, h, true);
    } else {
      taosLRUCacheRelease(pCache, h, false);
    }
  }

  // getTableCacheKey(uid, "l", key, &keyLen);
  getTableCacheKey(uid, 1, key, &keyLen);
  h = taosLRUCacheLookup(pCache, key, keyLen);
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

    if (invalidate) {
      taosLRUCacheRelease(pCache, h, true);
    } else {
      taosLRUCacheRelease(pCache, h, false);
    }
    // void taosLRUCacheErase(SLRUCache * cache, const void *key, size_t keyLen);
  }

  return code;
}
*/
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

static tb_uid_t getTableSuidByUid(tb_uid_t uid, STsdb *pTsdb) {
  tb_uid_t suid = 0;

  SMetaReader mr = {0};
  metaReaderInit(&mr, pTsdb->pVnode->pMeta, 0);
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
/*
static int32_t getTableDelIdx(SDelFReader *pDelFReader, tb_uid_t suid, tb_uid_t uid, SDelIdx *pDelIdx) {
  int32_t code = 0;
  SArray *pDelIdxArray = NULL;

  // SMapData delIdxMap;
  pDelIdxArray = taosArrayInit(32, sizeof(SDelIdx));
  SDelIdx idx = {.suid = suid, .uid = uid};

  // tMapDataReset(&delIdxMap);
  code = tsdbReadDelIdx(pDelFReader, pDelIdxArray);
  if (code) goto _err;

  // code = tMapDataSearch(&delIdxMap, &idx, tGetDelIdx, tCmprDelIdx, pDelIdx);
  SDelIdx *pIdx = taosArraySearch(pDelIdxArray, &idx, tCmprDelIdx, TD_EQ);

  *pDelIdx = *pIdx;

_err:
  if (pDelIdxArray) {
    taosArrayDestroy(pDelIdxArray);
  }
  return code;
}
*/
typedef enum {
  SFSLASTNEXTROW_FS,
  SFSLASTNEXTROW_FILESET,
  SFSLASTNEXTROW_BLOCKDATA,
  SFSLASTNEXTROW_BLOCKROW
} SFSLASTNEXTROWSTATES;

typedef struct {
  SFSLASTNEXTROWSTATES state;     // [input]
  STsdb               *pTsdb;     // [input]
  STSchema            *pTSchema;  // [input]
  tb_uid_t             suid;
  tb_uid_t             uid;
  int32_t              nFileSet;
  int32_t              iFileSet;
  SArray              *aDFileSet;
  SDataFReader       **pDataFReader;
  TSDBROW              row;

  bool               checkRemainingRow;
  SMergeTree         mergeTree;
  SMergeTree        *pMergeTree;
  SSttBlockLoadInfo *pLoadInfo;
  SLDataIter        *pDataIter;
  int64_t            lastTs;
} SFSLastNextRowIter;

static int32_t getNextRowFromFSLast(void *iter, TSDBROW **ppRow, bool *pIgnoreEarlierTs, bool isLast, int16_t *aCols,
                                    int nCols) {
  SFSLastNextRowIter *state = (SFSLastNextRowIter *)iter;
  int32_t             code = 0;
  bool                checkRemainingRow = true;

  switch (state->state) {
    case SFSLASTNEXTROW_FS:
      state->nFileSet = taosArrayGetSize(state->aDFileSet);
      state->iFileSet = state->nFileSet;

    case SFSLASTNEXTROW_FILESET: {
      SDFileSet *pFileSet = NULL;
    _next_fileset:
      if (state->pMergeTree != NULL) {
        tMergeTreeClose(state->pMergeTree);
        state->pMergeTree = NULL;
      }

      if (--state->iFileSet >= 0) {
        pFileSet = (SDFileSet *)taosArrayGet(state->aDFileSet, state->iFileSet);
      } else {
        *ppRow = NULL;
        return code;
      }

      if (*state->pDataFReader == NULL || (*state->pDataFReader)->pSet->fid != pFileSet->fid) {
        if (*state->pDataFReader != NULL) {
          tsdbDataFReaderClose(state->pDataFReader);

          resetLastBlockLoadInfo(state->pLoadInfo);
        }

        code = tsdbDataFReaderOpen(state->pDataFReader, state->pTsdb, pFileSet);
        if (code) goto _err;
      }

      int  nTmpCols = nCols;
      bool hasTs = false;
      if (aCols[0] == PRIMARYKEY_TIMESTAMP_COL_ID) {
        --nTmpCols;
        hasTs = true;
      }
      for (int i = 0; i < state->pLoadInfo->numOfStt; ++i) {
        state->pLoadInfo[i].colIds = hasTs ? aCols + 1 : aCols;
        state->pLoadInfo[i].numOfCols = nTmpCols;
        state->pLoadInfo[i].isLast = isLast;
      }
      tMergeTreeOpen(&state->mergeTree, 1, *state->pDataFReader, state->suid, state->uid,
                     &(STimeWindow){.skey = state->lastTs, .ekey = TSKEY_MAX},
                     &(SVersionRange){.minVer = 0, .maxVer = UINT64_MAX}, state->pLoadInfo, false, NULL, true,
                     state->pDataIter);
      state->pMergeTree = &state->mergeTree;
      state->state = SFSLASTNEXTROW_BLOCKROW;
    }
    case SFSLASTNEXTROW_BLOCKROW: {
      if (nCols != state->pLoadInfo->numOfCols) {
        for (int i = 0; i < state->pLoadInfo->numOfStt; ++i) {
          state->pLoadInfo[i].numOfCols = nCols;

          state->pLoadInfo[i].checkRemainingRow = state->checkRemainingRow;
        }
      }
      bool hasVal = tMergeTreeNext(&state->mergeTree);
      if (!hasVal) {
        if (tMergeTreeIgnoreEarlierTs(&state->mergeTree)) {
          *pIgnoreEarlierTs = true;
          *ppRow = NULL;
          return code;
        }
        state->state = SFSLASTNEXTROW_FILESET;
        goto _next_fileset;
      }
      state->row = *tMergeTreeGetRow(&state->mergeTree);
      *ppRow = &state->row;

      if (TSDBROW_TS(&state->row) <= state->lastTs) {
        *pIgnoreEarlierTs = true;
        *ppRow = NULL;
        return code;
      }

      *pIgnoreEarlierTs = false;
      if (!hasVal) {
        state->state = SFSLASTNEXTROW_FILESET;
      }

      if (!state->checkRemainingRow) {
        state->checkRemainingRow = true;
      }
      return code;
    }
    default:
      ASSERT(0);
      break;
  }

_err:
  /*if (state->pDataFReader) {
    tsdbDataFReaderClose(&state->pDataFReader);
    state->pDataFReader = NULL;
    }*/
  if (state->pMergeTree != NULL) {
    tMergeTreeClose(state->pMergeTree);
    state->pMergeTree = NULL;
  }

  *ppRow = NULL;

  return code;
}

int32_t clearNextRowFromFSLast(void *iter) {
  SFSLastNextRowIter *state = (SFSLastNextRowIter *)iter;
  int32_t             code = 0;

  if (!state) {
    return code;
  }
  /*
  if (state->pDataFReader) {
    tsdbDataFReaderClose(&state->pDataFReader);
    state->pDataFReader = NULL;
  }
  */
  if (state->pMergeTree != NULL) {
    tMergeTreeClose(state->pMergeTree);
    state->pMergeTree = NULL;
  }

  return code;
}

typedef enum SFSNEXTROWSTATES {
  SFSNEXTROW_FS,
  SFSNEXTROW_FILESET,
  SFSNEXTROW_BLOCKDATA,
  SFSNEXTROW_BLOCKROW
} SFSNEXTROWSTATES;

typedef struct SFSNextRowIter {
  SFSNEXTROWSTATES   state;         // [input]
  STsdb             *pTsdb;         // [input]
  SBlockIdx         *pBlockIdxExp;  // [input]
  STSchema          *pTSchema;      // [input]
  tb_uid_t           suid;
  tb_uid_t           uid;
  int32_t            nFileSet;
  int32_t            iFileSet;
  SArray            *aDFileSet;
  SDataFReader     **pDataFReader;
  SArray            *aBlockIdx;
  LRUHandle         *aBlockIdxHandle;
  SBlockIdx         *pBlockIdx;
  SMapData           blockMap;
  int32_t            nBlock;
  int32_t            iBlock;
  SDataBlk           block;
  SBlockData         blockData;
  SBlockData        *pBlockData;
  int32_t            nRow;
  int32_t            iRow;
  TSDBROW            row;
  SSttBlockLoadInfo *pLoadInfo;
  int64_t            lastTs;
} SFSNextRowIter;

static int32_t getNextRowFromFS(void *iter, TSDBROW **ppRow, bool *pIgnoreEarlierTs, bool isLast, int16_t *aCols,
                                int nCols) {
  SFSNextRowIter *state = (SFSNextRowIter *)iter;
  int32_t         code = 0;
  bool            checkRemainingRow = true;

  switch (state->state) {
    case SFSNEXTROW_FS:
      // state->aDFileSet = state->pTsdb->pFS->cState->aDFileSet;
      state->nFileSet = taosArrayGetSize(state->aDFileSet);
      state->iFileSet = state->nFileSet;

      state->pBlockData = NULL;

    case SFSNEXTROW_FILESET: {
      SDFileSet *pFileSet = NULL;
    _next_fileset:
      if (--state->iFileSet >= 0) {
        pFileSet = (SDFileSet *)taosArrayGet(state->aDFileSet, state->iFileSet);
      } else {
        // tBlockDataDestroy(&state->blockData, 1);
        if (state->pBlockData) {
          tBlockDataDestroy(state->pBlockData);
          state->pBlockData = NULL;
        }

        *ppRow = NULL;
        return code;
      }

      if (*state->pDataFReader == NULL || (*state->pDataFReader)->pSet->fid != pFileSet->fid) {
        if (*state->pDataFReader != NULL) {
          tsdbDataFReaderClose(state->pDataFReader);

          // resetLastBlockLoadInfo(state->pLoadInfo);
        }

        code = tsdbDataFReaderOpen(state->pDataFReader, state->pTsdb, pFileSet);
        if (code) goto _err;
      }

      // tMapDataReset(&state->blockIdxMap);
      /*
      if (!state->aBlockIdx) {
        state->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
      } else {
        taosArrayClear(state->aBlockIdx);
      }
      code = tsdbReadBlockIdx(*state->pDataFReader, state->aBlockIdx);
      if (code) goto _err;
      */
      int32_t code = tsdbCacheGetBlockIdx(state->pTsdb->biCache, *state->pDataFReader, &state->aBlockIdxHandle);
      if (code != TSDB_CODE_SUCCESS || state->aBlockIdxHandle == NULL) {
        goto _err;
      }
      state->aBlockIdx = (SArray *)taosLRUCacheValue(state->pTsdb->biCache, state->aBlockIdxHandle);

      /* if (state->pBlockIdx) { */
      /* } */
      /* code = tMapDataSearch(&state->blockIdxMap, state->pBlockIdxExp, tGetBlockIdx, tCmprBlockIdx,
       * &state->blockIdx);
       */
      state->pBlockIdx = taosArraySearch(state->aBlockIdx, state->pBlockIdxExp, tCmprBlockIdx, TD_EQ);
      if (!state->pBlockIdx) {
        tsdbBICacheRelease(state->pTsdb->biCache, state->aBlockIdxHandle);

        state->aBlockIdxHandle = NULL;
        state->aBlockIdx = NULL;
        /*
         tsdbDataFReaderClose(state->pDataFReader);
         *state->pDataFReader = NULL;
         resetLastBlockLoadInfo(state->pLoadInfo);*/
        goto _next_fileset;
      }

      tMapDataReset(&state->blockMap);
      /*
      if (state->blockMap.pData != NULL) {
        tMapDataClear(&state->blockMap);
      }
      */
      code = tsdbReadDataBlk(*state->pDataFReader, state->pBlockIdx, &state->blockMap);
      if (code) goto _err;

      state->nBlock = state->blockMap.nItem;
      state->iBlock = state->nBlock - 1;

      if (!state->pBlockData) {
        state->pBlockData = &state->blockData;

        code = tBlockDataCreate(&state->blockData);
        if (code) goto _err;
      }
    }
    case SFSNEXTROW_BLOCKDATA:
    _next_datablock:
      if (state->iBlock >= 0) {
        SDataBlk block = {0};
        bool     skipBlock = true;
        int      inputColIndex = 0;

        tDataBlkReset(&block);
        tBlockDataReset(state->pBlockData);

        tMapDataGetItemByIdx(&state->blockMap, state->iBlock, &block, tGetDataBlk);
        if (block.maxKey.ts <= state->lastTs) {
          *pIgnoreEarlierTs = true;
          if (state->pBlockData) {
            tBlockDataDestroy(state->pBlockData);
            state->pBlockData = NULL;
          }

          *ppRow = NULL;
          return code;
        }
        *pIgnoreEarlierTs = false;
        tBlockDataReset(state->pBlockData);
        TABLEID tid = {.suid = state->suid, .uid = state->uid};
        int     nTmpCols = nCols;
        bool    hasTs = false;
        if (aCols[0] == PRIMARYKEY_TIMESTAMP_COL_ID) {
          --nTmpCols;
          skipBlock = false;
          hasTs = true;
        }
        code = tBlockDataInit(state->pBlockData, &tid, state->pTSchema, hasTs ? aCols + 1 : aCols, nTmpCols);
        if (code) goto _err;

        code = tsdbReadDataBlock(*state->pDataFReader, &block, state->pBlockData);
        if (code) goto _err;

        for (int colIndex = 0; colIndex < state->pBlockData->nColData; ++colIndex) {
          SColData *pColData = &state->pBlockData->aColData[colIndex];

          if (isLast && (pColData->flag & HAS_VALUE)) {
            skipBlock = false;
            break;
          } /*else if (pColData->flag & (HAS_VALUE | HAS_NULL)) {
            skipBlock = false;
            break;
            }*/
        }

        if (!isLast) {
          skipBlock = false;
        }

        if (skipBlock) {
          if (--state->iBlock < 0) {
            tsdbDataFReaderClose(state->pDataFReader);
            *state->pDataFReader = NULL;
            // resetLastBlockLoadInfo(state->pLoadInfo);

            if (state->aBlockIdx) {
              // taosArrayDestroy(state->aBlockIdx);
              tsdbBICacheRelease(state->pTsdb->biCache, state->aBlockIdxHandle);

              state->aBlockIdxHandle = NULL;
              state->aBlockIdx = NULL;
            }

            state->state = SFSNEXTROW_FILESET;
            goto _next_fileset;
          } else {
            goto _next_datablock;
          }
        }

        state->nRow = state->blockData.nRow;
        state->iRow = state->nRow - 1;

        state->state = SFSNEXTROW_BLOCKROW;
        checkRemainingRow = false;
      }
    case SFSNEXTROW_BLOCKROW: {
      if (checkRemainingRow) {
        bool skipBlock = true;
        int  inputColIndex = 0;
        if (aCols[0] == PRIMARYKEY_TIMESTAMP_COL_ID) {
          ++inputColIndex;
        }
        for (int colIndex = 0; colIndex < state->pBlockData->nColData; ++colIndex) {
          SColData *pColData = &state->pBlockData->aColData[colIndex];
          int16_t   cid = pColData->cid;

          if (inputColIndex < nCols && cid == aCols[inputColIndex]) {
            if (isLast && (pColData->flag & HAS_VALUE)) {
              skipBlock = false;
              break;
            } /*else if (pColData->flag & (HAS_VALUE | HAS_NULL)) {
              skipBlock = false;
              break;
              }*/

            ++inputColIndex;
          }
        }

        if (!isLast) {
          skipBlock = false;
        }

        if (skipBlock) {
          if (--state->iBlock < 0) {
            tsdbDataFReaderClose(state->pDataFReader);
            *state->pDataFReader = NULL;
            // resetLastBlockLoadInfo(state->pLoadInfo);

            if (state->aBlockIdx) {
              // taosArrayDestroy(state->aBlockIdx);
              tsdbBICacheRelease(state->pTsdb->biCache, state->aBlockIdxHandle);

              state->aBlockIdxHandle = NULL;
              state->aBlockIdx = NULL;
            }

            state->state = SFSNEXTROW_FILESET;
            goto _next_fileset;
          } else {
            goto _next_datablock;
          }
        }
      }

      if (state->iRow >= 0) {
        state->row = tsdbRowFromBlockData(state->pBlockData, state->iRow);
        *ppRow = &state->row;

        if (--state->iRow < 0) {
          state->state = SFSNEXTROW_BLOCKDATA;
          if (--state->iBlock < 0) {
            tsdbDataFReaderClose(state->pDataFReader);
            *state->pDataFReader = NULL;
            // resetLastBlockLoadInfo(state->pLoadInfo);

            if (state->aBlockIdx) {
              // taosArrayDestroy(state->aBlockIdx);
              tsdbBICacheRelease(state->pTsdb->biCache, state->aBlockIdxHandle);

              state->aBlockIdxHandle = NULL;
              state->aBlockIdx = NULL;
            }

            state->state = SFSNEXTROW_FILESET;
          }
        }
      }

      return code;
    }
    default:
      ASSERT(0);
      break;
  }

_err:
  /*
  if (*state->pDataFReader) {
    tsdbDataFReaderClose(state->pDataFReader);
    *state->pDataFReader = NULL;
    resetLastBlockLoadInfo(state->pLoadInfo);
    }*/
  if (state->aBlockIdx) {
    // taosArrayDestroy(state->aBlockIdx);
    tsdbBICacheRelease(state->pTsdb->biCache, state->aBlockIdxHandle);

    state->aBlockIdxHandle = NULL;
    state->aBlockIdx = NULL;
  }
  if (state->pBlockData) {
    tBlockDataDestroy(state->pBlockData);
    state->pBlockData = NULL;
  }

  *ppRow = NULL;

  return code;
}

int32_t clearNextRowFromFS(void *iter) {
  int32_t code = 0;

  SFSNextRowIter *state = (SFSNextRowIter *)iter;
  if (!state) {
    return code;
  }
  /*
  if (state->pDataFReader) {
    tsdbDataFReaderClose(&state->pDataFReader);
    state->pDataFReader = NULL;
    }*/
  if (state->aBlockIdx) {
    // taosArrayDestroy(state->aBlockIdx);
    tsdbBICacheRelease(state->pTsdb->biCache, state->aBlockIdxHandle);

    state->aBlockIdxHandle = NULL;
    state->aBlockIdx = NULL;
  }
  if (state->pBlockData) {
    // tBlockDataDestroy(&state->blockData, 1);
    tBlockDataDestroy(state->pBlockData);
    state->pBlockData = NULL;
  }

  if (state->blockMap.pData != NULL) {
    tMapDataClear(&state->blockMap);
  }

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
  // bool              iterOpened;
  // TSDBROW          *curRow;
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

/* static int32_t tsRowFromTsdbRow(STSchema *pTSchema, TSDBROW *pRow, STSRow **ppRow) { */
/*   int32_t code = 0; */

/*   SColVal *pColVal = &(SColVal){0}; */

/*   if (pRow->type == 0) { */
/*     *ppRow = tdRowDup(pRow->pTSRow); */
/*   } else { */
/*     SArray *pArray = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal)); */
/*     if (pArray == NULL) { */
/*       code = TSDB_CODE_OUT_OF_MEMORY; */
/*       goto _exit; */
/*     } */

/*     TSDBKEY   key = TSDBROW_KEY(pRow); */
/*     STColumn *pTColumn = &pTSchema->columns[0]; */
/*     *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.ts = key.ts}); */

/*     if (taosArrayPush(pArray, pColVal) == NULL) { */
/*       code = TSDB_CODE_OUT_OF_MEMORY; */
/*       goto _exit; */
/*     } */

/*     for (int16_t iCol = 1; iCol < pTSchema->numOfCols; iCol++) { */
/*       tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal); */
/*       if (taosArrayPush(pArray, pColVal) == NULL) { */
/*         code = TSDB_CODE_OUT_OF_MEMORY; */
/*         goto _exit; */
/*       } */
/*     } */

/*     code = tdSTSRowNew(pArray, pTSchema, ppRow); */
/*     if (code) goto _exit; */
/*   } */

/* _exit: */
/*   return code; */
/* } */

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

typedef struct {
  SArray *pSkyline;
  int64_t iSkyline;

  SBlockIdx          idx;
  SMemNextRowIter    memState;
  SMemNextRowIter    imemState;
  SFSLastNextRowIter fsLastState;
  SFSNextRowIter     fsState;
  TSDBROW            memRow, imemRow, fsLastRow, fsRow;

  TsdbNextRowState input[4];
  STsdb           *pTsdb;
} CacheNextRowIter;

static int32_t nextRowIterOpen(CacheNextRowIter *pIter, tb_uid_t uid, STsdb *pTsdb, STSchema *pTSchema, tb_uid_t suid,
                               SSttBlockLoadInfo *pLoadInfo, SLDataIter *pLDataIter, STsdbReadSnap *pReadSnap,
                               SDataFReader **pDataFReader, SDataFReader **pDataFReaderLast, int64_t lastTs) {
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

  pIter->pSkyline = taosArrayInit(32, sizeof(TSDBKEY));

  SDelFile *pDelFile = pReadSnap->fs.pDelFile;
  if (pDelFile) {
    SDelFReader *pDelFReader;

    code = tsdbDelFReaderOpen(&pDelFReader, pDelFile, pTsdb);
    if (code) goto _err;

    SArray *pDelIdxArray = taosArrayInit(32, sizeof(SDelIdx));

    code = tsdbReadDelIdx(pDelFReader, pDelIdxArray);
    if (code) {
      taosArrayDestroy(pDelIdxArray);
      tsdbDelFReaderClose(&pDelFReader);
      goto _err;
    }

    SDelIdx *delIdx = taosArraySearch(pDelIdxArray, &(SDelIdx){.suid = suid, .uid = uid}, tCmprDelIdx, TD_EQ);

    code = getTableDelSkyline(pMem, pIMem, pDelFReader, delIdx, pIter->pSkyline);
    if (code) {
      taosArrayDestroy(pDelIdxArray);
      tsdbDelFReaderClose(&pDelFReader);
      goto _err;
    }

    taosArrayDestroy(pDelIdxArray);
    tsdbDelFReaderClose(&pDelFReader);
  } else {
    code = getTableDelSkyline(pMem, pIMem, NULL, NULL, pIter->pSkyline);
    if (code) goto _err;
  }

  pIter->iSkyline = taosArrayGetSize(pIter->pSkyline) - 1;

  pIter->idx = (SBlockIdx){.suid = suid, .uid = uid};

  pIter->fsLastState.state = (SFSLASTNEXTROWSTATES)SFSNEXTROW_FS;
  pIter->fsLastState.pTsdb = pTsdb;
  pIter->fsLastState.aDFileSet = pReadSnap->fs.aDFileSet;
  pIter->fsLastState.pTSchema = pTSchema;
  pIter->fsLastState.suid = suid;
  pIter->fsLastState.uid = uid;
  pIter->fsLastState.pLoadInfo = pLoadInfo;
  pIter->fsLastState.pDataFReader = pDataFReaderLast;
  pIter->fsLastState.lastTs = lastTs;
  pIter->fsLastState.pDataIter = pLDataIter;

  pIter->fsState.state = SFSNEXTROW_FS;
  pIter->fsState.pTsdb = pTsdb;
  pIter->fsState.aDFileSet = pReadSnap->fs.aDFileSet;
  pIter->fsState.pBlockIdxExp = &pIter->idx;
  pIter->fsState.pTSchema = pTSchema;
  pIter->fsState.suid = suid;
  pIter->fsState.uid = uid;
  pIter->fsState.pLoadInfo = pLoadInfo;
  pIter->fsState.pDataFReader = pDataFReader;
  pIter->fsState.lastTs = lastTs;

  pIter->input[0] = (TsdbNextRowState){&pIter->memRow, true, false, false, &pIter->memState, getNextRowFromMem, NULL};
  pIter->input[1] = (TsdbNextRowState){&pIter->imemRow, true, false, false, &pIter->imemState, getNextRowFromMem, NULL};
  pIter->input[2] = (TsdbNextRowState){
      &pIter->fsLastRow, false, true, false, &pIter->fsLastState, getNextRowFromFSLast, clearNextRowFromFSLast};
  pIter->input[3] =
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

  return code;
_err:
  return code;
}

static int32_t nextRowIterClose(CacheNextRowIter *pIter) {
  int code = 0;

  for (int i = 0; i < 4; ++i) {
    if (pIter->input[i].nextRowClearFn) {
      pIter->input[i].nextRowClearFn(pIter->input[i].iter);
    }
  }

  if (pIter->pSkyline) {
    taosArrayDestroy(pIter->pSkyline);
  }

_err:
  return code;
}

// iterate next row non deleted backward ts, version (from high to low)
static int32_t nextRowIterGet(CacheNextRowIter *pIter, TSDBROW **ppRow, bool *pIgnoreEarlierTs, bool isLast,
                              int16_t *aCols, int nCols) {
  int code = 0;
  for (;;) {
    for (int i = 0; i < 4; ++i) {
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

    if (pIter->input[0].stop && pIter->input[1].stop && pIter->input[2].stop && pIter->input[3].stop) {
      *ppRow = NULL;
      *pIgnoreEarlierTs = (pIter->input[0].ignoreEarlierTs || pIter->input[1].ignoreEarlierTs ||
                           pIter->input[2].ignoreEarlierTs || pIter->input[3].ignoreEarlierTs);
      return code;
    }

    // select maxpoint(s) from mem, imem, fs and last
    TSDBROW *max[4] = {0};
    int      iMax[4] = {-1, -1, -1, -1};
    int      nMax = 0;
    TSKEY    maxKey = TSKEY_MIN;

    for (int i = 0; i < 4; ++i) {
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
  return metaGetTbTSchemaEx(pReader->pTsdb->pVnode->pMeta, pReader->suid, uid, sversion, &pReader->pCurrSchema);
}

static int32_t mergeLastRow(tb_uid_t uid, STsdb *pTsdb, bool *dup, SArray **ppColArray, SCacheRowsReader *pr) {
  STSchema *pTSchema = pr->pSchema;  // metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1, 1);
  int16_t   nLastCol = pTSchema->numOfCols;
  int16_t   iCol = 0;
  int16_t   noneCol = 0;
  bool      setNoneCol = false;
  bool      hasRow = false;
  bool      ignoreEarlierTs = false;
  SArray   *pColArray = NULL;
  SColVal  *pColVal = &(SColVal){0};

  int32_t code = initLastColArray(pTSchema, &pColArray);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  TSKEY lastRowTs = TSKEY_MAX;

  CacheNextRowIter iter = {0};
  nextRowIterOpen(&iter, uid, pTsdb, pTSchema, pr->suid, pr->pLoadInfo, pr->pDataIter, pr->pReadSnap, &pr->pDataFReader,
                  &pr->pDataFReaderLast, pr->lastTs);

  do {
    TSDBROW *pRow = NULL;
    nextRowIterGet(&iter, &pRow, &ignoreEarlierTs, false, NULL, 0);

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
    int16_t nCol = pTSchema->numOfCols;

    TSKEY rowTs = TSDBROW_TS(pRow);

    if (lastRowTs == TSKEY_MAX) {
      lastRowTs = rowTs;
      STColumn *pTColumn = &pTSchema->columns[0];

      *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.val = lastRowTs});
      taosArraySet(pColArray, 0, &(SLastCol){.ts = lastRowTs, .colVal = *pColVal});

      for (iCol = 1; iCol < nCol; ++iCol) {
        if (iCol >= nLastCol) {
          break;
        }
        SLastCol *pCol = taosArrayGet(pColArray, iCol);
        if (pCol->colVal.cid != pTSchema->columns[iCol].colId) {
          continue;
        }
        tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal);

        *pCol = (SLastCol){.ts = lastRowTs, .colVal = *pColVal};
        if (IS_VAR_DATA_TYPE(pColVal->type) && pColVal->value.nData > 0) {
          pCol->colVal.value.pData = taosMemoryMalloc(pCol->colVal.value.nData);
          if (pCol->colVal.value.pData == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _err;
          }
          memcpy(pCol->colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
        }

        if (COL_VAL_IS_NONE(pColVal) && !setNoneCol) {
          noneCol = iCol;
          setNoneCol = true;
        }
      }
      if (!setNoneCol) {
        // done, goto return pColArray
        break;
      } else {
        continue;
      }
    }

    if ((rowTs < lastRowTs)) {
      // done, goto return pColArray
      break;
    }

    // merge into pColArray
    setNoneCol = false;
    for (iCol = noneCol; iCol < nCol; ++iCol) {
      // high version's column value
      SColVal *tColVal = (SColVal *)taosArrayGet(pColArray, iCol);

      tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal);
      if (COL_VAL_IS_NONE(tColVal) && !COL_VAL_IS_NONE(pColVal)) {
        SLastCol lastCol = {.ts = rowTs, .colVal = *pColVal};
        if (IS_VAR_DATA_TYPE(pColVal->type) && pColVal->value.nData > 0) {
          SLastCol *pLastCol = (SLastCol *)taosArrayGet(pColArray, iCol);
          taosMemoryFree(pLastCol->colVal.value.pData);

          lastCol.colVal.value.pData = taosMemoryMalloc(lastCol.colVal.value.nData);
          if (lastCol.colVal.value.pData == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _err;
          }
          memcpy(lastCol.colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
        }

        taosArraySet(pColArray, iCol, &lastCol);
      } else if (COL_VAL_IS_NONE(tColVal) && COL_VAL_IS_NONE(pColVal) && !setNoneCol) {
        noneCol = iCol;
        setNoneCol = true;
      }
    }
  } while (setNoneCol);

  // build the result ts row here
  *dup = false;
  // if (taosArrayGetSize(pColArray) != nCol) {
  //*ppColArray = NULL;
  // taosArrayDestroy(pColArray);
  //} else {
  if (!hasRow) {
    if (ignoreEarlierTs) {
      taosArrayDestroy(pColArray);
      pColArray = NULL;
    } else {
      taosArrayClear(pColArray);
    }
  }
  *ppColArray = pColArray;
  //}

  nextRowIterClose(&iter);
  // taosMemoryFreeClear(pTSchema);
  return code;

_err:
  nextRowIterClose(&iter);
  taosArrayDestroy(pColArray);
  // taosMemoryFreeClear(pTSchema);
  return code;
}

static int32_t mergeLast(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr) {
  STSchema *pTSchema = pr->pSchema;  // metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1, 1);
  int16_t   nLastCol = pTSchema->numOfCols;
  int16_t   noneCol = 0;
  bool      setNoneCol = false;
  bool      hasRow = false;
  bool      ignoreEarlierTs = false;
  SArray   *pColArray = NULL;
  SColVal  *pColVal = &(SColVal){0};
  int16_t   nCols = nLastCol;

  int32_t code = initLastColArray(pTSchema, &pColArray);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  SArray *aColArray = taosArrayInit(nCols, sizeof(int16_t));
  if (NULL == aColArray) {
    taosArrayDestroy(pColArray);

    return TSDB_CODE_OUT_OF_MEMORY;
  }
  for (int i = 1; i < pTSchema->numOfCols; ++i) {
    taosArrayPush(aColArray, &pTSchema->columns[i].colId);
  }

  TSKEY lastRowTs = TSKEY_MAX;

  CacheNextRowIter iter = {0};
  nextRowIterOpen(&iter, uid, pTsdb, pTSchema, pr->suid, pr->pLoadInfo, pr->pDataIter, pr->pReadSnap, &pr->pDataFReader,
                  &pr->pDataFReaderLast, pr->lastTs);

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
    int16_t nCol = pTSchema->numOfCols;

    TSKEY rowTs = TSDBROW_TS(pRow);

    if (lastRowTs == TSKEY_MAX) {
      lastRowTs = rowTs;
      STColumn *pTColumn = &pTSchema->columns[0];

      *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.val = lastRowTs});
      taosArraySet(pColArray, 0, &(SLastCol){.ts = lastRowTs, .colVal = *pColVal});

      for (int16_t iCol = 1; iCol < nCol; ++iCol) {
        if (iCol >= nLastCol) {
          break;
        }
        SLastCol *pCol = taosArrayGet(pColArray, iCol);
        if (pCol->colVal.cid != pTSchema->columns[iCol].colId) {
          continue;
        }
        tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal);

        *pCol = (SLastCol){.ts = lastRowTs, .colVal = *pColVal};
        if (IS_VAR_DATA_TYPE(pColVal->type) && pColVal->value.nData > 0) {
          pCol->colVal.value.pData = taosMemoryMalloc(pCol->colVal.value.nData);
          if (pCol->colVal.value.pData == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _err;
          }
          memcpy(pCol->colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
        }

        if (!COL_VAL_IS_VALUE(pColVal)) {
          if (!setNoneCol) {
            noneCol = iCol;
            setNoneCol = true;
          }
        } else {
          int32_t aColIndex = taosArraySearchIdx(aColArray, &pColVal->cid, compareInt16Val, TD_EQ);
          taosArrayRemove(aColArray, aColIndex);
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
    for (int16_t iCol = noneCol; iCol < nCol; ++iCol) {
      if (iCol >= nLastCol) {
        break;
      }
      // high version's column value
      SLastCol *lastColVal = (SLastCol *)taosArrayGet(pColArray, iCol);
      if (lastColVal->colVal.cid != pTSchema->columns[iCol].colId) {
        continue;
      }
      SColVal *tColVal = &lastColVal->colVal;

      tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal);
      if (!COL_VAL_IS_VALUE(tColVal) && COL_VAL_IS_VALUE(pColVal)) {
        SLastCol lastCol = {.ts = rowTs, .colVal = *pColVal};
        if (IS_VAR_DATA_TYPE(pColVal->type) && pColVal->value.nData > 0) {
          SLastCol *pLastCol = (SLastCol *)taosArrayGet(pColArray, iCol);
          taosMemoryFree(pLastCol->colVal.value.pData);

          lastCol.colVal.value.pData = taosMemoryMalloc(lastCol.colVal.value.nData);
          if (lastCol.colVal.value.pData == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _err;
          }
          memcpy(lastCol.colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
        }

        taosArraySet(pColArray, iCol, &lastCol);
        int32_t aColIndex = taosArraySearchIdx(aColArray, &lastCol.colVal.cid, compareInt16Val, TD_EQ);
        taosArrayRemove(aColArray, aColIndex);
      } else if (!COL_VAL_IS_VALUE(tColVal) && !COL_VAL_IS_VALUE(pColVal) && !setNoneCol) {
        noneCol = iCol;
        setNoneCol = true;
      }
    }
  } while (setNoneCol);

  // if (taosArrayGetSize(pColArray) <= 0) {
  //*ppLastArray = NULL;
  // taosArrayDestroy(pColArray);
  //} else {
  if (!hasRow) {
    if (ignoreEarlierTs) {
      taosArrayDestroy(pColArray);
      pColArray = NULL;
    } else {
      taosArrayClear(pColArray);
    }
  }
  *ppLastArray = pColArray;
  //}

  nextRowIterClose(&iter);
  taosArrayDestroy(aColArray);
  // taosMemoryFreeClear(pTSchema);
  return code;

_err:
  nextRowIterClose(&iter);
  // taosMemoryFreeClear(pTSchema);
  *ppLastArray = NULL;
  taosArrayDestroy(pColArray);
  taosArrayDestroy(aColArray);
  return code;
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
  nextRowIterOpen(&iter, uid, pTsdb, pTSchema, pr->suid, pr->pLoadInfo, pr->pDataIter, pr->pReadSnap, &pr->pDataFReader,
                  &pr->pDataFReaderLast, pr->lastTs);

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
          pCol->colVal.value.pData = taosMemoryMalloc(pCol->colVal.value.nData);
          if (pCol->colVal.value.pData == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _err;
          }
          if (pColVal->value.nData > 0) {
            memcpy(pCol->colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
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
        if (IS_VAR_DATA_TYPE(pColVal->type) && pColVal->value.nData > 0) {
          SLastCol *pLastCol = (SLastCol *)taosArrayGet(pColArray, iCol);
          taosMemoryFree(pLastCol->colVal.value.pData);

          lastCol.colVal.value.pData = taosMemoryMalloc(lastCol.colVal.value.nData);
          if (lastCol.colVal.value.pData == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _err;
          }
          memcpy(lastCol.colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
        }

        taosArraySet(pColArray, iCol, &lastCol);
        int32_t aColIndex = taosArraySearchIdx(aColArray, &lastCol.colVal.cid, compareInt16Val, TD_EQ);
        taosArrayRemove(aColArray, aColIndex);
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
  nextRowIterOpen(&iter, uid, pTsdb, pTSchema, pr->suid, pr->pLoadInfo, pr->pDataIter, pr->pReadSnap, &pr->pDataFReader,
                  &pr->pDataFReaderLast, pr->lastTs);

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

    if (lastRowTs == TSKEY_MAX) {
      lastRowTs = rowTs;

      for (int16_t iCol = noneCol; iCol < nCols; ++iCol) {
        if (iCol >= nLastCol) {
          break;
        }
        SLastCol *pCol = taosArrayGet(pColArray, iCol);
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
          pCol->colVal.value.pData = taosMemoryMalloc(pCol->colVal.value.nData);
          if (pCol->colVal.value.pData == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _err;
          }
          if (pColVal->value.nData > 0) {
            memcpy(pCol->colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
          }
        }

        /*if (COL_VAL_IS_NONE(pColVal)) {
          if (!setNoneCol) {
            noneCol = iCol;
            setNoneCol = true;
          }
          } else {*/
        int32_t aColIndex = taosArraySearchIdx(aColArray, &pColVal->cid, compareInt16Val, TD_EQ);
        if (aColIndex >= 0) {
          taosArrayRemove(aColArray, aColIndex);
        }
        //}
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
      if (COL_VAL_IS_NONE(tColVal) && !COL_VAL_IS_NONE(pColVal)) {
        SLastCol lastCol = {.ts = rowTs, .colVal = *pColVal};
        if (IS_VAR_DATA_TYPE(pColVal->type) && pColVal->value.nData > 0) {
          SLastCol *pLastCol = (SLastCol *)taosArrayGet(pColArray, iCol);
          taosMemoryFree(pLastCol->colVal.value.pData);

          lastCol.colVal.value.pData = taosMemoryMalloc(lastCol.colVal.value.nData);
          if (lastCol.colVal.value.pData == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _err;
          }
          memcpy(lastCol.colVal.value.pData, pColVal->value.pData, pColVal->value.nData);
        }

        taosArraySet(pColArray, iCol, &lastCol);
        int32_t aColIndex = taosArraySearchIdx(aColArray, &lastCol.colVal.cid, compareInt16Val, TD_EQ);
        taosArrayRemove(aColArray, aColIndex);
      } else if (COL_VAL_IS_NONE(tColVal) && !COL_VAL_IS_NONE(pColVal) && !setNoneCol) {
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

  *ppLastArray = NULL;
  taosArrayDestroy(pColArray);
  taosArrayDestroy(aColArray);
  return code;
}

int32_t tsdbCacheGetLastrowH(SLRUCache *pCache, tb_uid_t uid, SCacheRowsReader *pr, LRUHandle **handle) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  //  getTableCacheKeyS(uid, "lr", key, &keyLen);
  getTableCacheKey(uid, 0, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (!h) {
    STsdb *pTsdb = pr->pVnode->pTsdb;
    taosThreadMutexLock(&pTsdb->lruMutex);

    h = taosLRUCacheLookup(pCache, key, keyLen);
    if (!h) {
      SArray *pArray = NULL;
      bool    dup = false;  // which is always false for now
      code = mergeLastRow(uid, pTsdb, &dup, &pArray, pr);
      // if table's empty or error or ignore ignore earlier ts, set handle NULL and return
      if (code < 0 || pArray == NULL) {
        if (!dup && pArray) {
          taosArrayDestroy(pArray);
        }

        taosThreadMutexUnlock(&pTsdb->lruMutex);

        *handle = NULL;

        return 0;
      }

      size_t              charge = pArray->capacity * pArray->elemSize + sizeof(*pArray);
      _taos_lru_deleter_t deleter = deleteTableCacheLast;
      LRUStatus status = taosLRUCacheInsert(pCache, key, keyLen, pArray, charge, deleter, &h, TAOS_LRU_PRIORITY_LOW);
      if (status != TAOS_LRU_STATUS_OK) {
        code = -1;
      }
    }
    taosThreadMutexUnlock(&pTsdb->lruMutex);
  }

  *handle = h;

  return code;
}

int32_t tsdbCacheGetLastH(SLRUCache *pCache, tb_uid_t uid, SCacheRowsReader *pr, LRUHandle **handle) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  // getTableCacheKeyS(uid, "l", key, &keyLen);
  getTableCacheKey(uid, 1, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (!h) {
    STsdb *pTsdb = pr->pVnode->pTsdb;
    taosThreadMutexLock(&pTsdb->lruMutex);

    h = taosLRUCacheLookup(pCache, key, keyLen);
    if (!h) {
      SArray *pLastArray = NULL;
      code = mergeLast(uid, pTsdb, &pLastArray, pr);
      // if table's empty or error or ignore ignore earlier ts, set handle NULL and return
      if (code < 0 || pLastArray == NULL) {
        taosThreadMutexUnlock(&pTsdb->lruMutex);

        *handle = NULL;
        return 0;
      }

      size_t              charge = pLastArray->capacity * pLastArray->elemSize + sizeof(*pLastArray);
      _taos_lru_deleter_t deleter = deleteTableCacheLast;
      LRUStatus           status =
          taosLRUCacheInsert(pCache, key, keyLen, pLastArray, charge, deleter, &h, TAOS_LRU_PRIORITY_LOW);
      if (status != TAOS_LRU_STATUS_OK) {
        code = -1;
      }
    }
    taosThreadMutexUnlock(&pTsdb->lruMutex);
  }

  *handle = h;

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

size_t tsdbCacheGetCapacity(SVnode *pVnode) { return taosLRUCacheGetCapacity(pVnode->pTsdb->lruCache); }

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

static void deleteBICache(const void *key, size_t keyLen, void *value) {
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
      LRUStatus status = taosLRUCacheInsert(pCache, key, keyLen, pArray, charge, deleter, &h, TAOS_LRU_PRIORITY_LOW);
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
