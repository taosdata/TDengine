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

typedef struct {
  TSKEY  ts;
  int8_t dirty;
  struct {
    int16_t cid;
    int8_t  type;
    int8_t  flag;
    union {
      int64_t val;
      struct {
        uint32_t nData;
        uint8_t *pData;
      };
    } value;
  } colVal;
} SLastColV0;

/*
typedef struct {
  SLastColV0 lastColV0;
  char       colData[];
  int8_t     version;
  uint8_t    numOfPKs;
  SValue     pks[0];
  char       pk0Data[];
  SValue     pks[1];
  char       pk1Data[];
  ...
} SLastColDisk;
*/

static int32_t tsdbCacheDeserializeV0(char const *value, SLastCol *pLastCol) {
  SLastColV0 *pLastColV0 = (SLastColV0 *)value;

  pLastCol->rowKey.ts = pLastColV0->ts;
  pLastCol->rowKey.numOfPKs = 0;
  pLastCol->dirty = pLastColV0->dirty;
  pLastCol->colVal.cid = pLastColV0->colVal.cid;
  pLastCol->colVal.flag = pLastColV0->colVal.flag;
  pLastCol->colVal.value.type = pLastColV0->colVal.type;

  pLastCol->cacheStatus = TSDB_LAST_CACHE_VALID;

  if (IS_VAR_DATA_TYPE(pLastCol->colVal.value.type)) {
    pLastCol->colVal.value.nData = pLastColV0->colVal.value.nData;
    pLastCol->colVal.value.pData = NULL;
    if (pLastCol->colVal.value.nData > 0) {
      pLastCol->colVal.value.pData = (uint8_t *)(&pLastColV0[1]);
    }
    return sizeof(SLastColV0) + pLastColV0->colVal.value.nData;
  } else if (pLastCol->colVal.value.type == TSDB_DATA_TYPE_DECIMAL) {
    pLastCol->colVal.value.nData = pLastColV0->colVal.value.nData;
    pLastCol->colVal.value.pData = (uint8_t *)(&pLastColV0[1]);
    return sizeof(SLastColV0) + pLastColV0->colVal.value.nData;
  } else {
    pLastCol->colVal.value.val = pLastColV0->colVal.value.val;
    return sizeof(SLastColV0);
  }
}

int32_t tsdbCacheDeserialize(char const *value, size_t size, SLastCol **ppLastCol) {
  if (!value) {
    return TSDB_CODE_INVALID_PARA;
  }

  SLastCol *pLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
  if (NULL == pLastCol) {
    return terrno;
  }

  int32_t offset = tsdbCacheDeserializeV0(value, pLastCol);
  if (offset == size) {
    // version 0
    *ppLastCol = pLastCol;

    TAOS_RETURN(TSDB_CODE_SUCCESS);
  } else if (offset > size) {
    taosMemoryFreeClear(pLastCol);

    TAOS_RETURN(TSDB_CODE_INVALID_DATA_FMT);
  }

  // version
  int8_t version = *(int8_t *)(value + offset);
  offset += sizeof(int8_t);

  // numOfPKs
  pLastCol->rowKey.numOfPKs = *(uint8_t *)(value + offset);
  offset += sizeof(uint8_t);

  // pks
  for (int32_t i = 0; i < pLastCol->rowKey.numOfPKs; i++) {
    pLastCol->rowKey.pks[i] = *(SValue *)(value + offset);
    offset += sizeof(SValue);

    if (IS_VAR_DATA_TYPE(pLastCol->rowKey.pks[i].type)) {
      pLastCol->rowKey.pks[i].pData = NULL;
      if (pLastCol->rowKey.pks[i].nData > 0) {
        pLastCol->rowKey.pks[i].pData = (uint8_t *)value + offset;
        offset += pLastCol->rowKey.pks[i].nData;
      }
    }
  }

  if (version >= LAST_COL_VERSION_2) {
    pLastCol->cacheStatus = *(uint8_t *)(value + offset);
  }

  if (offset > size) {
    taosMemoryFreeClear(pLastCol);

    TAOS_RETURN(TSDB_CODE_INVALID_DATA_FMT);
  }

  *ppLastCol = pLastCol;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t tsdbCacheSerializeV0(char const *value, SLastCol *pLastCol) {
  SLastColV0 *pLastColV0 = (SLastColV0 *)value;

  pLastColV0->ts = pLastCol->rowKey.ts;
  pLastColV0->dirty = pLastCol->dirty;
  pLastColV0->colVal.cid = pLastCol->colVal.cid;
  pLastColV0->colVal.flag = pLastCol->colVal.flag;
  pLastColV0->colVal.type = pLastCol->colVal.value.type;
  if (IS_VAR_DATA_TYPE(pLastCol->colVal.value.type)) {
    pLastColV0->colVal.value.nData = pLastCol->colVal.value.nData;
    if (pLastCol->colVal.value.nData > 0) {
      memcpy(&pLastColV0[1], pLastCol->colVal.value.pData, pLastCol->colVal.value.nData);
    }
    return sizeof(SLastColV0) + pLastCol->colVal.value.nData;
  } else if (pLastCol->colVal.value.type == TSDB_DATA_TYPE_DECIMAL) {
    pLastColV0->colVal.value.nData = pLastCol->colVal.value.nData;
    if (pLastCol->colVal.value.nData > 0) {
      memcpy(&pLastColV0[1], pLastCol->colVal.value.pData, pLastCol->colVal.value.nData);
    }
    return sizeof(SLastColV0) + pLastCol->colVal.value.nData;
  } else {
    pLastColV0->colVal.value.val = pLastCol->colVal.value.val;
    return sizeof(SLastColV0);
  }

  return 0;
}

static int32_t tsdbCacheSerialize(SLastCol *pLastCol, char **value, size_t *size) {
  *size = sizeof(SLastColV0);
  if (IS_VAR_DATA_TYPE(pLastCol->colVal.value.type)) {
    *size += pLastCol->colVal.value.nData;
  }
  if (pLastCol->colVal.value.type == TSDB_DATA_TYPE_DECIMAL) {
    *size += DECIMAL128_BYTES;
  }
  *size += sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t);  // version + numOfPKs + cacheStatus

  for (int8_t i = 0; i < pLastCol->rowKey.numOfPKs; i++) {
    *size += sizeof(SValue);
    if (IS_VAR_DATA_TYPE(pLastCol->rowKey.pks[i].type)) {
      *size += pLastCol->rowKey.pks[i].nData;
    }
  }

  *value = taosMemoryMalloc(*size);
  if (NULL == *value) {
    TAOS_RETURN(terrno);
  }

  int32_t offset = tsdbCacheSerializeV0(*value, pLastCol);

  // version
  ((uint8_t *)(*value + offset))[0] = LAST_COL_VERSION;
  offset++;

  // numOfPKs
  ((uint8_t *)(*value + offset))[0] = pLastCol->rowKey.numOfPKs;
  offset++;

  // pks
  for (int8_t i = 0; i < pLastCol->rowKey.numOfPKs; i++) {
    ((SValue *)(*value + offset))[0] = pLastCol->rowKey.pks[i];
    offset += sizeof(SValue);
    if (IS_VAR_DATA_TYPE(pLastCol->rowKey.pks[i].type)) {
      if (pLastCol->rowKey.pks[i].nData > 0) {
        memcpy(*value + offset, pLastCol->rowKey.pks[i].pData, pLastCol->rowKey.pks[i].nData);
      }
      offset += pLastCol->rowKey.pks[i].nData;
    }
  }

  ((uint8_t *)(*value + offset))[0] = pLastCol->cacheStatus;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static void tsdbCacheDeleter(const void *key, size_t klen, void *value, void *ud) {
  SLastCol *pLastCol = (SLastCol *)value;

  if (pLastCol->dirty) {
    if (tsdbCacheFlushDirty(key, klen, pLastCol, ud) != 0) {
      STsdb *pTsdb = (STsdb *)ud;
      tsdbTrace("tsdb/cache: vgId:%d, flush cache %s failed at line %d.", TD_VID(pTsdb->pVnode), __func__, __LINE__);
    }
  }

  for (uint8_t i = 0; i < pLastCol->rowKey.numOfPKs; ++i) {
    SValue *pValue = &pLastCol->rowKey.pks[i];
    if (IS_VAR_DATA_TYPE(pValue->type)) {
      taosMemoryFree(pValue->pData);
    }
  }

  taosMemoryFree(value);
}

static void tsdbCacheOverWriter(const void *key, size_t klen, void *value, void *ud) {
  SLastCol *pLastCol = (SLastCol *)value;
  pLastCol->dirty = 0;
}

static int32_t reallocVarDataVal(SValue *pValue) {
  if (IS_VAR_DATA_TYPE(pValue->type)) {
    uint8_t *pVal = pValue->pData;
    uint32_t nData = pValue->nData;
    if (nData > 0 && pVal != NULL) {
      uint8_t *p = taosMemoryMalloc(nData);
      if (!p) {
        TAOS_RETURN(terrno);
      }
      pValue->pData = p;
      (void)memcpy(pValue->pData, pVal, nData);
    } else {
      pValue->pData = NULL;
    }
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t reallocVarData(SColVal *pColVal) { return reallocVarDataVal(&pColVal->value); }

// realloc pk data and col data.
int32_t tsdbCacheReallocSLastCol(SLastCol *pCol, size_t *pCharge) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  size_t  charge = sizeof(SLastCol);

  int8_t i = 0;
  for (; i < pCol->rowKey.numOfPKs; i++) {
    SValue *pValue = &pCol->rowKey.pks[i];
    if (IS_VAR_DATA_TYPE(pValue->type)) {
      TAOS_CHECK_EXIT(reallocVarDataVal(pValue));
      charge += pValue->nData;
    }
  }

  if (IS_VAR_DATA_TYPE(pCol->colVal.value.type)) {
    TAOS_CHECK_EXIT(reallocVarData(&pCol->colVal));
    charge += pCol->colVal.value.nData;
  }

  if (pCol->colVal.value.type == TSDB_DATA_TYPE_DECIMAL) {
    if (pCol->colVal.value.nData > 0) {
      void *p = taosMemoryMalloc(pCol->colVal.value.nData);
      if (!p) TAOS_CHECK_EXIT(terrno);
      (void)memcpy(p, pCol->colVal.value.pData, pCol->colVal.value.nData);
      pCol->colVal.value.pData = p;
    }
    charge += pCol->colVal.value.nData;
  }

  if (pCharge) {
    *pCharge = charge;
  }

_exit:
  if (TSDB_CODE_SUCCESS != code) {
    for (int8_t j = 0; j < i; j++) {
      if (IS_VAR_DATA_TYPE(pCol->rowKey.pks[j].type)) {
        taosMemoryFree(pCol->rowKey.pks[j].pData);
      }
    }

    (void)memset(pCol, 0, sizeof(SLastCol));
  }

  TAOS_RETURN(code);
}

int32_t tsdbCachePutToRocksdb(STsdb *pTsdb, SLastKey *pLastKey, SLastCol *pLastCol) {
  int32_t code = 0;
#ifdef USE_ROCKSDB
  char  *rocks_value = NULL;
  size_t vlen = 0;

  code = tsdbCacheSerialize(pLastCol, &rocks_value, &vlen);
  if (code) {
    tsdbError("tsdb/cache/putrocks: vgId:%d, serialize failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
    TAOS_RETURN(code);
  }

  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  (void)taosThreadMutexLock(&pTsdb->rCache.writeBatchMutex);
  rocksdb_writebatch_put(wb, (char *)pLastKey, TSDB_CACHE_KEY_LEN, rocks_value, vlen);
  (void)taosThreadMutexUnlock(&pTsdb->rCache.writeBatchMutex);

  taosMemoryFree(rocks_value);
#endif
  TAOS_RETURN(code);
}

static int32_t tsdbCachePutToLRU(STsdb *pTsdb, SLastKey *pLastKey, SLastCol *pLastCol, int8_t dirty) {
  int32_t code = 0, lino = 0;

  SLastCol *pLRULastCol = taosMemoryCalloc(1, sizeof(SLastCol));
  if (!pLRULastCol) {
    return terrno;
  }

  size_t charge = 0;
  *pLRULastCol = *pLastCol;
  pLRULastCol->dirty = dirty;
  TAOS_CHECK_EXIT(tsdbCacheReallocSLastCol(pLRULastCol, &charge));

  LRUStatus status = taosLRUCacheInsert(pTsdb->lruCache, pLastKey, ROCKS_KEY_LEN, pLRULastCol, charge, tsdbCacheDeleter,
                                        tsdbCacheOverWriter, NULL, TAOS_LRU_PRIORITY_LOW, pTsdb);
  if (TAOS_LRU_STATUS_OK != status && TAOS_LRU_STATUS_OK_OVERWRITTEN != status) {
    tsdbError("vgId:%d, %s failed at line %d status %d.", TD_VID(pTsdb->pVnode), __func__, __LINE__, status);
    code = TSDB_CODE_FAILED;
    pLRULastCol = NULL;
  }

_exit:
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(pLRULastCol);
    tsdbError("tsdb/cache/putlru: vgId:%d, failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

int tsdbColCacheCmp(void *state, const char *a, size_t alen, const char *b, size_t blen) {
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

  if ((lhs->lflag & LFLAG_LAST) < (rhs->lflag & LFLAG_LAST)) {
    return -1;
  } else if ((lhs->lflag & LFLAG_LAST) > (rhs->lflag & LFLAG_LAST)) {
    return 1;
  }

  return 0;
}

int32_t tsdbColCacheNewTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type, int8_t lflag,
                                   SSchemaWrapper *pOldSchemaWrapper, SSchemaWrapper *pNewSchemaWrapper) {
  int32_t code = 0, lino = 0;

  // Schema wrappers are required for column operations
  if (pOldSchemaWrapper == NULL || pNewSchemaWrapper == NULL) {
    tsdbError("vgId:%d, %s failed: schema wrappers are required for column operations", TD_VID(pTsdb->pVnode),
              __func__);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }
  // Original column-based cache implementation
  SLRUCache *pCache = pTsdb->lruCache;
  SRowKey    emptyRowKey = {.ts = TSKEY_MIN, .numOfPKs = 0};
  SLastCol   emptyCol = {
        .rowKey = emptyRowKey, .colVal = COL_VAL_NONE(cid, col_type), .dirty = 1, .cacheStatus = TSDB_LAST_CACHE_VALID};

  SLastKey *pLastKey = &(SLastKey){.lflag = lflag, .uid = uid, .cid = cid};
  code = tsdbCachePutToLRU(pTsdb, pLastKey, &emptyCol, 1);
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t tsdbColCacheDropTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t lflag,
                                    SSchemaWrapper *pOldSchemaWrapper, SSchemaWrapper *pNewSchemaWrapper,
                                    bool hasPrimaryKey) {
  // Original column-based cache implementation
  // build keys & multi get from rocks
  int32_t code = 0, lino = 0;
  char  **keys_list = taosMemoryCalloc(2, sizeof(char *));
  if (!keys_list) {
    return terrno;
  }
  size_t *keys_list_sizes = taosMemoryCalloc(2, sizeof(size_t));
  if (!keys_list_sizes) {
    taosMemoryFree(keys_list);
    return terrno;
  }
  const size_t klen = TSDB_CACHE_KEY_LEN;

  char *keys = taosMemoryCalloc(2, sizeof(SLastKey));
  if (!keys) {
    taosMemoryFree(keys_list);
    taosMemoryFree(keys_list_sizes);
    return terrno;
  }
  ((SLastKey *)keys)[0] = (SLastKey){.lflag = LFLAG_LAST, .uid = uid, .cid = cid};
  ((SLastKey *)keys)[1] = (SLastKey){.lflag = LFLAG_LAST_ROW, .uid = uid, .cid = cid};

  keys_list[0] = keys;
  keys_list[1] = keys + sizeof(SLastKey);
  keys_list_sizes[0] = klen;
  keys_list_sizes[1] = klen;

  char  **values_list = NULL;
  size_t *values_list_sizes = NULL;

  // was written by caller
  // rocksMayWrite(pTsdb, true); // flush writebatch cache

  TAOS_CHECK_GOTO(tsdbCacheGetValuesFromRocks(pTsdb, 2, (const char *const *)keys_list, keys_list_sizes, &values_list,
                                              &values_list_sizes),
                  NULL, _exit);
#ifdef USE_ROCKSDB
  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
#endif
  {
#ifdef USE_ROCKSDB
    SLastCol *pLastCol = NULL;
    if (values_list[0] != NULL) {
      code = tsdbCacheDeserialize(values_list[0], values_list_sizes[0], &pLastCol);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbError("vgId:%d, %s deserialize failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
        goto _exit;
      }
      if (NULL != pLastCol) {
        rocksdb_writebatch_delete(wb, keys_list[0], klen);
      }
      taosMemoryFreeClear(pLastCol);
    }

    pLastCol = NULL;
    if (values_list[1] != NULL) {
      code = tsdbCacheDeserialize(values_list[1], values_list_sizes[1], &pLastCol);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbError("vgId:%d, %s deserialize failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
        goto _exit;
      }
      if (NULL != pLastCol) {
        rocksdb_writebatch_delete(wb, keys_list[1], klen);
      }
      taosMemoryFreeClear(pLastCol);
    }

    rocksdb_free(values_list[0]);
    rocksdb_free(values_list[1]);
#endif

    for (int i = 0; i < 2; i++) {
      LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, keys_list[i], klen);
      if (h) {
        tsdbLRUCacheRelease(pTsdb->lruCache, h, true);
        taosLRUCacheErase(pTsdb->lruCache, keys_list[i], klen);
      }
    }
  }

_exit:
  taosMemoryFree(keys_list[0]);

  taosMemoryFree(keys_list);
  taosMemoryFree(keys_list_sizes);
  taosMemoryFree(values_list);
  taosMemoryFree(values_list_sizes);

  TAOS_RETURN(code);
}

// Delete table cache for column-based cache
int32_t tsdbColCacheDeleteTable(STsdb *pTsdb, int64_t uid, STSchema *pTSchema) {
  int32_t code = 0;

  // Delete cache entries for all columns
  for (int j = 0; j < pTSchema->numOfCols; j++) {
    int16_t cid = pTSchema->columns[j].colId;

    SLastKey lastKeys[2] = {{.lflag = LFLAG_LAST, .uid = uid, .cid = cid},
                            {.lflag = LFLAG_LAST_ROW, .uid = uid, .cid = cid}};

    for (int i = 0; i < 2; i++) {
#ifdef USE_ROCKSDB
      rocksdb_writebatch_delete(pTsdb->rCache.writebatch, (char *)&lastKeys[i], TSDB_CACHE_KEY_LEN);
#endif
      LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, &lastKeys[i], TSDB_CACHE_KEY_LEN);
      if (h) {
        tsdbLRUCacheRelease(pTsdb->lruCache, h, true);
        taosLRUCacheErase(pTsdb->lruCache, &lastKeys[i], TSDB_CACHE_KEY_LEN);
      }
    }
  }

  TAOS_RETURN(code);
}

// Create table cache for column-based cache
int32_t tsdbColCacheCreateTable(STsdb *pTsdb, int64_t uid, STSchema *pTSchema) {
  int32_t code = 0;

  // Create cache entries for all columns
  for (int j = 0; j < pTSchema->numOfCols; j++) {
    int16_t cid = pTSchema->columns[j].colId;
    int8_t  col_type = pTSchema->columns[j].type;

    SLastKey lastKeys[2] = {{.lflag = LFLAG_LAST, .uid = uid, .cid = cid},
                            {.lflag = LFLAG_LAST_ROW, .uid = uid, .cid = cid}};

    for (int i = 0; i < 2; i++) {
      // Create new empty column cache entry
      SRowKey  emptyRowKey = {.ts = TSKEY_MIN, .numOfPKs = 0};
      SLastCol emptyCol = {.rowKey = emptyRowKey,
                           .colVal = COL_VAL_NONE(cid, col_type),
                           .dirty = 1,
                           .cacheStatus = TSDB_LAST_CACHE_VALID};

      SLastKey *pLastKey = &lastKeys[i];
      code = tsdbCachePutToLRU(pTsdb, pLastKey, &emptyCol, 1);
      if (code) {
        tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
      }
    }
  }

  TAOS_RETURN(code);
}

int32_t tsdbColCacheUpdateFromCtxArray(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SArray *updCtxArray) {
  int32_t code = 0, lino = 0;

  int        num_keys = TARRAY_SIZE(updCtxArray);
  SArray    *remainCols = NULL;
  SLRUCache *pCache = pTsdb->lruCache;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);
  for (int i = 0; i < num_keys; ++i) {
    SLastUpdateCtx *updCtx = &((SLastUpdateCtx *)TARRAY_DATA(updCtxArray))[i];
    int8_t          lflag = updCtx->lflag;
    SRowKey        *pRowKey = &updCtx->tsdbRowKey.key;
    SColVal        *pColVal = &updCtx->colVal;

    if (lflag == LFLAG_LAST && !COL_VAL_IS_VALUE(pColVal)) {
      continue;
    }

    SLastKey  *key = &(SLastKey){.lflag = lflag, .uid = uid, .cid = pColVal->cid};
    LRUHandle *h = taosLRUCacheLookup(pCache, key, TSDB_CACHE_KEY_LEN);
    if (h) {
      SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pCache, h);
      if (pLastCol->cacheStatus != TSDB_LAST_CACHE_NO_CACHE) {
        int32_t cmp_res = tRowKeyCompare(&pLastCol->rowKey, pRowKey);
        if (cmp_res < 0 || (cmp_res == 0 && !COL_VAL_IS_NONE(pColVal))) {
          SLastCol newLastCol = {
              .rowKey = *pRowKey, .colVal = *pColVal, .dirty = 1, .cacheStatus = TSDB_LAST_CACHE_VALID};
          code = tsdbCachePutToLRU(pTsdb, key, &newLastCol, 1);
        }
      }

      tsdbLRUCacheRelease(pCache, h, false);
      TAOS_CHECK_EXIT(code);
    } else {
      if (!remainCols) {
        remainCols = taosArrayInit(num_keys * 2, sizeof(SIdxKey));
        if (!remainCols) {
          TAOS_CHECK_GOTO(terrno, &lino, _exit);
        }
      }
      if (!taosArrayPush(remainCols, &(SIdxKey){i, *key})) {
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
    }
  }

  if (remainCols) {
    num_keys = TARRAY_SIZE(remainCols);
  }
  if (remainCols && num_keys > 0) {
    char  **keys_list = NULL;
    size_t *keys_list_sizes = NULL;
    char  **values_list = NULL;
    size_t *values_list_sizes = NULL;
    char  **errs = NULL;
    keys_list = taosMemoryCalloc(num_keys, sizeof(char *));
    if (!keys_list) {
      (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
      return terrno;
    }
    keys_list_sizes = taosMemoryCalloc(num_keys, sizeof(size_t));
    if (!keys_list_sizes) {
      taosMemoryFree(keys_list);
      (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
      return terrno;
    }
    for (int i = 0; i < num_keys; ++i) {
      SIdxKey *idxKey = &((SIdxKey *)TARRAY_DATA(remainCols))[i];

      keys_list[i] = (char *)&idxKey->key;
      keys_list_sizes[i] = TSDB_CACHE_KEY_LEN;
    }

    rocksMayWrite(pTsdb, true);  // flush writebatch cache

    code = tsdbCacheGetValuesFromRocks(pTsdb, num_keys, (const char *const *)keys_list, keys_list_sizes, &values_list,
                                       &values_list_sizes);
    if (code) {
      taosMemoryFree(keys_list);
      taosMemoryFree(keys_list_sizes);
      goto _exit;
    }

    // rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
    for (int i = 0; i < num_keys; ++i) {
      SIdxKey        *idxKey = &((SIdxKey *)TARRAY_DATA(remainCols))[i];
      SLastUpdateCtx *updCtx = (SLastUpdateCtx *)taosArrayGet(updCtxArray, idxKey->idx);
      SRowKey        *pRowKey = &updCtx->tsdbRowKey.key;
      SColVal        *pColVal = &updCtx->colVal;

      SLastCol *pLastCol = NULL;
      if (values_list[i] != NULL) {
        code = tsdbCacheDeserialize(values_list[i], values_list_sizes[i], &pLastCol);
        if (code != TSDB_CODE_SUCCESS) {
          tsdbError("vgId:%d, %s deserialize failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                    tstrerror(code));
          goto _exit;
        }
      }
      /*
      if (code) {
        tsdbError("tsdb/cache: vgId:%d, deserialize failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
      }
      */
      SLastCol *pToFree = pLastCol;

      if (pLastCol && pLastCol->cacheStatus == TSDB_LAST_CACHE_NO_CACHE) {
        if ((code = tsdbCachePutToLRU(pTsdb, &idxKey->key, pLastCol, 0)) != TSDB_CODE_SUCCESS) {
          tsdbError("tsdb/cache: vgId:%d, put lru failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino,
                    tstrerror(code));
          taosMemoryFreeClear(pToFree);
          break;
        }

        // cache invalid => skip update
        taosMemoryFreeClear(pToFree);
        continue;
      }

      if (IS_LAST_KEY(idxKey->key) && !COL_VAL_IS_VALUE(pColVal)) {
        taosMemoryFreeClear(pToFree);
        continue;
      }

      int32_t cmp_res = 1;
      if (pLastCol) {
        cmp_res = tRowKeyCompare(&pLastCol->rowKey, pRowKey);
      }

      if (NULL == pLastCol || cmp_res < 0 || (cmp_res == 0 && !COL_VAL_IS_NONE(pColVal))) {
        SLastCol lastColTmp = {
            .rowKey = *pRowKey, .colVal = *pColVal, .dirty = 0, .cacheStatus = TSDB_LAST_CACHE_VALID};
        if ((code = tsdbCachePutToRocksdb(pTsdb, &idxKey->key, &lastColTmp)) != TSDB_CODE_SUCCESS) {
          tsdbError("tsdb/cache: vgId:%d, put rocks failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino,
                    tstrerror(code));
          taosMemoryFreeClear(pToFree);
          break;
        }
        if ((code = tsdbCachePutToLRU(pTsdb, &idxKey->key, &lastColTmp, 0)) != TSDB_CODE_SUCCESS) {
          tsdbError("tsdb/cache: vgId:%d, put lru failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino,
                    tstrerror(code));
          taosMemoryFreeClear(pToFree);
          break;
        }
      }

      taosMemoryFreeClear(pToFree);
    }

    rocksMayWrite(pTsdb, false);

    taosMemoryFree(keys_list);
    taosMemoryFree(keys_list_sizes);
    if (values_list) {
#ifdef USE_ROCKSDB
      for (int i = 0; i < num_keys; ++i) {
        rocksdb_free(values_list[i]);
      }
#endif
      taosMemoryFree(values_list);
    }
    taosMemoryFree(values_list_sizes);
  }

_exit:
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
  taosArrayDestroy(remainCols);

  if (code) {
    tsdbError("tsdb/cache: vgId:%d, update failed at line %d since %s.", TD_VID(pTsdb->pVnode), __LINE__,
              tstrerror(code));
  }

  TAOS_RETURN(code);
}

#ifndef UPDATE_CACHE_BATCH
int32_t tsdbColCacheRowFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, int64_t version, int32_t nRow,
                                    SRow **aRow) {
  int32_t code = 0, lino = 0;

  // 1. prepare last
  TSDBROW      lRow = {.type = TSDBROW_ROW_FMT, .pTSRow = aRow[nRow - 1], .version = version};
  STSchema    *pTSchema = NULL;
  int32_t      sver = TSDBROW_SVERSION(&lRow);
  SSHashObj   *iColHash = NULL;
  STSDBRowIter iter = {0};

  TAOS_CHECK_GOTO(tsdbUpdateSkm(pTsdb, suid, uid, sver), &lino, _exit);
  pTSchema = pTsdb->rCache.pTSchema;

  TSDBROW tRow = {.type = TSDBROW_ROW_FMT, .version = version};
  int32_t nCol = pTSchema->numOfCols;
  SArray *ctxArray = pTsdb->rCache.ctxArray;

  // 1. prepare by lrow
  STsdbRowKey tsdbRowKey = {0};
  tsdbRowGetKey(&lRow, &tsdbRowKey);

  TAOS_CHECK_GOTO(tsdbRowIterOpen(&iter, &lRow, pTSchema), &lino, _exit);

  int32_t iCol = 0;
  for (SColVal *pColVal = tsdbRowIterNext(&iter); pColVal && iCol < nCol; pColVal = tsdbRowIterNext(&iter), iCol++) {
    SLastUpdateCtx updateCtx = {.lflag = LFLAG_LAST_ROW, .tsdbRowKey = tsdbRowKey, .colVal = *pColVal};
    if (!taosArrayPush(ctxArray, &updateCtx)) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }

    if (COL_VAL_IS_VALUE(pColVal)) {
      updateCtx.lflag = LFLAG_LAST;
      if (!taosArrayPush(ctxArray, &updateCtx)) {
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
    } else {
      if (!iColHash) {
        iColHash = tSimpleHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
        if (iColHash == NULL) {
          TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
        }
      }

      if (tSimpleHashPut(iColHash, &iCol, sizeof(iCol), NULL, 0)) {
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
      }
    }
  }

  // 2. prepare by the other rows
  for (int32_t iRow = nRow - 2; iRow >= 0; --iRow) {
    if (tSimpleHashGetSize(iColHash) == 0) {
      break;
    }

    tRow.pTSRow = aRow[iRow];

    STsdbRowKey tsdbRowKey = {0};
    tsdbRowGetKey(&tRow, &tsdbRowKey);

    void   *pIte = NULL;
    int32_t iter = 0;
    while ((pIte = tSimpleHashIterate(iColHash, pIte, &iter)) != NULL) {
      int32_t iCol = ((int32_t *)pIte)[0];
      SColVal colVal = COL_VAL_NONE(0, 0);
      tsdbRowGetColVal(&tRow, pTSchema, iCol, &colVal);

      if (COL_VAL_IS_VALUE(&colVal)) {
        SLastUpdateCtx updateCtx = {.lflag = LFLAG_LAST, .tsdbRowKey = tsdbRowKey, .colVal = colVal};
        if (!taosArrayPush(ctxArray, &updateCtx)) {
          TAOS_CHECK_GOTO(terrno, &lino, _exit);
        }
        code = tSimpleHashIterateRemove(iColHash, &iCol, sizeof(iCol), &pIte, &iter);
        if (code != TSDB_CODE_SUCCESS) {
          tsdbTrace("vgId:%d, %s tSimpleHashIterateRemove failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__,
                    __LINE__, tstrerror(code));
        }
      }
    }
  }

  TAOS_CHECK_GOTO(tsdbCacheUpdate(pTsdb, suid, uid, ctxArray), &lino, _exit);

_exit:
  TAOS_RETURN(code);
}
#endif

int32_t tsdbCacheLoadFromRocks(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SArray *remainCols,
                               SArray *ignoreFromRocks, SCacheRowsReader *pr, int8_t ltype) {
  int32_t code = 0, lino = 0;
  int     num_keys = TARRAY_SIZE(remainCols);
  char  **keys_list = taosMemoryMalloc(num_keys * sizeof(char *));
  size_t *keys_list_sizes = taosMemoryMalloc(num_keys * sizeof(size_t));
  char   *key_list = taosMemoryMalloc(num_keys * TSDB_CACHE_KEY_LEN);
  if (!keys_list || !keys_list_sizes || !key_list) {
    taosMemoryFree(keys_list);
    taosMemoryFree(keys_list_sizes);
    TAOS_RETURN(terrno);
  }
  char  **values_list = NULL;
  size_t *values_list_sizes = NULL;
  for (int i = 0; i < num_keys; ++i) {
    memcpy(key_list + i * TSDB_CACHE_KEY_LEN, &((SIdxKey *)taosArrayGet(remainCols, i))->key, TSDB_CACHE_KEY_LEN);
    keys_list[i] = key_list + i * TSDB_CACHE_KEY_LEN;
    keys_list_sizes[i] = TSDB_CACHE_KEY_LEN;
  }

  rocksMayWrite(pTsdb, true);  // flush writebatch cache

  code = tsdbCacheGetValuesFromRocks(pTsdb, num_keys, (const char *const *)keys_list, keys_list_sizes, &values_list,
                                     &values_list_sizes);
  if (code) {
    taosMemoryFree(key_list);
    taosMemoryFree(keys_list);
    taosMemoryFree(keys_list_sizes);
    TAOS_RETURN(code);
  }

  SLRUCache *pCache = pTsdb->lruCache;
  for (int i = 0, j = 0; i < num_keys && j < TARRAY_SIZE(remainCols); ++i) {
    SLastCol *pLastCol = NULL;
    bool      ignore = ((bool *)TARRAY_DATA(ignoreFromRocks))[i];
    if (ignore) {
      ++j;
      continue;
    }

    if (values_list[i] != NULL) {
      code = tsdbCacheDeserialize(values_list[i], values_list_sizes[i], &pLastCol);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbError("vgId:%d, %s deserialize failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
        goto _exit;
      }
    }
    SLastCol *pToFree = pLastCol;
    SIdxKey  *idxKey = &((SIdxKey *)TARRAY_DATA(remainCols))[j];
    if (pLastCol && pLastCol->cacheStatus != TSDB_LAST_CACHE_NO_CACHE) {
#if TSDB_CACHE_ROW_BASED
      // Convert column to row-based cache
      SLastRowKey rowKey = {.uid = uid, .lflag = (idxKey->key.lflag == LFLAG_LAST) ? LFLAG_LAST_ROW : LFLAG_LAST_ROW};
      STsdbRowKey tsdbRowKey = {.key = pLastCol->rowKey};
      SLastRow    tmpRow = {.rowKey = tsdbRowKey, .pRow = NULL, .dirty = 0, .numCols = 0, .colStatus = NULL};
      code = tsdbRowCachePutToLRU(pTsdb, &rowKey, &tmpRow, 0);
#else
      code = tsdbCachePutToLRU(pTsdb, &idxKey->key, pLastCol, 0);
#endif
      if (code) {
        tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
        taosMemoryFreeClear(pToFree);
        TAOS_CHECK_EXIT(code);
      }

      SLastCol lastCol = *pLastCol;
      code = tsdbCacheReallocSLastCol(&lastCol, NULL);
      if (TSDB_CODE_SUCCESS != code) {
        taosMemoryFreeClear(pToFree);
        TAOS_CHECK_EXIT(code);
      }

      taosArraySet(pLastArray, idxKey->idx, &lastCol);
      taosArrayRemove(remainCols, j);
      taosArrayRemove(ignoreFromRocks, j);
    } else {
      ++j;
    }

    taosMemoryFreeClear(pToFree);
  }

  if (TARRAY_SIZE(remainCols) > 0) {
    // tsdbTrace("tsdb/cache: vgId: %d, load %" PRId64 " from raw", TD_VID(pTsdb->pVnode), uid);
    code = tsdbCacheLoadFromRaw(pTsdb, uid, pLastArray, remainCols, pr, ltype);
  }

_exit:
  taosMemoryFree(key_list);
  taosMemoryFree(keys_list);
  taosMemoryFree(keys_list_sizes);
  if (values_list) {
#ifdef USE_ROCKSDB
    for (int i = 0; i < num_keys; ++i) {
      rocksdb_free(values_list[i]);
    }
#endif
    taosMemoryFree(values_list);
  }
  taosMemoryFree(values_list_sizes);

  TAOS_RETURN(code);
}

int tsdbCacheFlushDirty(const void *key, size_t klen, void *value, void *ud) {
  SLastCol *pLastCol = (SLastCol *)value;

  if (pLastCol->dirty) {
    STsdb *pTsdb = (STsdb *)ud;

    int32_t code = tsdbCachePutToRocksdb(pTsdb, (SLastKey *)key, pLastCol);
    if (code) {
      tsdbError("tsdb/cache: vgId:%d, flush dirty lru failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
      return code;
    }

    pLastCol->dirty = 0;

    rocksMayWrite(pTsdb, false);
  }

  return 0;
}

int32_t tsdbColCacheDel(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey) {
  int32_t   code = 0, lino = 0;
  STSchema *pTSchema = NULL;
  int       sver = -1;
  int       numKeys = 0;
  SArray   *remainCols = NULL;

  TAOS_CHECK_RETURN(metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, sver, &pTSchema));

  int numCols = pTSchema->numOfCols;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  for (int i = 0; i < numCols; ++i) {
    int16_t cid = pTSchema->columns[i].colId;
    for (int8_t lflag = LFLAG_LAST_ROW; lflag <= LFLAG_LAST; ++lflag) {
      SLastKey   lastKey = {.lflag = lflag, .uid = uid, .cid = cid};
      LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, &lastKey, ROCKS_KEY_LEN);
      if (h) {
        SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pTsdb->lruCache, h);
        if (pLastCol->rowKey.ts <= eKey && pLastCol->rowKey.ts >= sKey) {
          SLastCol noneCol = {.rowKey.ts = TSKEY_MIN,
                              .colVal = COL_VAL_NONE(cid, pTSchema->columns[i].type),
                              .dirty = 1,
                              .cacheStatus = TSDB_LAST_CACHE_NO_CACHE};
          code = tsdbCachePutToLRU(pTsdb, &lastKey, &noneCol, 1);
        }
        tsdbLRUCacheRelease(pTsdb->lruCache, h, false);
        TAOS_CHECK_EXIT(code);
      } else {
        if (!remainCols) {
          remainCols = taosArrayInit(numCols * 2, sizeof(SIdxKey));
        }
        if (!taosArrayPush(remainCols, &(SIdxKey){i, lastKey})) {
          TAOS_CHECK_EXIT(terrno);
        }
      }
    }
  }

  if (remainCols) {
    numKeys = TARRAY_SIZE(remainCols);
  }

  char  **keys_list = taosMemoryCalloc(numKeys, sizeof(char *));
  size_t *keys_list_sizes = taosMemoryCalloc(numKeys, sizeof(size_t));
  char  **values_list = NULL;
  size_t *values_list_sizes = NULL;

  if (!keys_list || !keys_list_sizes) {
    code = terrno;
    goto _exit;
  }
  const size_t klen = ROCKS_KEY_LEN;

  for (int i = 0; i < numKeys; ++i) {
    char *key = taosMemoryCalloc(1, sizeof(SLastKey));
    if (!key) {
      code = terrno;
      goto _exit;
    }
    SIdxKey *idxKey = taosArrayGet(remainCols, i);

    ((SLastKey *)key)[0] = idxKey->key;

    keys_list[i] = key;
    keys_list_sizes[i] = klen;
  }

  rocksMayWrite(pTsdb, true);  // flush writebatch cache

  TAOS_CHECK_GOTO(tsdbCacheGetValuesFromRocks(pTsdb, numKeys, (const char *const *)keys_list, keys_list_sizes,
                                              &values_list, &values_list_sizes),
                  NULL, _exit);

  // rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  for (int i = 0; i < numKeys; ++i) {
    SLastCol *pLastCol = NULL;
    if (values_list[i] != NULL) {
      code = tsdbCacheDeserialize(values_list[i], values_list_sizes[i], &pLastCol);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbError("vgId:%d, %s deserialize failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
        goto _exit;
      }
    }
    SIdxKey  *idxKey = taosArrayGet(remainCols, i);
    SLastKey *pLastKey = &idxKey->key;
    if (NULL != pLastCol && (pLastCol->rowKey.ts <= eKey && pLastCol->rowKey.ts >= sKey)) {
      SLastCol noCacheCol = {.rowKey.ts = TSKEY_MIN,
                             .colVal = COL_VAL_NONE(pLastKey->cid, pTSchema->columns[idxKey->idx].type),
                             .dirty = 0,
                             .cacheStatus = TSDB_LAST_CACHE_NO_CACHE};

      if ((code = tsdbCachePutToRocksdb(pTsdb, pLastKey, &noCacheCol)) != TSDB_CODE_SUCCESS) {
        taosMemoryFreeClear(pLastCol);
        tsdbError("tsdb/cache/del: vgId:%d, put to rocks failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
        goto _exit;
      }
      if ((code = tsdbCachePutToLRU(pTsdb, pLastKey, &noCacheCol, 0)) != TSDB_CODE_SUCCESS) {
        taosMemoryFreeClear(pLastCol);
        tsdbError("tsdb/cache/del: vgId:%d, put to lru failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
        goto _exit;
      }
    }

    if (pLastCol == NULL) {
      tsdbDebug("tsdb/cache/del: vgId:%d, no cache found for uid:%d ,cid:%" PRId64 ", lflag:%d.", TD_VID(pTsdb->pVnode),
                pLastKey->cid, pLastKey->uid, pLastKey->lflag);
    }

    taosMemoryFreeClear(pLastCol);
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
  taosArrayDestroy(remainCols);
  taosMemoryFree(pTSchema);

  TAOS_RETURN(code);
}

static int32_t tsdbCacheGetBatchFromLru(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SCacheRowsReader *pr,
                                        int8_t ltype, SArray *keyArray) {
  int32_t    code = 0, lino = 0;
  SArray    *remainCols = NULL;
  SArray    *ignoreFromRocks = NULL;
  SLRUCache *pCache = pTsdb->lruCache;
  SArray    *pCidList = pr->pCidList;
  int        numKeys = TARRAY_SIZE(pCidList);

  for (int i = 0; i < numKeys; ++i) {
    int16_t cid = ((int16_t *)TARRAY_DATA(pCidList))[i];

    SLastKey key = {.lflag = ltype, .uid = uid, .cid = cid};
    // for select last_row, last case
    int32_t funcType = FUNCTION_TYPE_CACHE_LAST;
    if (pr->pFuncTypeList != NULL && taosArrayGetSize(pr->pFuncTypeList) > i) {
      funcType = ((int32_t *)TARRAY_DATA(pr->pFuncTypeList))[i];
    }
    if (((pr->type & CACHESCAN_RETRIEVE_LAST) == CACHESCAN_RETRIEVE_LAST) && FUNCTION_TYPE_CACHE_LAST_ROW == funcType) {
      int8_t tempType = CACHESCAN_RETRIEVE_LAST_ROW | (pr->type ^ CACHESCAN_RETRIEVE_LAST);
      key.lflag = (tempType & CACHESCAN_RETRIEVE_LAST) >> 3;
    }

    if (!taosArrayPush(keyArray, &key)) {
      TAOS_CHECK_EXIT(terrno);
    }

    LRUHandle *h = taosLRUCacheLookup(pCache, &key, TSDB_CACHE_KEY_LEN);
    SLastCol  *pLastCol = h ? (SLastCol *)taosLRUCacheValue(pCache, h) : NULL;
    if (h && pLastCol->cacheStatus != TSDB_LAST_CACHE_NO_CACHE) {
      SLastCol lastCol = *pLastCol;
      if (TSDB_CODE_SUCCESS != (code = tsdbCacheReallocSLastCol(&lastCol, NULL))) {
        tsdbLRUCacheRelease(pCache, h, false);
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }

      if (taosArrayPush(pLastArray, &lastCol) == NULL) {
        code = terrno;
        tsdbLRUCacheRelease(pCache, h, false);
        goto _exit;
      }
    } else {
      // no cache or cache is invalid
      SLastCol noneCol = {.rowKey.ts = TSKEY_MIN,
                          .colVal = COL_VAL_NONE(cid, pr->pSchema->columns[pr->pSlotIds[i]].type)};

      if (taosArrayPush(pLastArray, &noneCol) == NULL) {
        code = terrno;
        tsdbLRUCacheRelease(pCache, h, false);
        goto _exit;
      }

      if (!remainCols) {
        if ((remainCols = taosArrayInit(numKeys, sizeof(SIdxKey))) == NULL) {
          code = terrno;
          tsdbLRUCacheRelease(pCache, h, false);
          goto _exit;
        }
      }
      if (!ignoreFromRocks) {
        if ((ignoreFromRocks = taosArrayInit(numKeys, sizeof(bool))) == NULL) {
          code = terrno;
          tsdbLRUCacheRelease(pCache, h, false);
          goto _exit;
        }
      }
      if (taosArrayPush(remainCols, &(SIdxKey){i, key}) == NULL) {
        code = terrno;
        tsdbLRUCacheRelease(pCache, h, false);
        goto _exit;
      }
      bool ignoreRocks = pLastCol ? (pLastCol->cacheStatus == TSDB_LAST_CACHE_NO_CACHE) : false;
      if (taosArrayPush(ignoreFromRocks, &ignoreRocks) == NULL) {
        code = terrno;
        tsdbLRUCacheRelease(pCache, h, false);
        goto _exit;
      }
    }

    if (h) {
      tsdbLRUCacheRelease(pCache, h, false);
    }
  }

  if (remainCols && TARRAY_SIZE(remainCols) > 0) {
    (void)taosThreadMutexLock(&pTsdb->lruMutex);

    for (int i = 0; i < TARRAY_SIZE(remainCols);) {
      SIdxKey   *idxKey = &((SIdxKey *)TARRAY_DATA(remainCols))[i];
      LRUHandle *h = taosLRUCacheLookup(pCache, &idxKey->key, TSDB_CACHE_KEY_LEN);
      SLastCol  *pLastCol = h ? (SLastCol *)taosLRUCacheValue(pCache, h) : NULL;
      if (h && pLastCol->cacheStatus != TSDB_LAST_CACHE_NO_CACHE) {
        SLastCol lastCol = *pLastCol;
        code = tsdbCacheReallocSLastCol(&lastCol, NULL);
        if (code) {
          tsdbLRUCacheRelease(pCache, h, false);
          (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
          TAOS_RETURN(code);
        }

        taosArraySet(pLastArray, idxKey->idx, &lastCol);

        taosArrayRemove(remainCols, i);
        taosArrayRemove(ignoreFromRocks, i);
      } else {
        // no cache or cache is invalid
        ++i;
      }
      if (h) {
        tsdbLRUCacheRelease(pCache, h, false);
      }
    }

    // tsdbTrace("tsdb/cache: vgId: %d, load %" PRId64 " from rocks", TD_VID(pTsdb->pVnode), uid);
    code = tsdbCacheLoadFromRocks(pTsdb, uid, pLastArray, remainCols, ignoreFromRocks, pr, ltype);

    (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
  }

_exit:
  if (remainCols) {
    taosArrayDestroy(remainCols);
  }
  if (ignoreFromRocks) {
    taosArrayDestroy(ignoreFromRocks);
  }

  TAOS_RETURN(code);
}