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
#include "tsdbIter.h"
#include "tsdbReadUtil.h"
#include "vnd.h"

#define ROCKS_BATCH_SIZE (4096)

static int32_t tsdbOpenBCache(STsdb *pTsdb) {
  int32_t    code = 0, lino = 0;
  int32_t    szPage = pTsdb->pVnode->config.tsdbPageSize;
  int64_t    szBlock = tsS3BlockSize <= 1024 ? 1024 : tsS3BlockSize;
  SLRUCache *pCache = taosLRUCacheInit((int64_t)tsS3BlockCacheSize * szBlock * szPage, 0, .5);
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

  SLRUCache *pCache = taosLRUCacheInit((int64_t)tsS3PageCacheSize * szPage, 0, .5);
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

#define ROCKS_KEY_LEN (sizeof(tb_uid_t) + sizeof(int16_t) + sizeof(int8_t))

enum {
  LFLAG_LAST_ROW = 0,
  LFLAG_LAST = 1,
};

typedef struct {
  tb_uid_t uid;
  int16_t  cid;
  int8_t   lflag;
} SLastKey;

#define IS_LAST_ROW_KEY(k) (((k).lflag & LFLAG_LAST) == LFLAG_LAST_ROW)
#define IS_LAST_KEY(k)     (((k).lflag & LFLAG_LAST) == LFLAG_LAST)

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

  if ((lhs->lflag & LFLAG_LAST) < (rhs->lflag & LFLAG_LAST)) {
    return -1;
  } else if ((lhs->lflag & LFLAG_LAST) > (rhs->lflag & LFLAG_LAST)) {
    return 1;
  }

  return 0;
}

static int32_t tsdbOpenRocksCache(STsdb *pTsdb) {
  int32_t code = 0, lino = 0;

  rocksdb_comparator_t *cmp = rocksdb_comparator_create(NULL, myCmpDestroy, myCmp, myCmpName);
  if (NULL == cmp) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  rocksdb_cache_t *cache = rocksdb_cache_create_lru(5 * 1024 * 1024);
  pTsdb->rCache.blockcache = cache;

  rocksdb_block_based_table_options_t *tableoptions = rocksdb_block_based_options_create();
  pTsdb->rCache.tableoptions = tableoptions;

  rocksdb_options_t *options = rocksdb_options_create();
  if (NULL == options) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err);
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
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err2);
  }
  rocksdb_writeoptions_disable_WAL(writeoptions, 1);

  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  if (NULL == readoptions) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err3);
  }

  char *err = NULL;
  char  cachePath[TSDB_FILENAME_LEN] = {0};
  tsdbGetRocksPath(pTsdb, cachePath);

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
  rocksdb_writebatch_t *rwritebatch = rocksdb_writebatch_create();

  pTsdb->rCache.writebatch = writebatch;
  pTsdb->rCache.rwritebatch = rwritebatch;
  pTsdb->rCache.my_comparator = cmp;
  pTsdb->rCache.options = options;
  pTsdb->rCache.writeoptions = writeoptions;
  pTsdb->rCache.readoptions = readoptions;
  pTsdb->rCache.flushoptions = flushoptions;
  pTsdb->rCache.db = db;
  pTsdb->rCache.pTSchema = NULL;

  TAOS_RETURN(code);

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

  TAOS_RETURN(code);
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
  taosMemoryFree(pTsdb->rCache.pTSchema);
}

static void rocksMayWrite(STsdb *pTsdb, bool force, bool read) {
  rocksdb_writebatch_t *wb = read ? pTsdb->rCache.rwritebatch : pTsdb->rCache.writebatch;

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
}

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
  } else {
    pLastCol->colVal.value.val = pLastColV0->colVal.value.val;
    return sizeof(SLastColV0);
  }
}

static int32_t tsdbCacheDeserialize(char const *value, size_t size, SLastCol **ppLastCol) {
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
  *size += sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t);  // version + numOfPKs + cacheStatus

  for (int8_t i = 0; i < pLastCol->rowKey.numOfPKs; i++) {
    *size += sizeof(SValue);
    if (IS_VAR_DATA_TYPE(pLastCol->rowKey.pks[i].type)) {
      *size += pLastCol->rowKey.pks[i].nData;
    }
  }

  *value = taosMemoryMalloc(*size);
  if (NULL == *value) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
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

static void tsdbCachePutBatch(SLastCol *pLastCol, const void *key, size_t klen, SCacheFlushState *state) {
  int32_t               code = 0;
  STsdb                *pTsdb = state->pTsdb;
  SRocksCache          *rCache = &pTsdb->rCache;
  rocksdb_writebatch_t *wb = rCache->writebatch;
  char                 *rocks_value = NULL;
  size_t                vlen = 0;

  code = tsdbCacheSerialize(pLastCol, &rocks_value, &vlen);
  if (code) {
    tsdbError("tsdb/cache: vgId:%d, serialize failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
    return;
  }

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

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  taosLRUCacheApply(pCache, tsdbCacheFlushDirty, &pTsdb->flushState);

  rocksMayWrite(pTsdb, true, false);
  rocksMayWrite(pTsdb, true, true);
  rocksdb_flush(pTsdb->rCache.db, pTsdb->rCache.flushoptions, &err);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  if (NULL != err) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, err);
    rocksdb_free(err);
    code = TSDB_CODE_FAILED;
  }

  TAOS_RETURN(code);
}

static int32_t reallocVarDataVal(SValue *pValue) {
  if (IS_VAR_DATA_TYPE(pValue->type)) {
    uint8_t *pVal = pValue->pData;
    uint32_t nData = pValue->nData;
    if (nData > 0) {
      uint8_t *p = taosMemoryMalloc(nData);
      if (!p) {
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
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
static int32_t tsdbCacheReallocSLastCol(SLastCol *pCol, size_t *pCharge) {
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

void tsdbCacheFreeSLastColItem(void *pItem) {
  SLastCol *pCol = (SLastCol *)pItem;
  for (int i = 0; i < pCol->rowKey.numOfPKs; i++) {
    if (IS_VAR_DATA_TYPE(pCol->rowKey.pks[i].type)) {
      taosMemoryFree(pCol->rowKey.pks[i].pData);
    }
  }

  if (IS_VAR_DATA_TYPE(pCol->colVal.value.type) && pCol->colVal.value.pData) {
    taosMemoryFree(pCol->colVal.value.pData);
  }
}

static void tsdbCacheDeleter(const void *key, size_t klen, void *value, void *ud) {
  SLastCol *pLastCol = (SLastCol *)value;

  if (pLastCol->dirty) {
    tsdbCachePutBatch(pLastCol, key, klen, (SCacheFlushState *)ud);
  }

  for (uint8_t i = 0; i < pLastCol->rowKey.numOfPKs; ++i) {
    SValue *pValue = &pLastCol->rowKey.pks[i];
    if (IS_VAR_DATA_TYPE(pValue->type)) {
      taosMemoryFree(pValue->pData);
    }
  }

  if (IS_VAR_DATA_TYPE(pLastCol->colVal.value.type) /* && pLastCol->colVal.value.nData > 0*/) {
    taosMemoryFree(pLastCol->colVal.value.pData);
  }

  taosMemoryFree(value);
}

static int32_t tsdbCacheNewTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type, int8_t lflag) {
  int32_t code = 0, lino = 0;

  SLRUCache            *pCache = pTsdb->lruCache;
  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  SRowKey               emptyRowKey = {.ts = TSKEY_MIN, .numOfPKs = 0};
  SLastCol              emptyCol = {
                   .rowKey = emptyRowKey, .colVal = COL_VAL_NONE(cid, col_type), .dirty = 1, .cacheStatus = TSDB_LAST_CACHE_VALID};

  SLastCol *pLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
  if (!pLastCol) {
    return terrno;
  }

  size_t charge = 0;
  *pLastCol = emptyCol;
  TAOS_CHECK_EXIT(tsdbCacheReallocSLastCol(pLastCol, &charge));

  SLastKey *pLastKey = &(SLastKey){.lflag = lflag, .uid = uid, .cid = cid};
  LRUStatus status = taosLRUCacheInsert(pCache, pLastKey, ROCKS_KEY_LEN, pLastCol, charge, tsdbCacheDeleter, NULL,
                                        TAOS_LRU_PRIORITY_LOW, &pTsdb->flushState);
  if (status != TAOS_LRU_STATUS_OK) {
    tsdbError("vgId:%d, %s failed at line %d status %d.", TD_VID(pTsdb->pVnode), __func__, __LINE__, status);
    tsdbCacheFreeSLastColItem(pLastCol);
    code = TSDB_CODE_FAILED;
  }

_exit:
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(pLastCol);
  }

  TAOS_RETURN(code);
}

int32_t tsdbCacheCommitNoLock(STsdb *pTsdb) {
  int32_t code = 0;
  char   *err = NULL;

  SLRUCache            *pCache = pTsdb->lruCache;
  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;

  taosLRUCacheApply(pCache, tsdbCacheFlushDirty, &pTsdb->flushState);

  rocksMayWrite(pTsdb, true, false);
  rocksMayWrite(pTsdb, true, true);
  rocksdb_flush(pTsdb->rCache.db, pTsdb->rCache.flushoptions, &err);

  if (NULL != err) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, err);
    rocksdb_free(err);
    code = TSDB_CODE_FAILED;
  }

  TAOS_RETURN(code);
}

static int32_t tsdbCacheGetValuesFromRocks(STsdb *pTsdb, size_t numKeys, const char *const *ppKeysList,
                                           size_t *pKeysListSizes, char ***pppValuesList, size_t **ppValuesListSizes) {
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
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t tsdbCacheDropTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, bool hasPrimaryKey) {
  int32_t code = 0;

  // build keys & multi get from rocks
  char **keys_list = taosMemoryCalloc(2, sizeof(char *));
  if (!keys_list) {
    return terrno;
  }
  size_t *keys_list_sizes = taosMemoryCalloc(2, sizeof(size_t));
  if (!keys_list_sizes) {
    taosMemoryFree(keys_list);
    return terrno;
  }
  const size_t klen = ROCKS_KEY_LEN;

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
  TAOS_CHECK_GOTO(tsdbCacheGetValuesFromRocks(pTsdb, 2, (const char *const *)keys_list, keys_list_sizes, &values_list,
                                              &values_list_sizes),
                  NULL, _exit);

  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  {
    SLastCol *pLastCol = NULL;
    int32_t code = tsdbCacheDeserialize(values_list[0], values_list_sizes[0], &pLastCol);
    if (NULL != pLastCol && code == TSDB_CODE_SUCCESS) {
      rocksdb_writebatch_delete(wb, keys_list[0], klen);
    }
    taosMemoryFreeClear(pLastCol);

    pLastCol = NULL;
    code = tsdbCacheDeserialize(values_list[1], values_list_sizes[1], &pLastCol);
    if (NULL != pLastCol && code == TSDB_CODE_SUCCESS) {
      rocksdb_writebatch_delete(wb, keys_list[1], klen);
    }
    taosMemoryFreeClear(pLastCol);

    rocksdb_free(values_list[0]);
    rocksdb_free(values_list[1]);

    for (int i = 0; i < 2; i++) {
      LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, keys_list[i], klen);
      if (h) {
        TAOS_UNUSED(taosLRUCacheRelease(pTsdb->lruCache, h, true));
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

int32_t tsdbCacheNewTable(STsdb *pTsdb, tb_uid_t uid, tb_uid_t suid, SSchemaWrapper *pSchemaRow) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  if (suid < 0) {
    for (int i = 0; i < pSchemaRow->nCols; ++i) {
      int16_t cid = pSchemaRow->pSchema[i].colId;
      int8_t  col_type = pSchemaRow->pSchema[i].type;

      TAOS_UNUSED(tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, LFLAG_LAST_ROW));
      TAOS_UNUSED(tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, LFLAG_LAST));
    }
  } else {
    STSchema *pTSchema = NULL;
    code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, -1, &pTSchema);
    if (code != TSDB_CODE_SUCCESS) {
      (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

      TAOS_RETURN(code);
    }

    for (int i = 0; i < pTSchema->numOfCols; ++i) {
      int16_t cid = pTSchema->columns[i].colId;
      int8_t  col_type = pTSchema->columns[i].type;

      TAOS_UNUSED(tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, LFLAG_LAST_ROW));
      TAOS_UNUSED(tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, LFLAG_LAST));
    }

    taosMemoryFree(pTSchema);
  }

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheDropTable(STsdb *pTsdb, tb_uid_t uid, tb_uid_t suid, SSchemaWrapper *pSchemaRow) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  TAOS_UNUSED(tsdbCacheCommitNoLock(pTsdb));

  if (pSchemaRow != NULL) {
    bool hasPrimayKey = false;
    int  nCols = pSchemaRow->nCols;
    if (nCols >= 2) {
      hasPrimayKey = (pSchemaRow->pSchema[1].flags & COL_IS_KEY) ? true : false;
    }
    for (int i = 0; i < nCols; ++i) {
      int16_t cid = pSchemaRow->pSchema[i].colId;
      int8_t  col_type = pSchemaRow->pSchema[i].type;

      TAOS_UNUSED(tsdbCacheDropTableColumn(pTsdb, uid, cid, hasPrimayKey));
    }
  } else {
    STSchema *pTSchema = NULL;
    code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, -1, &pTSchema);
    if (code != TSDB_CODE_SUCCESS) {
      (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

      TAOS_RETURN(code);
    }

    bool hasPrimayKey = false;
    int  nCols = pTSchema->numOfCols;
    if (nCols >= 2) {
      hasPrimayKey = (pTSchema->columns[1].flags & COL_IS_KEY) ? true : false;
    }
    for (int i = 0; i < nCols; ++i) {
      int16_t cid = pTSchema->columns[i].colId;
      int8_t  col_type = pTSchema->columns[i].type;

      TAOS_UNUSED(tsdbCacheDropTableColumn(pTsdb, uid, cid, hasPrimayKey));
    }

    taosMemoryFree(pTSchema);
  }

  rocksMayWrite(pTsdb, true, false);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheDropSubTables(STsdb *pTsdb, SArray *uids, tb_uid_t suid) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  TAOS_UNUSED(tsdbCacheCommitNoLock(pTsdb));

  STSchema *pTSchema = NULL;
  code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, suid, -1, &pTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

    TAOS_RETURN(code);
  }

  for (int i = 0; i < TARRAY_SIZE(uids); ++i) {
    int64_t uid = ((tb_uid_t *)TARRAY_DATA(uids))[i];

    bool hasPrimayKey = false;
    int  nCols = pTSchema->numOfCols;
    if (nCols >= 2) {
      hasPrimayKey = (pTSchema->columns[1].flags & COL_IS_KEY) ? true : false;
    }

    for (int i = 0; i < nCols; ++i) {
      int16_t cid = pTSchema->columns[i].colId;
      int8_t  col_type = pTSchema->columns[i].type;

      TAOS_UNUSED(tsdbCacheDropTableColumn(pTsdb, uid, cid, hasPrimayKey));
    }
  }

  taosMemoryFree(pTSchema);

  rocksMayWrite(pTsdb, true, false);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheNewNTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  TAOS_UNUSED(tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 0));
  TAOS_UNUSED(tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 1));

  // rocksMayWrite(pTsdb, true, false, false);
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheDropNTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, bool hasPrimayKey) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  TAOS_UNUSED(tsdbCacheCommitNoLock(pTsdb));

  TAOS_UNUSED(tsdbCacheDropTableColumn(pTsdb, uid, cid, hasPrimayKey));

  rocksMayWrite(pTsdb, true, false);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheNewSTableColumn(STsdb *pTsdb, SArray *uids, int16_t cid, int8_t col_type) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  for (int i = 0; i < TARRAY_SIZE(uids); ++i) {
    tb_uid_t uid = ((tb_uid_t *)TARRAY_DATA(uids))[i];

    TAOS_UNUSED(tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 0));
    TAOS_UNUSED(tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 1));
  }

  // rocksMayWrite(pTsdb, true, false, false);
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheDropSTableColumn(STsdb *pTsdb, SArray *uids, int16_t cid, bool hasPrimayKey) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  TAOS_UNUSED(tsdbCacheCommitNoLock(pTsdb));

  for (int i = 0; i < TARRAY_SIZE(uids); ++i) {
    int64_t uid = ((tb_uid_t *)TARRAY_DATA(uids))[i];

    TAOS_UNUSED(tsdbCacheDropTableColumn(pTsdb, uid, cid, hasPrimayKey));
  }

  rocksMayWrite(pTsdb, true, false);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

typedef struct {
  int      idx;
  SLastKey key;
} SIdxKey;

static int32_t tsdbCacheUpdateValue(SValue *pOld, SValue *pNew) {
  uint8_t *pFree = NULL;
  int      nData = 0;

  if (IS_VAR_DATA_TYPE(pOld->type)) {
    pFree = pOld->pData;
    nData = pOld->nData;
  }

  *pOld = *pNew;
  if (IS_VAR_DATA_TYPE(pNew->type)) {
    if (nData < pNew->nData) {
      pOld->pData = taosMemoryCalloc(1, pNew->nData);
      if (!pOld->pData) {
        return terrno;
      }
    } else {
      pOld->pData = pFree;
      pFree = NULL;
    }

    if (pNew->nData) {
      memcpy(pOld->pData, pNew->pData, pNew->nData);
    } else {
      pFree = pOld->pData;
      pOld->pData = NULL;
    }
  }

  taosMemoryFreeClear(pFree);

  TAOS_RETURN(TSDB_CODE_SUCCESS);
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
      pPKValue->val = 0;
    }
  }
  pLastCol->rowKey.numOfPKs = 0;

  // update colval
  if (IS_VAR_DATA_TYPE(pLastCol->colVal.value.type) && pLastCol->colVal.value.nData > 0) {
    taosMemoryFreeClear(pLastCol->colVal.value.pData);
    pLastCol->colVal.value.nData = 0;
  } else {
    pLastCol->colVal.value.val = 0;
  }

  pLastCol->colVal = COL_VAL_NONE(pLastCol->colVal.cid, pLastCol->colVal.value.type);

  if (!pLastCol->dirty) {
    pLastCol->dirty = 1;
  }

  pLastCol->cacheStatus = cacheStatus;
}

static int32_t tsdbCachePutToRocksdb(STsdb *pTsdb, SLastKey *pLastKey, SLastCol *pLastCol) {
  int32_t code = 0;
  char   *rocks_value = NULL;
  size_t  vlen = 0;

  code = tsdbCacheSerialize(pLastCol, &rocks_value, &vlen);
  if (code) {
    tsdbError("tsdb/cache/putrocks: vgId:%d, serialize failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
    TAOS_RETURN(code);
  }

  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  rocksdb_writebatch_put(wb, (char *)pLastKey, ROCKS_KEY_LEN, rocks_value, vlen);

  taosMemoryFree(rocks_value);

  TAOS_RETURN(code);
}

static int32_t tsdbCachePutToLRU(STsdb *pTsdb, SLastKey *pLastKey, SLastCol *pLastCol) {
  int32_t code = 0, lino = 0;

  SLastCol *pLRULastCol = taosMemoryCalloc(1, sizeof(SLastCol));
  if (!pLRULastCol) {
    return terrno;
  }

  size_t charge = 0;
  *pLRULastCol = *pLastCol;
  pLRULastCol->dirty = 1;
  TAOS_CHECK_EXIT(tsdbCacheReallocSLastCol(pLRULastCol, &charge));

  LRUStatus status = taosLRUCacheInsert(pTsdb->lruCache, pLastKey, ROCKS_KEY_LEN, pLRULastCol, charge, tsdbCacheDeleter,
                                        NULL, TAOS_LRU_PRIORITY_LOW, &pTsdb->flushState);
  if (TAOS_LRU_STATUS_OK != status && TAOS_LRU_STATUS_OK_OVERWRITTEN != status) {
    tsdbError("vgId:%d, %s failed at line %d status %d.", TD_VID(pTsdb->pVnode), __func__, __LINE__, status);
    tsdbCacheFreeSLastColItem(pLRULastCol);
    code = TSDB_CODE_FAILED;
  }

_exit:
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(pLRULastCol);
    tsdbError("tsdb/cache/putlru: vgId:%d, failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t tsdbCacheUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SArray *updCtxArray) {
  if (!updCtxArray || TARRAY_SIZE(updCtxArray) == 0) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  int32_t code = 0, lino = 0;

  int        num_keys = TARRAY_SIZE(updCtxArray);
  SArray    *remainCols = NULL;
  SLRUCache *pCache = pTsdb->lruCache;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);
  for (int i = 0; i < num_keys; ++i) {
    SLastUpdateCtx *updCtx = (SLastUpdateCtx *)taosArrayGet(updCtxArray, i);

    int8_t   lflag = updCtx->lflag;
    SRowKey *pRowKey = &updCtx->tsdbRowKey.key;
    SColVal *pColVal = &updCtx->colVal;

    if (lflag == LFLAG_LAST && !COL_VAL_IS_VALUE(pColVal)) {
      continue;
    }

    SLastKey  *key = &(SLastKey){.lflag = lflag, .uid = uid, .cid = pColVal->cid};
    size_t     klen = ROCKS_KEY_LEN;
    LRUHandle *h = taosLRUCacheLookup(pCache, key, klen);
    if (h) {
      SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pCache, h);
      if (pLastCol->cacheStatus != TSDB_LAST_CACHE_NO_CACHE) {
        int32_t cmp_res = tRowKeyCompare(&pLastCol->rowKey, pRowKey);
        if (cmp_res < 0 || (cmp_res == 0 && !COL_VAL_IS_NONE(pColVal))) {
          SLastCol newLastCol = {.rowKey = *pRowKey, .colVal = *pColVal, .cacheStatus = TSDB_LAST_CACHE_VALID};
          code = tsdbCachePutToLRU(pTsdb, key, &newLastCol);
        }
      }

      TAOS_UNUSED(taosLRUCacheRelease(pCache, h, false));
      TAOS_CHECK_EXIT(code);
    } else {
      if (!remainCols) {
        remainCols = taosArrayInit(num_keys * 2, sizeof(SIdxKey));
        if (!remainCols) {
          TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
        }
      }
      if (!taosArrayPush(remainCols, &(SIdxKey){i, *key})) {
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
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
      keys_list_sizes[i] = ROCKS_KEY_LEN;
    }

    code = tsdbCacheGetValuesFromRocks(pTsdb, num_keys, (const char *const *)keys_list, keys_list_sizes, &values_list,
                                       &values_list_sizes);
    if (code) {
      taosMemoryFree(keys_list);
      taosMemoryFree(keys_list_sizes);
      goto _exit;
    }

    rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
    for (int i = 0; i < num_keys; ++i) {
      SIdxKey        *idxKey = &((SIdxKey *)TARRAY_DATA(remainCols))[i];
      SLastUpdateCtx *updCtx = (SLastUpdateCtx *)taosArrayGet(updCtxArray, idxKey->idx);
      SRowKey        *pRowKey = &updCtx->tsdbRowKey.key;
      SColVal        *pColVal = &updCtx->colVal;

      SLastCol *pLastCol = NULL;
      code = tsdbCacheDeserialize(values_list[i], values_list_sizes[i], &pLastCol);
      if (code) {
        tsdbError("tsdb/cache: vgId:%d, deserialize failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
      }
      SLastCol *pToFree = pLastCol;

      if (pLastCol && pLastCol->cacheStatus == TSDB_LAST_CACHE_NO_CACHE) {
        if ((code = tsdbCachePutToLRU(pTsdb, &idxKey->key, pLastCol)) != TSDB_CODE_SUCCESS) {
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
        SLastCol lastColTmp = {.rowKey = *pRowKey, .colVal = *pColVal, .cacheStatus = TSDB_LAST_CACHE_VALID};
        if ((code = tsdbCachePutToRocksdb(pTsdb, &idxKey->key, &lastColTmp)) != TSDB_CODE_SUCCESS) {
          tsdbError("tsdb/cache: vgId:%d, put rocks failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino,
                    tstrerror(code));
          taosMemoryFreeClear(pToFree);
          break;
        }
        if ((code = tsdbCachePutToLRU(pTsdb, &idxKey->key, &lastColTmp)) != TSDB_CODE_SUCCESS) {
          tsdbError("tsdb/cache: vgId:%d, put lru failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino,
                    tstrerror(code));
          taosMemoryFreeClear(pToFree);
          break;
        }
      }

      taosMemoryFreeClear(pToFree);
    }

    rocksMayWrite(pTsdb, true, false);

    taosMemoryFree(keys_list);
    taosMemoryFree(keys_list_sizes);
    if (values_list) {
      for (int i = 0; i < num_keys; ++i) {
        rocksdb_free(values_list[i]);
      }
      taosMemoryFree(values_list);
    }
    taosMemoryFree(values_list_sizes);
  }

_exit:
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
  taosArrayDestroy(remainCols);

  if (code) {
    tsdbError("tsdb/cache: vgId:%d, update failed at line %d since %s.", TD_VID(pTsdb->pVnode), lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

int32_t tsdbCacheRowFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, int64_t version, int32_t nRow,
                                 SRow **aRow) {
  int32_t code = 0, lino = 0;

  // 1. prepare last
  TSDBROW lRow = {.type = TSDBROW_ROW_FMT, .pTSRow = aRow[nRow - 1], .version = version};

  STSchema  *pTSchema = NULL;
  int32_t    sver = TSDBROW_SVERSION(&lRow);
  SArray    *ctxArray = NULL;
  SSHashObj *iColHash = NULL;

  TAOS_CHECK_GOTO(metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, sver, &pTSchema), &lino, _exit);

  TSDBROW tRow = {.type = TSDBROW_ROW_FMT, .version = version};
  int32_t nCol = pTSchema->numOfCols;

  ctxArray = taosArrayInit(nCol, sizeof(SLastUpdateCtx));
  iColHash = tSimpleHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));

  // 1. prepare by lrow
  STsdbRowKey tsdbRowKey = {0};
  tsdbRowGetKey(&lRow, &tsdbRowKey);

  STSDBRowIter iter = {0};
  TAOS_CHECK_GOTO(tsdbRowIterOpen(&iter, &lRow, pTSchema), &lino, _exit);
  int32_t iCol = 0;
  for (SColVal *pColVal = tsdbRowIterNext(&iter); pColVal && iCol < nCol; pColVal = tsdbRowIterNext(&iter), iCol++) {
    SLastUpdateCtx updateCtx = {.lflag = LFLAG_LAST_ROW, .tsdbRowKey = tsdbRowKey, .colVal = *pColVal};
    if (!taosArrayPush(ctxArray, &updateCtx)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }

    if (!COL_VAL_IS_VALUE(pColVal)) {
      if (tSimpleHashPut(iColHash, &iCol, sizeof(iCol), NULL, 0)) {
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
      }
      continue;
    }
    updateCtx.lflag = LFLAG_LAST;
    if (!taosArrayPush(ctxArray, &updateCtx)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
  }
  tsdbRowClose(&iter);

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
          TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
        }
        TAOS_UNUSED(tSimpleHashIterateRemove(iColHash, &iCol, sizeof(iCol), &pIte, &iter));
      }
    }
  }

  // 3. do update
  TAOS_UNUSED(tsdbCacheUpdate(pTsdb, suid, uid, ctxArray));

_exit:
  taosMemoryFreeClear(pTSchema);
  taosArrayDestroy(ctxArray);
  tSimpleHashCleanup(iColHash);

  TAOS_RETURN(code);
}

int32_t tsdbCacheColFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SBlockData *pBlockData) {
  int32_t code = 0, lino = 0;

  TSDBROW lRow = tsdbRowFromBlockData(pBlockData, pBlockData->nRow - 1);

  STSchema *pTSchema = NULL;
  int32_t   sver = TSDBROW_SVERSION(&lRow);
  SArray   *ctxArray = NULL;

  TAOS_CHECK_RETURN(metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, sver, &pTSchema));

  ctxArray = taosArrayInit(pBlockData->nColData, sizeof(SLastUpdateCtx));

  // 1. prepare last
  STsdbRowKey tsdbRowKey = {0};
  tsdbRowGetKey(&lRow, &tsdbRowKey);

  {
    SLastUpdateCtx updateCtx = {
        .lflag = LFLAG_LAST,
        .tsdbRowKey = tsdbRowKey,
        .colVal = COL_VAL_VALUE(PRIMARYKEY_TIMESTAMP_COL_ID, ((SValue){.type = TSDB_DATA_TYPE_TIMESTAMP,
                                                                       .val = lRow.pBlockData->aTSKEY[lRow.iRow]}))};
    if (!taosArrayPush(ctxArray, &updateCtx)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
  }

  TSDBROW tRow = tsdbRowFromBlockData(pBlockData, 0);

  for (int32_t iColData = 0; iColData < pBlockData->nColData; ++iColData) {
    SColData *pColData = &pBlockData->aColData[iColData];
    if ((pColData->flag & HAS_VALUE) != HAS_VALUE) {
      continue;
    }

    for (tRow.iRow = pBlockData->nRow - 1; tRow.iRow >= 0; --tRow.iRow) {
      STsdbRowKey tsdbRowKey = {0};
      tsdbRowGetKey(&tRow, &tsdbRowKey);

      uint8_t colType = tColDataGetBitValue(pColData, tRow.iRow);
      if (colType == 2) {
        SColVal colVal = COL_VAL_NONE(pColData->cid, pColData->type);
        tColDataGetValue(pColData, tRow.iRow, &colVal);

        SLastUpdateCtx updateCtx = {.lflag = LFLAG_LAST, .tsdbRowKey = tsdbRowKey, .colVal = colVal};
        if (!taosArrayPush(ctxArray, &updateCtx)) {
          TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
        }
        break;
      }
    }
  }

  // 2. prepare last row
  STSDBRowIter iter = {0};
  TAOS_CHECK_GOTO(tsdbRowIterOpen(&iter, &lRow, pTSchema), &lino, _exit);
  for (SColVal *pColVal = tsdbRowIterNext(&iter); pColVal; pColVal = tsdbRowIterNext(&iter)) {
    SLastUpdateCtx updateCtx = {.lflag = LFLAG_LAST_ROW, .tsdbRowKey = tsdbRowKey, .colVal = *pColVal};
    if (!taosArrayPush(ctxArray, &updateCtx)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
  }
  tsdbRowClose(&iter);

  // 3. do update
  TAOS_UNUSED(tsdbCacheUpdate(pTsdb, suid, uid, ctxArray));

_exit:
  taosMemoryFreeClear(pTSchema);
  taosArrayDestroy(ctxArray);

  TAOS_RETURN(code);
}

static int32_t mergeLastCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                            int nCols, int16_t *slotIds);

static int32_t mergeLastRowCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                               int nCols, int16_t *slotIds);

static int32_t tsdbCacheLoadFromRaw(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SArray *remainCols,
                                    SCacheRowsReader *pr, int8_t ltype) {
  int32_t               code = 0, lino = 0;
  rocksdb_writebatch_t *wb = NULL;
  SArray               *pTmpColArray = NULL;

  SIdxKey *idxKey = taosArrayGet(remainCols, 0);
  if (idxKey->key.cid != PRIMARYKEY_TIMESTAMP_COL_ID) {
    // ignore 'ts' loaded from cache and load it from tsdb
    SLastCol *pLastCol = taosArrayGet(pLastArray, 0);
    tsdbCacheUpdateLastColToNone(pLastCol, TSDB_LAST_CACHE_NO_CACHE);

    SLastKey *key = &(SLastKey){.lflag = ltype, .uid = uid, .cid = PRIMARYKEY_TIMESTAMP_COL_ID};
    if (!taosArrayInsert(remainCols, 0, &(SIdxKey){0, *key})) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  int      num_keys = TARRAY_SIZE(remainCols);
  int16_t *slotIds = taosMemoryMalloc(num_keys * sizeof(int16_t));

  int16_t *lastColIds = NULL;
  int16_t *lastSlotIds = NULL;
  int16_t *lastrowColIds = NULL;
  int16_t *lastrowSlotIds = NULL;
  lastColIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  lastSlotIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  lastrowColIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  lastrowSlotIds = taosMemoryMalloc(num_keys * sizeof(int16_t));
  SArray *lastTmpColArray = NULL;
  SArray *lastTmpIndexArray = NULL;
  SArray *lastrowTmpColArray = NULL;
  SArray *lastrowTmpIndexArray = NULL;

  int lastIndex = 0;
  int lastrowIndex = 0;

  if (!slotIds || !lastColIds || !lastSlotIds || !lastrowColIds || !lastrowSlotIds) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  for (int i = 0; i < num_keys; ++i) {
    SIdxKey *idxKey = taosArrayGet(remainCols, i);
    slotIds[i] = pr->pSlotIds[idxKey->idx];
    if (IS_LAST_KEY(idxKey->key)) {
      if (NULL == lastTmpIndexArray) {
        lastTmpIndexArray = taosArrayInit(num_keys, sizeof(int32_t));
        if (!lastTmpIndexArray) {
          TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
        }
      }
      if (!taosArrayPush(lastTmpIndexArray, &(i))) {
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
      lastColIds[lastIndex] = idxKey->key.cid;
      lastSlotIds[lastIndex] = pr->pSlotIds[idxKey->idx];
      lastIndex++;
    } else {
      if (NULL == lastrowTmpIndexArray) {
        lastrowTmpIndexArray = taosArrayInit(num_keys, sizeof(int32_t));
        if (!lastrowTmpIndexArray) {
          TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
        }
      }
      if (!taosArrayPush(lastrowTmpIndexArray, &(i))) {
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
      lastrowColIds[lastrowIndex] = idxKey->key.cid;
      lastrowSlotIds[lastrowIndex] = pr->pSlotIds[idxKey->idx];
      lastrowIndex++;
    }
  }

  pTmpColArray = taosArrayInit(lastIndex + lastrowIndex, sizeof(SLastCol));
  if (!pTmpColArray) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (lastTmpIndexArray != NULL) {
    TAOS_CHECK_EXIT(mergeLastCid(uid, pTsdb, &lastTmpColArray, pr, lastColIds, lastIndex, lastSlotIds));
    for (int i = 0; i < taosArrayGetSize(lastTmpColArray); i++) {
      if (!taosArrayInsert(pTmpColArray, *(int32_t *)taosArrayGet(lastTmpIndexArray, i),
                           taosArrayGet(lastTmpColArray, i))) {
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
    }
  }

  if (lastrowTmpIndexArray != NULL) {
    TAOS_CHECK_EXIT(mergeLastRowCid(uid, pTsdb, &lastrowTmpColArray, pr, lastrowColIds, lastrowIndex, lastrowSlotIds));
    for (int i = 0; i < taosArrayGetSize(lastrowTmpColArray); i++) {
      if (!taosArrayInsert(pTmpColArray, *(int32_t *)taosArrayGet(lastrowTmpIndexArray, i),
                           taosArrayGet(lastrowTmpColArray, i))) {
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
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

    taosArraySet(pLastArray, idxKey->idx, pLastCol);
    // taosArrayRemove(remainCols, i);

    if (/*!pTmpColArray*/ lastTmpIndexArray && !lastTmpColArray) {
      continue;
    }
    if (/*!pTmpColArray*/ lastrowTmpIndexArray && !lastrowTmpColArray) {
      continue;
    }

    SLastCol *pTmpLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
    if (!pTmpLastCol) {
      TAOS_CHECK_EXIT(terrno);
    }

    size_t charge = 0;
    *pTmpLastCol = *pLastCol;
    pLastCol = pTmpLastCol;
    code = tsdbCacheReallocSLastCol(pLastCol, &charge);
    if (TSDB_CODE_SUCCESS != code) {
      taosMemoryFree(pLastCol);
      TAOS_CHECK_EXIT(code);
    }

    LRUStatus status = taosLRUCacheInsert(pCache, &idxKey->key, ROCKS_KEY_LEN, pLastCol, charge, tsdbCacheDeleter, NULL,
                                          TAOS_LRU_PRIORITY_LOW, &pTsdb->flushState);
    if (TAOS_LRU_STATUS_OK != status && TAOS_LRU_STATUS_OK_OVERWRITTEN != status) {
      tsdbError("vgId:%d, %s failed at line %d status %d.", TD_VID(pTsdb->pVnode), __func__, __LINE__, status);
      tsdbCacheFreeSLastColItem(pLastCol);
      taosMemoryFree(pLastCol);
      TAOS_CHECK_EXIT(TSDB_CODE_FAILED);
    }

    // store result back to rocks cache
    wb = pTsdb->rCache.rwritebatch;
    char  *value = NULL;
    size_t vlen = 0;
    code = tsdbCacheSerialize(pLastCol, &value, &vlen);
    if (code) {
      tsdbError("tsdb/cache: vgId:%d, serialize failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
    } else {
      SLastKey *key = &idxKey->key;
      size_t    klen = ROCKS_KEY_LEN;
      rocksdb_writebatch_put(wb, (char *)key, klen, value, vlen);
      taosMemoryFree(value);
    }
  }

  if (wb) {
    rocksMayWrite(pTsdb, false, true);
  }

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

static int32_t tsdbCacheLoadFromRocks(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SArray *remainCols,
                                      SArray *ignoreFromRocks, SCacheRowsReader *pr, int8_t ltype) {
  int32_t code = 0, lino = 0;
  int     num_keys = TARRAY_SIZE(remainCols);
  char  **keys_list = taosMemoryMalloc(num_keys * sizeof(char *));
  size_t *keys_list_sizes = taosMemoryMalloc(num_keys * sizeof(size_t));
  char   *key_list = taosMemoryMalloc(num_keys * ROCKS_KEY_LEN);
  if (!keys_list || !keys_list_sizes || !key_list) {
    taosMemoryFree(keys_list);
    taosMemoryFree(keys_list_sizes);
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  char  **values_list = NULL;
  size_t *values_list_sizes = NULL;
  for (int i = 0; i < num_keys; ++i) {
    memcpy(key_list + i * ROCKS_KEY_LEN, &((SIdxKey *)taosArrayGet(remainCols, i))->key, ROCKS_KEY_LEN);
    keys_list[i] = key_list + i * ROCKS_KEY_LEN;
    keys_list_sizes[i] = ROCKS_KEY_LEN;
  }

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

    code = tsdbCacheDeserialize(values_list[i], values_list_sizes[i], &pLastCol);
    SLastCol *pToFree = pLastCol;
    SIdxKey  *idxKey = &((SIdxKey *)TARRAY_DATA(remainCols))[j];
    if (code == TSDB_CODE_SUCCESS && pLastCol && pLastCol->cacheStatus != TSDB_LAST_CACHE_NO_CACHE) {
      SLastCol *pTmpLastCol = taosMemoryCalloc(1, sizeof(SLastCol));
      if (!pTmpLastCol) {
        taosMemoryFreeClear(pToFree);
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }

      size_t charge = 0;
      *pTmpLastCol = *pLastCol;
      pLastCol = pTmpLastCol;
      code = tsdbCacheReallocSLastCol(pLastCol, &charge);
      if (TSDB_CODE_SUCCESS != code) {
        taosMemoryFreeClear(pLastCol);
        taosMemoryFreeClear(pToFree);
        TAOS_CHECK_EXIT(code);
      }

      SLastCol lastCol = *pLastCol;
      code = tsdbCacheReallocSLastCol(&lastCol, NULL);
      if (TSDB_CODE_SUCCESS != code) {
        tsdbCacheFreeSLastColItem(pLastCol);
        taosMemoryFreeClear(pLastCol);
        taosMemoryFreeClear(pToFree);
        TAOS_CHECK_EXIT(code);
      }

      LRUStatus status = taosLRUCacheInsert(pCache, &idxKey->key, ROCKS_KEY_LEN, pLastCol, charge, tsdbCacheDeleter,
                                            NULL, TAOS_LRU_PRIORITY_LOW, &pTsdb->flushState);
      if (TAOS_LRU_STATUS_OK != status && TAOS_LRU_STATUS_OK_OVERWRITTEN != status) {
        tsdbError("vgId:%d, %s failed at line %d status %d.", TD_VID(pTsdb->pVnode), __func__, __LINE__, status);
        tsdbCacheFreeSLastColItem(pLastCol);
        taosMemoryFreeClear(pLastCol);
        taosMemoryFreeClear(pToFree);
        TAOS_CHECK_EXIT(TSDB_CODE_FAILED);
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
    for (int i = 0; i < num_keys; ++i) {
      rocksdb_free(values_list[i]);
    }
    taosMemoryFree(values_list);
  }
  taosMemoryFree(values_list_sizes);

  TAOS_RETURN(code);
}

int32_t tsdbCacheGetBatch(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SCacheRowsReader *pr, int8_t ltype) {
  int32_t    code = 0;
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

    LRUHandle *h = taosLRUCacheLookup(pCache, &key, ROCKS_KEY_LEN);
    SLastCol  *pLastCol = h ? (SLastCol *)taosLRUCacheValue(pCache, h) : NULL;
    if (h && pLastCol->cacheStatus != TSDB_LAST_CACHE_NO_CACHE) {
      SLastCol lastCol = *pLastCol;
      TAOS_CHECK_GOTO(tsdbCacheReallocSLastCol(&lastCol, NULL), NULL, _exit);

      if (taosArrayPush(pLastArray, &lastCol) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
    } else {
      // no cache or cache is invalid
      SLastCol noneCol = {.rowKey.ts = TSKEY_MIN,
                          .colVal = COL_VAL_NONE(cid, pr->pSchema->columns[pr->pSlotIds[i]].type)};

      if (taosArrayPush(pLastArray, &noneCol) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }

      if (!remainCols) {
        if ((remainCols = taosArrayInit(numKeys, sizeof(SIdxKey))) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _exit;
        }
      }
      if (!ignoreFromRocks) {
        if ((ignoreFromRocks = taosArrayInit(numKeys, sizeof(bool))) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _exit;
        }
      }
      if (taosArrayPush(remainCols, &(SIdxKey){i, key}) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
      bool ignoreRocks = pLastCol ? (pLastCol->cacheStatus == TSDB_LAST_CACHE_NO_CACHE) : false;
      if (taosArrayPush(ignoreFromRocks, &ignoreRocks) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
    }

    if (h) {
      TAOS_UNUSED(taosLRUCacheRelease(pCache, h, false));
    }
  }

  if (remainCols && TARRAY_SIZE(remainCols) > 0) {
    (void)taosThreadMutexLock(&pTsdb->lruMutex);
    for (int i = 0; i < TARRAY_SIZE(remainCols);) {
      SIdxKey   *idxKey = &((SIdxKey *)TARRAY_DATA(remainCols))[i];
      LRUHandle *h = taosLRUCacheLookup(pCache, &idxKey->key, ROCKS_KEY_LEN);
      SLastCol  *pLastCol = h ? (SLastCol *)taosLRUCacheValue(pCache, h) : NULL;
      if (h && pLastCol->cacheStatus != TSDB_LAST_CACHE_NO_CACHE) {
        SLastCol lastCol = *pLastCol;
        code = tsdbCacheReallocSLastCol(&lastCol, NULL);
        if (code) {
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
        TAOS_UNUSED(taosLRUCacheRelease(pCache, h, false));
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

int32_t tsdbCacheDel(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey) {
  int32_t code = 0, lino = 0;
  // fetch schema
  STSchema *pTSchema = NULL;
  int       sver = -1;

  TAOS_CHECK_RETURN(metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, sver, &pTSchema));

  // build keys & multi get from rocks
  int     numCols = pTSchema->numOfCols;
  int     numKeys = 0;
  SArray *remainCols = NULL;

  TAOS_UNUSED(tsdbCacheCommit(pTsdb));

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
                              .cacheStatus = TSDB_LAST_CACHE_NO_CACHE};
          code = tsdbCachePutToLRU(pTsdb, &lastKey, &noneCol);
        }
        TAOS_UNUSED(taosLRUCacheRelease(pTsdb->lruCache, h, false));
        TAOS_CHECK_EXIT(code);
      } else {
        if (!remainCols) {
          remainCols = taosArrayInit(numCols * 2, sizeof(SLastKey));
        }
        if (!taosArrayPush(remainCols, &lastKey)) {
          TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
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
    ((SLastKey *)key)[0] = *(SLastKey *)taosArrayGet(remainCols, i);

    keys_list[i] = key;
    keys_list_sizes[i] = klen;
  }

  TAOS_CHECK_GOTO(tsdbCacheGetValuesFromRocks(pTsdb, numKeys, (const char *const *)keys_list, keys_list_sizes,
                                              &values_list, &values_list_sizes),
                  NULL, _exit);

  rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;
  for (int i = 0; i < numKeys; ++i) {
    SLastCol *pLastCol = NULL;
    code = tsdbCacheDeserialize(values_list[i], values_list_sizes[i], &pLastCol);
    SLastKey *pLastKey = (SLastKey *)keys_list[i];
    if (code == TSDB_CODE_SUCCESS && NULL != pLastCol && (pLastCol->rowKey.ts <= eKey && pLastCol->rowKey.ts >= sKey)) {
      SLastCol noCacheCol = {.rowKey.ts = TSKEY_MIN,
                             .colVal = COL_VAL_NONE(pLastKey->cid, pTSchema->columns[i].type),
                             .cacheStatus = TSDB_LAST_CACHE_NO_CACHE};

      if ((code = tsdbCachePutToRocksdb(pTsdb, pLastKey, &noCacheCol)) != TSDB_CODE_SUCCESS) {
        taosMemoryFreeClear(pLastCol);
        tsdbError("tsdb/cache/del: vgId:%d, put to rocks failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
        goto _exit;
      }
      if ((code = tsdbCachePutToLRU(pTsdb, pLastKey, &noCacheCol)) != TSDB_CODE_SUCCESS) {
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

  rocksMayWrite(pTsdb, true, false);

_exit:
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  for (int i = 0; i < numKeys; ++i) {
    taosMemoryFree(keys_list[i]);
  }
  taosMemoryFree(keys_list);
  taosMemoryFree(keys_list_sizes);
  if (values_list) {
    for (int i = 0; i < numKeys; ++i) {
      rocksdb_free(values_list[i]);
    }
    taosMemoryFree(values_list);
  }
  taosMemoryFree(values_list_sizes);
  taosArrayDestroy(remainCols);
  taosMemoryFree(pTSchema);

  TAOS_RETURN(code);
}

int32_t tsdbOpenCache(STsdb *pTsdb) {
  int32_t code = 0, lino = 0;
  size_t  cfgCapacity = pTsdb->pVnode->config.cacheLastSize * 1024 * 1024;

  SLRUCache *pCache = taosLRUCacheInit(cfgCapacity, 0, .5);
  if (pCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err);
  }

  TAOS_CHECK_GOTO(tsdbOpenBCache(pTsdb), &lino, _err);

  TAOS_CHECK_GOTO(tsdbOpenPgCache(pTsdb), &lino, _err);

  TAOS_CHECK_GOTO(tsdbOpenRocksCache(pTsdb), &lino, _err);

  taosLRUCacheSetStrictCapacity(pCache, false);

  (void)taosThreadMutexInit(&pTsdb->lruMutex, NULL);

  pTsdb->flushState.pTsdb = pTsdb;
  pTsdb->flushState.flush_count = 0;

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
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  TAOS_RETURN(code);
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

static int32_t loadTombFromBlk(const TTombBlkArray *pTombBlkArray, SCacheRowsReader *pReader, void *pFileReader,
                               bool isFile) {
  int32_t   code = 0;
  int32_t   numOfTables = pReader->numOfTables;
  int64_t   suid = pReader->info.suid;
  uint64_t *uidList = getUidList(pReader);

  if (!uidList) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
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
            code = TSDB_CODE_OUT_OF_MEMORY;
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
          TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
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

        TAOS_UNUSED(loadDataTomb(state->pr, state->pr->pFileReader));

        TAOS_CHECK_GOTO(tsdbDataFileReadBrinBlk(state->pr->pFileReader, &state->pr->pBlkArray), &lino, _err);
      }

      if (!state->pIndexList) {
        state->pIndexList = taosArrayInit(1, sizeof(SBrinBlk));
        if (!state->pIndexList) {
          TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err);
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
              TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err);
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
        (void)lastIterClose(&state->pLastIter);
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
        (void)lastIterClose(&state->pLastIter);
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
      TAOS_CHECK_GOTO(tsdbRowMergerInit(pMerger, state->pTSchema), &lino, _err);

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
        if (state->pMem->maxKey <= state->lastTs) {
          *ppRow = NULL;
          *pIgnoreEarlierTs = true;

          TAOS_RETURN(code);
        }
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
    TAOS_RETURN(code);
  }

  if (state->pLastIter) {
    (void)lastIterClose(&state->pLastIter);
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
    (void)lastIterClose(&state->pLastIter);
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
        TSDB_CHECK_NULL(pIter->pSkyline, code, lino, _err, TSDB_CODE_OUT_OF_MEMORY);

        uint64_t        uid = pIter->idx.uid;
        STableLoadInfo *pInfo = getTableLoadInfo(pIter->pr, uid);
        TSDB_CHECK_NULL(pInfo, code, lino, _err, TSDB_CODE_OUT_OF_MEMORY);

        if (pInfo->pTombData == NULL) {
          pInfo->pTombData = taosArrayInit(4, sizeof(SDelData));
          TSDB_CHECK_NULL(pInfo->pTombData, code, lino, _err, TSDB_CODE_OUT_OF_MEMORY);
        }

        if (!taosArrayAddAll(pInfo->pTombData, pIter->pMemDelData)) {
          TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _err);
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
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  for (int32_t i = 0; i < nCols; ++i) {
    int16_t  slotId = slotIds[i];
    SLastCol col = {.rowKey.ts = 0,
                    .colVal = COL_VAL_NULL(pTSchema->columns[slotId].colId, pTSchema->columns[slotId].type)};
    if (!taosArrayPush(pColArray, &col)) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
  }
  *ppColArray = pColArray;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cloneTSchema(STSchema *pSrc, STSchema **ppDst) {
  int32_t len = sizeof(STSchema) + sizeof(STColumn) * pSrc->numOfCols;
  *ppDst = taosMemoryMalloc(len);
  if (NULL == *ppDst) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
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

    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  for (int i = 0; i < nCols; ++i) {
    if (!taosArrayPush(aColArray, &aCols[i])) {
      taosArrayDestroy(pColArray);

      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
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
          *pColVal = COL_VAL_VALUE(pTColumn->colId, ((SValue){.type = pTColumn->type, .val = rowKey.key.ts}));

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

    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  for (int i = 0; i < nCols; ++i) {
    if (!taosArrayPush(aColArray, &aCols[i])) {
      taosArrayDestroy(pColArray);

      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
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
        *pColVal = COL_VAL_VALUE(pTColumn->colId, ((SValue){.type = pTColumn->type, .val = rowKey.key.ts}));

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

void tsdbCacheRelease(SLRUCache *pCache, LRUHandle *h) { TAOS_UNUSED(taosLRUCacheRelease(pCache, h, false)); }

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

  int64_t block_offset = (pFD->blkno - 1) * tsS3BlockSize * pFD->szPage;

  TAOS_CHECK_RETURN(s3GetObjectBlock(pFD->objName, block_offset, tsS3BlockSize * pFD->szPage, 0, ppBlock));

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
    (void)taosThreadMutexLock(&pTsdb->bMutex);

    h = taosLRUCacheLookup(pCache, key, keyLen);
    if (!h) {
      uint8_t *pBlock = NULL;
      code = tsdbCacheLoadBlockS3(pFD, &pBlock);
      //  if table's empty or error, return code of -1
      if (code != TSDB_CODE_SUCCESS || pBlock == NULL) {
        (void)taosThreadMutexUnlock(&pTsdb->bMutex);

        *handle = NULL;
        if (code == TSDB_CODE_SUCCESS && !pBlock) {
          code = TSDB_CODE_OUT_OF_MEMORY;
        }

        TAOS_RETURN(code);
      }

      size_t              charge = tsS3BlockSize * pFD->szPage;
      _taos_lru_deleter_t deleter = deleteBCache;
      LRUStatus           status =
          taosLRUCacheInsert(pCache, key, keyLen, pBlock, charge, deleter, &h, TAOS_LRU_PRIORITY_LOW, NULL);
      if (status != TAOS_LRU_STATUS_OK) {
        // code = -1;
      }
    }

    (void)taosThreadMutexUnlock(&pTsdb->bMutex);
  }

  *handle = h;

  TAOS_RETURN(code);
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
  (void)taosThreadMutexLock(&pFD->pTsdb->pgMutex);
  handle = taosLRUCacheLookup(pFD->pTsdb->pgCache, key, keyLen);
  if (!handle) {
    size_t              charge = pFD->szPage;
    _taos_lru_deleter_t deleter = deleteBCache;
    uint8_t            *pPg = taosMemoryMalloc(charge);
    if (!pPg) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    memcpy(pPg, pPage, charge);

    LRUStatus status =
        taosLRUCacheInsert(pCache, key, keyLen, pPg, charge, deleter, &handle, TAOS_LRU_PRIORITY_LOW, NULL);
    if (status != TAOS_LRU_STATUS_OK) {
      // ignore cache updating if not ok
      // code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  (void)taosThreadMutexUnlock(&pFD->pTsdb->pgMutex);

  tsdbCacheRelease(pFD->pTsdb->pgCache, handle);

  TAOS_RETURN(code);
}
