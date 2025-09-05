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
#include "functionMgt.h"
#include "tsdb.h"
#include "tsdbDataFileRW.h"
#include "tsdbIter.h"
#include "tsdbReadUtil.h"
#include "tss.h"
#include "vnd.h"

#define ROCKS_BATCH_SIZE (4096)

// Row-based cache control macro
// Set to 1 for row-based cache, 0 for column-based cache
#define TSDB_CACHE_ROW_BASED 1

void tsdbLRUCacheRelease(SLRUCache *cache, LRUHandle *handle, bool eraseIfLastRef) {
  if (!taosLRUCacheRelease(cache, handle, eraseIfLastRef)) {
    tsdbTrace(" release lru cache failed");
  }
}

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

#define ROCKS_KEY_LEN (sizeof(tb_uid_t) + sizeof(int16_t) + sizeof(int8_t))

// Macro to get the appropriate key length based on cache type
#if TSDB_CACHE_ROW_BASED
#define TSDB_CACHE_KEY_LEN ROCKS_ROW_KEY_LEN
#else
#define TSDB_CACHE_KEY_LEN ROCKS_KEY_LEN
#endif

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

#if TSDB_CACHE_ROW_BASED
// Row-based cache key structure
#define ROCKS_ROW_KEY_LEN (sizeof(tb_uid_t) + sizeof(int8_t))

typedef struct {
  tb_uid_t uid;
  int8_t   lflag;  // LFLAG_LAST_ROW or LFLAG_LAST
} SLastRowKey;

// Column cache status structure - tracks cache status for each column
typedef struct {
  int16_t          cid;     // Column ID
  ELastCacheStatus status;  // Cache status for this column
} SColCacheStatus;

// Row-based cache value structure with per-column cache status
typedef struct {
  STsdbRowKey      rowKey;     // Row key with timestamp and primary keys
  SRow            *pRow;       // Complete row data
  int8_t           dirty;      // Dirty flag for write-back
  int32_t          numCols;    // Number of columns with cache status
  SColCacheStatus *colStatus;  // Array of column cache statuses
} SLastRow;

#define IS_ROW_LAST_ROW_KEY(k) (((k).lflag & LFLAG_LAST) == LFLAG_LAST_ROW)
#define IS_ROW_LAST_KEY(k)     (((k).lflag & LFLAG_LAST) == LFLAG_LAST)
#endif

static void tsdbGetRocksPath(STsdb *pTsdb, char *path) {
  SVnode *pVnode = pTsdb->pVnode;
  vnodeGetPrimaryPath(pVnode, false, path, TSDB_FILENAME_LEN);

  int32_t offset = strlen(path);
  snprintf(path + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s%scache.rdb", TD_DIRSEP, pTsdb->name, TD_DIRSEP);
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

#if TSDB_CACHE_ROW_BASED
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
#else
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
#endif
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

static void rocksMayWrite(STsdb *pTsdb, bool force) {
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
  } else if (pLastCol->colVal.value.type == TSDB_DATA_TYPE_DECIMAL) {
    pLastCol->colVal.value.nData = pLastColV0->colVal.value.nData;
    pLastCol->colVal.value.pData = (uint8_t *)(&pLastColV0[1]);
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

#if TSDB_CACHE_ROW_BASED
// Forward declarations for row-based cache functions
static int32_t tsdbRowCacheRowFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, int64_t version, int32_t nRow,
                                           SRow **aRow);
static int32_t tsdbRowCacheColFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SBlockData *pBlockData);
static int32_t tsdbRowCacheUpdateFromCtxArray(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SArray *updCtxArray);
static void    tsdbRowCacheDeleter(const void *key, size_t klen, void *value, void *ud);
static void    tsdbRowCacheOverWriter(const void *key, size_t klen, void *value, void *ud);
int            tsdbRowCacheFlushDirty(const void *key, size_t klen, void *value, void *ud);

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
  }

  return TSDB_CODE_SUCCESS;
}

static ELastCacheStatus tsdbLastRowGetColStatus(SLastRow *pLastRow, int16_t cid) {
  if (!pLastRow || !pLastRow->colStatus) {
    return TSDB_LAST_CACHE_NO_CACHE;
  }

  for (int32_t i = 0; i < pLastRow->numCols; i++) {
    if (pLastRow->colStatus[i].cid == cid) {
      return pLastRow->colStatus[i].status;
    }
  }

  return TSDB_LAST_CACHE_NO_CACHE;
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

static int32_t tsdbLastRowInvalidateCol(SLastRow *pLastRow, int16_t cid) {
  return tsdbLastRowSetColStatus(pLastRow, cid, TSDB_LAST_CACHE_NO_CACHE);
}
#endif

static int32_t tsdbCachePutToRocksdb(STsdb *pTsdb, SLastKey *pLastKey, SLastCol *pLastCol);

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

static int32_t tsdbUpdateSkm(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, int32_t sver) {
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

static int32_t tsdbCacheUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SArray *updCtxArray);

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

static int32_t tsdbCacheUpdateFromIMem(STsdb *pTsdb) {
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
  // rocksdb_writebatch_t *wb = pTsdb->rCache.writebatch;

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

static int32_t reallocVarDataVal(SValue *pValue) {
  if (IS_VAR_DATA_TYPE(pValue->type)) {
    uint8_t *pVal = pValue->pData;
    uint32_t nData = pValue->nData;
    if (nData > 0) {
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

  if (IS_VAR_DATA_TYPE(pLastCol->colVal.value.type) ||
      pLastCol->colVal.value.type == TSDB_DATA_TYPE_DECIMAL /* && pLastCol->colVal.value.nData > 0*/) {
    taosMemoryFree(pLastCol->colVal.value.pData);
  }

  taosMemoryFree(value);
}

static void tsdbCacheOverWriter(const void *key, size_t klen, void *value, void *ud) {
  SLastCol *pLastCol = (SLastCol *)value;
  pLastCol->dirty = 0;
}

static int32_t tsdbCachePutToLRU(STsdb *pTsdb, SLastKey *pLastKey, SLastCol *pLastCol, int8_t dirty);

#if TSDB_CACHE_ROW_BASED
static int32_t tsdbRowCachePutToLRU(STsdb *pTsdb, SLastRowKey *pLastRowKey, SLastRow *pLastRow, int8_t dirty);
static int32_t tsdbRowCachePutToRocksdb(STsdb *pTsdb, SLastRowKey *pLastRowKey, SLastRow *pLastRow);
#endif

static int32_t tsdbCacheNewTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type, int8_t lflag) {
  int32_t code = 0, lino = 0;

#if TSDB_CACHE_ROW_BASED
  // Create empty column similar to line 1242-1244, then encode to row and write to LRU
  STSchema *pTSchema = NULL;
  code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, 0, uid, -1, &pTSchema);
  if (code != 0) {
    TAOS_RETURN(code);
  }

  // Create empty column for the new column
  SRowKey  emptyRowKey = {.ts = TSKEY_MIN, .numOfPKs = 0};
  SLastCol emptyCol = {
      .rowKey = emptyRowKey, .colVal = COL_VAL_NONE(cid, col_type), .dirty = 1, .cacheStatus = TSDB_LAST_CACHE_VALID};

  // Create array with just this one column to build row
  SArray *colVals = taosArrayInit(1, sizeof(SColVal));
  if (!colVals) {
    taosMemoryFree(pTSchema);
    TAOS_RETURN(terrno);
  }

  taosArrayPush(colVals, &emptyCol.colVal);

  // Build SRow from the column values
  SRow             *pRow = NULL;
  SRowBuildScanInfo scanInfo = {0};
  code = tRowBuild(colVals, pTSchema, &pRow, &scanInfo);

  taosArrayDestroy(colVals);

  if (code != 0) {
    taosMemoryFree(pTSchema);
    TAOS_RETURN(code);
  }

  // Create row cache entry with the built row
  STsdbRowKey rowKey = {.key = emptyRowKey};
  SLastRow    newRow = {.rowKey = rowKey, .pRow = pRow, .dirty = 1, .numCols = 0, .colStatus = NULL};

  // Initialize column status for the new row
  code = tsdbLastRowInitColStatus(&newRow, pTSchema);
  if (code != 0) {
    taosMemoryFree(pTSchema);
    if (pRow) taosMemoryFree(pRow);
    TAOS_RETURN(code);
  }

  // Set this specific column as valid
  tsdbLastRowSetColStatus(&newRow, cid, TSDB_LAST_CACHE_VALID);

  // Write to LRU for both LAST_ROW and LAST flags
  SLastRowKey lastRowKey = {.lflag = LFLAG_LAST_ROW, .uid = uid};
  code = tsdbRowCachePutToLRU(pTsdb, &lastRowKey, &newRow, 1);
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
    taosMemoryFree(pTSchema);
    if (pRow) taosMemoryFree(pRow);
    if (newRow.colStatus) taosMemoryFree(newRow.colStatus);
    TAOS_RETURN(code);
  }

  SLastRowKey lastKey = {.lflag = LFLAG_LAST, .uid = uid};
  code = tsdbRowCachePutToLRU(pTsdb, &lastKey, &newRow, 1);
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
  }

  taosMemoryFree(pTSchema);
#else
  // Original column-based cache logic
  SLRUCache *pCache = pTsdb->lruCache;
  SRowKey  emptyRowKey = {.ts = TSKEY_MIN, .numOfPKs = 0};
  SLastCol emptyCol = {
      .rowKey = emptyRowKey, .colVal = COL_VAL_NONE(cid, col_type), .dirty = 1, .cacheStatus = TSDB_LAST_CACHE_VALID};

  SLastKey *pLastKey = &(SLastKey){.lflag = lflag, .uid = uid, .cid = cid};
  code = tsdbCachePutToLRU(pTsdb, pLastKey, &emptyCol, 1);
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
  }
#endif

  TAOS_RETURN(code);
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

static int32_t tsdbCacheGetValuesFromRocks(STsdb *pTsdb, size_t numKeys, const char *const *ppKeysList,
                                           size_t *pKeysListSizes, char ***pppValuesList, size_t **ppValuesListSizes) {
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

static int32_t tsdbCacheDropTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, bool hasPrimaryKey) {
  int32_t code = 0;

#if TSDB_CACHE_ROW_BASED
  // For row-based cache, handle LFLAG_LAST_ROW and LFLAG_LAST row cache entries
  SLastRowKey rowKeys[2] = {{.lflag = LFLAG_LAST_ROW, .uid = uid}, {.lflag = LFLAG_LAST, .uid = uid}};

  for (int i = 0; i < 2; i++) {
    // 1. Handle RocksDB row cache
#ifdef USE_ROCKSDB
    char   *rowKey = (char *)&rowKeys[i];
    size_t  rowKeySize = ROCKS_ROW_KEY_LEN;
    char  **values_list = NULL;
    size_t *values_list_sizes = NULL;

    // Get row from RocksDB
    rocksMayWrite(pTsdb, true);
    code = tsdbCacheGetValuesFromRocks(pTsdb, 1, (const char *const *)&rowKey, &rowKeySize, &values_list,
                                       &values_list_sizes);
    if (code != TSDB_CODE_SUCCESS) {
      continue;  // Skip to next row key
    }

    if (values_list && values_list[0] != NULL) {
      SLastRow *pLastRow = NULL;
      code = tsdbRowCacheDeserialize(values_list[0], values_list_sizes[0], &pLastRow);
      if (code == TSDB_CODE_SUCCESS && pLastRow && pLastRow->pRow) {
        // Check if the row contains the column to be dropped
        STSchema *pTSchema = NULL;
        code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, 0, uid, -1, &pTSchema);
        if (code == TSDB_CODE_SUCCESS && pTSchema) {
          // Check if column exists in the row
          bool columnExists = false;
          for (int j = 0; j < pTSchema->numOfCols; j++) {
            if (pTSchema->columns[j].colId == cid) {
              columnExists = true;
              break;
            }
          }

          if (columnExists) {
            // Build new row without the dropped column
            SArray *colVals = taosArrayInit(pTSchema->numOfCols - 1, sizeof(SColVal));
            if (colVals) {
              // Extract all columns except the one to be dropped
              for (int j = 0; j < pTSchema->numOfCols; j++) {
                if (pTSchema->columns[j].colId != cid) {
                  SColVal colVal = {0};
                  code = tRowGet(pLastRow->pRow, pTSchema, j, &colVal);
                  if (code == TSDB_CODE_SUCCESS) {
                    taosArrayPush(colVals, &colVal);
                  }
                }
              }

              // Build new schema without dropped column
              STSchema *pNewTSchema =
                  taosMemoryCalloc(1, sizeof(STSchema) + (pTSchema->numOfCols - 1) * sizeof(STColumn));
              if (pNewTSchema) {
                pNewTSchema->version = pTSchema->version;
                pNewTSchema->numOfCols = pTSchema->numOfCols - 1;
                pNewTSchema->flen = pTSchema->flen;

                pNewTSchema->tlen = pTSchema->tlen;

                int newColIdx = 0;
                for (int j = 0; j < pTSchema->numOfCols; j++) {
                  if (pTSchema->columns[j].colId != cid) {
                    pNewTSchema->columns[newColIdx] = pTSchema->columns[j];
                    newColIdx++;
                  }
                }

                // Build new row
                SRow             *pNewRow = NULL;
                SRowBuildScanInfo scanInfo = {0};
                code = tRowBuild(colVals, pNewTSchema, &pNewRow, &scanInfo);
                if (code == TSDB_CODE_SUCCESS && pNewRow) {
                  // Update the cached row
                  SLastRow newLastRow = {
                      .rowKey = pLastRow->rowKey, .pRow = pNewRow, .dirty = 1, .numCols = 0, .colStatus = NULL};

                  // Initialize column status for the new row
                  code = tsdbLastRowInitColStatus(&newLastRow, pNewTSchema);
                  if (code == TSDB_CODE_SUCCESS) {
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

                    // Serialize and write back to RocksDB
                    code = tsdbRowCachePutToRocksdb(pTsdb, &rowKeys[i], &newLastRow);
                  }

                  // Clean up new row structure
                  if (newLastRow.colStatus) {
                    taosMemoryFree(newLastRow.colStatus);
                  }
                  if (pNewRow) {
                    taosMemoryFree(pNewRow);
                  }
                }

                taosMemoryFree(pNewTSchema);
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
              code = tsdbRowCachePutToRocksdb(pTsdb, &rowKeys[i], pLastRow);
            }
          }

          taosMemoryFree(pTSchema);
        }
      }

      // Clean up
      if (pLastRow) {
        if (pLastRow->pRow) {
          taosMemoryFree(pLastRow->pRow);
        }
        if (pLastRow->colStatus) {
          taosMemoryFree(pLastRow->colStatus);
        }
        taosMemoryFree(pLastRow);
      }

      rocksdb_free(values_list[0]);
    }

    if (values_list) {
      taosMemoryFree(values_list);
    }
    if (values_list_sizes) {
      taosMemoryFree(values_list_sizes);
    }
#endif

    // 2. Handle LRU row cache
    LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, &rowKeys[i], ROCKS_ROW_KEY_LEN);
    if (h) {
      SLastRow *pLastRow = (SLastRow *)taosLRUCacheValue(pTsdb->lruCache, h);
      if (pLastRow && pLastRow->pRow) {
        // Check if the row contains the column to be dropped
        STSchema *pTSchema = NULL;
        code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, 0, uid, -1, &pTSchema);
        if (code == TSDB_CODE_SUCCESS && pTSchema) {
          // Check if column exists in the row
          bool columnExists = false;
          for (int j = 0; j < pTSchema->numOfCols; j++) {
            if (pTSchema->columns[j].colId == cid) {
              columnExists = true;
              break;
            }
          }

          if (columnExists) {
            // Build new row without the dropped column
            SArray *colVals = taosArrayInit(pTSchema->numOfCols - 1, sizeof(SColVal));
            if (colVals) {
              // Extract all columns except the one to be dropped
              for (int j = 0; j < pTSchema->numOfCols; j++) {
                if (pTSchema->columns[j].colId != cid) {
                  SColVal colVal = {0};
                  code = tRowGet(pLastRow->pRow, pTSchema, j, &colVal);
                  if (code == TSDB_CODE_SUCCESS) {
                    taosArrayPush(colVals, &colVal);
                  }
                }
              }

              // Build new schema without dropped column
              STSchema *pNewTSchema =
                  taosMemoryCalloc(1, sizeof(STSchema) + (pTSchema->numOfCols - 1) * sizeof(STColumn));
              if (pNewTSchema) {
                pNewTSchema->version = pTSchema->version;
                pNewTSchema->numOfCols = pTSchema->numOfCols - 1;
                pNewTSchema->flen = pTSchema->flen;

                pNewTSchema->tlen = pTSchema->tlen;

                int newColIdx = 0;
                for (int j = 0; j < pTSchema->numOfCols; j++) {
                  if (pTSchema->columns[j].colId != cid) {
                    pNewTSchema->columns[newColIdx] = pTSchema->columns[j];
                    newColIdx++;
                  }
                }

                // Build new row
                SRow             *pNewRow = NULL;
                SRowBuildScanInfo scanInfo = {0};
                code = tRowBuild(colVals, pNewTSchema, &pNewRow, &scanInfo);
                if (code == TSDB_CODE_SUCCESS && pNewRow) {
                  // Update the cached row
                  SLastRow newLastRow = {
                      .rowKey = pLastRow->rowKey, .pRow = pNewRow, .dirty = 1, .numCols = 0, .colStatus = NULL};

                  // Initialize column status for the new row
                  code = tsdbLastRowInitColStatus(&newLastRow, pNewTSchema);
                  if (code == TSDB_CODE_SUCCESS) {
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

                    // Update LRU cache
                    code = tsdbRowCachePutToLRU(pTsdb, &rowKeys[i], &newLastRow, 1);
                  }

                  // Clean up new row structure
                  if (newLastRow.colStatus) {
                    taosMemoryFree(newLastRow.colStatus);
                  }
                  // Note: don't free pNewRow here as it's now owned by the cache
                }

                taosMemoryFree(pNewTSchema);
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

          taosMemoryFree(pTSchema);
        }
      }

      tsdbLRUCacheRelease(pTsdb->lruCache, h, false);
    }
  }

#else
  // Original column-based cache implementation
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
#endif

  TAOS_RETURN(code);
}

int32_t tsdbCacheNewTable(STsdb *pTsdb, tb_uid_t uid, tb_uid_t suid, const SSchemaWrapper *pSchemaRow) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  if (suid < 0) {
    for (int i = 0; i < pSchemaRow->nCols; ++i) {
      int16_t cid = pSchemaRow->pSchema[i].colId;
      int8_t  col_type = pSchemaRow->pSchema[i].type;

      code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, LFLAG_LAST_ROW);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
      }
      code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, LFLAG_LAST);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
      }
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

      code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, LFLAG_LAST_ROW);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
      }
      code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, LFLAG_LAST);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
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
    bool hasPrimayKey = false;
    int  nCols = pSchemaRow->nCols;
    if (nCols >= 2) {
      hasPrimayKey = (pSchemaRow->pSchema[1].flags & COL_IS_KEY) ? true : false;
    }
    for (int i = 0; i < nCols; ++i) {
      int16_t cid = pSchemaRow->pSchema[i].colId;
      int8_t  col_type = pSchemaRow->pSchema[i].type;

      code = tsdbCacheDropTableColumn(pTsdb, uid, cid, hasPrimayKey);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbTrace("vgId:%d, %s drop table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
      }
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

      code = tsdbCacheDropTableColumn(pTsdb, uid, cid, hasPrimayKey);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbTrace("vgId:%d, %s drop table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
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

    bool hasPrimayKey = false;
    int  nCols = pTSchema->numOfCols;
    if (nCols >= 2) {
      hasPrimayKey = (pTSchema->columns[1].flags & COL_IS_KEY) ? true : false;
    }

    for (int i = 0; i < nCols; ++i) {
      int16_t cid = pTSchema->columns[i].colId;
      int8_t  col_type = pTSchema->columns[i].type;

      code = tsdbCacheDropTableColumn(pTsdb, uid, cid, hasPrimayKey);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbTrace("vgId:%d, %s drop table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                  tstrerror(code));
      }
    }
  }

  taosMemoryFree(pTSchema);

  rocksMayWrite(pTsdb, false);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheNewNTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, int8_t col_type) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 0);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }
  code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 1);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }
  // rocksMayWrite(pTsdb, true, false, false);
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheDropNTableColumn(STsdb *pTsdb, int64_t uid, int16_t cid, bool hasPrimayKey) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  code = tsdbCacheCommitNoLock(pTsdb);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s commit with no lock failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }

  code = tsdbCacheDropTableColumn(pTsdb, uid, cid, hasPrimayKey);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s drop table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }

  rocksMayWrite(pTsdb, false);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

int32_t tsdbCacheNewSTableColumn(STsdb *pTsdb, SArray *uids, int16_t cid, int8_t col_type) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  for (int i = 0; i < TARRAY_SIZE(uids); ++i) {
    tb_uid_t uid = ((tb_uid_t *)TARRAY_DATA(uids))[i];

    code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 0);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                tstrerror(code));
    }
    code = tsdbCacheNewTableColumn(pTsdb, uid, cid, col_type, 1);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbTrace("vgId:%d, %s new table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                tstrerror(code));
    }
  }

  // rocksMayWrite(pTsdb, true, false, false);
  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
  TAOS_RETURN(code);
}

int32_t tsdbCacheDropSTableColumn(STsdb *pTsdb, SArray *uids, int16_t cid, bool hasPrimayKey) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&pTsdb->lruMutex);

  code = tsdbCacheCommitNoLock(pTsdb);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, %s commit with no lock failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }

  for (int i = 0; i < TARRAY_SIZE(uids); ++i) {
    int64_t uid = ((tb_uid_t *)TARRAY_DATA(uids))[i];

    code = tsdbCacheDropTableColumn(pTsdb, uid, cid, hasPrimayKey);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbTrace("vgId:%d, %s drop table column failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, __LINE__,
                tstrerror(code));
    }
  }

  rocksMayWrite(pTsdb, false);

  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);

  TAOS_RETURN(code);
}

typedef struct {
  int      idx;
  SLastKey key;
} SIdxKey;

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

static int32_t tsdbCachePutToRocksdb(STsdb *pTsdb, SLastKey *pLastKey, SLastCol *pLastCol) {
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

#if TSDB_CACHE_ROW_BASED
// Row-based cache RocksDB operations
static int32_t tsdbRowCachePutToRocksdb(STsdb *pTsdb, SLastRowKey *pLastRowKey, SLastRow *pLastRow) {
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

static int32_t tsdbRowCachePutToLRU(STsdb *pTsdb, SLastRowKey *pLastRowKey, SLastRow *pLastRow, int8_t dirty) {
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
#endif

static int32_t tsdbCacheUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SArray *updCtxArray) {
  if (!updCtxArray || TARRAY_SIZE(updCtxArray) == 0) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

#if TSDB_CACHE_ROW_BASED
  // For row-based cache, we need to handle this differently
  // Group the updates by lflag and process as complete rows
  return tsdbRowCacheUpdateFromCtxArray(pTsdb, suid, uid, updCtxArray);
#else
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
#endif
}

int32_t tsdbCacheRowFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, int64_t version, int32_t nRow,
                                 SRow **aRow) {
#if TSDB_CACHE_ROW_BASED
  return tsdbRowCacheRowFormatUpdate(pTsdb, suid, uid, version, nRow, aRow);
#else
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
  int32_t      code = 0, lino = 0;
  STSDBRowIter iter = {0};
  STSchema    *pTSchema = NULL;
  SArray      *ctxArray = NULL;

  TSDBROW lRow = tsdbRowFromBlockData(pBlockData, pBlockData->nRow - 1);
  int32_t sver = TSDBROW_SVERSION(&lRow);

  TAOS_CHECK_RETURN(metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, suid, uid, sver, &pTSchema));

  ctxArray = taosArrayInit(pBlockData->nColData, sizeof(SLastUpdateCtx));
  if (ctxArray == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  // 1. prepare last
  STsdbRowKey tsdbRowKey = {0};
  tsdbRowGetKey(&lRow, &tsdbRowKey);

  {
    SValue tsVal = {.type = TSDB_DATA_TYPE_TIMESTAMP};
    VALUE_SET_TRIVIAL_DATUM(&tsVal, lRow.pBlockData->aTSKEY[lRow.iRow]);
    SLastUpdateCtx updateCtx = {
        .lflag = LFLAG_LAST, .tsdbRowKey = tsdbRowKey, .colVal = COL_VAL_VALUE(PRIMARYKEY_TIMESTAMP_COL_ID, tsVal)};
    if (!taosArrayPush(ctxArray, &updateCtx)) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
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
        TAOS_CHECK_GOTO(tColDataGetValue(pColData, tRow.iRow, &colVal), &lino, _exit);

        SLastUpdateCtx updateCtx = {.lflag = LFLAG_LAST, .tsdbRowKey = tsdbRowKey, .colVal = colVal};
        if (!taosArrayPush(ctxArray, &updateCtx)) {
          TAOS_CHECK_GOTO(terrno, &lino, _exit);
        }
        break;
      }
    }
  }

  // 2. prepare last row
  TAOS_CHECK_GOTO(tsdbRowIterOpen(&iter, &lRow, pTSchema), &lino, _exit);
  for (SColVal *pColVal = tsdbRowIterNext(&iter); pColVal; pColVal = tsdbRowIterNext(&iter)) {
    SLastUpdateCtx updateCtx = {.lflag = LFLAG_LAST_ROW, .tsdbRowKey = tsdbRowKey, .colVal = *pColVal};
    if (!taosArrayPush(ctxArray, &updateCtx)) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }
  }

  TAOS_CHECK_GOTO(tsdbCacheUpdate(pTsdb, suid, uid, ctxArray), &lino, _exit);

_exit:
  tsdbRowClose(&iter);
  taosMemoryFreeClear(pTSchema);
  taosArrayDestroy(ctxArray);

  TAOS_RETURN(code);
#endif
}

#if TSDB_CACHE_ROW_BASED
// Row-based cache update functions
static int32_t tsdbRowCacheUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, STsdbRowKey *pRowKey, SRow *pRow,
                                  int8_t lflag) {
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

// Row cache update from context array function
static int32_t tsdbRowCacheUpdateFromCtxArray(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SArray *updCtxArray) {
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

  // Process LAST updates
  if (hasLast && pTSchema) {
    lastColVals = taosArrayInit(16, sizeof(SColVal));
    if (!lastColVals) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }

    // Collect all column values with the latest timestamp for LAST
    for (int i = 0; i < num_keys; ++i) {
      SLastUpdateCtx *updCtx = &((SLastUpdateCtx *)TARRAY_DATA(updCtxArray))[i];
      if (updCtx->lflag == LFLAG_LAST && COL_VAL_IS_VALUE(&updCtx->colVal) &&
          tRowKeyCompare(&updCtx->tsdbRowKey.key, &lastKey.key) == 0) {
        if (!taosArrayPush(lastColVals, &updCtx->colVal)) {
          TAOS_CHECK_GOTO(terrno, &lino, _exit);
        }
      }
    }

    // Build SRow and update cache
    if (TARRAY_SIZE(lastColVals) > 0) {
      SRow             *pRow = NULL;
      SRowBuildScanInfo scanInfo = {0};

      TAOS_CHECK_GOTO(tRowBuild(lastColVals, pTSchema, &pRow, &scanInfo), &lino, _exit);
      if (pRow) {
        code = tsdbRowCacheUpdate(pTsdb, suid, uid, &lastKey, pRow, LFLAG_LAST);
        // Free pRow since tsdbRowCacheUpdate makes a copy
        taosMemoryFree(pRow);
        pRow = NULL;
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
    }
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
#endif

static int32_t mergeLastCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                            int nCols, int16_t *slotIds);

static int32_t mergeLastRowCid(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray, SCacheRowsReader *pr, int16_t *aCols,
                               int nCols, int16_t *slotIds);

static int32_t tsdbCacheLoadFromRaw(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SArray *remainCols,
                                    SCacheRowsReader *pr, int8_t ltype) {
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

    // store result back to rocks cache
    code = tsdbCachePutToRocksdb(pTsdb, &idxKey->key, pLastCol);
    if (code) {
      tsdbError("vgId:%d, %s failed at line %d since %s.", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
      TAOS_CHECK_EXIT(code);
    }

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
      tsdbError("vgId:%d, %s failed at line %d since %s.", TD_VID(pTsdb->pVnode), __func__, __LINE__, tstrerror(code));
      TAOS_CHECK_EXIT(code);
    }

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

static int32_t tsdbCacheLoadFromRocks(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SArray *remainCols,
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

static int32_t tsdbCacheGetBatchFromMem(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SCacheRowsReader *pr,
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

#if TSDB_CACHE_ROW_BASED
static int32_t tsdbCacheGetBatchFromRowLru(STsdb *pTsdb, tb_uid_t uid, SArray *pLastArray, SCacheRowsReader *pr,
                                           int8_t ltype) {
  int32_t    code = 0, lino = 0;
  SLRUCache *pCache = pTsdb->lruCache;
  SArray    *pCidList = pr->pCidList;
  int        numKeys = TARRAY_SIZE(pCidList);
  SArray    *remainCols = NULL;  // Track columns that need to be loaded from subsequent layers

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
    STSchema *pTSchema = NULL;
    code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, pr->info.suid, uid, -1, &pTSchema);
    if (code != 0) {
      tsdbLRUCacheRelease(pCache, h, false);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }

    // Create a new array to store columns that are still invalid after LRU check
    SArray *newRemainCols = taosArrayInit(numKeys, sizeof(SIdxKey));
    if (!newRemainCols) {
      taosMemoryFree(pTSchema);
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
      ELastCacheStatus colStatus = tsdbLastRowGetColStatus(pLastRow, cid);
      
      if (colStatus == TSDB_LAST_CACHE_NO_CACHE) {
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
      lastCol.cacheStatus = colStatus;

      if (pCol && colIndex >= 0) {
        // Extract column value from row
        SColVal colVal;
        code = tRowGet(pLastRow->pRow, pTSchema, colIndex, &colVal);
        if (code == 0) {
          lastCol.colVal = colVal;

          // Reallocate if needed for variable length data
          if (IS_VAR_DATA_TYPE(colVal.value.type) && COL_VAL_IS_VALUE(&colVal)) {
            code = tsdbCacheReallocSLastCol(&lastCol, NULL);
            if (code != 0) {
              taosMemoryFree(pTSchema);
              tsdbLRUCacheRelease(pCache, h, false);
              taosArrayDestroy(newRemainCols);
              TAOS_CHECK_GOTO(code, &lino, _exit);
            }
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
        taosMemoryFree(pTSchema);
        tsdbLRUCacheRelease(pCache, h, false);
        taosArrayDestroy(newRemainCols);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
    }

    taosMemoryFree(pTSchema);
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
        goto _exit;
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
      // Found in LRU after double check, re-check remainCols and update them
      STSchema *pTSchema = NULL;
      code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, pr->info.suid, uid, -1, &pTSchema);
      if (code == 0) {
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
            ELastCacheStatus colStatus = tsdbLastRowGetColStatus(pLastRow, cid);
            if (colStatus == TSDB_LAST_CACHE_NO_CACHE) {
              // Column is still invalid, keep it in remainCols
              taosArrayPush(newRemainCols, idxKey);
              continue;
            }

            // Column is now valid, extract from cached row
            SLastCol lastCol;
            lastCol.rowKey = pLastRow->rowKey.key;
            lastCol.cacheStatus = colStatus;

            if (pCol && colIndex >= 0) {
              SColVal colVal;
              code = tRowGet(pLastRow->pRow, pTSchema, colIndex, &colVal);
              if (code == 0) {
                lastCol.colVal = colVal;
                if (IS_VAR_DATA_TYPE(colVal.value.type) && COL_VAL_IS_VALUE(&colVal)) {
                  code = tsdbCacheReallocSLastCol(&lastCol, NULL);
                  if (code != 0) {
                    taosMemoryFree(pTSchema);
                    tsdbLRUCacheRelease(pCache, h, false);
                    taosArrayDestroy(newRemainCols);
                    (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
                    TAOS_CHECK_GOTO(code, &lino, _exit);
                  }
                }
              } else {
                lastCol.colVal = COL_VAL_NONE(cid, pCol->type);
              }
            } else {
              lastCol.colVal = COL_VAL_NONE(cid, TSDB_DATA_TYPE_NULL);
            }

            // Add to result array
            if (taosArrayPush(pLastArray, &lastCol) == NULL) {
              taosMemoryFree(pTSchema);
              tsdbLRUCacheRelease(pCache, h, false);
              taosArrayDestroy(newRemainCols);
              (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
              TAOS_CHECK_GOTO(terrno, &lino, _exit);
            }
          }

          // Update remainCols with only invalid columns
          taosArrayDestroy(remainCols);
          remainCols = newRemainCols;
        }
        taosMemoryFree(pTSchema);
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
            STSchema *pTSchema = NULL;
            code = metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, pr->info.suid, uid, -1, &pTSchema);
            if (code == 0) {
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
                  ELastCacheStatus colStatus = tsdbLastRowGetColStatus(pRocksRow, cid);
                  if (colStatus == TSDB_LAST_CACHE_NO_CACHE) {
                    // Column is still invalid in RocksDB, keep it for TSDB loading
                    taosArrayPush(newRemainCols, idxKey);
                    continue;
                  }

                  // Column is valid in RocksDB, extract it
                  SLastCol lastCol;
                  lastCol.rowKey = pRocksRow->rowKey.key;
                  lastCol.cacheStatus = colStatus;

                  if (pCol && colIndex >= 0) {
                    SColVal colVal;
                    code = tRowGet(pRocksRow->pRow, pTSchema, colIndex, &colVal);
                    if (code == 0) {
                      lastCol.colVal = colVal;
                      if (IS_VAR_DATA_TYPE(colVal.value.type) && COL_VAL_IS_VALUE(&colVal)) {
                        code = tsdbCacheReallocSLastCol(&lastCol, NULL);
                        if (code != 0) {
                          taosMemoryFree(pTSchema);
                          taosArrayDestroy(newRemainCols);
                          (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
                          TAOS_CHECK_GOTO(code, &lino, _exit);
                        }
                      }
                    } else {
                      lastCol.colVal = COL_VAL_NONE(cid, pCol->type);
                    }
                  } else {
                    lastCol.colVal = COL_VAL_NONE(cid, TSDB_DATA_TYPE_NULL);
                  }

                // Add to result array
                if (taosArrayPush(pLastArray, &lastCol) == NULL) {
                  taosMemoryFree(pTSchema);
                  taosArrayDestroy(newRemainCols);
                  (void)taosThreadMutexUnlock(&pTsdb->lruMutex);
                  TAOS_CHECK_GOTO(terrno, &lino, _exit);
                }
                }

                // Update remainCols with only invalid columns
                taosArrayDestroy(remainCols);
                remainCols = newRemainCols;
              }
              taosMemoryFree(pTSchema);
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

  // Step 3: If remainCols still > 0, load from TSDB
  if (remainCols && TARRAY_SIZE(remainCols) > 0) {
    code = tsdbCacheLoadFromRaw(pTsdb, uid, pLastArray, remainCols, pr, ltype);
    if (code) {
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }
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

  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}
#endif

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
      LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, &lastKey, TSDB_CACHE_KEY_LEN);
      if (h) {
        SLastCol *pLastCol = (SLastCol *)taosLRUCacheValue(pTsdb->lruCache, h);
        if (pLastCol->rowKey.ts <= eKey && pLastCol->rowKey.ts >= sKey) {
#if TSDB_CACHE_ROW_BASED
          // For row-based cache, get the entire row and invalidate specific column
          SLastRowKey rowKey = {.uid = uid, .lflag = lflag};
          LRUHandle  *rowHandle = taosLRUCacheLookup(pTsdb->lruCache, &rowKey, ROCKS_ROW_KEY_LEN);
          if (rowHandle) {
            SLastRow *pLastRow = (SLastRow *)taosLRUCacheValue(pTsdb->lruCache, rowHandle);
            if (pLastRow && pLastRow->rowKey.key.ts <= eKey && pLastRow->rowKey.key.ts >= sKey) {
              // Invalidate this specific column in the row
              tsdbLastRowInvalidateCol(pLastRow, cid);
              pLastRow->dirty = 1;  // Mark row as dirty for flush

              tsdbDebug("vgId:%d, invalidated column %d in row cache for uid:%" PRIu64 ", ts:%" PRId64,
                        TD_VID(pTsdb->pVnode), cid, uid, pLastRow->rowKey.key.ts);
            }
            tsdbLRUCacheRelease(pTsdb->lruCache, rowHandle, false);
          }
#else
          // Column-level invalidation: mark this specific column as invalid
          pLastCol->cacheStatus = TSDB_LAST_CACHE_NO_CACHE;
          pLastCol->dirty = 1;  // Mark as dirty for flush
          tsdbDebug("vgId:%d, invalidated column cache for uid:%" PRIu64 ", cid:%d, lflag:%d, ts:%" PRId64,
                    TD_VID(pTsdb->pVnode), uid, cid, lflag, pLastCol->rowKey.ts);
#endif
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
  const size_t klen = TSDB_CACHE_KEY_LEN;

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
                  &lino, _exit);

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
#if TSDB_CACHE_ROW_BASED
      // For row-based cache, invalidate the entire row
      SLastRowKey rowKey = {.uid = uid, .lflag = (pLastKey->lflag == LFLAG_LAST) ? LFLAG_LAST_ROW : LFLAG_LAST_ROW};
      STsdbRowKey tsdbRowKey = {.key = {.ts = TSKEY_MIN, .numOfPKs = 0}};
      SLastRow    noCacheRow = {.rowKey = tsdbRowKey, .pRow = NULL, .dirty = 0, .numCols = 0, .colStatus = NULL};

      if ((code = tsdbRowCachePutToRocksdb(pTsdb, &rowKey, &noCacheRow)) != TSDB_CODE_SUCCESS) {
        taosMemoryFreeClear(pLastCol);
        tsdbError("tsdb/rowcache/del: vgId:%d, put to rocks failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
        goto _exit;
      }
      if ((code = tsdbRowCachePutToLRU(pTsdb, &rowKey, &noCacheRow, 0)) != TSDB_CODE_SUCCESS) {
        taosMemoryFreeClear(pLastCol);
        tsdbError("tsdb/rowcache/del: vgId:%d, put to lru failed since %s.", TD_VID(pTsdb->pVnode), tstrerror(code));
        goto _exit;
      }
#else
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
#endif
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

int32_t tsdbOpenCache(STsdb *pTsdb) {
  int32_t code = 0, lino = 0;
  size_t  cfgCapacity = (size_t)pTsdb->pVnode->config.cacheLastSize * 1024 * 1024;

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
