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
#ifndef _TD_VNODE_TSDB_CACHE_INT_H_
#define _TD_VNODE_TSDB_CACHE_INT_H_

#include "functionMgt.h"
#include "tsdb.h"
#include "tsdbDataFileRW.h"
#include "tsdbIter.h"
#include "tsdbReadUtil.h"
#include "tss.h"
#include "vnd.h"

#ifdef __cplusplus
extern "C" {
#endif
#define ROCKS_BATCH_SIZE (4096)

// Cache format constants
#define TSDB_CACHE_FORMAT_COL 0  // Column-based cache (legacy)
#define TSDB_CACHE_FORMAT_ROW 1  // Row-based cache (new)

// Row-based cache control macro
// Set to 1 for row-based cache, 0 for column-based cache
#define TSDB_CACHE_ROW_BASED 1

#define ROCKS_KEY_LEN (sizeof(tb_uid_t) + sizeof(int16_t) + sizeof(int8_t))
// Macro to get the appropriate key length based on cache type

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
  TSKEY            lastTs;  // Timestamp of the last non-null value for this column
} SColCacheStatus;

// Row-based cache value structure with per-column cache status
typedef struct {
  STsdbRowKey      rowKey;     // Row key with timestamp and primary keys
  SRow*            pRow;       // Complete row data
  int8_t           dirty;      // Dirty flag for write-back
  int32_t          numCols;    // Number of columns with cache status
  SColCacheStatus* colStatus;  // Array of column cache statuses
} SLastRow;

typedef struct {
  int      idx;
  SLastKey key;
} SIdxKey;

#define IS_ROW_LAST_ROW_KEY(k) (((k).lflag & LFLAG_LAST) == LFLAG_LAST_ROW)
#define IS_ROW_LAST_KEY(k)     (((k).lflag & LFLAG_LAST) == LFLAG_LAST)

#if TSDB_CACHE_ROW_BASED
#define TSDB_CACHE_KEY_LEN ROCKS_ROW_KEY_LEN
#else
#define TSDB_CACHE_KEY_LEN ROCKS_KEY_LEN
#endif


// tsdb row cache
int32_t tsdbRowCacheCmp(void* state, const char* a, size_t alen, const char* b, size_t blen);
int     tsdbRowCacheFlushDirty(const void* key, size_t klen, void* value, void* ud);
int32_t tsdbRowCacheNewTableColumn(STsdb* pTsdb, int64_t uid, int16_t cid, int8_t col_type, int8_t lflag,
                                   SSchemaWrapper* pOldSchemaWrapper, SSchemaWrapper* pNewSchemaWrapper);
int32_t tsdbRowCacheDropTableColumn(STsdb* pTsdb, int64_t uid, int16_t cid, int8_t lflag,
                                    SSchemaWrapper* pOldSchemaWrapper, SSchemaWrapper* pNewSchemaWrapper,
                                    bool hasPrimaryKey);
int32_t tsdbRowCacheDeleteTable(STsdb* pTsdb, int64_t uid);
int32_t tsdbRowCacheUpdateFromCtxArray(STsdb* pTsdb, tb_uid_t suid, tb_uid_t uid, SArray* updCtxArray);
int32_t tsdbRowCacheColFormatUpdate(STsdb* pTsdb, tb_uid_t suid, tb_uid_t uid, SBlockData* pBlockData);
int32_t tsdbRowCacheCreateTable(STsdb* pTsdb, int64_t uid, STSchema* pTSchema);
int32_t tsdbRowCacheRowFormatUpdate(STsdb* pTsdb, tb_uid_t suid, tb_uid_t uid, int64_t version, int32_t nRow,
                                    SRow** aRow);
int32_t tsdbRowCachePutToLRU(STsdb* pTsdb, SLastRowKey* pLastRowKey, SLastRow* pLastRow, int8_t dirty);
int32_t tsdbCacheGetBatchFromRowLru(STsdb* pTsdb, tb_uid_t uid, SArray* pLastArray, SCacheRowsReader* pr, int8_t ltype);
int32_t tsdbLastRowInvalidateCol(SLastRow* pLastRow, int16_t cid);
int32_t tsdbRowCachePutToRocksdb(STsdb* pTsdb, SLastRowKey* pLastRowKey, SLastRow* pLastRow);
int32_t tsdbRowCacheDel(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey);
SColCacheStatus* tsdbLastRowGetLastColValColStatus(SLastRow* pLastRow, int16_t cid);

// tsdb col cache
int32_t tsdbColCacheCmp(void* state, const char* a, size_t alen, const char* b, size_t blen);
int32_t tsdbColCacheNewTableColumn(STsdb* pTsdb, int64_t uid, int16_t cid, int8_t col_type, int8_t lflag,
                                   SSchemaWrapper* pOldSchemaWrapper, SSchemaWrapper* pNewSchemaWrapper);
int32_t tsdbColCacheDropTableColumn(STsdb* pTsdb, int64_t uid, int16_t cid, int8_t lflag,
                                    SSchemaWrapper* pOldSchemaWrapper, SSchemaWrapper* pNewSchemaWrapper,
                                    bool hasPrimaryKey);
int32_t tsdbColCacheDeleteTable(STsdb* pTsdb, int64_t uid, STSchema* pTSchema);
int32_t tsdbColCacheCreateTable(STsdb* pTsdb, int64_t uid, STSchema* pTSchema);
int32_t tsdbColCacheUpdateFromCtxArray(STsdb* pTsdb, tb_uid_t suid, tb_uid_t uid, SArray* updCtxArray);
int32_t tsdbColCacheRowFormatUpdate(STsdb* pTsdb, tb_uid_t suid, tb_uid_t uid, int64_t version, int32_t nRow,
                                    SRow** aRow);
int32_t tsdbCacheLoadFromRocks(STsdb* pTsdb, tb_uid_t uid, SArray* pLastArray, SArray* remainCols,
                               SArray* ignoreFromRocks, SCacheRowsReader* pr, int8_t ltype);
int32_t tsdbCacheReallocSLastCol(SLastCol* pCol, size_t* pCharge);
int     tsdbCacheFlushDirty(const void* key, size_t klen, void* value, void* ud);
int32_t tsdbColCacheDel(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey);

// tsdb cache util
void tsdbLRUCacheRelease(SLRUCache* cache, LRUHandle* handle, bool eraseIfLastRef);
int32_t tsdbUpdateSkm(STsdb* pTsdb, tb_uid_t suid, tb_uid_t uid, int32_t sver);
int32_t tsdbCacheGetValuesFromRocks(STsdb* pTsdb, size_t numKeys, const char* const* ppKeysList, size_t* pKeysListSizes,
                                    char*** pppValuesList, size_t** ppValuesListSizes);
int32_t tsdbCacheUpdate(STsdb* pTsdb, tb_uid_t suid, tb_uid_t uid, SArray* updCtxArray);
void    rocksMayWrite(STsdb* pTsdb, bool force);
int32_t tsdbCacheUpdateFromIMem(STsdb* pTsdb);
int32_t tsdbCacheLoadFromRaw(STsdb* pTsdb, tb_uid_t uid, SArray* pLastArray, SArray* remainCols, SCacheRowsReader* pr,
                             int8_t ltype);
int32_t tsdbCacheGetBatchFromMem(STsdb* pTsdb, tb_uid_t uid, SArray* pLastArray, SCacheRowsReader* pr,
                                 SArray* keyArray);
int32_t tsdbCachePutToRocksdb(STsdb* pTsdb, SLastKey* pLastKey, SLastCol* pLastCol);
int32_t tsdbCacheDeserialize(char const* value, size_t size, SLastCol** ppLastCol);

// tsdb cache upgrade
bool    tsdbNeedCacheUpgrade(SVnode* pVnode);
int32_t tsdbUpgradeCache(STsdb* pTsdb);
int32_t tsdbSetCacheFormat(SVnode* pVnode, int8_t cacheFormat);

// External functions needed for column cache RocksDB operations
int32_t tsdbOpenColCacheRocksDB(STsdb* pTsdb, rocksdb_t** ppColDB, rocksdb_options_t** ppOptions,
                                rocksdb_readoptions_t** ppReadoptions, rocksdb_comparator_t** ppColCmp);
void    tsdbCloseColCacheRocksDB(rocksdb_t* colDB, rocksdb_options_t* options, rocksdb_readoptions_t* readoptions,
                                 rocksdb_comparator_t* colCmp);
int32_t tsdbDeleteColCacheRocksDB(STsdb* pTsdb);

#ifdef __cplusplus
}
#endif

#endif