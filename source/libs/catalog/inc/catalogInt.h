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

#ifndef _TD_CATALOG_INT_H_
#define _TD_CATALOG_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "catalog.h"
#include "common.h"
#include "query.h"

#define CTG_DEFAULT_CACHE_CLUSTER_NUMBER 6
#define CTG_DEFAULT_CACHE_VGROUP_NUMBER 100
#define CTG_DEFAULT_CACHE_DB_NUMBER 20
#define CTG_DEFAULT_CACHE_TABLEMETA_NUMBER 100000
#define CTG_DEFAULT_RENT_SECOND 10
#define CTG_DEFAULT_RENT_SLOT_SIZE 10

#define CTG_RENT_SLOT_SECOND 1.5

#define CTG_DEFAULT_INVALID_VERSION (-1)

#define CTG_ERR_CODE_TABLE_NOT_EXIST TSDB_CODE_TDB_INVALID_TABLE_ID

enum {
  CTG_READ = 1,
  CTG_WRITE,
};

enum {
  CTG_RENT_DB = 1,
  CTG_RENT_STABLE,
};

typedef struct SCTGDebug {
  int32_t lockDebug;
} SCTGDebug;


typedef struct SVgroupListCache {
  int32_t vgroupVersion;
  SHashObj *cache;        // key:vgId, value:SVgroupInfo
} SVgroupListCache;

typedef struct SDBVgroupCache {
  SHashObj *cache;      //key:dbname, value:SDBVgroupInfo
} SDBVgroupCache;

typedef struct STableMetaCache {
  SRWLatch  stableLock;
  SHashObj *cache;           //key:fulltablename, value:STableMeta
  SHashObj *stableCache;     //key:suid, value:STableMeta*
} STableMetaCache;

typedef struct SRentSlotInfo {
  SRWLatch lock;
  bool     needSort;
  SArray  *meta;  // element is SDbVgVersion or SSTableMetaVersion
} SRentSlotInfo;

typedef struct SMetaRentMgmt {
  int8_t         type;
  uint16_t       slotNum;
  uint16_t       slotRIdx;
  int64_t        lastReadMsec;
  SRentSlotInfo *slots;
} SMetaRentMgmt;

typedef struct SCatalog {
  uint64_t         clusterId;
  SDBVgroupCache   dbCache;
  STableMetaCache  tableCache;
  SMetaRentMgmt    dbRent;
  SMetaRentMgmt    stableRent;
} SCatalog;

typedef struct SCtgApiStat {

} SCtgApiStat;

typedef struct SCtgResourceStat {

} SCtgResourceStat;

typedef struct SCtgCacheStat {

} SCtgCacheStat;

typedef struct SCatalogStat {
  SCtgApiStat      api;
  SCtgResourceStat resource;
  SCtgCacheStat    cache;
} SCatalogStat;

typedef struct SCatalogMgmt {
  SHashObj             *pCluster;     //key: clusterId, value: SCatalog*
  SCatalogStat          stat;
  SCatalogCfg           cfg;
} SCatalogMgmt;

typedef uint32_t (*tableNameHashFp)(const char *, uint32_t);

#define CTG_IS_META_NULL(type) ((type) == META_TYPE_NULL_TABLE)
#define CTG_IS_META_CTABLE(type) ((type) == META_TYPE_CTABLE)
#define CTG_IS_META_TABLE(type) ((type) == META_TYPE_TABLE)
#define CTG_IS_META_BOTH(type) ((type) == META_TYPE_BOTH_TABLE)

#define CTG_IS_STABLE(isSTable) (1 == (isSTable))
#define CTG_IS_NOT_STABLE(isSTable) (0 == (isSTable))
#define CTG_IS_UNKNOWN_STABLE(isSTable) ((isSTable) < 0)
#define CTG_SET_STABLE(isSTable, tbType) do { (isSTable) = ((tbType) == TSDB_SUPER_TABLE) ? 1 : ((tbType) > TSDB_SUPER_TABLE ? 0 : -1); } while (0)
#define CTG_TBTYPE_MATCH(isSTable, tbType) (CTG_IS_UNKNOWN_STABLE(isSTable) || (CTG_IS_STABLE(isSTable) && (tbType) == TSDB_SUPER_TABLE) || (CTG_IS_NOT_STABLE(isSTable) && (tbType) != TSDB_SUPER_TABLE))

#define CTG_TABLE_NOT_EXIST(code) (code == CTG_ERR_CODE_TABLE_NOT_EXIST) 

#define ctgFatal(param, ...)  qFatal("CTG:%p " param, pCatalog, __VA_ARGS__)
#define ctgError(param, ...)  qError("CTG:%p " param, pCatalog, __VA_ARGS__)
#define ctgWarn(param, ...)   qWarn("CTG:%p " param, pCatalog, __VA_ARGS__)
#define ctgInfo(param, ...)   qInfo("CTG:%p " param, pCatalog, __VA_ARGS__)
#define ctgDebug(param, ...)  qDebug("CTG:%p " param, pCatalog, __VA_ARGS__)
#define ctgTrace(param, ...)  qTrace("CTG:%p " param, pCatalog, __VA_ARGS__)

#define CTG_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define CTG_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define CTG_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)

#define CTG_LOCK_DEBUG(...) do { if (gCTGDebug.lockDebug) { qDebug(__VA_ARGS__); } } while (0)

#define TD_RWLATCH_WRITE_FLAG_COPY 0x40000000

#define CTG_LOCK(type, _lock) do {   \
  if (CTG_READ == (type)) {          \
    assert(atomic_load_32((_lock)) >= 0);  \
    CTG_LOCK_DEBUG("CTG RLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosRLockLatch(_lock);           \
    CTG_LOCK_DEBUG("CTG RLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) > 0);  \
  } else {                                                \
    assert(atomic_load_32((_lock)) >= 0);  \
    CTG_LOCK_DEBUG("CTG WLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);  \
    taosWLockLatch(_lock);                                \
    CTG_LOCK_DEBUG("CTG WLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);  \
    assert(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY);  \
  }                                                       \
} while (0)

#define CTG_UNLOCK(type, _lock) do {                       \
  if (CTG_READ == (type)) {                                \
    assert(atomic_load_32((_lock)) > 0);  \
    CTG_LOCK_DEBUG("CTG RULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosRUnLockLatch(_lock);                              \
    CTG_LOCK_DEBUG("CTG RULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) >= 0);  \
  } else {                                                \
    assert(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY);  \
    CTG_LOCK_DEBUG("CTG WULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosWUnLockLatch(_lock);                              \
    CTG_LOCK_DEBUG("CTG WULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) >= 0);  \
  }                                                       \
} while (0)


#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_INT_H_*/
