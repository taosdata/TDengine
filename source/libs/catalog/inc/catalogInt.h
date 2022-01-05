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
#include "tlog.h"

#define CTG_DEFAULT_CACHE_CLUSTER_NUMBER 6
#define CTG_DEFAULT_CACHE_VGROUP_NUMBER 100
#define CTG_DEFAULT_CACHE_DB_NUMBER 20
#define CTG_DEFAULT_CACHE_TABLEMETA_NUMBER 100000

#define CTG_DEFAULT_INVALID_VERSION (-1)

enum {
  CTG_READ = 1,
  CTG_WRITE,
};

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

typedef struct SCatalog {
  SDBVgroupCache   dbCache;
  STableMetaCache  tableCache;
} SCatalog;

typedef struct SCatalogMgmt {
  void       *pMsgSender;   // used to send messsage to mnode to fetch necessary metadata
  SHashObj   *pCluster;     // items cached for each cluster, the hash key is the cluster-id got from mgmt node
  SCatalogCfg cfg;
} SCatalogMgmt;

typedef uint32_t (*tableNameHashFp)(const char *, uint32_t);

#define CTG_IS_STABLE(isSTable) (1 == (isSTable))
#define CTG_IS_NOT_STABLE(isSTable) (0 == (isSTable))
#define CTG_IS_UNKNOWN_STABLE(isSTable) ((isSTable) < 0)
#define CTG_SET_STABLE(isSTable, tbType) do { (isSTable) = ((tbType) == TSDB_SUPER_TABLE) ? 1 : ((tbType) > TSDB_SUPER_TABLE ? 0 : -1); } while (0)
#define CTG_TBTYPE_MATCH(isSTable, tbType) (CTG_IS_UNKNOWN_STABLE(isSTable) || (CTG_IS_STABLE(isSTable) && (tbType) == TSDB_SUPER_TABLE) || (CTG_IS_NOT_STABLE(isSTable) && (tbType) != TSDB_SUPER_TABLE))

#define CTG_TABLE_NOT_EXIST(code) (code == TSDB_CODE_TDB_INVALID_TABLE_ID) 

#define ctgFatal(...)  do { if (ctgDebugFlag & DEBUG_FATAL) { taosPrintLog("CTG FATAL ", ctgDebugFlag, __VA_ARGS__); }} while(0)
#define ctgError(...)  do { if (ctgDebugFlag & DEBUG_ERROR) { taosPrintLog("CTG ERROR ", ctgDebugFlag, __VA_ARGS__); }} while(0)
#define ctgWarn(...)   do { if (ctgDebugFlag & DEBUG_WARN)  { taosPrintLog("CTG WARN ", ctgDebugFlag, __VA_ARGS__); }}  while(0)
#define ctgInfo(...)   do { if (ctgDebugFlag & DEBUG_INFO)  { taosPrintLog("CTG ", ctgDebugFlag, __VA_ARGS__); }} while(0)
#define ctgDebug(...)  do { if (ctgDebugFlag & DEBUG_DEBUG) { taosPrintLog("CTG ", ctgDebugFlag, __VA_ARGS__); }} while(0)
#define ctgTrace(...)  do { if (ctgDebugFlag & DEBUG_TRACE) { taosPrintLog("CTG ", ctgDebugFlag, __VA_ARGS__); }} while(0)
#define ctgDebugL(...) do { if (ctgDebugFlag & DEBUG_DEBUG) { taosPrintLongString("CTG ", ctgDebugFlag, __VA_ARGS__); }} while(0)

#define CTG_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define CTG_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define CTG_ERR_LRET(c,...) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { ctgError(__VA_ARGS__); terrno = _code; return _code; } } while (0)
#define CTG_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)

#define TD_RWLATCH_WRITE_FLAG_COPY 0x40000000

#define CTG_LOCK(type, _lock) do {   \
  if (CTG_READ == (type)) {          \
    assert(atomic_load_32((_lock)) >= 0);  \
    ctgDebug("CTG RLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosRLockLatch(_lock);           \
    ctgDebug("CTG RLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) > 0);  \
  } else {                                                \
    assert(atomic_load_32((_lock)) >= 0);  \
    ctgDebug("CTG WLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);  \
    taosWLockLatch(_lock);                                \
    ctgDebug("CTG WLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);  \
    assert(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY);  \
  }                                                       \
} while (0)

#define CTG_UNLOCK(type, _lock) do {                       \
  if (CTG_READ == (type)) {                                \
    assert(atomic_load_32((_lock)) > 0);  \
    ctgDebug("CTG RULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosRUnLockLatch(_lock);                              \
    ctgDebug("CTG RULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) >= 0);  \
  } else {                                                \
    assert(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY);  \
    ctgDebug("CTG WULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosWUnLockLatch(_lock);                              \
    ctgDebug("CTG WULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) >= 0);  \
  }                                                       \
} while (0)


#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_INT_H_*/
