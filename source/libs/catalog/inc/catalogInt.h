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

typedef struct SVgroupListCache {
  int32_t vgroupVersion;
  SHashObj *cache;        // key:vgId, value:SVgroupInfo
} SVgroupListCache;

typedef struct SDBVgroupCache {
  SHashObj *cache;      //key:dbname, value:SDBVgroupInfo
} SDBVgroupCache;

typedef struct STableMetaCache {
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

#define ctgFatal(...)  do { if (ctgDebugFlag & DEBUG_FATAL) { taosPrintLog("CTG FATAL ", ctgDebugFlag, __VA_ARGS__); }} while(0)
#define ctgError(...)  do { if (ctgDebugFlag & DEBUG_ERROR) { taosPrintLog("CTG ERROR ", ctgDebugFlag, __VA_ARGS__); }} while(0)
#define ctgWarn(...)   do { if (ctgDebugFlag & DEBUG_WARN)  { taosPrintLog("CTG WARN ", ctgDebugFlag, __VA_ARGS__); }}  while(0)
#define ctgInfo(...)   do { if (ctgDebugFlag & DEBUG_INFO)  { taosPrintLog("CTG ", ctgDebugFlag, __VA_ARGS__); }} while(0)
#define ctgDebug(...)  do { if (ctgDebugFlag & DEBUG_DEBUG) { taosPrintLog("CTG ", ctgDebugFlag, __VA_ARGS__); }} while(0)
#define ctgTrace(...)  do { if (ctgDebugFlag & DEBUG_TRACE) { taosPrintLog("CTG ", ctgDebugFlag, __VA_ARGS__); }} while(0)
#define ctgDebugL(...) do { if (ctgDebugFlag & DEBUG_DEBUG) { taosPrintLongString("CTG ", ctgDebugFlag, __VA_ARGS__); }} while(0)

#define CTG_CACHE_ENABLED() (ctgMgmt.cfg.maxDBCacheNum > 0 || ctgMgmt.cfg.maxTblCacheNum > 0)

#define CTG_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define CTG_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define CTG_ERR_LRET(c,...) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { ctgError(__VA_ARGS__); terrno = _code; return _code; } } while (0)
#define CTG_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_INT_H_*/
