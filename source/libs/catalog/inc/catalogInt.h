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

#define CTG_DEFAULT_CLUSTER_NUMBER 3

typedef struct SVgroupListCache {
  int32_t vgroupNum;
  int32_t vgroupVersion;
  SHashObj *cache;      //key:vgId, value:SVgroupInfo
} SVgroupListCache;

typedef struct SDBVgroupCache {
  SHashObj *cache;      //key:dbname, value:SDBVgroupInfo
} SDBVgroupCache;

typedef struct STableMetaCache {
  SHashObj *cache;     //key:fulltablename, value:STableMeta
} STableMetaCache;

typedef struct SCatalog {
  SVgroupListCache vgroupCache;
  SDBVgroupCache dbCache;
  STableMetaCache tableCache;
} SCatalog;

typedef struct SCatalogMgmt {
  void       *pMsgSender;   // used to send messsage to mnode to fetch necessary metadata
  SHashObj   *pCluster;     // items cached for each cluster, the hash key is the cluster-id got from mgmt node
} SCatalogMgmt;



#define ctgFatal(...) tscFatal(__VA_ARGS__)
#define ctgError(...) tscError(__VA_ARGS__)
#define ctgWarn(...) tscWarn(__VA_ARGS__)
#define ctgInfo(...) tscInfo(__VA_ARGS__)
#define ctgDebug(...) tscDebug(__VA_ARGS__)
#define ctgTrace(...) tscTrace(__VA_ARGS__)

#define CTG_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { return _code; } } while (0)
#define CTG_ERR_LRET(c,...) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { ctgError(__VA_ARGS__); return _code; } } while (0)
#define CTG_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { goto _return; } } while (0)


#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_INT_H_*/
