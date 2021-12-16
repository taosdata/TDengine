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

#ifndef _TD_CATALOG_H_
#define _TD_CATALOG_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "thash.h"
#include "tarray.h"
#include "taosdef.h"
#include "transport.h"
#include "common.h"
#include "taosmsg.h"
#include "query.h"

struct SCatalog;

typedef struct SCatalogReq {
  char    dbName[TSDB_DB_NAME_LEN];
  SArray *pTableName;     // table full name
  SArray *pUdf;           // udf name
  bool    qNodeRequired;  // valid qnode
} SCatalogReq;

typedef struct SMetaData {
  SArray    *pTableMeta;  // STableMeta array
  SArray    *pVgroupInfo; // SVgroupInfo list
  SArray    *pUdfList;    // udf info list
  SEpSet    *pEpSet;      // qnode epset list
} SMetaData;

typedef struct SCatalogCfg {
  bool     enableVgroupCache;
  uint32_t maxTblCacheNum;
  uint32_t maxDBCacheNum;
} SCatalogCfg;

int32_t catalogInit(SCatalogCfg *cfg);

/**
 * Catalog service object, which is utilized to hold tableMeta (meta/vgroupInfo/udfInfo) at the client-side.
 * There is ONLY one SCatalog object for one process space, and this function returns a singleton.
 * @param clusterId
 * @return
 */
int32_t catalogGetHandle(const char *clusterId, struct SCatalog** catalogHandle);

int32_t catalogGetDBVgroupVersion(struct SCatalog* pCatalog, const char* dbName, int32_t* version);
int32_t catalogGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, int32_t forceUpdate, SDBVgroupInfo* dbInfo);
int32_t catalogUpdateDBVgroupCache(struct SCatalog* pCatalog, const char* dbName, SDBVgroupInfo* dbInfo);


int32_t catalogGetTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, STableMeta** pTableMeta);
int32_t catalogRenewTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName);
int32_t catalogRenewAndGetTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, STableMeta** pTableMeta);


/**
 * get table's vgroup list.
 * @param clusterId
 * @pVgroupList  - array of SVgroupInfo
 * @return
 */
int32_t catalogGetTableVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, SArray* pVgroupList);


/**
 * Get the required meta data from mnode.
 * Note that this is a synchronized API and is also thread-safety.
 * @param pCatalog
 * @param pMgmtEps
 * @param pMetaReq
 * @param pMetaData
 * @return
 */
int32_t catalogGetAllMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const SCatalogReq* pReq, SMetaData* pRsp);


int32_t catalogGetQnodeList(struct SCatalog* pCatalog, const SEpSet* pMgmtEps, SEpSet* pQnodeEpSet);



/**
 * Destroy catalog and relase all resources
 * @param pCatalog
 */
void catalogDestroy(void);

#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_H_*/
