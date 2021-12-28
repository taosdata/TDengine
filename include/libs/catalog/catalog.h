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
#include "tmsg.h"
#include "query.h"

struct SCatalog;

typedef struct SCatalogReq {
  SArray *pTableName;     // element is SNAME
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
 * Get a cluster's catalog handle for all later operations. 
 * @param clusterId (input, end with \0)
 * @param catalogHandle (output, NO need to free it)
 * @return error code
 */
int32_t catalogGetHandle(const char *clusterId, struct SCatalog** catalogHandle);

int32_t catalogGetDBVgroupVersion(struct SCatalog* pCatalog, const char* dbName, int32_t* version);
int32_t catalogGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, int32_t forceUpdate, SDBVgroupInfo* dbInfo);
int32_t catalogUpdateDBVgroupCache(struct SCatalog* pCatalog, const char* dbName, SDBVgroupInfo* dbInfo);

/**
 * Get a table's meta data. 
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pDBName (input, full db name)
 * @param pTableName (input, table name, NOT including db name)
 * @param pTableMeta(output, table meta data, NEED to free it by calller)
 * @return error code
 */
int32_t catalogGetTableMeta(struct SCatalog* pCatalog, void * pTransporter, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, STableMeta** pTableMeta);

/**
 * Force renew a table's local cached meta data. 
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pRpc (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pDBName (input, full db name)
 * @param pTableName (input, table name, NOT including db name)
 * @return error code
 */
int32_t catalogRenewTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName);

/**
 * Force renew a table's local cached meta data and get the new one. 
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pRpc (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pDBName (input, full db name)
 * @param pTableName (input, table name, NOT including db name)
 * @param pTableMeta(output, table meta data, NEED to free it by calller) 
 * @return error code
 */
int32_t catalogRenewAndGetTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, STableMeta** pTableMeta);


/**
 * Get a table's actual vgroup, for stable it's all possible vgroup list.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pRpc (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pDBName (input, full db name)
 * @param pTableName (input, table name, NOT including db name)
 * @param pVgroupList (output, vgroup info list, element is SVgroupInfo, NEED to simply free the array by caller)
 * @return error code
 */
int32_t catalogGetTableDistVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, SArray** pVgroupList);

/**
 * Get a table's vgroup from its name's hash value.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pDBName (input, full db name)
 * @param pTableName (input, table name, NOT including db name)
 * @param vgInfo (output, vgroup info)
 * @return error code
 */
int32_t catalogGetTableHashVgroup(struct SCatalog* pCatalog, void * pTransporter, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, SVgroupInfo* vgInfo);


/**
 * Get all meta data required in pReq.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pRpc (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pReq (input, reqest info)
 * @param pRsp (output, response data)
 * @return error code 
 */
int32_t catalogGetAllMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const SCatalogReq* pReq, SMetaData* pRsp);


int32_t catalogGetQnodeList(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, SArray* pQnodeList);



/**
 * Destroy catalog and relase all resources
 */
void catalogDestroy(void);

#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_H_*/
