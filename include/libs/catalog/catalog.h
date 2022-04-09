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
#include "taosdef.h"
#include "query.h"
#include "tname.h"
#include "tcommon.h"
#include "tarray.h"
#include "thash.h"
#include "tmsg.h"
#include "transport.h"

typedef struct SCatalog SCatalog;

enum {
  CTG_DBG_DB_NUM = 1,
  CTG_DBG_META_NUM,
  CTG_DBG_STB_NUM,
  CTG_DBG_DB_RENT_NUM,
  CTG_DBG_STB_RENT_NUM,
};


typedef struct SCatalogReq {
  SArray *pTableName;     // element is SNAME
  SArray *pUdf;           // udf name
  bool    qNodeRequired;  // valid qnode
} SCatalogReq;

typedef struct SMetaData {
  SArray    *pTableMeta;  // STableMeta array
  SArray    *pVgroupInfo; // SVgroupInfo list
  SArray    *pUdfList;    // udf info list
  SArray    *pEpSetList;  // qnode epset list, SArray<SEpSet>
} SMetaData;

typedef struct SCatalogCfg {
  uint32_t maxTblCacheNum;
  uint32_t maxDBCacheNum;
  uint32_t dbRentSec;
  uint32_t stbRentSec;
} SCatalogCfg;

typedef struct SSTableMetaVersion {
  char     dbFName[TSDB_DB_FNAME_LEN];
  char     stbName[TSDB_TABLE_NAME_LEN];
  uint64_t dbId;
  uint64_t suid;
  int16_t  sversion;
  int16_t  tversion;  
} SSTableMetaVersion;

typedef struct SDbVgVersion {
  char    dbFName[TSDB_DB_FNAME_LEN];
  int64_t dbId;
  int32_t vgVersion;
  int32_t numOfTable; // unit is TSDB_TABLE_NUM_UNIT
} SDbVgVersion;

typedef SDbCfgRsp SDbCfgInfo;
typedef SUserIndexRsp SIndexInfo;

int32_t catalogInit(SCatalogCfg *cfg);

/**
 * Get a cluster's catalog handle for all later operations. 
 * @param clusterId
 * @param catalogHandle (output, NO need to free it)
 * @return error code
 */
int32_t catalogGetHandle(uint64_t clusterId, SCatalog** catalogHandle);

/**
 * Free a cluster's all catalog info, usually it's not necessary, until the application is closing. 
 * no current or future usage should be guaranteed by application
 * @param pCatalog (input, NO more usage)
 * @return error code
 */
void catalogFreeHandle(SCatalog* pCatalog);

int32_t catalogGetDBVgVersion(SCatalog* pCtg, const char* dbFName, int32_t* version, int64_t* dbId, int32_t *tableNum);

/**
 * Get a DB's all vgroup info.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pDBName (input, full db name)
 * @param pVgroupList (output, vgroup info list, element is SVgroupInfo, NEED to simply free the array by caller)
 * @return error code
 */
int32_t catalogGetDBVgInfo(SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const char* pDBName, SArray** pVgroupList);

int32_t catalogUpdateDBVgInfo(SCatalog* pCatalog, const char* dbName, uint64_t dbId, SDBVgInfo* dbInfo);

int32_t catalogRemoveDB(SCatalog* pCatalog, const char* dbName, uint64_t dbId);

int32_t catalogRemoveTableMeta(SCatalog* pCtg, const SName* pTableName);

int32_t catalogRemoveStbMeta(SCatalog* pCtg, const char* dbFName, uint64_t dbId, const char* stbName, uint64_t suid);

/**
 * Get a table's meta data. 
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pTableName (input, table name)
 * @param pTableMeta(output, table meta data, NEED to free it by calller)
 * @return error code
 */
int32_t catalogGetTableMeta(SCatalog* pCatalog, void * pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta);

/**
 * Get a super table's meta data. 
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pTableName (input, table name)
 * @param pTableMeta(output, table meta data, NEED to free it by calller)
 * @return error code
 */
int32_t catalogGetSTableMeta(SCatalog* pCatalog, void * pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta);

int32_t catalogUpdateSTableMeta(SCatalog* pCatalog, STableMetaRsp *rspMsg);


/**
 * Force refresh DB's local cached vgroup info. 
 * @param pCtg (input, got with catalogGetHandle)
 * @param pTrans (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param dbFName (input, db full name)
 * @return error code
 */
int32_t catalogRefreshDBVgInfo(SCatalog* pCtg, void *pTrans, const SEpSet* pMgmtEps, const char* dbFName);

/**
 * Force refresh a table's local cached meta data. 
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pTableName (input, table name)
 * @param isSTable (input, is super table or not, 1:supposed to be stable, 0: supposed not to be stable, -1:not sure) 
 * @return error code
 */
int32_t catalogRefreshTableMeta(SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, int32_t isSTable);

/**
 * Force refresh a table's local cached meta data and get the new one. 
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pTableName (input, table name)
 * @param pTableMeta(output, table meta data, NEED to free it by calller) 
 * @param isSTable (input, is super table or not, 1:supposed to be stable, 0: supposed not to be stable, -1:not sure) 
 * @return error code
 */
int32_t catalogRefreshGetTableMeta(SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta, int32_t isSTable);



/**
 * Get a table's actual vgroup, for stable it's all possible vgroup list.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pTableName (input, table name)
 * @param pVgroupList (output, vgroup info list, element is SVgroupInfo, NEED to simply free the array by caller)
 * @return error code
 */
int32_t catalogGetTableDistVgInfo(SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, SArray** pVgroupList);

/**
 * Get a table's vgroup from its name's hash value.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pTableName (input, table name)
 * @param vgInfo (output, vgroup info)
 * @return error code
 */
int32_t catalogGetTableHashVgroup(SCatalog* pCatalog, void * pTransporter, const SEpSet* pMgmtEps, const SName* pName, SVgroupInfo* vgInfo);


/**
 * Get all meta data required in pReq.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pReq (input, reqest info)
 * @param pRsp (output, response data)
 * @return error code 
 */
int32_t catalogGetAllMeta(SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SCatalogReq* pReq, SMetaData* pRsp);


int32_t catalogGetQnodeList(SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, SArray* pQnodeList);

int32_t catalogGetExpiredSTables(SCatalog* pCatalog, SSTableMetaVersion **stables, uint32_t *num);

int32_t catalogGetExpiredDBs(SCatalog* pCatalog, SDbVgVersion **dbs, uint32_t *num);

int32_t catalogGetDBCfg(SCatalog* pCtg, void *pRpc, const SEpSet* pMgmtEps, const char* dbFName, SDbCfgInfo* pDbCfg);

int32_t catalogGetIndexInfo(SCatalog* pCtg, void *pRpc, const SEpSet* pMgmtEps, const char* indexName, SIndexInfo* pInfo);


/**
 * Destroy catalog and relase all resources
 */
void catalogDestroy(void);

#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_H_*/
