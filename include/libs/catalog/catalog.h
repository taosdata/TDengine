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
#include "query.h"
#include "taosdef.h"
#include "tarray.h"
#include "tcommon.h"
#include "thash.h"
#include "tmsg.h"
#include "tname.h"
#include "transport.h"
#include "nodes.h"

typedef struct SCatalog SCatalog;

enum {
  CTG_DBG_DB_NUM = 1,
  CTG_DBG_META_NUM,
  CTG_DBG_STB_NUM,
  CTG_DBG_VIEW_NUM,
  CTG_DBG_DB_RENT_NUM,
  CTG_DBG_STB_RENT_NUM,
  CTG_DBG_VIEW_RENT_NUM,
};

typedef enum {
  AUTH_TYPE_READ = 1,
  AUTH_TYPE_WRITE,
  AUTH_TYPE_ALTER,
  AUTH_TYPE_OTHER,
  AUTH_TYPE_READ_OR_WRITE,
} AUTH_TYPE;

typedef struct SUserAuthInfo {
  char      user[TSDB_USER_LEN];
  SName     tbName;
  bool      isView;
  AUTH_TYPE type;
} SUserAuthInfo;

typedef enum {
  AUTH_RES_BASIC = 0,
  AUTH_RES_VIEW,
  AUTH_RES_MAX_VALUE
} AUTH_RES_TYPE;

typedef struct SUserAuthRes {
  bool   pass[AUTH_RES_MAX_VALUE];
  SNode* pCond[AUTH_RES_MAX_VALUE];
} SUserAuthRes;

typedef struct SDbInfo {
  int32_t vgVer;
  int32_t tbNum;
  int64_t dbId;
  int64_t stateTs;
} SDbInfo;

typedef struct STablesReq {
  char    dbFName[TSDB_DB_FNAME_LEN];
  SArray* pTables;
} STablesReq;

typedef struct SCatalogReq {
  SArray* pDbVgroup;      // element is db full name
  SArray* pDbCfg;         // element is db full name
  SArray* pDbInfo;        // element is db full name
  SArray* pTableMeta;     // element is STablesReq
  SArray* pTableHash;     // element is STablesReq
  SArray* pUdf;           // element is udf name
  SArray* pIndex;         // element is index name
  SArray* pUser;          // element is SUserAuthInfo
  SArray* pTableIndex;    // element is SNAME
  SArray* pTableCfg;      // element is SNAME
  SArray* pTableTag;      // element is SNAME
  SArray* pView;          // element is STablesReq
  bool    qNodeRequired;  // valid qnode
  bool    dNodeRequired;  // valid dnode
  bool    svrVerRequired;
  bool    forceUpdate;
  bool    cloned;
} SCatalogReq;

typedef struct SMetaRes {
  int32_t code;
  void*   pRes;
} SMetaRes;

typedef struct SMetaData {
  bool      ctgFree;      // need to freed by catalog module
  SArray*   pDbVgroup;    // pRes = SArray<SVgroupInfo>*
  SArray*   pDbCfg;       // pRes = SDbCfgInfo*
  SArray*   pDbInfo;      // pRes = SDbInfo*
  SArray*   pTableMeta;   // pRes = STableMeta*
  SArray*   pTableHash;   // pRes = SVgroupInfo*
  SArray*   pTableIndex;  // pRes = SArray<STableIndexInfo>*
  SArray*   pUdfList;     // pRes = SFuncInfo*
  SArray*   pIndex;       // pRes = SIndexInfo*
  SArray*   pUser;        // pRes = SUserAuthRes*
  SArray*   pQnodeList;   // pRes = SArray<SQueryNodeLoad>*
  SArray*   pTableCfg;    // pRes = STableCfg*
  SArray*   pTableTag;    // pRes = SArray<STagVal>*
  SArray*   pDnodeList;   // pRes = SArray<SEpSet>*
  SArray*   pView;        // pRes = SViewMeta*
  SMetaRes* pSvrVer;      // pRes = char*
} SMetaData;

typedef struct SCatalogCfg {
  uint32_t maxTblCacheNum;
  uint32_t maxViewCacheNum;
  uint32_t maxDBCacheNum;
  uint32_t maxUserCacheNum;
  uint32_t dbRentSec;
  uint32_t stbRentSec;
  uint32_t viewRentSec;
} SCatalogCfg;

typedef struct SSTableVersion {
  char     dbFName[TSDB_DB_FNAME_LEN];
  char     stbName[TSDB_TABLE_NAME_LEN];
  int64_t  dbId;
  uint64_t suid;
  int32_t  sversion;
  int32_t  tversion;
  int32_t  smaVer;
} SSTableVersion;

typedef struct SDbCacheInfo {
  char    dbFName[TSDB_DB_FNAME_LEN];
  int64_t dbId;
  int32_t vgVersion;
  int32_t cfgVersion;
  int32_t numOfTable;  // unit is TSDB_TABLE_NUM_UNIT
  int64_t stateTs;
} SDbCacheInfo;

typedef struct SDynViewVersion {
  int64_t  svrBootTs;
  uint64_t dynViewVer;
} SDynViewVersion;

typedef struct SViewVersion {
  char     dbFName[TSDB_DB_FNAME_LEN];
  char     viewName[TSDB_VIEW_NAME_LEN];
  int64_t  dbId;
  uint64_t viewId;
  int32_t  version;
} SViewVersion;


typedef struct STbSVersion {
  char*   tbFName;
  int32_t sver;
  int32_t tver;
} STbSVersion;

typedef struct SUserAuthVersion {
  char    user[TSDB_USER_LEN];
  int32_t version;
} SUserAuthVersion;

typedef SUserIndexRsp SIndexInfo;

typedef void (*catalogCallback)(SMetaData* pResult, void* param, int32_t code);

int32_t catalogInit(SCatalogCfg* cfg);

/**
 * Get a cluster's catalog handle for all later operations.
 * @param clusterId
 * @param catalogHandle (output, NO need to free it)
 * @return error code
 */
int32_t catalogGetHandle(uint64_t clusterId, SCatalog** catalogHandle);

int32_t catalogGetDBVgVersion(SCatalog* pCtg, const char* dbFName, int32_t* version, int64_t* dbId, int32_t* tableNum, int64_t* stateTs);

/**
 * Get a DB's all vgroup info.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pDBName (input, full db name)
 * @param pVgroupList (output, vgroup info list, element is SVgroupInfo, NEED to simply free the array by caller)
 * @return error code
 */
int32_t catalogGetDBVgList(SCatalog* pCatalog, SRequestConnInfo* pConn, const char* pDBName, SArray** pVgroupList);

int32_t catalogGetDBVgInfo(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, TAOS_DB_ROUTE_INFO* pInfo);

int32_t catalogUpdateDBVgInfo(SCatalog* pCatalog, const char* dbName, uint64_t dbId, SDBVgInfo* dbInfo);

int32_t catalogUpdateDbCfg(SCatalog* pCtg, const char* dbFName, uint64_t dbId, SDbCfgInfo* cfgInfo);

int32_t catalogRemoveDB(SCatalog* pCatalog, const char* dbName, uint64_t dbId);

int32_t catalogRemoveTableMeta(SCatalog* pCtg, SName* pTableName);

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
int32_t catalogGetTableMeta(SCatalog* pCatalog, SRequestConnInfo* pConn, const SName* pTableName,
                            STableMeta** pTableMeta);

/**
 * Get a super table's meta data.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pTableName (input, table name)
 * @param pTableMeta(output, table meta data, NEED to free it by calller)
 * @return error code
 */
int32_t catalogGetSTableMeta(SCatalog* pCatalog, SRequestConnInfo* pConn, const SName* pTableName,
                             STableMeta** pTableMeta);

int32_t catalogUpdateTableMeta(SCatalog* pCatalog, STableMetaRsp* rspMsg);

int32_t catalogAsyncUpdateTableMeta(SCatalog* pCtg, STableMetaRsp* pMsg);

int32_t catalogGetCachedTableMeta(SCatalog* pCtg, const SName* pTableName, STableMeta** pTableMeta);

int32_t catalogGetCachedSTableMeta(SCatalog* pCtg, const SName* pTableName, STableMeta** pTableMeta);

int32_t catalogGetTablesHashVgId(SCatalog* pCtg, SRequestConnInfo* pConn, int32_t acctId, const char* pDb, const char* pTableName[],
                                  int32_t tableNum, int32_t *vgId);

int32_t catalogGetCachedTableHashVgroup(SCatalog* pCtg, const SName* pTableName, SVgroupInfo* pVgroup, bool* exists);

int32_t catalogGetCachedTableVgMeta(SCatalog* pCtg, const SName* pTableName,          SVgroupInfo* pVgroup, STableMeta** pTableMeta);

/**
 * Force refresh DB's local cached vgroup info.
 * @param pCtg (input, got with catalogGetHandle)
 * @param pTrans (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param dbFName (input, db full name)
 * @return error code
 */
int32_t catalogRefreshDBVgInfo(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName);

int32_t catalogChkTbMetaVersion(SCatalog* pCtg, SRequestConnInfo* pConn, SArray* pTables);

/**
 * Force refresh a table's local cached meta data.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pTableName (input, table name)
 * @param isSTable (input, is super table or not, 1:supposed to be stable, 0: supposed not to be stable, -1:not sure)
 * @return error code
 */
int32_t catalogRefreshTableMeta(SCatalog* pCatalog, SRequestConnInfo* pConn, const SName* pTableName, int32_t isSTable);

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
int32_t catalogRefreshGetTableMeta(SCatalog* pCatalog, SRequestConnInfo* pConn, const SName* pTableName,
                                   STableMeta** pTableMeta, int32_t isSTable);

/**
 * Get a table's actual vgroup, for stable it's all possible vgroup list.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pTableName (input, table name)
 * @param pVgroupList (output, vgroup info list, element is SVgroupInfo, NEED to simply free the array by caller)
 * @return error code
 */
int32_t catalogGetTableDistVgInfo(SCatalog* pCatalog, SRequestConnInfo* pConn, const SName* pTableName,
                                  SArray** pVgroupList);

/**
 * Get a table's vgroup from its name's hash value.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pTableName (input, table name)
 * @param vgInfo (output, vgroup info)
 * @return error code
 */
int32_t catalogGetTableHashVgroup(SCatalog* pCatalog, SRequestConnInfo* pConn, const SName* pName, SVgroupInfo* vgInfo);

/**
 * Get all meta data required in pReq.
 * @param pCatalog (input, got with catalogGetHandle)
 * @param pTransporter (input, rpc object)
 * @param pMgmtEps (input, mnode EPs)
 * @param pReq (input, reqest info)
 * @param pRsp (output, response data)
 * @return error code
 */
int32_t catalogGetAllMeta(SCatalog* pCatalog, SRequestConnInfo* pConn, const SCatalogReq* pReq, SMetaData* pRsp);

int32_t catalogAsyncGetAllMeta(SCatalog* pCtg, SRequestConnInfo* pConn, const SCatalogReq* pReq, catalogCallback fp,
                               void* param, int64_t* jobId);

int32_t catalogGetQnodeList(SCatalog* pCatalog, SRequestConnInfo* pConn, SArray* pQnodeList);

int32_t catalogGetDnodeList(SCatalog* pCatalog, SRequestConnInfo* pConn, SArray** pDnodeList);

int32_t catalogGetExpiredSTables(SCatalog* pCatalog, SSTableVersion** stables, uint32_t* num);

int32_t catalogGetExpiredViews(SCatalog* pCtg, SViewVersion** views, uint32_t* num, SDynViewVersion** dynViewVersion);

int32_t catalogGetExpiredDBs(SCatalog* pCatalog, SDbCacheInfo** dbs, uint32_t* num);

int32_t catalogGetExpiredUsers(SCatalog* pCtg, SUserAuthVersion** users, uint32_t* num);

int32_t catalogGetDBCfg(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, SDbCfgInfo* pDbCfg);

int32_t catalogGetIndexMeta(SCatalog* pCtg, SRequestConnInfo* pConn, const char* indexName, SIndexInfo* pInfo);

int32_t catalogGetTableIndex(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, SArray** pRes);

int32_t catalogGetTableTag(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, SArray** pRes);

int32_t catalogRefreshGetTableCfg(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, STableCfg** pCfg);

int32_t catalogUpdateTableIndex(SCatalog* pCtg, STableIndexRsp* pRsp);

int32_t catalogGetUdfInfo(SCatalog* pCtg, SRequestConnInfo* pConn, const char* funcName, SFuncInfo* pInfo);

int32_t catalogChkAuth(SCatalog* pCtg, SRequestConnInfo* pConn, SUserAuthInfo *pAuth, SUserAuthRes* pRes);

int32_t catalogChkAuthFromCache(SCatalog* pCtg, SUserAuthInfo *pAuth, SUserAuthRes* pRes, bool* exists);

int32_t catalogUpdateUserAuthInfo(SCatalog* pCtg, SGetUserAuthRsp* pAuth);

int32_t catalogUpdateVgEpSet(SCatalog* pCtg, const char* dbFName, int32_t vgId, SEpSet* epSet);

int32_t catalogGetServerVersion(SCatalog* pCtg, SRequestConnInfo* pConn, char** pVersion);

int32_t ctgdLaunchAsyncCall(SCatalog* pCtg, SRequestConnInfo* pConn, uint64_t reqId, bool forceUpdate);

int32_t catalogClearCache(void);

SMetaData* catalogCloneMetaData(SMetaData* pData);

void catalogFreeMetaData(SMetaData* pData);

int32_t catalogRemoveViewMeta(SCatalog* pCtg, const char* dbFName, uint64_t dbId, const char* viewName, uint64_t viewId);

int32_t catalogUpdateDynViewVer(SCatalog* pCtg, SDynViewVersion* pVer);

int32_t catalogUpdateViewMeta(SCatalog* pCtg, SViewMetaRsp* pMsg);

int32_t catalogAsyncUpdateViewMeta(SCatalog* pCtg, SViewMetaRsp* pMsg);

int32_t catalogGetViewMeta(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pViewName, STableMeta** pTableMeta);

int32_t ctgdEnableDebug(char* option, bool enable);

int32_t ctgdHandleDbgCommand(char* command);

/**
 * Destroy catalog and relase all resources
 */
void catalogDestroy(void);

#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_H_*/
