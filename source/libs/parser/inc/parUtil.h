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

#ifndef TDENGINE_PARSER_UTIL_H
#define TDENGINE_PARSER_UTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "catalog.h"
#include "os.h"
#include "parser.h"
#include "query.h"

#define parserFatal(param, ...) qFatal("PARSER: " param, ##__VA_ARGS__)
#define parserError(param, ...) qError("PARSER: " param, ##__VA_ARGS__)
#define parserWarn(param, ...)  qWarn("PARSER: " param, ##__VA_ARGS__)
#define parserInfo(param, ...)  qInfo("PARSER: " param, ##__VA_ARGS__)
#define parserDebug(param, ...) qDebug("PARSER: " param, ##__VA_ARGS__)
#define parserTrace(param, ...) qTrace("PARSER: " param, ##__VA_ARGS__)

#define ROWTS_PSEUDO_COLUMN_NAME "_rowts"
#define C0_PSEUDO_COLUMN_NAME    "_c0"

typedef struct SMsgBuf {
  int32_t len;
  char*   buf;
} SMsgBuf;

typedef struct SParseTablesMetaReq {
  char      dbFName[TSDB_DB_FNAME_LEN];
  SHashObj* pTables;
} SParseTablesMetaReq;

typedef enum ECatalogReqType {
  CATALOG_REQ_TYPE_META = 1,
  CATALOG_REQ_TYPE_VGROUP,
  CATALOG_REQ_TYPE_BOTH
} ECatalogReqType;

typedef struct SInsertTablesMetaReq {
  char    dbFName[TSDB_DB_FNAME_LEN];
  SArray* pTableMetaPos;
  SArray* pTableMetaReq;  // element is SName
  SArray* pTableVgroupPos;
  SArray* pTableVgroupReq;  // element is SName
} SInsertTablesMetaReq;

typedef struct SParseMetaCache {
  SHashObj* pTableMeta;    // key is tbFName, element is STableMeta*
  SHashObj* pDbVgroup;     // key is dbFName, element is SArray<SVgroupInfo>*
  SHashObj* pTableVgroup;  // key is tbFName, element is SVgroupInfo*
  SHashObj* pDbCfg;        // key is tbFName, element is SDbCfgInfo*
  SHashObj* pDbInfo;       // key is tbFName, element is SDbInfo*
  SHashObj* pUserAuth;     // key is SUserAuthInfo serialized string, element is bool indicating whether or not to pass
  SHashObj* pUdf;          // key is funcName, element is SFuncInfo*
  SHashObj* pTableIndex;   // key is tbFName, element is SArray<STableIndexInfo>*
  SHashObj* pTableCfg;     // key is tbFName, element is STableCfg*
  SHashObj* pViews;        // key is viewFName, element is SViewMeta*
  SArray*   pDnodes;       // element is SEpSet
  bool      dnodeRequired;
} SParseMetaCache;

int32_t generateSyntaxErrMsg(SMsgBuf* pBuf, int32_t errCode, ...);
int32_t generateSyntaxErrMsgExt(SMsgBuf* pBuf, int32_t errCode, const char* pFormat, ...);
int32_t buildInvalidOperationMsg(SMsgBuf* pMsgBuf, const char* msg);
int32_t buildSyntaxErrMsg(SMsgBuf* pBuf, const char* additionalInfo, const char* sourceStr);

SSchema*      getTableColumnSchema(const STableMeta* pTableMeta);
SSchema*      getTableTagSchema(const STableMeta* pTableMeta);
int32_t       getNumOfColumns(const STableMeta* pTableMeta);
int32_t       getNumOfTags(const STableMeta* pTableMeta);
STableComInfo getTableInfo(const STableMeta* pTableMeta);
STableMeta*   tableMetaDup(const STableMeta* pTableMeta);
int32_t getTableTypeFromTableNode(SNode *pTable);

int32_t trimString(const char* src, int32_t len, char* dst, int32_t dlen);
int32_t getVnodeSysTableTargetName(int32_t acctId, SNode* pWhere, SName* pName);

int32_t buildCatalogReq(const SParseMetaCache* pMetaCache, SCatalogReq* pCatalogReq);
int32_t putMetaDataToCache(const SCatalogReq* pCatalogReq, const SMetaData* pMetaData, SParseMetaCache* pMetaCache);
int32_t reserveTableMetaInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache);
int32_t reserveTableMetaInCacheExt(const SName* pName, SParseMetaCache* pMetaCache);
int32_t reserveViewMetaInCache(int32_t acctId, const char* pDb, const char* pView, SParseMetaCache* pMetaCache);
int32_t reserveViewMetaInCacheExt(const SName* pName, SParseMetaCache* pMetaCache);
int32_t reserveDbVgInfoInCache(int32_t acctId, const char* pDb, SParseMetaCache* pMetaCache);
int32_t reserveTableVgroupInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache);
int32_t reserveTableVgroupInCacheExt(const SName* pName, SParseMetaCache* pMetaCache);
int32_t reserveDbVgVersionInCache(int32_t acctId, const char* pDb, SParseMetaCache* pMetaCache);
int32_t reserveDbCfgInCache(int32_t acctId, const char* pDb, SParseMetaCache* pMetaCache);
int32_t reserveUserAuthInCache(int32_t acctId, const char* pUser, const char* pDb, const char* pTable, AUTH_TYPE type,
                               SParseMetaCache* pMetaCache);
int32_t reserveViewUserAuthInCache(int32_t acctId, const char* pUser, const char* pDb, const char* pTable, AUTH_TYPE type,
                              SParseMetaCache* pMetaCache);                               
int32_t reserveUdfInCache(const char* pFunc, SParseMetaCache* pMetaCache);
int32_t reserveTableIndexInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache);
int32_t reserveTableCfgInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache);
int32_t reserveDnodeRequiredInCache(SParseMetaCache* pMetaCache);
int32_t getTableMetaFromCache(SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta);
int32_t getViewMetaFromCache(SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta);
int32_t buildTableMetaFromViewMeta(STableMeta** pMeta, SViewMeta* pViewMeta);
int32_t getDbVgInfoFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, SArray** pVgInfo);
int32_t getTableVgroupFromCache(SParseMetaCache* pMetaCache, const SName* pName, SVgroupInfo* pVgroup);
int32_t getDbVgVersionFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, int32_t* pVersion, int64_t* pDbId,
                                int32_t* pTableNum, int64_t* pStateTs);
int32_t getDbCfgFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, SDbCfgInfo* pInfo);
int32_t getUserAuthFromCache(SParseMetaCache* pMetaCache, SUserAuthInfo* pAuthReq, SUserAuthRes* pAuthRes);
int32_t getUdfInfoFromCache(SParseMetaCache* pMetaCache, const char* pFunc, SFuncInfo* pInfo);
int32_t getTableIndexFromCache(SParseMetaCache* pMetaCache, const SName* pName, SArray** pIndexes);
int32_t getTableCfgFromCache(SParseMetaCache* pMetaCache, const SName* pName, STableCfg** pOutput);
int32_t getDnodeListFromCache(SParseMetaCache* pMetaCache, SArray** pDnodes);
void    destoryParseMetaCache(SParseMetaCache* pMetaCache, bool request);
SNode*  createSelectStmtImpl(bool isDistinct, SNodeList* pProjectionList, SNode* pTable, SNodeList* pHint);

/**
 * @brief return a - b with overflow check
 * @retval val range between [INT64_MIN, INT64_MAX]
 */
int64_t int64SafeSub(int64_t a, int64_t b);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_PARSER_UTIL_H
