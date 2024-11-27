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
#include "parToken.h"
#include "query.h"

#define parserFatal(param, ...) qFatal("PARSER: " param, ##__VA_ARGS__)
#define parserError(param, ...) qError("PARSER: " param, ##__VA_ARGS__)
#define parserWarn(param, ...)  qWarn("PARSER: " param, ##__VA_ARGS__)
#define parserInfo(param, ...)  qInfo("PARSER: " param, ##__VA_ARGS__)
#define parserDebug(param, ...) qDebug("PARSER: " param, ##__VA_ARGS__)
#define parserTrace(param, ...) qTrace("PARSER: " param, ##__VA_ARGS__)

#define ROWTS_PSEUDO_COLUMN_NAME "_rowts"
#define C0_PSEUDO_COLUMN_NAME    "_c0"

#define IS_NOW_STR(s, n)                                                       \
  ((*(s) == 'n' || *(s) == 'N') && (*((s) + 1) == 'o' || *((s) + 1) == 'O') && \
   (*((s) + 2) == 'w' || *((s) + 2) == 'W') && (n == 3 || (n == 5 && *((s) + 3) == '(' && *((s) + 4) == ')')))

#define IS_TODAY_STR(s, n)                                                                 \
  ((*(s) == 't' || *(s) == 'T') && (*((s) + 1) == 'o' || *((s) + 1) == 'O') &&             \
   (*((s) + 2) == 'd' || *((s) + 2) == 'D') && (*((s) + 3) == 'a' || *((s) + 3) == 'A') && \
   (*((s) + 4) == 'y' || *((s) + 4) == 'Y') && (n == 5 || (n == 7 && *((s) + 5) == '(' && *((s) + 6) == ')')))

#define IS_NULL_STR(s, n)                                                                \
  (n == 4 && (*(s) == 'N' || *(s) == 'n') && (*((s) + 1) == 'U' || *((s) + 1) == 'u') && \
   (*((s) + 2) == 'L' || *((s) + 2) == 'l') && (*((s) + 3) == 'L' || *((s) + 3) == 'l'))

#define NEXT_TOKEN_WITH_PREV(pSql, token)           \
  do {                                              \
    int32_t index = 0;                              \
    token = tStrGetToken(pSql, &index, true, NULL); \
    pSql += index;                                  \
  } while (0)

#define NEXT_TOKEN_WITH_PREV_EXT(pSql, token, pIgnoreComma) \
  do {                                                      \
    int32_t index = 0;                                      \
    token = tStrGetToken(pSql, &index, true, pIgnoreComma); \
    pSql += index;                                          \
  } while (0)

#define NEXT_TOKEN_KEEP_SQL(pSql, token, index)      \
  do {                                               \
    token = tStrGetToken(pSql, &index, false, NULL); \
  } while (0)

#define NEXT_VALID_TOKEN(pSql, token)           \
  do {                                          \
    (token).n = tGetToken(pSql, &(token).type); \
    (token).z = (char*)pSql;                    \
    pSql += (token).n;                          \
  } while (TK_NK_SPACE == (token).type)

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
  SHashObj* pTableTSMAs;   // key is tbFName, elements are SArray<STableTSMAInfo*>
  SHashObj* pTSMAs;        // key is tsmaFName, elements are STableTSMAInfo*
  SHashObj* pTableName;    // key is tbFUid, elements is STableMeta*(append with tbName)
  SArray*   pDnodes;       // element is SEpSet
  bool      dnodeRequired;
  bool      forceFetchViewMeta;
} SParseMetaCache;

int32_t generateSyntaxErrMsg(SMsgBuf* pBuf, int32_t errCode, ...);
int32_t generateSyntaxErrMsgExt(SMsgBuf* pBuf, int32_t errCode, const char* pFormat, ...);
int32_t buildInvalidOperationMsg(SMsgBuf* pMsgBuf, const char* msg);
int32_t buildInvalidOperationMsgExt(SMsgBuf* pBuf, const char* pFormat, ...);
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
int32_t checkAndTrimValue(SToken* pToken, char* tmpTokenBuf, SMsgBuf* pMsgBuf, int8_t type);
int32_t parseTagValue(SMsgBuf* pMsgBuf, const char** pSql, uint8_t precision, SSchema* pTagSchema, SToken* pToken,
                      SArray* pTagName, SArray* pTagVals, STag** pTag);
int32_t parseTbnameToken(SMsgBuf* pMsgBuf, char* tname, SToken* pToken, bool* pFoundCtbName);

int32_t buildCatalogReq(const SParseMetaCache* pMetaCache, SCatalogReq* pCatalogReq);
int32_t putMetaDataToCache(const SCatalogReq* pCatalogReq, const SMetaData* pMetaData, SParseMetaCache* pMetaCache);
int32_t reserveTableMetaInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache);
int32_t reserveTableMetaInCacheExt(const SName* pName, SParseMetaCache* pMetaCache);
int32_t reserveTableUidInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache);
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
int32_t reserveTableTSMAInfoInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache);
int32_t reserveTSMAInfoInCache(int32_t acctId, const char* pDb, const char* pTsmaName, SParseMetaCache* pMetaCache);
int32_t getTableMetaFromCache(SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta);
int32_t getTableNameFromCache(SParseMetaCache* pMetaCache, const SName* pName, char* pTbName);
int32_t getViewMetaFromCache(SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta);
int32_t buildTableMetaFromViewMeta(STableMeta** pMeta, SViewMeta* pViewMeta);
int32_t getDbVgInfoFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, SArray** pVgInfo);
int32_t getTableVgroupFromCache(SParseMetaCache* pMetaCache, const SName* pName, SVgroupInfo* pVgroup);
int32_t getDbTableVgroupFromCache(SParseMetaCache* pMetaCache, const SName* pName, SVgroupInfo* pVgroup);
int32_t getDbVgVersionFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, int32_t* pVersion, int64_t* pDbId,
                                int32_t* pTableNum, int64_t* pStateTs);
int32_t getDbCfgFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, SDbCfgInfo* pInfo);
int32_t getUserAuthFromCache(SParseMetaCache* pMetaCache, SUserAuthInfo* pAuthReq, SUserAuthRes* pAuthRes);
int32_t getUdfInfoFromCache(SParseMetaCache* pMetaCache, const char* pFunc, SFuncInfo* pInfo);
int32_t getTableIndexFromCache(SParseMetaCache* pMetaCache, const SName* pName, SArray** pIndexes);
int32_t getTableCfgFromCache(SParseMetaCache* pMetaCache, const SName* pName, STableCfg** pOutput);
int32_t getDnodeListFromCache(SParseMetaCache* pMetaCache, SArray** pDnodes);
void    destoryParseMetaCache(SParseMetaCache* pMetaCache, bool request);
int32_t createSelectStmtImpl(bool isDistinct, SNodeList* pProjectionList, SNode* pTable, SNodeList* pHint, SNode** ppSelect);
int32_t getTableTsmasFromCache(SParseMetaCache* pMetaCache, const SName* pTbName, SArray** pTsmas);
int32_t getTsmaFromCache(SParseMetaCache* pMetaCache, const SName* pTsmaName, STableTSMAInfo** pTsma);

/**
 * @brief return a - b with overflow check
 * @retval val range between [INT64_MIN, INT64_MAX]
 */
int64_t int64SafeSub(int64_t a, int64_t b);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_PARSER_UTIL_H
