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

#ifndef _TD_PARSER_H_
#define _TD_PARSER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "query.h"
#include "querynodes.h"
#include "catalog.h"

typedef struct SStmtCallback {
  TAOS_STMT* pStmt;
  int32_t (*getTbNameFn)(TAOS_STMT*, char**);
  int32_t (*setInfoFn)(TAOS_STMT*, STableMeta*, void*, SName*, bool, SHashObj*, SHashObj*, const char*);
  int32_t (*getExecInfoFn)(TAOS_STMT*, SHashObj**, SHashObj**);
} SStmtCallback;

typedef enum {
  PARSE_SQL_RES_QUERY = 1,
  PARSE_SQL_RES_SCHEMA,
} SParseResType;

typedef struct SParseSchemaRes {
  int8_t        precision;
  int32_t       numOfCols;
  SSchema*      pSchema;
} SParseSchemaRes;

typedef struct SParseQueryRes {
  SNode*              pQuery;
  SCatalogReq*        pCatalogReq;
  SMetaData           meta;
} SParseQueryRes;

typedef struct SParseSqlRes {
  SParseResType resType;
  union {
    SParseSchemaRes schemaRes;
    SParseQueryRes  queryRes;
  };
} SParseSqlRes;

typedef int32_t (*parseSqlFn)(void*, const char*, const char*, bool, const char*, SParseSqlRes*);

typedef struct SParseCsvCxt {
  TdFilePtr   fp;           // last parsed file
  int32_t     tableNo;      // last parsed table
  SName       tableName;    // last parsed table
  const char* pLastSqlPos;  // the location of the last parsed sql
} SParseCsvCxt;

typedef struct SParseContext {
  uint64_t         requestId;
  int64_t          requestRid;
  int32_t          acctId;
  const char*      db;
  bool             topicQuery;
  void*            pTransporter;
  SEpSet           mgmtEpSet;
  const char*      pSql;    // sql string
  size_t           sqlLen;  // length of the sql string
  char*            pMsg;    // extended error message if exists to help identifying the problem in sql statement.
  int32_t          msgLen;  // max length of the msg
  struct SCatalog* pCatalog;
  SStmtCallback*   pStmtCb;
  const char*      pUser;
  const char*      pEffectiveUser;
  bool             parseOnly;
  bool             isSuperUser;
  bool             enableSysInfo;
  bool             async;
  bool             hasInvisibleCol;
  bool             isView;
  bool             isAudit;
  bool             nodeOffline;
  const char*      svrVer;
  SArray*          pTableMetaPos;    // sql table pos => catalog data pos
  SArray*          pTableVgroupPos;  // sql table pos => catalog data pos
  int64_t          allocatorId;
  parseSqlFn       parseSqlFp;
  void*            parseSqlParam;
  int8_t           biMode;
  SArray*          pSubMetaList;
} SParseContext;

int32_t qParseSql(SParseContext* pCxt, SQuery** pQuery);
bool    qIsInsertValuesSql(const char* pStr, size_t length);

// for async mode
int32_t qParseSqlSyntax(SParseContext* pCxt, SQuery** pQuery, struct SCatalogReq* pCatalogReq);
int32_t qAnalyseSqlSemantic(SParseContext* pCxt, const struct SCatalogReq* pCatalogReq,
                            const struct SMetaData* pMetaData, SQuery* pQuery);
int32_t qContinueParseSql(SParseContext* pCxt, struct SCatalogReq* pCatalogReq, const struct SMetaData* pMetaData,
                          SQuery* pQuery);
int32_t qContinueParsePostQuery(SParseContext* pCxt, SQuery* pQuery, SSDataBlock* pBlock);

void qDestroyParseContext(SParseContext* pCxt);

void qDestroyQuery(SQuery* pQueryNode);

int32_t qExtractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema);
int32_t qSetSTableIdForRsma(SNode* pStmt, int64_t uid);
void    qCleanupKeywordsTable();

int32_t     qBuildStmtOutput(SQuery* pQuery, SHashObj* pVgHash, SHashObj* pBlockHash);
int32_t     qResetStmtDataBlock(STableDataCxt* block, bool keepBuf);
int32_t     qCloneStmtDataBlock(STableDataCxt** pDst, STableDataCxt* pSrc, bool reset);
int32_t     qRebuildStmtDataBlock(STableDataCxt** pDst, STableDataCxt* pSrc, uint64_t uid, uint64_t suid, int32_t vgId,
                                  bool rebuildCreateTb);
void        qDestroyStmtDataBlock(STableDataCxt* pBlock);
STableMeta* qGetTableMetaInDataBlock(STableDataCxt* pDataBlock);
int32_t     qCloneCurrentTbData(STableDataCxt* pDataBlock, SSubmitTbData** pData);

int32_t qStmtBindParams(SQuery* pQuery, TAOS_MULTI_BIND* pParams, int32_t colIdx);
int32_t qStmtParseQuerySql(SParseContext* pCxt, SQuery* pQuery);
int32_t qBindStmtColsValue(void* pBlock, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen);
int32_t qBindStmtSingleColValue(void* pBlock, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen, int32_t colIdx,
                                int32_t rowNum);
int32_t qBuildStmtColFields(void* pDataBlock, int32_t* fieldNum, TAOS_FIELD_E** fields);
int32_t qBuildStmtTagFields(void* pBlock, void* boundTags, int32_t* fieldNum, TAOS_FIELD_E** fields);
int32_t qBindStmtTagsValue(void* pBlock, void* boundTags, int64_t suid, const char* sTableName, char* tName,
                           TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen);
void    destroyBoundColumnInfo(void* pBoundInfo);
int32_t qCreateSName(SName* pName, const char* pTableName, int32_t acctId, char* dbName, char* msgBuf,
                     int32_t msgBufLen);

void qDestroyBoundColInfo(void* pInfo);

SQuery*        smlInitHandle();
int32_t        smlBuildRow(STableDataCxt* pTableCxt);
int32_t        smlBuildCol(STableDataCxt* pTableCxt, SSchema* schema, void* kv, int32_t index);
STableDataCxt* smlInitTableDataCtx(SQuery* query, STableMeta* pTableMeta);

void    clearColValArraySml(SArray* pCols);
int32_t smlBindData(SQuery* handle, bool dataFormat, SArray* tags, SArray* colsSchema, SArray* cols,
                    STableMeta* pTableMeta, char* tableName, const char* sTableName, int32_t sTableNameLen, int32_t ttl,
                    char* msgBuf, int32_t msgBufLen);
int32_t smlBuildOutput(SQuery* handle, SHashObj* pVgHash);
int     rawBlockBindData(SQuery *query, STableMeta* pTableMeta, void* data, SVCreateTbReq** pCreateTb, TAOS_FIELD *fields, int numFields, bool needChangeLength);

int32_t rewriteToVnodeModifyOpStmt(SQuery* pQuery, SArray* pBufArray);
SArray* serializeVgroupsCreateTableBatch(SHashObj* pVgroupHashmap);
SArray* serializeVgroupsDropTableBatch(SHashObj* pVgroupHashmap);
void    destoryCatalogReq(SCatalogReq *pCatalogReq);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_H_*/
