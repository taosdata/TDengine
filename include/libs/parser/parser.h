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

#include "querynodes.h"
#include "query.h"

typedef struct SStmtCallback {
  TAOS_STMT* pStmt;
  int32_t (*getTbNameFn)(TAOS_STMT*, char**);
  int32_t (*setBindInfoFn)(TAOS_STMT*, STableMeta*, void*);
  int32_t (*setExecInfoFn)(TAOS_STMT*, SHashObj*, SHashObj*);
  int32_t (*getExecInfoFn)(TAOS_STMT*, SHashObj**, SHashObj**);
} SStmtCallback;

typedef struct SParseContext {
  uint64_t         requestId;
  int32_t          acctId;
  const char      *db;
  bool             topicQuery;
  void            *pTransporter;
  SEpSet           mgmtEpSet;
  const char      *pSql;           // sql string
  size_t           sqlLen;         // length of the sql string
  char            *pMsg;           // extended error message if exists to help identifying the problem in sql statement.
  int32_t          msgLen;         // max length of the msg
  struct SCatalog *pCatalog;
  SStmtCallback   *pStmtCb;
} SParseContext;

typedef struct SCmdMsgInfo {
  int16_t msgType;
  SEpSet epSet;
  void* pMsg;
  int32_t msgLen;
  void* pExtension;  // todo remove it soon
} SCmdMsgInfo;

typedef enum EQueryExecMode {
  QUERY_EXEC_MODE_LOCAL = 1,
  QUERY_EXEC_MODE_RPC,
  QUERY_EXEC_MODE_SCHEDULE,
  QUERY_EXEC_MODE_EMPTY_RESULT
} EQueryExecMode;

typedef struct SQuery {
  EQueryExecMode execMode;
  bool haveResultSet;
  SNode* pRoot;
  int32_t numOfResCols;
  SSchema* pResSchema;
  SCmdMsgInfo* pCmdMsg;
  int32_t msgType;
  SArray* pDbList;
  SArray* pTableList;
  bool showRewrite;
} SQuery;

int32_t qParseQuerySql(SParseContext* pCxt, SQuery** pQuery);

void qDestroyQuery(SQuery* pQueryNode);

int32_t qExtractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema);

int32_t qBuildStmtOutput(SQuery* pQuery, SHashObj* pVgHash, SHashObj* pBlockHash);
void    qResetStmtDataBlock(void* pBlock, bool freeData);
int32_t qCloneStmtDataBlock(void** pDst, void* pSrc);
void    qFreeStmtDataBlock(void* pDataBlock);
int32_t qRebuildStmtDataBlock(void** pDst, void* pSrc);
void    qDestroyStmtDataBlock(void* pBlock);
int32_t qBindStmtColsValue(void *pDataBlock, TAOS_BIND_v2 *bind, char *msgBuf, int32_t msgBufLen);
int32_t qBuildStmtColFields(void *pDataBlock, int32_t *fieldNum, TAOS_FIELD** fields);
int32_t qBuildStmtTagFields(void *pBlock, void *boundTags, int32_t *fieldNum, TAOS_FIELD** fields);
int32_t qBindStmtTagsValue(void *pBlock, void *boundTags, int64_t suid, SName *pName, TAOS_BIND_v2 *bind, char *msgBuf, int32_t msgBufLen);
void destroyBoundColumnInfo(void* pBoundInfo);
int32_t qCreateSName(SName* pName, const char* pTableName, int32_t acctId, char* dbName, char *msgBuf, int32_t msgBufLen);


#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_H_*/
