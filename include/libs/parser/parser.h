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
} SParseContext;

typedef struct SCmdMsgInfo {
  int16_t msgType;
  SEpSet epSet;
  void* pMsg;
  int32_t msgLen;
  void* pExtension;  // todo remove it soon
} SCmdMsgInfo;

typedef struct SQuery {
  bool directRpc;
  bool haveResultSet;
  SNode* pRoot;
  int32_t numOfResCols;
  SSchema* pResSchema;
  SCmdMsgInfo* pCmdMsg;
  int32_t msgType;
  SArray* pDbList;
  SArray* pTableList;
} SQuery;

int32_t qParseQuerySql(SParseContext* pCxt, SQuery** pQuery);

void qDestroyQuery(SQuery* pQueryNode);

int32_t qExtractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_H_*/
