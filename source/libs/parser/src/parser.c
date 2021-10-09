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

#include "parserInt.h"
#include "ttoken.h"
#include "astGenerator.h"

bool qIsInsertSql(const char* pStr, size_t length) {
  return false;
}

int32_t qParseQuerySql(const char* pStr, size_t length, struct SQueryStmtInfo** pQueryInfo, int64_t id, char* msg, int32_t msgLen) {
  *pQueryInfo = calloc(1, sizeof(SQueryStmtInfo));
  if (*pQueryInfo == NULL) {
    return -1; // set correct error code.
  }

  SSqlInfo info = doGenerateAST(pStr);
  if (!info.valid) {
    strcpy(msg, info.msg);
    return -1; // set correct error code.
  }

  struct SCatalog* pCatalog = getCatalogHandle(NULL);
  int32_t code = qParserValidateSqlNode(pCatalog, &info, *pQueryInfo, id, msg);
  if (code != 0) {
    return code;
  }

  return 0;
}

int32_t qParseInsertSql(const char* pStr, size_t length, struct SInsertStmtInfo** pInsertInfo, int64_t id, char* msg, int32_t msgLen) {
  return 0;
}

int32_t qParserConvertSql(const char* pStr, size_t length, char** pConvertSql) {
  return 0;
}

int32_t qParserExtractRequestedMetaInfo(const struct SSqlNode* pSqlNode, SMetaReq* pMetaInfo) {
  return 0;
}