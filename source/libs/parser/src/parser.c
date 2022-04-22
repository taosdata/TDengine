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

#include "os.h"
#include "parser.h"

#include "parInt.h"
#include "parToken.h"

bool isInsertSql(const char* pStr, size_t length) {
  if (NULL == pStr) {
    return false;
  }
  
  int32_t index = 0;

  do {
    SToken t0 = tStrGetToken((char*) pStr, &index, false);
    if (t0.type != TK_NK_LP) {
      return t0.type == TK_INSERT || t0.type == TK_IMPORT;
    }
  } while (1);
}

static int32_t parseSqlIntoAst(SParseContext* pCxt, SQuery** pQuery) {
  int32_t code = parse(pCxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) {
    code = translate(pCxt, *pQuery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calculateConstant(pCxt, *pQuery);
  }
  return code;
}

int32_t qParseQuerySql(SParseContext* pCxt, SQuery** pQuery) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (isInsertSql(pCxt->pSql, pCxt->sqlLen)) {
    code = parseInsertSql(pCxt, pQuery);
  } else {
    code = parseSqlIntoAst(pCxt, pQuery);
  }
  terrno = code;
  return code;
}

void qDestroyQuery(SQuery* pQueryNode) {
  if (NULL == pQueryNode) {
    return;
  }
  nodesDestroyNode(pQueryNode->pRoot);
  taosMemoryFreeClear(pQueryNode->pResSchema);
  if (NULL != pQueryNode->pCmdMsg) {
    taosMemoryFreeClear(pQueryNode->pCmdMsg->pMsg);
    taosMemoryFreeClear(pQueryNode->pCmdMsg);
  }
  taosArrayDestroy(pQueryNode->pDbList);
  taosArrayDestroy(pQueryNode->pTableList);
  taosMemoryFreeClear(pQueryNode);
}

int32_t qExtractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema) {
  return extractResultSchema(pRoot, numOfCols, pSchema);
}
