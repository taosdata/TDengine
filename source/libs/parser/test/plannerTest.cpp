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

#include <gtest/gtest.h>
#include <iostream>
#pragma GCC diagnostic ignored "-Wwrite-strings"

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "astGenerator.h"
#include "parserInt.h"
#include "taos.h"
#include "tdef.h"
#include "tvariant.h"
#include "planner.h"

namespace {
void setSchema(SSchema* p, int32_t type, int32_t bytes, const char* name, int32_t colId) {
  p->colId = colId;
  p->bytes = bytes;
  p->type = type;
  strcpy(p->name, name);
}

void setTableMetaInfo(SQueryStmtInfo* pQueryInfo, SMetaReq *req) {
  pQueryInfo->numOfTables = 1;

  pQueryInfo->pTableMetaInfo = (STableMetaInfo**)calloc(1, POINTER_BYTES);
  STableMetaInfo* pTableMetaInfo = (STableMetaInfo*)calloc(1, sizeof(STableMetaInfo));
  pQueryInfo->pTableMetaInfo[0] = pTableMetaInfo;

  SName* name = (SName*)taosArrayGet(req->pTableName, 0);

  memcpy(&pTableMetaInfo->name, taosArrayGet(req->pTableName, 0), sizeof(SName));
  pTableMetaInfo->pTableMeta = (STableMeta*)calloc(1, sizeof(STableMeta) + 4 * sizeof(SSchema));
  strcpy(pTableMetaInfo->aliasName, name->tname);
  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
  pTableMeta->tableType = TSDB_NORMAL_TABLE;
  pTableMeta->tableInfo.numOfColumns = 4;
  pTableMeta->tableInfo.rowSize = 28;
  pTableMeta->uid = 110;

  pTableMetaInfo->tagColList = (SArray*) taosArrayInit(4, POINTER_BYTES);

  SSchema* pSchema = pTableMetaInfo->pTableMeta->schema;
  setSchema(&pSchema[0], TSDB_DATA_TYPE_TIMESTAMP, 8, "ts", 0);
  setSchema(&pSchema[1], TSDB_DATA_TYPE_INT, 4, "a", 1);
  setSchema(&pSchema[2], TSDB_DATA_TYPE_DOUBLE, 8, "b", 2);
  setSchema(&pSchema[3], TSDB_DATA_TYPE_DOUBLE, 8, "col", 3);

}
}

TEST(testCase, planner_test) {
  SSqlInfo info1 = doGenerateAST("select top(a*b / 99, 20) from `t.1abc` interval(10s, 1s)");
  ASSERT_EQ(info1.valid, true);

  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.list), 0);
  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SMetaReq req = {0};
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, msg, 128);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.list, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);
  ASSERT_EQ(ret, 0);

  SArray* pExprList = pQueryInfo->exprList;
  ASSERT_EQ(taosArrayGetSize(pExprList), 2);

  SExprInfo* p1 = (SExprInfo*) taosArrayGetP(pExprList, 1);
  ASSERT_EQ(p1->base.uid, 110);
  ASSERT_EQ(p1->base.numOfParams, 1);
  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_STRCASEEQ(p1->base.resSchema.name, "top(a*b / 99, 20)");
  ASSERT_EQ(p1->base.colInfo.flag, TSDB_COL_NORMAL);
  ASSERT_STRCASEEQ(p1->base.token, "top(a*b / 99, 20)");
  ASSERT_EQ(p1->base.interBytes, 16);

  ASSERT_EQ(p1->pExpr->nodeType, TEXPR_UNARYEXPR_NODE);
  ASSERT_EQ(p1->pExpr->_node.functionId, FUNCTION_TOP);
  ASSERT_TRUE(p1->pExpr->_node.pRight == NULL);

  tExprNode* pParam = p1->pExpr->_node.pLeft;

  ASSERT_EQ(pParam->nodeType, TEXPR_BINARYEXPR_NODE);
  ASSERT_EQ(pParam->_node.optr, TSDB_BINARY_OP_DIVIDE);
  ASSERT_EQ(pParam->_node.pLeft->nodeType, TEXPR_BINARYEXPR_NODE);
  ASSERT_EQ(pParam->_node.pRight->nodeType, TEXPR_VALUE_NODE);

  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 3);
  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 2);

  struct SQueryPlanNode* n = nullptr;
  code = qCreateQueryPlan(pQueryInfo, &n);

  char* str = NULL;
  qQueryPlanToString(n, &str);
  printf("%s\n", str);

  destroyQueryInfo(pQueryInfo);
  qParserClearupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}