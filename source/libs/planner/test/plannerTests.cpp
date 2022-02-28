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

#include "os.h"

#include "taos.h"
#include "parser.h"
#include "mockCatalog.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

class PlannerEnv : public testing::Environment {
public:
  virtual void SetUp() {
    initMetaDataEnv();
    generateMetaData();
  }

  virtual void TearDown() {
    destroyMetaDataEnv();
  }

  PlannerEnv() {}
  virtual ~PlannerEnv() {}
};

int main(int argc, char* argv[]) {
	testing::AddGlobalTestEnvironment(new PlannerEnv());
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

TEST(testCase, planner_test) {
  char    msg[128] = {0};
  const char* sql = "select top(a*b / 99, 20) from `t.1abc` interval(10s, 1s)";

  // SQueryStmtInfo* pQueryInfo = nullptr;
//  int32_t code = qParseQuerySql(sql, strlen(sql), &pQueryInfo, 0, msg, sizeof(msg));
//  ASSERT_EQ(code, 0);

//  SSqlNode* pNode = (SSqlNode*)taosArrayGetP(((SArray*)info1.list), 0);
//  int32_t   code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
//  ASSERT_EQ(code, 0);
//
//  SMetaReq req = {0};
//  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, msg, 128);
//  ASSERT_EQ(ret, 0);
//  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);
//
//  SQueryStmtInfo* pQueryInfo = createQueryInfo();
//  setTableMetaInfo(pQueryInfo, &req);
//
//  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.list, 0);
//  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);
//  ASSERT_EQ(ret, 0);
//
//  SArray* pExprList = pQueryInfo->exprList;
//  ASSERT_EQ(taosArrayGetSize(pExprList), 2);
//
//  SExprInfo* p1 = (SExprInfo*)taosArrayGetP(pExprList, 1);
//  ASSERT_EQ(p1->base.uid, 110);
//  ASSERT_EQ(p1->base.numOfParams, 1);
//  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_DOUBLE);
//  ASSERT_STRCASEEQ(p1->base.resSchema.name, "top(a*b / 99, 20)");
//  ASSERT_EQ(p1->base.colInfo.flag, TSDB_COL_NORMAL);
//  ASSERT_STRCASEEQ(p1->base.token, "top(a*b / 99, 20)");
//  ASSERT_EQ(p1->base.interBytes, 16);
//
//  ASSERT_EQ(p1->pExpr->nodeType, TEXPR_UNARYEXPR_NODE);
//  ASSERT_EQ(p1->pExpr->_node.functionId, FUNCTION_TOP);
//  ASSERT_TRUE(p1->pExpr->_node.pRight == NULL);
//
//  tExprNode* pParam = p1->pExpr->_node.pLeft;
//
//  ASSERT_EQ(pParam->nodeType, TEXPR_BINARYEXPR_NODE);
//  ASSERT_EQ(pParam->_node.optr, TSDB_BINARY_OP_DIVIDE);
//  ASSERT_EQ(pParam->_node.pLeft->nodeType, TEXPR_BINARYEXPR_NODE);
//  ASSERT_EQ(pParam->_node.pRight->nodeType, TEXPR_VALUE_NODE);
//
//  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 3);
//  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 2);
//
//  destroyQueryInfo(pQueryInfo);
//  qParserCleanupMetaRequestInfo(&req);
//  destroySqlInfo(&info1);
}

#pragma GCC diagnostic pop