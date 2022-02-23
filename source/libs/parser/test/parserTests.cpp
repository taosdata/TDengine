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

#include <function.h>
#include <gtest/gtest.h>
#include <iostream>
#include "tglobal.h"

#pragma GCC diagnostic push
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
#include "parserUtil.h"

namespace {
void setSchema(SSchema* p, int32_t type, int32_t bytes, const char* name, int32_t colId) {
  p->colId = colId;
  p->bytes = bytes;
  p->type = type;
  strcpy(p->name, name);
}

void setTableMetaInfo(SQueryStmtInfo* pQueryInfo, SCatalogReq* req) {
  pQueryInfo->numOfTables = 1;

  pQueryInfo->pTableMetaInfo = (STableMetaInfo**)calloc(1, POINTER_BYTES);
  STableMetaInfo* pTableMetaInfo = (STableMetaInfo*)calloc(1, sizeof(STableMetaInfo));
  pQueryInfo->pTableMetaInfo[0] = pTableMetaInfo;

  SName* name = (SName*)taosArrayGet(req->pTableName, 0);

  memcpy(&pTableMetaInfo->name, taosArrayGet(req->pTableName, 0), sizeof(SName));
  pTableMetaInfo->pTableMeta = (STableMeta*)calloc(1, sizeof(STableMeta) + 6 * sizeof(SSchema));
  strcpy(pTableMetaInfo->aliasName, name->tname);
  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
  pTableMeta->tableType = TSDB_NORMAL_TABLE;
  pTableMeta->tableInfo.numOfColumns = 6;
  pTableMeta->tableInfo.rowSize = 28;
  pTableMeta->uid = 110;

  pTableMetaInfo->tagColList = (SArray*)taosArrayInit(4, POINTER_BYTES);

  SSchema* pSchema = pTableMetaInfo->pTableMeta->schema;
  setSchema(&pSchema[0], TSDB_DATA_TYPE_TIMESTAMP, 8, "ts", 0);
  setSchema(&pSchema[1], TSDB_DATA_TYPE_INT, 4, "a", 1);
  setSchema(&pSchema[2], TSDB_DATA_TYPE_DOUBLE, 8, "b", 2);
  setSchema(&pSchema[3], TSDB_DATA_TYPE_DOUBLE, 8, "col", 3);
  setSchema(&pSchema[4], TSDB_DATA_TYPE_BINARY, 12, "c", 4);
  setSchema(&pSchema[5], TSDB_DATA_TYPE_BINARY, 44, "d", 5);
}

void sqlCheck(const char* sql, bool valid) {
  SSqlInfo info1 = doGenerateAST(sql);
  ASSERT_EQ(info1.valid, true);

  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SParseContext ctx = {0};
  ctx.db = "db1";
  ctx.acctId = 1;
  SSqlNode* pNode = (SSqlNode*)taosArrayGetP(((SArray*)info1.sub.node), 0);
  int32_t   code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SCatalogReq req = {0};
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);

  if (valid) {
    ASSERT_EQ(ret, 0);
  } else {
    ASSERT_NE(ret, 0);
  }

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

}  // namespace

TEST(testCase, validateAST_test) {
  SSqlInfo info1 = doGenerateAST("select a a1111, a+b + 22, tbname from `t.1abc` where ts<now+2h and `col` < 20 + 99");
  ASSERT_EQ(info1.valid, true);

  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SSqlNode* pNode = (SSqlNode*)taosArrayGetP(((SArray*)info1.sub.node), 0);
  int32_t   code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SCatalogReq req = {0};
   SParseContext ctx = {0};
  ctx.db = "db1";
  ctx.acctId = 1;
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);

  SArray* pExprList = pQueryInfo->exprList[0];
  ASSERT_EQ(taosArrayGetSize(pExprList), 3);

  SExprInfo* p1 = (SExprInfo*)taosArrayGetP(pExprList, 0);
  ASSERT_EQ(p1->base.pColumns->uid, 110);
  ASSERT_EQ(p1->base.numOfParams, 0);
  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_INT);
  ASSERT_STRCASEEQ(p1->base.resSchema.name, "a1111");
  ASSERT_STRCASEEQ(p1->base.pColumns->name, "t.1abc.a");
  ASSERT_EQ(p1->base.pColumns->info.colId, 1);
  ASSERT_EQ(p1->base.pColumns->flag, TSDB_COL_NORMAL);
  ASSERT_STRCASEEQ(p1->base.token, "a1111");

  ASSERT_EQ(taosArrayGetSize(pExprList), 3);

  SExprInfo* p2 = (SExprInfo*)taosArrayGetP(pExprList, 1);
  ASSERT_EQ(p2->base.pColumns->uid, 110);
  ASSERT_EQ(p2->base.numOfParams, 1);  // it is the serialized binary string of expression.
  ASSERT_EQ(p2->base.resSchema.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_STRCASEEQ(p2->base.resSchema.name, "a+b + 22");

  //  ASSERT_STRCASEEQ(p2->base.colInfo.name, "t.1abc.a");
  //  ASSERT_EQ(p1->base.colInfo.colId, 1);
  //  ASSERT_EQ(p1->base.colInfo.flag, TSDB_COL_NORMAL);
  ASSERT_STRCASEEQ(p2->base.token, "a+b + 22");

  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 3);
  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 3);

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

TEST(testCase, function_Test) {
  SSqlInfo info1 = doGenerateAST("select count(a) from `t.1abc`");
  ASSERT_EQ(info1.valid, true);

  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SSqlNode* pNode = (SSqlNode*)taosArrayGetP(((SArray*)info1.sub.node), 0);
  int32_t   code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SCatalogReq req = {0};
   SParseContext ctx = {0};
  ctx.db = "db1";
  ctx.acctId = 1;
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);

  SArray* pExprList = pQueryInfo->exprList[0];
  ASSERT_EQ(taosArrayGetSize(pExprList), 1);

  SExprInfo* p1 = (SExprInfo*)taosArrayGetP(pExprList, 0);
  ASSERT_EQ(p1->base.pColumns->uid, 110);
  ASSERT_EQ(p1->base.numOfParams, 0);
  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_STRCASEEQ(p1->base.resSchema.name, "count(a)");
  ASSERT_STRCASEEQ(p1->base.pColumns->name, "t.1abc.a");
  ASSERT_EQ(p1->base.pColumns->info.colId, 1);
  ASSERT_EQ(p1->base.pColumns->flag, TSDB_COL_NORMAL);
  ASSERT_STRCASEEQ(p1->base.token, "count(a)");
  ASSERT_EQ(p1->base.interBytes, 8);

  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 2);
  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 1);

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

TEST(testCase, function_Test2) {
  SSqlInfo info1 = doGenerateAST("select count(a) abc from `t.1abc`");
  ASSERT_EQ(info1.valid, true);

  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SSqlNode* pNode = (SSqlNode*)taosArrayGetP(((SArray*)info1.sub.node), 0);
  int32_t   code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SCatalogReq req = {0};
   SParseContext ctx = {0};
  ctx.db = "db1";
  ctx.acctId = 1;
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);

  SArray* pExprList = pQueryInfo->exprList[0];
  ASSERT_EQ(taosArrayGetSize(pExprList), 1);

  SExprInfo* p1 = (SExprInfo*)taosArrayGetP(pExprList, 0);
  ASSERT_EQ(p1->base.pColumns->uid, 110);
  ASSERT_EQ(p1->base.numOfParams, 0);
  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_STRCASEEQ(p1->base.resSchema.name, "abc");
  ASSERT_STRCASEEQ(p1->base.pColumns->name, "t.1abc.a");
  ASSERT_EQ(p1->base.pColumns->info.colId, 1);
  ASSERT_EQ(p1->base.pColumns->flag, TSDB_COL_NORMAL);
  ASSERT_STRCASEEQ(p1->base.token, "count(a)");
  ASSERT_EQ(p1->base.interBytes, 8);

  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 2);
  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 1);

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

TEST(testCase, function_Test3) {
  SSqlInfo info1 = doGenerateAST("select first(*) from `t.1abc`");
  ASSERT_EQ(info1.valid, true);

  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SSqlNode* pNode = (SSqlNode*)taosArrayGetP(((SArray*)info1.sub.node), 0);
  int32_t   code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SCatalogReq req = {0};
   SParseContext ctx = {0};
  ctx.db = "db1";
  ctx.acctId = 1;
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);

  SArray* pExprList = pQueryInfo->exprList[0];
  ASSERT_EQ(taosArrayGetSize(pExprList), 6);

  SExprInfo* p1 = (SExprInfo*)taosArrayGetP(pExprList, 0);
  ASSERT_EQ(p1->base.pColumns->uid, 110);
  ASSERT_EQ(p1->base.numOfParams, 0);
  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT_STRCASEEQ(p1->base.resSchema.name, "first(ts)");
  ASSERT_STRCASEEQ(p1->base.pColumns->name, "t.1abc.ts");
  ASSERT_EQ(p1->base.pColumns->info.colId, 0);
  ASSERT_EQ(p1->base.pColumns->flag, TSDB_COL_NORMAL);
  ASSERT_STRCASEEQ(p1->base.token, "first(ts)");
  ASSERT_EQ(p1->base.interBytes, 24);

  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 6);

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

TEST(testCase, function_Test4) {
  SSqlInfo info1 = doGenerateAST("select block_dist() as a1 from `t.1abc`");
  ASSERT_EQ(info1.valid, true);

  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SSqlNode* pNode = (SSqlNode*)taosArrayGetP(((SArray*)info1.sub.node), 0);
  int32_t   code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SCatalogReq req = {0};
   SParseContext ctx = {0};
  ctx.db = "db1";
  ctx.acctId = 1;
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);

  SArray* pExprList = pQueryInfo->exprList[0];
  ASSERT_EQ(taosArrayGetSize(pExprList), 1);

  SExprInfo* p1 = (SExprInfo*)taosArrayGetP(pExprList, 0);
  ASSERT_EQ(p1->base.pColumns->uid, 110);
  ASSERT_EQ(p1->base.numOfParams, 1);
  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_BINARY);
  ASSERT_STRCASEEQ(p1->base.resSchema.name, "a1");
  //  ASSERT_STRCASEEQ(p1->base.colInfo.name, "t.1abc.ts");
  //  ASSERT_EQ(p1->base.colInfo.colId, 0);
  ASSERT_EQ(p1->base.pColumns->flag, TSDB_COL_UDC);
  ASSERT_STRCASEEQ(p1->base.token, "block_dist()");
  ASSERT_EQ(p1->base.interBytes, 0);

  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 1);
  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 1);

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

TEST(testCase, function_Test5) {
  // todo  select concat(concat(a, b), concat(b, a)) from `t.1abc`;

  SSqlInfo info1 = doGenerateAST("select sum(a) + avg(b)  as a1 from `t.1abc`");
  ASSERT_EQ(info1.valid, true);

  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SSqlNode* pNode = (SSqlNode*)taosArrayGetP(((SArray*)info1.sub.node), 0);
  int32_t   code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SCatalogReq req = {0};
   SParseContext ctx = {0};
  ctx.db = "db1";
  ctx.acctId = 1;
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);
  ASSERT_EQ(ret, 0);

  SArray* pExprList = pQueryInfo->exprList[0];
  ASSERT_EQ(taosArrayGetSize(pExprList), 1);

  SExprInfo* p1 = (SExprInfo*)taosArrayGetP(pExprList, 0);
  ASSERT_EQ(p1->base.numOfCols, 2);
  ASSERT_EQ(p1->base.pColumns->uid, 110);

  ASSERT_EQ(p1->base.numOfParams, 1);
  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_STRCASEEQ(p1->base.resSchema.name, "a1");

  ASSERT_EQ(p1->base.pColumns->flag, TSDB_COL_TMP);
  ASSERT_STREQ(p1->base.pColumns->name, "sum(a)");
  ASSERT_STRCASEEQ(p1->base.token, "sum(a) + avg(b)");
  ASSERT_EQ(p1->base.interBytes, 0);

  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 3);
  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 1);

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

TEST(testCase, function_Test10) {
  sqlCheck("select c from `t.1abc`", true);
  sqlCheck("select length(c) from `t.1abc`", true);
  sqlCheck("select length(sum(col)) from `t.1abc`", true);
  sqlCheck("select sum(length(a+b)) from `t.1abc`", true);
  sqlCheck("select sum(sum(a+b)) from `t.1abc`", false);
  sqlCheck("select sum(length(a) + length(b)) from `t.1abc`", true);
  sqlCheck("select length(sum(a) + sum(b)) + length(sum(a) + sum(b)) from `t.1abc`", true);
  sqlCheck("select sum(length(sum(a))) from `t.1abc`", true);
  sqlCheck("select cov(a, b) from `t.1abc`", true);
  sqlCheck("select sum(length(a) + count(b)) from `t.1abc`", false);

  sqlCheck("select concat(sum(a), count(b)) from `t.1abc`", true);

  sqlCheck("select concat(concat(a,b), concat(a,b)) from `t.1abc`", true);
  sqlCheck("select length(length(length(a))) from `t.1abc`", true);
  sqlCheck("select count() from `t.1abc`", false);
  sqlCheck("select block_dist() from `t.1abc`", true);
  sqlCheck("select block_dist(a) from `t.1abc`", false);
  sqlCheck("select count(*) from `t.1abc` interval(1s) group by a", false);

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  sqlCheck("select length119(a,b) from `t.1abc`", false);
  sqlCheck("select length(a, b) from `t.1abc`", false);
  sqlCheck("select block_dist() + 20 from `t.1abc`", true);
  sqlCheck("select count(b), c from `t.1abc`", false);
  sqlCheck("select top(a, 20), count(b) from `t.1abc`", false);
  sqlCheck("select top(a, 20), b from `t.1abc`", false);
  sqlCheck("select top(a, 20), a+20 from `t.1abc`", true);
//  sqlCheck("select top(a, 20), bottom(a, 10) from `t.1abc`", false);
//  sqlCheck("select last_row(*), count(b) from `t.1abc`", false);
//  sqlCheck("select last_row(a, b) + 20 from `t.1abc`", false);
//  sqlCheck("select last_row(count(*)) from `t.1abc`", false);
}

TEST(testCase, function_Test6) {
  SSqlInfo info1 = doGenerateAST(
      "select sum(a+b) as a1, first(b*a), count(b+b), count(1), count(42.1) from `t.1abc` interval(10s, 1s)");
  ASSERT_EQ(info1.valid, true);

  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SSqlNode* pNode = (SSqlNode*)taosArrayGetP(((SArray*)info1.sub.node), 0);
  int32_t   code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SCatalogReq req = {0};
  SParseContext ctx = {0};
  ctx.db = "db1";
  ctx.acctId = 1;
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);
  ASSERT_EQ(ret, 0);

  SArray* pExprList = pQueryInfo->exprList[0];
  if (tsCompatibleModel) {
      ASSERT_EQ(taosArrayGetSize(pExprList), 6);
  } else {
      ASSERT_EQ(taosArrayGetSize(pExprList), 5);
  }

  int32_t index = tsCompatibleModel? 1:0;
  SExprInfo* p1 = (SExprInfo*)taosArrayGetP(pExprList, index);
  ASSERT_EQ(p1->base.pColumns->uid, 110);
  ASSERT_EQ(p1->base.numOfParams, 0);
  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_STRCASEEQ(p1->base.resSchema.name, "a1");
  ASSERT_EQ(p1->base.pColumns->flag, TSDB_COL_TMP);
  ASSERT_STRCASEEQ(p1->base.token, "sum(a+b)");
  ASSERT_EQ(p1->base.interBytes, 16);
  ASSERT_EQ(p1->pExpr->nodeType, TEXPR_FUNCTION_NODE);
  ASSERT_STRCASEEQ(p1->pExpr->_function.functionName, "sum");
  ASSERT_EQ(p1->pExpr->_function.num, 1);

  tExprNode* pParam = p1->pExpr->_function.pChild[0];

  ASSERT_EQ(pParam->nodeType, TEXPR_COL_NODE);
  ASSERT_STREQ(pParam->pSchema->name, "t.1abc.a+b");

  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 3);

  int32_t numOfResCol = tsCompatibleModel? 6:5;
  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, numOfResCol);

  index = tsCompatibleModel? 2:1;
  SExprInfo* p2 = (SExprInfo*)taosArrayGetP(pExprList, index);
  ASSERT_EQ(p2->base.pColumns->uid, 110);
  ASSERT_EQ(p2->base.numOfParams, 0);
  ASSERT_EQ(p2->base.resSchema.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_STRCASEEQ(p2->base.resSchema.name, "first(b*a)");

  ASSERT_EQ(p2->base.pColumns->flag, TSDB_COL_TMP);
  ASSERT_STREQ(p2->base.pColumns->name, "t.1abc.b*a");

  ASSERT_STRCASEEQ(p2->base.token, "first(b*a)");
  ASSERT_EQ(p2->base.interBytes, 24);
  ASSERT_EQ(p2->pExpr->nodeType, TEXPR_FUNCTION_NODE);
  ASSERT_STRCASEEQ(p2->pExpr->_function.functionName, "first");
  ASSERT_EQ(p2->pExpr->_function.num, 1);
  ASSERT_EQ(p2->pExpr->_function.pChild[0]->nodeType, TEXPR_COL_NODE);
  ASSERT_STREQ(p2->pExpr->_function.pChild[0]->pSchema->name, "t.1abc.b*a");

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

 TEST(testCase, function_Test7) {
  SSqlInfo info1 = doGenerateAST("select count(a+b),count(1) from `t.1abc` interval(10s, 1s)");
  ASSERT_EQ(info1.valid, true);

  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.sub.node), 0);
  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SCatalogReq req = {0};
   SParseContext ctx = {0};
  ctx.db = "db1";
  ctx.acctId = 1;
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);
  ASSERT_EQ(ret, 0);

  SArray* pExprList = pQueryInfo->exprList[0];
  ASSERT_EQ(taosArrayGetSize(pExprList), 3);

  int32_t index = tsCompatibleModel? 1:0;
  SExprInfo* p1 = (SExprInfo*) taosArrayGetP(pExprList, index);
  ASSERT_EQ(p1->base.pColumns->uid, 110);
  ASSERT_EQ(p1->base.numOfParams, 0);
  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_STRCASEEQ(p1->base.resSchema.name, "count(a+b)");
  ASSERT_EQ(p1->base.pColumns->flag, TSDB_COL_TMP);
  ASSERT_STRCASEEQ(p1->base.token, "count(a+b)");
  ASSERT_EQ(p1->base.interBytes, 8);
  ASSERT_EQ(p1->pExpr->nodeType, TEXPR_FUNCTION_NODE);
  ASSERT_STREQ(p1->pExpr->_function.functionName, "count");

  tExprNode* pParam = p1->pExpr->_function.pChild[0];
  ASSERT_EQ(pParam->nodeType, TEXPR_COL_NODE);

  SExprInfo* p2 = (SExprInfo*) taosArrayGetP(pQueryInfo->exprList[1], 0);
  ASSERT_EQ(p2->pExpr->nodeType, TEXPR_BINARYEXPR_NODE);

  ASSERT_EQ(p2->pExpr->_node.optr, OP_TYPE_ADD);
  ASSERT_EQ(p2->pExpr->_node.pLeft->nodeType, TEXPR_COL_NODE);
  ASSERT_EQ(p2->pExpr->_node.pRight->nodeType, TEXPR_COL_NODE);

  ASSERT_EQ(pParam->pSchema->colId, p2->base.resSchema.colId);

  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 3);

  int32_t numOfCols = tsCompatibleModel? 3:2;
  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, numOfCols);

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

 TEST(testCase, function_Test8) {
  SSqlInfo info1 = doGenerateAST("select top(a*b / 99, 20) from `t.1abc` interval(10s, 1s)");
  ASSERT_EQ(info1.valid, true);

  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.sub.node), 0);
  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SCatalogReq req = {0};
   SParseContext ctx = {0};
  ctx.db = "db1";
  ctx.acctId = 1;
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);
  ASSERT_EQ(ret, 0);

  SArray* pExprList = pQueryInfo->exprList[0];
  ASSERT_EQ(taosArrayGetSize(pExprList), 2);

  SExprInfo* p1 = (SExprInfo*) taosArrayGetP(pExprList, 1);
  ASSERT_EQ(p1->base.pColumns->uid, 110);
  ASSERT_EQ(p1->base.numOfParams, 1);
  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_STRCASEEQ(p1->base.resSchema.name, "top(a*b / 99, 20)");
  ASSERT_EQ(p1->base.pColumns->flag, TSDB_COL_TMP);
  ASSERT_STRCASEEQ(p1->base.token, "top(a*b / 99, 20)");
  ASSERT_EQ(p1->base.interBytes, 16);

  ASSERT_EQ(p1->pExpr->nodeType, TEXPR_FUNCTION_NODE);
  ASSERT_STRCASEEQ(p1->pExpr->_function.functionName, "top");
  ASSERT_TRUE(p1->pExpr->_function.num == 1);

  tExprNode* pParam = p1->pExpr->_function.pChild[0];

  ASSERT_EQ(pParam->nodeType, TSDB_COL_TMP);
//  ASSERT_EQ(pParam->.optr, TSDB_BINARY_OP_DIVIDE);
//  ASSERT_EQ(pParam->_node.pLeft->nodeType, TEXPR_BINARYEXPR_NODE);
//  ASSERT_EQ(pParam->_node.pRight->nodeType, TEXPR_VALUE_NODE);

  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 3);
  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 2);

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  info1 = doGenerateAST("select sum(length(a)+length(b)) from `t.1abc` interval(10s, 1s)");
  ASSERT_EQ(info1.valid, true);

  pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.sub.node), 0);
  code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);
  ASSERT_EQ(ret, 0);

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

 TEST(testCase, invalid_sql_Test) {
  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  SSqlInfo info1 = doGenerateAST("select count(k) from `t.1abc` interval(10s, 1s)");
  ASSERT_EQ(info1.valid, true);

  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.sub.node), 0);
  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  SCatalogReq req = {0};
   SParseContext ctx = {0};
  ctx.db = "db1";
  ctx.acctId = 1;
  int32_t  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  SQueryStmtInfo* pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  SSqlNode* pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);
  ASSERT_NE(ret, 0);

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
//===============================================================================================================
  info1 = doGenerateAST("select top(a*b, ABC) from `t.1abc` interval(10s, 1s)");
  ASSERT_EQ(info1.valid, true);

  pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.sub.node), 0);
  code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
  ASSERT_EQ(code, 0);

  ret = qParserExtractRequestedMetaInfo(&info1, &req, &ctx, msg, 128);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  pQueryInfo = createQueryInfo();
  setTableMetaInfo(pQueryInfo, &req);

  pSqlNode = (SSqlNode*)taosArrayGetP(info1.sub.node, 0);
  ret = validateSqlNode(pSqlNode, pQueryInfo, &buf);
  ASSERT_NE(ret, 0);

  destroyQueryInfo(pQueryInfo);
  qParserCleanupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

TEST(testCase, show_user_Test) {
  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  char sql1[] = "show users";
  SSqlInfo info1 = doGenerateAST(sql1);
  ASSERT_EQ(info1.valid, true);

  SParseContext ct= {.requestId = 1, .acctId = 1, .db = "abc", .pTransporter = NULL};
  SDclStmtInfo* output = qParserValidateDclSqlNode(&info1, &ct, msg, buf.len);
  ASSERT_NE(output, nullptr);

  // convert the show command to be the select query
  // select name, privilege, create_time, account from information_schema.users;
}

TEST(testCase, create_user_Test) {
  char    msg[128] = {0};
  SMsgBuf buf;
  buf.len = 128;
  buf.buf = msg;

  char sql[] = {"create user abc pass 'abc'"};

  SSqlInfo info1 = doGenerateAST(sql);
  ASSERT_EQ(info1.valid, true);
  ASSERT_EQ(isDclSqlStatement(&info1), true);

  SParseContext ct= {.requestId = 1, .acctId = 1, .db = "abc"};
  SDclStmtInfo* output = qParserValidateDclSqlNode(&info1, &ct, msg, buf.len);
  ASSERT_NE(output, nullptr);

  destroySqlInfo(&info1);
}

#pragma GCC diagnostic pop