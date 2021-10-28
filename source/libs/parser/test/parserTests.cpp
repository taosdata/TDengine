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

TEST(testCase, validateAST_test) {
  SSqlInfo info1 = doGenerateAST("select a a1111, a+b + 22, tbname from `t.1abc` where ts<now+2h and `col` < 20 + 99");
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

  SArray* pExprList = pQueryInfo->exprList;
  ASSERT_EQ(taosArrayGetSize(pExprList), 3);

  SExprInfo* p1 = (SExprInfo*) taosArrayGetP(pExprList, 0);
  ASSERT_EQ(p1->base.uid, 110);
  ASSERT_EQ(p1->base.numOfParams, 0);
  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_INT);
  ASSERT_STRCASEEQ(p1->base.resSchema.name, "a1111");
  ASSERT_STRCASEEQ(p1->base.colInfo.name, "t.1abc.a");
  ASSERT_EQ(p1->base.colInfo.colId, 1);
  ASSERT_EQ(p1->base.colInfo.flag, TSDB_COL_NORMAL);
  ASSERT_STRCASEEQ(p1->base.token, "a");

  ASSERT_EQ(taosArrayGetSize(pExprList), 3);

  SExprInfo* p2 = (SExprInfo*) taosArrayGetP(pExprList, 1);
  ASSERT_EQ(p2->base.uid, 0);
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
  qParserClearupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

//TEST(testCase, function_Test) {
//  SSqlInfo info1 = doGenerateAST("select count(a) from `t.1abc`");
//  ASSERT_EQ(info1.valid, true);
//
//  char    msg[128] = {0};
//  SMsgBuf buf;
//  buf.len = 128;
//  buf.buf = msg;
//
//  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.list), 0);
//  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
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
//
//  SArray* pExprList = pQueryInfo->exprList;
//  ASSERT_EQ(taosArrayGetSize(pExprList), 1);
//
//  SExprInfo* p1 = (SExprInfo*) taosArrayGetP(pExprList, 0);
//  ASSERT_EQ(p1->base.uid, 110);
//  ASSERT_EQ(p1->base.numOfParams, 0);
//  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_BIGINT);
//  ASSERT_STRCASEEQ(p1->base.resSchema.name, "count(a)");
//  ASSERT_STRCASEEQ(p1->base.colInfo.name, "t.1abc.a");
//  ASSERT_EQ(p1->base.colInfo.colId, 1);
//  ASSERT_EQ(p1->base.colInfo.flag, TSDB_COL_NORMAL);
//  ASSERT_STRCASEEQ(p1->base.token, "count(a)");
//  ASSERT_EQ(p1->base.interBytes, 8);
//
//  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 2);
//  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 1);
//
//  destroyQueryInfo(pQueryInfo);
//  qParserClearupMetaRequestInfo(&req);
//  destroySqlInfo(&info1);
//}
//
//TEST(testCase, function_Test2) {
//  SSqlInfo info1 = doGenerateAST("select count(a) abc from `t.1abc`");
//  ASSERT_EQ(info1.valid, true);
//
//  char    msg[128] = {0};
//  SMsgBuf buf;
//  buf.len = 128;
//  buf.buf = msg;
//
//  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.list), 0);
//  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
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
//
//  SArray* pExprList = pQueryInfo->exprList;
//  ASSERT_EQ(taosArrayGetSize(pExprList), 1);
//
//  SExprInfo* p1 = (SExprInfo*) taosArrayGetP(pExprList, 0);
//  ASSERT_EQ(p1->base.uid, 110);
//  ASSERT_EQ(p1->base.numOfParams, 0);
//  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_BIGINT);
//  ASSERT_STRCASEEQ(p1->base.resSchema.name, "abc");
//  ASSERT_STRCASEEQ(p1->base.colInfo.name, "t.1abc.a");
//  ASSERT_EQ(p1->base.colInfo.colId, 1);
//  ASSERT_EQ(p1->base.colInfo.flag, TSDB_COL_NORMAL);
//  ASSERT_STRCASEEQ(p1->base.token, "count(a)");
//  ASSERT_EQ(p1->base.interBytes, 8);
//
//  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 2);
//  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 1);
//
//  destroyQueryInfo(pQueryInfo);
//  qParserClearupMetaRequestInfo(&req);
//  destroySqlInfo(&info1);
//}
//
//TEST(testCase, function_Test3) {
//  SSqlInfo info1 = doGenerateAST("select first(*) from `t.1abc`");
//  ASSERT_EQ(info1.valid, true);
//
//  char    msg[128] = {0};
//  SMsgBuf buf;
//  buf.len = 128;
//  buf.buf = msg;
//
//  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.list), 0);
//  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
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
//
//  SArray* pExprList = pQueryInfo->exprList;
//  ASSERT_EQ(taosArrayGetSize(pExprList), 4);
//
//  SExprInfo* p1 = (SExprInfo*) taosArrayGetP(pExprList, 0);
//  ASSERT_EQ(p1->base.uid, 110);
//  ASSERT_EQ(p1->base.numOfParams, 0);
//  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_TIMESTAMP);
//  ASSERT_STRCASEEQ(p1->base.resSchema.name, "first(ts)");
//  ASSERT_STRCASEEQ(p1->base.colInfo.name, "t.1abc.ts");
//  ASSERT_EQ(p1->base.colInfo.colId, 0);
//  ASSERT_EQ(p1->base.colInfo.flag, TSDB_COL_NORMAL);
//  ASSERT_STRCASEEQ(p1->base.token, "first(ts)");
//  ASSERT_EQ(p1->base.interBytes, 24);
//
//  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 4);
//
//  destroyQueryInfo(pQueryInfo);
//  qParserClearupMetaRequestInfo(&req);
//  destroySqlInfo(&info1);
//}
//
//TEST(testCase, function_Test4) {
//  SSqlInfo info1 = doGenerateAST("select _block_dist() as a1 from `t.1abc`");
//  ASSERT_EQ(info1.valid, true);
//
//  char    msg[128] = {0};
//  SMsgBuf buf;
//  buf.len = 128;
//  buf.buf = msg;
//
//  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.list), 0);
//  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
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
//
//  SArray* pExprList = pQueryInfo->exprList;
//  ASSERT_EQ(taosArrayGetSize(pExprList), 1);
//
//  SExprInfo* p1 = (SExprInfo*) taosArrayGetP(pExprList, 0);
//  ASSERT_EQ(p1->base.uid, 110);
//  ASSERT_EQ(p1->base.numOfParams, 1);
//  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_BINARY);
//  ASSERT_STRCASEEQ(p1->base.resSchema.name, "a1");
////  ASSERT_STRCASEEQ(p1->base.colInfo.name, "t.1abc.ts");
////  ASSERT_EQ(p1->base.colInfo.colId, 0);
//  ASSERT_EQ(p1->base.colInfo.flag, TSDB_COL_NORMAL);
//  ASSERT_STRCASEEQ(p1->base.token, "_block_dist()");
//  ASSERT_EQ(p1->base.interBytes, 0);
//
//  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 1);
//  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 1);
//
//  destroyQueryInfo(pQueryInfo);
//  qParserClearupMetaRequestInfo(&req);
//  destroySqlInfo(&info1);
//}
//
//TEST(testCase, function_Test5) {
//  SSqlInfo info1 = doGenerateAST("select sum(a) + avg(b)  as a1 from `t.1abc`");
//  ASSERT_EQ(info1.valid, true);
//
//  char    msg[128] = {0};
//  SMsgBuf buf;
//  buf.len = 128;
//  buf.buf = msg;
//
//  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.list), 0);
//  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
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
//  ASSERT_EQ(taosArrayGetSize(pExprList), 3);
//
//  SExprInfo* p1 = (SExprInfo*) taosArrayGetP(pExprList, 0);
//  ASSERT_EQ(p1->base.uid, 0);
//  ASSERT_EQ(p1->base.numOfParams, 1);
//  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_DOUBLE);
//  ASSERT_STRCASEEQ(p1->base.resSchema.name, "a1");
////  ASSERT_STRCASEEQ(p1->base.colInfo.name, "t.1abc.ts");
////  ASSERT_EQ(p1->base.colInfo.colId, 0);
//  ASSERT_EQ(p1->base.colInfo.flag, TSDB_COL_NORMAL);
//  ASSERT_STRCASEEQ(p1->base.token, "sum(a) + avg(b)");
//  ASSERT_EQ(p1->base.interBytes, 0);
//
//  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 3);
//  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 1);
//
//  destroyQueryInfo(pQueryInfo);
//  qParserClearupMetaRequestInfo(&req);
//  destroySqlInfo(&info1);
//}
//
//TEST(testCase, function_Test6) {
//  SSqlInfo info1 = doGenerateAST("select sum(a+b) as a1, first(b*a) from `t.1abc` interval(10s, 1s)");
//  ASSERT_EQ(info1.valid, true);
//
//  char    msg[128] = {0};
//  SMsgBuf buf;
//  buf.len = 128;
//  buf.buf = msg;
//
//  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.list), 0);
//  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &buf);
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
//  SExprInfo* p1 = (SExprInfo*) taosArrayGetP(pExprList, 0);
//  ASSERT_EQ(p1->base.uid, 110);
//  ASSERT_EQ(p1->base.numOfParams, 0);
//  ASSERT_EQ(p1->base.resSchema.type, TSDB_DATA_TYPE_DOUBLE);
//  ASSERT_STRCASEEQ(p1->base.resSchema.name, "a1");
//  ASSERT_EQ(p1->base.colInfo.flag, TSDB_COL_NORMAL);
//  ASSERT_STRCASEEQ(p1->base.token, "sum(a+b)");
//  ASSERT_EQ(p1->base.interBytes, 16);
//  ASSERT_EQ(p1->pExpr->nodeType, TEXPR_UNARYEXPR_NODE);
//  ASSERT_EQ(p1->pExpr->_node.functionId, FUNCTION_SUM);
//  ASSERT_TRUE(p1->pExpr->_node.pRight == NULL);
//
//  tExprNode* pParam = p1->pExpr->_node.pLeft;
//
//  ASSERT_EQ(pParam->nodeType, TEXPR_BINARYEXPR_NODE);
//  ASSERT_EQ(pParam->_node.optr, TSDB_BINARY_OP_ADD);
//  ASSERT_EQ(pParam->_node.pLeft->nodeType, TEXPR_COL_NODE);
//  ASSERT_EQ(pParam->_node.pRight->nodeType, TEXPR_COL_NODE);
//
//  ASSERT_EQ(taosArrayGetSize(pQueryInfo->colList), 3);
//  ASSERT_EQ(pQueryInfo->fieldsInfo.numOfOutput, 2);
//
//  destroyQueryInfo(pQueryInfo);
//  qParserClearupMetaRequestInfo(&req);
//  destroySqlInfo(&info1);
//}