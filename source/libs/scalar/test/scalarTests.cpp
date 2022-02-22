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
#include <tglobal.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

#include "os.h"

#include "taos.h"
#include "tdef.h"
#include "tvariant.h"
#include "tep.h"
#include "stub.h"
#include "addr_any.h"
#include "scalar.h"
#include "nodes.h"
#include "tlog.h"

namespace {

int64_t scltLeftV = 21, scltRightV = 10;
double scltLeftVd = 21.0, scltRightVd = 10.0;

void scltInitLogFile() {
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;

  char temp[128] = {0};
  sprintf(temp, "%s/%s", tsLogDir, defaultLogFileNamePrefix);
  if (taosInitLog(temp, tsNumOfLogLines, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}


void scltMakeValueNode(SNode **pNode, int32_t dataType, void *value) {
  SNode *node = nodesMakeNode(QUERY_NODE_VALUE);
  SValueNode *vnode = (SValueNode *)node;
  vnode->node.resType.type = dataType;

  if (IS_VAR_DATA_TYPE(dataType)) {
    vnode->datum.p = (char *)malloc(varDataTLen(value));
    varDataCopy(vnode->datum.p, value);
    vnode->node.resType.bytes = varDataLen(value);
  } else {
    vnode->node.resType.bytes = tDataTypes[dataType].bytes;
    assignVal((char *)nodesGetValueFromNode(vnode), (const char *)value, 0, dataType);
  }
  
  *pNode = (SNode *)vnode;
}

void scltMakeColRefNode(SNode **pNode, SSDataBlock **block, int32_t dataType, int32_t dataBytes, int32_t rowNum, void *value) {
  SNode *node = nodesMakeNode(QUERY_NODE_COLUMN_REF);
  SColumnRefNode *rnode = (SColumnRefNode *)node;
  rnode->dataType.type = dataType;
  rnode->dataType.bytes = dataBytes;
  rnode->tupleId = 0;

  if (NULL == *block) {
    SSDataBlock *res = (SSDataBlock *)calloc(1, sizeof(SSDataBlock));
    res->info.numOfCols = 3;
    res->info.rows = rowNum;
    res->pDataBlock = taosArrayInit(3, sizeof(SColumnInfoData));
    for (int32_t i = 0; i < 2; ++i) {
      SColumnInfoData idata = {{0}};
      idata.info.type  = TSDB_DATA_TYPE_NULL;
      idata.info.bytes = 10;
      idata.info.colId = 0;

      int32_t size = idata.info.bytes * rowNum;
      idata.pData = (char *)calloc(1, size);
      taosArrayPush(res->pDataBlock, &idata);
    }

    SColumnInfoData idata = {{0}};
    idata.info.type  = dataType;
    idata.info.bytes = dataBytes;
    idata.info.colId = 55;
    idata.pData = (char *)value;
    if (IS_VAR_DATA_TYPE(dataType)) {
      idata.varmeta.offset = (int32_t *)calloc(rowNum, sizeof(int32_t));
      for (int32_t i = 0; i < rowNum; ++i) {
        idata.varmeta.offset[i] = (dataBytes + VARSTR_HEADER_SIZE) * i;
      }
    }
    taosArrayPush(res->pDataBlock, &idata);

    rnode->slotId = 2;
    rnode->columnId = 55;

    *block = res;
  } else {
    SSDataBlock *res = *block;
    
    int32_t idx = taosArrayGetSize(res->pDataBlock);
    SColumnInfoData idata = {{0}};
    idata.info.type  = dataType;
    idata.info.bytes = dataBytes;
    idata.info.colId = 55 + idx;
    idata.pData = (char *)value;
    taosArrayPush(res->pDataBlock, &idata);
    
    rnode->slotId = idx;
    rnode->columnId = 55 + idx;
  }

  *pNode = (SNode *)rnode;
}

void scltMakeOpNode(SNode **pNode, EOperatorType opType, int32_t resType, SNode *pLeft, SNode *pRight) {
  SNode *node = nodesMakeNode(QUERY_NODE_OPERATOR);
  SOperatorNode *onode = (SOperatorNode *)node;
  onode->node.resType.type = resType;
  onode->node.resType.bytes = tDataTypes[resType].bytes;
  
  onode->opType = opType;
  onode->pLeft = pLeft;
  onode->pRight = pRight;

  *pNode = (SNode *)onode;
}


void scltMakeListNode(SNode **pNode, SNodeList *list, int32_t resType) {
  SNode *node = nodesMakeNode(QUERY_NODE_NODE_LIST);
  SNodeListNode *lnode = (SNodeListNode *)node;
  lnode->dataType.type = resType;
  lnode->pNodeList = list;

  *pNode = (SNode *)lnode;
}


}

TEST(constantTest, bigint_add_bigint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BIGINT, &scltLeftV);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BIGINT, &scltRightV);
  scltMakeOpNode(&opNode, OP_TYPE_ADD, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(v->datum.d, (scltLeftV + scltRightV));
}

TEST(constantTest, double_sub_bigint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_DOUBLE, &scltLeftVd);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BIGINT, &scltRightV);
  scltMakeOpNode(&opNode, OP_TYPE_SUB, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(v->datum.d, (scltLeftVd - scltRightV));
}

TEST(constantTest, tinyint_and_smallint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_TINYINT, &scltLeftV);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &scltRightV);
  scltMakeOpNode(&opNode, OP_TYPE_BIT_AND, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(v->datum.i, (int64_t)scltLeftV & (int64_t)scltRightV);
}

TEST(constantTest, bigint_or_double) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BIGINT, &scltLeftV);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &scltRightVd);
  scltMakeOpNode(&opNode, OP_TYPE_BIT_OR, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(v->datum.i, (int64_t)scltLeftV | (int64_t)scltRightVd);
}

TEST(constantTest, int_or_binary) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char binaryStr[64] = {0};
  sprintf(&binaryStr[2], "%d", scltRightV);
  varDataSetLen(binaryStr, strlen(&binaryStr[2]));
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &scltLeftV);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, binaryStr);
  scltMakeOpNode(&opNode, OP_TYPE_BIT_OR, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(v->datum.b, scltLeftV | scltRightV);
}


TEST(constantTest, int_greater_double) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &scltLeftV);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &scltRightVd);
  scltMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, scltLeftV > scltRightVd);
}

TEST(constantTest, int_greater_equal_binary) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char binaryStr[64] = {0};
  sprintf(&binaryStr[2], "%d", scltRightV);
  varDataSetLen(binaryStr, strlen(&binaryStr[2]));
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &scltLeftV);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, binaryStr);
  scltMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, scltLeftV > scltRightVd);
}

TEST(constantTest, tinyint_lower_ubigint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_TINYINT, &scltLeftV);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_UBIGINT, &scltRightV);
  scltMakeOpNode(&opNode, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, scltLeftV < scltRightV);
}

TEST(constantTest, usmallint_lower_equal_ubigint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_USMALLINT, &leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_UBIGINT, &rightv);
  scltMakeOpNode(&opNode, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, leftv <= rightv);
}

TEST(constantTest, int_equal_smallint1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv);
  scltMakeOpNode(&opNode, OP_TYPE_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, leftv == rightv);
}

TEST(constantTest, int_equal_smallint2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 0, rightv = 1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv);
  scltMakeOpNode(&opNode, OP_TYPE_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, leftv == rightv);
}

TEST(constantTest, int_not_equal_smallint1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv);
  scltMakeOpNode(&opNode, OP_TYPE_NOT_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, leftv != rightv);
}

TEST(constantTest, int_not_equal_smallint2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 0, rightv = 1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv);
  scltMakeOpNode(&opNode, OP_TYPE_NOT_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, leftv != rightv);
}



TEST(constantTest, int_in_smallint1) {
  scltInitLogFile();
  
  SNode *pLeft = NULL, *pRight = NULL, *listNode = NULL, *res = NULL, *opNode = NULL;
  int32_t leftv = 1, rightv1 = 1,rightv2 = 2,rightv3 = 3;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  SNodeList* list = nodesMakeList();
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv1);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv2);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv3);
  nodesListAppend(list, pRight);
  scltMakeListNode(&listNode,list, TSDB_DATA_TYPE_INT);
  scltMakeOpNode(&opNode, OP_TYPE_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
}

TEST(constantTest, int_in_smallint2) {
  scltInitLogFile();
  
  SNode *pLeft = NULL, *pRight = NULL, *listNode = NULL, *res = NULL, *opNode = NULL;
  int32_t leftv = 4, rightv1 = 1,rightv2 = 2,rightv3 = 3;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  SNodeList* list = nodesMakeList();
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv1);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv2);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv3);
  nodesListAppend(list, pRight);
  scltMakeListNode(&listNode,list, TSDB_DATA_TYPE_INT);
  scltMakeOpNode(&opNode, OP_TYPE_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
}

TEST(constantTest, int_not_in_smallint1) {
  SNode *pLeft = NULL, *pRight = NULL, *listNode = NULL, *res = NULL, *opNode = NULL;
  int32_t leftv = 1, rightv1 = 1,rightv2 = 2,rightv3 = 3;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  SNodeList* list = nodesMakeList();
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv1);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv2);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv3);
  nodesListAppend(list, pRight);
  scltMakeListNode(&listNode,list, TSDB_DATA_TYPE_INT);
  scltMakeOpNode(&opNode, OP_TYPE_NOT_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
}

TEST(constantTest, int_not_in_smallint2) {
  scltInitLogFile();
  
  SNode *pLeft = NULL, *pRight = NULL, *listNode = NULL, *res = NULL, *opNode = NULL;
  int32_t leftv = 4, rightv1 = 1,rightv2 = 2,rightv3 = 3;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  SNodeList* list = nodesMakeList();
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv1);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv2);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv3);
  nodesListAppend(list, pRight);
  scltMakeListNode(&listNode,list, TSDB_DATA_TYPE_INT);
  scltMakeOpNode(&opNode, OP_TYPE_NOT_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
}

TEST(constantTest, binary_like_binary1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char leftv[64] = {0}, rightv[64] = {0};
  sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  sprintf(&rightv[2], "%s", "a_c");
  varDataSetLen(rightv, strlen(&rightv[2]));
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_LIKE, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
}

TEST(constantTest, binary_like_binary2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char leftv[64] = {0}, rightv[64] = {0};
  sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  sprintf(&rightv[2], "%s", "ac");
  varDataSetLen(rightv, strlen(&rightv[2]));
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_LIKE, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
}

TEST(constantTest, binary_not_like_binary1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char leftv[64] = {0}, rightv[64] = {0};
  sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  sprintf(&rightv[2], "%s", "a%c");
  varDataSetLen(rightv, strlen(&rightv[2]));
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_NOT_LIKE, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
}

TEST(constantTest, binary_not_like_binary2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char leftv[64] = {0}, rightv[64] = {0};
  sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  sprintf(&rightv[2], "%s", "ac");
  varDataSetLen(rightv, strlen(&rightv[2]));
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_NOT_LIKE, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
}

TEST(constantTest, binary_match_binary1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char leftv[64] = {0}, rightv[64] = {0};
  sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  sprintf(&rightv[2], "%s", ".*");
  varDataSetLen(rightv, strlen(&rightv[2]));
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_MATCH, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
}

TEST(constantTest, binary_match_binary2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char leftv[64] = {0}, rightv[64] = {0};
  sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  sprintf(&rightv[2], "%s", "abc.+");
  varDataSetLen(rightv, strlen(&rightv[2]));
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_MATCH, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
}

TEST(constantTest, binary_not_match_binary1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char leftv[64] = {0}, rightv[64] = {0};
  sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  sprintf(&rightv[2], "%s", "a[1-9]c");
  varDataSetLen(rightv, strlen(&rightv[2]));
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_NMATCH, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
}

TEST(constantTest, binary_not_match_binary2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char leftv[64] = {0}, rightv[64] = {0};
  sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  sprintf(&rightv[2], "%s", "a[ab]c");
  varDataSetLen(rightv, strlen(&rightv[2]));
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_NMATCH, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
}

TEST(constantTest, int_is_null1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeOpNode(&opNode, OP_TYPE_IS_NULL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
}

TEST(constantTest, int_is_null2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = TSDB_DATA_INT_NULL, rightv = 1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeOpNode(&opNode, OP_TYPE_IS_NULL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
}

TEST(constantTest, int_is_not_null1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeOpNode(&opNode, OP_TYPE_IS_NOT_NULL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
}

TEST(constantTest, int_is_not_null2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = TSDB_DATA_INT_NULL, rightv = 1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeOpNode(&opNode, OP_TYPE_IS_NOT_NULL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
}

TEST(constantTest, int_add_int_is_true1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &rightv);
  scltMakeOpNode(&opNode, OP_TYPE_ADD, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  scltMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
}

TEST(constantTest, int_add_int_is_true2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = -1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &rightv);
  scltMakeOpNode(&opNode, OP_TYPE_ADD, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  scltMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
}


TEST(constantTest, int_greater_int_is_true1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &rightv);
  scltMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  scltMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
}

TEST(constantTest, int_greater_int_is_true2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 0;
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &rightv);
  scltMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  scltMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);
  
  int32_t code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
}

TEST(columnTest, smallint_value_add_int_column) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int32_t leftv = 1;
  int16_t rightv[5]= {0, -5, -4, 23, 100};
  double eRes[5] = {1.0, -4, -3, 24, 101};
  SSDataBlock *src = NULL;
  SScalarParam res = {0};
  int32_t rowNum = sizeof(rightv)/sizeof(rightv[0]);
  scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  scltMakeColRefNode(&pRight, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_ADD, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  
  int32_t code = scalarCalculate(opNode, src, &res);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.num, rowNum);
  ASSERT_EQ(res.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(res.bytes, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)res.data + i), eRes[i]);
  }
}

TEST(columnTest, bigint_column_multi_binary_column) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int64_t leftv[5]= {1, 2, 3, 4, 5};
  char rightv[5][5]= {0};
  for (int32_t i = 0; i < 5; ++i) {
    rightv[i][2] = rightv[i][3] = '0';
    rightv[i][4] = '0' + i;
    varDataSetLen(rightv[i], 3);
  }
  double eRes[5] = {0, 2, 6, 12, 20};
  SSDataBlock *src = NULL;
  SScalarParam res = {0};
  int32_t rowNum = sizeof(rightv)/sizeof(rightv[0]);
  scltMakeColRefNode(&pLeft, &src, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), rowNum, leftv);
  scltMakeColRefNode(&pRight, &src, TSDB_DATA_TYPE_BINARY, 5, rowNum, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_MULTI, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  
  int32_t code = scalarCalculate(opNode, src, &res);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.num, rowNum);
  ASSERT_EQ(res.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(res.bytes, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)res.data + i), eRes[i]);
  }
}

TEST(columnTest, smallint_column_and_binary_column) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t leftv[5]= {1, 2, 3, 4, 5};
  char rightv[5][5]= {0};
  for (int32_t i = 0; i < 5; ++i) {
    rightv[i][2] = rightv[i][3] = '0';
    rightv[i][4] = '0' + i;
    varDataSetLen(rightv[i], 3);
  }
  int64_t eRes[5] = {0, 0, 2, 0, 4};
  SSDataBlock *src = NULL;
  SScalarParam res = {0};
  int32_t rowNum = sizeof(rightv)/sizeof(rightv[0]);
  scltMakeColRefNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  scltMakeColRefNode(&pRight, &src, TSDB_DATA_TYPE_BINARY, 5, rowNum, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_BIT_AND, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  
  int32_t code = scalarCalculate(opNode, src, &res);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.num, rowNum);
  ASSERT_EQ(res.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(res.bytes, tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int64_t *)res.data + i), eRes[i]);
  }
}

TEST(columnTest, smallint_column_or_float_column) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t leftv[5]= {1, 2, 3, 4, 5};
  float rightv[5]= {2.0, 3.0, 4.1, 5.2, 6.0};
  int64_t eRes[5] = {3, 3, 7, 5, 7};
  SSDataBlock *src = NULL;
  SScalarParam res = {0};
  int32_t rowNum = sizeof(rightv)/sizeof(rightv[0]);
  scltMakeColRefNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  scltMakeColRefNode(&pRight, &src, TSDB_DATA_TYPE_FLOAT, sizeof(float), rowNum, rightv);
  scltMakeOpNode(&opNode, OP_TYPE_BIT_OR, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  
  int32_t code = scalarCalculate(opNode, src, &res);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.num, rowNum);
  ASSERT_EQ(res.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(res.bytes, tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int64_t *)res.data + i), eRes[i]);
  }
}

TEST(columnTest, smallint_column_or_double_value) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t leftv[5]= {1, 2, 3, 4, 5};
  double rightv= 10.2;
  int64_t eRes[5] = {11, 10, 11, 14, 15};
  SSDataBlock *src = NULL;
  SScalarParam res = {0};
  int32_t rowNum = sizeof(leftv)/sizeof(leftv[0]);
  scltMakeColRefNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv);
  scltMakeOpNode(&opNode, OP_TYPE_BIT_OR, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  
  int32_t code = scalarCalculate(opNode, src, &res);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.num, rowNum);
  ASSERT_EQ(res.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(res.bytes, tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int64_t *)res.data + i), eRes[i]);
  }
}

TEST(columnTest, smallint_column_greater_double_value) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t leftv[5]= {1, 2, 3, 4, 5};
  double rightv= 2.5;
  bool eRes[5] = {false, false, true, true, true};
  SSDataBlock *src = NULL;
  SScalarParam res = {0};
  int32_t rowNum = sizeof(leftv)/sizeof(leftv[0]);
  scltMakeColRefNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv);
  scltMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  
  int32_t code = scalarCalculate(opNode, src, &res);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.num, rowNum);
  ASSERT_EQ(res.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(res.bytes, tDataTypes[TSDB_DATA_TYPE_BOOL].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((bool *)res.data + i), eRes[i]);
  }
}

TEST(columnTest, int_column_in_double_list) {
  SNode *pLeft = NULL, *pRight = NULL, *listNode = NULL, *opNode = NULL;
  int32_t leftv[5] = {1, 2, 3, 4, 5};
  double rightv1 = 1.1,rightv2 = 2.2,rightv3 = 3.3;
  bool eRes[5] = {true, true, true, false, false};  
  SSDataBlock *src = NULL;  
  SScalarParam res = {0};
  int32_t rowNum = sizeof(leftv)/sizeof(leftv[0]);
  scltMakeColRefNode(&pLeft, &src, TSDB_DATA_TYPE_INT, sizeof(int32_t), rowNum, leftv);  
  SNodeList* list = nodesMakeList();
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv1);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv2);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv3);
  nodesListAppend(list, pRight);
  scltMakeListNode(&listNode,list, TSDB_DATA_TYPE_INT);
  scltMakeOpNode(&opNode, OP_TYPE_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  
  int32_t code = scalarCalculate(opNode, src, &res);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.num, rowNum);
  ASSERT_EQ(res.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(res.bytes, tDataTypes[TSDB_DATA_TYPE_BOOL].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((bool *)res.data + i), eRes[i]);
  }
}

TEST(columnTest, binary_column_in_binary_list) {
  SNode *pLeft = NULL, *pRight = NULL, *listNode = NULL, *opNode = NULL;
  bool eRes[5] = {true, true, false, false, false};  
  SSDataBlock *src = NULL;  
  SScalarParam res = {0};
  char leftv[5][5]= {0};
  char rightv[3][5]= {0};
  for (int32_t i = 0; i < 5; ++i) {
    leftv[i][2] = 'a' + i;
    leftv[i][3] = 'b' + i;
    leftv[i][4] = '0' + i;
    varDataSetLen(leftv[i], 3);
  }  
  for (int32_t i = 0; i < 2; ++i) {
    rightv[i][2] = 'a' + i;
    rightv[i][3] = 'b' + i;
    rightv[i][4] = '0' + i;
    varDataSetLen(rightv[i], 3);
  }  
  for (int32_t i = 2; i < 3; ++i) {
    rightv[i][2] = 'a' + i;
    rightv[i][3] = 'a' + i;
    rightv[i][4] = 'a' + i;
    varDataSetLen(rightv[i], 3);
  }
  
  int32_t rowNum = sizeof(leftv)/sizeof(leftv[0]);
  scltMakeColRefNode(&pLeft, &src, TSDB_DATA_TYPE_BINARY, 3, rowNum, leftv);  
  SNodeList* list = nodesMakeList();
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv[0]);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv[1]);
  nodesListAppend(list, pRight);
  scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv[2]);
  nodesListAppend(list, pRight);
  scltMakeListNode(&listNode,list, TSDB_DATA_TYPE_BINARY);
  scltMakeOpNode(&opNode, OP_TYPE_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  
  int32_t code = scalarCalculate(opNode, src, &res);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.num, rowNum);
  ASSERT_EQ(res.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(res.bytes, tDataTypes[TSDB_DATA_TYPE_BOOL].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((bool *)res.data + i), eRes[i]);
  }
}



int main(int argc, char** argv) {
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop
