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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"
#include <addr_any.h>

#ifdef WINDOWS
#define TD_USE_WINSOCK
#endif
#include "os.h"

#include "filter.h"
#include "filterInt.h"
#include "nodes.h"
#include "scalar.h"
#include "stub.h"
#include "taos.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "tlog.h"
#include "tvariant.h"

namespace {

int64_t flttLeftV = 21, flttRightV = 10;
double  flttLeftVd = 21.0, flttRightVd = 10.0;

void flttInitLogFile() {
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  strcpy(tsLogDir, TD_LOG_DIR_PATH);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

void flttMakeValueNode(SNode **pNode, int32_t dataType, void *value) {
  SNode      *node = (SNode *)nodesMakeNode(QUERY_NODE_VALUE);
  SValueNode *vnode = (SValueNode *)node;
  vnode->node.resType.type = dataType;

  if (IS_VAR_DATA_TYPE(dataType)) {
    vnode->datum.p = (char *)taosMemoryMalloc(varDataTLen(value));
    varDataCopy(vnode->datum.p, value);
    vnode->node.resType.bytes = varDataLen(value);
  } else {
    vnode->node.resType.bytes = tDataTypes[dataType].bytes;
    assignVal((char *)nodesGetValueFromNode(vnode), (const char *)value, 0, dataType);
  }

  *pNode = (SNode *)vnode;
}

void flttMakeColumnNode(SNode **pNode, SSDataBlock **block, int32_t dataType, int32_t dataBytes, int32_t rowNum,
                        void *value) {
  static uint64_t dbidx = 0;

  SNode       *node = (SNode *)nodesMakeNode(QUERY_NODE_COLUMN);
  SColumnNode *rnode = (SColumnNode *)node;
  rnode->node.resType.type = dataType;
  rnode->node.resType.bytes = dataBytes;
  rnode->dataBlockId = 0;

  sprintf(rnode->dbName, "%" PRIu64, dbidx++);

  if (NULL == block) {
    rnode->slotId = 2;
    rnode->colId = 3;
    *pNode = (SNode *)rnode;

    return;
  }

  if (NULL == *block) {
    SSDataBlock *res = createDataBlock();
    for (int32_t i = 0; i < 2; ++i) {
      SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_NULL, 10, 1 + i);
      blockDataAppendColInfo(res, &idata);
    }

    SColumnInfoData idata = createColumnInfoData(dataType, dataBytes, 3);
    blockDataAppendColInfo(res, &idata);
    blockDataEnsureCapacity(res, rowNum);

    SColumnInfoData *pColumn = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
    for (int32_t i = 0; i < rowNum; ++i) {
      colDataSetVal(pColumn, i, (const char *)value, false);
      if (IS_VAR_DATA_TYPE(dataType)) {
        value = (char *)value + varDataTLen(value);
      } else {
        value = (char *)value + dataBytes;
      }
    }

    rnode->slotId = 2;
    rnode->colId = 3;
    res->info.rows = rowNum;

    *block = res;
  } else {
    SSDataBlock *res = *block;

    int32_t         idx = taosArrayGetSize(res->pDataBlock);
    SColumnInfoData idata = createColumnInfoData(dataType, dataBytes, 1 + idx);
    blockDataAppendColInfo(res, &idata);
    blockDataEnsureCapacity(res, rowNum);

    SColumnInfoData *pColumn = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);

    for (int32_t i = 0; i < rowNum; ++i) {
      colDataSetVal(pColumn, i, (const char *)value, false);
      if (IS_VAR_DATA_TYPE(dataType)) {
        value = (char *)value + varDataTLen(value);
      } else {
        value = (char *)value + dataBytes;
      }
    }

    rnode->slotId = idx;
    rnode->colId = 1 + idx;
  }

  *pNode = (SNode *)rnode;
}

void flttMakeOpNode(SNode **pNode, EOperatorType opType, int32_t resType, SNode *pLeft, SNode *pRight) {
  SNode         *node = (SNode *)nodesMakeNode(QUERY_NODE_OPERATOR);
  SOperatorNode *onode = (SOperatorNode *)node;
  onode->node.resType.type = resType;
  onode->node.resType.bytes = tDataTypes[resType].bytes;

  onode->opType = opType;
  onode->pLeft = pLeft;
  onode->pRight = pRight;

  *pNode = (SNode *)onode;
}

void flttMakeLogicNode(SNode **pNode, ELogicConditionType opType, SNode **nodeList, int32_t nodeNum) {
  SNode               *node = (SNode *)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  SLogicConditionNode *onode = (SLogicConditionNode *)node;
  onode->condType = opType;
  onode->node.resType.type = TSDB_DATA_TYPE_BOOL;
  onode->node.resType.bytes = sizeof(bool);

  onode->pParameterList = nodesMakeList();
  for (int32_t i = 0; i < nodeNum; ++i) {
    nodesListAppend(onode->pParameterList, nodeList[i]);
  }

  *pNode = (SNode *)onode;
}

void flttMakeLogicNodeFromList(SNode **pNode, ELogicConditionType opType, SNodeList *nodeList) {
  SNode               *node = (SNode *)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  SLogicConditionNode *onode = (SLogicConditionNode *)node;
  onode->condType = opType;
  onode->node.resType.type = TSDB_DATA_TYPE_BOOL;
  onode->node.resType.bytes = sizeof(bool);

  onode->pParameterList = nodeList;

  *pNode = (SNode *)onode;
}

void flttMakeListNode(SNode **pNode, SNodeList *list, int32_t resType) {
  SNode         *node = (SNode *)nodesMakeNode(QUERY_NODE_NODE_LIST);
  SNodeListNode *lnode = (SNodeListNode *)node;
  lnode->node.resType.type = resType;
  lnode->pNodeList = list;

  *pNode = (SNode *)lnode;
}

void initScalarParam(SScalarParam *pParam) {
  memset(pParam, 0, sizeof(SScalarParam));
  pParam->colAlloced = true;
}

}  // namespace

TEST(timerangeTest, greater) {
  SNode       *pcol = NULL, *pval = NULL, *opNode1 = NULL;
  bool         eRes[5] = {false, false, true, true, true};
  SScalarParam res;
  initScalarParam(&res);

  int64_t tsmall = 222, tbig = 333;
  flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tsmall);
  flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);

  // SFilterInfo *filter = NULL;
  // int32_t code = filterInitFromNode(opNode1, &filter, FLT_OPTION_NO_REWRITE|FLT_OPTION_TIMESTAMP);
  // ASSERT_EQ(code, 0);
  STimeWindow win = {0};
  bool        isStrict = false;
  int32_t     code = filterGetTimeRange(opNode1, &win, &isStrict);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(isStrict, true);
  ASSERT_EQ(win.skey, tsmall + 1);
  ASSERT_EQ(win.ekey, INT64_MAX);
  // filterFreeInfo(filter);
  nodesDestroyNode(opNode1);
}

TEST(timerangeTest, greater_and_lower) {
  SNode       *pcol = NULL, *pval = NULL, *opNode1 = NULL, *opNode2 = NULL, *logicNode = NULL;
  bool         eRes[5] = {false, false, true, true, true};
  SScalarParam res;
  initScalarParam(&res);
  int64_t tsmall = 222, tbig = 333;
  flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tsmall);
  flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tbig);
  flttMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  SNode *list[2] = {0};
  list[0] = opNode1;
  list[1] = opNode2;

  flttMakeLogicNode(&logicNode, LOGIC_COND_TYPE_AND, list, 2);

  // SFilterInfo *filter = NULL;
  // int32_t code = filterInitFromNode(logicNode, &filter, FLT_OPTION_NO_REWRITE|FLT_OPTION_TIMESTAMP);
  // ASSERT_EQ(code, 0);
  STimeWindow win = {0};
  bool        isStrict = false;
  int32_t     code = filterGetTimeRange(logicNode, &win, &isStrict);
  ASSERT_EQ(isStrict, true);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(win.skey, tsmall + 1);
  ASSERT_EQ(win.ekey, tbig - 1);
  // filterFreeInfo(filter);
  nodesDestroyNode(logicNode);
}

TEST(timerangeTest, greater_equal_and_lower_equal) {
  SNode       *pcol = NULL, *pval = NULL, *opNode1 = NULL, *opNode2 = NULL, *logicNode = NULL;
  bool         eRes[5] = {false, false, true, true, true};
  SScalarParam res;
  initScalarParam(&res);
  int64_t tsmall = 222, tbig = 333;
  flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tsmall);
  flttMakeOpNode(&opNode1, OP_TYPE_GREATER_EQUAL, TSDB_DATA_TYPE_BOOL, pcol, pval);
  flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tbig);
  flttMakeOpNode(&opNode2, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pcol, pval);
  SNode *list[2] = {0};
  list[0] = opNode1;
  list[1] = opNode2;

  flttMakeLogicNode(&logicNode, LOGIC_COND_TYPE_AND, list, 2);

  // SFilterInfo *filter = NULL;
  // int32_t code = filterInitFromNode(logicNode, &filter, FLT_OPTION_NO_REWRITE|FLT_OPTION_TIMESTAMP);
  // ASSERT_EQ(code, 0);
  STimeWindow win = {0};
  bool        isStrict = false;
  int32_t     code = filterGetTimeRange(logicNode, &win, &isStrict);
  ASSERT_EQ(isStrict, true);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(win.skey, tsmall);
  ASSERT_EQ(win.ekey, tbig);
  // filterFreeInfo(filter);
  nodesDestroyNode(logicNode);
}

TEST(timerangeTest, greater_and_lower_not_strict) {
  SNode       *pcol = NULL, *pval = NULL, *opNode1 = NULL, *opNode2 = NULL, *logicNode1 = NULL, *logicNode2 = NULL;
  bool         eRes[5] = {false, false, true, true, true};
  SScalarParam res;
  initScalarParam(&res);
  int64_t tsmall1 = 222, tbig1 = 333;
  int64_t tsmall2 = 444, tbig2 = 555;
  SNode  *list[2] = {0};

  flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tsmall1);
  flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tbig1);
  flttMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  list[0] = opNode1;
  list[1] = opNode2;

  flttMakeLogicNode(&logicNode1, LOGIC_COND_TYPE_AND, list, 2);

  flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tsmall2);
  flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tbig2);
  flttMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  list[0] = opNode1;
  list[1] = opNode2;

  flttMakeLogicNode(&logicNode2, LOGIC_COND_TYPE_AND, list, 2);

  list[0] = logicNode1;
  list[1] = logicNode2;
  flttMakeLogicNode(&logicNode1, LOGIC_COND_TYPE_OR, list, 2);

  // SFilterInfo *filter = NULL;
  // int32_t code = filterInitFromNode(logicNode, &filter, FLT_OPTION_NO_REWRITE|FLT_OPTION_TIMESTAMP);
  // ASSERT_EQ(code, 0);
  STimeWindow win = {0};
  bool        isStrict = false;
  int32_t     code = filterGetTimeRange(logicNode1, &win, &isStrict);
  ASSERT_EQ(isStrict, false);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(win.skey, tsmall1 + 1);
  ASSERT_EQ(win.ekey, tbig2 - 1);
  // filterFreeInfo(filter);
  nodesDestroyNode(logicNode1);
}

#if 0
TEST(columnTest, smallint_column_greater_double_value) {
  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t      leftv[5] = {1, 2, 3, 4, 5};
  double       rightv = 2.5;
  int8_t       eRes[5] = {0, 0, 1, 1, 1};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  flttMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv);
  flttMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg stat = {0};
  stat.colId = ((SColumnNode *)pLeft)->colId;
  stat.max = 10;
  stat.min = 5;
  stat.numOfNull = 0;
  bool keep = filterRangeExecute(filter, &stat, 1, rowNum);
  ASSERT_EQ(keep, true);

  stat.max = 1;
  stat.min = -1;
  keep = filterRangeExecute(filter, &stat, 1, rowNum);
  ASSERT_EQ(keep, true);

  stat.max = 10;
  stat.min = 5;
  stat.numOfNull = rowNum;
  keep = filterRangeExecute(filter, &stat, 1, rowNum);
  ASSERT_EQ(keep, true);

  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  keep = filterExecute(filter, src, &rowRes, &stat, (int32_t)taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  blockDataDestroy(src);
  nodesDestroyNode(opNode);
}

TEST(columnTest, int_column_greater_smallint_value) {
  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int32_t      leftv[5] = {1, 3, 5, 7, 9};
  int16_t      rightv = 4;
  int8_t       eRes[5] = {0, 0, 1, 1, 1};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_INT, sizeof(int32_t), rowNum, leftv);
  flttMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv);
  flttMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg stat = {0};
  stat.colId = ((SColumnNode *)pLeft)->colId;
  stat.max = 10;
  stat.min = 5;
  stat.numOfNull = 0;
  bool keep = filterRangeExecute(filter, &stat, 1, rowNum);
  ASSERT_EQ(keep, true);

  stat.max = 1;
  stat.min = -1;
  keep = filterRangeExecute(filter, &stat, 1, rowNum);
  ASSERT_EQ(keep, false);

  stat.max = 10;
  stat.min = 5;
  stat.numOfNull = rowNum;
  keep = filterRangeExecute(filter, &stat, 1, rowNum);
  ASSERT_EQ(keep, false);

  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  keep = filterExecute(filter, src, &rowRes, &stat, (int32_t)taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(columnTest, int_column_in_double_list) {
  SNode       *pLeft = NULL, *pRight = NULL, *listNode = NULL, *opNode = NULL;
  int32_t      leftv[5] = {1, 2, 3, 4, 5};
  double       rightv1 = 1.1, rightv2 = 2.2, rightv3 = 3.3;
  bool         eRes[5] = {true, true, true, false, false};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_INT, sizeof(int32_t), rowNum, leftv);
  SNodeList *list = nodesMakeList();
  flttMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv1);
  nodesListAppend(list, pRight);
  flttMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv2);
  nodesListAppend(list, pRight);
  flttMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv3);
  nodesListAppend(list, pRight);
  flttMakeListNode(&listNode, list, TSDB_DATA_TYPE_INT);
  flttMakeOpNode(&opNode, OP_TYPE_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, (int32_t)taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }

  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(columnTest, binary_column_in_binary_list) {
  SNode       *pLeft = NULL, *pRight = NULL, *listNode = NULL, *opNode = NULL;
  bool         eRes[5] = {true, true, false, false, false};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  char leftv[5][5] = {0};
  char rightv[3][5] = {0};
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

  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BINARY, 3, rowNum, leftv);
  SNodeList *list = nodesMakeList();
  flttMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv[0]);
  nodesListAppend(list, pRight);
  flttMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv[1]);
  nodesListAppend(list, pRight);
  flttMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv[2]);
  nodesListAppend(list, pRight);
  flttMakeListNode(&listNode, list, TSDB_DATA_TYPE_BINARY);
  flttMakeOpNode(&opNode, OP_TYPE_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, (int32_t)taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(columnTest, binary_column_like_binary) {
  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  char         rightv[64] = {0};
  char         leftv[5][5] = {0};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  bool eRes[5] = {true, false, true, false, true};

  for (int32_t i = 0; i < 5; ++i) {
    leftv[i][2] = 'a';
    leftv[i][3] = 'a';
    leftv[i][4] = '0' + i % 2;
    varDataSetLen(leftv[i], 3);
  }

  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BINARY, 3, rowNum, leftv);

  sprintf(&rightv[2], "%s", "__0");
  varDataSetLen(rightv, strlen(&rightv[2]));
  flttMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  flttMakeOpNode(&opNode, OP_TYPE_LIKE, TSDB_DATA_TYPE_BOOL, pLeft, pRight);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, (int32_t)taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(columnTest, binary_column_is_null) {
  SNode       *pLeft = NULL, *opNode = NULL;
  char         leftv[5][5] = {0};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  bool eRes[5] = {false, false, true, false, true};

  for (int32_t i = 0; i < 5; ++i) {
    leftv[i][2] = '0' + i % 2;
    leftv[i][3] = 'a';
    leftv[i][4] = '0' + i % 2;
    varDataSetLen(leftv[i], 3);
  }

  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BINARY, 3, rowNum, leftv);

  SColumnInfoData *pcolumn = (SColumnInfoData *)taosArrayGetLast(src->pDataBlock);
  colDataSetVal(pcolumn, 2, NULL, true);
  colDataSetVal(pcolumn, 4, NULL, true);
  flttMakeOpNode(&opNode, OP_TYPE_IS_NULL, TSDB_DATA_TYPE_BOOL, pLeft, NULL);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, (int32_t)taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(columnTest, binary_column_is_not_null) {
  SNode       *pLeft = NULL, *opNode = NULL;
  char         leftv[5][5] = {0};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  bool eRes[5] = {true, true, true, true, false};

  for (int32_t i = 0; i < 5; ++i) {
    leftv[i][2] = '0' + i % 2;
    leftv[i][3] = 'a';
    leftv[i][4] = '0' + i % 2;
    varDataSetLen(leftv[i], 3);
  }

  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BINARY, 3, rowNum, leftv);

  SColumnInfoData *pcolumn = (SColumnInfoData *)taosArrayGetLast(src->pDataBlock);
  colDataSetVal(pcolumn, 4, NULL, true);

  flttMakeOpNode(&opNode, OP_TYPE_IS_NOT_NULL, TSDB_DATA_TYPE_BOOL, pLeft, NULL);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, (int32_t)taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(opTest, smallint_column_greater_int_column) {
  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t      leftv[5] = {1, -6, -2, 11, 101};
  int32_t      rightv[5] = {0, -5, -4, 23, 100};
  bool         eRes[5] = {true, false, true, false, true};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  int32_t rowNum = sizeof(rightv) / sizeof(rightv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  flttMakeColumnNode(&pRight, &src, TSDB_DATA_TYPE_INT, sizeof(int32_t), rowNum, rightv);
  flttMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, (int32_t)taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(opTest, smallint_value_add_int_column) {
  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int32_t      leftv = 1;
  int16_t      rightv[5] = {0, -1, -4, -1, 100};
  bool         eRes[5] = {true, false, true, false, true};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  int32_t rowNum = sizeof(rightv) / sizeof(rightv[0]);
  flttMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  flttMakeColumnNode(&pRight, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, rightv);
  flttMakeOpNode(&opNode, OP_TYPE_ADD, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  flttMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, (int32_t)taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(opTest, bigint_column_multi_binary_column) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int64_t leftv[5] = {1, 2, 3, 4, 5};
  char    rightv[5][5] = {0};
  for (int32_t i = 0; i < 5; ++i) {
    rightv[i][2] = rightv[i][3] = '0';
    rightv[i][4] = '0' + i;
    varDataSetLen(rightv[i], 3);
  }
  bool         eRes[5] = {false, true, true, true, true};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  int32_t rowNum = sizeof(rightv) / sizeof(rightv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), rowNum, leftv);
  flttMakeColumnNode(&pRight, &src, TSDB_DATA_TYPE_BINARY, 5, rowNum, rightv);
  flttMakeOpNode(&opNode, OP_TYPE_MULTI, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  flttMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, (int32_t)taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(opTest, smallint_column_and_binary_column) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t leftv[5] = {1, 2, 3, 4, 5};
  char    rightv[5][5] = {0};
  for (int32_t i = 0; i < 5; ++i) {
    rightv[i][2] = rightv[i][3] = '0';
    rightv[i][4] = '0' + i;
    varDataSetLen(rightv[i], 3);
  }
  bool         eRes[5] = {false, false, true, false, true};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  int32_t rowNum = sizeof(rightv) / sizeof(rightv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  flttMakeColumnNode(&pRight, &src, TSDB_DATA_TYPE_BINARY, 5, rowNum, rightv);
  flttMakeOpNode(&opNode, OP_TYPE_BIT_AND, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  flttMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, (int32_t)taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(opTest, smallint_column_or_float_column) {
  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t      leftv[5] = {1, 2, 0, 4, 5};
  float        rightv[5] = {2.0, 3.0, 0, 5.2, 6.0};
  bool         eRes[5] = {true, true, false, true, true};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  int32_t rowNum = sizeof(rightv) / sizeof(rightv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  flttMakeColumnNode(&pRight, &src, TSDB_DATA_TYPE_FLOAT, sizeof(float), rowNum, rightv);
  flttMakeOpNode(&opNode, OP_TYPE_BIT_OR, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  flttMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(opTest, smallint_column_or_double_value) {
  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t      leftv[5] = {0, 2, 3, 0, -1};
  double       rightv = 10.2;
  bool         eRes[5] = {true, true, true, true, true};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  flttMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv);
  flttMakeOpNode(&opNode, OP_TYPE_BIT_OR, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  flttMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, true);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(opTest, binary_column_is_true) {
  SNode       *pLeft = NULL, *opNode = NULL;
  char         leftv[5][5] = {0};
  SSDataBlock *src = NULL;
  SScalarParam res;
  initScalarParam(&res);
  bool eRes[5] = {false, true, false, true, false};

  for (int32_t i = 0; i < 5; ++i) {
    leftv[i][2] = '0' + i % 2;
    leftv[i][3] = 'a';
    leftv[i][4] = '0' + i % 2;
    varDataSetLen(leftv[i], 3);
  }

  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  flttMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BINARY, 3, rowNum, leftv);

  flttMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, pLeft, NULL);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(opNode, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(opNode);
  blockDataDestroy(src);
}

TEST(filterModelogicTest, diff_columns_and_or_and) {
  flttInitLogFile();

  SNode       *pLeft1 = NULL, *pRight1 = NULL, *pLeft2 = NULL, *pRight2 = NULL, *opNode1 = NULL, *opNode2 = NULL;
  SNode       *logicNode1 = NULL, *logicNode2 = NULL;
  double       leftv1[8] = {1, 2, 3, 4, 5, -1, -2, -3}, leftv2[8] = {3.0, 4, 2, 9, -3, 3.9, 4.1, 5.2};
  int32_t      rightv1 = 3, rightv2 = 3;
  int8_t       eRes[8] = {1, 1, 0, 0, 1, 1, 1, 1};
  SSDataBlock *src = NULL;

  SNodeList *list = nodesMakeList();

  int32_t rowNum = sizeof(leftv1) / sizeof(leftv1[0]);
  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight1, TSDB_DATA_TYPE_INT, &rightv1);
  flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft1, pRight1);
  nodesListAppend(list, opNode1);

  flttMakeColumnNode(&pLeft2, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv2);
  flttMakeValueNode(&pRight2, TSDB_DATA_TYPE_INT, &rightv2);
  flttMakeOpNode(&opNode2, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft2, pRight2);
  nodesListAppend(list, opNode2);

  flttMakeLogicNodeFromList(&logicNode1, LOGIC_COND_TYPE_AND, list);

  list = nodesMakeList();

  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight1, TSDB_DATA_TYPE_INT, &rightv1);
  flttMakeOpNode(&opNode1, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft1, pRight1);
  nodesListAppend(list, opNode1);

  flttMakeColumnNode(&pLeft2, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv2);
  flttMakeValueNode(&pRight2, TSDB_DATA_TYPE_INT, &rightv2);
  flttMakeOpNode(&opNode2, OP_TYPE_GREATER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft2, pRight2);
  nodesListAppend(list, opNode2);

  flttMakeLogicNodeFromList(&logicNode2, LOGIC_COND_TYPE_AND, list);

  list = nodesMakeList();
  nodesListAppend(list, logicNode1);
  nodesListAppend(list, logicNode2);
  flttMakeLogicNodeFromList(&logicNode1, LOGIC_COND_TYPE_OR, list);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(logicNode1, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(logicNode1);
  blockDataDestroy(src);
}

TEST(filterModelogicTest, same_column_and_or_and) {
  SNode       *pLeft1 = NULL, *pRight1 = NULL, *pLeft2 = NULL, *pRight2 = NULL, *opNode1 = NULL, *opNode2 = NULL;
  SNode       *logicNode1 = NULL, *logicNode2 = NULL;
  double       leftv1[8] = {1, 2, 3, 4, 5, -1, -2, -3};
  int32_t      rightv1 = 3, rightv2 = 0, rightv3 = 2, rightv4 = -2;
  int8_t       eRes[8] = {1, 1, 0, 0, 0, 1, 1, 0};
  SSDataBlock *src = NULL;

  SNodeList *list = nodesMakeList();

  int32_t rowNum = sizeof(leftv1) / sizeof(leftv1[0]);
  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight1, TSDB_DATA_TYPE_INT, &rightv1);
  flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft1, pRight1);
  nodesListAppend(list, opNode1);

  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight2, TSDB_DATA_TYPE_INT, &rightv2);
  flttMakeOpNode(&opNode2, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft1, pRight2);
  nodesListAppend(list, opNode2);

  flttMakeLogicNodeFromList(&logicNode1, LOGIC_COND_TYPE_AND, list);

  list = nodesMakeList();

  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight1, TSDB_DATA_TYPE_INT, &rightv3);
  flttMakeOpNode(&opNode1, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft1, pRight1);
  nodesListAppend(list, opNode1);

  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight2, TSDB_DATA_TYPE_INT, &rightv4);
  flttMakeOpNode(&opNode2, OP_TYPE_GREATER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft1, pRight2);
  nodesListAppend(list, opNode2);

  flttMakeLogicNodeFromList(&logicNode2, LOGIC_COND_TYPE_AND, list);

  list = nodesMakeList();
  nodesListAppend(list, logicNode1);
  nodesListAppend(list, logicNode2);
  flttMakeLogicNodeFromList(&logicNode1, LOGIC_COND_TYPE_OR, list);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(logicNode1, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(logicNode1);
  blockDataDestroy(src);
}

TEST(filterModelogicTest, diff_columns_or_and_or) {
  SNode       *pLeft1 = NULL, *pRight1 = NULL, *pLeft2 = NULL, *pRight2 = NULL, *opNode1 = NULL, *opNode2 = NULL;
  SNode       *logicNode1 = NULL, *logicNode2 = NULL;
  double       leftv1[8] = {1, 2, 3, 4, 5, -1, -2, -3}, leftv2[8] = {3.0, 4, 2, 9, -3, 3.9, 4.1, 5.2};
  int32_t      rightv1 = 3, rightv2 = 3;
  int8_t       eRes[8] = {1, 0, 1, 1, 0, 0, 0, 0};
  SSDataBlock *src = NULL;

  SNodeList *list = nodesMakeList();

  int32_t rowNum = sizeof(leftv1) / sizeof(leftv1[0]);
  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight1, TSDB_DATA_TYPE_INT, &rightv1);
  flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft1, pRight1);
  nodesListAppend(list, opNode1);

  flttMakeColumnNode(&pLeft2, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv2);
  flttMakeValueNode(&pRight2, TSDB_DATA_TYPE_INT, &rightv2);
  flttMakeOpNode(&opNode2, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft2, pRight2);
  nodesListAppend(list, opNode2);

  flttMakeLogicNodeFromList(&logicNode1, LOGIC_COND_TYPE_OR, list);

  list = nodesMakeList();

  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight1, TSDB_DATA_TYPE_INT, &rightv1);
  flttMakeOpNode(&opNode1, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft1, pRight1);
  nodesListAppend(list, opNode1);

  flttMakeColumnNode(&pLeft2, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv2);
  flttMakeValueNode(&pRight2, TSDB_DATA_TYPE_INT, &rightv2);
  flttMakeOpNode(&opNode2, OP_TYPE_GREATER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft2, pRight2);
  nodesListAppend(list, opNode2);

  flttMakeLogicNodeFromList(&logicNode2, LOGIC_COND_TYPE_OR, list);

  list = nodesMakeList();
  nodesListAppend(list, logicNode1);
  nodesListAppend(list, logicNode2);
  flttMakeLogicNodeFromList(&logicNode1, LOGIC_COND_TYPE_AND, list);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(logicNode1, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(logicNode1);
  blockDataDestroy(src);
}

TEST(filterModelogicTest, same_column_or_and_or) {
  SNode       *pLeft1 = NULL, *pRight1 = NULL, *pLeft2 = NULL, *pRight2 = NULL, *opNode1 = NULL, *opNode2 = NULL;
  SNode       *logicNode1 = NULL, *logicNode2 = NULL;
  double       leftv1[8] = {1, 2, 3, 4, 5, -1, -2, -3};
  int32_t      rightv1 = 3, rightv2 = 0, rightv3 = 2, rightv4 = -2;
  int8_t       eRes[8] = {0, 0, 0, 1, 1, 1, 1, 1};
  SSDataBlock *src = NULL;

  SNodeList *list = nodesMakeList();

  int32_t rowNum = sizeof(leftv1) / sizeof(leftv1[0]);
  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight1, TSDB_DATA_TYPE_INT, &rightv1);
  flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft1, pRight1);
  nodesListAppend(list, opNode1);

  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight2, TSDB_DATA_TYPE_INT, &rightv2);
  flttMakeOpNode(&opNode2, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft1, pRight2);
  nodesListAppend(list, opNode2);

  flttMakeLogicNodeFromList(&logicNode1, LOGIC_COND_TYPE_OR, list);

  list = nodesMakeList();

  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight1, TSDB_DATA_TYPE_INT, &rightv3);
  flttMakeOpNode(&opNode1, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft1, pRight1);
  nodesListAppend(list, opNode1);

  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeValueNode(&pRight2, TSDB_DATA_TYPE_INT, &rightv4);
  flttMakeOpNode(&opNode2, OP_TYPE_GREATER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft1, pRight2);
  nodesListAppend(list, opNode2);

  flttMakeLogicNodeFromList(&logicNode2, LOGIC_COND_TYPE_OR, list);

  list = nodesMakeList();
  nodesListAppend(list, logicNode1);
  nodesListAppend(list, logicNode2);
  flttMakeLogicNodeFromList(&logicNode1, LOGIC_COND_TYPE_AND, list);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(logicNode1, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(logicNode1);
  blockDataDestroy(src);
}

TEST(scalarModelogicTest, diff_columns_or_and_or) {
  flttInitLogFile();

  SNode       *pLeft1 = NULL, *pRight1 = NULL, *pLeft2 = NULL, *pRight2 = NULL, *opNode1 = NULL, *opNode2 = NULL;
  SNode       *logicNode1 = NULL, *logicNode2 = NULL;
  double       leftv1[8] = {1, 2, 3, 4, 5, -1, -2, -3}, leftv2[8] = {3.0, 4, 2, 9, -3, 3.9, 4.1, 5.2};
  int32_t      rightv1[8] = {5, 8, 2, -3, 9, -7, 10, 0}, rightv2[8] = {-3, 5, 8, 2, -9, 11, -4, 0};
  int8_t       eRes[8] = {0, 1, 1, 0, 0, 1, 0, 0};
  SSDataBlock *src = NULL;

  SNodeList *list = nodesMakeList();

  int32_t rowNum = sizeof(leftv1) / sizeof(leftv1[0]);
  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeColumnNode(&pRight1, &src, TSDB_DATA_TYPE_INT, sizeof(int32_t), rowNum, rightv1);
  flttMakeOpNode(&opNode1, OP_TYPE_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft1, pRight1);
  nodesListAppend(list, opNode1);

  flttMakeColumnNode(&pLeft2, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv2);
  flttMakeColumnNode(&pRight2, &src, TSDB_DATA_TYPE_INT, sizeof(int32_t), rowNum, rightv2);
  flttMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pLeft2, pRight2);
  nodesListAppend(list, opNode2);

  flttMakeLogicNodeFromList(&logicNode1, LOGIC_COND_TYPE_OR, list);

  list = nodesMakeList();

  flttMakeColumnNode(&pLeft1, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv1);
  flttMakeColumnNode(&pRight1, &src, TSDB_DATA_TYPE_INT, sizeof(int32_t), rowNum, rightv1);
  flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft1, pRight1);
  nodesListAppend(list, opNode1);

  flttMakeColumnNode(&pLeft2, &src, TSDB_DATA_TYPE_DOUBLE, sizeof(double), rowNum, leftv2);
  flttMakeColumnNode(&pRight2, &src, TSDB_DATA_TYPE_INT, sizeof(int32_t), rowNum, rightv2);
  flttMakeOpNode(&opNode2, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft2, pRight2);
  nodesListAppend(list, opNode2);

  flttMakeLogicNodeFromList(&logicNode2, LOGIC_COND_TYPE_OR, list);

  list = nodesMakeList();
  nodesListAppend(list, logicNode1);
  nodesListAppend(list, logicNode2);
  flttMakeLogicNodeFromList(&logicNode1, LOGIC_COND_TYPE_AND, list);

  SFilterInfo *filter = NULL;
  int32_t      code = filterInitFromNode(logicNode1, &filter, 0);
  ASSERT_EQ(code, 0);

  SColumnDataAgg     stat = {0};
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(src->pDataBlock), src->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param);
  ASSERT_EQ(code, 0);

  stat.max = 5;
  stat.min = 1;
  stat.numOfNull = 0;
  int8_t *rowRes = NULL;
  bool    keep = filterExecute(filter, src, &rowRes, &stat, taosArrayGetSize(src->pDataBlock));
  ASSERT_EQ(keep, false);

  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)rowRes + i), eRes[i]);
  }
  taosMemoryFreeClear(rowRes);
  filterFreeInfo(filter);
  nodesDestroyNode(logicNode1);
  blockDataDestroy(src);
}
#endif

template <class SignedT, class UnsignedT>
int32_t compareSignedWithUnsigned(SignedT l, UnsignedT r) {
  if (l < 0) return -1;
  auto l_uint64 = static_cast<uint64_t>(l);
  auto r_uint64 = static_cast<uint64_t>(r);
  if (l_uint64 < r_uint64) return -1;
  if (l_uint64 > r_uint64) return 1;
  return 0;
}

template <class UnsignedT, class SignedT>
int32_t compareUnsignedWithSigned(UnsignedT l, SignedT r) {
  if (r < 0) return 1;
  auto l_uint64 = static_cast<uint64_t>(l);
  auto r_uint64 = static_cast<uint64_t>(r);
  if (l_uint64 < r_uint64) return -1;
  if (l_uint64 > r_uint64) return 1;
  return 0;
}

template <class SignedT, class UnsignedT>
void doCompareWithValueRange_SignedWithUnsigned(__compar_fn_t fp) {
  int32_t signedMin = -10, signedMax = 10;
  int32_t unsignedMin = 0, unsignedMax = 10;
  for (SignedT l = signedMin; l <= signedMax; ++l) {
    for (UnsignedT r = unsignedMin; r <= unsignedMax; ++r) {
      ASSERT_EQ(fp(&l, &r), compareSignedWithUnsigned(l, r));
    }
  }
}

template <class UnsignedT, class SignedT>
void doCompareWithValueRange_UnsignedWithSigned(__compar_fn_t fp) {
  int32_t signedMin = -10, signedMax = 10;
  int32_t unsignedMin = 0, unsignedMax = 10;
  for (UnsignedT l = unsignedMin; l <= unsignedMax; ++l) {
    for (SignedT r = signedMin; r <= signedMax; ++r) {
      ASSERT_EQ(fp(&l, &r), compareUnsignedWithSigned(l, r));
    }
  }
}

template <class LType>
void doCompareWithValueRange_OnlyLeftType(__compar_fn_t fp, int32_t rType) {
  switch (rType) {
    case TSDB_DATA_TYPE_UTINYINT:
      doCompareWithValueRange_SignedWithUnsigned<LType, uint8_t>(fp);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      doCompareWithValueRange_SignedWithUnsigned<LType, uint16_t>(fp);
      break;
    case TSDB_DATA_TYPE_UINT:
      doCompareWithValueRange_SignedWithUnsigned<LType, uint32_t>(fp);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      doCompareWithValueRange_SignedWithUnsigned<LType, uint64_t>(fp);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      doCompareWithValueRange_UnsignedWithSigned<LType, int8_t>(fp);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      doCompareWithValueRange_UnsignedWithSigned<LType, int16_t>(fp);
      break;
    case TSDB_DATA_TYPE_INT:
      doCompareWithValueRange_UnsignedWithSigned<LType, int32_t>(fp);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      doCompareWithValueRange_UnsignedWithSigned<LType, int64_t>(fp);
      break;
    default:
      FAIL();
  }
}

int main(int argc, char **argv) {
  taosSeedRand(taosGetTimestampSec());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop
