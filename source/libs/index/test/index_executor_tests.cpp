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
#include <tsort.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "index.h"
#include "stub.h"
#include "taos.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tdef.h"
#include "trpc.h"
#include "tvariant.h"

namespace {
SColumnInfo createColumnInfo(int32_t colId, int32_t type, int32_t bytes) {
  SColumnInfo info = {0};
  info.colId = colId;
  info.type = type;
  info.bytes = bytes;
  return info;
}

int64_t sifLeftV = 21, sifRightV = 10;
double  sifLeftVd = 21.0, sifRightVd = 10.0;

void sifFreeDataBlock(void *block) { blockDataDestroy(*(SSDataBlock **)block); }

void sifInitLogFile() {
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  strcpy(tsLogDir, TD_TMP_DIR_PATH "sif");
  taosRemoveDir(tsLogDir);
  taosMkDir(tsLogDir);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

void sifAppendReservedSlot(SArray *pBlockList, int16_t *dataBlockId, int16_t *slotId, bool newBlock, int32_t rows,
                           SColumnInfo *colInfo) {
  if (newBlock) {
    SSDataBlock *res = (SSDataBlock *)taosMemoryCalloc(1, sizeof(SSDataBlock));
    res->info.numOfCols = 1;
    res->info.rows = rows;
    res->pDataBlock = taosArrayInit(1, sizeof(SColumnInfoData));
    SColumnInfoData idata = {0};
    idata.info = *colInfo;

    taosArrayPush(res->pDataBlock, &idata);
    taosArrayPush(pBlockList, &res);

    blockDataEnsureCapacity(res, rows);

    *dataBlockId = taosArrayGetSize(pBlockList) - 1;
    res->info.id.blockId = *dataBlockId;
    *slotId = 0;
  } else {
    SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(pBlockList);
    res->info.numOfCols++;
    SColumnInfoData idata = {0};
    idata.info = *colInfo;

    colInfoDataEnsureCapacity(&idata, rows, true);
    taosArrayPush(res->pDataBlock, &idata);

    *dataBlockId = taosArrayGetSize(pBlockList) - 1;
    *slotId = taosArrayGetSize(res->pDataBlock) - 1;
  }
}

void sifMakeValueNode(SNode **pNode, int32_t dataType, void *value) {
  SNode      *node = (SNode *)nodesMakeNode(QUERY_NODE_VALUE);
  SValueNode *vnode = (SValueNode *)node;
  vnode->node.resType.type = dataType;

  if (IS_VAR_DATA_TYPE(dataType)) {
    vnode->datum.p = (char *)taosMemoryMalloc(varDataTLen(value));
    varDataCopy(vnode->datum.p, value);
    vnode->node.resType.bytes = varDataTLen(value);
  } else {
    vnode->node.resType.bytes = tDataTypes[dataType].bytes;
    assignVal((char *)nodesGetValueFromNode(vnode), (const char *)value, 0, dataType);
  }

  *pNode = (SNode *)vnode;
}

void sifMakeColumnNode(SNode **pNode, const char *db, const char *colName, EColumnType colType, uint8_t colValType) {
  SNode       *node = (SNode *)nodesMakeNode(QUERY_NODE_COLUMN);
  SColumnNode *rnode = (SColumnNode *)node;
  memcpy(rnode->dbName, db, strlen(db));
  memcpy(rnode->colName, colName, strlen(colName));

  rnode->colType = colType;
  rnode->node.resType.type = colValType;

  *pNode = (SNode *)rnode;
}

void sifMakeOpNode(SNode **pNode, EOperatorType opType, int32_t resType, SNode *pLeft, SNode *pRight) {
  SNode         *node = (SNode *)nodesMakeNode(QUERY_NODE_OPERATOR);
  SOperatorNode *onode = (SOperatorNode *)node;
  onode->node.resType.type = resType;
  onode->node.resType.bytes = tDataTypes[resType].bytes;

  onode->opType = opType;
  onode->pLeft = pLeft;
  onode->pRight = pRight;

  *pNode = (SNode *)onode;
}

void sifMakeListNode(SNode **pNode, SNodeList *list, int32_t resType) {
  SNode         *node = (SNode *)nodesMakeNode(QUERY_NODE_NODE_LIST);
  SNodeListNode *lnode = (SNodeListNode *)node;
  lnode->node.resType.type = resType;
  lnode->pNodeList = list;

  *pNode = (SNode *)lnode;
}

void sifMakeLogicNode(SNode **pNode, ELogicConditionType opType, SNode **nodeList, int32_t nodeNum) {
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

void sifMakeTargetNode(SNode **pNode, int16_t dataBlockId, int16_t slotId, SNode *snode) {
  SNode       *node = (SNode *)nodesMakeNode(QUERY_NODE_TARGET);
  STargetNode *onode = (STargetNode *)node;
  onode->pExpr = snode;
  onode->dataBlockId = dataBlockId;
  onode->slotId = slotId;

  *pNode = (SNode *)onode;
}

}  // namespace

#if 1
TEST(testCase, index_filter) {
  {
    SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
    sifMakeColumnNode(&pLeft, "test", "col", COLUMN_TYPE_TAG, TSDB_DATA_TYPE_INT);
    sifMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &sifRightV);
    sifMakeOpNode(&opNode, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_INT, pLeft, pRight);
    SArray *result = taosArrayInit(4, sizeof(uint64_t));
    doFilterTag(opNode, result);
    EXPECT_EQ(1, taosArrayGetSize(result));
    taosArrayDestroy(result);
    nodesDestroyNode(res);
  }
  {
    SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
    sifMakeColumnNode(&pLeft, "test", "col", COLUMN_TYPE_TAG, TSDB_DATA_TYPE_INT);
    sifMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &sifRightV);
    sifMakeOpNode(&opNode, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);

    SArray *result = taosArrayInit(4, sizeof(uint64_t));
    doFilterTag(opNode, result);
    EXPECT_EQ(1, taosArrayGetSize(result));

    taosArrayDestroy(result);
    nodesDestroyNode(res);
  }
  {
    SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
    sifMakeColumnNode(&pLeft, "test", "col", COLUMN_TYPE_TAG, TSDB_DATA_TYPE_INT);
    sifMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &sifRightV);
    sifMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_INT, pLeft, pRight);
    SArray *result = taosArrayInit(4, sizeof(uint64_t));
    doFilterTag(opNode, result);
    EXPECT_EQ(0, taosArrayGetSize(result));
    taosArrayDestroy(result);
    nodesDestroyNode(res);
  }
  {
    SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
    sifMakeColumnNode(&pLeft, "test", "col", COLUMN_TYPE_TAG, TSDB_DATA_TYPE_INT);
    sifMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &sifRightV);
    sifMakeOpNode(&opNode, OP_TYPE_GREATER_EQUAL, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);

    SArray *result = taosArrayInit(4, sizeof(uint64_t));
    doFilterTag(opNode, result);
    EXPECT_EQ(0, taosArrayGetSize(result));

    taosArrayDestroy(result);
    nodesDestroyNode(res);
  }
}

// add other greater/lower/equal/in compare func test

TEST(testCase, index_filter_varify) {
  {
    SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
    sifMakeColumnNode(&pLeft, "test", "col", COLUMN_TYPE_TAG, TSDB_DATA_TYPE_INT);
    sifMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &sifRightV);
    sifMakeOpNode(&opNode, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_INT, pLeft, pRight);
    nodesDestroyNode(res);

    SIdxFltStatus st = idxGetFltStatus(opNode);
    EXPECT_EQ(st, SFLT_ACCURATE_INDEX);
  }
  {
    SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
    sifMakeColumnNode(&pLeft, "test", "col", COLUMN_TYPE_TAG, TSDB_DATA_TYPE_INT);
    sifMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &sifRightV);
    sifMakeOpNode(&opNode, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);

    SIdxFltStatus st = idxGetFltStatus(opNode);
    EXPECT_EQ(st, SFLT_ACCURATE_INDEX);
    nodesDestroyNode(res);
  }
  {
    SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
    sifMakeColumnNode(&pLeft, "test", "col", COLUMN_TYPE_TAG, TSDB_DATA_TYPE_INT);
    sifMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &sifRightV);
    sifMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_INT, pLeft, pRight);
    nodesDestroyNode(res);

    SIdxFltStatus st = idxGetFltStatus(opNode);
    EXPECT_EQ(st, SFLT_ACCURATE_INDEX);
  }
  {
    SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
    sifMakeColumnNode(&pLeft, "test", "col", COLUMN_TYPE_TAG, TSDB_DATA_TYPE_INT);
    sifMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &sifRightV);
    sifMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);

    SIdxFltStatus st = idxGetFltStatus(opNode);
    EXPECT_EQ(st, SFLT_ACCURATE_INDEX);
    nodesDestroyNode(res);
  }
}

#endif

#pragma GCC diagnostic pop
