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

#include "filterInt.h"
#include "nodes.h"
#include "parUtil.h"
#include "scalar.h"
#include "sclInt.h"
#include "stub.h"
#include "taos.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "tlog.h"
#include "tvariant.h"
#include "tcompare.h"

#define _DEBUG_PRINT_ 0

#if _DEBUG_PRINT_
#define PRINTF(...) (void)printf(__VA_ARGS__)
#else
#define PRINTF(...)
#endif

class constantTest {
 public:
  constantTest() { (void)InitRegexCache(); }
  ~constantTest() { (void)DestroyRegexCache(); }
};
static constantTest test;
namespace {

SColumnInfo createColumnInfo(int32_t colId, int32_t type, int32_t bytes) {
  SColumnInfo info = {0};
  info.colId = colId;
  info.type = type;
  info.bytes = bytes;
  return info;
}

int64_t scltLeftV = 21, scltRightV = 10;
double  scltLeftVd = 21.0, scltRightVd = 10.0;

void scltFreeDataBlock(void *block) { (void)blockDataDestroy(*(SSDataBlock **)block); }

void scltInitLogFile() {
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  tstrncpy(tsLogDir, TD_LOG_DIR_PATH, PATH_MAX);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum, false) < 0) {
    (void)printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

int32_t scltAppendReservedSlot(SArray *pBlockList, int16_t *dataBlockId, int16_t *slotId, bool newBlock, int32_t rows,
                               SColumnInfo *colInfo) {
  if (newBlock) {
    SSDataBlock *res = NULL;
    int32_t      code = createDataBlock(&res);
    if (code != 0 || NULL == res->pDataBlock) {
      SCL_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    SColumnInfoData idata = {0};
    idata.info = *colInfo;

    code = colInfoDataEnsureCapacity(&idata, rows, true);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFreeClear(&idata);
      SCL_ERR_RET(code);
    }

    code = blockDataAppendColInfo(res, &idata);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataFreeRes(res);
      SCL_ERR_RET(code);
    }

    res->info.capacity = rows;
    res->info.rows = rows;
    SColumnInfoData *p = static_cast<SColumnInfoData *>(taosArrayGet(res->pDataBlock, 0));
    if (p->pData == NULL || p->nullbitmap == NULL) {
      sclError("data block is not initialized since pData or nullbitmap is NULL");
      SCL_ERR_RET(TSDB_CODE_APP_ERROR);
    }

    if (NULL == taosArrayPush(pBlockList, &res)) {
      SCL_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    *dataBlockId = taosArrayGetSize(pBlockList) - 1;
    res->info.id.blockId = *dataBlockId;
    *slotId = 0;
  } else {
    SSDataBlock    *res = *(SSDataBlock **)taosArrayGetLast(pBlockList);
    SColumnInfoData idata = {0};
    idata.info = *colInfo;
    int32_t code = colInfoDataEnsureCapacity(&idata, rows, true);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFreeClear(&idata);
      SCL_ERR_RET(code);
    }

    code = blockDataAppendColInfo(res, &idata);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataFreeRes(res);
      SCL_ERR_RET(code);
    }

    *dataBlockId = taosArrayGetSize(pBlockList) - 1;
    *slotId = taosArrayGetSize(res->pDataBlock) - 1;
  }
  SCL_RET(TSDB_CODE_SUCCESS);

}

int32_t scltMakeValueNode(SNode **pNode, int32_t dataType, void *value) {
  SNode  *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, &node);
  if (NULL == node) {
    SCL_ERR_RET(code);
  }
  SValueNode *vnode = (SValueNode *)node;
  vnode->node.resType.type = dataType;

  if (IS_VAR_DATA_TYPE(dataType)) {
    vnode->datum.p = (char *)taosMemoryMalloc(varDataTLen(value));
    if (NULL == vnode->datum.p) {
      SCL_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    varDataCopy(vnode->datum.p, value);
    vnode->node.resType.bytes = varDataTLen(value);
  } else {
    vnode->node.resType.bytes = tDataTypes[dataType].bytes;
    (void)assignVal((char *)nodesGetValueFromNode(vnode), (const char *)value, 0, dataType);
  }

  *pNode = (SNode *)vnode;
  SCL_RET(TSDB_CODE_SUCCESS);
}

int32_t scltMakeColumnNode(SNode **pNode, SSDataBlock **block, int32_t dataType, int32_t dataBytes, int32_t rowNum,
                           void *value) {
  SNode       *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, &node);
  if (NULL == node) {
    SCL_ERR_RET(code);
  }
  SColumnNode *rnode = (SColumnNode *)node;
  rnode->node.resType.type = dataType;
  rnode->node.resType.bytes = dataBytes;
  rnode->dataBlockId = 0;

  if (NULL == block) {
    *pNode = (SNode *)rnode;
    SCL_RET(TSDB_CODE_SUCCESS);
  }

  if (NULL == *block) {
    SSDataBlock *res = NULL;

    SCL_ERR_RET(createDataBlock(&res));

    for (int32_t i = 0; i < 2; ++i) {
      SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_INT, 10, i + 1);
      code = colInfoDataEnsureCapacity(&idata, rowNum, true);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFreeClear(&idata);
        SCL_ERR_RET(code);
      }
      code = blockDataAppendColInfo(res, &idata);
      if (code != TSDB_CODE_SUCCESS) {
        blockDataFreeRes(res);
        SCL_ERR_RET(code);
      }
    }

    SColumnInfoData idata = createColumnInfoData(dataType, dataBytes, 3);
    code = colInfoDataEnsureCapacity(&idata, rowNum, true);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFreeClear(&idata);
      SCL_ERR_RET(code);
    }
    code = blockDataAppendColInfo(res, &idata);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataFreeRes(res);
      SCL_ERR_RET(code);
    }
    res->info.capacity = rowNum;

    res->info.rows = rowNum;
    SColumnInfoData *pColumn = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
    for (int32_t i = 0; i < rowNum; ++i) {
      SCL_ERR_RET(colDataSetVal(pColumn, i, (const char *)value, false));
      if (IS_VAR_DATA_TYPE(dataType)) {
        value = (char *)value + varDataTLen(value);
      } else {
        value = (char *)value + dataBytes;
      }
    }

    rnode->slotId = 2;
    rnode->colId = 3;

    *block = res;
  } else {
    SSDataBlock *res = *block;

    int32_t         idx = taosArrayGetSize(res->pDataBlock);
    SColumnInfoData idata = createColumnInfoData(dataType, dataBytes, 1 + idx);
    code = colInfoDataEnsureCapacity(&idata, rowNum, true);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFreeClear(&idata);
      SCL_ERR_RET(code);
    }

    res->info.capacity = rowNum;
    code = blockDataAppendColInfo(res, &idata);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataFreeRes(res);
      SCL_ERR_RET(code);
    }

    SColumnInfoData *pColumn = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);

    for (int32_t i = 0; i < rowNum; ++i) {
      SCL_ERR_RET(colDataSetVal(pColumn, i, (const char *)value, false));
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
  SCL_RET(code);
}

int32_t scltMakeOpNode2(SNode **pNode, EOperatorType opType, int32_t resType, SNode *pLeft, SNode *pRight,
                     bool isReverse) {
  SNode  *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, &node);
  if (NULL == node) {
    SCL_ERR_RET(code);
  }
  SOperatorNode *onode = (SOperatorNode *)node;
  onode->node.resType.type = resType;
  onode->node.resType.bytes = tDataTypes[resType].bytes;

  onode->opType = opType;
  if (isReverse) {
    onode->pLeft = pRight;
    onode->pRight = pLeft;
  } else {
    onode->pLeft = pLeft;
    onode->pRight = pRight;
  }

  *pNode = (SNode *)onode;
  SCL_RET(TSDB_CODE_SUCCESS);
}

int32_t scltMakeOpNode(SNode **pNode, EOperatorType opType, int32_t resType, SNode *pLeft, SNode *pRight) {
  SNode         *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, &node);
  if (NULL == node) {
    SCL_ERR_RET(code);
  }
  SOperatorNode *onode = (SOperatorNode *)node;
  onode->node.resType.type = resType;
  onode->node.resType.bytes = tDataTypes[resType].bytes;

  onode->opType = opType;
  onode->pLeft = pLeft;
  onode->pRight = pRight;

  *pNode = (SNode *)onode;
  SCL_RET(TSDB_CODE_SUCCESS);
}

int32_t scltMakeListNode(SNode **pNode, SNodeList *list, int32_t resType) {
  SNode         *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_NODE_LIST, &node);
  if (NULL == node) {
    SCL_ERR_RET(code);
  }
  SNodeListNode *lnode = (SNodeListNode *)node;
  lnode->node.resType.type = resType;
  lnode->pNodeList = list;

  *pNode = (SNode *)lnode;
  SCL_RET(TSDB_CODE_SUCCESS);
}

int32_t scltMakeLogicNode(SNode **pNode, ELogicConditionType opType, SNode **nodeList, int32_t nodeNum) {
  SNode               *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, &node);
  if (NULL == node) {
    SCL_ERR_RET(code);
  }
  SLogicConditionNode *onode = (SLogicConditionNode *)node;
  onode->condType = opType;
  onode->node.resType.type = TSDB_DATA_TYPE_BOOL;
  onode->node.resType.bytes = sizeof(bool);

  onode->pParameterList = NULL;
  code = nodesMakeList(&onode->pParameterList);
  if (TSDB_CODE_SUCCESS != code) {
    SCL_ERR_RET(code);
  }
  for (int32_t i = 0; i < nodeNum; ++i) {
    SCL_ERR_RET(nodesListAppend(onode->pParameterList, nodeList[i]));
  }

  *pNode = (SNode *)onode;
  SCL_RET(TSDB_CODE_SUCCESS);
}

int32_t scltMakeTargetNode(SNode **pNode, int16_t dataBlockId, int16_t slotId, SNode *snode) {
  SNode       *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_TARGET, &node);
  if (NULL == node) {
    SCL_ERR_RET(code);
  }
  STargetNode *onode = (STargetNode *)node;
  onode->pExpr = snode;
  onode->dataBlockId = dataBlockId;
  onode->slotId = slotId;

  *pNode = (SNode *)onode;
  SCL_RET(TSDB_CODE_SUCCESS);
}
}  // namespace

TEST(constantTest, bigint_add_bigint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BIGINT, &scltLeftV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BIGINT, &scltRightV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_ADD, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_FLOAT_EQ(v->datum.d, (scltLeftV + scltRightV));
  nodesDestroyNode(res);
}

TEST(constantTest, double_sub_bigint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_DOUBLE, &scltLeftVd);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BIGINT, &scltRightV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_SUB, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_FLOAT_EQ(v->datum.d, (scltLeftVd - scltRightV));
  nodesDestroyNode(res);
}

TEST(constantTest, tinyint_and_smallint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_TINYINT, &scltLeftV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &scltRightV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_BIT_AND, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(v->datum.i, (int64_t)scltLeftV & (int64_t)scltRightV);
  nodesDestroyNode(res);
}

TEST(constantTest, bigint_or_double) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BIGINT, &scltLeftV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &scltRightVd);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_BIT_OR, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(v->datum.i, (int64_t)scltLeftV | (int64_t)scltRightVd);
  nodesDestroyNode(res);
}

TEST(constantTest, int_or_binary) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   binaryStr[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&binaryStr[2], "%d", scltRightV);
  varDataSetLen(binaryStr, strlen(&binaryStr[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &scltLeftV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, binaryStr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_BIT_OR, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(v->datum.i, scltLeftV | scltRightV);
  nodesDestroyNode(res);
}

TEST(constantTest, int_greater_double) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &scltLeftV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &scltRightVd);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, scltLeftV > scltRightVd);
  nodesDestroyNode(res);
}

TEST(constantTest, int_greater_equal_binary) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   binaryStr[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&binaryStr[2], "%d", scltRightV);
  varDataSetLen(binaryStr, strlen(&binaryStr[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &scltLeftV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, binaryStr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, scltLeftV > scltRightVd);
  nodesDestroyNode(res);
}

TEST(constantTest, tinyint_lower_ubigint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_TINYINT, &scltLeftV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_UBIGINT, &scltRightV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, scltLeftV < scltRightV);
  nodesDestroyNode(res);
}

TEST(constantTest, usmallint_lower_equal_ubigint) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1;
  int64_t rightv = 1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_USMALLINT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_UBIGINT, &rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, leftv <= rightv);
  nodesDestroyNode(res);
}

TEST(constantTest, int_equal_smallint1) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1;
  int16_t rightv = 1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, leftv == rightv);
  nodesDestroyNode(res);
}

TEST(constantTest, int_equal_smallint2) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 0, rightv = 1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, leftv == rightv);
  nodesDestroyNode(res);
}

TEST(constantTest, int_not_equal_smallint1) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_NOT_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, leftv != rightv);
  nodesDestroyNode(res);
}

TEST(constantTest, int_not_equal_smallint2) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 0, rightv = 1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_NOT_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, leftv != rightv);
  nodesDestroyNode(res);
}

TEST(constantTest, int_in_smallint1) {
  scltInitLogFile();

  SNode  *pLeft = NULL, *pRight = NULL, *listNode = NULL, *res = NULL, *opNode = NULL;
  int32_t leftv = 1, rightv1 = 1, rightv2 = 2, rightv3 = 3;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  SNodeList *list = NULL;
  code = nodesMakeList(&list);
  ASSERT_NE(list, nullptr);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv3);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeListNode(&listNode, list, TSDB_DATA_TYPE_INT);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, int_in_smallint2) {
  scltInitLogFile();

  SNode  *pLeft = NULL, *pRight = NULL, *listNode = NULL, *res = NULL, *opNode = NULL;
  int32_t leftv = 4, rightv1 = 1, rightv2 = 2, rightv3 = 3;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  SNodeList* list = NULL;
  code = nodesMakeList(&list);
  ASSERT_NE(list, nullptr);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv3);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeListNode(&listNode, list, TSDB_DATA_TYPE_INT);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
  nodesDestroyNode(res);
}

TEST(constantTest, int_not_in_smallint1) {
  SNode  *pLeft = NULL, *pRight = NULL, *listNode = NULL, *res = NULL, *opNode = NULL;
  int32_t leftv = 1, rightv1 = 1, rightv2 = 2, rightv3 = 3;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  SNodeList *list = NULL;
  code = nodesMakeList(&list);
  ASSERT_NE(list, nullptr);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv3);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeListNode(&listNode, list, TSDB_DATA_TYPE_INT);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_NOT_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
  nodesDestroyNode(res);
}

TEST(constantTest, int_not_in_smallint2) {
  scltInitLogFile();

  SNode  *pLeft = NULL, *pRight = NULL, *listNode = NULL, *res = NULL, *opNode = NULL;
  int32_t leftv = 4, rightv1 = 1, rightv2 = 2, rightv3 = 3;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  SNodeList *list = NULL;
  code = nodesMakeList(&list);
  ASSERT_NE(list, nullptr);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_SMALLINT, &rightv3);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeListNode(&listNode, list, TSDB_DATA_TYPE_INT);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_NOT_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, binary_like_binary1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   leftv[64] = {0}, rightv[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  (void)sprintf(&rightv[2], "%s", "a_c");
  varDataSetLen(rightv, strlen(&rightv[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_LIKE, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, binary_like_binary2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   leftv[64] = {0}, rightv[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  (void)sprintf(&rightv[2], "%s", "ac");
  varDataSetLen(rightv, strlen(&rightv[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_LIKE, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
  nodesDestroyNode(res);
}

TEST(constantTest, binary_not_like_binary1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   leftv[64] = {0}, rightv[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  (void)sprintf(&rightv[2], "%s", "a%c");
  varDataSetLen(rightv, strlen(&rightv[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_NOT_LIKE, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
  nodesDestroyNode(res);
}

TEST(constantTest, binary_not_like_binary2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   leftv[64] = {0}, rightv[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  (void)sprintf(&rightv[2], "%s", "ac");
  varDataSetLen(rightv, strlen(&rightv[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_NOT_LIKE, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, binary_match_binary1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   leftv[64] = {0}, rightv[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  (void)sprintf(&rightv[2], "%s", ".*");
  varDataSetLen(rightv, strlen(&rightv[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_MATCH, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, binary_match_binary2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   leftv[64] = {0}, rightv[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  (void)sprintf(&rightv[2], "%s", "abc.+");
  varDataSetLen(rightv, strlen(&rightv[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_MATCH, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
  nodesDestroyNode(res);
}

TEST(constantTest, binary_not_match_binary1) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   leftv[64] = {0}, rightv[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  (void)sprintf(&rightv[2], "%s", "a[1-9]c");
  varDataSetLen(rightv, strlen(&rightv[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_NMATCH, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, binary_not_match_binary2) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   leftv[64] = {0}, rightv[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&leftv[2], "%s", "abc");
  varDataSetLen(leftv, strlen(&leftv[2]));
  (void)sprintf(&rightv[2], "%s", "a[ab]c");
  varDataSetLen(rightv, strlen(&rightv[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_BINARY, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_NMATCH, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
  nodesDestroyNode(res);
}

TEST(constantTest, int_is_null1) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IS_NULL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
  nodesDestroyNode(res);
}

TEST(constantTest, int_is_null2) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = TSDB_DATA_INT_NULL, rightv = 1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_NULL, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IS_NULL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, int_is_not_null1) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IS_NOT_NULL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, int_is_not_null2) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_NULL, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IS_NOT_NULL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
  nodesDestroyNode(res);
}

TEST(constantTest, int_add_int_is_true1) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_ADD, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, int_add_int_is_true2) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = -1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_ADD, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
  nodesDestroyNode(res);
}

TEST(constantTest, int_greater_int_is_true1) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 1;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
  nodesDestroyNode(res);
}

TEST(constantTest, int_greater_int_is_true2) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t leftv = 1, rightv = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_INT, &rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, opNode, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, greater_and_lower) {
  scltInitLogFile();

  SNode  *pval1 = NULL, *pval2 = NULL, *opNode1 = NULL, *opNode2 = NULL, *logicNode = NULL, *res = NULL;
  bool    eRes[5] = {false, false, true, true, true};
  int64_t v1 = 333, v2 = 222, v3 = -10, v4 = 20;
  SNode  *list[2] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pval1, TSDB_DATA_TYPE_BIGINT, &v1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pval2, TSDB_DATA_TYPE_BIGINT, &v2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pval1, pval2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pval1, TSDB_DATA_TYPE_BIGINT, &v3);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pval2, TSDB_DATA_TYPE_BIGINT, &v4);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pval1, pval2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  list[0] = opNode1;
  list[1] = opNode2;
  code = scltMakeLogicNode(&logicNode, LOGIC_COND_TYPE_AND, list, 2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(logicNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, column_and_value1) {
  scltInitLogFile();

  SNode  *pval1 = NULL, *pval2 = NULL, *opNode1 = NULL, *opNode2 = NULL, *logicNode = NULL, *res = NULL;
  bool    eRes[5] = {false, false, true, true, true};
  int64_t v1 = 333, v2 = 222, v3 = -10, v4 = 20;
  SNode  *list[2] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pval1, TSDB_DATA_TYPE_BIGINT, &v1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pval2, TSDB_DATA_TYPE_BIGINT, &v2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pval1, pval2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pval1, TSDB_DATA_TYPE_BIGINT, &v3);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeColumnNode(&pval2, NULL, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pval1, pval2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  list[0] = opNode1;
  list[1] = opNode2;
  code = scltMakeLogicNode(&logicNode, LOGIC_COND_TYPE_AND, list, 2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(logicNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_LOGIC_CONDITION);
  SLogicConditionNode *v = (SLogicConditionNode *)res;
  ASSERT_EQ(v->condType, LOGIC_COND_TYPE_AND);
  ASSERT_EQ(v->pParameterList->length, 1);
  nodesDestroyNode(res);
}

TEST(constantTest, column_and_value2) {
  scltInitLogFile();

  SNode  *pval1 = NULL, *pval2 = NULL, *opNode1 = NULL, *opNode2 = NULL, *logicNode = NULL, *res = NULL;
  bool    eRes[5] = {false, false, true, true, true};
  int64_t v1 = 333, v2 = 222, v3 = -10, v4 = 20;
  SNode  *list[2] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pval1, TSDB_DATA_TYPE_BIGINT, &v1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pval2, TSDB_DATA_TYPE_BIGINT, &v2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode1, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pval1, pval2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pval1, TSDB_DATA_TYPE_BIGINT, &v3);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeColumnNode(&pval2, NULL, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pval1, pval2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  list[0] = opNode1;
  list[1] = opNode2;
  code = scltMakeLogicNode(&logicNode, LOGIC_COND_TYPE_AND, list, 2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(logicNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, false);
  nodesDestroyNode(res);
}

TEST(constantTest, column_and_value3) {
  scltInitLogFile();

  SNode  *pval1 = NULL, *pval2 = NULL, *opNode1 = NULL, *opNode2 = NULL, *logicNode = NULL, *res = NULL;
  bool    eRes[5] = {false, false, true, true, true};
  int64_t v1 = 333, v2 = 222, v3 = -10, v4 = 20;
  SNode  *list[2] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pval1, TSDB_DATA_TYPE_BIGINT, &v1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pval2, TSDB_DATA_TYPE_BIGINT, &v2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pval1, pval2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pval1, TSDB_DATA_TYPE_BIGINT, &v3);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeColumnNode(&pval2, NULL, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pval1, pval2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  list[0] = opNode1;
  list[1] = opNode2;
  code = scltMakeLogicNode(&logicNode, LOGIC_COND_TYPE_OR, list, 2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(logicNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, true);
  nodesDestroyNode(res);
}

TEST(constantTest, column_and_value4) {
  scltInitLogFile();

  SNode  *pval1 = NULL, *pval2 = NULL, *opNode1 = NULL, *opNode2 = NULL, *logicNode = NULL, *res = NULL;
  bool    eRes[5] = {false, false, true, true, true};
  int64_t v1 = 333, v2 = 222, v3 = -10, v4 = 20;
  SNode  *list[2] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pval1, TSDB_DATA_TYPE_BIGINT, &v1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pval2, TSDB_DATA_TYPE_BIGINT, &v2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode1, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pval1, pval2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pval1, TSDB_DATA_TYPE_BIGINT, &v3);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeColumnNode(&pval2, NULL, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pval1, pval2);
  list[0] = opNode1;
  list[1] = opNode2;
  code = scltMakeLogicNode(&logicNode, LOGIC_COND_TYPE_OR, list, 2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(logicNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_LOGIC_CONDITION);
  SLogicConditionNode *v = (SLogicConditionNode *)res;
  ASSERT_EQ(v->condType, LOGIC_COND_TYPE_OR);
  ASSERT_EQ(v->pParameterList->length, 1);
  nodesDestroyNode(res);
}

int32_t makeJsonArrow(SSDataBlock **src, SNode **opNode, void *json, char *key) {
  char keyVar[32] = {0};
  (void)memcpy(varDataVal(keyVar), key, strlen(key));
  varDataLen(keyVar) = strlen(key);

  SNode *pLeft = NULL, *pRight = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  SCL_ERR_RET(scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, keyVar));
  SCL_ERR_RET(scltMakeColumnNode(&pLeft, src, TSDB_DATA_TYPE_JSON, ((STag *)json)->len, 1, json));
  SCL_ERR_RET(scltMakeOpNode(opNode, OP_TYPE_JSON_GET_VALUE, TSDB_DATA_TYPE_JSON, pLeft, pRight));
  SCL_RET(code);
}

int32_t makeOperator(SNode **opNode, SArray *blockList, EOperatorType opType, int32_t rightType, void *rightData,
                     bool isReverse) {
  int32_t resType = TSDB_DATA_TYPE_NULL;
  if (opType == OP_TYPE_ADD || opType == OP_TYPE_SUB || opType == OP_TYPE_MULTI || opType == OP_TYPE_DIV ||
      opType == OP_TYPE_REM || opType == OP_TYPE_MINUS) {
    resType = TSDB_DATA_TYPE_DOUBLE;
  } else if (opType == OP_TYPE_BIT_AND || opType == OP_TYPE_BIT_OR) {
    resType = TSDB_DATA_TYPE_BIGINT;
  } else if (opType == OP_TYPE_GREATER_THAN || opType == OP_TYPE_GREATER_EQUAL || opType == OP_TYPE_LOWER_THAN ||
             opType == OP_TYPE_LOWER_EQUAL || opType == OP_TYPE_EQUAL || opType == OP_TYPE_NOT_EQUAL ||
             opType == OP_TYPE_IS_NULL || opType == OP_TYPE_IS_NOT_NULL || opType == OP_TYPE_IS_TRUE ||
             opType == OP_TYPE_LIKE || opType == OP_TYPE_NOT_LIKE || opType == OP_TYPE_MATCH ||
             opType == OP_TYPE_NMATCH) {
    resType = TSDB_DATA_TYPE_BOOL;
  }

  SNode *right = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  SCL_ERR_RET(scltMakeValueNode(&right, rightType, rightData));
  SCL_ERR_RET(scltMakeOpNode2(opNode, opType, resType, *opNode, right, isReverse));

  SColumnInfo colInfo = createColumnInfo(1, resType, tDataTypes[resType].bytes);
  int16_t     dataBlockId = 0, slotId = 0;
  SCL_ERR_RET(scltAppendReservedSlot(blockList, &dataBlockId, &slotId, true, 1, &colInfo));
  SCL_ERR_RET(scltMakeTargetNode(opNode, dataBlockId, slotId, *opNode));
  SCL_RET(code);
}

int32_t makeCalculate(void *json, void *key, int32_t rightType, void *rightData, double exceptValue,
                      EOperatorType opType, bool isReverse) {
  SArray      *blockList = taosArrayInit(2, POINTER_BYTES);
  SSDataBlock *src = NULL;
  SNode       *opNode = NULL;

  SCL_ERR_RET(makeJsonArrow(&src, &opNode, json, (char *)key));
  if (NULL == taosArrayPush(blockList, &src)) {
    SCL_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCL_ERR_RET(makeOperator(&opNode, blockList, opType, rightType, rightData, isReverse));

  SCL_ERR_RET(scalarCalculate(opNode, blockList, NULL));

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  if (res->info.rows != 1) {
    (void)printf("expect 1 row, but got %d\n", res->info.rows);
    SCL_ERR_RET(TSDB_CODE_FAILED);
  }
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);

  if (colDataIsNull_f(column->nullbitmap, 0)) {
    if (DBL_MAX != exceptValue) {
      (void)printf("expect value = DBL_MAX, but got %d\n", exceptValue);
      SCL_ERR_RET(TSDB_CODE_FAILED);
    }
    (void)printf("result:NULL\n");

  } else if (opType == OP_TYPE_ADD || opType == OP_TYPE_SUB || opType == OP_TYPE_MULTI || opType == OP_TYPE_DIV ||
             opType == OP_TYPE_REM || opType == OP_TYPE_MINUS) {
    (void)printf("op:%s,1result:%f,except:%f\n", operatorTypeStr(opType), *((double *)colDataGetData(column, 0)),
                 exceptValue);
    if (fabs(*((double *)colDataGetData(column, 0)) - exceptValue) >= 0.0001) {
      (void)printf("expect value %d, but got %d\n", *((double *)colDataGetData(column, 0)), exceptValue);
      SCL_ERR_RET(TSDB_CODE_FAILED);
    }
  } else if (opType == OP_TYPE_BIT_AND || opType == OP_TYPE_BIT_OR) {
    (void)printf("op:%s,2result:%" PRId64 ",except:%f\n", operatorTypeStr(opType),
                 *((int64_t *)colDataGetData(column, 0)), exceptValue);
    if (*((int64_t *)colDataGetData(column, 0)) != exceptValue) {
      (void)printf("expect value %d, but got %d\n", *((int64_t *)colDataGetData(column, 0)), exceptValue);
      SCL_ERR_RET(TSDB_CODE_FAILED);
    }
  } else if (opType == OP_TYPE_GREATER_THAN || opType == OP_TYPE_GREATER_EQUAL || opType == OP_TYPE_LOWER_THAN ||
             opType == OP_TYPE_LOWER_EQUAL || opType == OP_TYPE_EQUAL || opType == OP_TYPE_NOT_EQUAL ||
             opType == OP_TYPE_IS_NULL || opType == OP_TYPE_IS_NOT_NULL || opType == OP_TYPE_IS_TRUE ||
             opType == OP_TYPE_LIKE || opType == OP_TYPE_NOT_LIKE || opType == OP_TYPE_MATCH ||
             opType == OP_TYPE_NMATCH) {
    (void)printf("op:%s,3result:%d,except:%f\n", operatorTypeStr(opType), *((bool *)colDataGetData(column, 0)),
                 exceptValue);
    if(*(bool *)colDataGetData(column, 0) != exceptValue) {
      (void)printf("expect value %d, but got %d\n", *((bool *)colDataGetData(column, 0)), exceptValue);
      SCL_ERR_RET(TSDB_CODE_FAILED);
    }
  }

  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
  SCL_RET(TSDB_CODE_SUCCESS);
}

TEST(columnTest, json_column_arith_op) {
  scltInitLogFile();
  char *rightvTmp =
      "{\"k1\":4,\"k2\":\"hello\",\"k3\":null,\"k4\":true,\"k5\":5.44,\"k6\":-10,\"k7\":-9.8,\"k8\":false,\"k9\":"
      "\"8hel\"}";

  char rightv[256] = {0};
  (void)memcpy(rightv, rightvTmp, strlen(rightvTmp));
  SArray *tags = taosArrayInit(1, sizeof(STagVal));
  ASSERT_NE(tags, nullptr);
  STag   *row = NULL;
  int32_t code = parseJsontoTagData(rightv, tags, &row, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  const int32_t len = 8;
  EOperatorType op[len] = {OP_TYPE_ADD, OP_TYPE_SUB,   OP_TYPE_MULTI,   OP_TYPE_DIV,
                           OP_TYPE_REM, OP_TYPE_MINUS, OP_TYPE_BIT_AND, OP_TYPE_BIT_OR};
  int32_t       input[len] = {1, 8, 2, 2, 3, 0, -4, 9};

  (void)printf("--------------------json int-4 op {1, 8, 2, 2, 3, 0, -4, 9}--------------------\n");
  char  *key = "k1";
  double eRes00[len] = {5.0, -4, 8.0, 2.0, 1.0, -4, 4 & -4, 4 | 9};
  double eRes01[len] = {5.0, 4, 8.0, 0.5, 3, 0, 4 & -4, 4 | 9};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes00[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes01[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  (void)printf("--------------------json string- 0 op {1, 8, 2, 2, 3, 0, -4, 9}--------------------\n");

  key = "k2";
  double eRes10[len] = {1.0, -8, 0, 0, 0, 0, 0, 9};
  double eRes11[len] = {1.0, 8, 0, DBL_MAX, DBL_MAX, 0, 0, 9};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes10[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes11[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  (void)printf("---------------------json null- null op {1, 8, 2, 2, 3, 0, -4, 9}-------------------\n");

  key = "k3";
  double eRes20[len] = {DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX};
  double eRes21[len] = {DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, 0, DBL_MAX, DBL_MAX};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes20[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes21[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  (void)printf("---------------------json bool- true op {1, 8, 2, 2, 3, 0, -4, 9}-------------------\n");

  key = "k4";
  double eRes30[len] = {2.0, -7, 2, 0.5, 1, -1, 1 & -4, 1 | 9};
  double eRes31[len] = {2.0, 7, 2, 2, 0, 0, 1 & -4, 1 | 9};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes30[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes31[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  (void)printf("----------------------json double-- 5.44 op {1, 8, 2, 2, 3, 0, -4, 9}------------------\n");

  key = "k5";
  double eRes40[len] = {6.44, -2.56, 10.88, 2.72, 2.44, -5.44, 5 & -4, 5 | 9};
  double eRes41[len] = {6.44, 2.56, 10.88, 0.3676470588235294, 3, 0, 5 & -4, 5 | 9};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes40[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes41[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  (void)printf("----------------------json int-- -10 op {1, 8, 2, 2, 3, 0, -4, 9}------------------\n");

  key = "k6";
  double eRes50[len] = {-9, -18, -20, -5, -10 % 3, 10, -10 & -4, -10 | 9};
  double eRes51[len] = {-9, 18, -20, -0.2, 3 % -10, 0, -10 & -4, -10 | 9};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes50[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes51[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  (void)printf("----------------------json double-- -9.8 op {1, 8, 2, 2, 3, 0, -4, 9}------------------\n");

  key = "k7";
  double eRes60[len] = {-8.8, -17.8, -19.6, -4.9, -0.8, 9.8, -9 & -4, -9 | 9};
  double eRes61[len] = {-8.8, 17.8, -19.6, -0.2040816326530612, 3, 0, -9 & -4, -9 | 9};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes60[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes61[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  (void)printf("----------------------json bool-- 0 op {1, 8, 2, 2, 3, 0, -4, 9}------------------\n");

  key = "k8";
  double eRes70[len] = {1.0, -8, 0, 0, 0, 0, 0, 9};
  double eRes71[len] = {1.0, 8, 0, DBL_MAX, DBL_MAX, 0, 0, 9};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes70[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes71[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  (void)printf("----------------------json string-- 8 op {1, 8, 2, 2, 3, 0, -4, 9}------------------\n");

  key = "k9";
  double eRes80[len] = {9, 0, 16, 4, 8 % 3, -8, 8 & -4, 8 | 9};
  double eRes81[len] = {9, 0, 16, 0.25, 3 % 8, 0, 8 & -4, 8 | 9};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes80[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes81[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  (void)printf("---------------------json not exist-- NULL op {1, 8, 2, 2, 3, 0, -4, 9}------------------\n");

  key = "k10";
  double eRes90[len] = {DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX};
  double eRes91[len] = {DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, 0, DBL_MAX, DBL_MAX};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes90[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes91[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  taosArrayDestroy(tags);
  taosMemoryFreeClear(row);
}

void *prepareNchar(char *rightData) {
  int32_t len = 0;
  int32_t inputLen = strlen(rightData);

  char *t = (char *)taosMemoryCalloc(1, (inputLen + 1) * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
  taosMbsToUcs4(rightData, inputLen, (TdUcs4 *)varDataVal(t), inputLen * TSDB_NCHAR_SIZE, &len);
  varDataSetLen(t, len);
  return t;
}

TEST(columnTest, json_column_logic_op) {
  scltInitLogFile();
  char *rightvTmp =
      "{\"k1\":4,\"k2\":\"hello\",\"k3\":null,\"k4\":true,\"k5\":5.44,\"k6\":-10,\"k7\":-9.8,\"k8\":false,\"k9\":\"6."
      "6hello\"}";

  char rightv[256] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)memcpy(rightv, rightvTmp, strlen(rightvTmp));
  SArray *tags = taosArrayInit(1, sizeof(STagVal));
  ASSERT_NE(tags, nullptr);
  STag   *row = NULL;
  code = parseJsontoTagData(rightv, tags, &row, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  const int32_t len0 = 6;
  const int32_t len = 9;
  const int32_t len1 = 4;
  EOperatorType op[len + len1] = {OP_TYPE_GREATER_THAN, OP_TYPE_GREATER_EQUAL, OP_TYPE_LOWER_THAN, OP_TYPE_LOWER_EQUAL,
                                  OP_TYPE_EQUAL,        OP_TYPE_NOT_EQUAL,     OP_TYPE_IS_NULL,    OP_TYPE_IS_NOT_NULL,
                                  OP_TYPE_IS_TRUE,      OP_TYPE_LIKE,          OP_TYPE_NOT_LIKE,   OP_TYPE_MATCH,
                                  OP_TYPE_NMATCH};

  int32_t input[len] = {1, 8, 2, 2, 3, 0, 0, 0, 0};
  char   *inputNchar[len1] = {"hell_", "hel%", "hell", "llll"};
  (void)printf("--------------------json int---4 {1, 8, 2, 2, 3, 0, 0, 0, 0}------------------\n");
  char *key = "k1";
  bool  eRes[len + len1] = {true, false, false, false, false, true, false, true, true, false, false, false, false};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  bool eRes_0[len0] = {false, true, true, true, false, true};
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes_0[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFreeClear(rightData);
  }

  (void)printf("--------------------json string--0 {1, 8, 2, 2, 3, 0, 0, 0, 0}-------------------\n");

  key = "k2";
  bool eRes1[len + len1] = {false, false, false, false, false, false, false, true, false, true, false, true, true};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes1[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  bool eRes_1[len0] = {false, false, false, false, false, false};
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes_1[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes1[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFreeClear(rightData);
  }

  (void)printf("--------------------json null---null {1, 8, 2, 2, 3, 0, 0, 0, 0}------------------\n");

  key = "k3";  // (null is true) return NULL, so use DBL_MAX represent NULL
  bool eRes2[len + len1] = {false, false, false, false, false, false, true, false, false, false, false, false, false};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes2[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  bool eRes_2[len0] = {false, false, false, false, false, false};
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes_2[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes2[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFreeClear(rightData);
  }

  (void)printf("--------------------json bool--1 {1, 8, 2, 2, 3, 0, 0, 0, 0}-------------------\n");

  key = "k4";
  bool eRes3[len + len1] = {false, false, false, false, false, false, false, true, true, false, false, false, false};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes3[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  bool eRes_3[len0] = {false, false, false, false, false, false};
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes_3[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes3[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFreeClear(rightData);
  }

  (void)printf("--------------------json double--5.44 {1, 8, 2, 2, 3, 0, 0, 0, 0}-------------------\n");

  key = "k5";
  bool eRes4[len + len1] = {true, false, false, false, false, true, false, true, true, false, false, false, false};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes4[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  bool eRes_4[len0] = {false, true, true, true, false, true};
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes_4[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes4[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFreeClear(rightData);
  }

  (void)printf("--------------------json int--  -10 {1, 8, 2, 2, 3, 0, 0, 0, 0}-------------------\n");

  key = "k6";
  bool eRes5[len + len1] = {false, false, true, true, false, true, false, true, true, false, false, false, false};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes5[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  bool eRes_5[len0] = {true, true, false, false, false, true};
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes_5[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes5[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFreeClear(rightData);
  }

  (void)printf("--------------------json double--  -9.8 {1, 8, 2, 2, 3, 0, 0, 0, 0}-------------------\n");

  key = "k7";
  bool eRes6[len + len1] = {false, false, true, true, false, true, false, true, true, false, false, false, false};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes6[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  bool eRes_6[len0] = {true, true, false, false, false, true};
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes_6[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes6[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFreeClear(rightData);
  }

  (void)printf("--------------------json bool--  0 {1, 8, 2, 2, 3, 0, 0, 0, 0}-------------------\n");

  key = "k8";
  bool eRes7[len + len1] = {false, false, false, false, false, false, false, true, false, false, false, false, false};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes7[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  bool eRes_7[len0] = {false, false, false, false, false, false};
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes_7[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes7[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFreeClear(rightData);
  }

  (void)printf("--------------------json string--  6.6hello {1, 8, 2, 2, 3, 0, 0, 0, 0}-------------------\n");

  key = "k9";
  bool eRes8[len + len1] = {false, false, false, false, false, false, false, true, true, false, true, true, true};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes8[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  bool eRes_8[len0] = {false, false, false, false, false, false};
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes_8[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    if (i == 11) {
      (void)printf("abc\n");
    }
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes8[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFreeClear(rightData);
  }

  (void)printf("---------------------json not exist-- NULL {1, 8, 2, 2, 3, 0, 0, 0, 0}------------------\n");

  key = "k10";  // (NULL is true) return NULL, so use DBL_MAX represent NULL
  bool eRes9[len + len1] = {false, false, false, false, false, false, true, false, false, false, false, false, false};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes9[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  bool eRes_9[len0] = {false, false, false, false, false, false};
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes_9[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes9[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFreeClear(rightData);
  }

  taosArrayDestroy(tags);
  taosMemoryFreeClear(row);
}

TEST(columnTest, smallint_value_add_int_column) {
  scltInitLogFile();

  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int32_t      leftv = 1;
  int16_t      rightv[5] = {0, -5, -4, 23, 100};
  double       eRes[5] = {1.0, -4, -3, 24, 101};
  SSDataBlock *src = NULL;
  int32_t      rowNum = sizeof(rightv) / sizeof(rightv[0]);
  int32_t      code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_INT, &leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeColumnNode(&pRight, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_ADD, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(2, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, true, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(column, i)), eRes[i]);
  }

  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, bigint_column_multi_binary_column) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int64_t leftv[5] = {1, 2, 3, 4, 5};
  char    rightv[5][5] = {0};
  for (int32_t i = 0; i < 5; ++i) {
    rightv[i][2] = rightv[i][3] = '0';
    rightv[i][4] = '0' + i;
    varDataSetLen(rightv[i], 3);
  }
  double       eRes[5] = {0, 2, 6, 12, 20};
  SSDataBlock *src = NULL;
  int32_t      rowNum = sizeof(rightv) / sizeof(rightv[0]);
  int32_t      code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), rowNum, leftv);
  code = scltMakeColumnNode(&pRight, &src, TSDB_DATA_TYPE_BINARY, 5, rowNum, rightv);
  code = scltMakeOpNode(&opNode, OP_TYPE_MULTI, TSDB_DATA_TYPE_DOUBLE, pLeft, pRight);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);

  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, false, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(column, i)), eRes[i]);
  }
  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, smallint_column_and_binary_column) {
  SNode  *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t leftv[5] = {1, 2, 3, 4, 5};
  char    rightv[5][5] = {0};
  for (int32_t i = 0; i < 5; ++i) {
    rightv[i][2] = rightv[i][3] = '0';
    rightv[i][4] = '0' + i;
    varDataSetLen(rightv[i], 3);
  }
  int64_t      eRes[5] = {0, 0, 2, 0, 4};
  SSDataBlock *src = NULL;
  int32_t      rowNum = sizeof(rightv) / sizeof(rightv[0]);
  int32_t      code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeColumnNode(&pRight, &src, TSDB_DATA_TYPE_BINARY, 5, rowNum, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_BIT_AND, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, false, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int64_t *)colDataGetData(column, i)), eRes[i]);
  }
  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, smallint_column_or_float_column) {
  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t      leftv[5] = {1, 2, 3, 4, 5};
  float        rightv[5] = {2.0, 3.0, 4.1, 5.2, 6.0};
  int64_t      eRes[5] = {3, 3, 7, 5, 7};
  SSDataBlock *src = NULL;
  int32_t      rowNum = sizeof(rightv) / sizeof(rightv[0]);
  int32_t      code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeColumnNode(&pRight, &src, TSDB_DATA_TYPE_FLOAT, sizeof(float), rowNum, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_BIT_OR, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, true, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int64_t *)colDataGetData(column, i)), eRes[i]);
  }
  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, smallint_column_or_double_value) {
  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t      leftv[5] = {1, 2, 3, 4, 5};
  double       rightv = 10.2;
  int64_t      eRes[5] = {11, 10, 11, 14, 15};
  SSDataBlock *src = NULL;
  int32_t      rowNum = sizeof(leftv) / sizeof(leftv[0]);
  int32_t      code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_BIT_OR, TSDB_DATA_TYPE_BIGINT, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, true, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int64_t *)colDataGetData(column, i)), eRes[i]);
  }
  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, smallint_column_greater_double_value) {
  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  int16_t      leftv[5] = {1, 2, 3, 4, 5};
  double       rightv = 2.5;
  bool         eRes[5] = {false, false, true, true, true};
  SSDataBlock *src = NULL;
  int32_t      rowNum = sizeof(leftv) / sizeof(leftv[0]);
  int32_t      code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BOOL, sizeof(bool));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, true, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_BOOL].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((bool *)colDataGetData(column, i)), eRes[i]);
  }
  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, int_column_in_double_list) {
  SNode       *pLeft = NULL, *pRight = NULL, *listNode = NULL, *opNode = NULL;
  int32_t      leftv[5] = {1, 2, 3, 4, 5};
  double       rightv1 = 1.1, rightv2 = 2.2, rightv3 = 3.3;
  bool         eRes[5] = {true, true, true, false, false};
  SSDataBlock *src = NULL;
  int32_t      rowNum = sizeof(leftv) / sizeof(leftv[0]);
  int32_t      code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_INT, sizeof(int32_t), rowNum, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  SNodeList *list = NULL;
  code = nodesMakeList(&list);
  ASSERT_NE(list, nullptr);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_DOUBLE, &rightv3);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeListNode(&listNode, list, TSDB_DATA_TYPE_INT);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BOOL, sizeof(bool));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, true, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_BOOL].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((bool *)colDataGetData(column, i)), eRes[i]);
  }
  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, binary_column_in_binary_list) {
  SNode       *pLeft = NULL, *pRight = NULL, *listNode = NULL, *opNode = NULL;
  bool         eRes[5] = {true, true, false, false, false};
  SSDataBlock *src = NULL;
  char         leftv[5][5] = {0};
  char         rightv[3][5] = {0};
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
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BINARY, 3, rowNum, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  SNodeList *list = NULL;
  code = nodesMakeList(&list);
  ASSERT_NE(list, nullptr);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv[0]);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv[1]);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv[2]);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(list, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeListNode(&listNode, list, TSDB_DATA_TYPE_BINARY);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IN, TSDB_DATA_TYPE_BOOL, pLeft, listNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BOOL, sizeof(bool));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, false, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_BOOL].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((bool *)colDataGetData(column, i)), eRes[i]);
  }
  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, binary_column_like_binary) {
  SNode       *pLeft = NULL, *pRight = NULL, *opNode = NULL;
  char         rightv[64] = {0};
  char         leftv[5][5] = {0};
  SSDataBlock *src = NULL;
  bool         eRes[5] = {true, false, true, false, true};

  for (int32_t i = 0; i < 5; ++i) {
    leftv[i][2] = 'a';
    leftv[i][3] = 'a';
    leftv[i][4] = '0' + i % 2;
    varDataSetLen(leftv[i], 3);
  }

  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BINARY, 3, rowNum, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  (void)sprintf(&rightv[2], "%s", "__0");
  varDataSetLen(rightv, strlen(&rightv[2]));
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, rightv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_LIKE, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BOOL, sizeof(bool));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, false, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_BOOL].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((bool *)colDataGetData(column, i)), eRes[i]);
  }

  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, binary_column_is_true) {
  SNode       *pLeft = NULL, *opNode = NULL;
  char         leftv[5][5] = {0};
  SSDataBlock *src = NULL;
  bool         eRes[5] = {false, true, false, true, false};

  for (int32_t i = 0; i < 5; ++i) {
    leftv[i][2] = '0' + i % 2;
    leftv[i][3] = 'a';
    leftv[i][4] = '0' + i % 2;
    varDataSetLen(leftv[i], 3);
  }

  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BINARY, 5, rowNum, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_IS_TRUE, TSDB_DATA_TYPE_BOOL, pLeft, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BOOL, sizeof(bool));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, false, rowNum, &colInfo);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);

  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_BOOL].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((bool *)colDataGetData(column, i)), eRes[i]);
  }
  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, binary_column_is_null) {
  SNode       *pLeft = NULL, *opNode = NULL;
  char         leftv[5][5] = {0};
  SSDataBlock *src = NULL;
  bool         eRes[5] = {false, false, true, false, true};

  for (int32_t i = 0; i < 5; ++i) {
    leftv[i][2] = '0' + i % 2;
    leftv[i][3] = 'a';
    leftv[i][4] = '0' + i % 2;
    varDataSetLen(leftv[i], 3);
  }

  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BINARY, 3, rowNum, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SColumnInfoData *pcolumn = (SColumnInfoData *)taosArrayGetLast(src->pDataBlock);
  code = colDataSetVal(pcolumn, 2, NULL, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = colDataSetVal(pcolumn, 4, NULL, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scltMakeOpNode(&opNode, OP_TYPE_IS_NULL, TSDB_DATA_TYPE_BOOL, pLeft, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BOOL, sizeof(bool));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, false, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_BOOL].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((bool *)colDataGetData(column, i)), eRes[i]);
  }
  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, binary_column_is_not_null) {
  SNode       *pLeft = NULL, *opNode = NULL;
  char         leftv[5][5] = {0};
  SSDataBlock *src = NULL;
  bool         eRes[5] = {true, true, true, true, false};

  for (int32_t i = 0; i < 5; ++i) {
    leftv[i][2] = '0' + i % 2;
    leftv[i][3] = 'a';
    leftv[i][4] = '0' + i % 2;
    varDataSetLen(leftv[i], 3);
  }

  int32_t rowNum = sizeof(leftv) / sizeof(leftv[0]);
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pLeft, &src, TSDB_DATA_TYPE_BINARY, 3, rowNum, leftv);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SColumnInfoData *pcolumn = (SColumnInfoData *)taosArrayGetLast(src->pDataBlock);
  code = colDataSetVal(pcolumn, 4, NULL, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scltMakeOpNode(&opNode, OP_TYPE_IS_NOT_NULL, TSDB_DATA_TYPE_BOOL, pLeft, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BOOL, sizeof(bool));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, false, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&opNode, dataBlockId, slotId, opNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculate(opNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_BOOL].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((bool *)colDataGetData(column, i)), eRes[i]);
  }
  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(opNode);
}

TEST(columnTest, greater_and_lower) {
  SNode       *pcol1 = NULL, *pcol2 = NULL, *opNode1 = NULL, *opNode2 = NULL, *logicNode = NULL;
  SNode       *list[2] = {0};
  int16_t      v1[5] = {1, 2, 3, 4, 5};
  int32_t      v2[5] = {5, 1, 4, 2, 6};
  int64_t      v3[5] = {1, 2, 3, 4, 5};
  int32_t      v4[5] = {5, 3, 4, 2, 6};
  bool         eRes[5] = {false, true, false, false, false};
  SSDataBlock *src = NULL;
  int32_t      rowNum = sizeof(v1) / sizeof(v1[0]);
  int32_t      code = TSDB_CODE_SUCCESS;
  code = scltMakeColumnNode(&pcol1, &src, TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), rowNum, v1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeColumnNode(&pcol2, &src, TSDB_DATA_TYPE_INT, sizeof(int32_t), rowNum, v2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pcol1, pcol2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeColumnNode(&pcol1, &src, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), rowNum, v3);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeColumnNode(&pcol2, &src, TSDB_DATA_TYPE_INT, sizeof(int32_t), rowNum, v4);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pcol1, pcol2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  list[0] = opNode1;
  list[1] = opNode2;
  code = scltMakeLogicNode(&logicNode, LOGIC_COND_TYPE_AND, list, 2);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *blockList = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blockList, nullptr);
  ASSERT_NE(taosArrayPush(blockList, &src), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BOOL, sizeof(bool));
  int16_t     dataBlockId = 0, slotId = 0;
  code = scltAppendReservedSlot(blockList, &dataBlockId, &slotId, false, rowNum, &colInfo);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeTargetNode(&logicNode, dataBlockId, slotId, logicNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculate(logicNode, blockList, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_BOOL].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((bool *)colDataGetData(column, i)), eRes[i]);
  }
  taosArrayDestroyEx(blockList, scltFreeDataBlock);
  nodesDestroyNode(logicNode);
}

int32_t scltMakeDataBlock(SScalarParam **pInput, int32_t type, void *pVal, int32_t num, bool setVal) {
  SScalarParam *input = (SScalarParam *)taosMemoryCalloc(1, sizeof(SScalarParam));
  if (NULL == input) {
    SCL_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  int32_t       bytes;
  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: {
      bytes = sizeof(int8_t);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      bytes = sizeof(int16_t);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      bytes = sizeof(int32_t);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      bytes = sizeof(int64_t);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      bytes = sizeof(float);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      bytes = sizeof(double);
      break;
    }
  }

  input->columnData = (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  if (NULL == input->columnData) {
    SCL_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  input->numOfRows = num;

  input->columnData->info = createColumnInfo(0, type, bytes);
  SCL_ERR_RET(colInfoDataEnsureCapacity(input->columnData, num, true));

  if (setVal) {
    for (int32_t i = 0; i < num; ++i) {
      SCL_ERR_RET(colDataSetVal(input->columnData, i, (const char *)pVal, false));
    }
  } else {
    //    memset(input->data, 0, num * bytes);
  }

  *pInput = input;
  SCL_RET(TSDB_CODE_SUCCESS);
}

void scltDestroyDataBlock(SScalarParam *pInput) {
  colDataDestroy(pInput->columnData);
  taosMemoryFreeClear(pInput->columnData);
  taosMemoryFreeClear(pInput);
}

TEST(ScalarFunctionTest, absFunction_constant) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;

  // TINYINT
  int8_t val_tinyint = 10;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)colDataGetData(pOutput->columnData, i)), val_tinyint);
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  val_tinyint = -10;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)colDataGetData(pOutput->columnData, i)), -val_tinyint);
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // SMALLINT
  int16_t val_smallint = 10;
  type = TSDB_DATA_TYPE_SMALLINT;
  code = scltMakeDataBlock(&pInput, type, &val_smallint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int16_t *)colDataGetData(pOutput->columnData, i)), val_smallint);
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  val_smallint = -10;
  code = scltMakeDataBlock(&pInput, type, &val_smallint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int16_t *)colDataGetData(pOutput->columnData, i)), -val_smallint);
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // INT
  int32_t val_int = 10;
  type = TSDB_DATA_TYPE_INT;
  code = scltMakeDataBlock(&pInput, type, &val_int, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int32_t *)colDataGetData(pOutput->columnData, i)), val_int);
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  val_int = -10;
  code = scltMakeDataBlock(&pInput, type, &val_int, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int32_t *)colDataGetData(pOutput->columnData, i)), -val_int);
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // BIGINT
  int64_t val_bigint = 10;
  type = TSDB_DATA_TYPE_BIGINT;
  code = scltMakeDataBlock(&pInput, type, &val_bigint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int64_t *)colDataGetData(pOutput->columnData, i)), val_bigint);
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  val_bigint = -10;
  code = scltMakeDataBlock(&pInput, type, &val_bigint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int64_t *)colDataGetData(pOutput->columnData, i)), -val_bigint);
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 10.15;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before ABS:%f\n", *(float *)pInput->data);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((float *)colDataGetData(pOutput->columnData, i)), val_float);
    PRINTF("float after ABS:%f\n", *((float *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  val_float = -10.15;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before ABS:%f\n", *(float *)pInput->data);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((float *)colDataGetData(pOutput->columnData, i)), -val_float);
    PRINTF("float after ABS:%f\n", *((float *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // DOUBLE
  double val_double = 10.15;
  type = TSDB_DATA_TYPE_DOUBLE;
  code = scltMakeDataBlock(&pInput, type, &val_double, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), val_double);
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  val_double = -10.15;
  code = scltMakeDataBlock(&pInput, type, &val_double, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), -val_double);
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, absFunction_column) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 5;
  int32_t       type;

  // TINYINT
  int8_t val_tinyint = 10;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    int8_t v = val_tinyint + i;
    colDataSetVal(pInput->columnData, i, (const char *)&v, false);
    PRINTF("tiny_int before ABS:%d\n", v);
  }

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)colDataGetData(pOutput->columnData, i)), val_tinyint + i);
    PRINTF("tiny_int after ABS:%d\n", *((int8_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  val_tinyint = -10;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    int8_t v = val_tinyint + i;
    colDataSetVal(pInput->columnData, i, (const char *)&v, false);
    PRINTF("tiny_int before ABS:%d\n", v);
  }

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)colDataGetData(pOutput->columnData, i)), -(val_tinyint + i));
    PRINTF("tiny_int after ABS:%d\n", *((int8_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // SMALLINT
  int16_t val_smallint = 10;
  type = TSDB_DATA_TYPE_SMALLINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    int16_t v = val_smallint + i;
    colDataSetVal(pInput->columnData, i, (const char *)&v, false);
    PRINTF("small_int before ABS:%d\n", v);
  }

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int16_t *)colDataGetData(pOutput->columnData, i)), val_smallint + i);
    PRINTF("small_int after ABS:%d\n", *((int16_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  val_smallint = -10;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    int16_t v = val_smallint + i;
    code = colDataSetVal(pInput->columnData, i, (const char *)&v, false);
    PRINTF("small_int before ABS:%d\n", v);
  }

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int16_t *)colDataGetData(pOutput->columnData, i)), -(val_smallint + i));
    PRINTF("small_int after ABS:%d\n", *((int16_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // INT
  int32_t val_int = 10;
  type = TSDB_DATA_TYPE_INT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  for (int32_t i = 0; i < rowNum; ++i) {
    int32_t v = val_int + i;
    code = colDataSetVal(pInput->columnData, i, (const char *)&v, false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("int before ABS:%d\n", v);
  }

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int32_t *)colDataGetData(pOutput->columnData, i)), val_int + i);
    PRINTF("int after ABS:%d\n", *((int32_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  val_int = -10;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    int32_t v = val_int + i;
    code = colDataSetVal(pInput->columnData, i, (const char *)&v, false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("int before ABS:%d\n", v);
  }

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int32_t *)colDataGetData(pOutput->columnData, i)), -(val_int + i));
    PRINTF("int after ABS:%d\n", *((int32_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 10.15;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    float v = val_float + i;
    code = colDataSetVal(pInput->columnData, i, (const char *)&v, false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("float before ABS:%f\n", v);
  }

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((float *)colDataGetData(pOutput->columnData, i)), val_float + i);
    PRINTF("float after ABS:%f\n", *((float *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  val_float = -10.15;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    float v = val_float + i;
    code = colDataSetVal(pInput->columnData, i, (const char *)&v, false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("float before ABS:%f\n", v);
  }

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((float *)colDataGetData(pOutput->columnData, i)), -(val_float + i));
    PRINTF("float after ABS:%f\n", *((float *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // DOUBLE
  double val_double = 10.15;
  type = TSDB_DATA_TYPE_DOUBLE;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    double v = val_double + i;
    code = colDataSetVal(pInput->columnData, i, (const char *)&v, false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("double before ABS:%f\n", v);
  }

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), val_double + i);
    PRINTF("double after ABS:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  val_double = -10.15;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    double v = val_double + i;
    code = colDataSetVal(pInput->columnData, i, (const char *)&v, false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("double before ABS:%f\n", v);
  }

  code = absFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), -(val_double + i));
    PRINTF("double after ABS:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, sinFunction_constant) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result = 0.42016703682664092;

  // TINYINT
  int8_t val_tinyint = 13;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before SIN:%d\n", *((int8_t *)pInput->data));

  code = sinFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int after SIN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 13.00;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before SIN:%f\n", *((float *)pInput->data));

  code = sinFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("float after SIN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, sinFunction_column) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result[] = {0.42016703682664092, 0.99060735569487035, 0.65028784015711683};

  // TINYINT
  int8_t val_tinyint[] = {13, 14, 15};
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    code = colDataSetVal(pInput->columnData, i, (const char *)&val_tinyint[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("tiny_int before SIN:%d\n", *(int8_t *)colDataGetData(pInput->columnData, i));
  }

  code = sinFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int after SIN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {13.00, 14.00, 15.00};
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    code = colDataSetVal(pInput->columnData, i, (const char *)&val_float[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("float before SIN:%f\n", *((float *)colDataGetData(pInput->columnData, i)));
  }

  code = sinFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("float after SIN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, cosFunction_constant) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result = 0.90744678145019619;

  // TINYINT
  int8_t val_tinyint = 13;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before COS:%d\n", *((int8_t *)pInput->data));

  code = cosFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int after COS:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 13.00;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before COS:%f\n", *((float *)pInput->data));

  code = cosFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("float after COS:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, cosFunction_column) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result[] = {0.90744678145019619, 0.13673721820783361, -0.75968791285882131};

  // TINYINT
  int8_t val_tinyint[] = {13, 14, 15};
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    code = colDataSetVal(pInput->columnData, i, (const char *)&val_tinyint[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("tiny_int before COS:%d\n", *(int8_t *)colDataGetData(pInput->columnData, i));
  }

  code = cosFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int after COS:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {13.00, 14.00, 15.00};
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    code = colDataSetVal(pInput->columnData, i, (const char *)&val_float[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("float before COS:%f\n", *(float *)colDataGetData(pInput->columnData, i));
  }

  code = cosFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("float after COS:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, tanFunction_constant) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result = 0.46302113293648961;

  // TINYINT
  int8_t val_tinyint = 13;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before TAN:%d\n", *((int8_t *)pInput->data));

  code = tanFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int after TAN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 13.00;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before TAN:%f\n", *((float *)pInput->data));

  code = tanFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("float after TAN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, tanFunction_column) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result[] = {0.46302113293648961, 7.24460661609480550, -0.85599340090851872};

  // TINYINT
  int8_t val_tinyint[] = {13, 14, 15};
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    code = colDataSetVal(pInput->columnData, i, (const char *)&val_tinyint[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("tiny_int before TAN:%d\n", *((int8_t *)colDataGetData(pInput->columnData, i)));
  }

  code = tanFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_NEAR(*((double *)colDataGetData(pOutput->columnData, i)), result[i], 1e-15);
    PRINTF("tiny_int after TAN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {13.00, 14.00, 15.00};
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    code = colDataSetVal(pInput->columnData, i, (const char *)&val_float[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("float before TAN:%f\n", *((float *)colDataGetData(pInput->columnData, i)));
  }

  code = tanFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_NEAR(*((double *)colDataGetData(pOutput->columnData, i)), result[i], 1e-15);
    PRINTF("float after TAN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, asinFunction_constant) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result = 1.57079632679489656;

  // TINYINT
  int8_t val_tinyint = 1;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before ASIN:%d\n", *((int8_t *)pInput->data));

  code = asinFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int after ASIN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 1.00;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before ASIN:%f\n", *((float *)pInput->data));

  code = asinFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("float after ASIN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, asinFunction_column) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result[] = {-1.57079632679489656, 0.0, 1.57079632679489656};

  // TINYINT
  int8_t val_tinyint[] = {-1, 0, 1};
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    code = colDataSetVal(pInput->columnData, i, (const char *)&val_tinyint[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    PRINTF("tiny_int before ASIN:%d\n", *((int8_t *)colDataGetData(pInput->columnData, i)));
  }

  code = asinFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int after ASIN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {-1.0, 0.0, 1.0};
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((float *)colDataGetData(pInput->columnData, i)) = val_float[i];
    PRINTF("float before ASIN:%f\n", *((float *)colDataGetData(pInput->columnData, i)));
  }

  code = asinFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("float after ASIN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, acosFunction_constant) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result = 0.0;

  // TINYINT
  int8_t val_tinyint = 1;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before ACOS:%d\n", *((int8_t *)pInput->data));

  code = acosFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int after ACOS:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 1.00;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before ACOS:%f\n", *((float *)pInput->data));

  code = acosFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("float after ACOS:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, acosFunction_column) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result[] = {3.14159265358979312, 1.57079632679489656, 0.0};

  // TINYINT
  int8_t val_tinyint[] = {-1, 0, 1};
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((int8_t *)colDataGetData(pInput->columnData, i)) = val_tinyint[i];
    PRINTF("tiny_int before ACOS:%d\n", *((int8_t *)colDataGetData(pInput->columnData, i)));
  }

  code = acosFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int after ACOS:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {-1.0, 0.0, 1.0};
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((float *)colDataGetData(pInput->columnData, i)) = val_float[i];
    PRINTF("float before ACOS:%f\n", *((float *)colDataGetData(pInput->columnData, i)));
  }

  code = acosFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("float after ACOS:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, atanFunction_constant) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result = 0.78539816339744828;

  // TINYINT
  int8_t val_tinyint = 1;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before ATAN:%d\n", *((int8_t *)pInput->data));

  code = atanFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int after ATAN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 1.00;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before ATAN:%f\n", *((float *)pInput->data));

  code = atanFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("float after ATAN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, atanFunction_column) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result[] = {-0.78539816339744828, 0.0, 0.78539816339744828};

  // TINYINT
  int8_t val_tinyint[] = {-1, 0, 1};
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((int8_t *)colDataGetData(pInput->columnData, i)) = val_tinyint[i];
    PRINTF("tiny_int before ATAN:%d\n", *((int8_t *)colDataGetData(pInput->columnData, i)));
  }

  code = atanFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int after ATAN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {-1.0, 0.0, 1.0};
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((float *)colDataGetData(pInput->columnData, i)) = val_float[i];
    PRINTF("float before ATAN:%f\n", *((float *)colDataGetData(pInput->columnData, i)));
  }

  code = atanFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("float after ATAN:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, ceilFunction_constant) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  double        result = 10.0;

  // TINYINT
  int8_t val_tinyint = 10;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before CEIL:%d\n", *((int8_t *)pInput->data));

  code = ceilFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)colDataGetData(pOutput->columnData, i)), (int8_t)result);
    PRINTF("tiny_int after CEIL:%d\n", *((int8_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 9.5;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before CEIL:%f\n", *((float *)pInput->data));

  code = ceilFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((float *)colDataGetData(pOutput->columnData, i)), (float)result);
    PRINTF("float after CEIL:%f\n", *((float *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, ceilFunction_column) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  double        result[] = {-10.0, 0.0, 10.0};

  // TINYINT
  int8_t val_tinyint[] = {-10, 0, 10};
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((int8_t *)colDataGetData(pInput->columnData, i)) = val_tinyint[i];
    PRINTF("tiny_int before CEIL:%d\n", *((int8_t *)colDataGetData(pInput->columnData, i)));
  }

  code = ceilFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int after CEIL:%d\n", *((int8_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {-10.5, 0.0, 9.5};
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((float *)colDataGetData(pInput->columnData, i)) = val_float[i];
    PRINTF("float before CEIL:%f\n", *((float *)colDataGetData(pInput->columnData, i)));
  }

  code = ceilFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((float *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("float after CEIL:%f\n", *((float *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, floorFunction_constant) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  double        result = 10.0;

  // TINYINT
  int8_t val_tinyint = 10;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before FLOOR:%d\n", *((int8_t *)pInput->data));

  code = floorFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)colDataGetData(pOutput->columnData, i)), (int8_t)result);
    PRINTF("tiny_int after FLOOR:%d\n", *((int8_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 10.5;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before FLOOR:%f\n", *((float *)pInput->data));

  code = floorFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((float *)colDataGetData(pOutput->columnData, i)), (float)result);
    PRINTF("float after FLOOR:%f\n", *((float *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, floorFunction_column) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  double        result[] = {-10.0, 0.0, 10.0};

  // TINYINT
  int8_t val_tinyint[] = {-10, 0, 10};
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((int8_t *)colDataGetData(pInput->columnData, i)) = val_tinyint[i];
    PRINTF("tiny_int before FLOOR:%d\n", *((int8_t *)colDataGetData(pInput->columnData, i)));
  }

  code = floorFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int after FLOOR:%d\n", *((int8_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {-9.5, 0.0, 10.5};
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((float *)colDataGetData(pInput->columnData, i)) = val_float[i];
    PRINTF("float before FLOOR:%f\n", *((float *)colDataGetData(pInput->columnData, i)));
  }

  code = floorFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((float *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("float after FLOOR:%f\n", *((float *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, roundFunction_constant) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  double        result = 10.0;

  // TINYINT
  int8_t val_tinyint = 10;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before ROUND:%d\n", *((int8_t *)pInput->data));

  code = roundFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)colDataGetData(pOutput->columnData, i)), (int8_t)result);
    PRINTF("tiny_int after ROUND:%d\n", *((int8_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 9.5;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before ROUND:%f\n", *((float *)pInput->data));

  code = roundFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((float *)colDataGetData(pOutput->columnData, i)), (float)result);
    PRINTF("float after ROUND:%f\n", *((float *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, roundFunction_column) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  double        result[] = {-10.0, 0.0, 10.0};

  // TINYINT
  int8_t val_tinyint[] = {-10, 0, 10};
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((int8_t *)colDataGetData(pInput->columnData, i)) = val_tinyint[i];
    PRINTF("tiny_int before ROUND:%d\n", *((int8_t *)colDataGetData(pInput->columnData, i)));
  }

  code = roundFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((int8_t *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int after ROUND:%d\n", *((int8_t *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {-9.5, 0.0, 9.5};
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((float *)colDataGetData(pInput->columnData, i)) = val_float[i];
    PRINTF("float before ROUND:%f\n", *((float *)colDataGetData(pInput->columnData, i)));
  }

  code = roundFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((float *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("float after ROUND:%f\n", *((float *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, sqrtFunction_constant) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result = 5.0;

  // TINYINT
  int8_t val_tinyint = 25;
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, &val_tinyint, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before SQRT:%d\n", *((int8_t *)pInput->data));

  code = sqrtFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int after SQRT:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float = 25.0;
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, &val_float, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before SQRT:%f\n", *((float *)pInput->data));

  code = sqrtFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("float after SQRT:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, sqrtFunction_column) {
  SScalarParam *pInput, *pOutput;
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result[] = {5.0, 9.0, 10.0};

  // TINYINT
  int8_t val_tinyint[] = {25, 81, 100};
  type = TSDB_DATA_TYPE_TINYINT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((int8_t *)colDataGetData(pInput->columnData, i)) = val_tinyint[i];
    PRINTF("tiny_int before SQRT:%d\n", *((int8_t *)colDataGetData(pInput->columnData, i)));
  }

  code = sqrtFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int after SQRT:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {25.0, 81.0, 100.0};
  type = TSDB_DATA_TYPE_FLOAT;
  code = scltMakeDataBlock(&pInput, type, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    *((float *)colDataGetData(pInput->columnData, i)) = val_float[i];
    PRINTF("float before SQRT:%f\n", *((float *)colDataGetData(pInput->columnData, i)));
  }

  code = sqrtFunction(pInput, 1, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("float after SQRT:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(pInput);
  scltDestroyDataBlock(pOutput);
}

TEST(ScalarFunctionTest, logFunction_constant) {
  SScalarParam *pInput, *pOutput;
  SScalarParam *input[2];
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result = 3.0;
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));

  // TINYINT
  int8_t val_tinyint[] = {27, 3};
  type = TSDB_DATA_TYPE_TINYINT;
  for (int32_t i = 0; i < 2; ++i) {
    code = scltMakeDataBlock(&input[i], type, &val_tinyint[i], rowNum, true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    pInput[i] = *input[i];
  }
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before LOG: %d,%d\n", *((int8_t *)pInput[0].data), *((int8_t *)pInput[1].data));

  code = logFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int after LOG:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {64.0, 4.0};
  type = TSDB_DATA_TYPE_FLOAT;
  for (int32_t i = 0; i < 2; ++i) {
    code = scltMakeDataBlock(&input[i], type, &val_float[i], rowNum, true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    pInput[i] = *input[i];
  }
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before LOG: %f,%f\n", *((float *)pInput[0].data), *((float *)pInput[1].data));

  code = logFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("float after LOG:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);

  // TINYINT AND FLOAT
  int8_t param0 = 64;
  float  param1 = 4.0;
  code = scltMakeDataBlock(&input[0], TSDB_DATA_TYPE_TINYINT, &param0, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  pInput[0] = *input[0];
  code = scltMakeDataBlock(&input[1], TSDB_DATA_TYPE_FLOAT, &param1, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  pInput[1] = *input[1];
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int,float before LOG: %d,%f\n", *((int8_t *)pInput[0].data), *((float *)pInput[1].data));

  code = logFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int,float after LOG:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);
  taosMemoryFreeClear(pInput);
}

TEST(ScalarFunctionTest, logFunction_column) {
  SScalarParam *pInput, *pOutput;
  SScalarParam *input[2];
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result[] = {2.0, 3.0, 4.0};
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));

  // TINYINT
  int8_t val_tinyint[2][3] = {{9, 27, 81}, {3, 3, 3}};
  type = TSDB_DATA_TYPE_TINYINT;
  for (int32_t i = 0; i < 2; ++i) {
    code = scltMakeDataBlock(&input[i], type, 0, rowNum, false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    pInput[i] = *input[i];
    for (int32_t j = 0; j < rowNum; ++j) {
      code = colDataSetVal(pInput[i].columnData, j, (const char *)&val_tinyint[i][j], false);
      ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    }
    PRINTF("tiny_int before LOG:%d,%d,%d\n", *((int8_t *)pInput[i].data + 0), *((int8_t *)pInput[i].data + 1),
           *((int8_t *)pInput[i].data + 2));
  }
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = logFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int after LOG:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[2][3] = {{9.0, 27.0, 81.0}, {3.0, 3.0, 3.0}};
  type = TSDB_DATA_TYPE_FLOAT;
  for (int32_t i = 0; i < 2; ++i) {
    code = scltMakeDataBlock(&input[i], type, 0, rowNum, false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    pInput[i] = *input[i];
    for (int32_t j = 0; j < rowNum; ++j) {
      code = colDataSetVal(pInput[i].columnData, j, (const char *)&val_float[i][j], false);
      ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    }
    PRINTF("float before LOG:%f,%f,%f\n", *((float *)colDataGetData(pInput[i], 0)),
           *((float *)colDataGetData(pInput[i], 1)), *((float *)colDataGetData(pInput[i], 2)));
  }
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = logFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("float after LOG:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);

  // TINYINT AND FLOAT
  int8_t param0[] = {9, 27, 81};
  float  param1[] = {3.0, 3.0, 3.0};
  code = scltMakeDataBlock(&input[0], TSDB_DATA_TYPE_TINYINT, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  pInput[0] = *input[0];
  for (int32_t i = 0; i < rowNum; ++i) {
    code = colDataSetVal(pInput[0].columnData, i, (const char *)&param0[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  code = scltMakeDataBlock(&input[1], TSDB_DATA_TYPE_FLOAT, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  pInput[1] = *input[1];
  for (int32_t i = 0; i < rowNum; ++i) {
    code = colDataSetVal(pInput[1].columnData, i, (const char *)&param1[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  PRINTF("tiny_int, float before LOG:{%d,%f}, {%d,%f}, {%d,%f}\n", *((int8_t *)pInput[0].data + 0),
         *((float *)pInput[1].data + 0), *((int8_t *)pInput[0].data + 1), *((float *)pInput[1].data + 1),
         *((int8_t *)pInput[0].data + 2), *((float *)pInput[1].data + 2));
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = logFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int,float after LOG:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);
  taosMemoryFreeClear(pInput);
}

TEST(ScalarFunctionTest, powFunction_constant) {
  SScalarParam *pInput, *pOutput;
  SScalarParam *input[2];
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result = 16.0;
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));

  // TINYINT
  int8_t val_tinyint[] = {2, 4};
  type = TSDB_DATA_TYPE_TINYINT;
  for (int32_t i = 0; i < 2; ++i) {
    code = scltMakeDataBlock(&input[i], type, &val_tinyint[i], rowNum, true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    pInput[i] = *input[i];
  }
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int before POW: %d,%d\n", *((int8_t *)pInput[0].data), *((int8_t *)pInput[1].data));

  code = powFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int after POW:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[] = {2.0, 4.0};
  type = TSDB_DATA_TYPE_FLOAT;
  for (int32_t i = 0; i < 2; ++i) {
    code = scltMakeDataBlock(&input[i], type, &val_float[i], rowNum, true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    pInput[i] = *input[i];
  }
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("float before POW: %f,%f\n", *((float *)pInput[0].data), *((float *)pInput[1].data));

  code = powFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("float after POW:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);

  // TINYINT AND FLOAT
  int8_t param0 = 2;
  float  param1 = 4.0;
  code = scltMakeDataBlock(&input[0], TSDB_DATA_TYPE_TINYINT, &param0, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  pInput[0] = *input[0];
  code = scltMakeDataBlock(&input[1], TSDB_DATA_TYPE_FLOAT, &param1, rowNum, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  pInput[1] = *input[1];
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  PRINTF("tiny_int,float before POW: %d,%f\n", *((int8_t *)pInput[0].data), *((float *)pInput[1].data));

  code = powFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int,float after POW:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);
  taosMemoryFreeClear(pInput);
}

TEST(ScalarFunctionTest, powFunction_column) {
  SScalarParam *pInput, *pOutput;
  SScalarParam *input[2];
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       rowNum = 3;
  int32_t       type;
  int32_t       otype = TSDB_DATA_TYPE_DOUBLE;
  double        result[] = {8.0, 27.0, 64.0};
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));

  // TINYINT
  int8_t val_tinyint[2][3] = {{2, 3, 4}, {3, 3, 3}};
  type = TSDB_DATA_TYPE_TINYINT;
  for (int32_t i = 0; i < 2; ++i) {
    code = scltMakeDataBlock(&input[i], type, 0, rowNum, false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    pInput[i] = *input[i];
    for (int32_t j = 0; j < rowNum; ++j) {
      code = colDataSetVal(pInput[i].columnData, j, (const char *)&val_tinyint[i][j], false);
      ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    }
    PRINTF("tiny_int before POW:%d,%d,%d\n", *((int8_t *)pInput[i].data + 0), *((int8_t *)pInput[i].data + 1),
           *((int8_t *)pInput[i].data + 2));
  }
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = powFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int after POW:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);

  // FLOAT
  float val_float[2][3] = {{2.0, 3.0, 4.0}, {3.0, 3.0, 3.0}};
  type = TSDB_DATA_TYPE_FLOAT;
  for (int32_t i = 0; i < 2; ++i) {
    code = scltMakeDataBlock(&input[i], type, 0, rowNum, false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    pInput[i] = *input[i];
    for (int32_t j = 0; j < rowNum; ++j) {
      code = colDataSetVal(pInput[i].columnData, j, (const char *)&val_float[i][j], false);
      ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    }
    PRINTF("float before POW:%f,%f,%f\n", *((float *)pInput[i].data + 0), *((float *)pInput[i].data + 1),
           *((float *)pInput[i].data + 2));
  }
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = powFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("float after POW:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }
  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);

  // TINYINT AND FLOAT
  int8_t param0[] = {2, 3, 4};
  float  param1[] = {3.0, 3.0, 3.0};
  code = scltMakeDataBlock(&input[0], TSDB_DATA_TYPE_TINYINT, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  pInput[0] = *input[0];
  for (int32_t i = 0; i < rowNum; ++i) {
    code = colDataSetVal(pInput[0].columnData, i, (const char *)&param0[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  code = scltMakeDataBlock(&input[1], TSDB_DATA_TYPE_FLOAT, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  pInput[1] = *input[1];
  for (int32_t i = 0; i < rowNum; ++i) {
    code = colDataSetVal(pInput[1].columnData, i, (const char *)&param1[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  PRINTF("tiny_int, float before POW:{%d,%f}, {%d,%f}, {%d,%f}\n", *((int8_t *)pInput[0].data + 0),
         *((float *)pInput[1].data + 0), *((int8_t *)pInput[0].data + 1), *((float *)pInput[1].data + 1),
         *((int8_t *)pInput[0].data + 2), *((float *)pInput[1].data + 2));
  code = scltMakeDataBlock(&pOutput, otype, 0, rowNum, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = powFunction(pInput, 2, pOutput);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int,float after POW:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);
  taosMemoryFreeClear(pInput);
}

int main(int argc, char **argv) {
  taosSeedRand(taosGetTimestampSec());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop
