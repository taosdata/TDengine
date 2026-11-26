/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#endif

#include <gtest/gtest.h>
#include <iostream>
#include <limits>
#include <vector>

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

#include "os.h"

#include "filter.h"
#include "filterInt.h"
#include "nodes.h"
#include "scalar.h"
#include "stub.h"
#include "taos.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "thash.h"
#include "tlog.h"
#include "tvariant.h"

namespace {

int64_t flttLeftV = 21, flttRightV = 10;
double  flttLeftVd = 21.0, flttRightVd = 10.0;

class QDebugFlagGuard {
 public:
  explicit QDebugFlagGuard(int32_t flag) : previousFlag_(qDebugFlag) { qDebugFlag |= flag; }
  ~QDebugFlagGuard() { qDebugFlag = previousFlag_; }
  QDebugFlagGuard(const QDebugFlagGuard &) = delete;
  QDebugFlagGuard &operator=(const QDebugFlagGuard &) = delete;

 private:
  int32_t previousFlag_;
};

class FilterScalarModeGuard {
 public:
  FilterScalarModeGuard() : previous_(tsFilterScalarMode) { tsFilterScalarMode = true; }
  ~FilterScalarModeGuard() { tsFilterScalarMode = previous_; }
  FilterScalarModeGuard(const FilterScalarModeGuard &) = delete;
  FilterScalarModeGuard &operator=(const FilterScalarModeGuard &) = delete;

 private:
  bool previous_;
};

void flttInitLogFile() {
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  tstrncpy(tsLogDir, TD_LOG_DIR_PATH, PATH_MAX);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum, false) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

int32_t flttMakeValueNode(SNode **pNode, int32_t dataType, void *value) {
  SNode      *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, &node);
  if (NULL == node) {
    FLT_ERR_RET(code);
  }
  SValueNode *vnode = (SValueNode *)node;
  vnode->node.resType.type = dataType;

  if (IS_VAR_DATA_TYPE(dataType)) {
    if (IS_STR_DATA_BLOB(dataType)) {
      vnode->datum.p = (char *)taosMemoryMalloc(blobDataTLen(value));
      if (NULL == vnode->datum.p) {
        FLT_ERR_RET(terrno);
      }
      blobDataCopy(vnode->datum.p, value);
      vnode->node.resType.bytes = blobDataTLen(value);
    } else {
      vnode->datum.p = (char *)taosMemoryMalloc(varDataTLen(value));
      if (NULL == vnode->datum.p) {
        FLT_ERR_RET(terrno);
      }
      varDataCopy(vnode->datum.p, value);
      vnode->node.resType.bytes = varDataLen(value);
    }
  } else {
    vnode->node.resType.bytes = tDataTypes[dataType].bytes;
    assignVal((char *)nodesGetValueFromNode(vnode), (const char *)value, 0, dataType);
  }

  *pNode = (SNode *)vnode;
  FLT_RET(TSDB_CODE_SUCCESS);
}

int32_t flttMakeColumnNode(SNode **pNode, SSDataBlock **block, int32_t dataType, int32_t dataBytes, int32_t rowNum,
                           void *value) {
  static uint64_t dbidx = 0;

  SNode       *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, &node);
  if (NULL == node) {
    FLT_ERR_RET(code);
  }
  SColumnNode *rnode = (SColumnNode *)node;
  rnode->node.resType.type = dataType;
  rnode->node.resType.bytes = dataBytes;
  rnode->dataBlockId = 0;

  snprintf(rnode->dbName, TSDB_DB_NAME_LEN, "%" PRIu64, dbidx++);

  if (NULL == block) {
    rnode->slotId = 2;
    rnode->colId = 3;
    *pNode = (SNode *)rnode;

    FLT_RET(TSDB_CODE_SUCCESS);
  }

  if (NULL == *block) {
    SSDataBlock *res = NULL;
    FLT_ERR_RET(createDataBlock(&res));

    for (int32_t i = 0; i < 2; ++i) {
      SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_NULL, 10, 1 + i);
      FLT_ERR_RET(blockDataAppendColInfo(res, &idata));
    }

    SColumnInfoData idata = createColumnInfoData(dataType, dataBytes, 3);
    FLT_ERR_RET(blockDataAppendColInfo(res, &idata));
    FLT_ERR_RET(blockDataEnsureCapacity(res, rowNum));

    SColumnInfoData *pColumn = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
    if (NULL == pColumn) {
      fltError("fail to get the last task, num:%d", (int32_t)taosArrayGetSize(res->pDataBlock));
      FLT_ERR_RET(TSDB_CODE_QRY_SYS_ERROR);
    }
    for (int32_t i = 0; i < rowNum; ++i) {
      FLT_ERR_RET(colDataSetVal(pColumn, i, (const char *)value, false));
      if (IS_VAR_DATA_TYPE(dataType)) {
        if (IS_STR_DATA_BLOB(dataType)) {
          value = (char *)value + blobDataTLen(value);
        } else {
          value = (char *)value + varDataTLen(value);
        }
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
    FLT_ERR_RET(blockDataAppendColInfo(res, &idata));
    FLT_ERR_RET(blockDataEnsureCapacity(res, rowNum));

    SColumnInfoData *pColumn = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
    if (NULL == pColumn) {
      fltError("fail to get the last task, num:%d", (int32_t)taosArrayGetSize(res->pDataBlock));
      FLT_ERR_RET(TSDB_CODE_QRY_SYS_ERROR);
    }
    for (int32_t i = 0; i < rowNum; ++i) {
      FLT_ERR_RET(colDataSetVal(pColumn, i, (const char *)value, false));
      if (IS_VAR_DATA_TYPE(dataType)) {
        if (IS_STR_DATA_BLOB(dataType)) {
          value = (char *)value + blobDataTLen(value);
        } else {
          value = (char *)value + varDataTLen(value);
        }
      } else {
        value = (char *)value + dataBytes;
      }
    }

    rnode->slotId = idx;
    rnode->colId = 1 + idx;
  }

  *pNode = (SNode *)rnode;
  FLT_RET(TSDB_CODE_SUCCESS);
}

int32_t flttMakeOpNode(SNode **pNode, EOperatorType opType, int32_t resType, SNode *pLeft, SNode *pRight) {
  SNode         *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, &node);
  if (NULL == node) {
    FLT_ERR_RET(code);
  }
  SOperatorNode *onode = (SOperatorNode *)node;
  onode->node.resType.type = resType;
  onode->node.resType.bytes = tDataTypes[resType].bytes;

  onode->opType = opType;
  onode->pLeft = pLeft;
  onode->pRight = pRight;

  *pNode = (SNode *)onode;
  FLT_RET(TSDB_CODE_SUCCESS);
}

int32_t flttMakeLogicNode(SNode **pNode, ELogicConditionType opType, SNode **nodeList, int32_t nodeNum) {
  SNode               *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, &node);
  if (NULL == node) {
    FLT_ERR_RET(code);
  }
  SLogicConditionNode *onode = (SLogicConditionNode *)node;
  onode->condType = opType;
  onode->node.resType.type = TSDB_DATA_TYPE_BOOL;
  onode->node.resType.bytes = sizeof(bool);

  onode->pParameterList = NULL;
  code = nodesMakeList(&onode->pParameterList);
  for (int32_t i = 0; i < nodeNum; ++i) {
    FLT_ERR_RET(nodesListAppend(onode->pParameterList, nodeList[i]));
  }

  *pNode = (SNode *)onode;
  FLT_RET(TSDB_CODE_SUCCESS);
}

int32_t flttMakeLogicNodeFromList(SNode **pNode, ELogicConditionType opType, SNodeList *nodeList) {
  SNode               *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, &node);
  if (NULL == node) {
    FLT_ERR_RET(code);
  }
  SLogicConditionNode *onode = (SLogicConditionNode *)node;
  onode->condType = opType;
  onode->node.resType.type = TSDB_DATA_TYPE_BOOL;
  onode->node.resType.bytes = sizeof(bool);

  onode->pParameterList = nodeList;

  *pNode = (SNode *)onode;
  FLT_RET(TSDB_CODE_SUCCESS);
}

int32_t flttMakeListNode(SNode **pNode, SNodeList *list, int32_t resType) {
  SNode         *node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_NODE_LIST, &node);
  if (NULL == node) {
    FLT_ERR_RET(code);
  }
  SNodeListNode *lnode = (SNodeListNode *)node;
  lnode->node.resType.type = resType;
  lnode->pNodeList = list;

  *pNode = (SNode *)lnode;
  FLT_RET(TSDB_CODE_SUCCESS);
}

void initScalarParam(SScalarParam *pParam) {
  memset(pParam, 0, sizeof(SScalarParam));
  pParam->colAlloced = true;
}

void flttCheckBlobPredicate(EOperatorType opType, const char *pRightText, const bool *expected) {
  SCOPED_TRACE(operatorTypeStr(opType));
  const char  *values[] = {"beta", "alpha", "beta", "null"};
  char         blobValues[128] = {0};
  char        *pValue = blobValues;
  SNode       *pLeft = NULL;
  SNode       *pRight = NULL;
  SNode       *pOperator = NULL;
  SSDataBlock *pBlock = NULL;

  for (int32_t i = 0; i < 4; ++i) {
    size_t len = strlen(values[i]);
    blobDataSetLen(pValue, len);
    (void)memcpy(blobDataVal(pValue), values[i], len);
    pValue += blobDataTLen(pValue);
  }

  int32_t code = flttMakeColumnNode(&pLeft, &pBlock, TSDB_DATA_TYPE_BLOB, 32, 4, blobValues);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  SColumnInfoData *pColumn = (SColumnInfoData *)taosArrayGetLast(pBlock->pDataBlock);
  ASSERT_NE(pColumn, nullptr);
  ASSERT_EQ(colDataSetVal(pColumn, 3, NULL, true), TSDB_CODE_SUCCESS);

  if (NULL != pRightText) {
    char   rightValue[64] = {0};
    size_t rightLen = strlen(pRightText);
    blobDataSetLen(rightValue, rightLen);
    (void)memcpy(blobDataVal(rightValue), pRightText, rightLen);
    ASSERT_EQ(flttMakeValueNode(&pRight, TSDB_DATA_TYPE_BLOB, rightValue), TSDB_CODE_SUCCESS);
  }

  ASSERT_EQ(flttMakeOpNode(&pOperator, opType, TSDB_DATA_TYPE_BOOL, pLeft, pRight), TSDB_CODE_SUCCESS);

  SFilterInfo *pFilter = NULL;
  ASSERT_EQ(filterInitFromNode(pOperator, &pFilter, 0, NULL), TSDB_CODE_SUCCESS);

  SFilterColumnParam param = {(int32_t)taosArrayGetSize(pBlock->pDataBlock), pBlock->pDataBlock};
  ASSERT_EQ(filterSetDataFromSlotId(pFilter, &param), TSDB_CODE_SUCCESS);

  SColumnInfoData *pRowResult = NULL;
  int32_t          resultStatus = 0;
  ASSERT_EQ(filterExecute(pFilter, pBlock, &pRowResult, NULL, taosArrayGetSize(pBlock->pDataBlock), &resultStatus),
            TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < 4; ++i) {
    bool actual = FILTER_RESULT_ALL_QUALIFIED == resultStatus ||
                  (FILTER_RESULT_PARTIAL_QUALIFIED == resultStatus && *((bool *)colDataGetData(pRowResult, i)));
    ASSERT_EQ(actual, expected[i]);
  }

  colDataDestroy(pRowResult);
  taosMemoryFree(pRowResult);
  filterFreeInfo(pFilter);
  nodesDestroyNode(pOperator);
  blockDataDestroy(pBlock);
}

void flttCheckBlobFilterInit(EOperatorType opType, const char *pRightText) {
  char   rightValue[64] = {0};
  SNode *pLeft = NULL;
  SNode *pRight = NULL;
  SNode *pOperator = NULL;

  size_t rightLen = strlen(pRightText);
  blobDataSetLen(rightValue, rightLen);
  (void)memcpy(blobDataVal(rightValue), pRightText, rightLen);

  ASSERT_EQ(flttMakeColumnNode(&pLeft, NULL, TSDB_DATA_TYPE_BLOB, 32, 0, NULL), TSDB_CODE_SUCCESS);
  ASSERT_EQ(flttMakeValueNode(&pRight, TSDB_DATA_TYPE_BLOB, rightValue), TSDB_CODE_SUCCESS);
  ASSERT_EQ(flttMakeOpNode(&pOperator, opType, TSDB_DATA_TYPE_BOOL, pLeft, pRight), TSDB_CODE_SUCCESS);

  SFilterInfo *pFilter = NULL;
  ASSERT_EQ(filterInitFromNode(pOperator, &pFilter, 0, NULL), TSDB_CODE_SUCCESS);

  filterFreeInfo(pFilter);
  nodesDestroyNode(pOperator);
}

void flttCheckBlobLogicalPredicate(EOperatorType compareType, EOperatorType nullType,
                                   ELogicConditionType conditionType) {
  char   rightValue[64] = {0};
  SNode *pCompareLeft = NULL;
  SNode *pCompareRight = NULL;
  SNode *pCompare = NULL;
  SNode *pNullLeft = NULL;
  SNode *pNull = NULL;
  SNode *pLogic = NULL;

  const char *rightText = "{}";
  size_t      rightLen = strlen(rightText);
  blobDataSetLen(rightValue, rightLen);
  (void)memcpy(blobDataVal(rightValue), rightText, rightLen);

  ASSERT_EQ(flttMakeColumnNode(&pCompareLeft, NULL, TSDB_DATA_TYPE_BLOB, 32, 0, NULL), TSDB_CODE_SUCCESS);
  ASSERT_EQ(flttMakeValueNode(&pCompareRight, TSDB_DATA_TYPE_BLOB, rightValue), TSDB_CODE_SUCCESS);
  ASSERT_EQ(flttMakeOpNode(&pCompare, compareType, TSDB_DATA_TYPE_BOOL, pCompareLeft, pCompareRight),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(flttMakeColumnNode(&pNullLeft, NULL, TSDB_DATA_TYPE_BLOB, 32, 0, NULL), TSDB_CODE_SUCCESS);
  ASSERT_EQ(flttMakeOpNode(&pNull, nullType, TSDB_DATA_TYPE_BOOL, pNullLeft, NULL), TSDB_CODE_SUCCESS);

  SNode *parameters[] = {pCompare, pNull};
  ASSERT_EQ(flttMakeLogicNode(&pLogic, conditionType, parameters, 2), TSDB_CODE_SUCCESS);

  SFilterInfo *pFilter = NULL;
  ASSERT_EQ(filterInitFromNode(pLogic, &pFilter, 0, NULL), TSDB_CODE_SUCCESS);

  filterFreeInfo(pFilter);
  nodesDestroyNode(pLogic);
}

void flttCheckBlobInPredicate(EOperatorType opType, const bool *expected) {
  const char  *values[] = {"beta", "alpha", "beta", "null"};
  const char  *setValues[] = {"beta", "gamma"};
  char         blobValues[128] = {0};
  char        *pValue = blobValues;
  SNode       *pLeft = NULL;
  SNode       *pRight = NULL;
  SNode       *pOperator = NULL;
  SNodeList   *pList = NULL;
  SSDataBlock *pBlock = NULL;

  for (int32_t i = 0; i < 4; ++i) {
    size_t len = strlen(values[i]);
    blobDataSetLen(pValue, len);
    (void)memcpy(blobDataVal(pValue), values[i], len);
    pValue += blobDataTLen(pValue);
  }

  ASSERT_EQ(flttMakeColumnNode(&pLeft, &pBlock, TSDB_DATA_TYPE_BLOB, 32, 4, blobValues), TSDB_CODE_SUCCESS);
  SColumnInfoData *pColumn = (SColumnInfoData *)taosArrayGetLast(pBlock->pDataBlock);
  ASSERT_NE(pColumn, nullptr);
  ASSERT_EQ(colDataSetVal(pColumn, 3, NULL, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeList(&pList), TSDB_CODE_SUCCESS);

  for (const char *pSetValue : setValues) {
    char   blobValue[64] = {0};
    size_t len = strlen(pSetValue);
    blobDataSetLen(blobValue, len);
    (void)memcpy(blobDataVal(blobValue), pSetValue, len);

    SNode *pSetNode = NULL;
    ASSERT_EQ(flttMakeValueNode(&pSetNode, TSDB_DATA_TYPE_BLOB, blobValue), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesListStrictAppend(pList, pSetNode), TSDB_CODE_SUCCESS);
  }
  ASSERT_EQ(flttMakeListNode(&pRight, pList, TSDB_DATA_TYPE_BLOB), TSDB_CODE_SUCCESS);
  ASSERT_EQ(flttMakeOpNode(&pOperator, opType, TSDB_DATA_TYPE_BOOL, pLeft, pRight), TSDB_CODE_SUCCESS);

  SFilterInfo *pFilter = NULL;
  ASSERT_EQ(filterInitFromNode(pOperator, &pFilter, 0, NULL), TSDB_CODE_SUCCESS);
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(pBlock->pDataBlock), pBlock->pDataBlock};
  ASSERT_EQ(filterSetDataFromSlotId(pFilter, &param), TSDB_CODE_SUCCESS);

  SColumnInfoData *pRowResult = NULL;
  int32_t          resultStatus = 0;
  ASSERT_EQ(filterExecute(pFilter, pBlock, &pRowResult, NULL, taosArrayGetSize(pBlock->pDataBlock), &resultStatus),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(resultStatus, FILTER_RESULT_PARTIAL_QUALIFIED);
  for (int32_t i = 0; i < 4; ++i) {
    ASSERT_EQ(*((bool *)colDataGetData(pRowResult, i)), expected[i]);
  }

  colDataDestroy(pRowResult);
  taosMemoryFree(pRowResult);
  filterFreeInfo(pFilter);
  nodesDestroyNode(pOperator);
  blockDataDestroy(pBlock);
}

}  // namespace

TEST(columnTest, blob_column_supported_predicates) {
  ASSERT_EQ(InitRegexCache(), TSDB_CODE_SUCCESS);
  const bool equalExpected[] = {true, false, true, false};
  const bool notEqualExpected[] = {false, true, false, false};
  const bool greaterExpected[] = {true, false, true, false};
  const bool greaterEqualExpected[] = {true, true, true, false};
  const bool lowerExpected[] = {false, true, false, false};
  const bool lowerEqualExpected[] = {true, true, true, false};
  const bool likeExpected[] = {true, false, true, false};
  const bool notLikeExpected[] = {false, true, false, false};
  const bool isNullExpected[] = {false, false, false, true};
  const bool isNotNullExpected[] = {true, true, true, false};

  flttCheckBlobPredicate(OP_TYPE_EQUAL, "beta", equalExpected);
  flttCheckBlobPredicate(OP_TYPE_NOT_EQUAL, "beta", notEqualExpected);
  flttCheckBlobPredicate(OP_TYPE_GREATER_THAN, "alpha", greaterExpected);
  flttCheckBlobPredicate(OP_TYPE_GREATER_EQUAL, "alpha", greaterEqualExpected);
  flttCheckBlobPredicate(OP_TYPE_LOWER_THAN, "beta", lowerExpected);
  flttCheckBlobPredicate(OP_TYPE_LOWER_EQUAL, "beta", lowerEqualExpected);
  flttCheckBlobPredicate(OP_TYPE_LIKE, "%et%", likeExpected);
  flttCheckBlobPredicate(OP_TYPE_NOT_LIKE, "%et%", notLikeExpected);
  flttCheckBlobPredicate(OP_TYPE_MATCH, "^be.*", likeExpected);
  flttCheckBlobPredicate(OP_TYPE_NMATCH, "^be.*", notLikeExpected);
  flttCheckBlobPredicate(OP_TYPE_IS_NULL, NULL, isNullExpected);
  flttCheckBlobPredicate(OP_TYPE_IS_NOT_NULL, NULL, isNotNullExpected);
  DestoryThreadLocalRegComp();
  DestroyRegexCache();
}

TEST(columnTest, null_value_uses_scalar_filter) {
  int32_t values[] = {1, 2};
  SNode *pColumn = NULL;
  SNode *pValue = NULL;
  SNode *pPredicate = NULL;
  SFilterInfo *pFilter = NULL;
  SSDataBlock *pBlock = NULL;

  ASSERT_EQ(flttMakeColumnNode(&pColumn, &pBlock, TSDB_DATA_TYPE_INT, sizeof(int32_t), 2, values),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_VALUE, &pValue), TSDB_CODE_SUCCESS);
  ((SValueNode *)pValue)->node.resType = ((SExprNode *)pColumn)->resType;
  ((SValueNode *)pValue)->isNull = true;
  ASSERT_EQ(flttMakeOpNode(&pPredicate, OP_TYPE_EQUAL, TSDB_DATA_TYPE_BOOL, pColumn, pValue), TSDB_CODE_SUCCESS);

  ASSERT_EQ(filterInitFromNode(pPredicate, &pFilter, 0, NULL), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(pFilter->scalarMode);
  SFilterColumnParam param = {(int32_t)taosArrayGetSize(pBlock->pDataBlock), pBlock->pDataBlock};
  ASSERT_EQ(filterSetDataFromSlotId(pFilter, &param), TSDB_CODE_SUCCESS);
  SColumnInfoData *pResult = NULL;
  int32_t resultStatus = 0;
  ASSERT_EQ(filterExecute(pFilter, pBlock, &pResult, NULL, param.numOfCols, &resultStatus), TSDB_CODE_SUCCESS);
  EXPECT_EQ(resultStatus, FILTER_RESULT_NONE_QUALIFIED);

  colDataDestroy(pResult);
  taosMemoryFree(pResult);
  filterFreeInfo(pFilter);
  nodesDestroyNode(pPredicate);
  blockDataDestroy(pBlock);
}

TEST(columnTest, blob_column_comparator_scope) {
  struct SBlobComparatorCase {
    EOperatorType opType;
    __compar_fn_t  compareFunc;
  };
  const SBlobComparatorCase supported[] = {
      {OP_TYPE_EQUAL, compareBlobVal},
      {OP_TYPE_NOT_EQUAL, compareBlobVal},
      {OP_TYPE_GREATER_THAN, compareBlobVal},
      {OP_TYPE_GREATER_EQUAL, compareBlobVal},
      {OP_TYPE_LOWER_THAN, compareBlobVal},
      {OP_TYPE_LOWER_EQUAL, compareBlobVal},
      {OP_TYPE_IS_NULL, compareBlobVal},
      {OP_TYPE_IS_NOT_NULL, compareBlobVal},
      {OP_TYPE_LIKE, compareBlobPatternMatch},
      {OP_TYPE_NOT_LIKE, compareBlobPatternNMatch},
      {OP_TYPE_MATCH, nullptr},
      {OP_TYPE_NMATCH, nullptr},
      {OP_TYPE_IN, compareChkInBlob},
      {OP_TYPE_NOT_IN, compareChkNotInBlob},
  };
  for (const SBlobComparatorCase &testCase : supported) {
    __compar_fn_t compareFunc = NULL;
    ASSERT_EQ(filterGetCompFunc(&compareFunc, TSDB_DATA_TYPE_BLOB, testCase.opType), TSDB_CODE_SUCCESS);
    ASSERT_EQ(compareFunc, testCase.compareFunc);
  }

  const EOperatorType unsupported[] = {OP_TYPE_ADD, OP_TYPE_JSON_GET_VALUE, OP_TYPE_JSON_CONTAINS};
  for (EOperatorType opType : unsupported) {
    __compar_fn_t compareFunc = NULL;
    ASSERT_EQ(filterGetCompFunc(&compareFunc, TSDB_DATA_TYPE_BLOB, opType), TSDB_CODE_BLOB_OP_NOT_SUPPORTED);
    ASSERT_EQ(compareFunc, nullptr);
  }
}

TEST(columnTest, incompatible_comparator_types_are_rejected) {
  EXPECT_EQ(nullptr,
            filterGetCompFuncEx(TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BLOB,
                                OP_TYPE_EQUAL));
  EXPECT_EQ(nullptr,
            filterGetCompFuncEx(TSDB_DATA_TYPE_BOOL,
                                TSDB_DATA_TYPE_MEDIUMBLOB, OP_TYPE_EQUAL));
  EXPECT_NE(nullptr,
            filterGetCompFuncEx(TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BOOL,
                                OP_TYPE_EQUAL));
  EXPECT_EQ(nullptr,
            filterGetCompFuncEx(TSDB_DATA_TYPE_JSON, TSDB_DATA_TYPE_INT,
                                OP_TYPE_EQUAL));
}

TEST(columnTest, blob_column_in_predicates) {
  const bool inExpected[] = {true, false, true, false};
  const bool notInExpected[] = {false, true, false, false};

  flttCheckBlobInPredicate(OP_TYPE_IN, inExpected);
  flttCheckBlobInPredicate(OP_TYPE_NOT_IN, notInExpected);
}

TEST(columnTest, blob_like_accepts_pattern_above_varchar_limit) {
  char left[BLOBSTR_HEADER_SIZE + 1] = {0};
  blobDataSetLen(left, 1);
  blobDataVal(left)[0] = 'a';

  int32_t patternLen = TSDB_MAX_FIELD_LEN + 1;
  char   *pPattern = (char *)taosMemoryCalloc(1, BLOBSTR_HEADER_SIZE + patternLen);
  ASSERT_NE(pPattern, nullptr);
  blobDataSetLen(pPattern, patternLen);
  (void)memset(blobDataVal(pPattern), '%', patternLen);
  blobDataVal(pPattern)[patternLen / 2] = 'a';

  ASSERT_EQ(compareBlobPatternMatch(left, pPattern), 0);
  taosMemoryFree(pPattern);
}

TEST(columnTest, blob_like_rejects_pattern_above_blob_limit) {
  char left[BLOBSTR_HEADER_SIZE + 1] = {0};
  blobDataSetLen(left, 1);
  blobDataVal(left)[0] = 'a';

  int32_t patternLen = TSDB_MAX_BLOB_LEN - BLOBSTR_HEADER_SIZE + 1;
  char   *pPattern = (char *)taosMemoryCalloc(1, BLOBSTR_HEADER_SIZE + patternLen);
  ASSERT_NE(pPattern, nullptr);
  blobDataSetLen(pPattern, patternLen);
  (void)memset(blobDataVal(pPattern), '%', patternLen);

  ASSERT_EQ(compareBlobPatternMatch(left, pPattern), 1);
  taosMemoryFree(pPattern);
}

TEST(columnTest, blob_column_default_hash_uses_full_value) {
  char first[16] = {0};
  char second[16] = {0};
  blobDataSetLen(first, 4);
  blobDataSetLen(second, 4);
  (void)memcpy(blobDataVal(first), "beta", 4);
  (void)memcpy(blobDataVal(second), "zeta", 4);

  _hash_fn_t hashFunc = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BLOB);
  ASSERT_NE(hashFunc(first, blobDataTLen(first)), hashFunc(second, blobDataTLen(second)));
}

TEST(columnTest, blob_column_debug_dump_predicates) {
  QDebugFlagGuard debugFlagGuard(DEBUG_DEBUG);

  const bool inExpected[] = {true, false, true, false};

  flttCheckBlobFilterInit(OP_TYPE_EQUAL, "beta");
  flttCheckBlobFilterInit(OP_TYPE_LIKE, "%et%");
  flttCheckBlobFilterInit(OP_TYPE_MATCH, "^be.*");
  flttCheckBlobInPredicate(OP_TYPE_IN, inExpected);
  flttCheckBlobLogicalPredicate(OP_TYPE_EQUAL, OP_TYPE_IS_NULL, LOGIC_COND_TYPE_OR);
  flttCheckBlobLogicalPredicate(OP_TYPE_NOT_EQUAL, OP_TYPE_IS_NOT_NULL, LOGIC_COND_TYPE_AND);
}

TEST(timerangeTest, greater) {
  SNode       *pcol = NULL, *pval = NULL, *opNode1 = NULL;
  bool         eRes[5] = {false, false, true, true, true};
  SScalarParam res;
  initScalarParam(&res);

  int32_t code = TSDB_CODE_SUCCESS;
  int64_t tsmall = 222, tbig = 333;
  code = flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, 0);
  code = flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tsmall);
  ASSERT_EQ(code, 0);
  code = flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  ASSERT_EQ(code, 0);

  // SFilterInfo *filter = NULL;
  // int32_t code = filterInitFromNode(opNode1, &filter, FLT_OPTION_NO_REWRITE|FLT_OPTION_TIMESTAMP);
  // ASSERT_EQ(code, 0);
  STimeWindow win = {0};
  bool        isStrict = false;
  code = filterGetTimeRange(opNode1, &win, &isStrict, NULL);
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
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t tsmall = 222, tbig = 333;
  code = flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, 0);
  code = flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tsmall);
  ASSERT_EQ(code, 0);
  code = flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  ASSERT_EQ(code, 0);
  code = flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, 0);
  code = flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tbig);
  ASSERT_EQ(code, 0);
  code = flttMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  ASSERT_EQ(code, 0);
  SNode *list[2] = {0};
  list[0] = opNode1;
  list[1] = opNode2;

  code = flttMakeLogicNode(&logicNode, LOGIC_COND_TYPE_AND, list, 2);
  ASSERT_EQ(code, 0);

  // SFilterInfo *filter = NULL;
  // int32_t code = filterInitFromNode(logicNode, &filter, FLT_OPTION_NO_REWRITE|FLT_OPTION_TIMESTAMP);
  // ASSERT_EQ(code, 0);
  STimeWindow win = {0};
  bool        isStrict = false;
  code = filterGetTimeRange(logicNode, &win, &isStrict, NULL);
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
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t tsmall = 222, tbig = 333;
  code = flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, 0);
  code = flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tsmall);
  ASSERT_EQ(code, 0);
  code = flttMakeOpNode(&opNode1, OP_TYPE_GREATER_EQUAL, TSDB_DATA_TYPE_BOOL, pcol, pval);
  ASSERT_EQ(code, 0);
  code = flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, 0);
  code = flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tbig);
  ASSERT_EQ(code, 0);
  code = flttMakeOpNode(&opNode2, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, pcol, pval);
  ASSERT_EQ(code, 0);
  SNode *list[2] = {0};
  list[0] = opNode1;
  list[1] = opNode2;

  code = flttMakeLogicNode(&logicNode, LOGIC_COND_TYPE_AND, list, 2);
  ASSERT_EQ(code, 0);

  // SFilterInfo *filter = NULL;
  // int32_t code = filterInitFromNode(logicNode, &filter, FLT_OPTION_NO_REWRITE|FLT_OPTION_TIMESTAMP);
  // ASSERT_EQ(code, 0);
  STimeWindow win = {0};
  bool        isStrict = false;
  code = filterGetTimeRange(logicNode, &win, &isStrict, NULL);
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
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t tsmall1 = 222, tbig1 = 333;
  int64_t tsmall2 = 444, tbig2 = 555;
  SNode  *list[2] = {0};

  code = flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, 0);
  code = flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tsmall1);
  ASSERT_EQ(code, 0);
  code = flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  ASSERT_EQ(code, 0);
  code = flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, 0);
  code = flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tbig1);
  ASSERT_EQ(code, 0);
  code = flttMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  ASSERT_EQ(code, 0);
  list[0] = opNode1;
  list[1] = opNode2;

  code = flttMakeLogicNode(&logicNode1, LOGIC_COND_TYPE_AND, list, 2);
  ASSERT_EQ(code, 0);

  code = flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, 0);
  code = flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tsmall2);
  ASSERT_EQ(code, 0);
  code = flttMakeOpNode(&opNode1, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  ASSERT_EQ(code, 0);
  code = flttMakeColumnNode(&pcol, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL);
  ASSERT_EQ(code, 0);
  code = flttMakeValueNode(&pval, TSDB_DATA_TYPE_TIMESTAMP, &tbig2);
  ASSERT_EQ(code, 0);
  code = flttMakeOpNode(&opNode2, OP_TYPE_LOWER_THAN, TSDB_DATA_TYPE_BOOL, pcol, pval);
  ASSERT_EQ(code, 0);
  list[0] = opNode1;
  list[1] = opNode2;

  code = flttMakeLogicNode(&logicNode2, LOGIC_COND_TYPE_AND, list, 2);
  ASSERT_EQ(code, 0);

  list[0] = logicNode1;
  list[1] = logicNode2;
  code = flttMakeLogicNode(&logicNode1, LOGIC_COND_TYPE_OR, list, 2);
  ASSERT_EQ(code, 0);

  // SFilterInfo *filter = NULL;
  // int32_t code = filterInitFromNode(logicNode, &filter, FLT_OPTION_NO_REWRITE|FLT_OPTION_TIMESTAMP);
  // ASSERT_EQ(code, 0);
  STimeWindow win = {0};
  bool        isStrict = false;
  code = filterGetTimeRange(logicNode1, &win, &isStrict, NULL);
  ASSERT_EQ(isStrict, false);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(win.skey, tsmall1 + 1);
  ASSERT_EQ(win.ekey, tbig2 - 1);
  // filterFreeInfo(filter);
  nodesDestroyNode(logicNode1);
}

static SNode *flttRangePredicate(int16_t colId, EOperatorType op, int64_t value,
                                int8_t dataType = TSDB_DATA_TYPE_BIGINT, bool isPrimTs = false) {
  SNode *col = NULL, *val = NULL, *predicate = NULL;
  int64_t datum = value;
  if (IS_INVALID_TYPE(dataType)) {
    ADD_FAILURE() << "invalid range predicate type " << static_cast<int>(dataType);
    return NULL;
  }
  if (flttMakeColumnNode(&col, NULL, dataType, tDataTypes[dataType].bytes, 0, NULL) != 0) {
    ADD_FAILURE() << "range column construction failed: colId=" << colId;
    return NULL;
  }
  if (flttMakeValueNode(&val, dataType, &datum) != 0) {
    ADD_FAILURE() << "range value construction failed: colId=" << colId << ", op=" << op;
    nodesDestroyNode(col);
    return NULL;
  }
  if (flttMakeOpNode(&predicate, op, TSDB_DATA_TYPE_BOOL, col, val) != 0) {
    ADD_FAILURE() << "range operator construction failed: colId=" << colId << ", op=" << op;
    // On failure, flttMakeOpNode did not create a parent to own the operands.
    nodesDestroyNode(col);
    nodesDestroyNode(val);
    return NULL;
  }
  ((SColumnNode *)col)->colId = colId;
  ((SColumnNode *)col)->isPrimTs = isPrimTs;
  ((SColumnNode *)col)->colType = COLUMN_TYPE_COLUMN;
  // SMA pruning uses colId; slotId is a block-column position, not a column ID.
  if (TSDB_DATA_TYPE_TIMESTAMP == dataType) {
    if (((SColumnNode *)col)->node.resType.bytes != sizeof(int64_t) ||
        ((SValueNode *)val)->node.resType.type != TSDB_DATA_TYPE_TIMESTAMP) {
      ADD_FAILURE() << "timestamp range predicate metadata mismatch";
      nodesDestroyNode(predicate);
      return NULL;
    }
    ((SColumnNode *)col)->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
    ((SValueNode *)val)->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  }
  // assignVal initializes the value buffer; range extraction reads datum.i.
  ((SValueNode *)val)->datum.i = datum;
  return predicate;
}

static SNode *flttRangeLogic(ELogicConditionType type, SNode *left, SNode *right) {
  if (NULL == left || NULL == right) {
    ADD_FAILURE() << "range logic operand construction failed: left=" << (left != NULL)
                  << ", right=" << (right != NULL);
    nodesDestroyNode(left);
    nodesDestroyNode(right);
    return NULL;
  }
  SNode *logic = NULL;
  if (nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, &logic) != TSDB_CODE_SUCCESS) {
    ADD_FAILURE() << "range logic node construction failed";
    nodesDestroyNode(left);
    nodesDestroyNode(right);
    return NULL;
  }
  SLogicConditionNode *condition = (SLogicConditionNode *)logic;
  condition->condType = type;
  condition->node.resType.type = TSDB_DATA_TYPE_BOOL;
  condition->node.resType.bytes = sizeof(bool);
  if (nodesMakeList(&condition->pParameterList) != TSDB_CODE_SUCCESS) {
    ADD_FAILURE() << "range logic list construction failed";
    // The operands have not been appended, so the parent does not own them yet.
    nodesDestroyNode(left);
    nodesDestroyNode(right);
    nodesDestroyNode(logic);
    return NULL;
  }
  if (nodesListAppend(condition->pParameterList, left) != TSDB_CODE_SUCCESS) {
    ADD_FAILURE() << "range logic left operand append failed";
    nodesDestroyNode(logic);
    nodesDestroyNode(left);
    nodesDestroyNode(right);
    return NULL;
  }
  // The parent owns and frees left after the first append; right is still independent.
  if (nodesListAppend(condition->pParameterList, right) != TSDB_CODE_SUCCESS) {
    ADD_FAILURE() << "range logic right operand append failed";
    nodesDestroyNode(logic);
    nodesDestroyNode(right);
    return NULL;
  }
  return logic;
}

static SNode *flttTimestampPredicate(EOperatorType op, int64_t value) {
  return flttRangePredicate(PRIMARYKEY_TIMESTAMP_COL_ID, op, value, TSDB_DATA_TYPE_TIMESTAMP, true);
}

static SNode *flttDecimal64Predicate(int16_t colId, int64_t value) {
  // The value is already quantized for scale zero.
  SDataType decimalType = {};
  decimalType.type = TSDB_DATA_TYPE_DECIMAL64;
  decimalType.precision = 18;
  decimalType.scale = 0;
  decimalType.bytes = 8;
  SNode *col = NULL, *val = NULL, *predicate = NULL;
  if (flttMakeColumnNode(&col, NULL, decimalType.type, decimalType.bytes, 0, NULL) != 0) {
    ADD_FAILURE() << "decimal column construction failed: colId=" << colId;
    return NULL;
  }
  ((SColumnNode *)col)->colId = colId;
  ((SColumnNode *)col)->colType = COLUMN_TYPE_COLUMN;
  ((SColumnNode *)col)->node.resType = decimalType;
  if (flttMakeValueNode(&val, decimalType.type, &value) != 0) {
    ADD_FAILURE() << "decimal value construction failed: colId=" << colId;
    nodesDestroyNode(col);
    return NULL;
  }
  ((SValueNode *)val)->node.resType = decimalType;
  ((SValueNode *)val)->datum.i = value;
  if (flttMakeOpNode(&predicate, OP_TYPE_EQUAL, TSDB_DATA_TYPE_BOOL, col, val) != 0) {
    ADD_FAILURE() << "decimal operator construction failed: colId=" << colId;
    nodesDestroyNode(col);
    nodesDestroyNode(val);
    return NULL;
  }
  return predicate;
}

static SNode *flttMixedValuePredicate(int16_t colId, int8_t columnType, int8_t valueType,
                                     const char *literal) {
  SNode *predicate = flttRangePredicate(colId, OP_TYPE_EQUAL, 1, columnType);
  if (NULL == predicate) return NULL;
  SValueNode *value = (SValueNode *)((SOperatorNode *)predicate)->pRight;
  value->node.resType.type = valueType;
  if (valueType == TSDB_DATA_TYPE_VARCHAR || valueType == TSDB_DATA_TYPE_NCHAR) {
    value->datum.p = NULL;
    value->literal = taosStrdup(literal);
    if (NULL == value->literal) {
      nodesDestroyNode(predicate);
      return NULL;
    }
  } else if (valueType == TSDB_DATA_TYPE_DECIMAL64) {
    value->node.resType.precision = 18;
    value->node.resType.scale = 0;
    value->datum.i = 1;
  }
  return predicate;
}

TEST(scalarOrRangeTest, mixedNumericLiteralsFallBackConservatively) {
  struct Case { int8_t columnType; int8_t valueType; const char *literal; };
  const Case cases[] = {
      {TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_VARCHAR, "1.5"},
      {TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_VARCHAR, "1.5e0"},
      {TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_NCHAR, "1.5"},
      {TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_DECIMAL64, NULL},
  };
  for (const Case &test : cases) {
    SCOPED_TRACE(::testing::Message() << "column=" << static_cast<int>(test.columnType)
                                      << " value=" << static_cast<int>(test.valueType));
    SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                     flttMixedValuePredicate(3, test.columnType, test.valueType, test.literal),
                                     flttRangePredicate(4, OP_TYPE_EQUAL, 2));
    ASSERT_NE(condition, nullptr);
    SFilterInfo *filter = NULL;
    ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
    ASSERT_NE(filter, nullptr);
    SColumnDataAgg stats[2] = {{0}, {0}};
    stats[0].colId = 3;
    stats[1].colId = 4;
    if (test.columnType == TSDB_DATA_TYPE_DOUBLE || test.columnType == TSDB_DATA_TYPE_FLOAT) {
      double value = 1.5;
      memcpy(&stats[0].min, &value, sizeof(value));
      memcpy(&stats[0].max, &value, sizeof(value));
    } else {
      stats[0].min = stats[0].max = 1;
    }
    stats[1].min = stats[1].max = 0;
    bool keep = false;
    ASSERT_EQ(filterRangeExecute(filter, stats, 2, 100, &keep), TSDB_CODE_SUCCESS);
    EXPECT_TRUE(keep);
    filterFreeInfo(filter);
    nodesDestroyNode(condition);
  }
}

TEST(scalarOrRangeTest, wideOrWithIndependentAnd) {
  SNode *orNode = NULL;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, &orNode), TSDB_CODE_SUCCESS);
  SLogicConditionNode *orCondition = (SLogicConditionNode *)orNode;
  orCondition->condType = LOGIC_COND_TYPE_OR;
  orCondition->node.resType.type = TSDB_DATA_TYPE_BOOL;
  orCondition->node.resType.bytes = sizeof(bool);
  ASSERT_EQ(nodesMakeList(&orCondition->pParameterList), TSDB_CODE_SUCCESS);
  for (int16_t colId = 3; colId < 123; ++colId) {
    SNode *arm = flttRangePredicate(colId, OP_TYPE_EQUAL, 0);
    ASSERT_NE(arm, nullptr);
    ASSERT_EQ(nodesListAppend(orCondition->pParameterList, arm), TSDB_CODE_SUCCESS);
  }
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_AND, orNode,
                                   flttRangePredicate(123, OP_TYPE_EQUAL, 0));
  ASSERT_NE(condition, nullptr);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  SColumnDataAgg stats[121] = {{0}};
  for (int32_t i = 0; i < 121; ++i) {
    stats[i].colId = i + 3;
    stats[i].min = 1;
    stats[i].max = 2;
  }
  bool keep = true;
  for (int32_t i = 0; i < 1000; ++i) {
    ASSERT_EQ(filterRangeExecute(filter, stats, 121, 100, &keep), TSDB_CODE_SUCCESS);
    ASSERT_FALSE(keep);
  }
  stats[120].min = 0;
  ASSERT_EQ(filterRangeExecute(filter, stats, 121, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(keep);
  stats[0].min = 0;
  ASSERT_EQ(filterRangeExecute(filter, stats, 121, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  filterFreeInfo(filter);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, decimal64SmaUsesDecimalBounds) {
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                   flttDecimal64Predicate(3, 1),
                                   flttDecimal64Predicate(3, 2));
  ASSERT_NE(condition, nullptr);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  SColumnDataAgg stat = {0};
  stat.colId = 3 | DECIMAL_AGG_FLAG;
  // DECIMAL64 uses the low word of each decimal128 SMA slot; this column has scale 0.
  stat.decimal128Min[0] = 5;
  stat.decimal128Max[0] = 5;
  bool keep = true;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(keep);
  // tGetColumnDataAgg clears the decimal flag after decoding the SMA.
  stat.colId = 3;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(keep);
  stat.colId |= DECIMAL_AGG_FLAG;
  stat.decimal128Min[0] = 1;
  stat.decimal128Max[0] = 1;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  stat.decimal128Min[0] = 5;
  stat.decimal128Max[0] = 5;
  stat.overflow = 1;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  stat.overflow = 0;
  stat.decimal128Min[0] = 6;
  stat.decimal128Max[0] = 5;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  stat.decimal128Min[0] = 5;
  stat.decimal128Max[0] = 5;
  stat.decimal128Min[1] = UINT64_MAX;
  stat.decimal128Max[1] = 0;
  // DECIMAL64 ignores these deliberately contradictory high words.
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(keep);
  filterFreeInfo(filter);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, sameColumnGapAndBoundary) {
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                   flttRangePredicate(3, OP_TYPE_LOWER_THAN, 10),
                                   flttRangePredicate(3, OP_TYPE_GREATER_THAN, 20));
  ASSERT_NE(condition, nullptr);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  SColumnDataAgg stat = {0};
  stat.colId = 3;
  stat.numOfNull = 0;
  stat.min = 10;
  stat.max = 20;
  bool keep = true;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(keep);
  stat.min = 9;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  stat.min = 20;
  stat.max = 30;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  stat.min = 30;
  stat.max = 20;
  // Invalid SMA bounds must conservatively retain the block.
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  // The unflagged integer view [10,20] is prunable; the flagged decimal view
  // has invalid bounds and must retain the block instead.
  stat.max = 20;
  stat.decimal128Max[0] = 10;  // Also sets the integer min to 10.
  stat.decimal128Min[0] = 15;
  stat.colId = 3;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(keep);
  stat.colId |= DECIMAL_AGG_FLAG;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  stat.colId = -1;  // Missing-SMA sentinel must not match column 3.
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  filterFreeInfo(filter);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, crossColumnAndNested) {
  SNode *orCondition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                     flttRangePredicate(3, OP_TYPE_EQUAL, 1),
                                     flttRangePredicate(4, OP_TYPE_EQUAL, 2));
  ASSERT_NE(orCondition, nullptr);
  SNode *right = flttRangePredicate(5, OP_TYPE_GREATER_THAN, 10);
  if (NULL == right) {
    nodesDestroyNode(orCondition);
    FAIL() << "right AND predicate construction failed";
  }
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_AND, orCondition, right);
  ASSERT_NE(condition, nullptr);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  SColumnDataAgg stats[3] = {{0}, {0}, {0}};
  stats[0].colId = 3; stats[0].min = 3; stats[0].max = 5; stats[0].numOfNull = 0;
  stats[1].colId = 4; stats[1].min = 4; stats[1].max = 6; stats[1].numOfNull = 0;
  stats[2].colId = 5; stats[2].min = 11; stats[2].max = 20; stats[2].numOfNull = 0;
  bool keep = true;
  ASSERT_EQ(filterRangeExecute(filter, stats, 3, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(keep);
  stats[1].min = 2;
  ASSERT_EQ(filterRangeExecute(filter, stats, 3, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  stats[2].min = 5;
  stats[2].max = 10;
  const SColumnDataAgg noMatchCol5 = stats[2];
  // col4's [2,6] still matches the OR arm; col5 is the only pruning term.
  ASSERT_EQ(filterRangeExecute(filter, stats, 3, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(keep);
  // Without the invalid-count guard, [5,10] would also prune the AND term here.
  stats[2] = noMatchCol5;
  stats[2].numOfNull = -1;
  ASSERT_EQ(filterRangeExecute(filter, stats, 3, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  stats[2] = noMatchCol5;
  stats[2].numOfNull = 101;
  // A NULL count larger than the row count is invalid; retain the block.
  ASSERT_EQ(filterRangeExecute(filter, stats, 3, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  // With only stats[0] available, the other column predicates remain unproven.
  ASSERT_EQ(filterRangeExecute(filter, stats, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  filterFreeInfo(filter);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, unknownOrBranchAndMissingStatistics) {
  SNode *col = NULL, *isNull = NULL;
  ASSERT_EQ(flttMakeColumnNode(&col, NULL, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0, NULL), TSDB_CODE_SUCCESS);
  ((SColumnNode *)col)->colId = 3;
  ASSERT_EQ(flttMakeOpNode(&isNull, OP_TYPE_IS_NULL, TSDB_DATA_TYPE_BOOL, col, NULL), TSDB_CODE_SUCCESS);
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                   flttRangePredicate(3, OP_TYPE_EQUAL, 1), isNull);
  ASSERT_NE(condition, nullptr);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  // A non-range OR arm must retain the full SMA path even without a range tree.
  // Confirm that this OR has no range tree; behavior alone cannot distinguish that path.
  EXPECT_EQ(filter->sclCtx.orRangeExpr, nullptr);
  EXPECT_FALSE(filterBlockSmaOnlyUsesNumOfNull(filter));
  bool keep = false;
  ASSERT_EQ(filterRangeExecute(filter, NULL, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  SColumnDataAgg stat = {0};
  stat.colId = 3;
  stat.min = 3;
  stat.max = 5;
  keep = false;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  filterFreeInfo(filter);
  filter = NULL;
  nodesDestroyNode(condition);

  condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                             flttRangePredicate(3, OP_TYPE_EQUAL, 1),
                             flttRangePredicate(4, OP_TYPE_EQUAL, 2));
  ASSERT_NE(condition, nullptr);
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  EXPECT_FALSE(filterBlockSmaOnlyUsesNumOfNull(filter));
  EXPECT_NE(filter->sclCtx.orRangeExpr, nullptr);
  keep = false;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  filterFreeInfo(filter);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, unsupportedAndTermsDoNotExhaustOrBudget) {
  // Keep this value aligned with filter.c's FLT_SCL_RANGE_EXPR_BUDGET.
  constexpr int32_t kRangeExprBudget = 128;
  // Siblings do not increase tree depth.
  // Unsupported terms must not consume the budget needed by the final OR arm.
  SNode *children[kRangeExprBudget + 1] = {0};
  for (int32_t i = 0; i < kRangeExprBudget; ++i) {
    SNode *col = NULL;
    ASSERT_EQ(flttMakeColumnNode(&col, NULL, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0, NULL), TSDB_CODE_SUCCESS);
    ((SColumnNode *)col)->colId = 3;
    ((SColumnNode *)col)->colType = COLUMN_TYPE_COLUMN;
    ASSERT_EQ(flttMakeOpNode(&children[i], OP_TYPE_IS_NULL, TSDB_DATA_TYPE_BOOL, col, NULL), TSDB_CODE_SUCCESS);
  }
  children[kRangeExprBudget] = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                flttRangePredicate(3, OP_TYPE_EQUAL, 1),
                                flttRangePredicate(3, OP_TYPE_EQUAL, 2));
  ASSERT_NE(children[kRangeExprBudget], nullptr);
  SNode *condition = NULL;
  ASSERT_EQ(flttMakeLogicNode(&condition, LOGIC_COND_TYPE_AND, children, kRangeExprBudget + 1), TSDB_CODE_SUCCESS);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  EXPECT_NE(filter->sclCtx.orRangeExpr, nullptr);
  SColumnDataAgg stat = {0};
  stat.colId = 3;
  stat.min = 3;
  stat.max = 5;
  bool keep = true;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(keep);
  filterFreeInfo(filter);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, timeWindowIsConservativeAndNotStrict) {
  FilterScalarModeGuard guard;
  SNode *left = flttRangeLogic(LOGIC_COND_TYPE_AND,
                              flttTimestampPredicate(OP_TYPE_GREATER_THAN, 100),
                              flttTimestampPredicate(OP_TYPE_LOWER_THAN, 200));
  SNode *right = flttRangeLogic(LOGIC_COND_TYPE_AND,
                               flttTimestampPredicate(OP_TYPE_GREATER_THAN, 400),
                               flttTimestampPredicate(OP_TYPE_LOWER_THAN, 500));
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR, left, right);
  ASSERT_NE(condition, nullptr);
  STimeWindow window = {0};
  bool strict = true;
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, 101);
  EXPECT_EQ(window.ekey, 499);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, timestampNotEqualKeepsFullWindow) {
  FilterScalarModeGuard guard;
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                   flttTimestampPredicate(OP_TYPE_NOT_EQUAL, 100),
                                   flttTimestampPredicate(OP_TYPE_EQUAL, 200));
  ASSERT_NE(condition, nullptr);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_NO_REWRITE | FLT_OPTION_TIMESTAMP, NULL),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  bool hasRangeExpr = NULL != filter->sclCtx.orRangeExpr;
  filterFreeInfo(filter);
  ASSERT_TRUE(hasRangeExpr);
  STimeWindow window = {0};
  bool strict = true;
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, INT64_MIN);
  EXPECT_EQ(window.ekey, INT64_MAX);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, timeWindowBoundaryDoesNotOverflow) {
  FilterScalarModeGuard guard;
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                   flttTimestampPredicate(OP_TYPE_GREATER_THAN, INT64_MAX),
                                   flttTimestampPredicate(OP_TYPE_EQUAL, 100));
  ASSERT_NE(condition, nullptr);
  STimeWindow window = {0};
  bool strict = true;
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, 100);
  EXPECT_EQ(window.ekey, 100);
  EXPECT_LE(window.skey, window.ekey);
  nodesDestroyNode(condition);

  condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                             flttTimestampPredicate(OP_TYPE_LOWER_THAN, INT64_MIN),
                             flttTimestampPredicate(OP_TYPE_EQUAL, 100));
  ASSERT_NE(condition, nullptr);
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, 100);
  EXPECT_EQ(window.ekey, 100);
  nodesDestroyNode(condition);

  condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                             flttTimestampPredicate(OP_TYPE_GREATER_EQUAL, INT64_MAX),
                             flttTimestampPredicate(OP_TYPE_EQUAL, 100));
  ASSERT_NE(condition, nullptr);
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, 100);
  EXPECT_EQ(window.ekey, INT64_MAX);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, nonPrimaryTimestampDoesNotNarrowScanWindow) {
  FilterScalarModeGuard guard;
  SNode *otherTimestamp = flttRangePredicate(3, OP_TYPE_GREATER_THAN, 200, TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT_NE(otherTimestamp, nullptr);
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                   flttTimestampPredicate(OP_TYPE_GREATER_THAN, 100), otherTimestamp);
  ASSERT_NE(condition, nullptr);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE | FLT_OPTION_TIMESTAMP, NULL),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  // Both timestamp arms must reach the OR range tree for this window test.
  ASSERT_NE(filter->sclCtx.orRangeExpr, nullptr);
  filterFreeInfo(filter);
  STimeWindow window = {0};
  bool strict = true;
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, INT64_MIN);
  EXPECT_EQ(window.ekey, INT64_MAX);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, unavailableOrTreeDoesNotNarrowScanWindow) {
  FilterScalarModeGuard guard;
  SNode *col = NULL, *isNull = NULL;
  ASSERT_EQ(flttMakeColumnNode(&col, NULL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0, NULL),
            TSDB_CODE_SUCCESS);
  ((SColumnNode *)col)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  ((SColumnNode *)col)->isPrimTs = true;
  ASSERT_EQ(flttMakeOpNode(&isNull, OP_TYPE_IS_NULL, TSDB_DATA_TYPE_BOOL, col, NULL), TSDB_CODE_SUCCESS);
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                   flttTimestampPredicate(OP_TYPE_GREATER_THAN, 100), isNull);
  ASSERT_NE(condition, nullptr);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE | FLT_OPTION_TIMESTAMP, NULL),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  ASSERT_TRUE(filter->sclCtx.hasOrCondition);
  ASSERT_EQ(filter->sclCtx.orRangeExpr, nullptr);
  filterFreeInfo(filter);
  STimeWindow window = {0};
  bool strict = true;
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, INT64_MIN);
  EXPECT_EQ(window.ekey, INT64_MAX);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, mistypedPrimaryTimestampDoesNotPrune) {
  SNode *mistyped = flttRangePredicate(PRIMARYKEY_TIMESTAMP_COL_ID, OP_TYPE_EQUAL, 1,
                                      TSDB_DATA_TYPE_DOUBLE, true);
  ASSERT_NE(mistyped, nullptr);
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR, mistyped,
                                   flttRangePredicate(3, OP_TYPE_EQUAL, 2));
  ASSERT_NE(condition, nullptr);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(filter->sclCtx.orRangeExpr, nullptr);
  SColumnDataAgg stats[2] = {{0}, {0}};
  stats[0].colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  stats[0].min = 1;
  stats[0].max = 1;
  stats[1].colId = 3;
  stats[1].min = 5;
  stats[1].max = 5;
  bool keep = false;
  ASSERT_EQ(filterRangeExecute(filter, stats, 2, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  filterFreeInfo(filter);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, allNullOrArmPreservesOtherArm) {
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                   flttRangePredicate(3, OP_TYPE_EQUAL, 1),
                                   flttRangePredicate(4, OP_TYPE_EQUAL, 2));
  ASSERT_NE(condition, nullptr);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  SColumnDataAgg stats[2] = {{0}, {0}};
  stats[0].colId = 3;
  stats[0].numOfNull = 100;
  stats[1].colId = 4;
  stats[1].min = stats[1].max = 2;
  bool keep = false;
  ASSERT_EQ(filterRangeExecute(filter, stats, 2, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  stats[1].min = stats[1].max = 3;
  ASSERT_EQ(filterRangeExecute(filter, stats, 2, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(keep);
  filterFreeInfo(filter);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, invalidNullCountDoesNotPruneNotNullGroup) {
  SNode *col = NULL, *condition = NULL;
  ASSERT_EQ(flttMakeColumnNode(&col, NULL, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0, NULL), TSDB_CODE_SUCCESS);
  ((SColumnNode *)col)->colType = COLUMN_TYPE_COLUMN;
  ASSERT_EQ(flttMakeOpNode(&condition, OP_TYPE_IS_NOT_NULL, TSDB_DATA_TYPE_BOOL, col, NULL), TSDB_CODE_SUCCESS);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);
  ASSERT_NE(filter->pBlkOrNotNullGroups, nullptr);
  SColumnDataAgg stat = {0};
  stat.colId = 3;
  bool keep = false;
  stat.numOfNull = 101;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  stat.numOfNull = -1;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(keep);
  stat.numOfNull = 100;
  ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(keep);
  filterFreeInfo(filter);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, bigintTimestampLiteralNarrowsOrWindow) {
  FilterScalarModeGuard guard;
  SNode *left = flttTimestampPredicate(OP_TYPE_GREATER_THAN, 100);
  ASSERT_NE(left, nullptr);
  SValueNode *leftValue = (SValueNode *)((SOperatorNode *)left)->pRight;
  leftValue->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  leftValue->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR, left,
                                   flttTimestampPredicate(OP_TYPE_GREATER_THAN, 200));
  ASSERT_NE(condition, nullptr);
  STimeWindow window = {0};
  bool strict = true;
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, 101);
  EXPECT_EQ(window.ekey, INT64_MAX);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, notEqualAndLowerBoundRetainsAndWindow) {
  FilterScalarModeGuard guard;
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                   flttRangeLogic(LOGIC_COND_TYPE_AND,
                                                  flttTimestampPredicate(OP_TYPE_NOT_EQUAL, 100),
                                                  flttTimestampPredicate(OP_TYPE_GREATER_THAN, 500)),
                                   flttTimestampPredicate(OP_TYPE_EQUAL, 5));
  ASSERT_NE(condition, nullptr);
  STimeWindow window = {0};
  bool strict = true;
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, 5);
  EXPECT_EQ(window.ekey, INT64_MAX);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, timeWindowEmptyAndUnconstrainedBranches) {
  FilterScalarModeGuard guard;
  STimeWindow window = {0};
  bool strict = true;

  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                   flttTimestampPredicate(OP_TYPE_EQUAL, 100),
                                   flttTimestampPredicate(OP_TYPE_GREATER_THAN, INT64_MAX));
  ASSERT_NE(condition, nullptr);
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, 100);
  EXPECT_EQ(window.ekey, 100);
  nodesDestroyNode(condition);

  window = STimeWindow{};
  strict = true;
  condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                             flttRangeLogic(LOGIC_COND_TYPE_AND,
                                            flttTimestampPredicate(OP_TYPE_EQUAL, 100),
                                            flttTimestampPredicate(OP_TYPE_GREATER_THAN, INT64_MAX)),
                             flttTimestampPredicate(OP_TYPE_EQUAL, 200));
  ASSERT_NE(condition, nullptr);
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, 200);
  EXPECT_EQ(window.ekey, 200);
  nodesDestroyNode(condition);

  window = STimeWindow{};
  strict = true;
  condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                             flttRangeLogic(LOGIC_COND_TYPE_AND,
                                            flttTimestampPredicate(OP_TYPE_GREATER_THAN, INT64_MAX),
                                            flttTimestampPredicate(OP_TYPE_LOWER_THAN, 200)),
                             flttTimestampPredicate(OP_TYPE_EQUAL, 100));
  ASSERT_NE(condition, nullptr);
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, 100);
  EXPECT_EQ(window.ekey, 100);
  nodesDestroyNode(condition);

  window = STimeWindow{};
  strict = true;
  condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                             flttRangeLogic(LOGIC_COND_TYPE_AND,
                                            flttRangeLogic(LOGIC_COND_TYPE_AND,
                                                           flttTimestampPredicate(OP_TYPE_GREATER_THAN, INT64_MAX),
                                                           flttTimestampPredicate(OP_TYPE_LOWER_THAN, 200)),
                                            flttTimestampPredicate(OP_TYPE_EQUAL, 300)),
                             flttTimestampPredicate(OP_TYPE_EQUAL, 100));
  ASSERT_NE(condition, nullptr);
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, 100);
  EXPECT_EQ(window.ekey, 100);
  nodesDestroyNode(condition);

  window = STimeWindow{};
  strict = true;
  condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                             flttTimestampPredicate(OP_TYPE_EQUAL, 100),
                             flttRangePredicate(3, OP_TYPE_EQUAL, 1));
  ASSERT_NE(condition, nullptr);
  ASSERT_EQ(filterGetTimeRange(condition, &window, &strict, NULL), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(strict);
  EXPECT_EQ(window.skey, INT64_MIN);
  EXPECT_EQ(window.ekey, INT64_MAX);
  nodesDestroyNode(condition);
}

TEST(scalarOrRangeTest, blockPruningScenario) {
  SNode *condition = flttRangeLogic(LOGIC_COND_TYPE_OR,
                                   flttRangePredicate(3, OP_TYPE_EQUAL, 1),
                                   flttRangePredicate(4, OP_TYPE_EQUAL, 2));
  ASSERT_NE(condition, nullptr);
  SFilterInfo *filter = NULL;
  ASSERT_EQ(filterInitFromNode(condition, &filter, FLT_OPTION_SCALAR_MODE, NULL), TSDB_CODE_SUCCESS);
  ASSERT_NE(filter, nullptr);

  int32_t keptBlocks = 0;
  for (int32_t block = 0; block < 100; ++block) {
    SColumnDataAgg stats[2] = {{0}, {0}};
    // Reverse column order deliberately: SMA lookup must use colId, not index.
    stats[0].colId = 4;
    stats[1].colId = 3;
    stats[0].numOfNull = 0;
    stats[1].numOfNull = 0;
    stats[0].min = stats[0].max = (block + 5) % 10;
    stats[1].min = stats[1].max = block % 10;
    SCOPED_TRACE(::testing::Message() << "block=" << block << " col=" << stats[0].colId << ":" << stats[0].min
                                      << " col=" << stats[1].colId << ":" << stats[1].min);
    bool keep = true;
    ASSERT_EQ(filterRangeExecute(filter, stats, 2, 100, &keep), TSDB_CODE_SUCCESS);
    EXPECT_EQ(keep, block % 10 == 1 || (block + 5) % 10 == 2) << "block=" << block;
    if (keep) {
      ++keptBlocks;
    }
  }
  // 10 blocks match a=1 and another 10 match b=2; the two sets do not overlap.
  EXPECT_EQ(keptBlocks, 20);
  filterFreeInfo(filter);
  nodesDestroyNode(condition);
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

  snprintf(&rightv[2], sizeof(rightv) - 2, "%s", "__0");
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
  // MATCH/NMATCH go through the process-wide regex cache, which taosd and the
  // client set up during init. Without it every regex compare fails with
  // TSDB_CODE_INVALID_PTR, so a match test would pass only vacuously.
  (void)InitRegexCache();
  testing::InitGoogleTest(&argc, argv);
  int32_t ret = RUN_ALL_TESTS();
  DestroyRegexCache();
  return ret;
}

#pragma GCC diagnostic pop

class RemoteListRangeTest : public testing::Test {
 protected:
  SNode *predicate = nullptr;
  SFilterInfo *filter = nullptr;
  SRemoteValueListNode *list = nullptr;

  void init(EOperatorType op, bool negative = false, SNode *sibling = nullptr) {
    predicate = flttTimestampPredicate(op, 0);
    ASSERT_NE(predicate, nullptr);
    auto *cmp = reinterpret_cast<SOperatorNode *>(predicate);
    SDataType type = reinterpret_cast<SExprNode *>(cmp->pLeft)->resType;
    nodesDestroyNode(cmp->pRight);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_REMOTE_VALUE_LIST, &cmp->pRight), 0);
    list = reinterpret_cast<SRemoteValueListNode *>(cmp->pRight);
    list->node.resType = type;
    list->targetType = type.type;
    list->flag = VALUELIST_FLAG_VAL_UNSET;
    if (negative) cmp->flag |= OPERATOR_FLAG_NEGATIVE_OP;
    if (sibling) predicate = flttRangeLogic(LOGIC_COND_TYPE_AND, predicate, sibling);
    ASSERT_EQ(filterInitFromNode(predicate, &filter, 0, nullptr), 0);
    ASSERT_NE(filter, nullptr);
  }

  void addKeys(std::vector<int64_t> keys) {
    SColumnInfoData col = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1);
    col.pData = reinterpret_cast<char *>(keys.data());
    char id[] = "RemoteListRangeTest";
    ASSERT_EQ(scalarBuildRemoteListHash(id, list, &col, keys.size()), TSDB_CODE_SUCCESS);
    list->hasValue |= !keys.empty();
  }

  void expectBlock(int64_t min, int64_t max, int nulls, bool expected) {
    SColumnDataAgg stat = {};
    stat.colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    stat.min = min;
    stat.max = max;
    stat.numOfNull = nulls;
    bool keep = true;
    ASSERT_EQ(filterRangeExecute(filter, &stat, 1, 100, &keep), 0);
    EXPECT_EQ(expected, keep) << min << ".." << max << " nulls=" << nulls;
  }

  void TearDown() override {
    filterFreeInfo(filter);
    nodesDestroyNode(predicate);
  }
};

TEST_F(RemoteListRangeTest, publishAfterLastBatchAndRebuildAfterInvalidation) {
  init(OP_TYPE_IN);
  const int64_t large = INT64_C(9007199254740993);
  addKeys({large});
  // TSWINDOW_INITIALIZER is a C compound literal, which MSVC rejects in C++ (C4576).
  STimeWindow window = {INT64_MIN, INT64_MAX};
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, &window), 0);
  EXPECT_FALSE(filter->sclCtx.remoteRangePrepared);
  expectBlock(1, 1, 0, true);

  addKeys({large - 2, large + 3});
  list->hasNull = true;
  list->flag = 0;
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, &window), 0);
  EXPECT_EQ(window.skey, large - 2);
  EXPECT_EQ(window.ekey, large + 3);
  auto *published = filter->sclCtx.remoteRange;
  ASSERT_NE(published, nullptr);
  for (int64_t x = large - 4; x < large + 6; ++x) {
    expectBlock(x, x, 0, x >= large - 2 && x <= large + 3);
  }
  expectBlock(large, large, 100, false);
  window = {large, large + 10};
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, &window), 0);
  EXPECT_EQ(filter->sclCtx.remoteRange, published);
  EXPECT_EQ(window.skey, large);
  EXPECT_EQ(window.ekey, large + 3);
  filterInvalidateRemoteRange(filter);
  addKeys({10});
  window = {INT64_MIN, INT64_MAX};
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, &window), 0);
  EXPECT_EQ(window.skey, 10);
  expectBlock(10, 10, 0, true);
}

TEST_F(RemoteListRangeTest, largeMembershipUsesBatchBounds) {
  init(OP_TYPE_IN);
  std::vector<int64_t> keys;
  for (int64_t n = 10000; n > 1000; --n) keys.push_back(n);
  addKeys(keys);
  addKeys({-400, 15000, 5000});
  list->flag = 0;
  STimeWindow window = {INT64_MIN, INT64_MAX};
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, &window), 0);
  EXPECT_EQ(window.skey, -400);
  EXPECT_EQ(window.ekey, 15000);
  expectBlock(15001, 16000, 0, false);

  // A pre-built hash without bounds cannot be summarized from only its next batch.
  filterInvalidateRemoteRange(filter);
  list->bounds.type = TSDB_DATA_TYPE_NULL;
  addKeys({10});
  window = {INT64_MIN, INT64_MAX};
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, &window), 0);
  EXPECT_EQ(window.skey, INT64_MIN);
  EXPECT_EQ(window.ekey, INT64_MAX);
  expectBlock(15000, 15000, 0, true);
}

TEST_F(RemoteListRangeTest, complementBudgetAndDroppedKeys) {
  init(OP_TYPE_NOT_IN);
  std::vector<int64_t> keys;
  for (int64_t i = 0; i < 63; ++i) keys.push_back(2 * i);
  addKeys(keys);
  list->flag = 0;
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, nullptr), 0);
  for (int64_t x = 0; x < 126; ++x) expectBlock(x, x, 0, x % 2 != 0);
  filterInvalidateRemoteRange(filter);
  addKeys({126});
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, nullptr), 0);
  EXPECT_TRUE(filter->sclCtx.remoteRangePrepared);
  EXPECT_EQ(filter->sclCtx.remoteRange, nullptr);
  expectBlock(10, 10, 0, true);
  filterInvalidateRemoteRange(filter);
  list->droppedKey = true;
  taosHashClear(list->pHashFilter);
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, nullptr), 0);
  expectBlock(1, 1, 0, true);
  filterInvalidateRemoteRange(filter);
  list->droppedKey = false;
  list->hasNotNull = false;
  ASSERT_TRUE(list->hasValue);
  ASSERT_FALSE(list->hasNull);
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, nullptr), 0);
  expectBlock(1, 1, 0, true);
}

TEST_F(RemoteListRangeTest, waitForSiblingBeforeUsingSharedSnapshot) {
  auto *sibling = reinterpret_cast<SOperatorNode *>(flttTimestampPredicate(OP_TYPE_GREATER_THAN, 100));
  ASSERT_NE(sibling, nullptr);
  SValueNode bound = *reinterpret_cast<SValueNode *>(sibling->pRight);
  nodesDestroyNode(sibling->pRight);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_REMOTE_VALUE, &sibling->pRight), 0);
  auto *remote = reinterpret_cast<SRemoteValueNode *>(sibling->pRight);
  remote->subQIdx = 1;
  remote->val.node.resType = bound.node.resType;
  init(OP_TYPE_IN, false, reinterpret_cast<SNode *>(sibling));
  list->subQIdx = 0;
  addKeys({10, 20});
  SRemoteValueListNode snapshot = *list;
  snapshot.flag = 0;
  struct Cache {
    const SNode *results[2];
    int lookups;
  } cache = {{reinterpret_cast<SNode *>(&snapshot), nullptr}, 0};
  auto peek = [](void *ctx, int32_t index) -> const SNode * {
    auto *cache = static_cast<Cache *>(ctx);
    ++cache->lookups;
    return cache->results[index];
  };
  STimeWindow window = {INT64_MIN, INT64_MAX};
  ASSERT_EQ(filterPrepareRemoteRange(filter, &cache, peek, &window), 0);
  EXPECT_FALSE(filter->sclCtx.remoteRangePrepared);
  expectBlock(10, 110, 0, true);
  cache.results[1] = reinterpret_cast<SNode *>(&bound);
  ASSERT_EQ(filterPrepareRemoteRange(filter, &cache, peek, &window), 0);
  EXPECT_TRUE(filter->sclCtx.remoteRangePrepared);
  EXPECT_GT(window.skey, window.ekey);
  expectBlock(10, 110, 0, false);
  int lookups = cache.lookups;
  ASSERT_EQ(filterPrepareRemoteRange(filter, &cache, peek, nullptr), 0);
  EXPECT_EQ(cache.lookups, lookups);
  EXPECT_NE(list->flag & VALUELIST_FLAG_VAL_UNSET, 0);
}

TEST_F(RemoteListRangeTest, equalityAllEmptyNullAndDistinctValues) {
  init(OP_TYPE_NOT_IN, true);
  list->filterValueType = TSDB_DATA_TYPE_TIMESTAMP;
  list->flag = 0;
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, nullptr), 0);
  expectBlock(0, 0, 100, true);
  filterInvalidateRemoteRange(filter);
  addKeys({4, 4});
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, nullptr), 0);
  expectBlock(4, 4, 0, true);
  expectBlock(3, 3, 0, false);
  filterInvalidateRemoteRange(filter);
  list->hasNull = true;
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, nullptr), 0);
  expectBlock(4, 4, 0, false);
  filterInvalidateRemoteRange(filter);
  list->hasNull = false;
  addKeys({8});
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, nullptr), 0);
  expectBlock(4, 8, 0, false);
}

TEST_F(RemoteListRangeTest, nullTestsUseCountsAlongsideRemoteRange) {
  SNode *label = nullptr, *value = nullptr, *hasLabel = nullptr, *nullValue = nullptr;
  ASSERT_EQ(flttMakeColumnNode(&label, nullptr, TSDB_DATA_TYPE_VARCHAR, 16, 0, nullptr), 0);
  ASSERT_EQ(flttMakeColumnNode(&value, nullptr, TSDB_DATA_TYPE_DOUBLE, sizeof(double), 0, nullptr), 0);
  reinterpret_cast<SColumnNode *>(label)->colType = COLUMN_TYPE_COLUMN;
  reinterpret_cast<SColumnNode *>(value)->colType = COLUMN_TYPE_COLUMN;
  reinterpret_cast<SColumnNode *>(label)->colId = 2;
  reinterpret_cast<SColumnNode *>(value)->colId = 3;
  ASSERT_EQ(flttMakeOpNode(&hasLabel, OP_TYPE_IS_NOT_NULL, TSDB_DATA_TYPE_BOOL, label, nullptr), 0);
  ASSERT_EQ(flttMakeOpNode(&nullValue, OP_TYPE_IS_NULL, TSDB_DATA_TYPE_BOOL, value, nullptr), 0);
  init(OP_TYPE_NOT_IN, false, flttRangeLogic(LOGIC_COND_TYPE_AND, hasLabel, nullValue));
  addKeys({10});
  list->flag = 0;
  ASSERT_EQ(filterPrepareRemoteRange(filter, this, nullptr, nullptr), 0);

  // The timestamp alone can rule out a block even when both siblings lack SMA.
  expectBlock(10, 10, 0, false);
  expectBlock(11, 11, 0, true);

  SColumnDataAgg stats[2] = {};
  stats[0].colId = 2;
  stats[1].colId = 3;
  double nan = std::numeric_limits<double>::quiet_NaN();
  memcpy(&stats[1].min, &nan, sizeof(nan));
  memcpy(&stats[1].max, &nan, sizeof(nan));
  bool keep = true;
  ASSERT_EQ(filterRangeExecute(filter, stats, 2, 100, &keep), 0);
  EXPECT_FALSE(keep);  // No NULL double, although its min/max are unordered.
  stats[1].numOfNull = 1;
  ASSERT_EQ(filterRangeExecute(filter, stats, 2, 100, &keep), 0);
  EXPECT_TRUE(keep);
  stats[0].numOfNull = 100;
  ASSERT_EQ(filterRangeExecute(filter, stats, 2, 100, &keep), 0);
  EXPECT_FALSE(keep);  // No non-NULL label.
  stats[0].numOfNull = 101;
  ASSERT_EQ(filterRangeExecute(filter, stats, 2, 100, &keep), 0);
  EXPECT_TRUE(keep);  // Invalid count cannot rule out a matching label.
}
