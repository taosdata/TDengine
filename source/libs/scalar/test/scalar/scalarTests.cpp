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

#include "filterInt.h"
#include "filter.h"
#include "functionMgt.h"
#include "nodes.h"
#include "parUtil.h"
#include "scalar.h"
#include "sclInt.h"
#include "stub.h"
#include "taos.h"
#include "tconv.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "tlog.h"
#include "tvariant.h"

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
      SCL_ERR_RET(code);
    }

    SColumnInfoData idata = {0};
    idata.info = *colInfo;

    code = colInfoDataEnsureCapacity(&idata, rows, true);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(&idata);
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
      SCL_ERR_RET(terrno);
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
      taosMemoryFree(&idata);
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
    if (IS_STR_DATA_BLOB(dataType)) {
    vnode->datum.p = (char *)taosMemoryMalloc(blobDataTLen(value));
    if (NULL == vnode->datum.p) {
      SCL_ERR_RET(terrno);
    }
    blobDataCopy(vnode->datum.p, value);
    vnode->node.resType.bytes = blobDataTLen(value);

    } else {
    vnode->datum.p = (char *)taosMemoryMalloc(varDataTLen(value));
    if (NULL == vnode->datum.p) {
      SCL_ERR_RET(terrno);
    }
    varDataCopy(vnode->datum.p, value);
    vnode->node.resType.bytes = varDataTLen(value);

    }
  } else {
    vnode->node.resType.bytes = tDataTypes[dataType].bytes;
    (void)assignVal((char *)nodesGetValueFromNode(vnode), (const char *)value, 0, dataType);
  }

  *pNode = (SNode *)vnode;
  SCL_RET(TSDB_CODE_SUCCESS);
}

SStreamAncestorContext *scltMakeAncestorContext(int64_t gid, int32_t paramIndex) {
  auto *context = static_cast<SStreamAncestorContext *>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
  EXPECT_NE(context, nullptr);
  if (context == nullptr) return nullptr;
  context->pParamContexts = taosArrayInit(1, sizeof(SStreamAncestorParamContext));
  EXPECT_NE(context->pParamContexts, nullptr);
  if (context->pParamContexts == nullptr) return context;

  SStreamAncestorParamContext param = {};
  param.paramIndex = paramIndex;
  param.leafIdentity.gid = gid;
  param.leafIdentity.triggerType = WINDOW_TYPE_COUNT;
  param.leafIdentity.lineage.pScopes = taosArrayInit(2, sizeof(SScopeInstanceId));
  EXPECT_NE(param.leafIdentity.lineage.pScopes, nullptr);
  SScopeInstanceId root = {};
  root.layerIndex = 0;
  root.triggerType = WINDOW_TYPE_SESSION;
  root.openingTs = 100;
  root.nativeDiscriminator = 1;
  SScopeInstanceId parent = {};
  parent.layerIndex = 1;
  parent.triggerType = WINDOW_TYPE_COUNT;
  parent.openingTs = 200;
  parent.nativeDiscriminator = 2;
  EXPECT_NE(taosArrayPush(param.leafIdentity.lineage.pScopes, &root), nullptr);
  EXPECT_NE(taosArrayPush(param.leafIdentity.lineage.pScopes, &parent), nullptr);
  param.pSnapshots = taosArrayInit(2, sizeof(SWindowAncestorSnapshot));
  EXPECT_NE(param.pSnapshots, nullptr);
  SWindowAncestorSnapshot rootSnapshot = {};
  rootSnapshot.layerIndex = 0;
  rootSnapshot.triggerType = WINDOW_TYPE_SESSION;
  rootSnapshot.placeholderMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND;
  rootSnapshot.values.window.start = 100;
  rootSnapshot.values.window.end = 149;
  SWindowAncestorSnapshot parentSnapshot = {};
  parentSnapshot.layerIndex = 1;
  parentSnapshot.triggerType = WINDOW_TYPE_COUNT;
  parentSnapshot.placeholderMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND;
  parentSnapshot.values.window.start = 200;
  parentSnapshot.values.window.end = 249;
  EXPECT_NE(taosArrayPush(param.pSnapshots, &rootSnapshot), nullptr);
  EXPECT_NE(taosArrayPush(param.pSnapshots, &parentSnapshot), nullptr);
  EXPECT_NE(taosArrayPush(context->pParamContexts, &param), nullptr);
  return context;
}

SStreamContextPolicy *scltMakeAncestorPolicy(int64_t gid, int32_t paramIndex) {
  auto *policy = static_cast<SStreamContextPolicy *>(taosMemoryCalloc(1, sizeof(SStreamContextPolicy)));
  EXPECT_NE(policy, nullptr);
  if (policy == nullptr) return nullptr;
  policy->pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
  EXPECT_NE(policy->pEntries, nullptr);
  if (policy->pEntries == nullptr) return policy;
  SStreamContextPolicyEntry entry = {};
  entry.gid = gid;
  entry.paramIndex = paramIndex;
  entry.contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR;
  EXPECT_NE(taosArrayPush(policy->pEntries, &entry), nullptr);
  return policy;
}

int32_t scltMakeOpNode(SNode **pNode, EOperatorType opType, int32_t resType, SNode *pLeft, SNode *pRight);

SNode *scltMakeStreamPlaceholder(const char *name, int32_t layerIndex = -1,
                                 int32_t resType = TSDB_DATA_TYPE_TIMESTAMP) {
  const int32_t funcId = fmGetFuncId(name);
  EXPECT_GE(funcId, 0);

  SFunctionNode *func = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION, reinterpret_cast<SNode **>(&func)), TSDB_CODE_SUCCESS);
  if (func == nullptr) return nullptr;
  func->funcId = funcId;
  func->funcType = fmGetFuncTypeFromId(funcId);
  func->node.resType.type = resType;
  func->node.resType.bytes = tDataTypes[resType].bytes;

  SValueNode *result = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&result)), TSDB_CODE_SUCCESS);
  if (result == nullptr) {
    nodesDestroyNode(reinterpret_cast<SNode *>(func));
    return nullptr;
  }
  result->node.resType = func->node.resType;
  EXPECT_EQ(nodesListMakeStrictAppend(&func->pParameterList, reinterpret_cast<SNode *>(result)), TSDB_CODE_SUCCESS);

  if (layerIndex >= 0) {
    SValueNode *layer = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&layer)), TSDB_CODE_SUCCESS);
    if (layer == nullptr) {
      nodesDestroyNode(reinterpret_cast<SNode *>(func));
      return nullptr;
    }
    layer->node.resType.type = TSDB_DATA_TYPE_INT;
    layer->node.resType.bytes = sizeof(int32_t);
    layer->datum.i = layerIndex;
    EXPECT_EQ(nodesListMakeStrictAppend(&func->pParameterList, reinterpret_cast<SNode *>(layer)), TSDB_CODE_SUCCESS);
  }
  return reinterpret_cast<SNode *>(func);
}

SNode *scltMakePrimaryTimestamp() {
  SColumnNode *column = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&column)), TSDB_CODE_SUCCESS);
  if (column == nullptr) return nullptr;
  column->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  column->node.resType.bytes = sizeof(int64_t);
  column->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  return reinterpret_cast<SNode *>(column);
}

SNode *scltMakeTypedTimestampOffset(const char *name, int32_t layerIndex, EOperatorType opType, int32_t offsetType,
                                    const void *offset) {
  SNode *value = nullptr;
  EXPECT_EQ(scltMakeValueNode(&value, offsetType, const_cast<void *>(offset)), TSDB_CODE_SUCCESS);
  SNode *result = nullptr;
  EXPECT_EQ(
      scltMakeOpNode(&result, opType, TSDB_DATA_TYPE_TIMESTAMP, scltMakeStreamPlaceholder(name, layerIndex), value),
      TSDB_CODE_SUCCESS);
  return result;
}

SNode *scltMakeTimestampOffset(const char *name, int32_t layerIndex, EOperatorType opType, int64_t offset) {
  return scltMakeTypedTimestampOffset(name, layerIndex, opType, TSDB_DATA_TYPE_BIGINT, &offset);
}

SNode *scltMakeExternalBound(EOperatorType opType, SNode *right) {
  SNode *result = nullptr;
  EXPECT_EQ(scltMakeOpNode(&result, opType, TSDB_DATA_TYPE_BOOL, scltMakePrimaryTimestamp(), right), TSDB_CODE_SUCCESS);
  return result;
}

SNode *scltMakeAndCondition(std::initializer_list<SNode *> nodes) {
  SLogicConditionNode *condition = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, reinterpret_cast<SNode **>(&condition)), TSDB_CODE_SUCCESS);
  if (condition == nullptr) return nullptr;
  condition->node.resType.type = TSDB_DATA_TYPE_BOOL;
  condition->node.resType.bytes = sizeof(bool);
  condition->condType = LOGIC_COND_TYPE_AND;
  for (SNode *node : nodes) {
    EXPECT_EQ(nodesListMakeStrictAppend(&condition->pParameterList, node), TSDB_CODE_SUCCESS);
  }
  return reinterpret_cast<SNode *>(condition);
}

SNode *scltMakeDurationComparison(EOperatorType opType, int64_t bound) {
  SNode *value = nullptr;
  EXPECT_EQ(scltMakeValueNode(&value, TSDB_DATA_TYPE_BIGINT, &bound), TSDB_CODE_SUCCESS);
  if (value == nullptr) return nullptr;

  SNode *comparison = nullptr;
  EXPECT_EQ(scltMakeOpNode(&comparison, opType, TSDB_DATA_TYPE_BOOL,
                           scltMakeStreamPlaceholder("_twduration", -1, TSDB_DATA_TYPE_BIGINT), value),
            TSDB_CODE_SUCCESS);
  return comparison;
}

struct ScltBoolResult {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t rows = 0;
  int8_t  type = TSDB_DATA_TYPE_NULL;
  bool    isNull = true;
  bool    value = false;
};

ScltBoolResult scltEvaluateRuntimeBool(SNode *root, SStreamRuntimeFuncInfo *runtime) {
  ScltBoolResult result;
  SScalarParam   output = {};
  output.columnData = static_cast<SColumnInfoData *>(taosMemoryCalloc(1, sizeof(SColumnInfoData)));
  if (output.columnData == nullptr) {
    result.code = terrno;
    return result;
  }
  output.columnData->info = createColumnInfo(0, TSDB_DATA_TYPE_BOOL, sizeof(bool));
  output.colAlloced = true;
  result.code = colInfoDataEnsureCapacity(output.columnData, 1, true);
  if (result.code == TSDB_CODE_SUCCESS) {
    SScalarExtraInfo extra = {};
    extra.pStreamInfo = runtime;
    extra.isStream = true;
    result.code = scalarCalculate(root, nullptr, &output, &extra);
  }
  if (result.code == TSDB_CODE_SUCCESS && output.columnData != nullptr) {
    result.rows = output.numOfRows;
    result.type = output.columnData->info.type;
    if (result.rows > 0) {
      result.isNull = colDataIsNull_s(output.columnData, 0);
      if (!result.isNull) {
        result.value = *reinterpret_cast<bool *>(colDataGetData(output.columnData, 0));
      }
    }
  }
  sclFreeParam(&output);
  return result;
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
        taosMemoryFree(&idata);
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
      taosMemoryFree(&idata);
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

    *block = res;
  } else {
    SSDataBlock *res = *block;

    int32_t         idx = taosArrayGetSize(res->pDataBlock);
    SColumnInfoData idata = createColumnInfoData(dataType, dataBytes, 1 + idx);
    code = colInfoDataEnsureCapacity(&idata, rowNum, true);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(&idata);
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

int32_t scltMakeTargetNode(SNode **pNode, int64_t dataBlockId, int16_t slotId, SNode *snode) {
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

TEST(constantTest, ncharComparisonKeepsLiteralUntouched) {
  char ncharValue[VARSTR_HEADER_SIZE + 2 * TSDB_NCHAR_SIZE] = {0};
  varDataSetLen(ncharValue, 2 * TSDB_NCHAR_SIZE);
  TdUcs4 expected[] = {0x4e2d, 0x6587};
  memcpy(varDataVal(ncharValue), expected, sizeof(expected));

  char gbkValue[VARSTR_HEADER_SIZE + 4] = {0};
  varDataSetLen(gbkValue, 4);
  memcpy(varDataVal(gbkValue), "\xd6\xd0\xce\xc4", 4);

  SSDataBlock *block = NULL;
  SNode *left = NULL, *right = NULL, *op = NULL;
  ASSERT_EQ(scltMakeColumnNode(&left, &block, TSDB_DATA_TYPE_NCHAR,
                               sizeof(ncharValue), 1, ncharValue),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&right, TSDB_DATA_TYPE_VARCHAR, gbkValue),
            TSDB_CODE_SUCCESS);
  ((SValueNode *)right)->charsetCxt = taosConvInit("GBK");
  ASSERT_NE(((SValueNode *)right)->charsetCxt, nullptr);
  ASSERT_EQ(scltMakeOpNode(&op, OP_TYPE_EQUAL, TSDB_DATA_TYPE_BOOL, left,
                           right),
            TSDB_CODE_SUCCESS);

  SNode *rewritten = NULL;
  ASSERT_EQ(scalarCalculateConstants(op, &rewritten), TSDB_CODE_SUCCESS);
  ASSERT_EQ(rewritten, op);
  SValueNode *value = (SValueNode *)((SOperatorNode *)rewritten)->pRight;
  ASSERT_EQ(value->node.resType.type, TSDB_DATA_TYPE_VARCHAR);
  ASSERT_EQ(varDataLen(value->datum.p), 4);
  ASSERT_EQ(memcmp(varDataVal(value->datum.p), "\xd6\xd0\xce\xc4", 4), 0);

  SArray *blocks = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blocks, nullptr);
  ASSERT_NE(taosArrayPush(blocks, &block), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_BOOL,
                                         sizeof(bool));
  int16_t dataBlockId = 0, slotId = 0;
  ASSERT_EQ(scltAppendReservedSlot(blocks, &dataBlockId, &slotId, true, 1,
                                   &colInfo),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeTargetNode(&rewritten, dataBlockId, slotId, rewritten),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scalarCalculate(rewritten, blocks, NULL, NULL),
            TSDB_CODE_SUCCESS);

  // The coercion happens at the parameter level: the comparison matches
  // (GBK bytes decode to the column's UCS-4 value) while the literal node
  // keeps its original VARCHAR type and bytes.
  SSDataBlock     *resBlock = *(SSDataBlock **)taosArrayGetLast(blocks);
  SColumnInfoData *resCol =
      (SColumnInfoData *)taosArrayGetLast(resBlock->pDataBlock);
  EXPECT_TRUE(*(bool *)colDataGetData(resCol, 0));
  ASSERT_EQ(value->node.resType.type, TSDB_DATA_TYPE_VARCHAR);
  ASSERT_EQ(varDataLen(value->datum.p), 4);
  ASSERT_EQ(memcmp(varDataVal(value->datum.p), "\xd6\xd0\xce\xc4", 4), 0);

  // A second execution sees the same untouched literal and still matches.
  ASSERT_EQ(scalarCalculate(rewritten, blocks, NULL, NULL),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(*(bool *)colDataGetData(resCol, 0));
  ASSERT_EQ(value->node.resType.type, TSDB_DATA_TYPE_VARCHAR);
  ASSERT_EQ(memcmp(varDataVal(value->datum.p), "\xd6\xd0\xce\xc4", 4), 0);

  taosArrayDestroyEx(blocks, scltFreeDataBlock);
  nodesDestroyNode(rewritten);
}

TEST(constantTest, pureNcharComparisonStillFolds) {
  char ncharValue[VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE] = {0};
  varDataSetLen(ncharValue, TSDB_NCHAR_SIZE);
  *(TdUcs4 *)varDataVal(ncharValue) = 0x4e2d;
  char gbkValue[VARSTR_HEADER_SIZE + 2] = {0};
  varDataSetLen(gbkValue, 2);
  memcpy(varDataVal(gbkValue), "\xd6\xd0", 2);

  SNode *left = NULL, *right = NULL, *op = NULL;
  ASSERT_EQ(scltMakeValueNode(&left, TSDB_DATA_TYPE_NCHAR, ncharValue),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&right, TSDB_DATA_TYPE_VARCHAR, gbkValue),
            TSDB_CODE_SUCCESS);
  ((SValueNode *)right)->charsetCxt = taosConvInit("GBK");
  ASSERT_EQ(scltMakeOpNode(&op, OP_TYPE_EQUAL, TSDB_DATA_TYPE_BOOL, left,
                           right),
            TSDB_CODE_SUCCESS);

  SNode *result = NULL;
  ASSERT_EQ(scalarCalculateConstants(op, &result), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodeType(result), QUERY_NODE_VALUE);
  EXPECT_TRUE(((SValueNode *)result)->datum.b);
  nodesDestroyNode(result);
}

TEST(constantTest, simpleCaseKeepsLiteralUntouched) {
  char ncharValue[VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE] = {0};
  varDataSetLen(ncharValue, TSDB_NCHAR_SIZE);
  *(TdUcs4 *)varDataVal(ncharValue) = 0x4e2d;
  char gbkValue[VARSTR_HEADER_SIZE + 2] = {0};
  varDataSetLen(gbkValue, 2);
  memcpy(varDataVal(gbkValue), "\xd6\xd0", 2);
  int32_t one = 1, zero = 0;

  SSDataBlock *block = NULL;
  SNode *caseExpr = NULL, *whenValue = NULL, *thenValue = NULL,
        *elseValue = NULL, *whenThenNode = NULL, *caseNode = NULL;
  ASSERT_EQ(scltMakeColumnNode(&caseExpr, &block, TSDB_DATA_TYPE_NCHAR,
                               sizeof(ncharValue), 1, ncharValue),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&whenValue, TSDB_DATA_TYPE_VARCHAR, gbkValue),
            TSDB_CODE_SUCCESS);
  ((SValueNode *)whenValue)->charsetCxt = taosConvInit("GBK");
  ASSERT_EQ(scltMakeValueNode(&thenValue, TSDB_DATA_TYPE_INT, &one),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&elseValue, TSDB_DATA_TYPE_INT, &zero),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_WHEN_THEN, &whenThenNode),
            TSDB_CODE_SUCCESS);
  ((SWhenThenNode *)whenThenNode)->pWhen = whenValue;
  ((SWhenThenNode *)whenThenNode)->pThen = thenValue;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CASE_WHEN, &caseNode),
            TSDB_CODE_SUCCESS);
  SCaseWhenNode *caseWhen = (SCaseWhenNode *)caseNode;
  caseWhen->node.resType.type = TSDB_DATA_TYPE_INT;
  caseWhen->node.resType.bytes = sizeof(int32_t);
  caseWhen->charsetCxt = taosConvInit("GBK");
  caseWhen->pCase = caseExpr;
  caseWhen->pElse = elseValue;
  ASSERT_EQ(nodesMakeList(&caseWhen->pWhenThenList), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListAppend(caseWhen->pWhenThenList, whenThenNode),
            TSDB_CODE_SUCCESS);

  SNode *rewritten = NULL;
  ASSERT_EQ(scalarCalculateConstants(caseNode, &rewritten),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(((SValueNode *)whenValue)->node.resType.type,
            TSDB_DATA_TYPE_VARCHAR);

  SArray *blocks = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blocks, nullptr);
  ASSERT_NE(taosArrayPush(blocks, &block), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_INT,
                                         sizeof(int32_t));
  int16_t dataBlockId = 0, slotId = 0;
  ASSERT_EQ(scltAppendReservedSlot(blocks, &dataBlockId, &slotId, true, 1,
                                   &colInfo),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeTargetNode(&rewritten, dataBlockId, slotId, rewritten),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scalarCalculate(rewritten, blocks, NULL, NULL),
            TSDB_CODE_SUCCESS);

  // The comparison uses the CASE charset context while the literal keeps
  // its original VARCHAR type and bytes.
  SSDataBlock     *resBlock = *(SSDataBlock **)taosArrayGetLast(blocks);
  SColumnInfoData *resCol =
      (SColumnInfoData *)taosArrayGetLast(resBlock->pDataBlock);
  EXPECT_EQ(*(int32_t *)colDataGetData(resCol, 0), one);
  EXPECT_EQ(((SValueNode *)whenValue)->node.resType.type,
            TSDB_DATA_TYPE_VARCHAR);
  EXPECT_EQ(memcmp(varDataVal(((SValueNode *)whenValue)->datum.p),
                   "\xd6\xd0", 2),
            0);

  taosArrayDestroyEx(blocks, scltFreeDataBlock);
  nodesDestroyNode(rewritten);
}

TEST(constantTest, simpleCaseEvaluatesLaterColumnAfterMiss) {
  int32_t caseValue = 5;
  int32_t firstWhenValue = 0;
  int32_t secondWhenValue = 5;
  int32_t one = 1;
  int32_t two = 2;
  int32_t elseValue = 99;

  SSDataBlock *block = NULL;
  SNode *caseExpr = NULL;
  SNode *firstWhen = NULL;
  SNode *secondWhen = NULL;
  SNode *thenOne = NULL;
  SNode *thenTwo = NULL;
  SNode *elseNode = NULL;
  SNode *firstBranch = NULL;
  SNode *secondBranch = NULL;
  SNode *caseNode = NULL;

  ASSERT_EQ(scltMakeColumnNode(&caseExpr, &block, TSDB_DATA_TYPE_INT,
                               sizeof(int32_t), 1, &caseValue),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeColumnNode(&firstWhen, &block, TSDB_DATA_TYPE_INT,
                               sizeof(int32_t), 1, &firstWhenValue),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeColumnNode(&secondWhen, &block,
                               TSDB_DATA_TYPE_INT, sizeof(int32_t), 1,
                               &secondWhenValue),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&thenOne, TSDB_DATA_TYPE_INT, &one),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&thenTwo, TSDB_DATA_TYPE_INT, &two),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&elseNode, TSDB_DATA_TYPE_INT, &elseValue),
            TSDB_CODE_SUCCESS);

  ASSERT_EQ(nodesMakeNode(QUERY_NODE_WHEN_THEN, &firstBranch),
            TSDB_CODE_SUCCESS);
  ((SWhenThenNode *)firstBranch)->pWhen = firstWhen;
  ((SWhenThenNode *)firstBranch)->pThen = thenOne;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_WHEN_THEN, &secondBranch),
            TSDB_CODE_SUCCESS);
  ((SWhenThenNode *)secondBranch)->pWhen = secondWhen;
  ((SWhenThenNode *)secondBranch)->pThen = thenTwo;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CASE_WHEN, &caseNode),
            TSDB_CODE_SUCCESS);
  SCaseWhenNode *caseWhen = (SCaseWhenNode *)caseNode;
  caseWhen->node.resType.type = TSDB_DATA_TYPE_INT;
  caseWhen->node.resType.bytes = sizeof(int32_t);
  caseWhen->pCase = caseExpr;
  caseWhen->pElse = elseNode;
  ASSERT_EQ(nodesMakeList(&caseWhen->pWhenThenList), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListAppend(caseWhen->pWhenThenList, firstBranch),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListAppend(caseWhen->pWhenThenList, secondBranch),
            TSDB_CODE_SUCCESS);

  SArray *blocks = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blocks, nullptr);
  ASSERT_NE(taosArrayPush(blocks, &block), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_INT,
                                         sizeof(int32_t));
  int16_t dataBlockId = 0;
  int16_t slotId = 0;
  ASSERT_EQ(scltAppendReservedSlot(blocks, &dataBlockId, &slotId, true, 1,
                                   &colInfo),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeTargetNode(&caseNode, dataBlockId, slotId, caseNode),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scalarCalculate(caseNode, blocks, NULL, NULL),
            TSDB_CODE_SUCCESS);

  SSDataBlock *resultBlock =
      *(SSDataBlock **)taosArrayGetLast(blocks);
  SColumnInfoData *resultColumn =
      (SColumnInfoData *)taosArrayGetLast(resultBlock->pDataBlock);
  EXPECT_EQ(*(int32_t *)colDataGetData(resultColumn, 0), two);

  taosArrayDestroyEx(blocks, scltFreeDataBlock);
  nodesDestroyNode(caseNode);
}

TEST(constantTest, simpleCaseMixedWhenCharsetsUseCaseContext) {
  char gbkCase[VARSTR_HEADER_SIZE + 2] = {0};
  varDataSetLen(gbkCase, 2);
  memcpy(varDataVal(gbkCase), "\xd6\xd0", 2);
  char asciiWhen[VARSTR_HEADER_SIZE + 1] = {0};
  varDataSetLen(asciiWhen, 1);
  *varDataVal(asciiWhen) = 'x';
  char ncharWhen[VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE] = {0};
  varDataSetLen(ncharWhen, TSDB_NCHAR_SIZE);
  *(TdUcs4 *)varDataVal(ncharWhen) = 0x4e2d;
  int32_t one = 1, two = 2, zero = 0;

  SNode *caseValue = NULL, *whenVarchar = NULL, *thenOne = NULL;
  SNode *whenNchar = NULL, *thenTwo = NULL, *elseValue = NULL;
  SNode *firstBranch = NULL, *secondBranch = NULL, *caseNode = NULL;
  ASSERT_EQ(scltMakeValueNode(&caseValue, TSDB_DATA_TYPE_VARCHAR, gbkCase), TSDB_CODE_SUCCESS);
  ((SValueNode *)caseValue)->charsetCxt = taosConvInit("GBK");
  ASSERT_EQ(scltMakeValueNode(&whenVarchar, TSDB_DATA_TYPE_VARCHAR, asciiWhen), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&thenOne, TSDB_DATA_TYPE_INT, &one), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&whenNchar, TSDB_DATA_TYPE_NCHAR, ncharWhen), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&thenTwo, TSDB_DATA_TYPE_INT, &two), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&elseValue, TSDB_DATA_TYPE_INT, &zero), TSDB_CODE_SUCCESS);

  ASSERT_EQ(nodesMakeNode(QUERY_NODE_WHEN_THEN, &firstBranch), TSDB_CODE_SUCCESS);
  ((SWhenThenNode *)firstBranch)->pWhen = whenVarchar;
  ((SWhenThenNode *)firstBranch)->pThen = thenOne;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_WHEN_THEN, &secondBranch), TSDB_CODE_SUCCESS);
  ((SWhenThenNode *)secondBranch)->pWhen = whenNchar;
  ((SWhenThenNode *)secondBranch)->pThen = thenTwo;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CASE_WHEN, &caseNode), TSDB_CODE_SUCCESS);
  SCaseWhenNode *caseWhen = (SCaseWhenNode *)caseNode;
  caseWhen->node.resType.type = TSDB_DATA_TYPE_INT;
  caseWhen->node.resType.bytes = sizeof(int32_t);
  caseWhen->charsetCxt = taosConvInit("GBK");
  caseWhen->pCase = caseValue;
  caseWhen->pElse = elseValue;
  ASSERT_EQ(nodesMakeList(&caseWhen->pWhenThenList), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListAppend(caseWhen->pWhenThenList, firstBranch), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListAppend(caseWhen->pWhenThenList, secondBranch), TSDB_CODE_SUCCESS);

  SNode *result = NULL;
  ASSERT_EQ(scalarCalculateConstants(caseNode, &result), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodeType(result), QUERY_NODE_VALUE);
  EXPECT_EQ(((SValueNode *)result)->datum.i, two);
  nodesDestroyNode(result);
}

TEST(constantTest, simpleCaseVarcharColumnKeepsOriginalComparisonType) {
  char gbkCase[VARSTR_HEADER_SIZE + 2] = {0};
  varDataSetLen(gbkCase, 2);
  memcpy(varDataVal(gbkCase), "\xd6\xd0", 2);
  char utf8Column[VARSTR_HEADER_SIZE + 3] = {0};
  varDataSetLen(utf8Column, 3);
  memcpy(varDataVal(utf8Column), "\xe4\xb8\xad", 3);
  char ncharWhen[VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE] = {0};
  varDataSetLen(ncharWhen, TSDB_NCHAR_SIZE);
  *(TdUcs4 *)varDataVal(ncharWhen) = 0x4e2d;
  int32_t one = 1, two = 2, zero = 0;

  SSDataBlock *block = NULL;
  SNode *caseValue = NULL, *whenVarchar = NULL, *thenOne = NULL;
  SNode *whenNchar = NULL, *thenTwo = NULL, *elseValue = NULL;
  SNode *firstBranch = NULL, *secondBranch = NULL, *caseNode = NULL;
  ASSERT_EQ(scltMakeValueNode(&caseValue, TSDB_DATA_TYPE_VARCHAR,
                              gbkCase),
            TSDB_CODE_SUCCESS);
  ((SValueNode *)caseValue)->charsetCxt = taosConvInit("GBK");
  ASSERT_EQ(scltMakeColumnNode(&whenVarchar, &block,
                               TSDB_DATA_TYPE_VARCHAR,
                               sizeof(utf8Column), 1, utf8Column),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&thenOne, TSDB_DATA_TYPE_INT, &one),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&whenNchar, TSDB_DATA_TYPE_NCHAR,
                              ncharWhen),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&thenTwo, TSDB_DATA_TYPE_INT, &two),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&elseValue, TSDB_DATA_TYPE_INT, &zero),
            TSDB_CODE_SUCCESS);

  ASSERT_EQ(nodesMakeNode(QUERY_NODE_WHEN_THEN, &firstBranch),
            TSDB_CODE_SUCCESS);
  ((SWhenThenNode *)firstBranch)->pWhen = whenVarchar;
  ((SWhenThenNode *)firstBranch)->pThen = thenOne;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_WHEN_THEN, &secondBranch),
            TSDB_CODE_SUCCESS);
  ((SWhenThenNode *)secondBranch)->pWhen = whenNchar;
  ((SWhenThenNode *)secondBranch)->pThen = thenTwo;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CASE_WHEN, &caseNode),
            TSDB_CODE_SUCCESS);
  SCaseWhenNode *caseWhen = (SCaseWhenNode *)caseNode;
  caseWhen->node.resType.type = TSDB_DATA_TYPE_INT;
  caseWhen->node.resType.bytes = sizeof(int32_t);
  caseWhen->charsetCxt = taosConvInit("GBK");
  caseWhen->pCase = caseValue;
  caseWhen->pElse = elseValue;
  ASSERT_EQ(nodesMakeList(&caseWhen->pWhenThenList), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListAppend(caseWhen->pWhenThenList, firstBranch),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListAppend(caseWhen->pWhenThenList, secondBranch),
            TSDB_CODE_SUCCESS);

  SArray *blocks = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(blocks, nullptr);
  ASSERT_NE(taosArrayPush(blocks, &block), nullptr);
  SColumnInfo colInfo = createColumnInfo(1, TSDB_DATA_TYPE_INT,
                                         sizeof(int32_t));
  int16_t dataBlockId = 0, slotId = 0;
  ASSERT_EQ(scltAppendReservedSlot(blocks, &dataBlockId, &slotId, true, 1,
                                   &colInfo),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeTargetNode(&caseNode, dataBlockId, slotId, caseNode),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scalarCalculate(caseNode, blocks, NULL, NULL),
            TSDB_CODE_SUCCESS);

  SSDataBlock *resultBlock = *(SSDataBlock **)taosArrayGetLast(blocks);
  SColumnInfoData *resultColumn =
      (SColumnInfoData *)taosArrayGetLast(resultBlock->pDataBlock);
  EXPECT_EQ(*(int32_t *)colDataGetData(resultColumn, 0), two);

  taosArrayDestroyEx(blocks, scltFreeDataBlock);
  nodesDestroyNode(caseNode);
}

TEST(constantTest, simpleCaseNcharMissThenVarcharMatchUsesOriginalCaseParam) {
  char gbkCase[VARSTR_HEADER_SIZE + 2] = {0};
  varDataSetLen(gbkCase, 2);
  memcpy(varDataVal(gbkCase), "\xd6\xd0", 2);
  char ncharMiss[VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE] = {0};
  varDataSetLen(ncharMiss, TSDB_NCHAR_SIZE);
  *(TdUcs4 *)varDataVal(ncharMiss) = 0x4e16;
  char gbkMatch[VARSTR_HEADER_SIZE + 2] = {0};
  varDataSetLen(gbkMatch, 2);
  memcpy(varDataVal(gbkMatch), "\xd6\xd0", 2);
  int32_t one = 1, two = 2, zero = 0;

  SNode *caseValue = NULL, *whenNchar = NULL, *thenOne = NULL;
  SNode *whenVarchar = NULL, *thenTwo = NULL, *elseValue = NULL;
  SNode *firstBranch = NULL, *secondBranch = NULL, *caseNode = NULL;
  ASSERT_EQ(scltMakeValueNode(&caseValue, TSDB_DATA_TYPE_VARCHAR, gbkCase), TSDB_CODE_SUCCESS);
  ((SValueNode *)caseValue)->charsetCxt = taosConvInit("GBK");
  ASSERT_EQ(scltMakeValueNode(&whenNchar, TSDB_DATA_TYPE_NCHAR, ncharMiss), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&thenOne, TSDB_DATA_TYPE_INT, &one), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&whenVarchar, TSDB_DATA_TYPE_VARCHAR, gbkMatch), TSDB_CODE_SUCCESS);
  ((SValueNode *)whenVarchar)->charsetCxt = taosConvInit("GBK");
  ASSERT_EQ(scltMakeValueNode(&thenTwo, TSDB_DATA_TYPE_INT, &two), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&elseValue, TSDB_DATA_TYPE_INT, &zero), TSDB_CODE_SUCCESS);

  ASSERT_EQ(nodesMakeNode(QUERY_NODE_WHEN_THEN, &firstBranch), TSDB_CODE_SUCCESS);
  ((SWhenThenNode *)firstBranch)->pWhen = whenNchar;
  ((SWhenThenNode *)firstBranch)->pThen = thenOne;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_WHEN_THEN, &secondBranch), TSDB_CODE_SUCCESS);
  ((SWhenThenNode *)secondBranch)->pWhen = whenVarchar;
  ((SWhenThenNode *)secondBranch)->pThen = thenTwo;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CASE_WHEN, &caseNode), TSDB_CODE_SUCCESS);
  SCaseWhenNode *caseWhen = (SCaseWhenNode *)caseNode;
  caseWhen->node.resType.type = TSDB_DATA_TYPE_INT;
  caseWhen->node.resType.bytes = sizeof(int32_t);
  caseWhen->charsetCxt = taosConvInit("GBK");
  caseWhen->pCase = caseValue;
  caseWhen->pElse = elseValue;
  ASSERT_EQ(nodesMakeList(&caseWhen->pWhenThenList), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListAppend(caseWhen->pWhenThenList, firstBranch), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListAppend(caseWhen->pWhenThenList, secondBranch), TSDB_CODE_SUCCESS);

  SNode *result = NULL;
  ASSERT_EQ(scalarCalculateConstants(caseNode, &result), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodeType(result), QUERY_NODE_VALUE);
  EXPECT_EQ(((SValueNode *)result)->datum.i, two);
  nodesDestroyNode(result);
}

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

TEST(constantTest, ubigint_add_ubigint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_UBIGINT, &scltLeftV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_UBIGINT, &scltRightV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_ADD, TSDB_DATA_TYPE_UBIGINT, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_UBIGINT);
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

TEST(constantTest, utinyint_and_usmallint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_UTINYINT, &scltLeftV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_USMALLINT, &scltRightV);
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

TEST(constantTest, uint_and_usmallint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_UINT, &scltLeftV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_USMALLINT, &scltRightV);
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

TEST(constantTest, ubigint_and_uint) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_UBIGINT, &scltLeftV);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_UINT, &scltRightV);
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

TEST(constantTest, binary_greater_equal_varbinary) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   binaryStr[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&binaryStr[2], "%d", scltRightV);
  varDataSetLen(binaryStr, strlen(&binaryStr[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_VARBINARY, binaryStr);
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
  ASSERT_EQ(v->datum.b, scltLeftV < scltRightVd);
  nodesDestroyNode(res);
}

TEST(constantTest, binary_equal_geo) {
  SNode *pLeft = NULL, *pRight = NULL, *opNode = NULL, *res = NULL;
  char   geoRawStr[64] = "POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))";
  char   geoStr[64] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  (void)sprintf(&geoStr[2], "%s", geoRawStr);
  varDataSetLen(geoStr, strlen(&geoStr[2]));
  code = scltMakeValueNode(&pLeft, TSDB_DATA_TYPE_GEOMETRY, geoStr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeValueNode(&pRight, TSDB_DATA_TYPE_BINARY, geoStr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = scltMakeOpNode(&opNode, OP_TYPE_EQUAL, TSDB_DATA_TYPE_BOOL, pLeft, pRight);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = scalarCalculateConstants(opNode, &res);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(res);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  SValueNode *v = (SValueNode *)res;
  ASSERT_EQ(v->node.resType.type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(v->datum.b, scltLeftV < scltRightVd);
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

TEST(constantTest, null_boolean_three_valued_logic) {
  struct TestCase {
    ELogicConditionType type;
    bool                other;
    bool                expectedNull;
    bool                expectedValue;
  } cases[] = {
      {LOGIC_COND_TYPE_AND, false, false, false},
      {LOGIC_COND_TYPE_AND, true, true, false},
      {LOGIC_COND_TYPE_OR, false, true, false},
      {LOGIC_COND_TYPE_OR, true, false, true},
  };

  for (const TestCase &tc : cases) {
    SNode *nullValue = NULL, *boolValue = NULL, *logicNode = NULL, *res = NULL;
    bool   placeholder = false;
    bool   other = tc.other;
    ASSERT_EQ(scltMakeValueNode(&nullValue, TSDB_DATA_TYPE_BOOL, &placeholder), TSDB_CODE_SUCCESS);
    ((SValueNode *)nullValue)->isNull = true;
    ASSERT_EQ(scltMakeValueNode(&boolValue, TSDB_DATA_TYPE_BOOL, &other), TSDB_CODE_SUCCESS);
    SNode *params[] = {nullValue, boolValue};
    ASSERT_EQ(scltMakeLogicNode(&logicNode, tc.type, params, 2), TSDB_CODE_SUCCESS);

    ASSERT_EQ(scalarCalculateConstants(logicNode, &res), TSDB_CODE_SUCCESS);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
    SValueNode *value = (SValueNode *)res;
    EXPECT_EQ(value->isNull, tc.expectedNull);
    if (!tc.expectedNull) EXPECT_EQ(value->datum.b, tc.expectedValue);
    nodesDestroyNode(res);
  }
}

TEST(constantTest, null_operator_and_function_propagation) {
  int64_t placeholder = 0;
  int64_t one = 1;

  SNode *nullValue = NULL, *oneValue = NULL, *opNode = NULL, *res = NULL;
  ASSERT_EQ(scltMakeValueNode(&nullValue, TSDB_DATA_TYPE_BIGINT, &placeholder), TSDB_CODE_SUCCESS);
  ((SValueNode *)nullValue)->isNull = true;
  ASSERT_EQ(scltMakeValueNode(&oneValue, TSDB_DATA_TYPE_BIGINT, &one), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeOpNode(&opNode, OP_TYPE_ADD, TSDB_DATA_TYPE_BIGINT, nullValue, oneValue), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scalarCalculateConstants(opNode, &res), TSDB_CODE_SUCCESS);
  ASSERT_NE(res, nullptr);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  EXPECT_TRUE(((SValueNode *)res)->isNull);
  EXPECT_EQ(((SValueNode *)res)->node.resType.type, TSDB_DATA_TYPE_BIGINT);
  nodesDestroyNode(res);

  SFunctionNode *func = NULL;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode **)&func), TSDB_CODE_SUCCESS);
  ASSERT_NE(func, nullptr);
  func->funcId = fmGetFuncId("abs");
  ASSERT_GE(func->funcId, 0);
  func->funcType = fmGetFuncTypeFromId(func->funcId);
  func->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  func->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  nullValue = NULL;
  ASSERT_EQ(scltMakeValueNode(&nullValue, TSDB_DATA_TYPE_BIGINT, &placeholder), TSDB_CODE_SUCCESS);
  ((SValueNode *)nullValue)->isNull = true;
  ASSERT_EQ(nodesListMakeStrictAppend(&func->pParameterList, nullValue), TSDB_CODE_SUCCESS);

  res = NULL;
  ASSERT_EQ(scalarCalculateConstants((SNode *)func, &res), TSDB_CODE_SUCCESS);
  ASSERT_NE(res, nullptr);
  ASSERT_EQ(nodeType(res), QUERY_NODE_VALUE);
  EXPECT_TRUE(((SValueNode *)res)->isNull);
  EXPECT_EQ(((SValueNode *)res)->node.resType.type, TSDB_DATA_TYPE_BIGINT);
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
    SCL_ERR_RET(terrno);
  }

  SCL_ERR_RET(makeOperator(&opNode, blockList, opType, rightType, rightData, isReverse));

  SCL_ERR_RET(scalarCalculate(opNode, blockList, NULL, NULL));

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  if (res->info.rows != 1) {
    (void)printf("expect 1 row, but got %d\n", res->info.rows);
    SCL_ERR_RET(TSDB_CODE_FAILED);
  }
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);

  if (colDataIsNull_f(column, 0)) {
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
  int32_t code = parseJsontoTagData(rightv, tags, &row, NULL, NULL);
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
  taosMemoryFree(row);
}

void *prepareNchar(char *rightData) {
  int32_t len = 0;
  int32_t inputLen = strlen(rightData);

  char *t = (char *)taosMemoryCalloc(1, (inputLen + 1) * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
  taosMbsToUcs4(rightData, inputLen, (TdUcs4 *)varDataVal(t), inputLen * TSDB_NCHAR_SIZE, &len, NULL);
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
  code = parseJsontoTagData(rightv, tags, &row, NULL, NULL);
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
    taosMemoryFree(rightData);
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
    taosMemoryFree(rightData);
  }

  (void)printf("--------------------json null---null {1, 8, 2, 2, 3, 0, 0, 0, 0}------------------\n");

  key = "k3";  // (null is true) return NULL, so use DBL_MAX represent NULL
  double eRes2[len + len1] = {DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, 1, 0, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes2[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes2[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes2[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFree(rightData);
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
    taosMemoryFree(rightData);
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
    taosMemoryFree(rightData);
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
    taosMemoryFree(rightData);
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
    taosMemoryFree(rightData);
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
    taosMemoryFree(rightData);
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
    taosMemoryFree(rightData);
  }

  (void)printf("---------------------json not exist-- NULL {1, 8, 2, 2, 3, 0, 0, 0, 0}------------------\n");

  key = "k10";  // (NULL is true) return NULL, so use DBL_MAX represent NULL
  double eRes9[len + len1] = {DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, 1, 0, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX};
  for (int i = 0; i < len; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes9[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  for (int i = 0; i < len0; i++) {
    code = makeCalculate(row, key, TSDB_DATA_TYPE_INT, &input[i], eRes9[i], op[i], true);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  for (int i = len; i < len + len1; i++) {
    void *rightData = prepareNchar(inputNchar[i - len]);
    code = makeCalculate(row, key, TSDB_DATA_TYPE_NCHAR, rightData, eRes9[i], op[i], false);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    taosMemoryFree(rightData);
  }

  taosArrayDestroy(tags);
  taosMemoryFree(row);
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

  code = scalarCalculate(opNode, blockList, NULL, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(column, i)), eRes[i]);
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

  code = scalarCalculate(opNode, blockList, NULL, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SSDataBlock *res = *(SSDataBlock **)taosArrayGetLast(blockList);
  ASSERT_EQ(res->info.rows, rowNum);
  SColumnInfoData *column = (SColumnInfoData *)taosArrayGetLast(res->pDataBlock);
  ASSERT_EQ(column->info.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(column->info.bytes, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes);
  for (int32_t i = 0; i < rowNum; ++i) {
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(column, i)), eRes[i]);
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

  code = scalarCalculate(opNode, blockList, NULL, NULL);
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

  code = scalarCalculate(opNode, blockList, NULL, NULL);
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

  code = scalarCalculate(opNode, blockList, NULL, NULL);
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

  code = scalarCalculate(opNode, blockList, NULL, NULL);
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
  bool         eRes[5] = {false, false, false, false, false};
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

  code = scalarCalculate(opNode, blockList, NULL, NULL);
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

  code = scalarCalculate(opNode, blockList, NULL, NULL);
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

  code = scalarCalculate(opNode, blockList, NULL, NULL);
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

  code = scalarCalculate(opNode, blockList, NULL, NULL);
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
  code = scalarCalculate(opNode, blockList, NULL, NULL);
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

  code = scalarCalculate(opNode, blockList, NULL, NULL);
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

  code = scalarCalculate(logicNode, blockList, NULL, NULL);
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
    SCL_ERR_RET(terrno);
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
    SCL_ERR_RET(terrno);
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
  taosMemoryFree(pInput->columnData);
  taosMemoryFree(pInput);
}

extern "C" int32_t regexpInSetFunction(SScalarParam *pInput,
                                       int32_t inputNum,
                                       SScalarParam *pOutput);

// Build a 1-row string SScalarParam holding bytes in the parameter's charset.
static void scltMakeRawStringParam(SScalarParam *pParam, int32_t type,
                                   const void *bytes, int32_t len) {
  char *buf = (char *)taosMemoryCalloc(1, VARSTR_HEADER_SIZE + len);
  ASSERT_NE(buf, nullptr);
  varDataSetLen(buf, len);
  if (len > 0) memcpy(varDataVal(buf), bytes, len);

  pParam->columnData =
      (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  ASSERT_NE(pParam->columnData, nullptr);
  pParam->columnData->info =
      createColumnInfo(0, type, VARSTR_HEADER_SIZE + len);
  ASSERT_EQ(colInfoDataEnsureCapacity(pParam->columnData, 1, true),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataSetVal(pParam->columnData, 0, buf, false),
            TSDB_CODE_SUCCESS);
  pParam->numOfRows = 1;
  taosMemoryFree(buf);
}

static void scltMakeRawStringRows(SScalarParam *pParam, int32_t type,
                                  const char *const *values,
                                  const int32_t *lengths, int32_t rows) {
  pParam->columnData =
      (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  ASSERT_NE(pParam->columnData, nullptr);
  pParam->columnData->info =
      createColumnInfo(0, type, VARSTR_HEADER_SIZE + 64);
  ASSERT_EQ(colInfoDataEnsureCapacity(pParam->columnData, rows, true),
            TSDB_CODE_SUCCESS);

  for (int32_t i = 0; i < rows; ++i) {
    char *buf =
        (char *)taosMemoryCalloc(1, VARSTR_HEADER_SIZE + lengths[i]);
    ASSERT_NE(buf, nullptr);
    varDataSetLen(buf, lengths[i]);
    memcpy(varDataVal(buf), values[i], lengths[i]);
    ASSERT_EQ(colDataSetVal(pParam->columnData, i, buf, false),
              TSDB_CODE_SUCCESS);
    taosMemoryFree(buf);
  }
  pParam->numOfRows = rows;
}

TEST(ScalarFunctionTest, repeatActualLength) {
  for (int type : {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_NCHAR}) {
    for (uint64_t count : {uint64_t(0), uint64_t(1), uint64_t(2), uint64_t(INT32_MAX),
                           uint64_t(4294967297ULL), UINT64_MAX}) {
      for (int len : {0, 4, 32756, 32760, 65516, 65517, 65518}) {
        if (type == TSDB_DATA_TYPE_NCHAR && len % TSDB_NCHAR_SIZE != 0) continue;
        SCOPED_TRACE(::testing::Message() << type << ":" << count << ":" << len);
        std::string value(len, 'x');
        SScalarParam input[2] = {}, output = {};
        scltMakeRawStringParam(&input[0], type, value.data(), len);
        input[0].columnData->info.bytes = TSDB_MAX_FIELD_LEN;
        SScalarParam *counts = nullptr;
        ASSERT_EQ(scltMakeDataBlock(&counts, TSDB_DATA_TYPE_BIGINT, &count, 1, true), 0);
        counts->columnData->info.type = TSDB_DATA_TYPE_UBIGINT;
        input[1] = *counts;
        scltMakeRawStringParam(&output, type, "", 0);
        output.columnData->info.bytes = TSDB_MAX_FIELD_LEN;
        bool overflow = len > 0 && count > (TSDB_MAX_FIELD_LEN - VARSTR_HEADER_SIZE) / len;
        EXPECT_EQ(repeatFunction(input, 2, &output),
                  overflow ? TSDB_CODE_FUNC_INVALID_RES_LENGTH : TSDB_CODE_SUCCESS);
        if (!overflow) {
          char *result = colDataGetData(output.columnData, 0);
          EXPECT_EQ(varDataLen(result), len * count);
          EXPECT_EQ(std::string(varDataVal(result), varDataLen(result)),
                    std::string(len * count, 'x'));
        }
        colDataDestroy(input[0].columnData);
        taosMemoryFree(input[0].columnData);
        scltDestroyDataBlock(counts);
        colDataDestroy(output.columnData);
        taosMemoryFree(output.columnData);
      }
    }
  }
}

TEST(ScalarFunctionTest, repeatBroadcastAndNull) {
  for (int stringRows : {1, 3}) {
    for (int countRows : {1, 3}) {
      SScalarParam input[2] = {}, output = {};
      const char *values[] = {"ab", "x", "z"};
      int lengths[] = {2, 1, 1};
      scltMakeRawStringRows(&input[0], TSDB_DATA_TYPE_VARCHAR, values, lengths, stringRows);
      input[0].columnData->info.bytes = TSDB_MAX_FIELD_LEN;
      if (stringRows == 3) colDataSetNULL(input[0].columnData, 2);
      int64_t count = 2;
      SScalarParam *counts = nullptr;
      ASSERT_EQ(scltMakeDataBlock(&counts, TSDB_DATA_TYPE_BIGINT, &count, countRows, true), 0);
      if (countRows == 3) {
        int64_t negative = -1;
        ASSERT_EQ(colDataSetVal(counts->columnData, 1, (const char *)&negative, false), 0);
        colDataSetNULL(counts->columnData, 2);
      }
      input[1] = *counts;
      scltMakeRawStringRows(&output, TSDB_DATA_TYPE_VARCHAR, values, lengths, 3);
      ASSERT_EQ(repeatFunction(input, 2, &output), TSDB_CODE_SUCCESS);
      EXPECT_EQ(output.numOfRows, std::max(stringRows, countRows));
      char *first = colDataGetData(output.columnData, 0);
      EXPECT_EQ(std::string(varDataVal(first), varDataLen(first)), "abab");
      if (output.numOfRows == 3) {
        char *second = colDataGetData(output.columnData, 1);
        EXPECT_EQ(std::string(varDataVal(second), varDataLen(second)), countRows == 3 ? "" : "xx");
        EXPECT_TRUE(colDataIsNull_s(output.columnData, 2));
      }
      colDataDestroy(input[0].columnData);
      taosMemoryFree(input[0].columnData);
      scltDestroyDataBlock(counts);
      colDataDestroy(output.columnData);
      taosMemoryFree(output.columnData);
    }
  }
}

TEST(ScalarFunctionTest, repeatTranslateCapacity) {
  for (int type : {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_NCHAR}) {
    for (int bytes : {34, TSDB_MAX_FIELD_LEN}) {
      for (uint64_t count : {uint64_t(0), uint64_t(2), UINT64_MAX}) {
        SFunctionNode *func = nullptr;
        SColumnNode *column = nullptr;
        SValueNode *times = nullptr;
        ASSERT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode **)&func), 0);
        ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, (SNode **)&column), 0);
        ASSERT_EQ(nodesMakeNode(QUERY_NODE_VALUE, (SNode **)&times), 0);
        tstrncpy(func->functionName, "repeat", sizeof(func->functionName));
        column->node.resType.type = type;
        column->node.resType.bytes = bytes;
        times->node.resType.type = TSDB_DATA_TYPE_UBIGINT;
        times->node.resType.bytes = sizeof(uint64_t);
        times->datum.u = count;
        ASSERT_EQ(nodesListMakeStrictAppend(&func->pParameterList, (SNode *)column), 0);
        ASSERT_EQ(nodesListMakeStrictAppend(&func->pParameterList, (SNode *)times), 0);
        char error[256] = {};
        ASSERT_EQ(fmGetFuncInfo(func, error, sizeof(error)), TSDB_CODE_SUCCESS) << error;
        int expected = count == 0 ? VARSTR_HEADER_SIZE :
                       bytes == 34 && count == 2 ? 66 : TSDB_MAX_FIELD_LEN;
        EXPECT_EQ(func->node.resType.bytes, expected);
        nodesDestroyNode((SNode *)func);
      }
    }
  }
}

// Temporarily overrides the process-global tsCharsetCxt and guarantees it is
// restored on every path (early return, fatal ASSERT, or normal exit), unlike
// a hand-rolled save/set/restore that skips the restore on any early exit.
struct CharsetCxtGuard {
  explicit CharsetCxtGuard(void *cxt) : saved_(tsCharsetCxt) {
    tsCharsetCxt = cxt;
  }
  ~CharsetCxtGuard() { tsCharsetCxt = saved_; }
  void *saved_;
};

// GBK character 0x81 0x5C contains the ASCII backslash separator byte as its
// trail byte. Character-aware splitting must ignore that byte and split only
// at the following standalone backslash.
TEST(ScalarFunctionTest, regexpInSetDoesNotSplitInsideGbkCharacter) {
  const char setBytes[] = {(char)0x81, (char)0x5C, '\\', 'x'};
  const char sepBytes[] = {'\\'};

  SScalarParam pInput[3] = {0};
  scltMakeRawStringParam(&pInput[0], TSDB_DATA_TYPE_VARCHAR, "x", 1);
  scltMakeRawStringParam(&pInput[1], TSDB_DATA_TYPE_VARCHAR, setBytes,
                         sizeof(setBytes));
  scltMakeRawStringParam(&pInput[2], TSDB_DATA_TYPE_VARCHAR, sepBytes,
                         sizeof(sepBytes));

  // Make the server UTF-8 and the set argument's connection GBK.
  void *utf8Cxt = taosConvInit("UTF-8");
  void *gbkCxt = taosConvInit("GBK");
  ASSERT_NE(utf8Cxt, nullptr);
  ASSERT_NE(gbkCxt, nullptr);
  pInput[1].charsetCxt = gbkCxt;

  SScalarParam output = {0};
  output.columnData =
      (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  ASSERT_NE(output.columnData, nullptr);
  output.columnData->info =
      createColumnInfo(0, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
  ASSERT_EQ(colInfoDataEnsureCapacity(output.columnData, 1, true),
            TSDB_CODE_SUCCESS);

  int32_t code;
  int64_t result;
  {
    CharsetCxtGuard guard(utf8Cxt);
    code = regexpInSetFunction(pInput, 3, &output);
    result = code == TSDB_CODE_SUCCESS
                 ? *(int64_t *)colDataGetData(output.columnData, 0)
                 : 0;
  }

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(result, 2)
      << "the GBK trail byte must not be mistaken for a separator";

  colDataDestroy(pInput[0].columnData);
  taosMemoryFree(pInput[0].columnData);
  colDataDestroy(pInput[1].columnData);
  taosMemoryFree(pInput[1].columnData);
  colDataDestroy(pInput[2].columnData);
  taosMemoryFree(pInput[2].columnData);
  colDataDestroy(output.columnData);
  taosMemoryFree(output.columnData);
}

// NCHAR operands already have code-point boundaries and are split in UCS-4.
TEST(ScalarFunctionTest, regexpInSetNcharUsesConvertedServerCharset) {
  const TdUcs4 setChars[] = {0x0080, ',', 'x'};
  const char   sepBytes[] = {','};

  SScalarParam pInput[3] = {0};
  scltMakeRawStringParam(&pInput[0], TSDB_DATA_TYPE_VARCHAR, "x", 1);
  scltMakeRawStringParam(&pInput[1], TSDB_DATA_TYPE_NCHAR, setChars,
                         sizeof(setChars));
  scltMakeRawStringParam(&pInput[2], TSDB_DATA_TYPE_VARCHAR, sepBytes,
                         sizeof(sepBytes));

  void *utf8Cxt = taosConvInit("UTF-8");
  void *gbkCxt = taosConvInit("GBK");
  ASSERT_NE(utf8Cxt, nullptr);
  ASSERT_NE(gbkCxt, nullptr);
  pInput[1].charsetCxt = gbkCxt;

  SScalarParam output = {0};
  output.columnData =
      (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  ASSERT_NE(output.columnData, nullptr);
  output.columnData->info =
      createColumnInfo(0, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
  ASSERT_EQ(colInfoDataEnsureCapacity(output.columnData, 1, true),
            TSDB_CODE_SUCCESS);

  int32_t code;
  int64_t result;
  {
    CharsetCxtGuard guard(utf8Cxt);
    code = regexpInSetFunction(pInput, 3, &output);
    result = code == TSDB_CODE_SUCCESS
                 ? *(int64_t *)colDataGetData(output.columnData, 0)
                 : 0;
  }

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(result, 2);

  colDataDestroy(pInput[0].columnData);
  taosMemoryFree(pInput[0].columnData);
  colDataDestroy(pInput[1].columnData);
  taosMemoryFree(pInput[1].columnData);
  colDataDestroy(pInput[2].columnData);
  taosMemoryFree(pInput[2].columnData);
  colDataDestroy(output.columnData);
  taosMemoryFree(output.columnData);
}

TEST(ScalarFunctionTest, regexpInSetAcceptsUtf8Alias) {
  const char setBytes[] = {"\xE4\xB8\xAD,x"};

  SScalarParam pInput[3] = {0};
  scltMakeRawStringParam(&pInput[0], TSDB_DATA_TYPE_VARCHAR, "x", 1);
  scltMakeRawStringParam(&pInput[1], TSDB_DATA_TYPE_VARCHAR, setBytes,
                         sizeof(setBytes) - 1);
  scltMakeRawStringParam(&pInput[2], TSDB_DATA_TYPE_VARCHAR, ",", 1);

  void *utf8AliasCxt = taosConvInit("UTF8");
  ASSERT_NE(utf8AliasCxt, nullptr);
  pInput[1].charsetCxt = utf8AliasCxt;

  SScalarParam output = {0};
  output.columnData =
      (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  ASSERT_NE(output.columnData, nullptr);
  output.columnData->info =
      createColumnInfo(0, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
  ASSERT_EQ(colInfoDataEnsureCapacity(output.columnData, 1, true),
            TSDB_CODE_SUCCESS);

  int32_t code = regexpInSetFunction(pInput, 3, &output);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(*(int64_t *)colDataGetData(output.columnData, 0), 2);

  for (auto &param : pInput) {
    colDataDestroy(param.columnData);
    taosMemoryFree(param.columnData);
  }
  colDataDestroy(output.columnData);
  taosMemoryFree(output.columnData);
}

TEST(ScalarFunctionTest, regexpInSetReusesBroadcastGbkTokensAcrossRows) {
  const char *patterns[] = {"x", "^no$", "x"};
  const int32_t patternLens[] = {1, 4, 1};
  const char setBytes[] = {(char)0x81, (char)0x5C, '\\', 'x'};

  SScalarParam pInput[3] = {0};
  scltMakeRawStringRows(&pInput[0], TSDB_DATA_TYPE_VARCHAR, patterns,
                        patternLens, 3);
  scltMakeRawStringParam(&pInput[1], TSDB_DATA_TYPE_VARCHAR, setBytes,
                         sizeof(setBytes));
  scltMakeRawStringParam(&pInput[2], TSDB_DATA_TYPE_VARCHAR, "\\", 1);

  void *gbkCxt = taosConvInit("GBK");
  ASSERT_NE(gbkCxt, nullptr);
  pInput[1].charsetCxt = gbkCxt;

  SScalarParam output = {0};
  output.columnData =
      (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  ASSERT_NE(output.columnData, nullptr);
  output.columnData->info =
      createColumnInfo(0, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
  ASSERT_EQ(colInfoDataEnsureCapacity(output.columnData, 3, true),
            TSDB_CODE_SUCCESS);

  ASSERT_EQ(regexpInSetFunction(pInput, 3, &output), TSDB_CODE_SUCCESS);
  EXPECT_EQ(*(int64_t *)colDataGetData(output.columnData, 0), 2);
  EXPECT_EQ(*(int64_t *)colDataGetData(output.columnData, 1), 0);
  EXPECT_EQ(*(int64_t *)colDataGetData(output.columnData, 2), 2);

  for (auto &param : pInput) {
    colDataDestroy(param.columnData);
    taosMemoryFree(param.columnData);
  }
  colDataDestroy(output.columnData);
  taosMemoryFree(output.columnData);
}

TEST(ScalarFunctionTest, regexpInSetHandlesNonBroadcastCharsetSafeSets) {
  const char set0[] = {(char)0x81, (char)0x5C, '\\', 'x'};
  const char set1[] = {'a', ':', ':', ':', ':', 'x'};
  const char* sets[] = {set0, set1};
  const int32_t setLens[] = {sizeof(set0), sizeof(set1)};
  const char* seps[] = {"\\", "::"};
  const int32_t sepLens[] = {1, 2};

  SScalarParam pInput[3] = {0};
  scltMakeRawStringParam(&pInput[0], TSDB_DATA_TYPE_VARCHAR, "x", 1);
  scltMakeRawStringRows(&pInput[1], TSDB_DATA_TYPE_VARCHAR, sets, setLens, 2);
  scltMakeRawStringRows(&pInput[2], TSDB_DATA_TYPE_VARCHAR, seps, sepLens, 2);

  void* utf8Cxt = taosConvInit("UTF-8");
  void* gbkCxt = taosConvInit("GBK");
  ASSERT_NE(utf8Cxt, nullptr);
  ASSERT_NE(gbkCxt, nullptr);
  pInput[1].charsetCxt = gbkCxt;

  SScalarParam output = {0};
  output.columnData =
      (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  ASSERT_NE(output.columnData, nullptr);
  output.columnData->info =
      createColumnInfo(0, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
  ASSERT_EQ(colInfoDataEnsureCapacity(output.columnData, 2, true),
            TSDB_CODE_SUCCESS);

  int32_t code;
  {
    CharsetCxtGuard guard(utf8Cxt);
    code = regexpInSetFunction(pInput, 3, &output);
  }

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(*(int64_t *)colDataGetData(output.columnData, 0), 2);
  EXPECT_EQ(*(int64_t *)colDataGetData(output.columnData, 1), 3);

  for (auto& param : pInput) {
    colDataDestroy(param.columnData);
    taosMemoryFree(param.columnData);
  }
  colDataDestroy(output.columnData);
  taosMemoryFree(output.columnData);
}

TEST(ScalarFunctionTest, castToJsonShouldReturnConvertError) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SScalarParam input = {0};
  SScalarParam output = {0};

  input.numOfRows = 1;
  input.columnData = (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  ASSERT_NE(input.columnData, nullptr);
  input.columnData->info = createColumnInfo(1, TSDB_DATA_TYPE_INT, (int32_t)sizeof(int32_t));
  code = colInfoDataEnsureCapacity(input.columnData, 1, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  int32_t v = 43;
  code = colDataSetVal(input.columnData, 0, (const char *)&v, false);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  output.numOfRows = 1;
  output.columnData = (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  ASSERT_NE(output.columnData, nullptr);
  output.columnData->info = createColumnInfo(2, TSDB_DATA_TYPE_JSON, TSDB_MAX_JSON_TAG_LEN);
  code = colInfoDataEnsureCapacity(output.columnData, 1, true);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = castFunction(&input, 1, &output);
  ASSERT_EQ(code, TSDB_CODE_SCALAR_CONVERT_ERROR);

  colDataDestroy(input.columnData);
  taosMemoryFree(input.columnData);
  colDataDestroy(output.columnData);
  taosMemoryFree(output.columnData);
}

TEST(ScalarFunctionTest,
     castNcharUnencodableInQueryCharsetReturnsNcharConvertError) {
  const TdUcs4 emoji = 0x1F600;

  SScalarParam input = {0};
  scltMakeRawStringParam(&input, TSDB_DATA_TYPE_NCHAR, &emoji, sizeof(emoji));
  input.charsetCxt = taosConvInit("GBK");
  ASSERT_NE(input.charsetCxt, nullptr);

  SScalarParam output = {0};
  output.columnData =
      (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  ASSERT_NE(output.columnData, nullptr);
  output.columnData->info = createColumnInfo(
      0, TSDB_DATA_TYPE_VARCHAR, VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE);
  ASSERT_EQ(colInfoDataEnsureCapacity(output.columnData, 1, true),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(castFunction(&input, 1, &output),
            TSDB_CODE_SCALAR_CONVERT_NCHAR_ERROR);

  colDataDestroy(input.columnData);
  taosMemoryFree(input.columnData);
  colDataDestroy(output.columnData);
  taosMemoryFree(output.columnData);
}

// ncharTobinary is the conversion used when an NCHAR column/tag is compared with a string
// (e.g. "tag1 = 'aaaa'"). It is not declared in any public header, so forward declare it here.
extern "C" int32_t ncharTobinary(void *buf, void **out, void *charsetCxt);

TEST(ScalarFunctionTest, ncharToBinaryInvalidDataShouldReturnNcharConvertError) {
  scltInitLogFile();

  /* Build an NCHAR (UCS-4LE) vardata buffer holding an invalid code point (0xFFFFFFFF) that
   * cannot be converted to the target charset. This simulates a corrupted / illegal NCHAR
   * value being converted to string during comparison, which used to be misreported as
   * "Operation not supported between data types" (TSDB_CODE_SCALAR_CONVERT_ERROR). */
  char     buf[VARSTR_HEADER_SIZE + sizeof(uint32_t)] = {0};
  uint32_t invalidUcs4 = 0xFFFFFFFF;
  (void)memcpy(varDataVal(buf), &invalidUcs4, sizeof(invalidUcs4));
  varDataSetLen(buf, sizeof(invalidUcs4));

  void   *out = NULL;
  int32_t code = ncharTobinary(buf, &out, NULL);
  ASSERT_EQ(code, TSDB_CODE_SCALAR_CONVERT_NCHAR_ERROR);
  // on failure ncharTobinary frees the output buffer internally, so do not free it here.

  /* Sanity check: a valid NCHAR value still converts successfully. */
  void *validNchar = prepareNchar((char *)"abc");
  ASSERT_NE(validNchar, nullptr);
  out = NULL;
  code = ncharTobinary(validNchar, &out, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(out, nullptr);
  taosMemoryFree(out);
  taosMemoryFree(validNchar);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), val_double);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), -val_double);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), val_double + i);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), -(val_double + i));
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int,float after LOG:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);
  taosMemoryFree(pInput);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int,float after LOG:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);
  taosMemoryFree(pInput);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result);
    PRINTF("tiny_int,float after POW:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);
  taosMemoryFree(pInput);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
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
    ASSERT_DOUBLE_EQ(*((double *)colDataGetData(pOutput->columnData, i)), result[i]);
    PRINTF("tiny_int,float after POW:%f\n", *((double *)colDataGetData(pOutput->columnData, i)));
  }

  scltDestroyDataBlock(input[0]);
  scltDestroyDataBlock(input[1]);
  scltDestroyDataBlock(pOutput);
  taosMemoryFree(pInput);
}

TEST(ScalarStreamPlaceholderTest, runtimeAndEvaluatesSingleDurationConditionWithoutDataBlock) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.curIdx = 0;
  runtime.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam param = {};
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &param), nullptr);

  SNode *root = scltMakeAndCondition({scltMakeDurationComparison(OP_TYPE_LOWER_EQUAL, 30000)});
  ASSERT_NE(root, nullptr);

  struct {
    int64_t duration;
    bool    expected;
  } cases[] = {{30000, true}, {30001, false}};
  for (const auto &testCase : cases) {
    auto *runtimeParam = static_cast<SSTriggerCalcParam *>(taosArrayGet(runtime.pStreamPesudoFuncVals, runtime.curIdx));
    ASSERT_NE(runtimeParam, nullptr);
    runtimeParam->wduration = testCase.duration;

    const ScltBoolResult result = scltEvaluateRuntimeBool(root, &runtime);
    ASSERT_EQ(result.code, TSDB_CODE_SUCCESS);
    ASSERT_EQ(result.rows, 1);
    ASSERT_EQ(result.type, TSDB_DATA_TYPE_BOOL);
    ASSERT_FALSE(result.isNull);
    EXPECT_EQ(result.value, testCase.expected);
  }

  nodesDestroyNode(root);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, runtimeAndEvaluatesBothDurationConditionsWithoutDataBlock) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.curIdx = 0;
  runtime.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam param = {};
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &param), nullptr);

  SNode *root = scltMakeAndCondition({scltMakeDurationComparison(OP_TYPE_GREATER_EQUAL, 10000),
                                      scltMakeDurationComparison(OP_TYPE_LOWER_EQUAL, 30000)});
  ASSERT_NE(root, nullptr);

  struct {
    int64_t duration;
    bool    expected;
  } cases[] = {{20000, true}, {9999, false}, {30001, false}};
  for (const auto &testCase : cases) {
    auto *runtimeParam = static_cast<SSTriggerCalcParam *>(taosArrayGet(runtime.pStreamPesudoFuncVals, runtime.curIdx));
    ASSERT_NE(runtimeParam, nullptr);
    runtimeParam->wduration = testCase.duration;

    const ScltBoolResult result = scltEvaluateRuntimeBool(root, &runtime);
    ASSERT_EQ(result.code, TSDB_CODE_SUCCESS);
    ASSERT_EQ(result.rows, 1);
    ASSERT_EQ(result.type, TSDB_DATA_TYPE_BOOL);
    ASSERT_FALSE(result.isNull);
    EXPECT_EQ(result.value, testCase.expected);
  }

  nodesDestroyNode(root);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, readsQualifiedAncestorByLayerIndex) {
  const int32_t funcId = fmGetFuncId("_twstart");
  ASSERT_GE(funcId, 0);

  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 101;
  runtime.curIdx = 0;
  runtime.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam leaf = {};
  leaf.wstart = 300;
  leaf.wend = 399;
  leaf.notifyType = BIT_FLAG_MASK(0);
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &leaf), nullptr);
  runtime.pContextPolicy = scltMakeAncestorPolicy(runtime.groupId, runtime.curIdx);
  runtime.pAncestorContext = scltMakeAncestorContext(runtime.groupId, runtime.curIdx);

  auto evaluate = [&](int32_t targetFuncId, int32_t layerIndex, int64_t *value) {
    SNodeList  *params = nullptr;
    SValueNode *result = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&result)), TSDB_CODE_SUCCESS);
    result->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    result->node.resType.bytes = sizeof(int64_t);
    EXPECT_EQ(nodesListMakeStrictAppend(&params, reinterpret_cast<SNode *>(result)), TSDB_CODE_SUCCESS);
    SValueNode *layer = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&layer)), TSDB_CODE_SUCCESS);
    layer->node.resType.type = TSDB_DATA_TYPE_INT;
    layer->node.resType.bytes = sizeof(int32_t);
    layer->datum.i = layerIndex;
    EXPECT_EQ(nodesListMakeStrictAppend(&params, reinterpret_cast<SNode *>(layer)), TSDB_CODE_SUCCESS);

    const int32_t code = fmSetStreamPseudoFuncParamVal(targetFuncId, params, &runtime);
    if (code == TSDB_CODE_SUCCESS) *value = result->datum.i;
    nodesDestroyList(params);
    return code;
  };

  int64_t value = 0;
  ASSERT_EQ(evaluate(funcId, 0, &value), TSDB_CODE_SUCCESS);
  EXPECT_EQ(value, 100);
  ASSERT_EQ(evaluate(funcId, 2, &value), TSDB_CODE_SUCCESS);
  EXPECT_EQ(value, 300);
  ASSERT_EQ(evaluate(fmGetFuncId("_tprev_ts"), 2, &value), TSDB_CODE_SUCCESS);
  EXPECT_EQ(value, 300);
  EXPECT_EQ(evaluate(funcId, 3, &value), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  auto *param = static_cast<SStreamAncestorParamContext *>(taosArrayGet(runtime.pAncestorContext->pParamContexts, 0));
  ASSERT_NE(param, nullptr);
  auto *rootScope = static_cast<SScopeInstanceId *>(taosArrayGet(param->leafIdentity.lineage.pScopes, 0));
  auto *rootSnapshot = static_cast<SWindowAncestorSnapshot *>(taosArrayGet(param->pSnapshots, 0));
  ASSERT_NE(rootScope, nullptr);
  ASSERT_NE(rootSnapshot, nullptr);
  rootScope->triggerType = WINDOW_TYPE_INTERVAL;
  rootSnapshot->triggerType = WINDOW_TYPE_INTERVAL;
  rootSnapshot->placeholderMask = PLACE_HOLDER_PREV_TS;
  rootSnapshot->values.sliding.prevTs = 50;
  ASSERT_EQ(evaluate(fmGetFuncId("_tprev_ts"), 0, &value), TSDB_CODE_SUCCESS);
  EXPECT_EQ(value, 50);
  EXPECT_EQ(evaluate(fmGetFuncId("_twend"), 0, &value), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  rootSnapshot->triggerType = WINDOW_TYPE_PERIOD;
  EXPECT_EQ(evaluate(fmGetFuncId("_tprev_ts"), 0, &value), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  rootSnapshot->triggerType = WINDOW_TYPE_INTERVAL;
  param->paramIndex = 1;
  EXPECT_EQ(evaluate(funcId, 0, &value), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  param->paramIndex = runtime.curIdx;
  ASSERT_NE(taosArrayPush(runtime.pAncestorContext->pParamContexts, param), nullptr);
  EXPECT_EQ(evaluate(funcId, 0, &value), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  ASSERT_NE(taosArrayPop(runtime.pAncestorContext->pParamContexts), nullptr);

  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, externalWindowRangeReadsQualifiedAncestor) {
  const int32_t funcId = fmGetFuncId("_twstart");
  ASSERT_GE(funcId, 0);

  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 101;
  runtime.curIdx = 0;
  runtime.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam leaf = {};
  leaf.wstart = 300;
  leaf.wend = 399;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &leaf), nullptr);
  SSTriggerCalcParam secondLeaf = {};
  secondLeaf.wstart = 600;
  secondLeaf.wend = 699;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &secondLeaf), nullptr);
  runtime.pAncestorContext = scltMakeAncestorContext(runtime.groupId, runtime.curIdx);
  SStreamAncestorContext *secondContext = scltMakeAncestorContext(runtime.groupId, 1);
  ASSERT_NE(secondContext, nullptr);
  auto *secondParam = static_cast<SStreamAncestorParamContext *>(taosArrayGet(secondContext->pParamContexts, 0));
  ASSERT_NE(secondParam, nullptr);
  auto *secondRoot = static_cast<SWindowAncestorSnapshot *>(taosArrayGet(secondParam->pSnapshots, 0));
  ASSERT_NE(secondRoot, nullptr);
  secondRoot->values.window.start = 400;
  ASSERT_NE(taosArrayPush(runtime.pAncestorContext->pParamContexts, secondParam), nullptr);
  ASSERT_NE(taosArrayPop(secondContext->pParamContexts), nullptr);
  tDestroyStreamAncestorContext(&secondContext);

  auto makePlaceholder = [&]() -> SNode * {
    SFunctionNode *func = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION, reinterpret_cast<SNode **>(&func)), TSDB_CODE_SUCCESS);
    if (func == nullptr) return nullptr;
    func->funcId = funcId;
    func->funcType = fmGetFuncTypeFromId(funcId);
    func->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    func->node.resType.bytes = sizeof(int64_t);

    SValueNode *result = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&result)), TSDB_CODE_SUCCESS);
    if (result == nullptr) {
      nodesDestroyNode(reinterpret_cast<SNode *>(func));
      return nullptr;
    }
    result->node.resType = func->node.resType;
    EXPECT_EQ(nodesListMakeStrictAppend(&func->pParameterList, reinterpret_cast<SNode *>(result)), TSDB_CODE_SUCCESS);

    SValueNode *layer = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&layer)), TSDB_CODE_SUCCESS);
    if (layer == nullptr) {
      nodesDestroyNode(reinterpret_cast<SNode *>(func));
      return nullptr;
    }
    layer->node.resType.type = TSDB_DATA_TYPE_INT;
    layer->node.resType.bytes = sizeof(int32_t);
    layer->datum.i = 0;
    EXPECT_EQ(nodesListMakeStrictAppend(&func->pParameterList, reinterpret_cast<SNode *>(layer)), TSDB_CODE_SUCCESS);
    return reinterpret_cast<SNode *>(func);
  };

  auto makePrimaryTs = []() -> SNode * {
    SColumnNode *column = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&column)), TSDB_CODE_SUCCESS);
    if (column == nullptr) return nullptr;
    column->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    column->node.resType.bytes = sizeof(int64_t);
    column->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    return reinterpret_cast<SNode *>(column);
  };

  SNode *start = nullptr;
  SNode *end = nullptr;
  ASSERT_EQ(scltMakeOpNode(&start, OP_TYPE_GREATER_EQUAL, TSDB_DATA_TYPE_BOOL, makePrimaryTs(), makePlaceholder()),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeOpNode(&end, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, makePrimaryTs(), makePlaceholder()),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(start, nullptr);
  ASSERT_NE(end, nullptr);

  STimeRangeNode range = {};
  range.type = QUERY_NODE_TIME_RANGE;
  range.pStart = start;
  range.pEnd = end;
  range.needCalc = true;
  SExtWinTimeWindow wins[2] = {};
  ASSERT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, wins), TSDB_CODE_SUCCESS);
  EXPECT_EQ(wins[0].tw.skey, 100);
  EXPECT_EQ(wins[0].tw.ekey, 101);
  EXPECT_EQ(wins[1].tw.skey, 400);
  EXPECT_EQ(wins[1].tw.ekey, 401);

  nodesDestroyNode(start);
  nodesDestroyNode(end);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, autoTwendJoinRangeIncludesOnlyDegenerateStart) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.pStreamPesudoFuncVals = taosArrayInit(2, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam degenerate = {};
  degenerate.wstart = 100;
  degenerate.wend = 100;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &degenerate), nullptr);
  SSTriggerCalcParam nondegenerate = {};
  nondegenerate.wstart = 200;
  nondegenerate.wend = 201;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &nondegenerate), nullptr);

  auto makePlaceholder = [](const char *name) -> SNode * {
    const int32_t funcId = fmGetFuncId(name);
    EXPECT_GE(funcId, 0);

    SFunctionNode *func = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION, reinterpret_cast<SNode **>(&func)), TSDB_CODE_SUCCESS);
    if (func == nullptr) return nullptr;
    func->funcId = funcId;
    func->funcType = fmGetFuncTypeFromId(funcId);
    func->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    func->node.resType.bytes = sizeof(int64_t);

    SValueNode *result = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&result)), TSDB_CODE_SUCCESS);
    if (result == nullptr) {
      nodesDestroyNode(reinterpret_cast<SNode *>(func));
      return nullptr;
    }
    result->node.resType = func->node.resType;
    EXPECT_EQ(nodesListMakeStrictAppend(&func->pParameterList, reinterpret_cast<SNode *>(result)), TSDB_CODE_SUCCESS);
    return reinterpret_cast<SNode *>(func);
  };

  auto makePrimaryTs = []() -> SNode * {
    SColumnNode *column = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&column)), TSDB_CODE_SUCCESS);
    if (column == nullptr) return nullptr;
    column->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    column->node.resType.bytes = sizeof(int64_t);
    column->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    return reinterpret_cast<SNode *>(column);
  };

  SNode *start = nullptr;
  SNode *end = nullptr;
  ASSERT_EQ(
      scltMakeOpNode(&start, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, makePrimaryTs(), makePlaceholder("_twstart")),
      TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeOpNode(&end, OP_TYPE_LOWER_EQUAL, TSDB_DATA_TYPE_BOOL, makePrimaryTs(), makePlaceholder("_twend")),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(start, nullptr);
  ASSERT_NE(end, nullptr);

  reinterpret_cast<SOperatorNode *>(start)->flag = OPERATOR_FLAG_STREAM_EXT_JOIN_AUTO_RANGE;
  STimeRangeNode range = {};
  range.type = QUERY_NODE_TIME_RANGE;
  range.pStart = start;
  range.pEnd = end;
  range.needCalc = true;
  SExtWinTimeWindow autoWins[2] = {};
  ASSERT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, autoWins), TSDB_CODE_SUCCESS);
  EXPECT_EQ(autoWins[0].tw.skey, 100);
  EXPECT_EQ(autoWins[0].tw.ekey, 101);
  EXPECT_EQ(autoWins[1].tw.skey, 201);
  EXPECT_EQ(autoWins[1].tw.ekey, 202);

  reinterpret_cast<SOperatorNode *>(start)->flag = 0;
  SExtWinTimeWindow explicitWins[2] = {};
  ASSERT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, explicitWins), TSDB_CODE_SUCCESS);
  EXPECT_EQ(explicitWins[0].tw.skey, 101);
  EXPECT_EQ(explicitWins[0].tw.ekey, 101);

  nodesDestroyNode(start);
  nodesDestroyNode(end);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, externalWindowUsesMaxLowerAndMinUpper) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam leaf = {};
  leaf.wstart = 100;
  leaf.wend = 200;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &leaf), nullptr);

  SNode *start = scltMakeAndCondition(
      {scltMakeExternalBound(OP_TYPE_GREATER_EQUAL, scltMakeTimestampOffset("_twstart", -1, OP_TYPE_SUB, 10)),
       scltMakeExternalBound(OP_TYPE_GREATER_THAN, scltMakeTimestampOffset("_twstart", -1, OP_TYPE_SUB, 5))});
  SNode *end = scltMakeAndCondition(
      {scltMakeExternalBound(OP_TYPE_LOWER_THAN, scltMakeTimestampOffset("_twend", -1, OP_TYPE_ADD, 10)),
       scltMakeExternalBound(OP_TYPE_LOWER_EQUAL, scltMakeTimestampOffset("_twend", -1, OP_TYPE_ADD, 5))});
  ASSERT_NE(start, nullptr);
  ASSERT_NE(end, nullptr);

  STimeRangeNode range = {};
  range.type = QUERY_NODE_TIME_RANGE;
  range.pStart = start;
  range.pEnd = end;
  range.needCalc = true;
  SExtWinTimeWindow win = {};
  ASSERT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, &win), TSDB_CODE_SUCCESS);
  EXPECT_EQ(win.tw.skey, 96);
  EXPECT_EQ(win.tw.ekey, 206);

  nodesDestroyNode(start);
  nodesDestroyNode(end);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, externalWindowAllowsLeafAncestorLayerMixing) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 101;
  runtime.curIdx = 0;
  runtime.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam leaf = {};
  leaf.wstart = 125;
  leaf.wend = 399;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &leaf), nullptr);
  runtime.pAncestorContext = scltMakeAncestorContext(runtime.groupId, runtime.curIdx);

  SNode *start = scltMakeExternalBound(OP_TYPE_GREATER_EQUAL, scltMakeStreamPlaceholder("_twstart", -1));
  SNode *end = scltMakeExternalBound(OP_TYPE_LOWER_EQUAL, scltMakeStreamPlaceholder("_twend", 1));
  ASSERT_NE(start, nullptr);
  ASSERT_NE(end, nullptr);

  STimeRangeNode range = {};
  range.type = QUERY_NODE_TIME_RANGE;
  range.pStart = start;
  range.pEnd = end;
  range.needCalc = true;
  SExtWinTimeWindow win = {};
  ASSERT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, &win), TSDB_CODE_SUCCESS);
  EXPECT_EQ(win.tw.skey, 125);
  EXPECT_EQ(win.tw.ekey, 250);

  nodesDestroyNode(start);
  nodesDestroyNode(end);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, externalWindowAppliesRepresentableUnsignedOffset) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam leaf = {};
  leaf.wstart = 100;
  leaf.wend = 200;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &leaf), nullptr);

  const uint64_t offset = 7;
  SNode         *start =
      scltMakeExternalBound(OP_TYPE_GREATER_EQUAL,
                            scltMakeTypedTimestampOffset("_twstart", -1, OP_TYPE_ADD, TSDB_DATA_TYPE_UBIGINT, &offset));
  SNode *end = scltMakeExternalBound(OP_TYPE_LOWER_THAN, scltMakeStreamPlaceholder("_twend"));
  ASSERT_NE(start, nullptr);
  ASSERT_NE(end, nullptr);

  STimeRangeNode range = {};
  range.type = QUERY_NODE_TIME_RANGE;
  range.pStart = start;
  range.pEnd = end;
  range.needCalc = true;
  SExtWinTimeWindow win = {};
  ASSERT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, &win), TSDB_CODE_SUCCESS);
  EXPECT_EQ(win.tw.skey, 107);
  EXPECT_EQ(win.tw.ekey, 200);

  nodesDestroyNode(start);
  nodesDestroyNode(end);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, externalWindowRejectsUnsignedOffsetBeyondTimestampRange) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam leaf = {};
  leaf.wstart = 0;
  leaf.wend = 200;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &leaf), nullptr);

  const uint64_t offset = static_cast<uint64_t>(INT64_MAX) + 1;
  SNode         *start =
      scltMakeExternalBound(OP_TYPE_GREATER_EQUAL,
                            scltMakeTypedTimestampOffset("_twstart", -1, OP_TYPE_ADD, TSDB_DATA_TYPE_UBIGINT, &offset));
  SNode *end = scltMakeExternalBound(OP_TYPE_LOWER_THAN, scltMakeStreamPlaceholder("_twend"));
  ASSERT_NE(start, nullptr);
  ASSERT_NE(end, nullptr);

  STimeRangeNode range = {};
  range.type = QUERY_NODE_TIME_RANGE;
  range.pStart = start;
  range.pEnd = end;
  range.needCalc = true;
  SExtWinTimeWindow win = {};
  EXPECT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, &win), TSDB_CODE_QRY_INVALID_INPUT);

  nodesDestroyNode(start);
  nodesDestroyNode(end);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, externalWindowRejectsDifferentParamContext) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 101;
  runtime.curIdx = 0;
  runtime.pStreamPesudoFuncVals = taosArrayInit(2, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam first = {};
  first.wstart = 300;
  first.wend = 399;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &first), nullptr);
  SSTriggerCalcParam second = {};
  second.wstart = 600;
  second.wend = 699;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &second), nullptr);
  runtime.pAncestorContext = scltMakeAncestorContext(runtime.groupId, 0);

  SNode *start = scltMakeExternalBound(OP_TYPE_GREATER_EQUAL, scltMakeStreamPlaceholder("_twstart", 0));
  SNode *end = scltMakeExternalBound(OP_TYPE_LOWER_THAN, scltMakeStreamPlaceholder("_twend", 0));
  ASSERT_NE(start, nullptr);
  ASSERT_NE(end, nullptr);

  STimeRangeNode range = {};
  range.type = QUERY_NODE_TIME_RANGE;
  range.pStart = start;
  range.pEnd = end;
  range.needCalc = true;
  SExtWinTimeWindow wins[2] = {};
  EXPECT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, wins), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  nodesDestroyNode(start);
  nodesDestroyNode(end);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, externalWindowPreservesEmptyHalfOpenRange) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam leaf = {};
  leaf.wstart = 200;
  leaf.wend = 100;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &leaf), nullptr);

  SNode *start = scltMakeExternalBound(OP_TYPE_GREATER_EQUAL, scltMakeStreamPlaceholder("_twstart"));
  SNode *end = scltMakeExternalBound(OP_TYPE_LOWER_THAN, scltMakeStreamPlaceholder("_twend"));
  ASSERT_NE(start, nullptr);
  ASSERT_NE(end, nullptr);

  STimeRangeNode range = {};
  range.type = QUERY_NODE_TIME_RANGE;
  range.pStart = start;
  range.pEnd = end;
  range.needCalc = true;
  SExtWinTimeWindow win = {};
  ASSERT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, &win), TSDB_CODE_SUCCESS);
  EXPECT_EQ(win.tw.skey, 200);
  EXPECT_EQ(win.tw.ekey, 100);

  nodesDestroyNode(start);
  nodesDestroyNode(end);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, externalWindowRejectsBoundaryOverflow) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam leaf = {};
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &leaf), nullptr);
  auto *param = static_cast<SSTriggerCalcParam *>(taosArrayGet(runtime.pStreamPesudoFuncVals, 0));
  ASSERT_NE(param, nullptr);

  auto expectInvalid = [&](const char *scenario, SNode *start, SNode *end) {
    SCOPED_TRACE(scenario);
    ASSERT_NE(start, nullptr);
    ASSERT_NE(end, nullptr);
    STimeRangeNode range = {};
    range.type = QUERY_NODE_TIME_RANGE;
    range.pStart = start;
    range.pEnd = end;
    range.needCalc = true;
    SExtWinTimeWindow win = {};
    EXPECT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, &win), TSDB_CODE_QRY_INVALID_INPUT);
    nodesDestroyNode(start);
    nodesDestroyNode(end);
  };

  param->wstart = INT64_MAX;
  param->wend = 0;
  expectInvalid("offset addition overflow",
                scltMakeExternalBound(OP_TYPE_GREATER_EQUAL, scltMakeTimestampOffset("_twstart", -1, OP_TYPE_ADD, 1)),
                scltMakeExternalBound(OP_TYPE_LOWER_THAN, scltMakeStreamPlaceholder("_twend")));
  expectInvalid("lower tick overflow",
                scltMakeExternalBound(OP_TYPE_GREATER_THAN, scltMakeStreamPlaceholder("_twstart")),
                scltMakeExternalBound(OP_TYPE_LOWER_THAN, scltMakeStreamPlaceholder("_twend")));

  param->wstart = 0;
  param->wend = INT64_MAX;
  expectInvalid("upper tick overflow",
                scltMakeExternalBound(OP_TYPE_GREATER_EQUAL, scltMakeStreamPlaceholder("_twstart")),
                scltMakeExternalBound(OP_TYPE_LOWER_EQUAL, scltMakeStreamPlaceholder("_twend")));
  expectInvalid(
      "INT64_MIN subtraction",
      scltMakeExternalBound(OP_TYPE_GREATER_EQUAL, scltMakeTimestampOffset("_twstart", -1, OP_TYPE_SUB, INT64_MIN)),
      scltMakeExternalBound(OP_TYPE_LOWER_THAN, scltMakeStreamPlaceholder("_twend")));

  tDestroyStRtFuncInfo(&runtime);
}

TEST(ScalarStreamPlaceholderTest, externalWindowEvaluatesEachGroupLineageSeparately) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 101;
  runtime.curIdx = 0;
  runtime.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(runtime.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam leaf = {};
  leaf.wstart = 300;
  leaf.wend = 399;
  ASSERT_NE(taosArrayPush(runtime.pStreamPesudoFuncVals, &leaf), nullptr);
  runtime.pAncestorContext = scltMakeAncestorContext(101, 0);

  SStreamAncestorContext *secondContext = scltMakeAncestorContext(202, 0);
  ASSERT_NE(secondContext, nullptr);
  auto *secondParam = static_cast<SStreamAncestorParamContext *>(taosArrayGet(secondContext->pParamContexts, 0));
  ASSERT_NE(secondParam, nullptr);
  auto *secondRoot = static_cast<SWindowAncestorSnapshot *>(taosArrayGet(secondParam->pSnapshots, 0));
  ASSERT_NE(secondRoot, nullptr);
  secondRoot->values.window.start = 400;
  secondRoot->values.window.end = 499;
  ASSERT_NE(taosArrayPush(runtime.pAncestorContext->pParamContexts, secondParam), nullptr);
  ASSERT_NE(taosArrayPop(secondContext->pParamContexts), nullptr);
  tDestroyStreamAncestorContext(&secondContext);

  SNode *start = scltMakeExternalBound(OP_TYPE_GREATER_EQUAL, scltMakeStreamPlaceholder("_twstart", 0));
  SNode *end = scltMakeExternalBound(OP_TYPE_LOWER_EQUAL, scltMakeStreamPlaceholder("_twend", 0));
  ASSERT_NE(start, nullptr);
  ASSERT_NE(end, nullptr);
  STimeRangeNode range = {};
  range.type = QUERY_NODE_TIME_RANGE;
  range.pStart = start;
  range.pEnd = end;
  range.needCalc = true;

  SExtWinTimeWindow firstWin = {};
  ASSERT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, &firstWin), TSDB_CODE_SUCCESS);
  EXPECT_EQ(firstWin.tw.skey, 100);
  EXPECT_EQ(firstWin.tw.ekey, 150);

  runtime.groupId = 202;
  SExtWinTimeWindow secondWin = {};
  ASSERT_EQ(scalarCalculateExtWinsTimeRange(&range, &runtime, &secondWin), TSDB_CODE_SUCCESS);
  EXPECT_EQ(secondWin.tw.skey, 400);
  EXPECT_EQ(secondWin.tw.ekey, 500);

  nodesDestroyNode(start);
  nodesDestroyNode(end);
  tDestroyStRtFuncInfo(&runtime);
}

constexpr int32_t kMaxCorrelationMockCalls = 8;

struct CorrelationScalarMock {
  int32_t seen[kMaxCorrelationMockCalls] = {};
  int32_t seenOther[kMaxCorrelationMockCalls] = {};
  int32_t calls = 0;
  int32_t expectedParamCount = 1;
  int32_t failOnCall = 0;
  int32_t nullOnValue = 0;
  const void *sourceData = nullptr;
  bool copiedParams = true;
  int64_t chargedBytes = 0;
  int64_t peakBytes = 0;
  int64_t limitBytes = 0;
  const SArray *pInheritedParams = nullptr;
  const SArray *expectedSources = nullptr;
  bool sourcesMatch = true;
  uint64_t recordedRows = 0;
  uint64_t recordedCalls = 0;
  int64_t recordedElapsedUs = 0;
  const void *varSources[kMaxCorrelationMockCalls] = {};
  bool varCopied = true;
};

static int32_t recordCorrelationStatsMock(void *pContext, int32_t templateId, bool called, int64_t elapsedUs) {
  auto *pMock = static_cast<CorrelationScalarMock *>(pContext);
  if (0 != templateId) {
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  ++pMock->recordedRows;
  pMock->recordedCalls += called ? 1 : 0;
  pMock->recordedElapsedUs += elapsedUs;
  return TSDB_CODE_SUCCESS;
}

static const SArray *getInheritedCorrelationParamsMock(const SCorrelationTaskSession *pSession) {
  auto *pMock = reinterpret_cast<const CorrelationScalarMock *>(pSession);
  return pMock->pInheritedParams;
}

static int32_t reserveCorrelationResultMock(SCorrelationTaskSession *pSession, int64_t bytes) {
  auto *pMock = reinterpret_cast<CorrelationScalarMock *>(pSession);
  if (pMock->limitBytes > 0 && pMock->chargedBytes + bytes > pMock->limitBytes) {
    return TSDB_CODE_QRY_QUERY_MEM_EXHAUSTED;
  }
  pMock->chargedBytes += bytes;
  pMock->peakBytes = TMAX(pMock->peakBytes, pMock->chargedBytes);
  return TSDB_CODE_SUCCESS;
}

static void releaseCorrelationResultMock(SCorrelationTaskSession *pSession, int64_t bytes) {
  auto *pMock = reinterpret_cast<CorrelationScalarMock *>(pSession);
  ASSERT_LE(bytes, pMock->chargedBytes);
  pMock->chargedBytes -= bytes;
}

static int32_t invokeCorrelationScalarMock(SCorrelationTaskSession *pSession,
                                           const SCorrelationScalarRequest *pRequest,
                                           SValueNode **ppResult) {
  auto *pMock = reinterpret_cast<CorrelationScalarMock *>(pSession);
  pMock->sourcesMatch &= pRequest->pSubquerySources == pMock->expectedSources;
  if (pRequest->templateId != 0 || nullptr == pRequest->pParams ||
      taosArrayGetSize(pRequest->pParams) != pMock->expectedParamCount) {
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  if (pMock->calls >= kMaxCorrelationMockCalls) {
    ADD_FAILURE() << "CorrelationScalarMock call capacity exceeded";
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  auto *pValue = static_cast<const SCorrelationParamValue *>(taosArrayGet(pRequest->pParams, 0));
  if (nullptr == pValue || pValue->isNull || pValue->length != sizeof(int32_t) || nullptr == pValue->pData) {
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  pMock->copiedParams &= pValue->pData != pMock->sourceData;
  int32_t value = 0;
  memcpy(&value, pValue->pData, sizeof(value));
  if (pMock->expectedParamCount == 2) {
    auto *pOther = static_cast<const SCorrelationParamValue *>(taosArrayGet(pRequest->pParams, 1));
    if (nullptr == pOther || pOther->paramId != 1 || pOther->isNull ||
        pOther->length != sizeof(int32_t) || nullptr == pOther->pData) {
      return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
    }
    memcpy(&pMock->seenOther[pMock->calls], pOther->pData, sizeof(int32_t));
  }
  pMock->seen[pMock->calls++] = value;
  if (pMock->calls == pMock->failOnCall) {
    return TSDB_CODE_QRY_SUBQ_EXEC_ERROR;
  }

  SNode *pNode = nullptr;
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, &pNode);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  auto *pResult = reinterpret_cast<SValueNode *>(pNode);
  pResult->node.resType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  pResult->isNull = value == pMock->nullOnValue;
  pResult->typeData = value * 10;
  *ppResult = pResult;
  return TSDB_CODE_SUCCESS;
}

static int32_t invokeCorrelationVarMock(SCorrelationTaskSession *pSession,
                                        const SCorrelationScalarRequest *pRequest,
                                        SValueNode **ppResult) {
  auto *pMock = reinterpret_cast<CorrelationScalarMock *>(pSession);
  if (nullptr == pRequest->pParams || taosArrayGetSize(pRequest->pParams) != 1) {
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  if (pMock->calls >= kMaxCorrelationMockCalls) {
    ADD_FAILURE() << "CorrelationScalarMock call capacity exceeded";
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  auto *pValue = static_cast<const SCorrelationParamValue *>(taosArrayGet(pRequest->pParams, 0));
  if (nullptr == pValue || pValue->dataType.type != TSDB_DATA_TYPE_VARCHAR) {
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  int32_t row = pMock->calls++;
  if (!pValue->isNull) {
    pMock->varCopied &= pValue->pData != pMock->varSources[row];
  }
  if (!pValue->isNull && (pValue->length <= VARSTR_HEADER_SIZE || nullptr == pValue->pData ||
                          varDataTLen(pValue->pData) != pValue->length)) {
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  if (pValue->isNull && (pValue->length != 0 || nullptr != pValue->pData)) {
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  SNode *pNode = nullptr;
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, &pNode);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  auto *pResult = reinterpret_cast<SValueNode *>(pNode);
  pResult->node.resType = {.type = TSDB_DATA_TYPE_VARCHAR, .bytes = 32};
  pResult->isNull = pValue->isNull;
  if (!pValue->isNull) {
    pResult->datum.p = static_cast<char *>(taosMemoryMalloc(pValue->length));
    if (nullptr == pResult->datum.p) {
      nodesDestroyNode(pNode);
      return terrno;
    }
    memcpy(pResult->datum.p, pValue->pData, pValue->length);
  }
  *ppResult = pResult;
  return TSDB_CODE_SUCCESS;
}

static SCorrelatedSubqueryNode *makeCorrelationScalarNode(int64_t blockId) {
  SNode *pNode = nullptr;
  if (TSDB_CODE_SUCCESS != nodesMakeNode(QUERY_NODE_CORRELATED_SUBQUERY, &pNode)) {
    return nullptr;
  }
  auto *pCall = reinterpret_cast<SCorrelatedSubqueryNode *>(pNode);
  pCall->node.resType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  pCall->templateId = 0;
  pCall->exprId = 1;
  pCall->scopeId = 2;
  pCall->parentScopeId = 1;

  SNode *pBindingNode = nullptr;
  if (TSDB_CODE_SUCCESS != nodesMakeNode(QUERY_NODE_CORRELATION_PARAM_BINDING, &pBindingNode)) {
    nodesDestroyNode(pNode);
    return nullptr;
  }
  auto *pBinding = reinterpret_cast<SCorrelationParamBindingNode *>(pBindingNode);
  pBinding->paramId = 0;
  pBinding->dataType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  pBinding->sourceKind = CORRELATION_SOURCE_BLOCK;
  pBinding->sourceBlockId = blockId;
  pBinding->sourceSlotId = 0;
  if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pCall->pParamBindings, pBindingNode)) {
    nodesDestroyNode(pNode);
    return nullptr;
  }
  return pCall;
}

TEST(CorrelationScalarTest, UsesExactBlockAndOriginalRows) {
  SArray *pBlocks = taosArrayInit(2, POINTER_BYTES);
  ASSERT_NE(pBlocks, nullptr);
  SColumnInfo columnInfo = createColumnInfo(1, TSDB_DATA_TYPE_INT, sizeof(int32_t));
  int16_t blockId = -1, slotId = -1;
  ASSERT_EQ(scltAppendReservedSlot(pBlocks, &blockId, &slotId, true, 3, &columnInfo), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltAppendReservedSlot(pBlocks, &blockId, &slotId, true, 3, &columnInfo), TSDB_CODE_SUCCESS);
  auto *pFirst = static_cast<SSDataBlock *>(taosArrayGetP(pBlocks, 0));
  auto *pSecond = static_cast<SSDataBlock *>(taosArrayGetP(pBlocks, 1));
  auto *pFirstCol = static_cast<SColumnInfoData *>(taosArrayGet(pFirst->pDataBlock, 0));
  auto *pSecondCol = static_cast<SColumnInfoData *>(taosArrayGet(pSecond->pDataBlock, 0));
  for (int32_t row = 0; row < 3; ++row) {
    int32_t first = 100 + row, second = row + 1;
    ASSERT_EQ(colDataSetVal(pFirstCol, row, reinterpret_cast<char *>(&first), false), TSDB_CODE_SUCCESS);
    ASSERT_EQ(colDataSetVal(pSecondCol, row, reinterpret_cast<char *>(&second), false), TSDB_CODE_SUCCESS);
  }

  SCorrelatedSubqueryNode *pCall = makeCorrelationScalarNode(pSecond->info.id.blockId);
  ASSERT_NE(pCall, nullptr);
  SNode *pOtherNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CORRELATION_PARAM_BINDING, &pOtherNode), TSDB_CODE_SUCCESS);
  auto *pOtherBinding = reinterpret_cast<SCorrelationParamBindingNode *>(pOtherNode);
  pOtherBinding->paramId = 1;
  pOtherBinding->dataType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  pOtherBinding->sourceKind = CORRELATION_SOURCE_BLOCK;
  pOtherBinding->sourceBlockId = pFirst->info.id.blockId;
  pOtherBinding->sourceSlotId = 0;
  ASSERT_EQ(nodesListStrictAppend(pCall->pParamBindings, pOtherNode), TSDB_CODE_SUCCESS);
  CorrelationScalarMock mock = {};
  mock.expectedParamCount = 2;
  mock.sourceData = pSecondCol->pData;
  mock.nullOnValue = 2;
  SCorrelationRuntimeOps ops = {};
  ops.invokeScalar = invokeCorrelationScalarMock;
  ops.reserveResultMemory = reserveCorrelationResultMock;
  ops.releaseResultMemory = releaseCorrelationResultMock;
  SCorrelationEvalContext correlation = {
      .pSession = reinterpret_cast<SCorrelationTaskSession *>(&mock), .pOps = &ops};
  SScalarExtraInfo extra = {};
  extra.pCorrelation = &correlation;
  SColumnInfoData result = {};
  result.info = columnInfo;
  SScalarParam output = {};
  output.columnData = &result;

  ASSERT_EQ(scalarCalculate(reinterpret_cast<SNode *>(pCall), pBlocks, &output, &extra), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mock.calls, 3);
  EXPECT_EQ(mock.seen[0], 1);
  EXPECT_EQ(mock.seen[1], 2);
  EXPECT_EQ(mock.seen[2], 3);
  EXPECT_EQ(mock.seenOther[0], 100);
  EXPECT_EQ(mock.seenOther[1], 101);
  EXPECT_EQ(mock.seenOther[2], 102);
  EXPECT_TRUE(mock.copiedParams);
  EXPECT_GT(mock.peakBytes, 0);
  EXPECT_EQ(mock.chargedBytes, 0);
  EXPECT_EQ(output.numOfRows, 3);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(&result, 0)), 10);
  EXPECT_TRUE(colDataIsNull(&result, 3, 1, nullptr));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(&result, 2)), 30);

  mock.calls = 0;
  correlation.hasRowRange = true;
  correlation.rowStart = 0;
  correlation.rowEnd = 0;
  correlation.hasJoinRows = true;
  correlation.leftBlockId = pFirst->info.id.blockId;
  correlation.rightBlockId = pSecond->info.id.blockId;
  correlation.leftRow = 1;
  correlation.rightRow = 2;
  ASSERT_EQ(scalarCalculate(reinterpret_cast<SNode *>(pCall), pBlocks, &output, &extra), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mock.calls, 1);
  EXPECT_EQ(mock.seen[0], 3);
  EXPECT_EQ(mock.seenOther[0], 101);
  EXPECT_EQ(mock.chargedBytes, 0);

  colDataDestroy(&result);
  nodesDestroyNode(reinterpret_cast<SNode *>(pCall));
  taosArrayDestroyEx(pBlocks, scltFreeDataBlock);
}

TEST(CorrelationScalarTest, RangePreservesOtherRowsAndFailureStopsParent) {
  SArray *pBlocks = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(pBlocks, nullptr);
  SColumnInfo columnInfo = createColumnInfo(1, TSDB_DATA_TYPE_INT, sizeof(int32_t));
  int16_t blockId = -1, slotId = -1;
  ASSERT_EQ(scltAppendReservedSlot(pBlocks, &blockId, &slotId, true, 4, &columnInfo), TSDB_CODE_SUCCESS);
  auto *pBlock = static_cast<SSDataBlock *>(taosArrayGetP(pBlocks, 0));
  auto *pSource = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  for (int32_t row = 0; row < 4; ++row) {
    int32_t value = row + 4;
    ASSERT_EQ(colDataSetVal(pSource, row, reinterpret_cast<char *>(&value), false), TSDB_CODE_SUCCESS);
  }

  SCorrelatedSubqueryNode *pCall = makeCorrelationScalarNode(pBlock->info.id.blockId);
  ASSERT_NE(pCall, nullptr);
  CorrelationScalarMock mock = {};
  SCorrelationRuntimeOps ops = {};
  ops.invokeScalar = invokeCorrelationScalarMock;
  ops.reserveResultMemory = reserveCorrelationResultMock;
  ops.releaseResultMemory = releaseCorrelationResultMock;
  SCorrelationEvalContext correlation = {
      .pSession = reinterpret_cast<SCorrelationTaskSession *>(&mock),
      .pOps = &ops,
      .hasRowRange = true,
      .rowStart = 1,
      .rowEnd = 1};
  SScalarExtraInfo extra = {};
  extra.pCorrelation = &correlation;
  SColumnInfoData result = {};
  result.info = columnInfo;
  ASSERT_EQ(colInfoDataEnsureCapacity(&result, 4, true), TSDB_CODE_SUCCESS);
  for (int32_t row = 0; row < 4; ++row) {
    int32_t value = 99;
    ASSERT_EQ(colDataSetVal(&result, row, reinterpret_cast<char *>(&value), false), TSDB_CODE_SUCCESS);
  }
  SScalarParam output = {};
  output.columnData = &result;
  output.numOfRows = 4;
  ASSERT_EQ(scalarCalculate(reinterpret_cast<SNode *>(pCall), pBlocks, &output, &extra), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mock.calls, 1);
  EXPECT_EQ(mock.seen[0], 5);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(&result, 0)), 99);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(&result, 1)), 50);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(&result, 2)), 99);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(&result, 3)), 99);
  EXPECT_EQ(mock.chargedBytes, 0);
  colDataDestroy(&result);

  SColumnInfoData freshResult = {};
  freshResult.info = columnInfo;
  SScalarParam freshOutput = {};
  freshOutput.columnData = &freshResult;
  ASSERT_EQ(scalarCalculate(reinterpret_cast<SNode *>(pCall), pBlocks, &freshOutput, &extra), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(colDataIsNull(&freshResult, 4, 0, nullptr));
  EXPECT_FALSE(colDataIsNull(&freshResult, 4, 1, nullptr));
  EXPECT_TRUE(colDataIsNull(&freshResult, 4, 2, nullptr));
  EXPECT_TRUE(colDataIsNull(&freshResult, 4, 3, nullptr));
  colDataDestroy(&freshResult);

  correlation.hasRowRange = false;
  mock.calls = 0;
  mock.nullOnValue = 5;
  mock.failOnCall = 3;
  SNode *pColumnNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &pColumnNode), TSDB_CODE_SUCCESS);
  auto *pColumn = reinterpret_cast<SColumnNode *>(pColumnNode);
  pColumn->node.resType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  pColumn->dataBlockId = pBlock->info.id.blockId;
  pColumn->slotId = 0;
  SNode *pParent = nullptr;
  ASSERT_EQ(scltMakeOpNode(&pParent, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pColumnNode,
                          reinterpret_cast<SNode *>(pCall)), TSDB_CODE_SUCCESS);
  SColumnInfoData parentResult = {};
  parentResult.info = createColumnInfo(2, TSDB_DATA_TYPE_BOOL, sizeof(bool));
  SScalarParam parentOutput = {};
  parentOutput.columnData = &parentResult;
  EXPECT_EQ(scalarCalculate(pParent, pBlocks, &parentOutput, &extra), TSDB_CODE_QRY_SUBQ_EXEC_ERROR);
  EXPECT_EQ(mock.calls, 3);
  EXPECT_EQ(mock.seen[0], 4);
  EXPECT_EQ(mock.seen[1], 5);
  EXPECT_EQ(mock.seen[2], 6);
  EXPECT_EQ(parentOutput.numOfRows, 0);
  EXPECT_EQ(parentResult.pData, nullptr);
  EXPECT_EQ(mock.chargedBytes, 0);

  colDataDestroy(&parentResult);
  nodesDestroyNode(pParent);
  taosArrayDestroyEx(pBlocks, scltFreeDataBlock);
}

TEST(CorrelationScalarTest, EmptyBlockDoesNotInvoke) {
  SArray *pBlocks = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(pBlocks, nullptr);
  SColumnInfo columnInfo = createColumnInfo(1, TSDB_DATA_TYPE_INT, sizeof(int32_t));
  int16_t blockId = -1, slotId = -1;
  ASSERT_EQ(scltAppendReservedSlot(pBlocks, &blockId, &slotId, true, 1, &columnInfo), TSDB_CODE_SUCCESS);
  auto *pBlock = static_cast<SSDataBlock *>(taosArrayGetP(pBlocks, 0));
  pBlock->info.rows = 0;
  SCorrelatedSubqueryNode *pCall = makeCorrelationScalarNode(pBlock->info.id.blockId);
  ASSERT_NE(pCall, nullptr);
  CorrelationScalarMock mock = {};
  SCorrelationRuntimeOps ops = {};
  ops.invokeScalar = invokeCorrelationScalarMock;
  ops.reserveResultMemory = reserveCorrelationResultMock;
  ops.releaseResultMemory = releaseCorrelationResultMock;
  SCorrelationEvalContext correlation = {
      .pSession = reinterpret_cast<SCorrelationTaskSession *>(&mock), .pOps = &ops};
  SScalarExtraInfo extra = {};
  extra.pCorrelation = &correlation;
  SColumnInfoData result = {};
  result.info = columnInfo;
  SScalarParam output = {};
  output.columnData = &result;
  EXPECT_EQ(scalarCalculate(reinterpret_cast<SNode *>(pCall), pBlocks, &output, &extra), TSDB_CODE_SUCCESS);
  EXPECT_EQ(output.numOfRows, 0);
  EXPECT_EQ(mock.calls, 0);
  EXPECT_EQ(mock.chargedBytes, 0);

  colDataDestroy(&result);
  nodesDestroyNode(reinterpret_cast<SNode *>(pCall));
  taosArrayDestroyEx(pBlocks, scltFreeDataBlock);
}

TEST(CorrelationScalarTest, InheritedParameterIsCopiedForEachRow) {
  SArray *pBlocks = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(pBlocks, nullptr);
  SColumnInfo columnInfo = createColumnInfo(1, TSDB_DATA_TYPE_INT, sizeof(int32_t));
  int16_t blockId = -1, slotId = -1;
  ASSERT_EQ(scltAppendReservedSlot(pBlocks, &blockId, &slotId, true, 2, &columnInfo), TSDB_CODE_SUCCESS);
  auto *pBlock = static_cast<SSDataBlock *>(taosArrayGetP(pBlocks, 0));
  SCorrelatedSubqueryNode *pCall = makeCorrelationScalarNode(pBlock->info.id.blockId);
  ASSERT_NE(pCall, nullptr);
  auto *pBinding = reinterpret_cast<SCorrelationParamBindingNode *>(
      nodesListGetNode(pCall->pParamBindings, 0));
  pBinding->sourceKind = CORRELATION_SOURCE_INHERITED;
  pBinding->sourceBlockId = -1;
  pBinding->sourceSlotId = -1;
  pBinding->inheritedParamId = 7;
  int32_t inheritedData = 42;
  SCorrelationParamValue inherited = {
      .paramId = 7,
      .dataType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)},
      .length = sizeof(int32_t),
      .pData = &inheritedData};
  SArray *pInherited = taosArrayInit(1, sizeof(SCorrelationParamValue));
  ASSERT_NE(pInherited, nullptr);
  ASSERT_NE(taosArrayPush(pInherited, &inherited), nullptr);

  CorrelationScalarMock mock = {};
  mock.pInheritedParams = pInherited;
  mock.sourceData = &inheritedData;
  SCorrelationRuntimeOps ops = {};
  ops.invokeScalar = invokeCorrelationScalarMock;
  ops.getInheritedParams = getInheritedCorrelationParamsMock;
  ops.reserveResultMemory = reserveCorrelationResultMock;
  ops.releaseResultMemory = releaseCorrelationResultMock;
  SCorrelationEvalContext correlation = {
      .pSession = reinterpret_cast<SCorrelationTaskSession *>(&mock), .pOps = &ops};
  SScalarExtraInfo extra = {};
  extra.pCorrelation = &correlation;
  SColumnInfoData result = {};
  result.info = columnInfo;
  SScalarParam output = {};
  output.columnData = &result;
  ASSERT_EQ(scalarCalculate(reinterpret_cast<SNode *>(pCall), pBlocks, &output, &extra), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mock.calls, 2);
  EXPECT_EQ(mock.seen[0], 42);
  EXPECT_EQ(mock.seen[1], 42);
  EXPECT_TRUE(mock.copiedParams);
  EXPECT_EQ(mock.chargedBytes, 0);

  colDataDestroy(&result);
  taosArrayDestroy(pInherited);
  nodesDestroyNode(reinterpret_cast<SNode *>(pCall));
  taosArrayDestroyEx(pBlocks, scltFreeDataBlock);
}

TEST(CorrelationScalarTest, CurrentRowAndAncestorUseChildParameterIds) {
  SArray *pBlocks = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(pBlocks, nullptr);
  SColumnInfo columnInfo = createColumnInfo(1, TSDB_DATA_TYPE_INT, sizeof(int32_t));
  int16_t blockId = -1, slotId = -1;
  ASSERT_EQ(scltAppendReservedSlot(pBlocks, &blockId, &slotId, true, 2, &columnInfo), TSDB_CODE_SUCCESS);
  auto *pBlock = static_cast<SSDataBlock *>(taosArrayGetP(pBlocks, 0));
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  for (int32_t row = 0; row < 2; ++row) {
    int32_t value = row + 1;
    ASSERT_EQ(colDataSetVal(pColumn, row, reinterpret_cast<char *>(&value), false), TSDB_CODE_SUCCESS);
  }

  SCorrelatedSubqueryNode *pCall = makeCorrelationScalarNode(pBlock->info.id.blockId);
  ASSERT_NE(pCall, nullptr);
  SNode *pAncestorNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CORRELATION_PARAM_BINDING, &pAncestorNode), TSDB_CODE_SUCCESS);
  auto *pAncestor = reinterpret_cast<SCorrelationParamBindingNode *>(pAncestorNode);
  pAncestor->paramId = 1;
  pAncestor->dataType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  pAncestor->sourceKind = CORRELATION_SOURCE_INHERITED;
  pAncestor->inheritedParamId = 7;
  ASSERT_EQ(nodesListStrictAppend(pCall->pParamBindings, pAncestorNode), TSDB_CODE_SUCCESS);

  int32_t ancestorData = 42;
  SCorrelationParamValue inherited = {
      .paramId = 7,
      .dataType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)},
      .length = sizeof(int32_t),
      .pData = &ancestorData};
  SArray *pInherited = taosArrayInit(1, sizeof(SCorrelationParamValue));
  ASSERT_NE(pInherited, nullptr);
  ASSERT_NE(taosArrayPush(pInherited, &inherited), nullptr);
  SArray *pSources = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(pSources, nullptr);

  CorrelationScalarMock mock = {};
  mock.expectedParamCount = 2;
  mock.pInheritedParams = pInherited;
  mock.expectedSources = pSources;
  mock.sourceData = pColumn->pData;
  SCorrelationRuntimeOps ops = {};
  ops.invokeScalar = invokeCorrelationScalarMock;
  ops.getInheritedParams = getInheritedCorrelationParamsMock;
  ops.reserveResultMemory = reserveCorrelationResultMock;
  ops.releaseResultMemory = releaseCorrelationResultMock;
  SCorrelationEvalContext correlation = {
      .pSession = reinterpret_cast<SCorrelationTaskSession *>(&mock), .pOps = &ops,
      .pSubquerySources = pSources, .pStatsContext = &mock, .recordStats = recordCorrelationStatsMock};
  SScalarExtraInfo extra = {};
  extra.pCorrelation = &correlation;
  SColumnInfoData result = {};
  result.info = columnInfo;
  SScalarParam output = {};
  output.columnData = &result;

  ASSERT_EQ(scalarCalculate(reinterpret_cast<SNode *>(pCall), pBlocks, &output, &extra), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mock.calls, 2);
  EXPECT_EQ(mock.seen[0], 1);
  EXPECT_EQ(mock.seen[1], 2);
  EXPECT_EQ(mock.seenOther[0], 42);
  EXPECT_EQ(mock.seenOther[1], 42);
  EXPECT_TRUE(mock.copiedParams);
  EXPECT_TRUE(mock.sourcesMatch);
  EXPECT_EQ(mock.recordedRows, 2);
  EXPECT_EQ(mock.recordedCalls, 2);
  EXPECT_GE(mock.recordedElapsedUs, 0);

  colDataDestroy(&result);
  taosArrayDestroy(pSources);
  taosArrayDestroy(pInherited);
  nodesDestroyNode(reinterpret_cast<SNode *>(pCall));
  taosArrayDestroyEx(pBlocks, scltFreeDataBlock);
}

TEST(CorrelationScalarTest, CaseCountsUnselectedBranchWithRepeatedParameters) {
  SArray *pBlocks = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(pBlocks, nullptr);
  SColumnInfo columnInfo = createColumnInfo(1, TSDB_DATA_TYPE_INT, sizeof(int32_t));
  int16_t blockId = -1, slotId = -1;
  ASSERT_EQ(scltAppendReservedSlot(pBlocks, &blockId, &slotId, true, 2, &columnInfo), TSDB_CODE_SUCCESS);
  auto *pBlock = static_cast<SSDataBlock *>(taosArrayGetP(pBlocks, 0));
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  int32_t repeated = 1;
  ASSERT_EQ(colDataSetVal(pColumn, 0, reinterpret_cast<char *>(&repeated), false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataSetVal(pColumn, 1, reinterpret_cast<char *>(&repeated), false), TSDB_CODE_SUCCESS);

  SNode *pWhen = nullptr;
  SNode *pThen = nullptr;
  SNode *pBranch = nullptr;
  SNode *pCase = nullptr;
  bool trueValue = true;
  int32_t selectedValue = 7;
  ASSERT_EQ(scltMakeValueNode(&pWhen, TSDB_DATA_TYPE_BOOL, &trueValue), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltMakeValueNode(&pThen, TSDB_DATA_TYPE_INT, &selectedValue), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_WHEN_THEN, &pBranch), TSDB_CODE_SUCCESS);
  ((SWhenThenNode *)pBranch)->pWhen = pWhen;
  ((SWhenThenNode *)pBranch)->pThen = pThen;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CASE_WHEN, &pCase), TSDB_CODE_SUCCESS);
  auto *pCaseWhen = reinterpret_cast<SCaseWhenNode *>(pCase);
  pCaseWhen->node.resType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  pCaseWhen->pElse = reinterpret_cast<SNode *>(makeCorrelationScalarNode(pBlock->info.id.blockId));
  ASSERT_NE(pCaseWhen->pElse, nullptr);
  ASSERT_EQ(nodesListMakeStrictAppend(&pCaseWhen->pWhenThenList, pBranch), TSDB_CODE_SUCCESS);

  CorrelationScalarMock mock = {};
  SCorrelationRuntimeOps ops = {};
  ops.invokeScalar = invokeCorrelationScalarMock;
  ops.reserveResultMemory = reserveCorrelationResultMock;
  ops.releaseResultMemory = releaseCorrelationResultMock;
  SCorrelationEvalContext correlation = {
      .pSession = reinterpret_cast<SCorrelationTaskSession *>(&mock), .pOps = &ops,
      .pStatsContext = &mock, .recordStats = recordCorrelationStatsMock};
  SScalarExtraInfo extra = {};
  extra.pCorrelation = &correlation;
  SColumnInfoData result = {};
  result.info = columnInfo;
  SScalarParam output = {};
  output.columnData = &result;

  ASSERT_EQ(scalarCalculate(pCase, pBlocks, &output, &extra), TSDB_CODE_SUCCESS);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(&result, 0)), selectedValue);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(&result, 1)), selectedValue);
  EXPECT_EQ(mock.calls, 2);
  EXPECT_EQ(mock.seen[0], repeated);
  EXPECT_EQ(mock.seen[1], repeated);
  EXPECT_EQ(mock.recordedRows, 2);
  EXPECT_EQ(mock.recordedCalls, 2);

  colDataDestroy(&result);
  nodesDestroyNode(pCase);
  taosArrayDestroyEx(pBlocks, scltFreeDataBlock);
}

TEST(CorrelationScalarTest, VariableInputAndResultOwnTheirBytes) {
  SArray *pBlocks = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(pBlocks, nullptr);
  SColumnInfo columnInfo = createColumnInfo(1, TSDB_DATA_TYPE_VARCHAR, 32);
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
  SColumnInfoData sourceColumn = {};
  sourceColumn.info = columnInfo;
  ASSERT_EQ(colInfoDataEnsureCapacity(&sourceColumn, 3, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &sourceColumn), TSDB_CODE_SUCCESS);
  pBlock->info.rows = 3;
  pBlock->info.capacity = 3;
  pBlock->info.id.blockId = 0;
  ASSERT_NE(taosArrayPush(pBlocks, &pBlock), nullptr);
  auto *pSource = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  char first[VARSTR_HEADER_SIZE + 3] = {};
  char last[VARSTR_HEADER_SIZE + 3] = {};
  varDataSetLen(first, 3);
  varDataSetLen(last, 3);
  memcpy(varDataVal(first), "one", 3);
  memcpy(varDataVal(last), "two", 3);
  ASSERT_EQ(colDataSetVal(pSource, 0, first, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataSetVal(pSource, 1, nullptr, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataSetVal(pSource, 2, last, false), TSDB_CODE_SUCCESS);

  SCorrelatedSubqueryNode *pCall = makeCorrelationScalarNode(pBlock->info.id.blockId);
  ASSERT_NE(pCall, nullptr);
  pCall->node.resType = {.type = TSDB_DATA_TYPE_VARCHAR, .bytes = 32};
  auto *pBinding = reinterpret_cast<SCorrelationParamBindingNode *>(
      nodesListGetNode(pCall->pParamBindings, 0));
  pBinding->dataType = pCall->node.resType;
  CorrelationScalarMock mock = {};
  mock.varSources[0] = colDataGetData(pSource, 0);
  mock.varSources[2] = colDataGetData(pSource, 2);
  SCorrelationRuntimeOps ops = {};
  ops.invokeScalar = invokeCorrelationVarMock;
  ops.reserveResultMemory = reserveCorrelationResultMock;
  ops.releaseResultMemory = releaseCorrelationResultMock;
  SCorrelationEvalContext correlation = {
      .pSession = reinterpret_cast<SCorrelationTaskSession *>(&mock), .pOps = &ops};
  SScalarExtraInfo extra = {};
  extra.pCorrelation = &correlation;
  SColumnInfoData result = {};
  result.info = columnInfo;
  SScalarParam output = {};
  output.columnData = &result;
  ASSERT_EQ(scalarCalculate(reinterpret_cast<SNode *>(pCall), pBlocks, &output, &extra), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mock.calls, 3);
  EXPECT_TRUE(mock.varCopied);
  EXPECT_EQ(mock.chargedBytes, 0);
  EXPECT_EQ(varDataTLen(colDataGetData(&result, 0)), sizeof(first));
  EXPECT_EQ(memcmp(varDataVal(colDataGetData(&result, 0)), "one", 3), 0);
  EXPECT_TRUE(colDataIsNull(&result, 3, 1, nullptr));
  EXPECT_EQ(memcmp(varDataVal(colDataGetData(&result, 2)), "two", 3), 0);

  colDataDestroy(&result);
  nodesDestroyNode(reinterpret_cast<SNode *>(pCall));
  taosArrayDestroyEx(pBlocks, scltFreeDataBlock);
}

TEST(CorrelationScalarTest, FilterUsesTemporaryResultAndPropagatesErrors) {
  SArray *pBlocks = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(pBlocks, nullptr);
  SColumnInfo columnInfo = createColumnInfo(1, TSDB_DATA_TYPE_INT, sizeof(int32_t));
  int16_t blockId = -1, slotId = -1;
  ASSERT_EQ(scltAppendReservedSlot(pBlocks, &blockId, &slotId, true, 3, &columnInfo), TSDB_CODE_SUCCESS);
  ASSERT_EQ(scltAppendReservedSlot(pBlocks, &blockId, &slotId, false, 3, &columnInfo), TSDB_CODE_SUCCESS);
  auto *pBlock = static_cast<SSDataBlock *>(taosArrayGetP(pBlocks, 0));
  auto *pParam = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  auto *pVoltage = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 1));
  int32_t voltages[] = {15, 15, 35};
  for (int32_t row = 0; row < 3; ++row) {
    int32_t param = row + 1;
    ASSERT_EQ(colDataSetVal(pParam, row, reinterpret_cast<char *>(&param), false), TSDB_CODE_SUCCESS);
    ASSERT_EQ(colDataSetVal(pVoltage, row, reinterpret_cast<char *>(&voltages[row]), false), TSDB_CODE_SUCCESS);
  }

  SCorrelatedSubqueryNode *pCall = makeCorrelationScalarNode(pBlock->info.id.blockId);
  ASSERT_NE(pCall, nullptr);
  SNode *pColumnNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &pColumnNode), TSDB_CODE_SUCCESS);
  auto *pColumn = reinterpret_cast<SColumnNode *>(pColumnNode);
  pColumn->node.resType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  pColumn->dataBlockId = pBlock->info.id.blockId;
  pColumn->slotId = 1;
  SNode *pCondition = nullptr;
  ASSERT_EQ(scltMakeOpNode(&pCondition, OP_TYPE_GREATER_THAN, TSDB_DATA_TYPE_BOOL, pColumnNode,
                          reinterpret_cast<SNode *>(pCall)), TSDB_CODE_SUCCESS);

  SFilterInfo *pFilter = nullptr;
  ASSERT_EQ(filterInitFromNode(pCondition, &pFilter, 0, nullptr), TSDB_CODE_SUCCESS);
  CorrelationScalarMock mock = {};
  SCorrelationRuntimeOps ops = {};
  ops.invokeScalar = invokeCorrelationScalarMock;
  ops.reserveResultMemory = reserveCorrelationResultMock;
  ops.releaseResultMemory = releaseCorrelationResultMock;
  SCorrelationEvalContext correlation = {
      .pSession = reinterpret_cast<SCorrelationTaskSession *>(&mock), .pOps = &ops};
  SColumnInfoData *pMask = nullptr;
  int32_t status = 0;
  int32_t filterCode = filterExecuteWithCorrelation(pFilter, pBlock, &pMask, nullptr, 2, &status, &correlation);
  EXPECT_EQ(mock.calls, 3);
  ASSERT_EQ(filterCode, TSDB_CODE_SUCCESS);
  ASSERT_NE(pMask, nullptr);
  EXPECT_EQ(status, FILTER_RESULT_PARTIAL_QUALIFIED);
  EXPECT_TRUE(*reinterpret_cast<bool *>(colDataGetData(pMask, 0)));
  EXPECT_FALSE(*reinterpret_cast<bool *>(colDataGetData(pMask, 1)));
  EXPECT_TRUE(*reinterpret_cast<bool *>(colDataGetData(pMask, 2)));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pParam, 1)), 2);
  EXPECT_EQ(mock.chargedBytes, 0);
  colDataDestroy(pMask);
  taosMemoryFree(pMask);

  mock.calls = 0;
  mock.failOnCall = 2;
  pMask = nullptr;
  EXPECT_EQ(filterExecuteWithCorrelation(pFilter, pBlock, &pMask, nullptr, 2, &status, &correlation),
            TSDB_CODE_QRY_SUBQ_EXEC_ERROR);
  EXPECT_EQ(mock.calls, 2);
  EXPECT_EQ(pMask, nullptr);
  EXPECT_EQ(mock.chargedBytes, 0);

  mock.calls = 0;
  mock.failOnCall = 0;
  mock.limitBytes = 1;
  EXPECT_EQ(filterExecuteWithCorrelation(pFilter, pBlock, &pMask, nullptr, 2, &status, &correlation),
            TSDB_CODE_QRY_QUERY_MEM_EXHAUSTED);
  EXPECT_EQ(mock.calls, 0);
  EXPECT_EQ(pMask, nullptr);
  EXPECT_EQ(mock.chargedBytes, 0);

  filterFreeInfo(pFilter);
  nodesDestroyNode(pCondition);
  taosArrayDestroyEx(pBlocks, scltFreeDataBlock);
}

int main(int argc, char **argv) {
  taosSeedRand(taosGetTimestampSec());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop
