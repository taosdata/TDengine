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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "builtins.h"
#include "builtinsimpl.h"
#include "decimal.h"
#include "functionMgt.h"
#include "functionMgtInt.h"
#include "functionResInfoInt.h"
#include "nodes.h"
#include "osEnv.h"
#include "osTimezone.h"
#include "tdatablock.h"
#include "tpagedbuf.h"
#include "tudf.h"
#include "tudfInt.h"
#include "tvariant.h"
#ifndef _WIN32
#include "stub.h"
#endif

extern "C" int32_t funcInputGetNextRowDescPk(SFuncInputRowIter* pIter,
                                               SFuncInputRow* pRow, bool* res);

namespace {

using ScopedDataBlock =
    std::unique_ptr<SSDataBlock, decltype(&blockDataDestroy)>;

class DecodedUdfRequestGuard {
 public:
  SUdfRequest value = {};
  bool        ownsCallResources = true;

  DecodedUdfRequestGuard() = default;
  DecodedUdfRequestGuard(const DecodedUdfRequestGuard&) = delete;
  DecodedUdfRequestGuard& operator=(const DecodedUdfRequestGuard&) = delete;
  DecodedUdfRequestGuard(DecodedUdfRequestGuard&&) = delete;
  DecodedUdfRequestGuard& operator=(DecodedUdfRequestGuard&&) = delete;

  ~DecodedUdfRequestGuard() {
    if (ownsCallResources && value.type == UDF_TASK_CALL &&
        (value.call.callType == TSDB_UDF_CALL_SCALA_PROC ||
         value.call.callType == TSDB_UDF_CALL_AGG_PROC)) {
      blockDataFreeRes(&value.call.block);
    }
    if (ownsCallResources && value.type == UDF_TASK_CALL) {
      freeUdfInterBuf(&value.call.interBuf);
      freeUdfInterBuf(&value.call.interBuf2);
    }
  }
};

struct BitDurationResultStorage {
  SResultRowEntryInfo resultInfo = {};
  alignas(SBitDurationInfo) unsigned char interBuffer[sizeof(SBitDurationInfo)] = {};
};

static_assert(offsetof(BitDurationResultStorage, interBuffer) % alignof(SBitDurationInfo) == 0,
              "the bit_duration inter-buffer offset must be aligned");
static_assert(sizeof(BitDurationResultStorage) >=
                  offsetof(BitDurationResultStorage, interBuffer) + sizeof(SBitDurationInfo),
              "the bit_duration result storage must contain the full inter-buffer");
static_assert(offsetof(BitDurationResultStorage, interBuffer) == sizeof(SResultRowEntryInfo),
              "the test storage must match GET_ROWCELL_INTERBUF layout");

class ScopedTimezoneDisplay {
 public:
  explicit ScopedTimezoneDisplay(const char* display) {
    tstrncpy(original_, tsTimezoneStr, sizeof(original_));
    tstrncpy(tsTimezoneStr, display, TD_TIMEZONE_LEN);
  }

  ~ScopedTimezoneDisplay() {
    tstrncpy(tsTimezoneStr, original_, TD_TIMEZONE_LEN);
  }

 private:
  char original_[TD_TIMEZONE_LEN] = {0};
};

class ScopedMissingTimezoneNameMap {
 public:
  ScopedMissingTimezoneNameMap() : original_(pTimezoneNameMap) {
    pTimezoneNameMap = nullptr;
  }

  ~ScopedMissingTimezoneNameMap() { pTimezoneNameMap = original_; }

 private:
  void* original_;
};

SFunctionNode* createFunctionNode(const char* functionName,
                                  const int8_t* parameterTypes,
                                  int32_t parameterCount) {
  SFunctionNode* pFunc = nullptr;
  if (nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc) !=
      TSDB_CODE_SUCCESS) {
    return nullptr;
  }
  tstrncpy(pFunc->functionName, functionName, TSDB_FUNC_NAME_LEN);

  for (int32_t i = 0; i < parameterCount; ++i) {
    SValueNode* pValue = nullptr;
    if (nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pValue) !=
        TSDB_CODE_SUCCESS) {
      nodesDestroyNode((SNode*)pFunc);
      return nullptr;
    }
    pValue->node.resType.type = parameterTypes[i];
    if (nodesListMakeAppend(&pFunc->pParameterList, (SNode*)pValue) !=
        TSDB_CODE_SUCCESS) {
      nodesDestroyNode((SNode*)pValue);
      nodesDestroyNode((SNode*)pFunc);
      return nullptr;
    }
  }

  return pFunc;
}

SColumnNode* makeColumnNode(int8_t type, int32_t bytes) {
  SColumnNode* pColumn = nullptr;
  if (nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pColumn) !=
      TSDB_CODE_SUCCESS) {
    return nullptr;
  }
  pColumn->node.resType.type = type;
  pColumn->node.resType.bytes = bytes;
  return pColumn;
}

// timetruncate(ts, 1d) requires a duration value node for the unit parameter;
// otherwise validateParam() rejects it before timezone injection.
SFunctionNode* createTimeTruncateNode() {
  const int8_t parameterTypes[] = {
      TSDB_DATA_TYPE_TIMESTAMP,
      TSDB_DATA_TYPE_BIGINT,
  };
  SFunctionNode* pFunc = createFunctionNode("timetruncate", parameterTypes, 2);
  if (pFunc == nullptr) {
    return nullptr;
  }

  SValueNode* pUnit = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  pUnit->literal = taosStrdup("1d");
  if (pUnit->literal == nullptr) {
    nodesDestroyNode((SNode*)pFunc);
    return nullptr;
  }
  pUnit->flag |= VALUE_FLAG_IS_DURATION;
  pUnit->unit = 'd';
  pUnit->translate = true;
  pUnit->datum.i = 86400000;

  return pFunc;
}

SNode* makeVarcharValue(const char* value, int32_t len) {
  SValueNode* node = nullptr;
  if (nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&node) !=
      TSDB_CODE_SUCCESS) {
    return nullptr;
  }
  node->node.resType.type = TSDB_DATA_TYPE_VARCHAR;
  node->node.resType.bytes = len + VARSTR_HEADER_SIZE;
  node->datum.p = (char*)taosMemoryMalloc(len + VARSTR_HEADER_SIZE);
  if (node->datum.p == nullptr) {
    nodesDestroyNode((SNode*)node);
    return nullptr;
  }
  varDataSetLen(node->datum.p, len);
  (void)memcpy(varDataVal(node->datum.p), value, len);
  return (SNode*)node;
}

SStreamAncestorContext* createDenseAncestorContext(int32_t count) {
  auto* context = static_cast<SStreamAncestorContext*>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
  if (context == nullptr) return nullptr;
  context->pParamContexts = taosArrayInit(count, sizeof(SStreamAncestorParamContext));
  if (context->pParamContexts == nullptr) {
    tDestroyStreamAncestorContext(&context);
    return nullptr;
  }

  SStreamAncestorParamContext param = {};
  for (int32_t i = 0; i < count; ++i) {
    param = {};
    param.paramIndex = 0;
    param.leafIdentity.gid = 1000 + i;
    param.leafIdentity.triggerType = WINDOW_TYPE_COUNT;
    param.leafIdentity.openingTs = i;
    param.leafIdentity.nativeDiscriminator = i;
    param.leafIdentity.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
    if (param.leafIdentity.lineage.pScopes == nullptr) goto _error;
    SScopeInstanceId scope = {};
    scope.layerIndex = 0;
    scope.triggerType = WINDOW_TYPE_INTERVAL;
    scope.openingTs = i;
    scope.nativeDiscriminator = i;
    if (taosArrayPush(param.leafIdentity.lineage.pScopes, &scope) == nullptr) goto _error;
    param.pSnapshots = taosArrayInit(1, sizeof(SWindowAncestorSnapshot));
    if (param.pSnapshots == nullptr) goto _error;
    SWindowAncestorSnapshot snapshot = {};
    snapshot.layerIndex = 0;
    snapshot.triggerType = WINDOW_TYPE_INTERVAL;
    snapshot.placeholderMask = PLACE_HOLDER_WSTART;
    snapshot.values.window.start = param.leafIdentity.gid;
    if (taosArrayPush(param.pSnapshots, &snapshot) == nullptr ||
        taosArrayPush(context->pParamContexts, &param) == nullptr) {
      goto _error;
    }
    param.leafIdentity.lineage.pScopes = nullptr;
    param.pSnapshots = nullptr;
  }
  return context;

_error:
  taosArrayDestroy(param.leafIdentity.lineage.pScopes);
  taosArrayDestroy(param.pSnapshots);
  tDestroyStreamAncestorContext(&context);
  return nullptr;
}

#ifndef _WIN32
// MSVC /GL can inline array access, bypassing an entry-point patch.
const SArray* gAncestorParamArray = nullptr;
int32_t       gAncestorParamArrayGetCalls = 0;

int32_t failBlockDataExtract(SSDataBlock*, int32_t, int32_t, SSDataBlock**) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

void* countAncestorParamArrayGet(const SArray* array, size_t index) {
  if (array == gAncestorParamArray) ++gAncestorParamArrayGetCalls;
  if (array == nullptr || index >= array->size) return nullptr;
  return TARRAY_GET_ELEM(array, index);
}

class AncestorParamArrayGetGuard {
 public:
  explicit AncestorParamArrayGetGuard(const SArray* array) {
    gAncestorParamArray = array;
    gAncestorParamArrayGetCalls = 0;
    stub_.set(taosArrayGet, countAncestorParamArrayGet);
  }

  ~AncestorParamArrayGetGuard() {
    stub_.reset(taosArrayGet);
    gAncestorParamArray = nullptr;
  }

 private:
  Stub stub_;
};
#endif

class BitDurationTestContext {
 public:
  BitDurationTestContext()
      : inputBlock_(nullptr, blockDataDestroy), outputBlock_(nullptr, blockDataDestroy) {}

  BitDurationTestContext(const BitDurationTestContext&) = delete;
  BitDurationTestContext& operator=(const BitDurationTestContext&) = delete;
  BitDurationTestContext(BitDurationTestContext&&) = delete;
  BitDurationTestContext& operator=(BitDurationTestContext&&) = delete;

  ~BitDurationTestContext() { funcInputRowIterCleanup(&ctx_.rowIter); }

  int32_t initialize(int64_t mode, int64_t mask = 1, int32_t numOfParams = 3) {
    SSDataBlock* inputBlock = nullptr;
    int32_t code = createDataBlock(&inputBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    inputBlock_.reset(inputBlock);

    SColumnInfoData tsInfo = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 0);
    code = blockDataAppendColInfo(inputBlock_.get(), &tsInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    SColumnInfoData valueInfo = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
    code = blockDataAppendColInfo(inputBlock_.get(), &valueInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    SColumnInfoData primaryKeyInfo = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
    code = blockDataAppendColInfo(inputBlock_.get(), &primaryKeyInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    SSDataBlock* outputBlock = nullptr;
    code = createDataBlock(&outputBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    outputBlock_.reset(outputBlock);
    SColumnInfoData outputInfo = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0);
    code = blockDataAppendColInfo(outputBlock_.get(), &outputInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = blockDataEnsureCapacity(outputBlock_.get(), 1);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (numOfParams > 1) {
      params_[1].type = FUNC_PARAM_TYPE_VALUE;
      params_[1].param.i = mask;
    }
    if (numOfParams > 2) {
      params_[2].type = FUNC_PARAM_TYPE_VALUE;
      params_[2].param.i = mode;
    }
    expr_.base.resSchema.slotId = 0;
    ctx_.param = params_;
    ctx_.numOfParams = numOfParams;
    ctx_.resultInfo = resultInfo();
    ctx_.resDataInfo.interBufSize = sizeof(SBitDurationInfo);
    ctx_.order = TSDB_ORDER_ASC;
    ctx_.pExpr = &expr_;
    ctx_.pSrcBlock = inputBlock_.get();
    ctx_.start.key = INT64_MIN;
    ctx_.end.key = INT64_MIN;
    return bitDurationFunctionSetup(&ctx_, resultInfo());
  }

  int32_t setRows(const std::vector<TSKEY>& timestamps, const std::vector<int32_t>& values,
                  bool valuesAreNull = false, const std::vector<int32_t>& primaryKeys = {},
                  uint64_t groupId = 0) {
    if (timestamps.size() != values.size() ||
        (!primaryKeys.empty() && timestamps.size() != primaryKeys.size())) {
      return TSDB_CODE_INVALID_PARA;
    }
    funcInputRowIterCleanup(&ctx_.rowIter);
    blockDataCleanup(inputBlock_.get());
    int32_t code = blockDataEnsureCapacity(inputBlock_.get(), timestamps.size());
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    SColumnInfoData* tsColumn = static_cast<SColumnInfoData*>(taosArrayGet(inputBlock_->pDataBlock, 0));
    SColumnInfoData* valueColumn = static_cast<SColumnInfoData*>(taosArrayGet(inputBlock_->pDataBlock, 1));
    SColumnInfoData* primaryKeyColumn =
        static_cast<SColumnInfoData*>(taosArrayGet(inputBlock_->pDataBlock, 2));
    if (tsColumn == nullptr || valueColumn == nullptr || primaryKeyColumn == nullptr) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    for (size_t row = 0; row < timestamps.size(); ++row) {
      code = colDataSetVal(tsColumn, row, reinterpret_cast<const char*>(&timestamps[row]), false);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      if (valuesAreNull) {
        colDataSetNULL(valueColumn, row);
      } else {
        code = colDataSetVal(valueColumn, row, reinterpret_cast<const char*>(&values[row]), false);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      if (!primaryKeys.empty()) {
        code = colDataSetVal(primaryKeyColumn, row,
                             reinterpret_cast<const char*>(&primaryKeys[row]), false);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      } else {
        colDataSetNULL(primaryKeyColumn, row);
      }
    }

    inputBlock_->info.rows = timestamps.size();
    inputBlock_->info.id.groupId = groupId;
    inputColumns_[0] = valueColumn;
    ctx_.input.totalRows = timestamps.size();
    ctx_.input.numOfRows = timestamps.size();
    ctx_.input.startRowIndex = 0;
    ctx_.input.numOfInputCols = 1;
    ctx_.input.pPTS = tsColumn;
    ctx_.input.pData = inputColumns_;
    ctx_.hasPrimaryKey = !primaryKeys.empty();
    ctx_.input.pPrimaryKey = ctx_.hasPrimaryKey ? primaryKeyColumn : nullptr;
    return TSDB_CODE_SUCCESS;
  }

  int32_t setSyntheticNullRows(const std::vector<TSKEY>& timestamps, bool hasPrimaryKey = false) {
    std::vector<int32_t> values(timestamps.size());
    std::vector<int32_t> primaryKeys(hasPrimaryKey ? timestamps.size() : 0, 1);
    int32_t code = setRows(timestamps, values, true, primaryKeys);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    SColumnInfoData* valueColumn = static_cast<SColumnInfoData*>(taosArrayGet(inputBlock_->pDataBlock, 1));
    if (valueColumn == nullptr) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    valueColumn->info.type = TSDB_DATA_TYPE_NULL;
    valueColumn->info.bytes = 0;
    if (hasPrimaryKey) {
      ctx_.input.pPTS = nullptr;
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t process() { return bitDurationFunction(&ctx_); }

  int32_t finalize() { return bitDurationFinalize(&ctx_, outputBlock_.get()); }

  SqlFunctionCtx* context() { return &ctx_; }

  SResultRowEntryInfo* resultInfo() {
    return &resultStorage_.resultInfo;
  }

  SBitDurationInfo* state() {
    return static_cast<SBitDurationInfo*>(GET_ROWCELL_INTERBUF(resultInfo()));
  }

  void setValueNull(int32_t row) {
    auto* valueColumn = static_cast<SColumnInfoData*>(taosArrayGet(inputBlock_->pDataBlock, 1));
    ASSERT_NE(nullptr, valueColumn);
    colDataSetNULL(valueColumn, row);
  }

  int64_t outputValue() {
    SColumnInfoData* outputColumn =
        static_cast<SColumnInfoData*>(taosArrayGet(outputBlock_->pDataBlock, 0));
    EXPECT_NE(nullptr, outputColumn);
    if (outputColumn == nullptr) return 0;
    EXPECT_EQ(sizeof(int64_t), outputColumn->info.bytes);
    const char* data = colDataGetData(outputColumn, 0);
    EXPECT_NE(nullptr, data);
    return data == nullptr ? 0 : *reinterpret_cast<const int64_t*>(data);
  }

 private:
  BitDurationResultStorage      resultStorage_ = {};
  SqlFunctionCtx                 ctx_ = {};
  SFunctParam                   params_[3] = {};
  SExprInfo                     expr_ = {};
  SColumnInfoData*              inputColumns_[1] = {};
  ScopedDataBlock               inputBlock_;
  ScopedDataBlock               outputBlock_;
};

}  // namespace

TEST(BitDurationFunctionTest, EmptyInputFinalizesWithoutAResult) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({}, {}));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(0, test.resultInfo()->numOfRes);
  EXPECT_EQ(1, test.resultInfo()->isNullRes);
}

#ifndef _WIN32
TEST(FunctionInputRowIterTest, FailedDescPkBlockReplacementPreservesCachedOwner) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10}, {1}, false, {1}));

  SFuncInputRowIter* iter = &test.context()->rowIter;
  iter->hasPrev = true;
  iter->prevBlockTsEnd = 10;
  iter->tsList = reinterpret_cast<TSKEY*>(test.context()->input.pPTS->pData);
  iter->inputEndIndex = 0;
  iter->pSrcBlock = test.context()->pSrcBlock;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&iter->pPrevRowBlock));
  SSDataBlock* cachedBlock = iter->pPrevRowBlock;

  Stub stub;
  stub.set(blockDataExtractBlock, failBlockDataExtract);
  SFuncInputRow row = {};
  bool hasRow = false;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, funcInputGetNextRowDescPk(iter, &row, &hasRow));
  EXPECT_EQ(cachedBlock, iter->pPrevRowBlock);
  stub.reset(blockDataExtractBlock);
  funcInputRowIterCleanup(iter);
  EXPECT_EQ(nullptr, iter->pPrevRowBlock);
}
#endif

TEST(BitDurationFunctionTest, EmptyInputEstablishesResultIteratorOwnership) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({}, {}));
  test.context()->rowIter.pOwnerResultInfo = reinterpret_cast<SResultRowEntryInfo*>(1);
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  EXPECT_EQ(test.resultInfo(), test.context()->rowIter.pOwnerResultInfo);
}

TEST(BitDurationFunctionTest, InvalidBoundaryStillEstablishesResultIteratorOwnership) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({}, {}));
  test.context()->rowIter.pOwnerResultInfo = reinterpret_cast<SResultRowEntryInfo*>(1);
  test.context()->start.key = 10;
  test.context()->start.val = 0.5;
  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR, test.process());
  EXPECT_EQ(test.resultInfo(), test.context()->rowIter.pOwnerResultInfo);
}

TEST(BitDurationFunctionTest, TotalModeWithOnlyNullSamplesReturnsZero) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_TOTAL));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10}, {0}, true));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_FALSE(test.state()->hasFirstTs);
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(1, test.resultInfo()->numOfRes);
  EXPECT_EQ(0, test.resultInfo()->isNullRes);
  EXPECT_EQ(0, test.outputValue());
}

TEST(BitDurationFunctionTest, NonTotalModeWithOnlyNullSamplesReturnsZero) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10}, {0}, true));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(1, test.resultInfo()->numOfRes);
  EXPECT_EQ(0, test.resultInfo()->isNullRes);
  EXPECT_EQ(0, test.outputValue());
}

TEST(BitDurationFunctionTest, SyntheticNullColumnReturnsZero) {
  for (int64_t mode : {BIT_DURATION_FLAG_NONZERO, BIT_DURATION_FLAG_TOTAL}) {
    BitDurationTestContext test;
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(mode));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.setSyntheticNullRows({10, 20}));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
    EXPECT_EQ(1, test.resultInfo()->numOfRes);
    EXPECT_EQ(0, test.resultInfo()->isNullRes);
    EXPECT_EQ(0, test.outputValue());
  }
}

TEST(BitDurationFunctionTest, SyntheticNullPrimaryKeyInputDoesNotNeedTimestampData) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setSyntheticNullRows({10}, true));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(1, test.resultInfo()->numOfRes);
  EXPECT_EQ(0, test.outputValue());
}

TEST(BitDurationFunctionTest, TotalModeReturnsFirstToLastSampleSpan) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_TOTAL));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10, 20, 30}, {0, 1, 0}));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(1, test.resultInfo()->numOfRes);
  EXPECT_EQ(0, test.resultInfo()->isNullRes);
  EXPECT_EQ(20, test.outputValue());
}

TEST(BitDurationFunctionTest, SetupValidatesExplicitParametersAndDefaults) {
  {
    BitDurationTestContext test;
    EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_VALUE, test.initialize(3));
    EXPECT_FALSE(test.resultInfo()->initialized);
  }
  {
    BitDurationTestContext test;
    EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_VALUE,
              test.initialize(BIT_DURATION_FLAG_NONZERO, 0));
  }
  {
    BitDurationTestContext test;
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_TOTAL, 99, 1));
    EXPECT_EQ(1, test.state()->mask);
    EXPECT_EQ(BIT_DURATION_FLAG_NONZERO, test.state()->mode);
  }
}

TEST(BitDurationFunctionTest, SeedAndBoundaryProduceNonTotalDuration) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({}, {}));
  test.context()->start.key = 10;
  test.context()->start.val = 1.0;
  test.context()->end.key = 19;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(1, test.resultInfo()->numOfRes);
  EXPECT_EQ(0, test.resultInfo()->isNullRes);
  EXPECT_EQ(10, test.outputValue());
}

TEST(BitDurationFunctionTest, LateSeedAddsWindowStartSegmentOnce) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({30}, {0}));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());

  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({}, {}));
  test.context()->start.key = 20;
  test.context()->start.val = 1.0;
  test.context()->end.key = 39;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(10, test.outputValue());
}

TEST(BitDurationFunctionTest, SkipsValidRowsBeforeSeed) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({9, 10}, {0, 1}));
  test.context()->start.key = 10;
  test.context()->start.val = 1.0;
  test.context()->end.key = 19;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(10, test.outputValue());
}

TEST(BitDurationFunctionTest, NullRowBeforeSeedDoesNotConsumeSeed) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({9, 10}, {0, 1}));
  test.setValueNull(0);
  test.context()->start.key = 10;
  test.context()->start.val = 1.0;
  test.context()->end.key = 19;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(10, test.outputValue());
}

TEST(BitDurationFunctionTest, RejectsDescendingScanOrder) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10}, {1}));
  test.context()->order = TSDB_ORDER_DESC;
  EXPECT_EQ(TSDB_CODE_FUNC_INVALID_TIMELINE, test.process());
}

TEST(BitDurationFunctionTest, NoSuccessorLeavesOnlyCompletedSegments) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10, 20}, {1, 0}));
  ASSERT_EQ(INT64_MIN, test.context()->end.key);
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(10, test.outputValue());
}

TEST(BitDurationFunctionTest, TotalModeIgnoresBoundarySeedWithoutRows) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_TOTAL));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({}, {}));
  test.context()->start.key = 10;
  test.context()->start.val = 1.0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(0, test.resultInfo()->numOfRes);
  EXPECT_EQ(1, test.resultInfo()->isNullRes);
}

TEST(BitDurationFunctionTest, SetupRejectsUndersizedIntermediateBuffer) {
  BitDurationTestContext test;
  test.context()->resDataInfo.interBufSize = sizeof(SBitDurationInfo) - 1;
  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR,
            bitDurationFunctionSetup(test.context(), test.resultInfo()));
  EXPECT_FALSE(test.resultInfo()->initialized);
}

TEST(BitDurationFunctionTest, ZeroModeAccumulatesOnlyZeroIntervals) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_ZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10, 15, 20}, {0, 1, 0}));
  test.context()->end.key = 24;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(1, test.resultInfo()->numOfRes);
  EXPECT_EQ(0, test.resultInfo()->isNullRes);
  EXPECT_EQ(10, test.outputValue());
}

TEST(BitDurationFunctionTest, RestoresPrimaryKeyStateOnlyWithinTheSameGroup) {
  {
    BitDurationTestContext test;
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10}, {1}, false, {1}, 100));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
    ASSERT_TRUE(test.state()->hasPendingPkTs);
    ASSERT_EQ(10, test.state()->pendingPkTs);
    ASSERT_EQ(100, test.state()->pendingPkGroupId);

    ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10, 20}, {0, 1}, false, {2, 1}, 100));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
    EXPECT_EQ(2, test.state()->numOfRows);
    EXPECT_EQ(10, test.state()->duration);
  }

  {
    BitDurationTestContext test;
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10}, {1}, false, {1}, 100));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());

    ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({20}, {1}, false, {1}, 200));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
    EXPECT_EQ(2, test.state()->numOfRows);
    EXPECT_EQ(200, test.state()->pendingPkGroupId);
  }
}

TEST(BitDurationFunctionTest, SetRowsClearsPrimaryKeyIteratorState) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10}, {1}, false, {7}, 100));
  ASSERT_NE(nullptr, test.context()->input.pPrimaryKey);
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_NE(nullptr, test.context()->rowIter.pPkCol);

  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({20}, {0}));
  EXPECT_FALSE(test.context()->hasPrimaryKey);
  EXPECT_EQ(nullptr, test.context()->input.pPrimaryKey);
  EXPECT_EQ(nullptr, test.context()->rowIter.pPkCol);
}

TEST(BitDurationFunctionTest, BoundaryNearMaximumTimestampDoesNotWrap) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({}, {}));
  test.context()->start.key = INT64_MAX - 2;
  test.context()->start.val = 1.0;
  test.context()->end.key = INT64_MAX - 1;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  EXPECT_EQ(INT64_MAX, test.state()->segmentStartTs);
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(2, test.outputValue());
}

TEST(BitDurationFunctionTest, BoundaryAtMaximumTimestampIncludesFinalTick) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({}, {}));
  test.context()->start.key = INT64_MAX - 2;
  test.context()->start.val = 1.0;
  test.context()->end.key = INT64_MAX;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
  EXPECT_EQ(3, test.outputValue());
}

TEST(BitDurationFunctionTest, TotalModeDistinguishesAllNullAndSingleValueInputs) {
  {
    BitDurationTestContext test;
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_TOTAL));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10, 20}, {0, 0}, true));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
    EXPECT_EQ(1, test.resultInfo()->numOfRes);
    EXPECT_EQ(0, test.outputValue());
  }
  {
    BitDurationTestContext test;
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_TOTAL));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({INT64_MAX}, {1}));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.process());
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.finalize());
    EXPECT_EQ(1, test.resultInfo()->numOfRes);
    EXPECT_EQ(0, test.outputValue());
  }
}

TEST(BitDurationFunctionTest, RejectsDuplicateAndDescendingTimestamps) {
  {
    BitDurationTestContext test;
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({10, 10}, {1, 1}));
    EXPECT_EQ(TSDB_CODE_FUNC_DUP_TIMESTAMP, test.process());
  }
  {
    BitDurationTestContext test;
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
    ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({11, 10}, {1, 1}));
    EXPECT_EQ(TSDB_CODE_FUNC_INVALID_TIMELINE, test.process());
  }
}

TEST(BitDurationFunctionTest, RejectsDurationOverflow) {
  BitDurationTestContext test;
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.initialize(BIT_DURATION_FLAG_NONZERO));
  ASSERT_EQ(TSDB_CODE_SUCCESS, test.setRows({INT64_MIN, INT64_MAX}, {1, 1}));
  EXPECT_EQ(TSDB_CODE_OUT_OF_RANGE, test.process());
}

TEST(FunctionMgtTest, identifiesSqlWindowFunctions) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  const char* windowFuncs[] = {"row_number", "rank", "dense_rank",  "percent_rank", "cume_dist",
                               "lag",        "lead", "first_value", "last_value",   "nth_value"};

  for (const char* func : windowFuncs) {
    EXPECT_TRUE(fmIsSqlWindowFunc(func)) << func;
  }
  EXPECT_FALSE(fmIsSqlWindowFunc("not_a_function"));
}

TEST(FunctionMgtTest, identifiesOrderRequiredSqlWindowFunctions) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  const char* orderRequiredFuncs[] = {"row_number", "rank", "dense_rank",  "percent_rank", "cume_dist",
                                      "lag",        "lead", "first_value", "last_value",   "nth_value", "csum"};

  for (const char* func : orderRequiredFuncs) {
    EXPECT_TRUE(fmIsSqlWindowOrderRequiredFunc(func)) << func;
  }
  EXPECT_FALSE(fmIsSqlWindowOrderRequiredFunc("sum"));
  EXPECT_FALSE(fmIsSqlWindowOrderRequiredFunc("not_a_function"));
}

TEST(FunctionMgtTest, identifiesAggregatesUsableAsSqlWindowAggregates) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  const char* windowAggFuncs[] = {"count", "sum", "min", "max", "avg", "percentile", "first", "last",
                                  "last_row", "csum"};

  for (const char* func : windowAggFuncs) {
    EXPECT_TRUE(fmCanUseAsSqlWindowAgg(func)) << func;
  }
  EXPECT_FALSE(fmCanUseAsSqlWindowAgg("row_number"));
  EXPECT_FALSE(fmCanUseAsSqlWindowAgg("not_a_function"));
}

TEST(FunctionMgtTest, identifiesCumulativeSqlWindowFunctions) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  EXPECT_TRUE(fmIsCumulativeSqlWindowFunc(fmGetFuncId("csum")));
  EXPECT_FALSE(fmIsCumulativeSqlWindowFunc(fmGetFuncId("sum")));
  EXPECT_FALSE(fmIsCumulativeSqlWindowFunc(fmGetFuncId("mavg")));
}

TEST(FunctionMgtTest, identifiesBitDurationFunctions) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  EXPECT_TRUE(fmIsBitDurationFunc(fmGetFuncId("bit_duration")));
  EXPECT_FALSE(fmIsBitDurationFunc(fmGetFuncId("bit_changes")));
  EXPECT_FALSE(fmIsBitDurationFunc(-1));
}

TEST(FunctionMgtTest, identifiesBitDurationPredecessorModes) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  SFunctParam params[3] = {};
  SqlFunctionCtx ctx = {};
  ctx.functionId = fmGetFuncId("bit_duration");
  ctx.param = params;
  ctx.numOfParams = 3;
  params[2].type = FUNC_PARAM_TYPE_VALUE;

  params[2].param.i = BIT_DURATION_FLAG_NONZERO;
  EXPECT_TRUE(fmBitDurationNeedsPrevRow(&ctx));
  params[2].param.i = BIT_DURATION_FLAG_ZERO;
  EXPECT_TRUE(fmBitDurationNeedsPrevRow(&ctx));
  params[2].param.i = BIT_DURATION_FLAG_TOTAL;
  EXPECT_FALSE(fmBitDurationNeedsPrevRow(&ctx));

  ctx.numOfParams = 1;
  EXPECT_TRUE(fmBitDurationNeedsPrevRow(&ctx));
  ctx.functionId = fmGetFuncId("bit_changes");
  EXPECT_FALSE(fmBitDurationNeedsPrevRow(&ctx));
}

TEST(FunctionMgtTest, irateDistMethodAcceptsCompositePrimaryKeys) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  const int8_t primaryKeyTypes[] = {
      TSDB_DATA_TYPE_INT,      TSDB_DATA_TYPE_UINT,
      TSDB_DATA_TYPE_BIGINT,   TSDB_DATA_TYPE_UBIGINT,
      TSDB_DATA_TYPE_VARCHAR,  TSDB_DATA_TYPE_TIMESTAMP,
  };
  const int8_t parameterTypes[] = {TSDB_DATA_TYPE_INT};

  for (int8_t primaryKeyType : primaryKeyTypes) {
    SCOPED_TRACE((int32_t)primaryKeyType);
    SFunctionNode* pFunc =
        createFunctionNode("irate", parameterTypes, 1);
    ASSERT_NE(nullptr, pFunc);

    char message[128] = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              fmGetFuncInfo(pFunc, message, sizeof(message)));

    SColumnNode* pTimestamp = makeColumnNode(
        TSDB_DATA_TYPE_TIMESTAMP,
        tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes);
    ASSERT_NE(nullptr, pTimestamp);
    pTimestamp->isPrimTs = true;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesListMakeAppend(&pFunc->pParameterList,
                                  (SNode*)pTimestamp));

    int32_t pkBytes = primaryKeyType == TSDB_DATA_TYPE_VARCHAR
                          ? 16 + VARSTR_HEADER_SIZE
                          : tDataTypes[primaryKeyType].bytes;
    SColumnNode* pPrimaryKey =
        makeColumnNode(primaryKeyType, pkBytes);
    ASSERT_NE(nullptr, pPrimaryKey);
    pPrimaryKey->isPk = true;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesListMakeAppend(&pFunc->pParameterList,
                                  (SNode*)pPrimaryKey));
    pFunc->hasPk = true;
    pFunc->pkBytes = pkBytes;

    SFunctionNode* pPartial = nullptr;
    SFunctionNode* pMerge = nullptr;
    int32_t code = fmGetDistMethod(pFunc, &pPartial, nullptr, &pMerge);
    EXPECT_EQ(TSDB_CODE_SUCCESS, code);
    if (code == TSDB_CODE_SUCCESS) {
      EXPECT_EQ(FUNCTION_TYPE_IRATE_PARTIAL, pPartial->funcType);
      EXPECT_EQ(FUNCTION_TYPE_IRATE_MERGE, pMerge->funcType);
    }
    nodesDestroyNode((SNode*)pPartial);
    nodesDestroyNode((SNode*)pMerge);
    nodesDestroyNode((SNode*)pFunc);
  }
}

TEST(FunctionMgtTest, rejectsUnexpectedArgumentsForZeroArgSqlWindowFunctions) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  SFunctionNode* pFunc = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc));
  tstrncpy(pFunc->functionName, "row_number", TSDB_FUNC_NAME_LEN);

  SNode* pArg = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pArg));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&pFunc->pParameterList, pArg));

  char msg[128] = {0};
  EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_NUM, fmGetFuncInfo(pFunc, msg, sizeof(msg)));

  nodesDestroyNode((SNode*)pFunc);
}

TEST(FunctionMgtTest, defersNonAsciiVarcharRegexpValidation) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  SFunctionNode* pFunc = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc));
  tstrncpy(pFunc->functionName, "regexp_extract", TSDB_FUNC_NAME_LEN);
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeAppend(&pFunc->pParameterList,
                                makeVarcharValue("abc", 3)));
  const char pattern[] = {(char)0xff, '['};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeAppend(
                &pFunc->pParameterList,
                makeVarcharValue(pattern, sizeof(pattern))));

  char msg[256] = {0};
  EXPECT_EQ(TSDB_CODE_SUCCESS, fmGetFuncInfo(pFunc, msg, sizeof(msg)));
  nodesDestroyNode((SNode*)pFunc);
}

TEST(FunctionMgtTest, injectsPosixTimezoneOffsetForToCharFallback) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  const int8_t parameterTypes[] = {
      TSDB_DATA_TYPE_TIMESTAMP,
      TSDB_DATA_TYPE_BINARY,
  };
  struct {
    const char* display;
    const char* expected;
  } testCases[] = {
      {"System (UTC, +0800)", "-0800"},
      {"System (UTC, -0500)", "+0500"},
      {"System (UTC, +0000)", "-0000"},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.display);
    ScopedTimezoneDisplay timezoneDisplay(testCase.display);
    SFunctionNode* pFunc =
        createFunctionNode("to_char", parameterTypes, 2);
    ASSERT_NE(nullptr, pFunc);

    char message[128] = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              fmGetFuncInfo(pFunc, message, sizeof(message)));
    ASSERT_EQ(3, LIST_LENGTH(pFunc->pParameterList));

    SValueNode* pTimezone =
        (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2);
    ASSERT_NE(nullptr, pTimezone);
    EXPECT_STREQ(testCase.expected, pTimezone->literal);

    nodesDestroyNode((SNode*)pFunc);
  }
}

TEST(FunctionMgtTest, keepsSessionTimezoneForToCharWithoutNameMap) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());
  ScopedMissingTimezoneNameMap missingNameMap;

  const int8_t parameterTypes[] = {
      TSDB_DATA_TYPE_TIMESTAMP,
      TSDB_DATA_TYPE_BINARY,
  };
  struct {
    const char* sessionName;
    const char* display;
    const char* expected;
  } testCases[] = {
      {"America/New_York", "System (UTC, +0800)", "America/New_York"},
      {"UTC-8", "System (UTC, +0000)", "UTC-8"},
      {"UTC+5:30", "System (UTC, +0000)", "UTC+5:30"},
      {"+08:00", "System (UTC, -0800)", "+0800"},
      {"-05", "System (UTC, +0500)", "-0500"},
      {"Z", "System (UTC, +0000)", "Z"},
      {"z", "System (UTC, +0000)", "z"},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.sessionName);
    ScopedTimezoneDisplay timezoneDisplay(testCase.display);
    SFunctionNode* pFunc =
        createFunctionNode("to_char", parameterTypes, 2);
    ASSERT_NE(nullptr, pFunc);
    auto functionGuard =
        std::unique_ptr<SNode, decltype(&nodesDestroyNode)>((SNode*)pFunc,
                                                            nodesDestroyNode);
    tstrncpy(pFunc->tzName, testCase.sessionName, sizeof(pFunc->tzName));

    char message[128] = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              fmGetFuncInfo(pFunc, message, sizeof(message)));
    ASSERT_EQ(3, LIST_LENGTH(pFunc->pParameterList));
    SValueNode* pTimezone =
        (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2);
    ASSERT_NE(nullptr, pTimezone);
    EXPECT_STREQ(testCase.expected, pTimezone->literal);
  }
}

TEST(FunctionMgtTest, keepsIsoTimezoneOffsetForToIso8601Fallback) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());
  ScopedMissingTimezoneNameMap missingNameMap;

  const int8_t parameterTypes[] = {TSDB_DATA_TYPE_TIMESTAMP};
  struct {
    const char* display;
    const char* sessionName;
    const char* expected;
  } testCases[] = {
      {"System (UTC, +0800)", "UTC-8", "+0800"},
      {"System (UTC, -0500)", "UTC+5", "-0500"},
      {"System (UTC, +0000)", "Z", "+0000"},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.display);
    ScopedTimezoneDisplay timezoneDisplay(testCase.display);
    SFunctionNode* pFunc =
        createFunctionNode("to_iso8601", parameterTypes, 1);
    ASSERT_NE(nullptr, pFunc);
    tstrncpy(pFunc->tzName, testCase.sessionName, sizeof(pFunc->tzName));

    char message[128] = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              fmGetFuncInfo(pFunc, message, sizeof(message)));
    ASSERT_EQ(2, LIST_LENGTH(pFunc->pParameterList));

    SValueNode* pTimezone =
        (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
    ASSERT_NE(nullptr, pTimezone);
    EXPECT_STREQ(testCase.expected, pTimezone->literal);

    nodesDestroyNode((SNode*)pFunc);
  }
}

TEST(FunctionMgtTest, injectsPosixTimezoneOffsetForTimetruncateFallback) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  struct {
    const char* display;
    const char* expected;
  } testCases[] = {
      {"System (UTC, +0800)", "-0800"},
      {"System (UTC, -0500)", "+0500"},
      {"System (UTC, +0000)", "-0000"},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.display);
    ScopedTimezoneDisplay timezoneDisplay(testCase.display);
    SFunctionNode* pFunc = createTimeTruncateNode();
    ASSERT_NE(nullptr, pFunc);

    char message[128] = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              fmGetFuncInfo(pFunc, message, sizeof(message)));
    // ts, unit, use_curr_tz, precision, tz_name, fdow, unitCh.
    ASSERT_EQ(7, LIST_LENGTH(pFunc->pParameterList));

    SValueNode* pTimezone =
        (SValueNode*)nodesListGetNode(pFunc->pParameterList, 4);
    ASSERT_NE(nullptr, pTimezone);
    EXPECT_STREQ(testCase.expected, pTimezone->literal);

    nodesDestroyNode((SNode*)pFunc);
  }
}

// Without an "(ABBR, +HHMM/-HHMM)" suffix, offset extraction fails. The
// leading token is injected verbatim, without sign flipping.
TEST(FunctionMgtTest, fallsBackToTimezoneNameWhenOffsetIsMissing) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  const int8_t parameterTypes[] = {
      TSDB_DATA_TYPE_TIMESTAMP,
      TSDB_DATA_TYPE_BINARY,
  };
  struct {
    const char* display;
    const char* expected;
  } testCases[] = {
      {"Asia/Shanghai (CST)", "Asia/Shanghai"},
      {"Asia/Shanghai", "Asia/Shanghai"},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.display);
    ScopedTimezoneDisplay timezoneDisplay(testCase.display);
    SFunctionNode* pFunc =
        createFunctionNode("to_char", parameterTypes, 2);
    ASSERT_NE(nullptr, pFunc);

    char message[128] = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              fmGetFuncInfo(pFunc, message, sizeof(message)));
    ASSERT_EQ(3, LIST_LENGTH(pFunc->pParameterList));

    SValueNode* pTimezone =
        (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2);
    ASSERT_NE(nullptr, pTimezone);
    EXPECT_STREQ(testCase.expected, pTimezone->literal);

    nodesDestroyNode((SNode*)pFunc);
  }
}

// An IANA default is injected by name so each row resolves its own offset;
// the display offset is only the one in effect when it was formatted.
TEST(FunctionMgtTest, injectsRuleNameForIanaTimezoneFallback) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  const int8_t parameterTypes[] = {
      TSDB_DATA_TYPE_TIMESTAMP,
      TSDB_DATA_TYPE_BINARY,
  };
  ScopedTimezoneDisplay timezoneDisplay("America/New_York (EDT, -0400)");
  struct {
    SFunctionNode* pFunc;
    int32_t        parameterCount;
    int32_t        timezoneIndex;
  } testCases[] = {
      {createFunctionNode("to_iso8601", parameterTypes, 1), 2, 1},
      {createFunctionNode("to_char", parameterTypes, 2), 3, 2},
      // ts, unit, use_curr_tz, precision, tz_name, fdow, unitCh.
      {createTimeTruncateNode(), 7, 4},
  };

  for (const auto& testCase : testCases) {
    ASSERT_NE(nullptr, testCase.pFunc);
    SCOPED_TRACE(testCase.pFunc->functionName);
    char message[128] = {0};
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              fmGetFuncInfo(testCase.pFunc, message, sizeof(message)));
    EXPECT_EQ(testCase.parameterCount,
              LIST_LENGTH(testCase.pFunc->pParameterList));

    SValueNode* pTimezone = (SValueNode*)nodesListGetNode(
        testCase.pFunc->pParameterList, testCase.timezoneIndex);
    EXPECT_NE(nullptr, pTimezone);
    if (pTimezone != nullptr) {
      EXPECT_STREQ("America/New_York", pTimezone->literal);
    }
    nodesDestroyNode((SNode*)testCase.pFunc);
  }
}

// Unloadable names keep the fixed-offset fallback.
TEST(FunctionMgtTest, keepsOffsetFallbackForUnloadableTimezoneName) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  const int8_t parameterTypes[] = {TSDB_DATA_TYPE_TIMESTAMP};
  struct {
    const char* display;
    const char* expected;
  } testCases[] = {
      {"n/a (UTC, +0000)", "+0000"},
      {"Not/A_Zone (XST, +0530)", "+0530"},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.display);
    ScopedTimezoneDisplay timezoneDisplay(testCase.display);
    SFunctionNode* pFunc =
        createFunctionNode("to_iso8601", parameterTypes, 1);
    ASSERT_NE(nullptr, pFunc);

    char message[128] = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              fmGetFuncInfo(pFunc, message, sizeof(message)));
    ASSERT_EQ(2, LIST_LENGTH(pFunc->pParameterList));

    SValueNode* pTimezone =
        (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
    ASSERT_NE(nullptr, pTimezone);
    EXPECT_STREQ(testCase.expected, pTimezone->literal);

    nodesDestroyNode((SNode*)pFunc);
  }
}

TEST(FunctionMgtTest, repeatedQualifiedPlaceholderEvaluationUsesPreparedAncestorIndex) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());
  const int32_t funcId = fmGetFuncId("_twstart");
  ASSERT_GE(funcId, 0);

  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 1063;
  runtime.curIdx = 0;
  runtime.pAncestorContext = createDenseAncestorContext(64);
  ASSERT_NE(nullptr, runtime.pAncestorContext);
  ASSERT_EQ(64, taosArrayGetSize(runtime.pAncestorContext->pParamContexts));
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamAncestorParamIndex(runtime.pAncestorContext));

  SNodeList*  params = nullptr;
  SValueNode* result = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode**>(&result)));
  result->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  result->node.resType.bytes = sizeof(int64_t);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&params, reinterpret_cast<SNode*>(result)));
  SValueNode* layer = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode**>(&layer)));
  layer->node.resType.type = TSDB_DATA_TYPE_INT;
  layer->node.resType.bytes = sizeof(int32_t);
  layer->datum.i = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&params, reinterpret_cast<SNode*>(layer)));

  {
#ifndef _WIN32
    AncestorParamArrayGetGuard arrayGetGuard(runtime.pAncestorContext->pParamContexts);
#endif
    ASSERT_EQ(TSDB_CODE_SUCCESS, fmSetStreamPseudoFuncParamVal(funcId, params, &runtime));
    EXPECT_EQ(1063, result->datum.i);
    ASSERT_EQ(TSDB_CODE_SUCCESS, fmSetStreamPseudoFuncParamVal(funcId, params, &runtime));
    EXPECT_EQ(1063, result->datum.i);
#ifndef _WIN32
    EXPECT_EQ(2, gAncestorParamArrayGetCalls);
#endif
  }

  nodesDestroyList(params);
  tDestroyStreamAncestorContext(&runtime.pAncestorContext);
}

TEST(FunctionMgtTest, validatesActualScalarInputTypesAndArity) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());
  const int32_t concatId = fmGetFuncId("concat");
  const int32_t replaceId = fmGetFuncId("replace");
  const int32_t placeholderId = fmGetFuncId("_twstart");
  ASSERT_GE(concatId, 0);
  ASSERT_GE(replaceId, 0);
  ASSERT_GE(placeholderId, 0);

  const int8_t invalidConcat[] = {
      TSDB_DATA_TYPE_VARCHAR,
      TSDB_DATA_TYPE_INT,
  };
  EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_TYPE,
            fmValidateFuncInputTypes(concatId, invalidConcat, 2));

  const int8_t validConcat[] = {
      TSDB_DATA_TYPE_VARCHAR,
      TSDB_DATA_TYPE_NCHAR,
  };
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            fmValidateFuncInputTypes(concatId, validConcat, 2));
  EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_NUM,
            fmValidateFuncInputTypes(replaceId, validConcat, 1));
  const int8_t tooManyReplace[] = {
      TSDB_DATA_TYPE_VARCHAR,
      TSDB_DATA_TYPE_VARCHAR,
      TSDB_DATA_TYPE_VARCHAR,
      TSDB_DATA_TYPE_VARCHAR,
  };
  EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_NUM,
            fmValidateFuncInputTypes(replaceId, tooManyReplace, 4));
  EXPECT_EQ(TSDB_CODE_QRY_INVALID_INPUT,
            fmValidateFuncInputTypes(-1, validConcat, 2));

  const int8_t executorOnlyPlaceholder[] = {
      TSDB_DATA_TYPE_TIMESTAMP,
      TSDB_DATA_TYPE_INT,
  };
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            fmValidateFuncInputTypes(placeholderId,
                                     executorOnlyPlaceholder, 2));

  const int32_t castId = fmGetFuncId("cast");
  const int32_t timeTruncateId = fmGetFuncId("timetruncate");
  ASSERT_GE(castId, 0);
  ASSERT_GE(timeTruncateId, 0);

  const int8_t translatedCast[] = {
      TSDB_DATA_TYPE_BIGINT,
      TSDB_DATA_TYPE_TINYINT,
  };
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            fmValidateFuncInputTypes(castId, translatedCast, 2));
  const int8_t translatedJsonCast[] = {
      TSDB_DATA_TYPE_JSON,
      TSDB_DATA_TYPE_TINYINT,
  };
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            fmValidateFuncInputTypes(castId, translatedJsonCast, 2));
  const int8_t translatedDecimalCast[] = {
      TSDB_DATA_TYPE_DECIMAL,
      TSDB_DATA_TYPE_TINYINT,
  };
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            fmValidateFuncInputTypes(castId, translatedDecimalCast, 2));
  const int8_t translatedDecimal64Cast[] = {
      TSDB_DATA_TYPE_DECIMAL64,
      TSDB_DATA_TYPE_TINYINT,
  };
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            fmValidateFuncInputTypes(castId, translatedDecimal64Cast, 2));
  const int8_t oversizedCast[] = {
      TSDB_DATA_TYPE_BIGINT,
      TSDB_DATA_TYPE_TINYINT,
      TSDB_DATA_TYPE_TINYINT,
  };
  EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_NUM,
            fmValidateFuncInputTypes(castId, oversizedCast, 3));

  const int8_t translatedTimeTruncate[] = {
      TSDB_DATA_TYPE_TIMESTAMP,
      TSDB_DATA_TYPE_BIGINT,
      TSDB_DATA_TYPE_TINYINT,
      TSDB_DATA_TYPE_TINYINT,
      TSDB_DATA_TYPE_VARCHAR,
      TSDB_DATA_TYPE_TINYINT,
      TSDB_DATA_TYPE_TINYINT,
  };
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            fmValidateFuncInputTypes(timeTruncateId,
                                     translatedTimeTruncate, 7));
  int8_t invalidTimeTruncate[7] = {};
  memcpy(invalidTimeTruncate, translatedTimeTruncate,
         sizeof(translatedTimeTruncate));
  invalidTimeTruncate[4] = TSDB_DATA_TYPE_INT;
  EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_TYPE,
            fmValidateFuncInputTypes(timeTruncateId,
                                     invalidTimeTruncate, 7));

  struct RuntimeLayout {
    const char*         functionName;
    std::vector<int8_t> parameterTypes;
  };
  const RuntimeLayout translatedLayouts[] = {
      {"now", {TSDB_DATA_TYPE_TINYINT}},
      {"today", {TSDB_DATA_TYPE_TINYINT}},
      {"rand", {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_NULL}},
      {"trim", {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_TINYINT}},
      {"to_unixtimestamp", {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT}},
      {"timediff", {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_BIGINT,
                    TSDB_DATA_TYPE_TINYINT}},
      {"weekday", {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_TINYINT}},
      {"dayofweek", {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_TINYINT}},
      {"week", {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT}},
      {"weekofyear", {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_TINYINT}},
      {"date", {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_TINYINT}},
      {"greatest", {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT}},
      {"least", {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT}},
      {"irate", {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT}},
      {"_irate_merge", {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_TINYINT}},
      {"twsum_linear", {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_TINYINT}},
      {"twsum_locf", {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_TINYINT}},
  };
  for (const RuntimeLayout& layout : translatedLayouts) {
    SCOPED_TRACE(layout.functionName);
    const int32_t functionId = fmGetFuncId(layout.functionName);
    ASSERT_GE(functionId, 0);
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              fmValidateFuncInputTypes(functionId, layout.parameterTypes.data(),
                                       static_cast<int32_t>(layout.parameterTypes.size())));
  }
}

TEST(FunctionMgtTest, preservesPreMySqlBitDurationPlanId) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());
  // Before the MySQL additions, bit_duration immediately followed bit_changes.
  const int32_t legacyId = fmGetFuncId("bit_changes") + 1;
  ASSERT_GT(legacyId, 0);
  EXPECT_EQ(legacyId, fmGetFuncId("bit_duration"));
  EXPECT_GT(fmGetFuncId("curdate"), legacyId);
  const int8_t types[] = {TSDB_DATA_TYPE_INT};
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> node(
      reinterpret_cast<SNode*>(createFunctionNode("bit_duration", types, 1)), nodesDestroyNode);
  ASSERT_NE(nullptr, node.get());
  auto* function = reinterpret_cast<SFunctionNode*>(node.get());
  function->funcId = legacyId;
  function->funcType = FUNCTION_TYPE_BIT_DURATION;
  function->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  function->node.resType.bytes = sizeof(int64_t);

  // Persisted stream plans use JSON; distributed plans also carry the ID in TLV.
  for (bool json : {true, false}) {
    SCOPED_TRACE(json ? "JSON" : "TLV");
    char* encoded = nullptr;
    int32_t length = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, json ? nodesNodeToString(node.get(), false, &encoded, &length)
                                     : nodesNodeToMsg(node.get(), &encoded, &length));
    SNode* decoded = nullptr;
    int32_t code = json ? nodesStringToNode(encoded, &decoded) : nodesMsgToNode(encoded, length, &decoded);
    taosMemoryFree(encoded);
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> restored(decoded, nodesDestroyNode);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    ASSERT_NE(nullptr, restored.get());
    auto* restoredFunction = reinterpret_cast<SFunctionNode*>(restored.get());
    EXPECT_EQ(legacyId, restoredFunction->funcId);
    char error[256] = {};
    EXPECT_EQ(TSDB_CODE_SUCCESS, fmValidateFuncNode(restoredFunction, error, sizeof(error))) << error;
    EXPECT_EQ(FUNCTION_TYPE_BIT_DURATION, fmGetFuncTypeFromId(restoredFunction->funcId));
  }
}

TEST(FunctionMgtTest, preservesMySqlDecimalConstantProjection) {
  const int64_t micros = 1709210096123456LL;
  Decimal128 decimal = DEFINE_DECIMAL128(micros, 0);
  for (int8_t type : {TSDB_DATA_TYPE_DECIMAL64, TSDB_DATA_TYPE_DECIMAL}) {
    SCOPED_TRACE(static_cast<int32_t>(type));
    SValueNode node = {};
    node.node.resType.type = type;
    node.node.resType.bytes = tDataTypes[type].bytes;
    node.node.resType.precision = 17;
    node.node.resType.scale = 6;
    if (type == TSDB_DATA_TYPE_DECIMAL64) {
      memcpy(&node.datum.d, &micros, sizeof(micros));
    } else {
      node.datum.p = reinterpret_cast<char*>(&decimal);
    }
    SVariant variant = {};
    std::unique_ptr<SVariant, decltype(&taosVariantDestroy)> variantGuard(&variant, taosVariantDestroy);
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesValueNodeToVariant(&node, &variant));
    const char* value = taosVariantGet(&variant, type);
    ASSERT_NE(nullptr, value);

    SSDataBlock* rawBlock = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&rawBlock));
    ScopedDataBlock block(rawBlock, blockDataDestroy);
    SColumnInfoData column = createColumnInfoData(type, tDataTypes[type].bytes, 0);
    column.info.precision = 17;
    column.info.scale = 6;
    ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(block.get(), &column));
    ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataEnsureCapacity(block.get(), 3));
    auto* output = static_cast<SColumnInfoData*>(taosArrayGet(block->pDataBlock, 0));
    const void* expected = type == TSDB_DATA_TYPE_DECIMAL64 ? static_cast<const void*>(&micros)
                                                          : static_cast<const void*>(&decimal);
    for (int32_t row = 0; row < 3; ++row) {
      ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(output, row, value, false));
      EXPECT_FALSE(colDataIsNull_s(output, row));
      EXPECT_EQ(0, memcmp(expected, colDataGetData(output, row), tDataTypes[type].bytes));
    }
  }
}

class MySqlTimeScalarTest : public ::testing::Test {
 protected:
  void initialize(const char* name, std::vector<int8_t> types, int8_t resultType, int32_t rows) {
    ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());
    const int32_t arguments = static_cast<int32_t>(types.size());
    types.insert(types.end(), {TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT,
                               TSDB_DATA_TYPE_BIGINT, resultType});
    SSDataBlock* block = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&block));
    block_.reset(block);
    for (int32_t i = 0; i < static_cast<int32_t>(types.size()); ++i) {
      SColumnInfoData column = createColumnInfoData(
          types[i], types[i] == TSDB_DATA_TYPE_VARCHAR ? 128 : tDataTypes[types[i]].bytes, i);
      ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(block_.get(), &column));
    }
    ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataEnsureCapacity(block_.get(), rows));
    input_.resize(arguments + 3);
    for (int32_t i = 0; i < arguments + 3; ++i) {
      input_[i].columnData = static_cast<SColumnInfoData*>(taosArrayGet(block_->pDataBlock, i));
      input_[i].numOfRows = i < arguments ? rows : 1;
    }
    output_.columnData = static_cast<SColumnInfoData*>(taosArrayGet(block_->pDataBlock, arguments + 3));
    output_.numOfRows = rows;
    const int8_t precision = TSDB_TIME_PRECISION_MICRO;
    const int8_t operation = fmGetFuncType(name) - FUNCTION_TYPE_MYSQL_CURDATE;
    const int64_t clock = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(input_[arguments].columnData, 0, (const char*)&precision, false));
    ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(input_[arguments + 1].columnData, 0, (const char*)&operation, false));
    ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(input_[arguments + 2].columnData, 0, (const char*)&clock, false));
    ASSERT_EQ(TSDB_CODE_SUCCESS, fmGetScalarFuncExecFuncs(fmGetFuncId(name), &functions_));
    ASSERT_NE(nullptr, functions_.process);
  }

  int32_t setText(int32_t argument, int32_t row, const char* text) {
    char buffer[128] = {};
    varDataSetLen(buffer, strlen(text));
    memcpy(varDataVal(buffer), text, strlen(text));
    return colDataSetVal(input_[argument].columnData, row, buffer, false);
  }

  int32_t execute() { return functions_.process(input_.data(), static_cast<int32_t>(input_.size()), &output_); }

  ScopedDataBlock block_{nullptr, blockDataDestroy};
  std::vector<SScalarParam> input_;
  SScalarParam output_ = {};
  SScalarFuncExecFuncs functions_ = {};
};

TEST_F(MySqlTimeScalarTest, EmptyUserInputDoesNotBroadcastHiddenConstants) {
  ASSERT_NO_FATAL_FAILURE(initialize("year", {TSDB_DATA_TYPE_TIMESTAMP}, TSDB_DATA_TYPE_BIGINT, 1));
  int64_t timestamp = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(input_[0].columnData, 0, (const char*)&timestamp, false));
  input_[0].numOfRows = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
  ASSERT_EQ(0, output_.numOfRows);

  for (int32_t emptyArgument : {0, 1}) {
    ASSERT_NO_FATAL_FAILURE(initialize("date_format", {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_VARCHAR},
                                      TSDB_DATA_TYPE_VARCHAR, 1));
    ASSERT_EQ(TSDB_CODE_SUCCESS, setText(0, 0, "2024-02-29"));
    ASSERT_EQ(TSDB_CODE_SUCCESS, setText(1, 0, "%Y"));
    SColumnInfoData empty = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 128, 0);
    input_[emptyArgument].columnData = &empty;
    input_[emptyArgument].numOfRows = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
    EXPECT_EQ(0, output_.numOfRows);
    input_[1 - emptyArgument].numOfRows = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
    EXPECT_EQ(0, output_.numOfRows);
  }
  ASSERT_NO_FATAL_FAILURE(initialize("unix_timestamp", {}, TSDB_DATA_TYPE_BIGINT, 1));
  ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
  EXPECT_EQ(1, output_.numOfRows);
}

TEST_F(MySqlTimeScalarTest, NativeTimestampRespectsCalendarYearRange) {
  struct ZoneGuard {
    timezone_t value;
    ~ZoneGuard() { tzfree(value); }
  } zone{tzalloc("UTC")};
  ASSERT_NE(nullptr, zone.value);
  const int64_t milliseconds[] = {-62167219200001LL, -62167219200000LL, 253402300799999LL, 253402300800000LL};
  for (int8_t precision : {TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO}) {
    ASSERT_NO_FATAL_FAILURE(initialize("year", {TSDB_DATA_TYPE_TIMESTAMP}, TSDB_DATA_TYPE_BIGINT, 4));
    input_[0].columnData->info.precision = precision;
    input_[0].tz = zone.value;
    for (int32_t row = 0; row < 4; ++row) {
      int64_t ticks = milliseconds[row] * (precision == TSDB_TIME_PRECISION_MICRO ? 1000 : 1);
      ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(input_[0].columnData, row, (const char*)&ticks, false));
    }
    ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
    ASSERT_TRUE(colDataIsNull_s(output_.columnData, 0));
    ASSERT_TRUE(colDataIsNull_s(output_.columnData, 3));
    ASSERT_FALSE(colDataIsNull_s(output_.columnData, 1));
    ASSERT_FALSE(colDataIsNull_s(output_.columnData, 2));
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(colDataGetData(output_.columnData, 1)));
    EXPECT_EQ(9999, *reinterpret_cast<int64_t*>(colDataGetData(output_.columnData, 2)));
  }
}

TEST_F(MySqlTimeScalarTest, ExplicitOffsetDoesNotNormalizeInvalidSeconds) {
  const char* texts[] = {"2024-02-29T12:34:60Z", "2024-12-31T23:59:61Z",
                         "2024-02-29T12:34:60+08:00", "2024-12-31T23:59:61-05:00",
                         "2024-02-29T12:34:59Z", "2024-02-29T12:34:59+08:00"};
  ASSERT_NO_FATAL_FAILURE(initialize("second", {TSDB_DATA_TYPE_VARCHAR}, TSDB_DATA_TYPE_BIGINT, 6));
  for (int32_t row = 0; row < 6; ++row) ASSERT_EQ(TSDB_CODE_SUCCESS, setText(0, row, texts[row]));
  ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
  for (int32_t row = 0; row < 6; ++row) {
    SCOPED_TRACE(texts[row]);
    ASSERT_EQ(row < 4, colDataIsNull_s(output_.columnData, row));
    if (row >= 4) EXPECT_EQ(59, *reinterpret_cast<int64_t*>(colDataGetData(output_.columnData, row)));
  }
}

TEST_F(MySqlTimeScalarTest, UnixTimestampDecimalCoefficientPreservesBoundaries) {
  const int64_t micros[] = {0, 1, 999999, 1000000, 1709210096123456LL, 32536771199999999LL, 32536771200000000LL};
  ASSERT_NO_FATAL_FAILURE(initialize("unix_timestamp", {TSDB_DATA_TYPE_TIMESTAMP}, TSDB_DATA_TYPE_DECIMAL, 7));
  input_[0].columnData->info.precision = TSDB_TIME_PRECISION_MICRO;
  output_.columnData->info.precision = 17;
  output_.columnData->info.scale = 6;
  for (int32_t row = 0; row < 7; ++row) {
    ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(input_[0].columnData, row, (const char*)&micros[row], false));
  }
  ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
  for (int32_t row = 0; row < 7; ++row) {
    ASSERT_EQ(row == 6, colDataIsNull_s(output_.columnData, row));
    if (row == 6) continue;
    const auto* value = reinterpret_cast<const Decimal128*>(colDataGetData(output_.columnData, row));
    EXPECT_EQ(0, DECIMAL128_HIGH_WORD(value));
    EXPECT_EQ(static_cast<uint64_t>(micros[row]), DECIMAL128_LOW_WORD(value));
  }
}

TEST_F(MySqlTimeScalarTest, ExplicitOffsetPreservesParsedCalendarFields) {
  struct ZoneGuard {
    timezone_t value;
    ~ZoneGuard() { tzfree(value); }
  } zone{tzalloc("UTC")};
  ASSERT_NE(nullptr, zone.value);
  const struct {
    const char* input;
    const char* expected;
  } cases[] = {
      {"24-02-29T12:34:56Z", "2024-02-29 12:34:56.000000"},
      {"69-12-31T23:00:00+08:00", "2069-12-31 15:00:00.000000"},
      {"70-01-01T00:00:00+00", "1970-01-01 00:00:00.000000"},
      {"24/02/29T12^34*56+0800", "2024-02-29 04:34:56.000000"},
      {"24-02-29T12-34-56Z", "2024-02-29 12:34:56.000000"},
      {"24-02-29T12-34-56+08", "2024-02-29 04:34:56.000000"},
      {"1969-12-31T23:59:59Z", "1969-12-31 23:59:59.000000"},
      {"1969-12-31T23:59:59.123456Z", "1969-12-31 23:59:59.123456"},
      {"1969-12-31T23:59:59+08:00", "1969-12-31 15:59:59.000000"},
      {"0000-01-01T00:00:00Z", "0000-01-01 00:00:00.000000"},
      {"0000-02-28T00:00:00Z", "0000-02-28 00:00:00.000000"},
      {"0000-03-01T00:00:00Z", "0000-03-01 00:00:00.000000"},
      {"9999-12-31T23:59:59.999999Z", "9999-12-31 23:59:59.999999"},
      {"9999-12-31T23:59:59-01:00", nullptr},
      {"0000-01-01T00:00:00+01:00", nullptr},
      {"2024-02-29T12:34:56+15:00", nullptr},
      {"2024-02-29T12:34:56+14:01", nullptr},
      {"2024-02-29T12:34:56+0", nullptr},
      {"2024-02-29T12:34:56+", nullptr},
  };
  int32_t rows = static_cast<int32_t>(sizeof(cases) / sizeof(cases[0]));
  ASSERT_NO_FATAL_FAILURE(initialize("date_format", {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_VARCHAR},
                                    TSDB_DATA_TYPE_VARCHAR, rows));
  input_[0].tz = zone.value;
  input_[1].numOfRows = 1;
  ASSERT_EQ(TSDB_CODE_SUCCESS, setText(1, 0, "%Y-%m-%d %H:%i:%s.%f"));
  for (int32_t row = 0; row < rows; ++row) ASSERT_EQ(TSDB_CODE_SUCCESS, setText(0, row, cases[row].input));
  ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
  for (int32_t row = 0; row < rows; ++row) {
    SCOPED_TRACE(cases[row].input);
    bool isNull = colDataIsNull_s(output_.columnData, row);
    EXPECT_EQ(cases[row].expected == nullptr, isNull);
    if (isNull) continue;
    if (cases[row].expected == nullptr) continue;
    const char* data = colDataGetData(output_.columnData, row);
    EXPECT_EQ(cases[row].expected, std::string(varDataVal(data), varDataLen(data)));
  }
  const char embeddedNul[] = "2024-02-29T12:34:56+08\0:00";
  char value[64] = {};
  varDataSetLen(value, sizeof(embeddedNul) - 1);
  memcpy(varDataVal(value), embeddedNul, sizeof(embeddedNul) - 1);
  ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(input_[0].columnData, 0, value, false));
  input_[0].numOfRows = 1;
  ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
  EXPECT_TRUE(colDataIsNull_s(output_.columnData, 0));
}

TEST_F(MySqlTimeScalarTest, ExtractDistinguishesDateUnitsFromTimeUnits) {
  const char* texts[] = {"12:34:56.123456", "-25:34:56.123456", "838:59:59", "2024-02-29",
                         "2024-00-10", "0000-00-00", "2060-11-31"};
  ASSERT_NO_FATAL_FAILURE(initialize("extract", {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_VARCHAR},
                                    TSDB_DATA_TYPE_BIGINT, 7));
  input_[0].numOfRows = 1;
  for (int32_t row = 0; row < 7; ++row) ASSERT_EQ(TSDB_CODE_SUCCESS, setText(1, row, texts[row]));
  struct DateUnit {
    const char* unit;
    int64_t complete;
    int64_t incomplete;
  };
  for (const DateUnit& unit : {DateUnit{"YEAR", 2024, 2024}, {"YEAR_MONTH", 202402, 202400},
                               {"QUARTER", 1, 0}, {"MONTH", 2, 0}, {"WEEK", 8, 613566753}, {"DAY", 29, 10}}) {
    SCOPED_TRACE(unit.unit);
    ASSERT_EQ(TSDB_CODE_SUCCESS, setText(0, 0, unit.unit));
    ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
    ASSERT_EQ(7, output_.numOfRows);
    for (int32_t row : {0, 1, 2, 6}) EXPECT_TRUE(colDataIsNull_s(output_.columnData, row)) << texts[row];
    ASSERT_FALSE(colDataIsNull_s(output_.columnData, 3));
    EXPECT_EQ(unit.complete, *reinterpret_cast<int64_t*>(colDataGetData(output_.columnData, 3)));
    for (int32_t row : {4, 5}) {
      bool null = row == 5 && strcmp(unit.unit, "WEEK") == 0;
      ASSERT_EQ(null, colDataIsNull_s(output_.columnData, row));
      if (!null) {
        EXPECT_EQ(row == 4 ? unit.incomplete : 0,
                  *reinterpret_cast<int64_t*>(colDataGetData(output_.columnData, row)));
      }
    }
  }
  for (const char* unit : {"HOUR", "HOUR_MINUTE"}) {
    SCOPED_TRACE(unit);
    ASSERT_EQ(TSDB_CODE_SUCCESS, setText(0, 0, unit));
    ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
    const int64_t expected[] = {12, -25, 838};
    for (int32_t row = 0; row < 3; ++row) {
      ASSERT_FALSE(colDataIsNull_s(output_.columnData, row));
      int64_t value = expected[row];
      if (strcmp(unit, "HOUR_MINUTE") == 0) value = value * 100 + (row == 1 ? -34 : row == 0 ? 34 : 59);
      EXPECT_EQ(value, *reinterpret_cast<int64_t*>(colDataGetData(output_.columnData, row)));
    }
    for (int32_t row : {3, 4, 5}) {
      ASSERT_FALSE(colDataIsNull_s(output_.columnData, row));
      EXPECT_EQ(0, *reinterpret_cast<int64_t*>(colDataGetData(output_.columnData, row)));
    }
  }
}

TEST_F(MySqlTimeScalarTest, StrToDateIgnoresLongTails) {
  for (int8_t textType : {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_NCHAR}) {
    for (int8_t formatType : {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_NCHAR}) {
      SCOPED_TRACE(static_cast<int>(textType));
      SCOPED_TRACE(static_cast<int>(formatType));
      ASSERT_NO_FATAL_FAILURE(initialize("str_to_date", {textType, formatType}, TSDB_DATA_TYPE_VARCHAR, 6));
      input_[1].numOfRows = 1;
      auto setLongText = [&](int32_t argument, int32_t row, const std::string& text) {
        bool nchar = input_[argument].columnData->info.type == TSDB_DATA_TYPE_NCHAR;
        size_t bytes = text.size() * (nchar ? sizeof(TdUcs4) : 1);
        std::vector<char> buffer(VARSTR_HEADER_SIZE + bytes);
        varDataSetLen(buffer.data(), bytes);
        if (nchar) {
          for (size_t i = 0; i < text.size(); ++i) {
            TdUcs4 character = static_cast<unsigned char>(text[i]);
            memcpy(varDataVal(buffer.data()) + i * sizeof(character), &character, sizeof(character));
          }
        } else {
          memcpy(varDataVal(buffer.data()), text.data(), bytes);
        }
        if (static_cast<size_t>(input_[argument].columnData->info.bytes) < buffer.size()) {
          input_[argument].columnData->info.bytes = static_cast<int32_t>(buffer.size());
        }
        return colDataSetVal(input_[argument].columnData, row, buffer.data(), false);
      };
      // A long NCHAR format exercises conversion-buffer reuse after converting the input.
      const std::string format = std::string("%Y-%m-%d") + std::string(1100, ' ');
      ASSERT_EQ(TSDB_CODE_SUCCESS, setLongText(1, 0, format));
      const int32_t lengths[] = {4095, 4096, 8192, 16000, 4096};
      for (int32_t row = 0; row < 5; ++row) {
        std::string text = row == 4 ? "2060-11-31" : "2024-02-29";
        text.append(lengths[row] - text.size(), 'x');
        ASSERT_EQ(TSDB_CODE_SUCCESS, setLongText(0, row, text));
      }
      colDataSetNULL(input_[0].columnData, 5);
      ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
      EXPECT_EQ(6, output_.numOfRows);
      for (int32_t row = 0; row < 4; ++row) {
        ASSERT_FALSE(colDataIsNull_s(output_.columnData, row)) << lengths[row];
        char* result = colDataGetData(output_.columnData, row);
        EXPECT_EQ("2024-02-29", std::string(varDataVal(result), varDataLen(result)));
      }
      EXPECT_TRUE(colDataIsNull_s(output_.columnData, 4));
      EXPECT_TRUE(colDataIsNull_s(output_.columnData, 5));
    }
  }
}

TEST_F(MySqlTimeScalarTest, DateOnlyTextKeepsCalendarTimeFields) {
  const char* names[] = {"hour", "minute", "second", "microsecond"};
  const int64_t expected[] = {12, 34, 56, 123456};
  for (int32_t i = 0; i < 4; ++i) {
    SCOPED_TRACE(names[i]);
    ASSERT_NO_FATAL_FAILURE(initialize(names[i], {TSDB_DATA_TYPE_VARCHAR}, TSDB_DATA_TYPE_BIGINT, 2));
    ASSERT_EQ(TSDB_CODE_SUCCESS, setText(0, 0, "2024-02-29"));
    ASSERT_EQ(TSDB_CODE_SUCCESS, setText(0, 1, "2024-02-29 12:34:56.123456"));
    ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
    for (int32_t row = 0; row < 2; ++row) {
      ASSERT_FALSE(colDataIsNull_s(output_.columnData, row));
      EXPECT_EQ(row == 0 ? 0 : expected[i],
                *reinterpret_cast<int64_t*>(colDataGetData(output_.columnData, row)));
    }
  }
}

TEST_F(MySqlTimeScalarTest, UnixTimestampRejectsNegativeTicksBeforePrecisionConversion) {
  const int64_t ticks[] = {-1000, -999, -1, 0, 1, 999, 1000};
  for (int8_t precision : {TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO}) {
    SCOPED_TRACE(static_cast<int32_t>(precision));
    ASSERT_NO_FATAL_FAILURE(initialize("unix_timestamp", {TSDB_DATA_TYPE_TIMESTAMP}, TSDB_DATA_TYPE_DECIMAL, 7));
    input_[0].columnData->info.precision = precision;
    output_.columnData->info.precision = 17;
    output_.columnData->info.scale = 6;
    ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(input_[1].columnData, 0, (const char*)&precision, false));
    for (int32_t row = 0; row < 7; ++row) {
      ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(input_[0].columnData, row, (const char*)&ticks[row], false));
    }
    ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
    ASSERT_EQ(7, output_.numOfRows);
    for (int32_t row = 0; row < 7; ++row) {
      SCOPED_TRACE(ticks[row]);
      ASSERT_EQ(ticks[row] < 0, colDataIsNull_s(output_.columnData, row));
      if (ticks[row] < 0) continue;
      const auto* value = reinterpret_cast<const Decimal128*>(colDataGetData(output_.columnData, row));
      int64_t microseconds = precision == TSDB_TIME_PRECISION_MILLI ? ticks[row] * 1000 :
                            precision == TSDB_TIME_PRECISION_MICRO ? ticks[row] : ticks[row] / 1000;
      EXPECT_EQ(0, DECIMAL128_HIGH_WORD(value));
      EXPECT_EQ(static_cast<uint64_t>(microseconds), DECIMAL128_LOW_WORD(value));
    }
  }
}

TEST_F(MySqlTimeScalarTest, ParsesRelaxedDatesAndPreservesTimeOnlyInputs) {
  const char* dates[] = {"2024/02/29", "24-02-29", "69@12@31", "70^01^01", "00/02/29", "23/02/29", "2060/11/31"};
  const int64_t years[] = {2024, 2024, 2069, 1970, 2000};
  ASSERT_NO_FATAL_FAILURE(initialize("year", {TSDB_DATA_TYPE_VARCHAR}, TSDB_DATA_TYPE_BIGINT, 7));
  for (int32_t row = 0; row < 7; ++row) ASSERT_EQ(TSDB_CODE_SUCCESS, setText(0, row, dates[row]));
  ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
  for (int32_t row = 0; row < 7; ++row) {
    SCOPED_TRACE(dates[row]);
    ASSERT_EQ(row >= 5, colDataIsNull_s(output_.columnData, row));
    if (row < 5) EXPECT_EQ(years[row], *reinterpret_cast<int64_t*>(colDataGetData(output_.columnData, row)));
  }
  const char* times[] = {"12:34:56", "-25:34:56", "2024/02/29 11*30*45", "24:02:29 13:34:56"};
  const int64_t hours[] = {12, 25, 11, 13};
  ASSERT_NO_FATAL_FAILURE(initialize("hour", {TSDB_DATA_TYPE_VARCHAR}, TSDB_DATA_TYPE_BIGINT, 4));
  for (int32_t row = 0; row < 4; ++row) ASSERT_EQ(TSDB_CODE_SUCCESS, setText(0, row, times[row]));
  ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
  for (int32_t row = 0; row < 4; ++row) {
    SCOPED_TRACE(times[row]);
    ASSERT_FALSE(colDataIsNull_s(output_.columnData, row));
    EXPECT_EQ(hours[row], *reinterpret_cast<int64_t*>(colDataGetData(output_.columnData, row)));
  }
}

TEST_F(MySqlTimeScalarTest, UnixTimestampMapsSpringGapsToFirstValidInstant) {
  struct ZoneGuard {
    timezone_t value;
    ~ZoneGuard() { tzfree(value); }
  };
  const struct {
    const char* zone;
    const char* text;
    int64_t micros;
  } cases[] = {
      {"America/New_York", "2024-03-10 01:59:59.999999", 1710053999999999LL},
      {"America/New_York", "2024-03-10 02:00:00", 1710054000000000LL},
      {"America/New_York", "2024-03-10 02:30:00.123456", 1710054000123456LL},
      {"America/New_York", "2024-03-10 02:59:59.999999", 1710054000999999LL},
      {"America/New_York", "2024-03-10 03:00:00", 1710054000000000LL},
      {"America/New_York", "2024-03-10T02:30:00-05:00", 1710055800000000LL},
      {"Australia/Lord_Howe", "2024-10-06 02:15:00.123456", 1728142200123456LL},
      {"Pacific/Apia", "2011-12-29 23:59:59.999999", 1325239199999999LL},
      {"Pacific/Apia", "2011-12-30 00:00:00", 1325239200000000LL},
      {"Pacific/Apia", "2011-12-30 12:00:00.123456", 1325239200123456LL},
      {"Pacific/Apia", "2011-12-31 00:00:00", 1325239200000000LL},
      {"Pacific/Apia", "2011-12-31 12:00:00.123456", 1325282400123456LL},
      {"UTC", "1970-01-01 00:00:00.123456", 123456},
      {"UTC", "1969-12-31 23:59:59.999999", -1},
      {"UTC", "2024-02-30 02:30:00", -1},
  };
  for (const auto& test : cases) {
    SCOPED_TRACE(test.zone);
    SCOPED_TRACE(test.text);
    ZoneGuard zone{tzalloc(test.zone)};
    ASSERT_NE(nullptr, zone.value);
    ASSERT_NO_FATAL_FAILURE(initialize("unix_timestamp", {TSDB_DATA_TYPE_VARCHAR}, TSDB_DATA_TYPE_DECIMAL, 1));
    input_[0].tz = zone.value;
    output_.columnData->info.precision = 17;
    output_.columnData->info.scale = 6;
    ASSERT_EQ(TSDB_CODE_SUCCESS, setText(0, 0, test.text));
    ASSERT_EQ(TSDB_CODE_SUCCESS, execute());
    ASSERT_EQ(test.micros < 0, colDataIsNull_s(output_.columnData, 0));
    if (test.micros < 0) continue;
    const auto* value = reinterpret_cast<const Decimal128*>(colDataGetData(output_.columnData, 0));
    EXPECT_EQ(0, DECIMAL128_HIGH_WORD(value));
    EXPECT_EQ(static_cast<uint64_t>(test.micros), DECIMAL128_LOW_WORD(value));
  }
}

TEST(FunctionMgtTest, validatesMySqlDateTimeRuntimeParameters) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());
  const int32_t curdateId = fmGetFuncId("curdate");
  const int32_t yearId = fmGetFuncId("year");
  ASSERT_GE(curdateId, 0);
  ASSERT_GE(yearId, 0);

  const int8_t clockOnly[] = {TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT};
  EXPECT_EQ(TSDB_CODE_SUCCESS, fmValidateFuncInputTypes(curdateId, clockOnly, 3));
  EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_NUM, fmValidateFuncInputTypes(curdateId, clockOnly, 2));

  const int8_t dateAndClock[] = {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_TINYINT,
                                TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT};
  EXPECT_EQ(TSDB_CODE_SUCCESS, fmValidateFuncInputTypes(yearId, dateAndClock, 4));
  EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_NUM, fmValidateFuncInputTypes(yearId, clockOnly, 3));
  const int8_t invalidClock[] = {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_TINYINT,
                                 TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT};
  EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_TYPE, fmValidateFuncInputTypes(yearId, invalidClock, 4));
}

TEST(FunctionMgtTest, validatesAllMySqlDateTimeUserInputTypes) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());
  const std::vector<int8_t> strings = {TSDB_DATA_TYPE_NULL, TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_NCHAR};
  std::vector<int8_t> dates = strings;
  dates.push_back(TSDB_DATA_TYPE_TIMESTAMP);
  const std::vector<int8_t> integers = {
      TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT,
      TSDB_DATA_TYPE_UTINYINT, TSDB_DATA_TYPE_USMALLINT, TSDB_DATA_TYPE_UINT, TSDB_DATA_TYPE_UBIGINT};
  std::vector<int8_t> modes = integers;
  modes.push_back(TSDB_DATA_TYPE_NULL);
  std::vector<int8_t> numbers = modes;
  numbers.insert(numbers.end(), {TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE,
                                 TSDB_DATA_TYPE_DECIMAL64, TSDB_DATA_TYPE_DECIMAL});
  struct Signature {
    const char* name;
    int32_t minArgs;
    std::vector<std::vector<int8_t>> types;
  };
  const Signature signatures[] = {
      {"curdate", 0, {}}, {"current_date", 0, {}},
      {"curtime", 0, {integers}}, {"current_time", 0, {integers}},
      {"current_timestamp", 0, {integers}}, {"datediff", 2, {dates, dates}},
      {"date_format", 2, {dates, strings}}, {"day", 1, {dates}},
      {"dayname", 1, {dates}}, {"dayofmonth", 1, {dates}}, {"dayofyear", 1, {dates}},
      {"extract", 2, {strings, dates}}, {"from_days", 1, {numbers}},
      {"from_unixtime", 1, {numbers, strings}}, {"hour", 1, {dates}},
      {"microsecond", 1, {dates}}, {"minute", 1, {dates}}, {"month", 1, {dates}},
      {"monthname", 1, {dates}}, {"quarter", 1, {dates}}, {"second", 1, {dates}},
      {"str_to_date", 2, {strings, strings}}, {"to_days", 1, {dates}},
      {"to_seconds", 1, {dates}}, {"unix_timestamp", 0, {dates}},
      {"year", 1, {dates}}, {"yearweek", 1, {dates, modes}},
  };
  for (const auto& signature : signatures) {
    SCOPED_TRACE(signature.name);
    const int32_t id = fmGetFuncId(signature.name);
    ASSERT_GE(id, 0);
    const int32_t maxArgs = static_cast<int32_t>(signature.types.size());
    for (int32_t count = 0; count <= maxArgs + 1; ++count) {
      std::vector<int8_t> types(count, TSDB_DATA_TYPE_VARCHAR);
      for (int32_t arg = 0; arg < (std::min)(count, maxArgs); ++arg) {
        types[arg] = signature.types[arg].back();
      }
      types.insert(types.end(), {TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT});
      const int32_t expected = count >= signature.minArgs && count <= maxArgs
                                   ? TSDB_CODE_SUCCESS : TSDB_CODE_FUNC_FUNTION_PARA_NUM;
      ASSERT_EQ(expected, fmValidateFuncInputTypes(id, types.data(), static_cast<int32_t>(types.size())));
      if (expected != TSDB_CODE_SUCCESS) continue;

      std::unique_ptr<SNode, decltype(&nodesDestroyNode)> translated(
          reinterpret_cast<SNode*>(createFunctionNode(signature.name, types.data(), count)), nodesDestroyNode);
      ASSERT_NE(nullptr, translated.get());
      auto* function = reinterpret_cast<SFunctionNode*>(translated.get());
      if (std::strcmp(signature.name, "extract") == 0) {
        auto* unit = reinterpret_cast<SValueNode*>(nodesListGetNode(function->pParameterList, 0));
        unit->literal = taosStrdup("YEAR");
        ASSERT_NE(nullptr, unit->literal);
      }
      char message[256] = {};
      ASSERT_EQ(TSDB_CODE_SUCCESS, fmGetFuncInfo(function, message, sizeof(message))) << message;
      ASSERT_EQ(count + 2, LIST_LENGTH(function->pParameterList));
      // Translation appends precision/operation; the parser then appends the statement clock.
      SNode* clock = nullptr;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &clock));
      reinterpret_cast<SValueNode*>(clock)->node.resType.type = TSDB_DATA_TYPE_BIGINT;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListStrictAppend(function->pParameterList, clock));
      EXPECT_EQ(TSDB_CODE_SUCCESS, fmValidateFuncNode(function, message, sizeof(message)));

      for (int32_t arg = 0; arg < count; ++arg) {
        SCOPED_TRACE(arg);
        const auto& accepted = signature.types[arg];
        for (int8_t type = TSDB_DATA_TYPE_NULL; type < TSDB_DATA_TYPE_MAX; ++type) {
          SCOPED_TRACE(static_cast<int32_t>(type));
          types[arg] = type;
          bool valid = std::find(accepted.begin(), accepted.end(), type) != accepted.end();
          EXPECT_EQ(valid ? TSDB_CODE_SUCCESS : TSDB_CODE_FUNC_FUNTION_PARA_TYPE,
                    fmValidateFuncInputTypes(id, types.data(), static_cast<int32_t>(types.size())));
        }
        types[arg] = accepted.back();
      }
    }
  }
}

TEST(FunctionMgtTest, rejectsInvalidMySqlDateTimeRuntimeContext) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());
  const int8_t types[] = {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_TINYINT,
                          TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT};
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> node(
      reinterpret_cast<SNode*>(createFunctionNode("year", types, 4)), nodesDestroyNode);
  ASSERT_NE(nullptr, node.get());
  SFunctionNode* function = reinterpret_cast<SFunctionNode*>(node.get());
  function->funcId = fmGetFuncId("year");
  ASSERT_GE(function->funcId, 0);
  auto* precision = reinterpret_cast<SValueNode*>(nodesListGetNode(function->pParameterList, 1));
  auto* operation = reinterpret_cast<SValueNode*>(nodesListGetNode(function->pParameterList, 2));
  operation->datum.i = FUNCTION_TYPE_MYSQL_YEAR - FUNCTION_TYPE_MYSQL_CURDATE;
  char error[256] = {};
  EXPECT_EQ(TSDB_CODE_SUCCESS, fmValidateFuncNode(function, error, sizeof(error)));
  operation->datum.i = FUNCTION_TYPE_MYSQL_STR_TO_DATE - FUNCTION_TYPE_MYSQL_CURDATE;
  EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_VALUE, fmValidateFuncNode(function, error, sizeof(error)));
  operation->datum.i = FUNCTION_TYPE_MYSQL_YEAR - FUNCTION_TYPE_MYSQL_CURDATE;
  precision->datum.i = TSDB_TIME_PRECISION_NANO + 1;
  EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_VALUE, fmValidateFuncNode(function, error, sizeof(error)));
  precision->datum.i = TSDB_TIME_PRECISION_MILLI;
  for (int32_t i = 1; i < 4; ++i) {
    auto* value = reinterpret_cast<SValueNode*>(nodesListGetNode(function->pParameterList, i));
    value->isNull = true;
    EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_TYPE, fmValidateFuncNode(function, error, sizeof(error)));
    value->isNull = false;
    value->node.resType.type = TSDB_DATA_TYPE_VARCHAR;
    EXPECT_EQ(TSDB_CODE_FUNC_FUNTION_PARA_TYPE, fmValidateFuncNode(function, error, sizeof(error)));
    value->node.resType.type = types[i];
    value->node.type = QUERY_NODE_COLUMN;
    EXPECT_EQ(TSDB_CODE_QRY_INVALID_INPUT, fmValidateFuncNode(function, error, sizeof(error)));
    value->node.type = QUERY_NODE_VALUE;
  }
  EXPECT_EQ(TSDB_CODE_SUCCESS, fmValidateFuncNode(function, error, sizeof(error)));
}

TEST(FunctionMgtTest, ValidatesFunctionNodeIdentityAndUdfIds) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());
  char error[256] = {};

  EXPECT_EQ(TSDB_CODE_QRY_INVALID_INPUT,
            fmValidateFuncNode(nullptr, error, sizeof(error)));

  SFunctionNode function = {};
  function.funcId = fmGetFuncId("concat");
  ASSERT_GE(function.funcId, 0);
  tstrncpy(function.functionName, "concat", sizeof(function.functionName));
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            fmValidateFuncNode(&function, error, sizeof(error)));

  tstrncpy(function.functionName, "replace", sizeof(function.functionName));
  EXPECT_EQ(TSDB_CODE_QRY_INVALID_INPUT,
            fmValidateFuncNode(&function, error, sizeof(error)));

  for (const char* pseudoName : {"_tidlestart", "_tidleend"}) {
    int32_t matched = 0;
    tstrncpy(function.functionName, pseudoName, sizeof(function.functionName));
    for (int32_t functionId = 0; functionId < funcMgtBuiltinsNum; ++functionId) {
      if (std::strcmp(funcMgtBuiltins[functionId].name, pseudoName) == 0) {
        function.funcId = functionId;
        EXPECT_EQ(TSDB_CODE_SUCCESS, fmValidateFuncNode(&function, error, sizeof(error)));
        ++matched;
      }
    }
    EXPECT_GT(matched, 0);
  }

  memset(function.functionName, 'x', sizeof(function.functionName));
  EXPECT_EQ(TSDB_CODE_QRY_INVALID_INPUT,
            fmValidateFuncNode(&function, error, sizeof(error)));

  function.funcId = FUNC_UDF_ID_START + 1;
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            fmValidateFuncNode(&function, error, sizeof(error)));
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            fmValidateFuncInputTypes(function.funcId, nullptr, 0));
}

TEST(UdfProtocolTest, BoundedRequestDecoderRejectsMalformedFrames) {
  SUdfRequest request = {};
  request.seqNum = 42;
  request.type = UDF_TASK_SETUP;
  tstrncpy(request.setup.udfName, "safe_udf", sizeof(request.setup.udfName));
  request.msgLen = encodeUdfRequest(nullptr, &request);
  ASSERT_GT(request.msgLen, 0);

  std::vector<char> frame(request.msgLen);
  void*             cursor = frame.data();
  ASSERT_GT(encodeUdfRequest(&cursor, &request), 0);
  ASSERT_EQ(request.msgLen,
            static_cast<char*>(cursor) - frame.data());

  DecodedUdfRequestGuard decoded;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            decodeUdfRequestBounded(frame.data(), frame.size(), &decoded.value));
  EXPECT_EQ(request.seqNum, decoded.value.seqNum);
  EXPECT_STREQ(request.setup.udfName, decoded.value.setup.udfName);

  SUdfRequest truncated = {};
  EXPECT_EQ(TSDB_CODE_MSG_DECODE_ERROR,
            decodeUdfRequestBounded(frame.data(), frame.size() - 1, &truncated));

  int32_t invalidLength = request.msgLen - 1;
  memcpy(frame.data(), &invalidLength, sizeof(invalidLength));
  SUdfRequest invalidFrame = {};
  EXPECT_EQ(TSDB_CODE_MSG_DECODE_ERROR,
            decodeUdfRequestBounded(frame.data(), frame.size(), &invalidFrame));

  SUdfRequest invalidCall = {};
  invalidCall.seqNum = 43;
  invalidCall.type = UDF_TASK_CALL;
  invalidCall.call.udfHandle = 1;
  invalidCall.call.callType = TSDB_UDF_CALL_AGG_MERGE;
  invalidCall.msgLen = encodeUdfRequest(nullptr, &invalidCall);
  ASSERT_GT(invalidCall.msgLen, 0);
  frame.resize(invalidCall.msgLen);
  cursor = frame.data();
  ASSERT_GT(encodeUdfRequest(&cursor, &invalidCall), 0);
  ASSERT_EQ(invalidCall.msgLen,
            static_cast<char*>(cursor) - frame.data());
  SUdfRequest decodedInvalidCall = {};
  EXPECT_EQ(TSDB_CODE_UDF_INVALID_INPUT,
            decodeUdfRequestBounded(frame.data(), frame.size(), &decodedInvalidCall));
}

TEST(UdfProtocolTest, BoundedResponseDecoderConsumesErrorBody) {
  SUdfResponse response = {};
  response.seqNum = 44;
  response.type = UDF_TASK_SETUP;
  response.code = TSDB_CODE_UDF_FUNC_EXEC_FAILURE;
  response.msgLen = encodeUdfResponse(nullptr, &response);
  ASSERT_GT(response.msgLen, 0);

  std::vector<char> frame(response.msgLen);
  void*             cursor = frame.data();
  ASSERT_EQ(response.msgLen, encodeUdfResponse(&cursor, &response));

  SUdfResponse decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            decodeUdfResponseBounded(frame.data(), frame.size(), &decoded));
  EXPECT_EQ(response.code, decoded.code);
  EXPECT_EQ(TSDB_CODE_MSG_DECODE_ERROR,
            decodeUdfResponseBounded(frame.data(), frame.size() - 1, &decoded));
}

TEST(UdfProtocolTest, BoundedResponseDecoderPreservesCallError) {
  SUdfResponse response = {};
  response.seqNum = 46;
  response.type = UDF_TASK_CALL;
  response.code = TSDB_CODE_UDF_FUNC_EXEC_FAILURE;
  response.callRsp.callType = TSDB_UDF_CALL_SCALA_PROC;
  response.msgLen = encodeUdfResponse(nullptr, &response);
  ASSERT_GT(response.msgLen, 0);

  std::vector<char> frame(response.msgLen);
  void*             cursor = frame.data();
  ASSERT_EQ(response.msgLen, encodeUdfResponse(&cursor, &response));

  SUdfResponse decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            decodeUdfResponseBounded(frame.data(), frame.size(), &decoded));
  EXPECT_EQ(response.code, decoded.code);

  frame.push_back('\0');
  int32_t extendedLength = static_cast<int32_t>(frame.size());
  memcpy(frame.data(), &extendedLength, sizeof(extendedLength));
  EXPECT_EQ(TSDB_CODE_MSG_DECODE_ERROR,
            decodeUdfResponseBounded(frame.data(), frame.size(), &decoded));
}

TEST(GconcatCleanupTest, TracksAllPendingResultRows) {
  struct ResultStorage {
    SResultRowEntryInfo info;
    SGconcatRes         state;
  } first = {}, second = {};
  SqlFunctionCtx context = {};
  first.state.result = static_cast<char*>(taosMemoryMalloc(1));
  second.state.result = static_cast<char*>(taosMemoryMalloc(1));
  ASSERT_NE(first.state.result, nullptr);
  ASSERT_NE(second.state.result, nullptr);
  context.pendingCleanup = 2;
  context.needCleanup = true;

  context.resultInfo = &first.info;
  gconcatFunctionCleanup(&context);
  EXPECT_EQ(context.pendingCleanup, 1);
  EXPECT_TRUE(context.needCleanup);

  context.resultInfo = &second.info;
  gconcatFunctionCleanup(&context);
  gconcatFunctionCleanup(&context);
  EXPECT_EQ(context.pendingCleanup, 0);
  EXPECT_FALSE(context.needCleanup);
}

TEST(UdfProtocolTest, BoundedRequestDecoderValidatesNestedDataBlock) {
  ScopedDataBlock source(nullptr, blockDataDestroy);
  SSDataBlock*    rawBlock = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&rawBlock));
  source.reset(rawBlock);

  SColumnInfoData integer =
      createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 0);
  SColumnInfoData text =
      createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 32, 1);
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            blockDataAppendColInfo(source.get(), &integer));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            blockDataAppendColInfo(source.get(), &text));
  ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataEnsureCapacity(source.get(), 2));

  auto* integerColumn = static_cast<SColumnInfoData*>(
      taosArrayGet(source->pDataBlock, 0));
  auto* textColumn = static_cast<SColumnInfoData*>(
      taosArrayGet(source->pDataBlock, 1));
  ASSERT_NE(nullptr, integerColumn);
  ASSERT_NE(nullptr, textColumn);
  ASSERT_NE(nullptr, integerColumn->pData);
  ASSERT_NE(nullptr, textColumn->varmeta.offset);
  const int32_t integers[] = {10, 20};
  const char*   strings[] = {"one", "two"};
  for (int32_t row = 0; row < 2; ++row) {
    char value[VARSTR_HEADER_SIZE + 3] = {};
    varDataSetLen(value, 3);
    memcpy(varDataVal(value), strings[row], 3);
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              colDataSetVal(integerColumn, row,
                            reinterpret_cast<const char*>(&integers[row]),
                            false));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              colDataSetVal(textColumn, row, value, false));
  }
  ASSERT_NE(nullptr, textColumn->pData);
  source->info.rows = 2;

  SUdfRequest request = {};
  request.seqNum = 45;
  request.type = UDF_TASK_CALL;
  request.call.udfHandle = 1;
  request.call.callType = TSDB_UDF_CALL_SCALA_PROC;
  // The encoder only reads this shallow view; source retains ownership.
  request.call.block = *source;
  request.msgLen = encodeUdfRequest(nullptr, &request);
  ASSERT_GT(request.msgLen, 0);

  std::vector<char> frame(request.msgLen);
  void*             cursor = frame.data();
  ASSERT_GT(encodeUdfRequest(&cursor, &request), 0);
  ASSERT_EQ(request.msgLen,
            static_cast<char*>(cursor) - frame.data());
  request.call.block = {};

  DecodedUdfRequestGuard decoded;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            decodeUdfRequestBounded(frame.data(), frame.size(),
                                    &decoded.value));
  ASSERT_EQ(2, decoded.value.call.block.info.rows);
  ASSERT_EQ(2, taosArrayGetSize(decoded.value.call.block.pDataBlock));
  auto* decodedText = static_cast<SColumnInfoData*>(
      taosArrayGet(decoded.value.call.block.pDataBlock, 1));
  ASSERT_NE(nullptr, decodedText);
  EXPECT_EQ(0, memcmp(varDataVal(colDataGetData(decodedText, 1)), "two", 3));

  request.call.block = *source;
  // Point the second row into the first row's record header;
  // tDecodeDataBlockBounded must reject the invalid record boundary.
  int32_t originalOffset = textColumn->varmeta.offset[1];
  textColumn->varmeta.offset[1] = textColumn->varmeta.offset[0] + 1;
  cursor = frame.data();
  ASSERT_GT(encodeUdfRequest(&cursor, &request), 0);
  request.call.block = {};
  textColumn->varmeta.offset[1] = originalOffset;
  DecodedUdfRequestGuard malformed;
  EXPECT_EQ(TSDB_CODE_MSG_DECODE_ERROR,
            decodeUdfRequestBounded(frame.data(), frame.size(),
                                    &malformed.value));
}

namespace {
struct AggregateWidthCase {
  EFunctionType function = FUNCTION_TYPE_LAST_ROW;
  int8_t tableType = TSDB_NORMAL_TABLE;
  int8_t dataType = TSDB_DATA_TYPE_VARCHAR;
  int32_t plannedBytes = 8;
  int32_t inputBytes = 8;
  int32_t actualBytes = 12;
  int32_t capacity = 8;
  int32_t resultBytes = 0;
  // Key widths include the header; pkSlotBytes overrides the allocated slot.
  int32_t plannedPkBytes = 4;
  int32_t inputPkBytes = 4;
  int32_t actualPkBytes = 12;
  int32_t pkSlotBytes = 0;
  bool hasRef = false;
  bool expressionInput = false;
  bool hasPrimaryKey = false;
  bool replaceExtreme = false;
  bool virtualSource = false;
  bool retainedRateKey = false;
  bool pkPlanPresent = true;
  bool functionNodePresent = true;
};

int32_t runAggregateWidthCase(const AggregateWidthCase& test) {
  SColumnNode column = {};
  column.node.type = QUERY_NODE_COLUMN;
  column.node.resType.type = test.dataType;
  column.node.resType.bytes = test.plannedBytes;
  column.tableType = test.tableType;
  column.colType = test.function == FUNCTION_TYPE_GROUP_KEY ? COLUMN_TYPE_TAG
                                                   : COLUMN_TYPE_COLUMN;
  column.hasRef = test.hasRef;
  column.isVirtualTable = test.virtualSource;
  column.tableId = 123;
  column.colId = 2;
  SNodeList* params = nullptr;
  if (nodesListMakeAppend(&params, (SNode*)&column) != 0) {
    nodesClearList(params);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  std::unique_ptr<SNodeList, decltype(&nodesClearList)> paramsGuard(
      params, nodesClearList);
  if (test.expressionInput) column.node.type = QUERY_NODE_OPERATOR;
  SFunctionNode function = {};
  function.hasPk = test.hasPrimaryKey;
  function.pkBytes = test.pkSlotBytes > 0 ? test.pkSlotBytes
                                        : test.plannedPkBytes;
  SColumnNode keyColumn = column;
  keyColumn.colType = COLUMN_TYPE_COLUMN;
  keyColumn.isPk = true;
  keyColumn.colId = 3;
  keyColumn.node.resType.type = TSDB_DATA_TYPE_VARCHAR;
  keyColumn.node.resType.bytes = test.plannedPkBytes;
  if (test.hasPrimaryKey && test.pkPlanPresent) {
    if (nodesListAppend(params, (SNode*)&keyColumn) != 0) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  function.pParameterList = params;
  tExprNode expr = {};
  expr.nodeType = QUERY_NODE_FUNCTION;
  expr._function.functionType = test.function;
  expr._function.pFunctNode = test.functionNodePresent ? &function : nullptr;
  SExprInfo expression = {};
  expression.pExpr = &expr;
  expression.base.pParamList = params;

  SColumnInfoData input = createColumnInfoData(test.dataType, test.inputBytes,
                                               2);
  SColumnInfoData ts = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 1);
  std::unique_ptr<SColumnInfoData, decltype(&colDataDestroy)> tsGuard(
      &ts, colDataDestroy);
  SColumnInfoData pk = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR,
                                            test.inputPkBytes, 3);
  std::unique_ptr<SColumnInfoData, decltype(&colDataDestroy)> inputGuard(
      &input, colDataDestroy);
  std::unique_ptr<SColumnInfoData, decltype(&colDataDestroy)> pkGuard(
      &pk, colDataDestroy);
  int32_t rows = test.replaceExtreme ? 2 : 1;
  if (colInfoDataEnsureCapacity(&input, rows, true) != 0 ||
      colInfoDataEnsureCapacity(&pk, 1, true) != 0 ||
      colInfoDataEnsureCapacity(&ts, 1, true) != 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int64_t timestamp = 100;
  int32_t timeCode = colDataSetVal(&ts, 0, (const char*)&timestamp, false);
  if (timeCode != 0) return timeCode;
  std::vector<char> value(test.actualBytes, 'x');
  if (IS_STR_DATA_BLOB(test.dataType)) {
    blobDataSetLen(value.data(), test.actualBytes - BLOBSTR_HEADER_SIZE);
  } else if (IS_VAR_DATA_TYPE(test.dataType)) {
    varDataSetLen(value.data(), test.actualBytes - VARSTR_HEADER_SIZE);
  } else {
    double number = 1.0;
    memcpy(value.data(), &number, sizeof(number));
  }
  if (test.dataType == TSDB_DATA_TYPE_NCHAR) {
    int32_t ch = 'x';
    for (int32_t i = VARSTR_HEADER_SIZE; i < test.actualBytes;
         i += sizeof(ch)) {
      memcpy(value.data() + i, &ch, sizeof(ch));
    }
  }
  int32_t code = colDataSetVal(&input, rows - 1, value.data(), false);
  if (code != 0) return code;
  if (test.replaceExtreme) {
    char ch = test.function == FUNCTION_TYPE_MIN ? 'z' : 'a';
    std::vector<char> seed(test.inputBytes, ch);
    varDataSetLen(seed.data(), test.inputBytes - VARSTR_HEADER_SIZE);
    if (test.dataType == TSDB_DATA_TYPE_NCHAR) {
      int32_t wideCh = ch;
      for (int32_t i = VARSTR_HEADER_SIZE; i < test.inputBytes;
           i += sizeof(wideCh)) {
        memcpy(seed.data() + i, &wideCh, sizeof(wideCh));
      }
    }
    code = colDataSetVal(&input, 0, seed.data(), false);
    if (code != 0) return code;
  }
  std::vector<char> key(test.actualPkBytes, 0);
  varDataSetLen(key.data(), test.actualPkBytes - VARSTR_HEADER_SIZE);
  if (test.hasPrimaryKey) {
    code = colDataSetVal(&pk, 0, key.data(), false);
    if (code != 0) return code;
  }

  size_t header = sizeof(SFirstLastRes);
  if (test.function == FUNCTION_TYPE_MIN ||
      test.function == FUNCTION_TYPE_MAX) {
    header = sizeof(SMinmaxResInfo);
  } else if (test.function == FUNCTION_TYPE_MODE) {
    header = sizeof(SModeInfo);
  } else if (test.function == FUNCTION_TYPE_SAMPLE) {
    header = sizeof(SSampleInfo);
  } else if (test.function == FUNCTION_TYPE_TAIL) {
    header = sizeof(STailInfo);
  } else if (test.function == FUNCTION_TYPE_VALUE_CHANGES) {
    header = sizeof(SValueChangesInfo);
  } else if (test.function == FUNCTION_TYPE_GROUP_KEY) {
    header = sizeof(SGroupKeyInfo);
  } else if (test.function == FUNCTION_TYPE_FILL_FORWARD) {
    header = sizeof(SFillforwardInfo);
  } else if (test.function == FUNCTION_TYPE_IRATE) {
    header = sizeof(SRateInfo);
  } else if (test.function != FUNCTION_TYPE_FIRST &&
             test.function != FUNCTION_TYPE_LAST &&
             test.function != FUNCTION_TYPE_LAST_ROW &&
             test.function != FUNCTION_TYPE_CACHE_LAST &&
             test.function != FUNCTION_TYPE_CACHE_LAST_ROW) {
    ADD_FAILURE() << "unsupported width-test function: " << test.function;
    return TSDB_CODE_QRY_INVALID_INPUT;
  }
  const size_t size = sizeof(SResultRowEntryInfo) + header +
                      test.actualBytes + key.size() + test.capacity + 256;
  std::vector<uint64_t> state((size + sizeof(uint64_t) - 1) /
                              sizeof(uint64_t), 0);
  SColumnInfoData* inputs[] = {&input};
  SColumnDataAgg* stats[] = {nullptr};
  SqlFunctionCtx ctx = {};
  SSDataBlock source = {};
  source.info.rows = rows;
  ctx.pSrcBlock = &source;
  ctx.pExpr = &expression;
  ctx.resultInfo = (SResultRowEntryInfo*)state.data();
  ctx.resDataInfo.type = test.dataType;
  ctx.resDataInfo.bytes = test.resultBytes > 0 ? test.resultBytes
                                             : test.plannedBytes;
  ctx.resDataInfo.interBufSize = header + test.capacity;
  ctx.input.pData = inputs;
  ctx.input.pColumnDataAgg = stats;
  ctx.input.numOfRows = rows;
  ctx.input.totalRows = rows;
  ctx.hasPrimaryKey = test.hasPrimaryKey;
  ctx.input.pPrimaryKey = test.hasPrimaryKey ? &pk : nullptr;
  ctx.input.pPTS = test.hasPrimaryKey ? &ts : nullptr;
  SFunctParam arguments[2] = {};
  arguments[1].type = FUNC_PARAM_TYPE_VALUE;
  arguments[1].param.i = 1;
  ctx.param = arguments;
  ctx.numOfParams = 2;
  if (test.function == FUNCTION_TYPE_SAMPLE ||
      test.function == FUNCTION_TYPE_TAIL) {
    ctx.input.pPTS = &ts;
    code = test.function == FUNCTION_TYPE_SAMPLE
               ? sampleFunctionSetup(&ctx, ctx.resultInfo)
               : tailFunctionSetup(&ctx, ctx.resultInfo);
    if (code != 0) return code;
    return test.function == FUNCTION_TYPE_SAMPLE ? sampleFunction(&ctx)
                                        : tailFunction(&ctx);
  }
  if (test.function == FUNCTION_TYPE_VALUE_CHANGES) {
    ctx.input.pPTS = &ts;
    code = valueChangesFunctionSetup(&ctx, ctx.resultInfo);
    return code == 0 ? valueChangesFunction(&ctx) : code;
  }
  if (test.function == FUNCTION_TYPE_GROUP_KEY) {
    code = functionSetup(&ctx, ctx.resultInfo);
    return code == 0 ? groupKeyFunction(&ctx) : code;
  }
  if (test.function == FUNCTION_TYPE_FILL_FORWARD) {
    arguments[1].type = FUNC_PARAM_TYPE_COLUMN;
    ctx.input.pPTS = &ts;
    code = fillforwardFunctionSetup(&ctx, ctx.resultInfo);
    if (code != 0) return code;
    SColumnInfoData output = createColumnInfoData(test.dataType,
                                                  test.plannedBytes, 2);
    std::unique_ptr<SColumnInfoData, decltype(&colDataDestroy)> guard(
        &output, colDataDestroy);
    code = colInfoDataEnsureCapacity(&output, 1, true);
    if (code != 0) return code;
    ctx.pOutput = (char*)&output;
    SArray* contexts = taosArrayInit(1, sizeof(SqlFunctionCtx*));
    if (contexts == nullptr) return TSDB_CODE_OUT_OF_MEMORY;
    std::unique_ptr<SArray, decltype(&taosArrayDestroy)> contextsGuard(
        contexts, taosArrayDestroy);
    SqlFunctionCtx* ptr = &ctx;
    if (taosArrayPush(contexts, &ptr) == nullptr) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    code = fillforwardFunctionByRow(contexts);
    fillforwardFunctionCleanupExt(&ctx);
    return code;
  }
  if (test.function == FUNCTION_TYPE_IRATE) {
    code = irateFuncSetup(&ctx, ctx.resultInfo);
    if (code != 0) return code;
    if (test.retainedRateKey) {
      // Exceed the logical slot, within the oversized backing allocation,
      // to exercise rejection when the retained last key becomes first.
      auto* info = (SRateInfo*)GET_ROWCELL_INTERBUF(ctx.resultInfo);
      info->lastKey = 50;
      memcpy(info->pkData + function.pkBytes, key.data(), key.size());
    }
    return irateFunction(&ctx);
  }
  if (test.function == FUNCTION_TYPE_MIN ||
      test.function == FUNCTION_TYPE_MAX) {
    code = minmaxFunctionSetup(&ctx, ctx.resultInfo);
    if (code != 0) return code;
    code = test.function == FUNCTION_TYPE_MIN ? minFunction(&ctx)
                                              : maxFunction(&ctx);
    auto* result = (SMinmaxResInfo*)GET_ROWCELL_INTERBUF(ctx.resultInfo);
    taosMemoryFree(result->str);
    return code;
  }
  if (test.function == FUNCTION_TYPE_MODE) {
    SDiskbasedBuf* tupleBuf = nullptr;
    code = createDiskbasedBuf(&tupleBuf, 4096, 8192, "agg-width", tsTempDir);
    if (code != 0) return code;
    std::unique_ptr<SDiskbasedBuf, decltype(&destroyDiskbasedBuf)> guard(
        tupleBuf, destroyDiskbasedBuf);
    ctx.saveHandle.pBuf = tupleBuf;
    ctx.saveHandle.currentPage = -1;
    code = modeFunctionSetup(&ctx, ctx.resultInfo);
    if (code == 0) code = modeFunction(&ctx);
    modeFunctionCleanupExt(&ctx);
    return code;
  }
  if (test.function == FUNCTION_TYPE_FIRST) return firstFunction(&ctx);
  if (test.function == FUNCTION_TYPE_LAST ||
      test.function == FUNCTION_TYPE_CACHE_LAST) {
    return lastFunction(&ctx);
  }
  if (test.function == FUNCTION_TYPE_CACHE_LAST_ROW) {
    return cachedLastRowFunction(&ctx);
  }
  return lastRowFunction(&ctx);
}

struct RatePoint {
  TSKEY       key;
  double      value;
  const char* primaryKey;
};

std::vector<char> makeIrateState(int32_t pkBytes, const RatePoint& first,
                                 const RatePoint& last) {
  int32_t resultBytes = getIrateInfoSize(pkBytes);
  std::vector<char> state(VARSTR_HEADER_SIZE + resultBytes, 0);
  varDataSetLen(state.data(), resultBytes);
  auto* info = (SRateInfo*)varDataVal(state.data());
  info->firstKey = first.key;
  info->firstValue = first.value;
  info->lastKey = last.key;
  info->lastValue = last.value;
  info->hasResult = 1;
  info->pkType = TSDB_DATA_TYPE_VARCHAR;
  info->pkBytes = pkBytes;

  int32_t firstLen = (int32_t)strlen(first.primaryKey);
  int32_t lastLen = (int32_t)strlen(last.primaryKey);
  if (firstLen + VARSTR_HEADER_SIZE > pkBytes ||
      lastLen + VARSTR_HEADER_SIZE > pkBytes) {
    return {};
  }
  varDataSetLen(info->pkData, firstLen);
  (void)memcpy(varDataVal(info->pkData), first.primaryKey, firstLen);
  char* lastPk = info->pkData + pkBytes;
  varDataSetLen(lastPk, lastLen);
  (void)memcpy(varDataVal(lastPk), last.primaryKey, lastLen);
  return state;
}

class IrateMergeContext {
 public:
  IrateMergeContext(int32_t plannedPkBytes, int32_t rows,
                    int32_t interBufSize = -1)
      : input_(createColumnInfoData(
            TSDB_DATA_TYPE_BINARY,
            getIrateInfoSize(plannedPkBytes) + VARSTR_HEADER_SIZE, 1)),
        result_((sizeof(SResultRowEntryInfo) +
                 getIrateInfoSize(plannedPkBytes) + sizeof(uint64_t) - 1) /
                    sizeof(uint64_t),
                0) {
    setupCode_ = colInfoDataEnsureCapacity(&input_, rows, true);
    if (setupCode_ != TSDB_CODE_SUCCESS) return;

    function_.hasPk = true;
    function_.pkBytes = plannedPkBytes;
    expr_.nodeType = QUERY_NODE_FUNCTION;
    expr_._function.functionType = FUNCTION_TYPE_IRATE_MERGE;
    expr_._function.pFunctNode = &function_;
    expression_.pExpr = &expr_;
    inputs_[0] = &input_;
    ctx_.pExpr = &expression_;
    ctx_.resultInfo = (SResultRowEntryInfo*)result_.data();
    ctx_.resDataInfo.interBufSize =
        interBufSize >= 0 ? interBufSize : getIrateInfoSize(plannedPkBytes);
    ctx_.input.pData = inputs_;
    ctx_.input.totalRows = rows;
    ctx_.hasPrimaryKey = true;
    ctx_.functionId = FUNCTION_TYPE_IRATE_MERGE;
    setupCode_ = irateFuncSetup(&ctx_, ctx_.resultInfo);
  }

  ~IrateMergeContext() { colDataDestroy(&input_); }

  int32_t setupCode() const { return setupCode_; }

  int32_t setState(int32_t row, const std::vector<char>& state) {
    return state.empty()
               ? TSDB_CODE_QRY_INVALID_INPUT
               : colDataSetVal(&input_, row, state.data(), false);
  }

  int32_t merge(int32_t start, int32_t rows) {
    ctx_.input.startRowIndex = start;
    ctx_.input.numOfRows = rows;
    return irateFunctionMerge(&ctx_);
  }

  const SRateInfo* info() const {
    return (const SRateInfo*)GET_ROWCELL_INTERBUF(ctx_.resultInfo);
  }

  int32_t numOfResults() const { return ctx_.resultInfo->numOfRes; }

 private:
  SFunctionNode         function_ = {};
  tExprNode             expr_ = {};
  SExprInfo             expression_ = {};
  SColumnInfoData       input_;
  SColumnInfoData*      inputs_[1] = {};
  std::vector<uint64_t> result_;
  SqlFunctionCtx        ctx_ = {};
  int32_t               setupCode_ = TSDB_CODE_SUCCESS;
};

void expectRatePrimaryKey(const char* actual, const char* expected) {
  ASSERT_NE(actual, nullptr);
  int32_t expectedLen = (int32_t)strlen(expected);
  ASSERT_EQ(varDataLen(actual), expectedLen);
  EXPECT_EQ(memcmp(varDataVal(actual), expected, expectedLen), 0);
}
}

TEST(FunctionMgtTest, aggregateWidthsDistinguishSchemaFromBuffers) {
  for (auto type : {FUNCTION_TYPE_FIRST, FUNCTION_TYPE_MIN, FUNCTION_TYPE_MAX,
                    FUNCTION_TYPE_MODE, FUNCTION_TYPE_LAST_ROW,
                    FUNCTION_TYPE_LAST, FUNCTION_TYPE_CACHE_LAST_ROW,
                    FUNCTION_TYPE_CACHE_LAST}) {
    AggregateWidthCase base;
    base.function = type;
    bool minMax = type == FUNCTION_TYPE_MIN || type == FUNCTION_TYPE_MAX;
    for (int8_t table : {TSDB_NORMAL_TABLE, TSDB_CHILD_TABLE,
                         TSDB_SUPER_TABLE}) {
      auto test = base;
      test.tableType = table;
      EXPECT_EQ(runAggregateWidthCase(test),
                TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
      test.actualBytes = test.plannedBytes;
      EXPECT_EQ(runAggregateWidthCase(test), 0);
    }
    auto test = base;
    test.tableType = TSDB_VIRTUAL_NORMAL_TABLE;
    EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
    test = base;
    test.tableType = TSDB_SUPER_TABLE;
    test.virtualSource = true;
    EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
    test = base;
    test.hasRef = true;
    EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
    test = base;
    test.expressionInput = true;
    EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
    test = base;
    test.plannedBytes = 16;
    // The value fits the plan but exceeds the MIN/MAX runtime buffer.
    test.inputBytes = minMax ? 8 : 16;
    EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
    if (!minMax) {
      test = base;
      test.plannedBytes = 4;
      test.inputBytes = 4;
      test.dataType = TSDB_DATA_TYPE_BLOB;
      EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
    }
    test = base;
    test.plannedBytes = 6;
    test.inputBytes = 6;
    test.actualBytes = 14;
    test.capacity = 6;
    test.dataType = TSDB_DATA_TYPE_NCHAR;
    EXPECT_EQ(runAggregateWidthCase(test),
              TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
    if (!minMax && type != FUNCTION_TYPE_MODE) {
      test = base;
      test.plannedBytes = 4;
      test.inputBytes = 4;
      test.actualBytes = 4;
      test.hasPrimaryKey = true;
      EXPECT_EQ(runAggregateWidthCase(test),
                TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
    }
  }
}

TEST(FunctionMgtTest, minMaxReplacementWidthsUsePhysicalColumnSchema) {
  for (auto type : {FUNCTION_TYPE_MIN, FUNCTION_TYPE_MAX}) {
    for (int8_t dataType : {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_VARBINARY,
                            TSDB_DATA_TYPE_NCHAR}) {
      AggregateWidthCase test;
      test.function = type;
      test.tableType = TSDB_CHILD_TABLE;
      test.dataType = dataType;
      test.plannedBytes = dataType == TSDB_DATA_TYPE_NCHAR ? 6 : 8;
      test.inputBytes = test.plannedBytes;
      test.capacity = test.plannedBytes;
      test.actualBytes = dataType == TSDB_DATA_TYPE_NCHAR ? 14 : 12;
      EXPECT_EQ(runAggregateWidthCase(test),
                TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
      test.replaceExtreme = true;
      EXPECT_EQ(runAggregateWidthCase(test),
                TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
      test.tableType = TSDB_VIRTUAL_CHILD_TABLE;
      EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
      test.tableType = TSDB_CHILD_TABLE;
      test.actualBytes = test.plannedBytes;
      EXPECT_EQ(runAggregateWidthCase(test), 0);
    }
  }
}

TEST(FunctionMgtTest, virtualColumnSourceSurvivesPlanTransport) {
  SColumnNode column = {};
  column.node.type = QUERY_NODE_COLUMN;
  column.tableType = TSDB_SUPER_TABLE;
  column.isVirtualTable = true;
  for (int transport = 0; transport < 3; ++transport) {
    SNode* restored = nullptr;
    char* data = nullptr;
    int32_t size = 0;
    if (transport == 0) {
      ASSERT_EQ(nodesCloneNode((SNode*)&column, &restored), 0);
    } else {
      int32_t code = transport == 1
                         ? nodesNodeToString((SNode*)&column, false, &data,
                                             &size)
                         : nodesNodeToMsg((SNode*)&column, &data, &size);
      ASSERT_EQ(code, 0);
      auto freeData = [](char* p) { taosMemoryFree(p); };
      std::unique_ptr<char, decltype(freeData)> guard(data, freeData);
      ASSERT_EQ(transport == 1 ? nodesStringToNode(data, &restored)
                               : nodesMsgToNode(data, size, &restored), 0);
    }
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> guard(
        restored, nodesDestroyNode);
    ASSERT_NE(restored, nullptr);
    EXPECT_EQ(((SColumnNode*)restored)->tableType, TSDB_SUPER_TABLE);
    EXPECT_TRUE(((SColumnNode*)restored)->isVirtualTable);
  }
}

TEST(FunctionMgtTest, otherStoredValuesClassifyPhysicalWidthFailures) {
  for (auto type : {FUNCTION_TYPE_SAMPLE, FUNCTION_TYPE_TAIL,
                    FUNCTION_TYPE_VALUE_CHANGES, FUNCTION_TYPE_FILL_FORWARD,
                    FUNCTION_TYPE_GROUP_KEY}) {
    for (int8_t dataType : {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_VARBINARY,
                            TSDB_DATA_TYPE_NCHAR}) {
      AggregateWidthCase base;
      base.function = type;
      base.tableType = TSDB_SUPER_TABLE;
      base.dataType = dataType;
      base.plannedBytes = dataType == TSDB_DATA_TYPE_NCHAR ? 6 : 8;
      base.inputBytes = base.plannedBytes;
      base.capacity = base.plannedBytes;
      base.actualBytes = dataType == TSDB_DATA_TYPE_NCHAR ? 14 : 12;
      for (bool virtualSource : {false, true}) {
        auto test = base;
        test.virtualSource = virtualSource;
        EXPECT_EQ(runAggregateWidthCase(test),
                  virtualSource ? TSDB_CODE_QRY_NOT_ENOUGH_BUFFER
                                : TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
        test.actualBytes = test.plannedBytes;
        EXPECT_EQ(runAggregateWidthCase(test), 0);
      }
    }
    AggregateWidthCase test;
    test.function = type;
    test.expressionInput = true;
    EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
    if (type != FUNCTION_TYPE_SAMPLE && type != FUNCTION_TYPE_TAIL) {
      test.expressionInput = false;
      test.plannedBytes = 16;
      test.inputBytes = type == FUNCTION_TYPE_FILL_FORWARD ? 8 : 16;
      EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
    }
  }
}

TEST(FunctionMgtTest, irateStoredKeysUseTheirOwnPlannedWidth) {
  for (bool retained : {false, true}) {
    for (bool virtualSource : {false, true}) {
      AggregateWidthCase test;
      test.function = FUNCTION_TYPE_IRATE;
      test.tableType = TSDB_CHILD_TABLE;
      test.actualBytes = 8;
      test.dataType = TSDB_DATA_TYPE_DOUBLE;
      test.hasPrimaryKey = true;
      test.virtualSource = virtualSource;
      test.retainedRateKey = retained;
      EXPECT_EQ(runAggregateWidthCase(test),
                virtualSource ? TSDB_CODE_QRY_NOT_ENOUGH_BUFFER
                              : TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
    }
  }
}

TEST(FunctionMgtTest, primaryKeyWidthsUsePlanRatherThanInputBlock) {
  for (auto type : {FUNCTION_TYPE_FIRST, FUNCTION_TYPE_LAST,
                    FUNCTION_TYPE_LAST_ROW, FUNCTION_TYPE_CACHE_LAST,
                    FUNCTION_TYPE_CACHE_LAST_ROW}) {
    AggregateWidthCase test;
    test.function = type;
    test.actualBytes = 8;
    test.hasPrimaryKey = true;
    test.plannedPkBytes = 16;
    EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
    test.plannedPkBytes = 4;
    test.inputPkBytes = 16;
    EXPECT_EQ(runAggregateWidthCase(test),
              TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
    test.inputPkBytes = 4;
    test.pkPlanPresent = false;
    EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
  }
}

TEST(FunctionMgtTest, irateKeySlotsFollowAllocatedPlanWidth) {
  AggregateWidthCase test;
  test.function = FUNCTION_TYPE_IRATE;
  test.dataType = TSDB_DATA_TYPE_DOUBLE;
  test.actualBytes = 8;
  test.hasPrimaryKey = true;
  test.inputPkBytes = 16;
  EXPECT_EQ(runAggregateWidthCase(test),
            TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
  test.inputPkBytes = 4;
  test.plannedPkBytes = 16;
  test.capacity = 32;
  EXPECT_EQ(runAggregateWidthCase(test), 0);
  test.capacity = 8;
  EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
  test.pkSlotBytes = 4;
  EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
}

TEST(FunctionMgtTest, irateMergeKeepsLatestTwoStatesAndPrimaryKeys) {
  constexpr int32_t pkBytes = 16;
  auto first = makeIrateState(pkBytes, {100, 10, "a"},
                              {300, 30, "ccc"});
  auto second = makeIrateState(pkBytes, {200, 20, "bb"},
                               {400, 40, "dddd"});
  ASSERT_FALSE(first.empty());
  ASSERT_FALSE(second.empty());

  IrateMergeContext merge(pkBytes, 2);
  ASSERT_EQ(merge.setupCode(), TSDB_CODE_SUCCESS);
  ASSERT_EQ(merge.setState(0, first), TSDB_CODE_SUCCESS);
  ASSERT_EQ(merge.setState(1, second), TSDB_CODE_SUCCESS);

  ASSERT_EQ(merge.merge(0, 1), TSDB_CODE_SUCCESS);
  const SRateInfo* info = merge.info();
  ASSERT_NE(info, nullptr);
  EXPECT_EQ(info->firstKey, 100);
  EXPECT_EQ(info->firstValue, 10);
  EXPECT_EQ(info->lastKey, 300);
  EXPECT_EQ(info->lastValue, 30);
  expectRatePrimaryKey(info->firstPk, "a");
  expectRatePrimaryKey(info->lastPk, "ccc");

  ASSERT_EQ(merge.merge(1, 1), TSDB_CODE_SUCCESS);
  info = merge.info();
  EXPECT_EQ(info->firstKey, 300);
  EXPECT_EQ(info->firstValue, 30);
  EXPECT_EQ(info->lastKey, 400);
  EXPECT_EQ(info->lastValue, 40);
  expectRatePrimaryKey(info->firstPk, "ccc");
  expectRatePrimaryKey(info->lastPk, "dddd");
  EXPECT_EQ(merge.numOfResults(), 1);
}

TEST(FunctionMgtTest, irateMergeRejectsInvalidStateAndCapacity) {
  constexpr int32_t plannedPkBytes = 16;
  auto valid = makeIrateState(plannedPkBytes, {100, 10, "a"},
                              {200, 20, "bb"});
  ASSERT_FALSE(valid.empty());
  IrateMergeContext shortBuffer(
      plannedPkBytes, 1, getIrateInfoSize(plannedPkBytes) - 1);
  ASSERT_EQ(shortBuffer.setupCode(), TSDB_CODE_SUCCESS);
  ASSERT_EQ(shortBuffer.setState(0, valid), TSDB_CODE_SUCCESS);
  const SRateInfo before = *shortBuffer.info();
  EXPECT_EQ(shortBuffer.merge(0, 1), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
  EXPECT_EQ(shortBuffer.info()->pkBytes, before.pkBytes);
  EXPECT_EQ(shortBuffer.info()->pkType, before.pkType);
  EXPECT_EQ(shortBuffer.info()->firstPk, before.firstPk);
  EXPECT_EQ(shortBuffer.info()->lastPk, before.lastPk);

  auto wrongWidth = makeIrateState(8, {100, 10, "a"},
                                   {200, 20, "bb"});
  ASSERT_FALSE(wrongWidth.empty());
  IrateMergeContext mismatchedState(plannedPkBytes, 1);
  ASSERT_EQ(mismatchedState.setupCode(), TSDB_CODE_SUCCESS);
  ASSERT_EQ(mismatchedState.setState(0, wrongWidth), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mismatchedState.merge(0, 1), TSDB_CODE_QRY_INVALID_INPUT);
}

TEST(FunctionMgtTest, irateCapacityFailurePreservesExistingKeyLayout) {
  const int32_t bytes = sizeof(SResultRowEntryInfo) + getIrateInfoSize(8);
  std::vector<uint64_t> state((bytes + sizeof(uint64_t) - 1) /
                                sizeof(uint64_t), 0);
  SColumnInfoData input = createColumnInfoData(TSDB_DATA_TYPE_DOUBLE, 8, 2);
  SColumnInfoData pk = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 16, 3);
  SColumnInfoData* inputs[] = {&input};
  SSDataBlock source = {};
  SqlFunctionCtx ctx = {};
  ctx.pSrcBlock = &source;
  ctx.input.pData = inputs;
  ctx.input.pPrimaryKey = &pk;
  ctx.hasPrimaryKey = true;
  ctx.resDataInfo.interBufSize = getIrateInfoSize(8);
  ctx.resultInfo = (SResultRowEntryInfo*)state.data();
  ASSERT_EQ(irateFuncSetup(&ctx, ctx.resultInfo), TSDB_CODE_SUCCESS);
  auto* info = (SRateInfo*)GET_ROWCELL_INTERBUF(ctx.resultInfo);
  info->pkBytes = 8;
  info->pkType = TSDB_DATA_TYPE_BIGINT;
  info->firstPk = info->pkData;
  info->lastPk = info->pkData + 8;
  EXPECT_EQ(irateFunction(&ctx), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
  EXPECT_EQ(info->pkBytes, 8);
  EXPECT_EQ(info->pkType, TSDB_DATA_TYPE_BIGINT);
  EXPECT_EQ(info->firstPk, info->pkData);
  EXPECT_EQ(info->lastPk, info->pkData + 8);
}

TEST(FunctionMgtTest, absentContextKeepsBufferError) {
  EXPECT_EQ(getFuncInputBufferError(nullptr, 100, 100),
            TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
}

TEST(FunctionMgtTest, aggregateWidthTypeBoundaries) {
  AggregateWidthCase test;
  test.function = FUNCTION_TYPE_LAST_ROW;
  test.dataType = TSDB_DATA_TYPE_GEOMETRY;
  EXPECT_EQ(runAggregateWidthCase(test),
            TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
  test.virtualSource = true;
  EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);

  test.dataType = TSDB_DATA_TYPE_MEDIUMBLOB;
  test.virtualSource = false;
  EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
  test.virtualSource = true;
  EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
}

TEST(FunctionMgtTest, absentFunctionNodeKeepsResultWidthFallback) {
  AggregateWidthCase test;
  test.function = FUNCTION_TYPE_SAMPLE;
  test.plannedBytes = 16;
  test.inputBytes = 16;
  test.resultBytes = 8;
  test.functionNodePresent = false;
  EXPECT_EQ(runAggregateWidthCase(test), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
}
