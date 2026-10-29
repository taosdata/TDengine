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

#include <cstring>
#include <memory>
#include <vector>

#include "builtins.h"
#include "builtinsimpl.h"
#include "functionMgt.h"
#include "functionMgtInt.h"
#include "functionResInfoInt.h"
#include "nodes.h"
#include "osEnv.h"
#include "tdatablock.h"
#include "tudf.h"
#include "tudfInt.h"
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

TEST(FunctionMgtTest, keepsIsoTimezoneOffsetForToIso8601Fallback) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  const int8_t parameterTypes[] = {TSDB_DATA_TYPE_TIMESTAMP};
  struct {
    const char* display;
    const char* expected;
  } testCases[] = {
      {"System (UTC, +0800)", "+0800"},
      {"System (UTC, -0500)", "-0500"},
      {"System (UTC, +0000)", "+0000"},
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
