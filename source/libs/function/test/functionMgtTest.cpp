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

#include "functionMgt.h"
#include "nodes.h"
#include "osEnv.h"
#ifndef _WIN32
#include "stub.h"
#endif

namespace {

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

}  // namespace

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
                                      "lag",        "lead", "first_value", "last_value",   "nth_value"};

  for (const char* func : orderRequiredFuncs) {
    EXPECT_TRUE(fmIsSqlWindowOrderRequiredFunc(func)) << func;
  }
  EXPECT_FALSE(fmIsSqlWindowOrderRequiredFunc("sum"));
  EXPECT_FALSE(fmIsSqlWindowOrderRequiredFunc("not_a_function"));
}

TEST(FunctionMgtTest, identifiesAggregatesUsableAsSqlWindowAggregates) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());

  const char* windowAggFuncs[] = {"count", "sum", "min", "max", "avg", "percentile", "first", "last", "last_row"};

  for (const char* func : windowAggFuncs) {
    EXPECT_TRUE(fmCanUseAsSqlWindowAgg(func)) << func;
  }
  EXPECT_FALSE(fmCanUseAsSqlWindowAgg("row_number"));
  EXPECT_FALSE(fmCanUseAsSqlWindowAgg("not_a_function"));
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
