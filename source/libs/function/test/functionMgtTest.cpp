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

#include "functionMgt.h"
#include "nodes.h"
#include "osEnv.h"

namespace {

class ScopedTimezoneDisplay {
 public:
  explicit ScopedTimezoneDisplay(const char* display) {
    tstrncpy(original_, tsTimezoneStr, sizeof(original_));
    tstrncpy(tsTimezoneStr, display, TD_TIMEZONE_LEN);
  }

  ~ScopedTimezoneDisplay() { tstrncpy(tsTimezoneStr, original_, TD_TIMEZONE_LEN); }

 private:
  char original_[TD_TIMEZONE_LEN] = {0};
};

SFunctionNode* createFunctionNode(const char* functionName, const int8_t* parameterTypes, int32_t parameterCount) {
  SFunctionNode* pFunc = nullptr;
  if (nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc) != TSDB_CODE_SUCCESS) {
    return nullptr;
  }
  tstrncpy(pFunc->functionName, functionName, TSDB_FUNC_NAME_LEN);

  for (int32_t i = 0; i < parameterCount; ++i) {
    SValueNode* pValue = nullptr;
    if (nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pValue) != TSDB_CODE_SUCCESS) {
      nodesDestroyNode((SNode*)pFunc);
      return nullptr;
    }
    pValue->node.resType.type = parameterTypes[i];
    if (nodesListMakeAppend(&pFunc->pParameterList, (SNode*)pValue) != TSDB_CODE_SUCCESS) {
      nodesDestroyNode((SNode*)pValue);
      nodesDestroyNode((SNode*)pFunc);
      return nullptr;
    }
  }

  return pFunc;
}

/* timetruncate(ts, 1d): the unit parameter must be a duration value node,
 * otherwise validateParam() rejects the call before timezone injection. */
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
    SFunctionNode*        pFunc = createFunctionNode("to_char", parameterTypes, 2);
    ASSERT_NE(nullptr, pFunc);

    char message[128] = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS, fmGetFuncInfo(pFunc, message, sizeof(message)));
    ASSERT_EQ(3, LIST_LENGTH(pFunc->pParameterList));

    SValueNode* pTimezone = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2);
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
    SFunctionNode*        pFunc = createFunctionNode("to_iso8601", parameterTypes, 1);
    ASSERT_NE(nullptr, pFunc);

    char message[128] = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS, fmGetFuncInfo(pFunc, message, sizeof(message)));
    ASSERT_EQ(2, LIST_LENGTH(pFunc->pParameterList));

    SValueNode* pTimezone = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
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
    SFunctionNode*        pFunc = createTimeTruncateNode();
    ASSERT_NE(nullptr, pFunc);

    char message[128] = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS, fmGetFuncInfo(pFunc, message, sizeof(message)));
    /* ts, unit, use_curr_tz, precision, tz_name, fdow, unitCh */
    ASSERT_EQ(7, LIST_LENGTH(pFunc->pParameterList));

    SValueNode* pTimezone = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 4);
    ASSERT_NE(nullptr, pTimezone);
    EXPECT_STREQ(testCase.expected, pTimezone->literal);

    nodesDestroyNode((SNode*)pFunc);
  }
}

/* No "(ABBR, ±HHMM)" suffix: offset extraction fails, so the leading token is
 * injected verbatim and no sign flipping happens. */
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
    SFunctionNode*        pFunc = createFunctionNode("to_char", parameterTypes, 2);
    ASSERT_NE(nullptr, pFunc);

    char message[128] = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS, fmGetFuncInfo(pFunc, message, sizeof(message)));
    ASSERT_EQ(3, LIST_LENGTH(pFunc->pParameterList));

    SValueNode* pTimezone = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2);
    ASSERT_NE(nullptr, pTimezone);
    EXPECT_STREQ(testCase.expected, pTimezone->literal);

    nodesDestroyNode((SNode*)pFunc);
  }
}
