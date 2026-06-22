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
