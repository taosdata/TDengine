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

#include <memory>

#include "builtinsimpl.h"
#include "tdatablock.h"

namespace {

class FillForwardTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_EQ(fmFuncMgtInit(), TSDB_CODE_SUCCESS); }
  void TearDown() override { fmFuncMgtDestroy(); }

  template <typename T>
  void checkIntegerWidth(int8_t type, T first, T last) {
    SFuncExecEnv env = {};
    ASSERT_TRUE(getFillforwardFuncEnv(nullptr, &env));
    std::unique_ptr<void, void (*)(void*)> result(taosMemoryCalloc(1, sizeof(SResultRowEntryInfo) + env.calcMemSize),
                                                  [](void* p) { taosMemoryFree(p); });
    ASSERT_NE(result.get(), nullptr);
    std::unique_ptr<SArray, decltype(&taosArrayDestroy)> contexts(taosArrayInit(1, sizeof(SqlFunctionCtx*)),
                                                                  taosArrayDestroy);
    ASSERT_NE(contexts.get(), nullptr);

    T               inputValues[] = {0, first, 0, last};
    char            inputNulls = 0;
    SColumnInfoData input = {};
    input.info.type = type;
    input.info.bytes = sizeof(T);
    input.pData = reinterpret_cast<char*>(inputValues);
    input.nullbitmap = &inputNulls;
    colDataSetNULL(&input, 0);
    colDataSetNULL(&input, 2);
    SColumnInfoData* inputs[] = {&input};
    SSDataBlock      block = {};
    block.info.rows = 4;
    block.info.id.groupId = 1;

    // Check untouched bytes explicitly: an unaligned wide store can escape ASAN.
    alignas(int64_t) char outputValues[6 * sizeof(T) + sizeof(int64_t)];
    memset(outputValues, 0x5a, sizeof(outputValues));
    char            outputNulls = 0;
    SColumnInfoData output = {};
    output.info = input.info;
    output.pData = outputValues;
    output.nullbitmap = &outputNulls;

    SqlFunctionCtx ctx = {};
    ctx.functionId = fmGetFuncId("fill_forward");
    ctx.resDataInfo.type = type;
    ctx.resDataInfo.bytes = sizeof(T);
    ctx.resDataInfo.interBufSize = env.calcMemSize;
    ctx.resultInfo = static_cast<SResultRowEntryInfo*>(result.get());
    ASSERT_EQ(fillforwardFunctionSetup(&ctx, ctx.resultInfo), TSDB_CODE_SUCCESS);
    ctx.pOutput = reinterpret_cast<char*>(&output);
    ctx.pSrcBlock = &block;
    ctx.input.pData = inputs;
    ctx.input.numOfRows = 4;
    ctx.offset = 1;
    SqlFunctionCtx* pCtx = &ctx;
    ASSERT_NE(taosArrayPush(contexts.get(), &pCtx), nullptr);
    ASSERT_EQ(fillforwardFunctionByRow(contexts.get()), TSDB_CODE_SUCCESS);
    EXPECT_EQ(ctx.resultInfo->numOfRes, 4);
    EXPECT_TRUE(colDataIsNull_s(&output, 1));
    const T expected[] = {first, first, last};
    for (int32_t i = 0; i < 3; ++i) {
      T actual = 0;
      memcpy(&actual, colDataGetData(&output, i + 2), sizeof(T));
      EXPECT_FALSE(colDataIsNull_s(&output, i + 2));
      EXPECT_EQ(actual, expected[i]);
    }
    for (size_t i = 0; i < sizeof(T); ++i) EXPECT_EQ(outputValues[i], 0x5a);
    for (size_t i = 5 * sizeof(T); i < sizeof(outputValues); ++i) EXPECT_EQ(outputValues[i], 0x5a);

    // A later input batch containing only NULL must reuse the last value.
    ctx.input.startRowIndex = 2;
    ctx.input.numOfRows = 1;
    ctx.offset = 5;
    ASSERT_EQ(fillforwardFunctionByRow(contexts.get()), TSDB_CODE_SUCCESS);
    EXPECT_EQ(ctx.resultInfo->numOfRes, 1);
    T actual = 0;
    memcpy(&actual, colDataGetData(&output, 5), sizeof(T));
    EXPECT_FALSE(colDataIsNull_s(&output, 5));
    EXPECT_EQ(actual, last);
    for (size_t i = 6 * sizeof(T); i < sizeof(outputValues); ++i) EXPECT_EQ(outputValues[i], 0x5a);
  }
};

TEST_F(FillForwardTest, BoolPreservesOutputBounds) { checkIntegerWidth<bool>(TSDB_DATA_TYPE_BOOL, false, true); }

TEST_F(FillForwardTest, TinyintPreservesOutputBounds) {
  checkIntegerWidth<int8_t>(TSDB_DATA_TYPE_TINYINT, INT8_MIN, INT8_MAX);
}

TEST_F(FillForwardTest, SmallintPreservesOutputBounds) {
  checkIntegerWidth<int16_t>(TSDB_DATA_TYPE_SMALLINT, INT16_MIN, INT16_MAX);
}

TEST_F(FillForwardTest, IntPreservesOutputBounds) {
  checkIntegerWidth<int32_t>(TSDB_DATA_TYPE_INT, INT32_MIN, INT32_MAX);
}

TEST_F(FillForwardTest, BigintPreservesOutputBounds) {
  checkIntegerWidth<int64_t>(TSDB_DATA_TYPE_BIGINT, INT64_MIN, INT64_MAX);
}

TEST_F(FillForwardTest, UnsignedTinyintPreservesOutputBounds) {
  checkIntegerWidth<uint8_t>(TSDB_DATA_TYPE_UTINYINT, 0, UINT8_MAX);
}

TEST_F(FillForwardTest, UnsignedSmallintPreservesOutputBounds) {
  checkIntegerWidth<uint16_t>(TSDB_DATA_TYPE_USMALLINT, 0, UINT16_MAX);
}

TEST_F(FillForwardTest, UnsignedIntPreservesOutputBounds) {
  checkIntegerWidth<uint32_t>(TSDB_DATA_TYPE_UINT, 0, UINT32_MAX);
}

TEST_F(FillForwardTest, UnsignedBigintPreservesOutputBounds) {
  checkIntegerWidth<uint64_t>(TSDB_DATA_TYPE_UBIGINT, 0, UINT64_MAX);
}

TEST_F(FillForwardTest, TimestampPreservesOutputBounds) {
  checkIntegerWidth<int64_t>(TSDB_DATA_TYPE_TIMESTAMP, INT64_MIN, INT64_MAX);
}

}  // namespace
