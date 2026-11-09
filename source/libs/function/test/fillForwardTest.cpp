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

  // Var-length columns: BLOB-family values carry a 4-byte length header, the
  // other var-data types a 2-byte header.  The filler must copy the whole
  // value image and re-emit it, including GEOMETRY and BLOB.
  void checkVarData(int8_t type, const char* first, const char* last) {
    SFuncExecEnv env = {};
    ASSERT_TRUE(getFillforwardFuncEnv(nullptr, &env));
    std::unique_ptr<void, void (*)(void*)> result(taosMemoryCalloc(1, sizeof(SResultRowEntryInfo) + env.calcMemSize),
                                                  [](void* p) { taosMemoryFree(p); });
    ASSERT_NE(result.get(), nullptr);
    std::unique_ptr<SArray, decltype(&taosArrayDestroy)> contexts(taosArrayInit(1, sizeof(SqlFunctionCtx*)),
                                                                  taosArrayDestroy);
    ASSERT_NE(contexts.get(), nullptr);

    const bool    isBlob = IS_STR_DATA_BLOB(type);
    const int32_t hdr = isBlob ? (int32_t)BLOBSTR_HEADER_SIZE : (int32_t)VARSTR_HEADER_SIZE;
    const int32_t firstLen = hdr + (int32_t)strlen(first);
    const int32_t lastLen = hdr + (int32_t)strlen(last);
    ASSERT_LE(firstLen + lastLen, 64);

    alignas(8) char inputData[64] = {0};
    auto put = [&](char* dst, const char* value) {
      if (isBlob) {
        blobDataSetLen(dst, static_cast<BlobDataLenT>(strlen(value)));
        memcpy(blobDataVal(dst), value, strlen(value));
      } else {
        varDataSetLen(dst, static_cast<VarDataLenT>(strlen(value)));
        memcpy(varDataVal(dst), value, strlen(value));
      }
    };
    put(inputData, first);
    put(inputData + firstLen, last);

    int32_t         inputOffsets[4] = {0, 0, 0, firstLen};
    char            inputNulls = 0;
    SColumnInfoData input = {};
    input.info.type = type;
    input.info.bytes = 16;
    input.pData = inputData;
    input.nullbitmap = &inputNulls;
    input.varmeta.offset = inputOffsets;
    colDataSetNULL(&input, 0);
    colDataSetNULL(&input, 2);

    SColumnInfoData* inputs[] = {&input};
    SSDataBlock      block = {};
    block.info.rows = 4;
    block.info.id.groupId = 1;

    alignas(8) char outputData[128];
    memset(outputData, 0x5a, sizeof(outputData));
    int32_t         outputOffsets[8] = {0};
    char            outputNulls = 0;
    SColumnInfoData output = {};
    output.info.type = type;
    output.info.bytes = 16;
    output.pData = outputData;
    output.nullbitmap = &outputNulls;
    output.varmeta.offset = outputOffsets;
    output.varmeta.allocLen = sizeof(outputData);

    SqlFunctionCtx ctx = {};
    ctx.functionId = fmGetFuncId("fill_forward");
    ctx.resDataInfo.type = type;
    ctx.resDataInfo.bytes = 16;
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

    const char* expectedValues[] = {first, first, last};
    for (int32_t i = 0; i < 3; ++i) {
      EXPECT_FALSE(colDataIsNull_s(&output, i + 2));
      char*   value = colDataGetData(&output, i + 2);
      int32_t valueLen = isBlob ? (int32_t)blobDataLen(value) : (int32_t)varDataLen(value);
      char*   valueData = isBlob ? blobDataVal(value) : varDataVal(value);
      EXPECT_EQ(valueLen, (int32_t)strlen(expectedValues[i]));
      EXPECT_EQ(0, memcmp(valueData, expectedValues[i], valueLen));
    }
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

TEST_F(FillForwardTest, FloatPreservesOutputBounds) {
  checkIntegerWidth<float>(TSDB_DATA_TYPE_FLOAT, -1.5f, 2.5f);
}

TEST_F(FillForwardTest, DoublePreservesOutputBounds) {
  checkIntegerWidth<double>(TSDB_DATA_TYPE_DOUBLE, -1.5, 2.5);
}

TEST_F(FillForwardTest, VarcharPreservesValues) { checkVarData(TSDB_DATA_TYPE_VARCHAR, "aa", "bbbb"); }

TEST_F(FillForwardTest, NcharPreservesValues) { checkVarData(TSDB_DATA_TYPE_NCHAR, "aa", "bbbb"); }

TEST_F(FillForwardTest, VarbinaryPreservesValues) { checkVarData(TSDB_DATA_TYPE_VARBINARY, "aa", "bbbb"); }

TEST_F(FillForwardTest, GeometryPreservesValues) { checkVarData(TSDB_DATA_TYPE_GEOMETRY, "aa", "bbbb"); }

TEST_F(FillForwardTest, BlobPreservesValues) { checkVarData(TSDB_DATA_TYPE_BLOB, "aa", "bbbb"); }

TEST_F(FillForwardTest, MediumBlobPreservesValues) { checkVarData(TSDB_DATA_TYPE_MEDIUMBLOB, "aa", "bbbb"); }

}  // namespace
