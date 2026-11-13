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
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>

#include <memory>

#include "builtinsimpl.h"
#include "tdatablock.h"

namespace {

// fill_next keeps unresolved output positions in the host result block and
// resolves them when a later non-NULL value arrives.  These tests drive the
// row processor exactly like projectApplyFunctions does: an accumulated
// result block whose rows survive across input blocks, with ctx->offset set
// to the row index where the current input block starts.
class FillNextTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_EQ(fmFuncMgtInit(), TSDB_CODE_SUCCESS); }
  void TearDown() override { fmFuncMgtDestroy(); }

  struct CtxHolder {
    std::unique_ptr<void, void (*)(void*)> result;
    SqlFunctionCtx                         ctx;
    SColumnInfoData*                       inputs[1];
  };

  static CtxHolder makeCtx(int8_t type, int32_t bytes, SColumnInfoData* output, SSDataBlock* block) {
    SFuncExecEnv env = {};
    EXPECT_TRUE(getFillnextFuncEnv(nullptr, &env));
    CtxHolder holder{
        std::unique_ptr<void, void (*)(void*)>(
            taosMemoryCalloc(1, sizeof(SResultRowEntryInfo) + env.calcMemSize), [](void* p) { taosMemoryFree(p); }),
        {}};
    EXPECT_NE(holder.result.get(), nullptr);
    holder.ctx.functionId = fmGetFuncId("fill_next");
    holder.ctx.resDataInfo.type = type;
    holder.ctx.resDataInfo.bytes = bytes;
    holder.ctx.resDataInfo.interBufSize = env.calcMemSize;
    holder.ctx.resultInfo = static_cast<SResultRowEntryInfo*>(holder.result.get());
    EXPECT_EQ(fillnextFunctionSetup(&holder.ctx, holder.ctx.resultInfo), TSDB_CODE_SUCCESS);
    holder.ctx.pOutput = reinterpret_cast<char*>(output);
    holder.ctx.pSrcBlock = block;
    return holder;
  }

  static void setInput(CtxHolder& holder, SColumnInfoData* input, int32_t rows, int32_t startRow, int32_t offset) {
    holder.inputs[0] = input;
    holder.ctx.input.pData = holder.inputs;
    holder.ctx.input.startRowIndex = startRow;
    holder.ctx.input.numOfRows = rows;
    holder.ctx.offset = offset;
  }

  static int32_t readInt(SColumnInfoData* col, int32_t row) {
    int32_t v = 0;
    memcpy(&v, colDataGetData(col, row), sizeof(v));
    return v;
  }

  static void expectInt(SColumnInfoData* col, int32_t row, bool isNull, int32_t expected) {
    EXPECT_EQ(colDataIsNull_s(col, row), isNull) << "row " << row;
    if (!isNull) {
      EXPECT_EQ(readInt(col, row), expected) << "row " << row;
    }
  }
};

TEST_F(FillNextTest, ResolvesPendingAcrossBlocks) {
  // Block 1: [NULL, 5, NULL, NULL] -> row0 resolves to 5, rows 2/3 stay pending.
  int32_t         in1[] = {0, 5, 0, 0};
  char            in1Nulls = 0;
  SColumnInfoData input1 = {};
  input1.info.type = TSDB_DATA_TYPE_INT;
  input1.info.bytes = sizeof(int32_t);
  input1.pData = reinterpret_cast<char*>(in1);
  input1.nullbitmap = &in1Nulls;
  colDataSetNULL(&input1, 0);
  colDataSetNULL(&input1, 2);
  colDataSetNULL(&input1, 3);

  // Accumulated result block: 6 rows total across two input blocks.
  int32_t         outVals[6] = {0};
  char            outNulls[2] = {0};
  SColumnInfoData output = {};
  output.info.type = TSDB_DATA_TYPE_INT;
  output.info.bytes = sizeof(int32_t);
  output.pData = reinterpret_cast<char*>(outVals);
  output.nullbitmap = outNulls;

  SSDataBlock block = {};
  block.info.rows = 4;
  block.info.id.groupId = 1;

  CtxHolder holder = makeCtx(TSDB_DATA_TYPE_INT, sizeof(int32_t), &output, &block);
  setInput(holder, &input1, 4, 0, 0);

  std::unique_ptr<SArray, decltype(&taosArrayDestroy)> contexts(taosArrayInit(1, sizeof(SqlFunctionCtx*)),
                                                                taosArrayDestroy);
  SqlFunctionCtx* pCtx = &holder.ctx;
  ASSERT_NE(taosArrayPush(contexts.get(), &pCtx), nullptr);
  ASSERT_EQ(fillnextFunctionByRow(contexts.get()), TSDB_CODE_SUCCESS);

  expectInt(&output, 0, false, 5);
  expectInt(&output, 1, false, 5);
  expectInt(&output, 2, true, 0);
  expectInt(&output, 3, true, 0);
  EXPECT_TRUE(fillnextHasPending(&holder.ctx));

  // Block 2: [NULL, 7] starts at result row 4.  The NULL row becomes pending,
  // then 7 resolves rows 2, 3 and 4; row 5 outputs 7.
  int32_t in2[] = {0, 7};
  char    in2Nulls = 0;
  SColumnInfoData input2 = {};
  input2.info = input1.info;
  input2.pData = reinterpret_cast<char*>(in2);
  input2.nullbitmap = &in2Nulls;
  colDataSetNULL(&input2, 0);

  block.info.rows = 2;
  setInput(holder, &input2, 2, 0, 4);
  ASSERT_EQ(fillnextFunctionByRow(contexts.get()), TSDB_CODE_SUCCESS);

  for (int32_t i = 0; i < 5; ++i) expectInt(&output, i, false, i < 2 ? 5 : 7);
  expectInt(&output, 5, false, 7);
  EXPECT_FALSE(fillnextHasPending(&holder.ctx));
}

TEST_F(FillNextTest, TrailingNullsStayPendingUntilCleanup) {
  int32_t         in[] = {1, 0, 0};
  char            nulls = 0;
  SColumnInfoData input = {};
  input.info.type = TSDB_DATA_TYPE_INT;
  input.info.bytes = sizeof(int32_t);
  input.pData = reinterpret_cast<char*>(in);
  input.nullbitmap = &nulls;
  colDataSetNULL(&input, 1);
  colDataSetNULL(&input, 2);

  int32_t         outVals[3] = {0};
  char            outNulls = 0;
  SColumnInfoData output = {};
  output.info = input.info;
  output.pData = reinterpret_cast<char*>(outVals);
  output.nullbitmap = &outNulls;

  SSDataBlock block = {};
  block.info.rows = 3;
  block.info.id.groupId = 1;

  CtxHolder holder = makeCtx(TSDB_DATA_TYPE_INT, sizeof(int32_t), &output, &block);
  setInput(holder, &input, 3, 0, 0);

  std::unique_ptr<SArray, decltype(&taosArrayDestroy)> contexts(taosArrayInit(1, sizeof(SqlFunctionCtx*)),
                                                                taosArrayDestroy);
  SqlFunctionCtx* pCtx = &holder.ctx;
  ASSERT_NE(taosArrayPush(contexts.get(), &pCtx), nullptr);
  ASSERT_EQ(fillnextFunctionByRow(contexts.get()), TSDB_CODE_SUCCESS);

  expectInt(&output, 0, false, 1);
  expectInt(&output, 1, true, 0);
  expectInt(&output, 2, true, 0);
  EXPECT_TRUE(fillnextHasPending(&holder.ctx));

  // Series end: unresolved rows stay NULL and cleanup releases the queue.
  fillnextFunctionCleanupExt(&holder.ctx);
  EXPECT_FALSE(fillnextHasPending(&holder.ctx));
  holder.ctx.resultInfo->initialized = false;
  EXPECT_EQ(fillnextFunctionSetup(&holder.ctx, holder.ctx.resultInfo), TSDB_CODE_SUCCESS);
}

TEST_F(FillNextTest, AllNullColumnStaysNull) {
  int32_t         in[] = {0, 0, 0};
  char            nulls = 0;
  SColumnInfoData input = {};
  input.info.type = TSDB_DATA_TYPE_INT;
  input.info.bytes = sizeof(int32_t);
  input.pData = reinterpret_cast<char*>(in);
  input.nullbitmap = &nulls;
  for (int32_t i = 0; i < 3; ++i) colDataSetNULL(&input, i);

  int32_t         outVals[3] = {0};
  char            outNulls = 0;
  SColumnInfoData output = {};
  output.info = input.info;
  output.pData = reinterpret_cast<char*>(outVals);
  output.nullbitmap = &outNulls;

  SSDataBlock block = {};
  block.info.rows = 3;
  block.info.id.groupId = 1;

  CtxHolder holder = makeCtx(TSDB_DATA_TYPE_INT, sizeof(int32_t), &output, &block);
  setInput(holder, &input, 3, 0, 0);

  std::unique_ptr<SArray, decltype(&taosArrayDestroy)> contexts(taosArrayInit(1, sizeof(SqlFunctionCtx*)),
                                                                taosArrayDestroy);
  SqlFunctionCtx* pCtx = &holder.ctx;
  ASSERT_NE(taosArrayPush(contexts.get(), &pCtx), nullptr);
  ASSERT_EQ(fillnextFunctionByRow(contexts.get()), TSDB_CODE_SUCCESS);

  for (int32_t i = 0; i < 3; ++i) expectInt(&output, i, true, 0);
  EXPECT_TRUE(fillnextHasPending(&holder.ctx));
}

TEST_F(FillNextTest, MultipleColumnsKeepIndependentPending) {
  int32_t         inA[] = {0, 1, 0, 0};
  int32_t         inB[] = {0, 0, 2, 0};
  char            nullA = 0;
  char            nullB = 0;
  SColumnInfoData inputA = {};
  inputA.info.type = TSDB_DATA_TYPE_INT;
  inputA.info.bytes = sizeof(int32_t);
  inputA.pData = reinterpret_cast<char*>(inA);
  inputA.nullbitmap = &nullA;
  colDataSetNULL(&inputA, 0);
  colDataSetNULL(&inputA, 2);
  colDataSetNULL(&inputA, 3);
  SColumnInfoData inputB = inputA;
  inputB.pData = reinterpret_cast<char*>(inB);
  inputB.nullbitmap = &nullB;
  colDataSetNULL(&inputB, 0);
  colDataSetNULL(&inputB, 1);
  colDataSetNULL(&inputB, 3);

  int32_t         outA[4] = {0};
  int32_t         outB[4] = {0};
  char            outNullA = 0;
  char            outNullB = 0;
  SColumnInfoData outputA = {};
  outputA.info = inputA.info;
  outputA.pData = reinterpret_cast<char*>(outA);
  outputA.nullbitmap = &outNullA;
  SColumnInfoData outputB = outputA;
  outputB.pData = reinterpret_cast<char*>(outB);
  outputB.nullbitmap = &outNullB;

  SSDataBlock block = {};
  block.info.rows = 4;
  block.info.id.groupId = 1;

  CtxHolder ha = makeCtx(TSDB_DATA_TYPE_INT, sizeof(int32_t), &outputA, &block);
  CtxHolder hb = makeCtx(TSDB_DATA_TYPE_INT, sizeof(int32_t), &outputB, &block);
  setInput(ha, &inputA, 4, 0, 0);
  setInput(hb, &inputB, 4, 0, 0);

  std::unique_ptr<SArray, decltype(&taosArrayDestroy)> contexts(taosArrayInit(2, sizeof(SqlFunctionCtx*)),
                                                                taosArrayDestroy);
  SqlFunctionCtx* pA = &ha.ctx;
  SqlFunctionCtx* pB = &hb.ctx;
  ASSERT_NE(taosArrayPush(contexts.get(), &pA), nullptr);
  ASSERT_NE(taosArrayPush(contexts.get(), &pB), nullptr);
  ASSERT_EQ(fillnextFunctionByRow(contexts.get()), TSDB_CODE_SUCCESS);

  expectInt(&outputA, 0, false, 1);
  expectInt(&outputA, 1, false, 1);
  expectInt(&outputA, 2, true, 0);
  expectInt(&outputA, 3, true, 0);
  expectInt(&outputB, 0, false, 2);
  expectInt(&outputB, 1, false, 2);
  expectInt(&outputB, 2, false, 2);
  expectInt(&outputB, 3, true, 0);
  EXPECT_TRUE(fillnextHasPending(&ha.ctx));
  EXPECT_TRUE(fillnextHasPending(&hb.ctx));
}

TEST_F(FillNextTest, VarDataValuesAreCopied) {
  const int32_t hdr = (int32_t)VARSTR_HEADER_SIZE;
  auto put = [&](char* dst, const char* value) {
    varDataSetLen(dst, static_cast<VarDataLenT>(strlen(value)));
    memcpy(varDataVal(dst), value, strlen(value));
  };
  alignas(8) char inputData[32] = {0};
  put(inputData, "aa");
  put(inputData + hdr + 2, "bbbb");
  int32_t         inputOffsets[4] = {0, 0, 0, hdr + 2};
  char            inputNulls = 0;
  SColumnInfoData input = {};
  input.info.type = TSDB_DATA_TYPE_VARCHAR;
  input.info.bytes = 16;
  input.pData = inputData;
  input.nullbitmap = &inputNulls;
  input.varmeta.offset = inputOffsets;
  colDataSetNULL(&input, 0);
  colDataSetNULL(&input, 2);

  alignas(8) char outData[128];
  memset(outData, 0x5a, sizeof(outData));
  int32_t         outOffsets[4] = {0};
  char            outNulls = 0;
  SColumnInfoData output = {};
  output.info = input.info;
  output.pData = outData;
  output.nullbitmap = &outNulls;
  output.varmeta.offset = outOffsets;
  output.varmeta.allocLen = sizeof(outData);

  SSDataBlock block = {};
  block.info.rows = 4;
  block.info.id.groupId = 1;

  CtxHolder holder = makeCtx(TSDB_DATA_TYPE_VARCHAR, 16, &output, &block);
  setInput(holder, &input, 4, 0, 0);

  std::unique_ptr<SArray, decltype(&taosArrayDestroy)> contexts(taosArrayInit(1, sizeof(SqlFunctionCtx*)),
                                                                taosArrayDestroy);
  SqlFunctionCtx* pCtx = &holder.ctx;
  ASSERT_NE(taosArrayPush(contexts.get(), &pCtx), nullptr);
  ASSERT_EQ(fillnextFunctionByRow(contexts.get()), TSDB_CODE_SUCCESS);

  const char* expected[] = {"aa", "aa", "bbbb", "bbbb"};
  for (int32_t i = 0; i < 4; ++i) {
    ASSERT_FALSE(colDataIsNull_s(&output, i));
    char*   v = colDataGetData(&output, i);
    int32_t len = (int32_t)varDataLen(v);
    EXPECT_EQ(len, (int32_t)strlen(expected[i]));
    EXPECT_EQ(0, memcmp(varDataVal(v), expected[i], len));
  }
  EXPECT_FALSE(fillnextHasPending(&holder.ctx));
}

}  // namespace
