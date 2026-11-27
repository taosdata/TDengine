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

#include "gtest/gtest.h"

#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "os.h"
#include "tdatablock.h"
#include "tpagedbuf.h"
#include "windowfunc.h"

extern "C" int32_t winInputPoolTestGetRegisteredPageCount(const SWindowInputPool *pPool);
extern "C" int32_t winInputStoreTestGetPageId(const SWindowInputStore *pStore, int32_t pageIndex, int32_t *pPageId);
extern "C" void    winInputStoreTestFailNextDirectoryCommit(SWindowInputStore *pStore);
extern "C" int32_t winFuncTestSameOrderKeys(SWindowInputStore *pStore,
                                            const SNodeList *pOrderKeys,
                                            int32_t lhs, int32_t rhs,
                                            bool *pSame);

namespace {

SNode *makeIntValue(SValueNode *pValue, int64_t value) {
  memset(pValue, 0, sizeof(*pValue));
  pValue->node.type = QUERY_NODE_VALUE;
  pValue->datum.i = value;
  return reinterpret_cast<SNode *>(pValue);
}

SNode *makeNullValue(SValueNode *pValue) {
  memset(pValue, 0, sizeof(*pValue));
  pValue->node.type = QUERY_NODE_VALUE;
  pValue->isNull = true;
  return reinterpret_cast<SNode *>(pValue);
}

SSDataBlock *createIntBlock(int32_t rows, int32_t startValue) {
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);

  SColumnInfoData col = {0};
  col.hasNull = true;
  col.info.type = TSDB_DATA_TYPE_INT;
  col.info.bytes = sizeof(int32_t);
  EXPECT_EQ(blockDataAppendColInfo(pBlock, &col), TSDB_CODE_SUCCESS);
  EXPECT_EQ(blockDataEnsureCapacity(pBlock, rows), TSDB_CODE_SUCCESS);

  SColumnInfoData *pCol = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  EXPECT_NE(pCol, nullptr);
  for (int32_t i = 0; i < rows; ++i) {
    int32_t value = startValue + i;
    EXPECT_EQ(colDataSetVal(pCol, i, reinterpret_cast<const char *>(&value), false), TSDB_CODE_SUCCESS);
  }
  pBlock->info.rows = rows;
  return pBlock;
}

}  // namespace

TEST(windowFuncFrameTest, rowsPrecedingToCurrentRowClipsAtPartitionStart) {
  SValueNode       offset;
  SWindowFrameNode frame;
  memset(&frame, 0, sizeof(frame));
  frame.frameUnit = WINDOW_FRAME_UNIT_ROWS;
  frame.start.boundType = WINDOW_BOUND_N_PRECEDING;
  frame.start.pOffset = makeIntValue(&offset, 2);
  frame.end.boundType = WINDOW_BOUND_CURRENT_ROW;

  SSqlWindowFrameRange range = {0};
  ASSERT_EQ(winCalcRowsFrame(1, 10, &frame, &range), TSDB_CODE_SUCCESS);
  EXPECT_EQ(range.start, 0);
  EXPECT_EQ(range.end, 1);
}

TEST(windowFuncFrameTest, rowsCurrentRowToFollowingClipsAtPartitionEnd) {
  SValueNode       offset;
  SWindowFrameNode frame;
  memset(&frame, 0, sizeof(frame));
  frame.frameUnit = WINDOW_FRAME_UNIT_ROWS;
  frame.start.boundType = WINDOW_BOUND_CURRENT_ROW;
  frame.end.boundType = WINDOW_BOUND_N_FOLLOWING;
  frame.end.pOffset = makeIntValue(&offset, 5);

  SSqlWindowFrameRange range = {0};
  ASSERT_EQ(winCalcRowsFrame(8, 10, &frame, &range), TSDB_CODE_SUCCESS);
  EXPECT_EQ(range.start, 8);
  EXPECT_EQ(range.end, 9);
}

TEST(windowFuncFrameTest, rowsFrameRejectsZeroPartitionRows) {
  SWindowFrameNode frame;
  memset(&frame, 0, sizeof(frame));
  frame.frameUnit = WINDOW_FRAME_UNIT_ROWS;
  frame.start.boundType = WINDOW_BOUND_CURRENT_ROW;
  frame.end.boundType = WINDOW_BOUND_CURRENT_ROW;

  SSqlWindowFrameRange range = {0};
  EXPECT_EQ(winCalcRowsFrame(0, 0, &frame, &range), TSDB_CODE_INVALID_PARA);
}

TEST(windowFuncFrameTest, rowsFrameRejectsPrecedingWithoutOffset) {
  SWindowFrameNode frame;
  memset(&frame, 0, sizeof(frame));
  frame.frameUnit = WINDOW_FRAME_UNIT_ROWS;
  frame.start.boundType = WINDOW_BOUND_N_PRECEDING;
  frame.end.boundType = WINDOW_BOUND_CURRENT_ROW;

  SSqlWindowFrameRange range = {0};
  EXPECT_EQ(winCalcRowsFrame(0, 10, &frame, &range), TSDB_CODE_INVALID_PARA);
}

TEST(windowFuncFrameTest, rowsFrameRejectsFollowingWithNegativeOffset) {
  SValueNode       offset;
  SWindowFrameNode frame;
  memset(&frame, 0, sizeof(frame));
  frame.frameUnit = WINDOW_FRAME_UNIT_ROWS;
  frame.start.boundType = WINDOW_BOUND_CURRENT_ROW;
  frame.end.boundType = WINDOW_BOUND_N_FOLLOWING;
  frame.end.pOffset = makeIntValue(&offset, -1);

  SSqlWindowFrameRange range = {0};
  EXPECT_EQ(winCalcRowsFrame(0, 10, &frame, &range), TSDB_CODE_INVALID_PARA);
}

TEST(windowFuncFrameTest, rowsFrameRejectsPrecedingWithNonValueOffset) {
  SColumnNode      offset;
  SWindowFrameNode frame;
  memset(&offset, 0, sizeof(offset));
  offset.node.type = QUERY_NODE_COLUMN;
  memset(&frame, 0, sizeof(frame));
  frame.frameUnit = WINDOW_FRAME_UNIT_ROWS;
  frame.start.boundType = WINDOW_BOUND_N_PRECEDING;
  frame.start.pOffset = reinterpret_cast<SNode *>(&offset);
  frame.end.boundType = WINDOW_BOUND_CURRENT_ROW;

  SSqlWindowFrameRange range = {0};
  EXPECT_EQ(winCalcRowsFrame(0, 10, &frame, &range), TSDB_CODE_INVALID_PARA);
}

TEST(windowFuncFrameTest, rowsFrameRejectsPrecedingWithNullValueOffset) {
  SValueNode       offset;
  SWindowFrameNode frame;
  memset(&frame, 0, sizeof(frame));
  frame.frameUnit = WINDOW_FRAME_UNIT_ROWS;
  frame.start.boundType = WINDOW_BOUND_N_PRECEDING;
  frame.start.pOffset = makeNullValue(&offset);
  frame.end.boundType = WINDOW_BOUND_CURRENT_ROW;

  SSqlWindowFrameRange range = {0};
  EXPECT_EQ(winCalcRowsFrame(0, 10, &frame, &range), TSDB_CODE_INVALID_PARA);
}

TEST(windowFuncFrameTest, rangeCurrentRowIncludesPeers) {
  const int64_t values[] = {10, 20, 20, 30};

  SSqlWindowFrameRange range = {0};
  ASSERT_EQ(winCalcRangeFrameForInt64(values, 4, 1, 0, 0, &range), TSDB_CODE_SUCCESS);
  EXPECT_EQ(range.start, 1);
  EXPECT_EQ(range.end, 2);
}

TEST(windowFuncFrameTest, rangePrecedingUsesOrderValueDistance) {
  const int64_t values[] = {10, 15, 20, 30};

  SSqlWindowFrameRange range = {0};
  ASSERT_EQ(winCalcRangeFrameForInt64(values, 4, 2, 5, 0, &range), TSDB_CODE_SUCCESS);
  EXPECT_EQ(range.start, 1);
  EXPECT_EQ(range.end, 2);
}

TEST(windowFuncFrameTest, rangeFrameClampsAtInt64Limits) {
  const int64_t values[] = {INT64_MIN, INT64_MIN + 1, 0, INT64_MAX - 1, INT64_MAX};

  SSqlWindowFrameRange range = {0};
  ASSERT_EQ(winCalcRangeFrameForInt64(values, 5, 0, INT64_MAX, 0, &range), TSDB_CODE_SUCCESS);
  EXPECT_EQ(range.start, 0);
  EXPECT_EQ(range.end, 0);

  ASSERT_EQ(winCalcRangeFrameForInt64(values, 5, 4, 0, INT64_MAX, &range), TSDB_CODE_SUCCESS);
  EXPECT_EQ(range.start, 4);
  EXPECT_EQ(range.end, 4);
}

TEST(windowFuncFrameTest, rangeFrameForDoubleSkipsNanValues) {
  const double values[] = {std::nan(""), 10.0, 15.0, 20.0};

  SSqlWindowFrameRange range = {0};
  ASSERT_EQ(winCalcRangeFrameForDouble(values, 4, 2, 5.0, 0.0, &range), TSDB_CODE_SUCCESS);
  EXPECT_EQ(range.start, 1);
  EXPECT_EQ(range.end, 2);
}

TEST(windowFuncFrameTest, rangeFrameForDoubleNanCurrentOnlyIncludesNanPeers) {
  const double values[] = {std::nan(""), std::nan(""), 10.0, 20.0};

  SSqlWindowFrameRange range = {0};
  ASSERT_EQ(winCalcRangeFrameForDouble(values, 4, 0, 5.0, 0.0, &range), TSDB_CODE_SUCCESS);
  EXPECT_EQ(range.start, 0);
  EXPECT_EQ(range.end, 1);
}

TEST(windowFuncFrameTest, rangeFrameForDoubleKeepsInfinitePeersFinite) {
  const double values[] = {10.0, 20.0, std::numeric_limits<double>::infinity()};

  SSqlWindowFrameRange range = {0};
  ASSERT_EQ(winCalcRangeFrameForDouble(values, 3, 2, 5.0, 0.0, &range), TSDB_CODE_SUCCESS);
  EXPECT_EQ(range.start, 2);
  EXPECT_EQ(range.end, 2);
}

TEST(windowFuncFrameTest, rankValueRejectsInvalidRowIndex) {
  int64_t rank = 0;

  EXPECT_EQ(winCalcRankValue(-1, 0, 1, &rank), TSDB_CODE_INVALID_PARA);
}

TEST(windowFuncFrameTest, rankValueRejectsInvalidDenseRank) {
  int64_t rank = 0;

  EXPECT_EQ(winCalcRankValue(0, 0, 0, &rank), TSDB_CODE_INVALID_PARA);
}

TEST(windowFuncFrameTest, rankValueRejectsPeerStartAfterRowIndex) {
  int64_t rank = 0;

  EXPECT_EQ(winCalcRankValue(1, 2, 1, &rank), TSDB_CODE_INVALID_PARA);
}

TEST(windowFuncFrameTest, percentRankReturnsZeroForSingleRowPartition) {
  double value = 1.0;

  ASSERT_EQ(winCalcPercentRank(1, 1, &value), TSDB_CODE_SUCCESS);
  EXPECT_DOUBLE_EQ(value, 0.0);
}

TEST(windowFuncFrameTest, percentRankRejectsRankAfterPartitionRows) {
  double value = 0.0;

  EXPECT_EQ(winCalcPercentRank(11, 10, &value), TSDB_CODE_INVALID_PARA);
}

TEST(windowFuncFrameTest, cumeDistUsesPeerEndInclusivePosition) {
  double value = 0.0;

  ASSERT_EQ(winCalcCumeDist(2, 4, &value), TSDB_CODE_SUCCESS);
  EXPECT_DOUBLE_EQ(value, 0.75);
}

TEST(windowFuncFrameTest, dedicatedFallbackOnlySkipsSqlWindowAggregates) {
  EXPECT_EQ(winFuncCheckDedicatedFallback("sum"), TSDB_CODE_SUCCESS);
  EXPECT_EQ(winFuncCheckDedicatedFallback("fill_forward"), TSDB_CODE_INVALID_PARA);
}

TEST(windowFuncFrameTest, outputBatchEndCapsAtCapacity) {
  int64_t endRow = 0;

  ASSERT_EQ(winCalcOutputBatchEnd(10, 0, 4, &endRow), TSDB_CODE_SUCCESS);
  EXPECT_EQ(endRow, 4);

  ASSERT_EQ(winCalcOutputBatchEnd(10, 8, 4, &endRow), TSDB_CODE_SUCCESS);
  EXPECT_EQ(endRow, 10);

  EXPECT_EQ(winCalcOutputBatchEnd(10, 0, 0, &endRow), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(winCalcOutputBatchEnd(10, 10, 4, &endRow), TSDB_CODE_INVALID_PARA);
}

TEST(windowFuncFrameTest, inputStoreSpillsPagedBlocksAndReadsRowsBack) {
  tstrncpy(tsTempDir, "/tmp", PATH_MAX);
  tsTempSpace.size.avail = 1024 * 1024;

  SSDataBlock *pInput = createIntBlock(64, 1000);
  ASSERT_NE(pInput, nullptr);

  SWindowInputPool *pPool = nullptr;
  ASSERT_EQ(winInputPoolCreate(128, 128, "windowInputStoreTest", &pPool), TSDB_CODE_SUCCESS);
  ASSERT_NE(pPool, nullptr);

  SWindowInputStore *pStore = nullptr;
  ASSERT_EQ(winInputStoreCreate(pInput, pPool, &pStore), TSDB_CODE_SUCCESS);
  ASSERT_NE(pStore, nullptr);

  ASSERT_EQ(winInputStoreAppendBlock(pStore, pInput), TSDB_CODE_SUCCESS);
  EXPECT_EQ(winInputStoreGetRows(pStore), 64);
  EXPECT_GT(winInputStoreGetPageCount(pStore), 1);

  SDiskbasedBufStatis statis = winInputPoolGetStatis(pPool);
  EXPECT_GT(statis.flushPages, 0);

  SSDataBlock *pFirstPage = nullptr;
  ASSERT_EQ(winInputStoreGetBlock(pStore, 0, &pFirstPage), TSDB_CODE_SUCCESS);
  ASSERT_NE(pFirstPage, nullptr);
  ASSERT_GT(pFirstPage->info.rows, 0);
  SColumnInfoData *pFirstCol = static_cast<SColumnInfoData *>(taosArrayGet(pFirstPage->pDataBlock, 0));
  ASSERT_NE(pFirstCol, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pFirstCol, 0)), 1000);

  int32_t      pageCount = winInputStoreGetPageCount(pStore);
  SSDataBlock *pLastPage = nullptr;
  ASSERT_EQ(winInputStoreGetBlock(pStore, pageCount - 1, &pLastPage), TSDB_CODE_SUCCESS);
  ASSERT_NE(pLastPage, nullptr);
  ASSERT_GT(pLastPage->info.rows, 0);
  SColumnInfoData *pLastCol = static_cast<SColumnInfoData *>(taosArrayGet(pLastPage->pDataBlock, 0));
  ASSERT_NE(pLastCol, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLastCol, pLastPage->info.rows - 1)), 1063);

  winInputStoreDestroy(pStore);
  winInputPoolDestroy(pPool);
  blockDataDestroy(pInput);
}

TEST(windowFuncFrameTest, inputStoresSharePoolAndKeepPagesIsolated) {
  tstrncpy(tsTempDir, "/tmp", PATH_MAX);
  tsTempSpace.size.avail = 1024 * 1024;

  SSDataBlock *pFirstInput = createIntBlock(64, 1000);
  SSDataBlock *pSecondInput = createIntBlock(64, 2000);
  ASSERT_NE(pFirstInput, nullptr);
  ASSERT_NE(pSecondInput, nullptr);

  SWindowInputPool *pPool = nullptr;
  ASSERT_EQ(winInputPoolCreate(128, 128, "sharedWindowInputPoolTest", &pPool), TSDB_CODE_SUCCESS);
  ASSERT_NE(pPool, nullptr);

  SWindowInputStore *pFirstStore = nullptr;
  SWindowInputStore *pSecondStore = nullptr;
  ASSERT_EQ(winInputStoreCreate(pFirstInput, pPool, &pFirstStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreCreate(pSecondInput, pPool, &pSecondStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreAppendBlock(pFirstStore, pFirstInput), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreAppendBlock(pSecondStore, pSecondInput), TSDB_CODE_SUCCESS);

  EXPECT_GT(winInputPoolGetStatis(pPool).flushPages, 0);
  EXPECT_EQ(winInputPoolGetPageSize(pPool), 128);

  SSDataBlock *pFirstReplay = nullptr;
  SSDataBlock *pSecondReplay = nullptr;
  ASSERT_EQ(winInputStoreGetBlock(pFirstStore, 0, &pFirstReplay), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreGetBlock(pSecondStore, 0, &pSecondReplay), TSDB_CODE_SUCCESS);
  auto *pFirstCol = static_cast<SColumnInfoData *>(taosArrayGet(pFirstReplay->pDataBlock, 0));
  auto *pSecondCol = static_cast<SColumnInfoData *>(taosArrayGet(pSecondReplay->pDataBlock, 0));
  ASSERT_NE(pFirstCol, nullptr);
  ASSERT_NE(pSecondCol, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pFirstCol, 0)), 1000);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pSecondCol, 0)), 2000);

  winInputStoreDestroy(pFirstStore);
  pFirstStore = nullptr;
  SWindowInputStore *pReplacementStore = nullptr;
  ASSERT_EQ(winInputStoreCreate(pFirstInput, pPool, &pReplacementStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreAppendBlock(pReplacementStore, pFirstInput), TSDB_CODE_SUCCESS);
  SSDataBlock *pReplacementReplay = nullptr;
  ASSERT_EQ(winInputStoreGetBlock(pReplacementStore, 0, &pReplacementReplay), TSDB_CODE_SUCCESS);
  auto *pReplacementCol = static_cast<SColumnInfoData *>(taosArrayGet(pReplacementReplay->pDataBlock, 0));
  ASSERT_NE(pReplacementCol, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pReplacementCol, 0)), 1000);

  winInputStoreDestroy(pReplacementStore);
  winInputStoreDestroy(pSecondStore);
  winInputPoolDestroy(pPool);
  blockDataDestroy(pFirstInput);
  blockDataDestroy(pSecondInput);
}

TEST(windowFuncFrameTest, destroyedBorrowerPagesAreReusedWithoutLoadingSiblingPages) {
  tstrncpy(tsTempDir, "/tmp", PATH_MAX);
  tsTempSpace.size.avail = 1024 * 1024;

  SSDataBlock *pFirstInput = createIntBlock(64, 1000);
  SSDataBlock *pSiblingInput = createIntBlock(64, 2000);
  ASSERT_NE(pFirstInput, nullptr);
  ASSERT_NE(pSiblingInput, nullptr);

  SWindowInputPool *pPool = nullptr;
  ASSERT_EQ(winInputPoolCreate(128, 128, "windowInputBorrowerChurnTest", &pPool), TSDB_CODE_SUCCESS);
  ASSERT_NE(pPool, nullptr);

  SWindowInputStore *pFirstStore = nullptr;
  SWindowInputStore *pSiblingStore = nullptr;
  ASSERT_EQ(winInputStoreCreate(pFirstInput, pPool, &pFirstStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreCreate(pSiblingInput, pPool, &pSiblingStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreAppendBlock(pFirstStore, pFirstInput), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreAppendBlock(pSiblingStore, pSiblingInput), TSDB_CODE_SUCCESS);

  std::vector<int32_t> firstPageIds;
  for (int32_t page = 0; page < winInputStoreGetPageCount(pFirstStore); ++page) {
    int32_t pageId = -1;
    ASSERT_EQ(winInputStoreTestGetPageId(pFirstStore, page, &pageId), TSDB_CODE_SUCCESS);
    firstPageIds.push_back(pageId);
  }
  ASSERT_GT(firstPageIds.size(), 2);
  int32_t registeredBeforeDestroy = winInputPoolTestGetRegisteredPageCount(pPool);
  int32_t loadsBeforeDestroy = winInputPoolGetStatis(pPool).loadPages;

  winInputStoreDestroy(pFirstStore);
  pFirstStore = nullptr;
  EXPECT_EQ(winInputPoolGetStatis(pPool).loadPages, loadsBeforeDestroy);

  SWindowInputStore *pReplacementStore = nullptr;
  ASSERT_EQ(winInputStoreCreate(pFirstInput, pPool, &pReplacementStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreAppendBlock(pReplacementStore, pFirstInput), TSDB_CODE_SUCCESS);
  EXPECT_EQ(winInputPoolTestGetRegisteredPageCount(pPool), registeredBeforeDestroy);
  ASSERT_EQ(winInputStoreGetPageCount(pReplacementStore), firstPageIds.size());
  for (int32_t page = 0; page < winInputStoreGetPageCount(pReplacementStore); ++page) {
    int32_t pageId = -1;
    ASSERT_EQ(winInputStoreTestGetPageId(pReplacementStore, page, &pageId), TSDB_CODE_SUCCESS);
    EXPECT_EQ(pageId, firstPageIds[page]);
  }

  SSDataBlock *pSiblingReplay = nullptr;
  ASSERT_EQ(winInputStoreGetBlock(pSiblingStore, 0, &pSiblingReplay), TSDB_CODE_SUCCESS);
  ASSERT_NE(pSiblingReplay, nullptr);
  auto *pSiblingCol = static_cast<SColumnInfoData *>(taosArrayGet(pSiblingReplay->pDataBlock, 0));
  ASSERT_NE(pSiblingCol, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pSiblingCol, 0)), 2000);

  winInputStoreDestroy(pReplacementStore);
  winInputStoreDestroy(pSiblingStore);
  winInputPoolDestroy(pPool);
  blockDataDestroy(pFirstInput);
  blockDataDestroy(pSiblingInput);
}

TEST(windowFuncFrameTest, failedDirectoryCommitPageIsReusedAndSiblingRemainsReadable) {
  tstrncpy(tsTempDir, "/tmp", PATH_MAX);
  tsTempSpace.size.avail = 1024 * 1024;

  SSDataBlock *pSiblingInput = createIntBlock(1, 2000);
  SSDataBlock *pFailedInput = createIntBlock(1, 3000);
  ASSERT_NE(pSiblingInput, nullptr);
  ASSERT_NE(pFailedInput, nullptr);

  SWindowInputPool *pPool = nullptr;
  ASSERT_EQ(winInputPoolCreate(128, 128, "windowInputAppendRollbackTest", &pPool), TSDB_CODE_SUCCESS);

  SWindowInputStore *pSiblingStore = nullptr;
  SWindowInputStore *pFailedStore = nullptr;
  ASSERT_EQ(winInputStoreCreate(pSiblingInput, pPool, &pSiblingStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreCreate(pFailedInput, pPool, &pFailedStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreAppendBlock(pSiblingStore, pSiblingInput), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreGetPageCount(pSiblingStore), 1);
  int32_t registeredBeforeFailure = winInputPoolTestGetRegisteredPageCount(pPool);

  winInputStoreTestFailNextDirectoryCommit(pFailedStore);
  EXPECT_EQ(winInputStoreAppendBlock(pFailedStore, pFailedInput), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(winInputStoreGetPageCount(pFailedStore), 0);
  int32_t registeredAfterFailure = winInputPoolTestGetRegisteredPageCount(pPool);
  EXPECT_EQ(registeredAfterFailure, registeredBeforeFailure + 1);
  winInputStoreDestroy(pFailedStore);

  SWindowInputStore *pReplacementStore = nullptr;
  ASSERT_EQ(winInputStoreCreate(pFailedInput, pPool, &pReplacementStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreAppendBlock(pReplacementStore, pFailedInput), TSDB_CODE_SUCCESS);
  EXPECT_EQ(winInputPoolTestGetRegisteredPageCount(pPool), registeredAfterFailure);
  ASSERT_EQ(winInputStoreGetPageCount(pReplacementStore), 1);
  int32_t replacementPageId = -1;
  ASSERT_EQ(winInputStoreTestGetPageId(pReplacementStore, 0, &replacementPageId), TSDB_CODE_SUCCESS);
  EXPECT_EQ(replacementPageId, registeredBeforeFailure);

  SSDataBlock *pSiblingReplay = nullptr;
  ASSERT_EQ(winInputStoreGetBlock(pSiblingStore, 0, &pSiblingReplay), TSDB_CODE_SUCCESS);
  ASSERT_NE(pSiblingReplay, nullptr);
  auto *pSiblingCol = static_cast<SColumnInfoData *>(taosArrayGet(pSiblingReplay->pDataBlock, 0));
  ASSERT_NE(pSiblingCol, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pSiblingCol, 0)), 2000);

  winInputStoreDestroy(pReplacementStore);
  winInputStoreDestroy(pSiblingStore);
  winInputPoolDestroy(pPool);
  blockDataDestroy(pSiblingInput);
  blockDataDestroy(pFailedInput);
}

TEST(windowFuncFrameTest, inputStoreRestoresNullsWhenReusingPageBlocks) {
  tstrncpy(tsTempDir, testing::TempDir().c_str(), PATH_MAX);
  tsTempSpace.size.avail = 1024 * 1024;
  SSDataBlock* pInput = createIntBlock(64, 1000);
  ASSERT_NE(pInput, nullptr);
  auto* pCol = static_cast<SColumnInfoData*>(
      taosArrayGet(pInput->pDataBlock, 0));
  for (int32_t row = 0; row < 64; row += 3) colDataSetNULL(pCol, row);

  SWindowInputPool* pPool = nullptr;
  ASSERT_EQ(winInputPoolCreate(128, 128, "nullPageReuse", &pPool),
            TSDB_CODE_SUCCESS);
  SWindowInputStore* pStore = nullptr;
  ASSERT_EQ(winInputStoreCreate(pInput, pPool, &pStore),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(winInputStoreAppendBlock(pStore, pInput), TSDB_CODE_SUCCESS);
  EXPECT_GT(winInputPoolGetStatis(pPool).flushPages, 0);
  int pages = winInputStoreGetPageCount(pStore);
  ASSERT_GT(pages, 1);
  for (bool reverse : {false, true}) {
    int start = reverse ? 64 : 0;
    for (int i = 0; i < pages; ++i) {
      int page = reverse ? pages - 1 - i : i;
      SSDataBlock* pBlock = nullptr;
      ASSERT_EQ(winInputStoreGetBlock(pStore, page, &pBlock),
                TSDB_CODE_SUCCESS);
      ASSERT_NE(pBlock, nullptr);
      if (reverse) start -= pBlock->info.rows;
      auto* pRead = static_cast<SColumnInfoData*>(
          taosArrayGet(pBlock->pDataBlock, 0));
      for (int local = 0; local < pBlock->info.rows; ++local) {
        int row = start + local;
        ASSERT_EQ(colDataIsNull_s(pRead, local), row % 3 == 0);
        if (row % 3 != 0) {
          const char* value = colDataGetData(pRead, local);
          EXPECT_EQ(*reinterpret_cast<const int32_t*>(value), 1000 + row);
        }
      }
      if (!reverse) start += pBlock->info.rows;
    }
    EXPECT_EQ(start, reverse ? 0 : 64);
  }
  EXPECT_GT(winInputPoolGetStatis(pPool).loadPages, 0);
  winInputStoreDestroy(pStore);
  winInputPoolDestroy(pPool);
  blockDataDestroy(pInput);
}

TEST(windowFuncFrameTest, peerComparisonKeepsBothPageCachesIndependent) {
  tstrncpy(tsTempDir, testing::TempDir().c_str(), PATH_MAX);
  tsTempSpace.size.avail = 1024 * 1024;
  SColumnNode column = {};
  column.node.type = QUERY_NODE_COLUMN;
  column.slotId = 0;
  SOrderByExprNode order = {};
  order.type = QUERY_NODE_ORDER_BY_EXPR;
  order.pExpr = reinterpret_cast<SNode*>(&column);
  order.order = ORDER_ASC;
  SNodeList* keys = nullptr;
  ASSERT_EQ(nodesListMakeAppend(&keys, reinterpret_cast<SNode*>(&order)),
            TSDB_CODE_SUCCESS);
  std::unique_ptr<SNodeList, decltype(&nodesClearList)> keyList(
      keys, nodesClearList);

  for (int type : {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_VARCHAR}) {
    SCOPED_TRACE(type);
    SSDataBlock* rawInput = nullptr;
    ASSERT_EQ(createDataBlock(&rawInput), TSDB_CODE_SUCCESS);
    std::unique_ptr<SSDataBlock, decltype(&blockDataDestroy)> input(
        rawInput, blockDataDestroy);
    int bytes = type == TSDB_DATA_TYPE_INT ? sizeof(int32_t) :
                                           VARSTR_HEADER_SIZE + 16;
    auto info = createColumnInfoData(type, bytes, 0);
    ASSERT_EQ(blockDataAppendColInfo(input.get(), &info), TSDB_CODE_SUCCESS);
    ASSERT_EQ(blockDataEnsureCapacity(input.get(), 64), TSDB_CODE_SUCCESS);
    auto* data = static_cast<SColumnInfoData*>(
        taosArrayGet(input->pDataBlock, 0));
    for (int row = 0; row < 64; ++row) {
      int32_t value = row % 7;
      char buffer[VARSTR_HEADER_SIZE + 16] = {};
      std::string text = std::to_string(value);
      varDataSetLen(buffer, text.size());
      memcpy(varDataVal(buffer), text.data(), text.size());
      const char* encoded = type == TSDB_DATA_TYPE_INT ?
          reinterpret_cast<const char*>(&value) : buffer;
      ASSERT_EQ(colDataSetVal(data, row, encoded, row % 4 == 0),
                TSDB_CODE_SUCCESS);
    }
    input->info.rows = 64;

    SWindowInputPool* rawPool = nullptr;
    ASSERT_EQ(winInputPoolCreate(128, 128, "peerPageReuse", &rawPool),
              TSDB_CODE_SUCCESS);
    std::unique_ptr<SWindowInputPool, decltype(&winInputPoolDestroy)> pool(
        rawPool, winInputPoolDestroy);
    SWindowInputStore* rawStore = nullptr;
    ASSERT_EQ(winInputStoreCreate(input.get(), pool.get(), &rawStore),
              TSDB_CODE_SUCCESS);
    std::unique_ptr<SWindowInputStore, decltype(&winInputStoreDestroy)> store(
        rawStore, winInputStoreDestroy);
    ASSERT_EQ(winInputStoreAppendBlock(store.get(), input.get()),
              TSDB_CODE_SUCCESS);
    ASSERT_GT(winInputStoreGetPageCount(store.get()), 1);
    EXPECT_GT(winInputPoolGetStatis(pool.get()).flushPages, 0);

    // Pin one row while the other cache crosses pages in both directions.
    for (bool reverse : {false, true}) {
      for (int anchor : {0, 1, 32, 63}) {
        for (int i = 0; i < 64; ++i) {
          int row = reverse ? 63 - i : i;
          bool expected = anchor % 4 == 0 && row % 4 == 0;
          if (anchor % 4 != 0 && row % 4 != 0) {
            expected = anchor % 7 == row % 7;
          }
          for (bool swap : {false, true}) {
            bool same = false;
            ASSERT_EQ(winFuncTestSameOrderKeys(store.get(), keys,
                                               swap ? row : anchor,
                                               swap ? anchor : row, &same),
                      TSDB_CODE_SUCCESS);
            ASSERT_EQ(same, expected) << anchor << ":" << row << ":" << swap;
          }
        }
      }
    }
    EXPECT_GT(winInputPoolGetStatis(pool.get()).loadPages, 0);
  }
}

TEST(windowFuncFrameTest, inputStoreRestoresVarNullsWhenReusingPageBlocks) {
  tstrncpy(tsTempDir, testing::TempDir().c_str(), PATH_MAX);
  tsTempSpace.size.avail = 1024 * 1024;
  SSDataBlock* pInput = nullptr;
  ASSERT_EQ(createDataBlock(&pInput), TSDB_CODE_SUCCESS);
  std::unique_ptr<SSDataBlock, decltype(&blockDataDestroy)> input(
      pInput, blockDataDestroy);
  auto col = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR,
                                 VARSTR_HEADER_SIZE + 16, 0);
  ASSERT_EQ(blockDataAppendColInfo(pInput, &col), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pInput, 64), TSDB_CODE_SUCCESS);
  auto* pCol = static_cast<SColumnInfoData*>(
      taosArrayGet(pInput->pDataBlock, 0));
  ASSERT_NE(pCol, nullptr);
  for (int32_t row = 0; row < 64; ++row) {
    std::string value = row % 3 == 1 ? "" : "row-" + std::to_string(row);
    char buffer[VARSTR_HEADER_SIZE + 16] = {};
    varDataSetLen(buffer, value.size());
    memcpy(varDataVal(buffer), value.data(), value.size());
    ASSERT_EQ(colDataSetVal(pCol, row, buffer, row % 3 == 0),
              TSDB_CODE_SUCCESS);
  }
  pInput->info.rows = 64;

  SWindowInputPool* pPool = nullptr;
  ASSERT_EQ(winInputPoolCreate(128, 128, "varNullPageReuse", &pPool),
            TSDB_CODE_SUCCESS);
  std::unique_ptr<SWindowInputPool, decltype(&winInputPoolDestroy)> pool(
      pPool, winInputPoolDestroy);
  SWindowInputStore* pStore = nullptr;
  ASSERT_EQ(winInputStoreCreate(pInput, pPool, &pStore),
            TSDB_CODE_SUCCESS);
  std::unique_ptr<SWindowInputStore, decltype(&winInputStoreDestroy)> store(
      pStore, winInputStoreDestroy);
  ASSERT_EQ(winInputStoreAppendBlock(pStore, pInput), TSDB_CODE_SUCCESS);
  ASSERT_GT(winInputStoreGetPageCount(pStore), 1);
  EXPECT_GT(winInputPoolGetStatis(pPool).flushPages, 0);

  // Cross-page cache reuse must restore hasNull after block cleanup.
  int pages = winInputStoreGetPageCount(pStore);
  for (bool reverse : {false, true}) {
    int start = reverse ? 64 : 0;
    for (int i = 0; i < pages; ++i) {
      int page = reverse ? pages - 1 - i : i;
      SSDataBlock* pBlock = nullptr;
      ASSERT_EQ(winInputStoreGetBlock(pStore, page, &pBlock),
                TSDB_CODE_SUCCESS);
      ASSERT_NE(pBlock, nullptr);
      if (reverse) start -= pBlock->info.rows;
      auto* pRead = static_cast<SColumnInfoData*>(
          taosArrayGet(pBlock->pDataBlock, 0));
      ASSERT_NE(pRead, nullptr);
      ASSERT_NE(pRead->varmeta.offset, nullptr);
      for (int local = 0; local < pBlock->info.rows; ++local) {
        int row = start + local;
        SCOPED_TRACE(std::to_string(reverse) + ":" + std::to_string(row));
        bool isNull = row % 3 == 0;
        ASSERT_EQ(pRead->varmeta.offset[local] == -1, isNull);
        ASSERT_EQ(colDataIsNull_s(pRead, local), isNull);
        if (isNull) {
          EXPECT_TRUE(pRead->hasNull);
        } else {
          std::string expected = row % 3 == 1 ? "" :
              "row-" + std::to_string(row);
          const char* value = colDataGetData(pRead, local);
          ASSERT_NE(value, nullptr);
          EXPECT_EQ(std::string(varDataVal(value), varDataLen(value)),
                    expected);
        }
      }
      if (!reverse) start += pBlock->info.rows;
    }
    EXPECT_EQ(start, reverse ? 0 : 64);
  }
  EXPECT_GT(winInputPoolGetStatis(pPool).loadPages, 0);
}
