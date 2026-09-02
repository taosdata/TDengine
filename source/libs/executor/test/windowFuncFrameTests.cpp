#include "gtest/gtest.h"

#include <cmath>
#include <limits>

#include "os.h"
#include "tdatablock.h"
#include "tpagedbuf.h"
#include "windowfunc.h"

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

  SWindowInputStore *pStore = nullptr;
  ASSERT_EQ(winInputStoreCreate(pInput, 128, 128, "windowInputStoreTest", &pStore), TSDB_CODE_SUCCESS);
  ASSERT_NE(pStore, nullptr);

  ASSERT_EQ(winInputStoreAppendBlock(pStore, pInput), TSDB_CODE_SUCCESS);
  EXPECT_EQ(winInputStoreGetRows(pStore), 64);
  EXPECT_GT(winInputStoreGetPageCount(pStore), 1);

  SDiskbasedBufStatis statis = winInputStoreGetStatis(pStore);
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
  blockDataDestroy(pInput);
}
