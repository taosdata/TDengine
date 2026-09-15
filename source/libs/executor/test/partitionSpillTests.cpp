/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can redistribute it and/or modify
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

#include <memory>
#include <vector>

#include "groupoperatorTest.h"
#include "taoserror.h"
#include "tdatablock.h"

namespace {

using PartitionPhaseResultPtr =
    std::unique_ptr<SPartitionPhaseTestResult>;

PartitionPhaseResultPtr makePartitionPhaseResult() {
  return PartitionPhaseResultPtr(new SPartitionPhaseTestResult{});
}

int32_t runPhased(const std::vector<SPartitionPhaseTestBlock>& blocks,
                  const SPartitionPhaseTestConfig& config,
                  SPartitionPhaseTestResult* pResult) {
  return partitionTestRunPhasedWithConfig(
      blocks.data(), static_cast<int32_t>(blocks.size()), &config, pResult);
}

void expectOutputsInPhase(const SPartitionPhaseTestResult& result,
                          int8_t phase) {
  for (int32_t i = 0; i < result.outputCount; ++i) {
    EXPECT_EQ(result.outputs[i].phase, phase) << "output " << i;
    EXPECT_NE(result.outputs[i].groupId, 0) << "output " << i;
    EXPECT_EQ(result.outputs[i].baseGId, 0) << "output " << i;
  }
}

}  // namespace

TEST(partitionPhaseTest, drainsMainAndSideBatchesWithoutIntermediateNull) {
  std::vector<SPartitionPhaseTestBlock> blocks = {
      {SCAN_PHASE_MAIN, 1, 2, 10, 1},
  };
  for (int32_t i = 0; i < 17; ++i) {
    blocks.push_back({SCAN_PHASE_NEXT, 1, 1, 20 + i, 0});
  }
  blocks.push_back({SCAN_PHASE_PREV, 1, 3, 9, -1});

  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;
  int32_t numBlocks = static_cast<int32_t>(blocks.size());
  ASSERT_EQ(partitionTestRunPhased(blocks.data(), numBlocks, &result),
            TSDB_CODE_SUCCESS);

  ASSERT_EQ(result.outputCount, 4);
  EXPECT_EQ(result.totalPulledBlocks, numBlocks);

  EXPECT_EQ(result.outputs[0].phase, SCAN_PHASE_MAIN);
  EXPECT_EQ(result.outputs[0].rows, 2);
  EXPECT_EQ(result.outputs[0].pulledBlocks, 2);
  EXPECT_EQ(result.outputs[0].firstTs, 10);
  EXPECT_EQ(result.outputs[0].lastTs, 11);

  EXPECT_EQ(result.outputs[1].phase, SCAN_PHASE_NEXT);
  EXPECT_EQ(result.outputs[1].rows, 16);
  EXPECT_EQ(result.outputs[1].pulledBlocks, 17);
  EXPECT_EQ(result.outputs[1].firstTs, 20);
  EXPECT_EQ(result.outputs[1].lastTs, 35);

  EXPECT_EQ(result.outputs[2].phase, SCAN_PHASE_NEXT);
  EXPECT_EQ(result.outputs[2].rows, 1);
  EXPECT_EQ(result.outputs[2].pulledBlocks, 19);
  EXPECT_EQ(result.outputs[2].firstTs, 36);
  EXPECT_EQ(result.outputs[2].lastTs, 36);

  EXPECT_EQ(result.outputs[3].phase, SCAN_PHASE_PREV);
  EXPECT_EQ(result.outputs[3].rows, 3);
  EXPECT_EQ(result.outputs[3].pulledBlocks, 19);
  EXPECT_EQ(result.outputs[3].firstTs, 9);
  EXPECT_EQ(result.outputs[3].lastTs, 7);
}

TEST(partitionPhaseTest, skipsEmptyPhasesWithoutReturningNull) {
  const SPartitionPhaseTestBlock blocks[] = {
      {SCAN_PHASE_PREV, 7, 2, 9, -1},
  };
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  ASSERT_EQ(partitionTestRunPhased(blocks, 1, &result),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(result.outputCount, 1);
  EXPECT_EQ(result.outputs[0].phase, SCAN_PHASE_PREV);
  EXPECT_EQ(result.outputs[0].rows, 2);
  EXPECT_EQ(result.outputs[0].firstTs, 9);
  EXPECT_EQ(result.outputs[0].lastTs, 8);
}

TEST(partitionPhaseTest, finishesEmptyInputWithoutOutput) {
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  ASSERT_EQ(partitionTestRunPhased(nullptr, 0, &result),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(result.outputCount, 0);
  EXPECT_EQ(result.totalPulledBlocks, 0);
}

TEST(partitionPhaseTest, rejectsPhaseRegression) {
  const SPartitionPhaseTestBlock blocks[] = {
      {SCAN_PHASE_MAIN, 1, 1, 10, 0},
      {SCAN_PHASE_PREV, 1, 1, 9, 0},
      {SCAN_PHASE_NEXT, 1, 1, 11, 0},
  };
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  EXPECT_EQ(partitionTestRunPhased(blocks, 3, &result),
            TSDB_CODE_PLAN_INTERNAL_ERROR);
}

TEST(partitionPhaseTest, drainsEachOuterEpochBeforeTheNext) {
  const SPartitionPhaseTestBlock blocks[] = {
      {SCAN_PHASE_MAIN, 7, 1, 10, 0, false, 10},
      {SCAN_PHASE_NEXT, 7, 1, 11, 0, false, 10},
      {SCAN_PHASE_PREV, 7, 1, 9, 0, false, 10},
      {SCAN_PHASE_MAIN, 7, 1, 20, 0, false, 20},
      {SCAN_PHASE_PREV, 7, 1, 19, 0, false, 20},
  };
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  ASSERT_EQ(partitionTestRunPhased(blocks, 5, &result),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(result.outputCount, 5);
  const int8_t expectedPhases[] = {
      SCAN_PHASE_MAIN,
      SCAN_PHASE_NEXT,
      SCAN_PHASE_PREV,
      SCAN_PHASE_MAIN,
      SCAN_PHASE_PREV,
  };
  for (int32_t i = 0; i < result.outputCount; ++i) {
    EXPECT_EQ(result.outputs[i].phase, expectedPhases[i]);
    EXPECT_EQ(result.outputs[i].baseGId, i < 3 ? 10 : 20);
  }
  EXPECT_EQ(result.outputs[0].groupId, result.outputs[1].groupId);
  EXPECT_EQ(result.outputs[0].groupId, result.outputs[2].groupId);
  EXPECT_EQ(result.outputs[3].groupId, result.outputs[4].groupId);
  EXPECT_NE(result.outputs[0].groupId, result.outputs[3].groupId);
}

TEST(partitionPhaseTest, rejectsOuterEpochRegression) {
  const SPartitionPhaseTestBlock blocks[] = {
      {SCAN_PHASE_MAIN, 7, 1, 10, 0, false, 20},
      {SCAN_PHASE_MAIN, 7, 1, 20, 0, false, 10},
  };
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  EXPECT_EQ(partitionTestRunPhased(blocks, 2, &result),
            TSDB_CODE_PLAN_INTERNAL_ERROR);
}

TEST(partitionPhaseTest, rejectsInvalidPhaseValue) {
  constexpr int8_t kInvalidPhase = INT8_MAX;
  const SPartitionPhaseTestBlock blocks[] = {
      {kInvalidPhase, 1, 1, 10, 0},
  };
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  EXPECT_EQ(partitionTestRunPhased(blocks, 1, &result),
            TSDB_CODE_INVALID_PARA);
}

TEST(partitionPhaseTest, rejectsMetadataOnlyInput) {
  const SPartitionPhaseTestBlock blocks[] = {
      {SCAN_PHASE_MAIN, 1, 1, 10, 0, true},
  };
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  EXPECT_EQ(partitionTestRunPhased(blocks, 1, &result),
            TSDB_CODE_PLAN_INTERNAL_ERROR);
}

TEST(partitionPhaseTest, invalidOutputDoesNotPartiallyUpdateResult) {
  SPartitionPhaseTestBlock block = {
      SCAN_PHASE_MAIN, 1, 3, 10, 1,
  };
  block.nullGroupRowMask = uint64_t{1} << 1;
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  EXPECT_EQ(partitionTestRunPhased(&block, 1, &result),
            TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  EXPECT_EQ(result.outputCount, 1);
  EXPECT_EQ(result.rowCount, 2);
  EXPECT_EQ(result.spillCountDuringOutput, 0);
  EXPECT_EQ(result.encryptedSpillCountDuringOutput, 0);
}

TEST(partitionPhaseTest, resetReleasesHeldBoundaryBlock) {
  SPartitionPhaseTestLifecycleResult result = {0};

  ASSERT_EQ(partitionTestHeldBlockLifecycle(PARTITION_PHASE_TEST_RESET,
                                            &result),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(result.heldBeforeAction);
  EXPECT_FALSE(result.heldAfterAction);
  EXPECT_FALSE(result.destroyed);
  EXPECT_EQ(result.phaseAfterAction, SCAN_PHASE_MAIN);
  EXPECT_TRUE(result.readingAfterAction);
  EXPECT_FALSE(result.inputDoneAfterAction);
}

TEST(partitionPhaseTest, destroyReleasesHeldBoundaryBlock) {
  SPartitionPhaseTestLifecycleResult result = {0};

  ASSERT_EQ(partitionTestHeldBlockLifecycle(PARTITION_PHASE_TEST_DESTROY,
                                            &result),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(result.heldBeforeAction);
  EXPECT_TRUE(result.destroyed);
}

TEST(partitionPhaseTest, prepareFailureLeavesHashAsSoleOwner) {
  const SPartitionPhaseTestBlock blocks[] = {
      {SCAN_PHASE_PREV, 7, 1, 10, 0},
  };
  SPartitionPhaseTestConfig config = {0};
  config.prepareOutputFailureCode = TSDB_CODE_OUT_OF_MEMORY;
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  EXPECT_EQ(partitionTestRunPhasedWithConfig(
                blocks, 1, &config, &result),
            TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(result.hashGroupCountOnExit, 1);
  EXPECT_EQ(result.sortedGroupCountOnExit, 0);
}

TEST(partitionPhaseTest, pageAllocationFailureLeavesNoResidentPage) {
  const SPartitionPhaseTestBlock blocks[] = {
      {SCAN_PHASE_MAIN, 7, 1, 10, 0},
  };
  SPartitionPhaseTestConfig config = {0};
  config.pageAllocationFailureCode = TSDB_CODE_OUT_OF_MEMORY;
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  EXPECT_EQ(partitionTestRunPhasedWithConfig(
                blocks, 1, &config, &result),
            TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(result.hashGroupCountOnExit, 1);
  EXPECT_EQ(result.sortedGroupCountOnExit, 0);
  EXPECT_EQ(result.residentPageCountOnExit, 0);
}

TEST(partitionPhaseTest,
     spillReadFailureLeavesTransferredGroupOwnedForCleanup) {
  std::vector<SPartitionPhaseTestBlock> blocks;
  for (int32_t i = 0; i < 5; ++i) {
    blocks.push_back({SCAN_PHASE_PREV, 7, 64, 1000 - i * 64, -1});
  }
  SPartitionPhaseTestConfig config = {0};
  config.memBudgetBytes = PARTITION_PHASE_TEST_PAGE_BUDGET;
  config.spillReadFailureCode = TSDB_CODE_FAILED;
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  EXPECT_EQ(runPhased(blocks, config, &result), TSDB_CODE_FAILED);
  EXPECT_EQ(result.hashGroupCountOnExit, 0);
  EXPECT_EQ(result.sortedGroupCountOnExit, 1);
  EXPECT_EQ(result.residentPageCountOnExit, 1);
  EXPECT_TRUE(result.spillFileOpenOnExit);
}

TEST(partitionPhaseTest, spillCryptFailureClosesPartialFile) {
  std::vector<SPartitionPhaseTestBlock> blocks;
  for (int32_t i = 0; i < 5; ++i) {
    blocks.push_back({SCAN_PHASE_PREV, 7, 64, 1000 - i * 64, -1});
  }
  SPartitionPhaseTestConfig config = {0};
  config.memBudgetBytes = PARTITION_PHASE_TEST_PAGE_BUDGET;
  config.encryptSpill = true;
  config.spillCryptFailureCode = TSDB_CODE_OUT_OF_MEMORY;
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;

  EXPECT_EQ(runPhased(blocks, config, &result),
            TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(result.hashGroupCountOnExit, 1);
  EXPECT_EQ(result.sortedGroupCountOnExit, 0);
  EXPECT_FALSE(result.spillFileOpenOnExit);
}

TEST(partitionPhaseTest, retainsSpillStateAcrossSideBatches) {
  std::vector<SPartitionPhaseTestBlock> blocks;
  constexpr int32_t kRowsPerBlock = 64;
  constexpr int32_t kBlockCount = 17;
  constexpr int64_t kFirstTs = 2000;
  for (int32_t i = 0; i < kBlockCount; ++i) {
    blocks.push_back({SCAN_PHASE_PREV, 7, kRowsPerBlock,
                      kFirstTs - (int64_t)i * kRowsPerBlock, -1});
  }

  SPartitionPhaseTestConfig config = {0};
  config.memBudgetBytes = PARTITION_PHASE_TEST_PAGE_BUDGET;
  config.encryptSpill = true;
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;
  ASSERT_EQ(runPhased(blocks, config, &result), TSDB_CODE_SUCCESS);

  EXPECT_GT(result.spillCountBeforeSecondBatch, 0);
  EXPECT_GT(result.encryptedSpillCountBeforeSecondBatch, 0);
  EXPECT_TRUE(result.spillScratchRetainedBeforeSecondBatch);
  EXPECT_TRUE(result.spillCryptReleasedBeforeSecondBatch);
  EXPECT_GT(result.spillCountDuringOutput, 0);
  EXPECT_GT(result.encryptedSpillCountDuringOutput, 0);
  ASSERT_EQ(result.rowCount, kRowsPerBlock * kBlockCount);
  for (int32_t i = 0; i < result.rowCount; ++i) {
    EXPECT_EQ(result.timestamps[i], kFirstTs - i) << "row " << i;
  }
  expectOutputsInPhase(result, SCAN_PHASE_PREV);
}

TEST(partitionPhaseTest, spillsEveryPhaseWithOrderedOutput) {
  const int8_t phases[] = {
      SCAN_PHASE_MAIN,
      SCAN_PHASE_NEXT,
      SCAN_PHASE_PREV,
  };
  constexpr int32_t kRowsPerBlock = 64;
  constexpr int32_t kBlockCount = 5;
  for (int8_t phase : phases) {
    SCOPED_TRACE(testing::Message() << "phase="
                                    << static_cast<int32_t>(phase));
    int64_t firstTs = phase == SCAN_PHASE_PREV ? 3000 : 1000;
    int64_t step = phase == SCAN_PHASE_PREV ? -1 : 1;
    std::vector<SPartitionPhaseTestBlock> blocks;
    std::vector<int64_t> expected;
    for (int32_t i = 0; i < kBlockCount; ++i) {
      int64_t blockFirst = firstTs + step * i * kRowsPerBlock;
      blocks.push_back({phase, 7, kRowsPerBlock, blockFirst, step});
      for (int32_t row = 0; row < kRowsPerBlock; ++row) {
        expected.push_back(blockFirst + step * row);
      }
    }

    SPartitionPhaseTestConfig config = {0};
    config.memBudgetBytes = PARTITION_PHASE_TEST_PAGE_BUDGET;
    auto resultOwner = makePartitionPhaseResult();
    SPartitionPhaseTestResult& result = *resultOwner;
    ASSERT_EQ(runPhased(blocks, config, &result),
              TSDB_CODE_SUCCESS);

    EXPECT_GT(result.spillCountDuringOutput, 0);
    ASSERT_EQ(result.rowCount, static_cast<int32_t>(expected.size()));
    for (int32_t i = 0; i < result.rowCount; ++i) {
      EXPECT_EQ(result.timestamps[i], expected[i]) << "row " << i;
      EXPECT_EQ(result.groupValues[i], 7) << "row " << i;
    }
    expectOutputsInPhase(result, phase);
  }
}

TEST(partitionPhaseTest, replaysSpilledBlocksWithReservedColumn) {
  for (bool encrypted : {false, true}) {
    SCOPED_TRACE(encrypted);
    std::vector<SPartitionPhaseTestBlock> blocks;
    for (int32_t i = 0; i < 5; ++i) {
      blocks.push_back({SCAN_PHASE_MAIN, 7, 64, 1000 + i * 64, 1});
    }
    SPartitionPhaseTestConfig config = {0};
    config.memBudgetBytes = PARTITION_PHASE_TEST_PAGE_BUDGET;
    config.encryptSpill = encrypted;
    config.reserveColumn = true;
    auto result = makePartitionPhaseResult();
    ASSERT_EQ(runPhased(blocks, config, result.get()), TSDB_CODE_SUCCESS);
    EXPECT_GT(result->outputCount, 1);
    EXPECT_GT(result->spillCountDuringOutput, 0);
    if (encrypted) {
      EXPECT_GT(result->encryptedSpillCountDuringOutput, 0);
    }
    ASSERT_EQ(result->rowCount, 320);
    for (int32_t i = 0; i < result->rowCount; ++i) {
      EXPECT_EQ(result->timestamps[i], 1000 + i);
      EXPECT_EQ(result->groupValues[i], 7);
    }
  }
}

TEST(partitionPhaseTest, ordersMultipleSpilledGroupsAcrossFragments) {
  std::vector<SPartitionPhaseTestBlock> blocks;
  std::vector<int64_t> expectedGroup7;
  std::vector<int64_t> expectedGroup8;
  constexpr int32_t kRowsPerBlock = 64;
  constexpr int32_t kBlockCount = 17;
  constexpr int64_t kFirstTs = 4000;
  for (int32_t i = 0; i < kBlockCount; ++i) {
    int32_t groupValue = 7 + i % 2;
    int64_t firstTs = kFirstTs - (int64_t)i * kRowsPerBlock;
    blocks.push_back({SCAN_PHASE_PREV, groupValue, kRowsPerBlock,
                      firstTs, -1});
    std::vector<int64_t>& expected =
        groupValue == 7 ? expectedGroup7 : expectedGroup8;
    for (int32_t row = 0; row < kRowsPerBlock; ++row) {
      expected.push_back(firstTs - row);
    }
  }

  SPartitionPhaseTestConfig config = {0};
  config.memBudgetBytes = PARTITION_PHASE_TEST_PAGE_BUDGET;
  auto resultOwner = makePartitionPhaseResult();
  SPartitionPhaseTestResult& result = *resultOwner;
  ASSERT_EQ(runPhased(blocks, config, &result), TSDB_CODE_SUCCESS);

  EXPECT_GT(result.spillCountDuringOutput, 2);
  ASSERT_EQ(result.rowCount, kRowsPerBlock * kBlockCount);
  std::vector<int64_t> actualGroup7;
  std::vector<int64_t> actualGroup8;
  for (int32_t i = 0; i < result.rowCount; ++i) {
    if (result.groupValues[i] == 7) {
      actualGroup7.push_back(result.timestamps[i]);
    } else if (result.groupValues[i] == 8) {
      actualGroup8.push_back(result.timestamps[i]);
    } else {
      ADD_FAILURE() << "unexpected group value "
                    << result.groupValues[i];
    }
  }
  EXPECT_EQ(actualGroup7, expectedGroup7);
  EXPECT_EQ(actualGroup8, expectedGroup8);
  expectOutputsInPhase(result, SCAN_PHASE_PREV);
}

TEST(partitionSpillTest, selectsGroupWithHighestGrowthLevel) {
  // Growth level, resident rows, and total rows deliberately rank different.
  // Group 202 must win by growth level; group 303 has no resident page.
  const SPartitionSpillTestGroup groups[] = {
      {101, 1, 200, 5000},
      {202, 2, 3, 1000},
      {303, -1, 1000, 9000},
  };
  uint64_t selectedGroupId = 0;

  ASSERT_EQ(partitionSpillTestSelectGroup(groups,
                                          sizeof(groups) / sizeof(groups[0]),
                                          &selectedGroupId),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(selectedGroupId, 202);
}

TEST(partitionSpillTest, keepsFifoOrderWithinGrowthLevelBucket) {
  // 401 and 502 tie on level. Groups within the same bucket keep
  // insertion order, so selecting a victim does not require another metric.
  const SPartitionSpillTestGroup groups[] = {
      {401, 2, 50, 500},
      {502, 2, 80, 100},
      {603, 1, 300, 900},
  };
  uint64_t selectedGroupId = 0;

  ASSERT_EQ(partitionSpillTestSelectGroup(groups,
                                          sizeof(groups) / sizeof(groups[0]),
                                          &selectedGroupId),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(selectedGroupId, 401);
}

TEST(partitionSpillTest, updatesSelectionAfterSpillMetricsChange) {
  const SPartitionSpillTestGroup groups[] = {
      {701, 3, 3, 300},
      {802, 2, 80, 200},
      {903, 2, 50, 100},
  };
  uint64_t selectedGroupIds[3] = {0};

  ASSERT_EQ(partitionSpillTestSelectSequence(groups, sizeof(groups) / sizeof(groups[0]), selectedGroupIds,
                                             sizeof(selectedGroupIds) / sizeof(selectedGroupIds[0])),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(selectedGroupIds[0], 701);
  EXPECT_EQ(selectedGroupIds[1], 802);
  EXPECT_EQ(selectedGroupIds[2], 903);
}

TEST(partitionSpillTest, calculatesTwentyPercentByteTarget) {
  EXPECT_EQ(partitionSpillTestBatchTargetBytes(0), 0);
  EXPECT_EQ(partitionSpillTestBatchTargetBytes(1), 1);
  EXPECT_EQ(partitionSpillTestBatchTargetBytes(4), 1);
  EXPECT_EQ(partitionSpillTestBatchTargetBytes(5), 1);
  EXPECT_EQ(partitionSpillTestBatchTargetBytes(10), 2);
  EXPECT_EQ(partitionSpillTestBatchTargetBytes(100 * 1024 * 1024),
            20 * 1024 * 1024);
}

TEST(partitionSpillTest, selectsGrowthClassForRemainingRun) {
  const int32_t rowCapacities[] = {10, 20, 40, 80, 160};
  int32_t numClasses = sizeof(rowCapacities) / sizeof(rowCapacities[0]);

  EXPECT_EQ(partitionTestSelectGrowthClass(rowCapacities, numClasses, 0, 11),
            1);
  EXPECT_EQ(partitionTestSelectGrowthClass(rowCapacities, numClasses, 0, 35),
            2);
  EXPECT_EQ(
      partitionTestSelectGrowthClass(rowCapacities, numClasses, 0, 150), 4);
  EXPECT_EQ(
      partitionTestSelectGrowthClass(rowCapacities, numClasses, 2, 200), 4);
}

TEST(partitionSpillTest, scansRunOnlyWhenPageCanGrow) {
  const int32_t rowCapacities[] = {10, 20, 40, 80, 160};
  int32_t numClasses = sizeof(rowCapacities) / sizeof(rowCapacities[0]);

  EXPECT_FALSE(partitionTestNeedsRunSize(rowCapacities, numClasses, 0, 9,
                                         true));
  EXPECT_TRUE(partitionTestNeedsRunSize(rowCapacities, numClasses, 0, 10,
                                        true));
  EXPECT_FALSE(partitionTestNeedsRunSize(rowCapacities, numClasses, 4, 160,
                                         true));
  EXPECT_FALSE(partitionTestNeedsRunSize(rowCapacities, numClasses, 0, 10,
                                         false));
}

TEST(partitionSpillTest, retainsFullMaxPagesUntilGroupSpill) {
  SPartitionMaxPageTestResult result = {0};

  ASSERT_EQ(partitionTestRetainMaxPages(3, &result), TSDB_CODE_SUCCESS);

  EXPECT_EQ(result.beforeSpill.groupPages, 3);
  EXPECT_EQ(result.beforeSpill.residentPages, 3);
  EXPECT_EQ(result.beforeSpill.spillCount, 0);
  EXPECT_EQ(result.beforeSpill.fragmentCount, 0);
  // The still-resident, still-bucketed group satisfies every accounting
  // invariant, including the positive spill-bucket-level branch that the
  // recycle-failure test's now-empty group cannot exercise.
  EXPECT_TRUE(result.residentBytesInvariantOk);
  EXPECT_TRUE(result.residentPagesInvariantOk);
  EXPECT_TRUE(result.spillBucketLevelInvariantOk);

  EXPECT_EQ(result.afterSpill.groupPages, 0);
  EXPECT_EQ(result.afterSpill.residentPages, 0);
  EXPECT_EQ(result.afterSpill.spillCount, 1);
  EXPECT_EQ(result.afterSpill.spillPages, 3);
  EXPECT_EQ(result.afterSpill.fragmentCount, 1);
  EXPECT_GT(result.afterSpill.logicalFileSize, 0);
}

TEST(partitionSpillTest, writesOnlyActualEncodedData) {
  const int32_t             rowsPerPage[] = {1, 2, 3};
  SPartitionSpillTestResult result = {0};

  ASSERT_EQ(
      partitionSpillTestRun(
          rowsPerPage, sizeof(rowsPerPage) / sizeof(rowsPerPage[0]), &result),
      TSDB_CODE_SUCCESS);
  ASSERT_EQ(result.blockCount, 3);

  int64_t expectedLength = 0;
  int64_t expectedValueSum = 0;
  for (int32_t pageIndex = 0; pageIndex < result.blockCount; ++pageIndex) {
    EXPECT_GT(result.blockSizes[pageIndex], 0);
    EXPECT_EQ(result.decodedRows[pageIndex], rowsPerPage[pageIndex]);
    expectedLength += sizeof(int32_t) + result.blockSizes[pageIndex];
    for (int32_t rowIndex = 0; rowIndex < rowsPerPage[pageIndex]; ++rowIndex) {
      expectedValueSum += pageIndex * 1000 + rowIndex;
    }
  }

  EXPECT_EQ(result.fragmentNumRows, result.inputRowCount);
  EXPECT_EQ(result.decodedValueSum, expectedValueSum);
  EXPECT_EQ(result.fragmentLength, expectedLength);
  EXPECT_EQ(result.logicalFileSize, expectedLength);
  EXPECT_EQ(result.physicalFileSize, expectedLength);
  EXPECT_LT(result.fragmentLength, result.inputPageCount * result.pageSize);
  EXPECT_EQ(result.writeCallCount, 1);
  EXPECT_EQ(result.readCallCount, 1);
}

TEST(partitionSpillTest, boundsWriteBufferWithinOneGroup) {
  const int32_t rowsPerPage[] = {32, 32, 32};
  SPartitionSpillTestResult unbounded = {0};
  SPartitionSpillTestResult bounded = {0};

  ASSERT_EQ(partitionSpillTestRun(
                rowsPerPage, sizeof(rowsPerPage) / sizeof(rowsPerPage[0]),
                &unbounded),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(unbounded.blockCount, 3);

  int32_t writeBatchLimit = sizeof(int32_t) + unbounded.blockSizes[0];
  ASSERT_EQ(unbounded.blockSizes[1], unbounded.blockSizes[0]);
  ASSERT_EQ(unbounded.blockSizes[2], unbounded.blockSizes[0]);
  ASSERT_EQ(partitionSpillTestRunWithWriteLimit(
                rowsPerPage, sizeof(rowsPerPage) / sizeof(rowsPerPage[0]),
                writeBatchLimit, &bounded),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(bounded.fragmentCount, 1);
  EXPECT_EQ(bounded.fragmentLength, unbounded.fragmentLength);
  EXPECT_EQ(bounded.decodedRows[0] + bounded.decodedRows[1] +
                bounded.decodedRows[2],
            bounded.inputRowCount);
  EXPECT_EQ(bounded.decodedValueSum, unbounded.decodedValueSum);
  EXPECT_EQ(bounded.writeCallCount, 3);
}

TEST(partitionSpillTest, mergesGroupsButKeepsIndependentFragments) {
  SPartitionSpillTestBatchWriteResult result = {0};

  ASSERT_EQ(partitionSpillTestBatchWrite(false, &result), TSDB_CODE_SUCCESS);
  ASSERT_EQ(result.spillCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(result.writeCallCount, 1);
  EXPECT_EQ(result.spillCount, 2);
  EXPECT_EQ(result.spillFailures, 0);
  EXPECT_EQ(result.remainingResidentPages, 8);
  EXPECT_EQ(result.logicalFileSize, result.physicalFileSize);

  for (int32_t groupIndex = 0; groupIndex < PARTITION_SPILL_TEST_BATCH_GROUPS; ++groupIndex) {
    if (groupIndex < 2) {
      EXPECT_EQ(result.groupPages[groupIndex], 0);
      EXPECT_EQ(result.groupRows[groupIndex], 0);
      ASSERT_EQ(result.fragmentCounts[groupIndex], 1);
      EXPECT_EQ(result.fragmentRows[groupIndex], 1);
    } else {
      EXPECT_EQ(result.groupPages[groupIndex], 1);
      EXPECT_EQ(result.groupRows[groupIndex], 1);
      EXPECT_EQ(result.fragmentCounts[groupIndex], 0);
    }
  }

  EXPECT_EQ(result.fragmentOffsets[0], 0);
  EXPECT_EQ(result.fragmentOffsets[1], result.fragmentLengths[0]);
  EXPECT_EQ(result.logicalFileSize, result.fragmentLengths[0] + result.fragmentLengths[1]);
  EXPECT_EQ(result.decodedRows, 2);
  EXPECT_EQ(result.decodedValueSum, 1000);
}

TEST(partitionSpillTest, failedMergedWriteKeepsEveryGroupResident) {
  SPartitionSpillTestBatchWriteResult result = {0};

  ASSERT_EQ(partitionSpillTestBatchWrite(true, &result), TSDB_CODE_SUCCESS);
  EXPECT_EQ(result.spillCode, TSDB_CODE_FAILED);
  EXPECT_EQ(result.writeCallCount, 1);
  EXPECT_GT(result.physicalFileSizeAtFailure, 0);
  EXPECT_EQ(result.logicalFileSize, 0);
  EXPECT_EQ(result.physicalFileSize, 0);
  EXPECT_EQ(result.spillCount, 0);
  EXPECT_EQ(result.spillFailures, 1);
  EXPECT_EQ(result.remainingResidentPages, PARTITION_SPILL_TEST_BATCH_GROUPS);

  for (int32_t groupIndex = 0; groupIndex < PARTITION_SPILL_TEST_BATCH_GROUPS; ++groupIndex) {
    EXPECT_EQ(result.groupPages[groupIndex], 1);
    EXPECT_EQ(result.groupRows[groupIndex], 1);
    EXPECT_EQ(result.fragmentCounts[groupIndex], 0);
  }
}

TEST(partitionSpillTest, spillsWholeGroupIntoOneFragment) {
  const int32_t             rowsPerPage[] = {2, 3, 1};
  SPartitionSpillTestResult result = {0};

  ASSERT_EQ(
      partitionSpillTestRun(
          rowsPerPage, sizeof(rowsPerPage) / sizeof(rowsPerPage[0]), &result),
      TSDB_CODE_SUCCESS);

  EXPECT_EQ(result.fragmentCount, 1);
  EXPECT_EQ(result.fragmentOffset, 0);
  EXPECT_EQ(result.blockCount, 3);
  EXPECT_EQ(result.fragmentNumRows, result.inputRowCount);
  EXPECT_EQ(result.fragmentOffset + result.fragmentLength,
            result.logicalFileSize);

  EXPECT_EQ(result.remainingGroupPages, 0);
  EXPECT_EQ(result.remainingGroupRows, 0);
  EXPECT_EQ(result.remainingResidentPages, 0);

  EXPECT_EQ(result.spillCount, 1);
  EXPECT_EQ(result.spillPages, 3);
  EXPECT_EQ(result.spillRows, result.inputRowCount);
  EXPECT_EQ(result.spillBytes, result.fragmentLength);

  // Reading went through the production reader, so load statistics must
  // account for the single fragment exactly once.
  EXPECT_EQ(result.loadCount, 1);
  EXPECT_EQ(result.loadBytes, result.fragmentLength);
}

TEST(partitionSpillTest, skipsEmptyPages) {
  const int32_t             rowsPerPage[] = {0, 2};
  SPartitionSpillTestResult result = {0};

  ASSERT_EQ(
      partitionSpillTestRun(
          rowsPerPage, sizeof(rowsPerPage) / sizeof(rowsPerPage[0]), &result),
      TSDB_CODE_SUCCESS);
  EXPECT_EQ(result.fragmentCount, 1);
  EXPECT_EQ(result.blockCount, 1);
  EXPECT_EQ(result.fragmentNumRows, 2);
  EXPECT_EQ(result.decodedRows[0], 2);
  EXPECT_EQ(result.decodedValueSum, 2001);
  EXPECT_EQ(result.spillPages, 2);
  EXPECT_EQ(result.spillRows, 2);
  EXPECT_EQ(result.remainingGroupPages, 0);
  EXPECT_EQ(result.remainingGroupRows, 0);
  EXPECT_EQ(result.remainingResidentPages, 0);
}

TEST(partitionSpillTest, writesNothingWhenAllPagesAreEmpty) {
  const int32_t             rowsPerPage[] = {0, 0};
  SPartitionSpillTestResult result = {0};

  ASSERT_EQ(
      partitionSpillTestRun(
          rowsPerPage, sizeof(rowsPerPage) / sizeof(rowsPerPage[0]), &result),
      TSDB_CODE_SUCCESS);
  EXPECT_EQ(result.fragmentCount, 0);
  EXPECT_EQ(result.blockCount, 0);
  EXPECT_EQ(result.logicalFileSize, 0);
  EXPECT_EQ(result.physicalFileSize, 0);
  EXPECT_EQ(result.loadCount, 0);
  EXPECT_EQ(result.loadBytes, 0);

  EXPECT_EQ(result.spillCount, 1);
  EXPECT_EQ(result.spillPages, 2);
  EXPECT_EQ(result.spillRows, 0);
  EXPECT_EQ(result.spillBytes, 0);
  EXPECT_EQ(result.remainingGroupPages, 0);
  EXPECT_EQ(result.remainingGroupRows, 0);
  EXPECT_EQ(result.remainingResidentPages, 0);
}

TEST(partitionSpillTest, appendsAndReadsMultipleFragments) {
  const int32_t rowsPerPage[] = {2, 1, 3};
  SPartitionSpillTestMultiFragmentResult result = {0};

  ASSERT_EQ(partitionSpillTestMultipleFragments(
                rowsPerPage, 2, 1, false, &result),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(result.readCode, TSDB_CODE_SUCCESS);
  ASSERT_EQ(result.fragmentCount, 2);
  EXPECT_EQ(result.fragmentOffsets[0], 0);
  EXPECT_EQ(result.fragmentOffsets[1],
            result.fragmentOffsets[0] + result.fragmentLengths[0]);
  EXPECT_EQ(result.logicalFileSize,
            result.fragmentOffsets[1] + result.fragmentLengths[1]);
  EXPECT_EQ(result.physicalFileSize, result.logicalFileSize);

  EXPECT_EQ(result.fragmentNumRows[0], 3);
  EXPECT_EQ(result.fragmentNumRows[1], 3);

  ASSERT_EQ(result.decodedBlockCount, 3);
  EXPECT_EQ(result.decodedBlockRows[0], 2);
  EXPECT_EQ(result.decodedBlockRows[1], 1);
  EXPECT_EQ(result.decodedBlockRows[2], 3);
  EXPECT_EQ(result.decodedFirstValues[0], 0);
  EXPECT_EQ(result.decodedFirstValues[1], 1000);
  EXPECT_EQ(result.decodedFirstValues[2], 2000);
  EXPECT_EQ(result.decodedRows, 6);
  EXPECT_EQ(result.decodedValueSum, 7004);

  EXPECT_EQ(result.afterFirstFragment.fragmentIndex, 1);
  EXPECT_EQ(result.afterFirstFragment.readOffset, 0);
  EXPECT_EQ(result.afterFirstFragment.decodedRows, 0);

  EXPECT_EQ(result.afterRead.fragmentIndex, 2);
  EXPECT_EQ(result.afterRead.readOffset, 0);
  EXPECT_EQ(result.afterRead.decodedRows, 0);
  EXPECT_EQ(result.loadCount, 2);
  EXPECT_EQ(result.loadBytes, result.logicalFileSize);
}

TEST(partitionSpillTest, validatesEveryFragmentRowTotal) {
  const int32_t rowsPerPage[] = {2, 1, 3};
  SPartitionSpillTestMultiFragmentResult result = {0};

  ASSERT_EQ(partitionSpillTestMultipleFragments(
                rowsPerPage, 2, 1, true, &result),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(result.readCode, TSDB_CODE_FILE_CORRUPTED);
  EXPECT_EQ(result.decodedBlockCount, 2);
  EXPECT_EQ(result.decodedRows, 3);
  // The tampered fragment is primed and its only block consumed before the
  // last-block reconciliation rejects it, so the cursor sits at the
  // fragment's end and both loads are already counted.
  EXPECT_EQ(result.afterRead.fragmentIndex, 1);
  EXPECT_EQ(result.afterRead.readOffset,
            result.fragmentOffsets[1] + result.fragmentLengths[1]);
  EXPECT_EQ(result.afterRead.decodedRows, 0);
  EXPECT_EQ(result.loadCount, 2);
  EXPECT_EQ(result.loadBytes, result.logicalFileSize);
}

TEST(partitionSpillTest, encryptsAndDecryptsPartitionFragments) {
  SPartitionSpillTestEncryptionResult result = {0};

  ASSERT_EQ(partitionSpillTestEncryption(3, &result), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(result.cryptContextCreated);
  EXPECT_TRUE(result.scopeRestored);
  EXPECT_EQ(result.fragmentCount, 1);
  EXPECT_EQ(result.fragmentNumRows, 3);
  EXPECT_GT(result.encryptedBlockSize, result.plainBlockSize);
  EXPECT_EQ(result.encryptedSpillCount, 1);
  EXPECT_EQ(result.logicalFileSize, result.physicalFileSize);

  EXPECT_EQ(result.readBackRows, 3);
  EXPECT_EQ(result.decodedValueSum, 3);
  EXPECT_EQ(result.loadCount, 1);
  EXPECT_EQ(result.loadBytes, result.logicalFileSize);
}

TEST(partitionSpillTest, rollsBackFailedFragmentAppend) {
  SPartitionSpillTestRollbackResult result = {0};

  ASSERT_EQ(partitionSpillTestRollback(2, 3, &result), TSDB_CODE_SUCCESS);
  EXPECT_EQ(result.spillCode, TSDB_CODE_FAILED);
  EXPECT_GT(result.physicalFileSizeAtFailure, result.before.physicalFileSize);
  EXPECT_EQ(result.before.logicalFileSize, result.before.physicalFileSize);
  EXPECT_EQ(result.before.logicalFileSize, result.after.logicalFileSize);
  EXPECT_EQ(result.before.physicalFileSize, result.after.physicalFileSize);
  EXPECT_EQ(result.after.logicalFileSize, result.after.physicalFileSize);
  EXPECT_EQ(result.before.logicalFileSize,
            result.fragmentOffsetBefore + result.fragmentLengthBefore);
  EXPECT_EQ(result.before.fragmentCount, 1);
  EXPECT_EQ(result.after.fragmentCount, 1);
  EXPECT_EQ(result.fragmentOffsetBefore, result.fragmentOffsetAfter);
  EXPECT_EQ(result.fragmentLengthBefore, result.fragmentLengthAfter);

  EXPECT_EQ(result.before.groupPages, 1);
  EXPECT_EQ(result.after.groupPages, result.before.groupPages);
  EXPECT_EQ(result.before.groupRows, 3);
  EXPECT_EQ(result.after.groupRows, result.before.groupRows);
  EXPECT_EQ(result.before.residentPages, 1);
  EXPECT_EQ(result.after.residentPages, result.before.residentPages);
  EXPECT_EQ(result.before.residentRows, 3);
  EXPECT_EQ(result.after.residentRows, result.before.residentRows);
  EXPECT_EQ(result.residentValueSumBefore, 3003);
  EXPECT_EQ(result.residentValueSumAfter, result.residentValueSumBefore);

  EXPECT_EQ(result.after.spillFailures, result.before.spillFailures + 1);
  EXPECT_EQ(result.after.spillCount, result.before.spillCount);
  EXPECT_EQ(result.after.spillPages, result.before.spillPages);
  EXPECT_EQ(result.after.spillRows, result.before.spillRows);
  EXPECT_EQ(result.after.spillBytes, result.before.spillBytes);
  EXPECT_EQ(result.readBackRows, 2);
  EXPECT_EQ(result.decodedValueSum, 1);
}

TEST(partitionSpillTest, rejectsCorruptedFragments) {
  const int32_t rowsPerPage[] = {1, 2, 3};
  const int32_t numPages = sizeof(rowsPerPage) / sizeof(rowsPerPage[0]);

  // The fragment's recorded row total no longer matches the decoded rows:
  // the last-block reconciliation in partitionReadFragmentBlock must
  // reject it.
  EXPECT_EQ(
      partitionSpillTestCorruptRead(rowsPerPage, numPages,
                                    PARTITION_SPILL_TEST_CORRUPT_FRAGMENT_ROWS),
      TSDB_CODE_FILE_CORRUPTED);
  // A block's leading row count disagrees with its fixed-column length:
  // blockDataFromBufBounded's validator must reject it.
  EXPECT_EQ(
      partitionSpillTestCorruptRead(rowsPerPage, numPages,
                                    PARTITION_SPILL_TEST_CORRUPT_BLOCK_ROWS),
      TSDB_CODE_FILE_CORRUPTED);
  // An inflated column length overruns the block bounds.
  EXPECT_EQ(
      partitionSpillTestCorruptRead(rowsPerPage, numPages,
                                    PARTITION_SPILL_TEST_CORRUPT_COL_LENGTH),
      TSDB_CODE_FILE_CORRUPTED);
  // A zeroed fixed-column length stays in bounds but would silently skip
  // the copy and surface stale data; the exact-length check must reject it.
  EXPECT_EQ(partitionSpillTestCorruptRead(
                rowsPerPage, numPages,
                PARTITION_SPILL_TEST_CORRUPT_COL_LENGTH_ZERO),
            TSDB_CODE_FILE_CORRUPTED);
}

TEST(partitionSpillTest, nullRowsSurviveBothReadPaths) {
  const int32_t rowsPerPage[] = {5, 4};
  const int32_t numPages = sizeof(rowsPerPage) / sizeof(rowsPerPage[0]);
  // Rows 0 and 3 of every page are NULL: 2 per page on page 0 (5 rows) and
  // 2 on page 1 (4 rows).
  const uint32_t                nullRowMask = (1u << 0) | (1u << 3);
  SPartitionSpillTestNullResult result = {0};

  ASSERT_EQ(partitionSpillTestNullRoundTrip(rowsPerPage, numPages, nullRowMask,
                                            &result),
            TSDB_CODE_SUCCESS);

  // Values are pageIndex * 1000 + rowIndex, NULL rows excluded: page 0 has
  // rows 0..4 so 1 + 2 + 4 survive, page 1 has rows 0..3 so 1001 + 1002 do.
  const int64_t expectedRows = 9;
  const int64_t expectedNullRows = 4;
  const int64_t expectedSum = (1 + 2 + 4) + (1001 + 1002);

  // The resident-page path decodes with blockDataFromPagedBuf...
  EXPECT_TRUE(result.pagedHasNull);
  EXPECT_EQ(result.pagedRows, expectedRows);
  EXPECT_EQ(result.pagedNullRows, expectedNullRows);
  EXPECT_EQ(result.pagedValueSum, expectedSum);

  // ...and the spill path with blockDataFromBufBounded. A NULL that only
  // survives one of them makes results depend on memory pressure.
  EXPECT_TRUE(result.spilledHasNull);
  EXPECT_EQ(result.spilledRows, expectedRows);
  EXPECT_EQ(result.spilledNullRows, expectedNullRows);
  EXPECT_EQ(result.spilledValueSum, expectedSum);
}

TEST(partitionSpillTest, propagatesSpillErrorViaTerrno) {
  int32_t reported = TSDB_CODE_SUCCESS;

  // The injected write failure returns TSDB_CODE_FAILED without touching
  // terrno; the group lookup must still surface that exact code to its
  // caller through terrno.
  ASSERT_EQ(partitionSpillTestErrnoPropagation(&reported), TSDB_CODE_SUCCESS);
  EXPECT_EQ(reported, TSDB_CODE_FAILED);
}

TEST(partitionSpillTest, recycleFailureKeepsFragmentConsistent) {
  const int32_t                    rowsPerPage[] = {2, 3, 1};
  SPartitionSpillTestRecycleResult result = {0};

  ASSERT_EQ(partitionSpillTestRecycleFailure(
                rowsPerPage, sizeof(rowsPerPage) / sizeof(rowsPerPage[0]), 1,
                &result),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(result.spillCode, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

  // Bookkeeping is fully cleared even though page 1 failed to recycle...
  EXPECT_EQ(result.remainingGroupPages, 0);
  EXPECT_EQ(result.remainingGroupRows, 0);
  // ...the failed page just stays uncounted (leaked until destruction)...
  EXPECT_EQ(result.remainingResidentPages, 1);
  // ...and the committed fragment remains fully readable.
  EXPECT_EQ(result.fragmentCount, 1);
  EXPECT_EQ(result.readBackRows, 6);
}

TEST(partitionSpillTest, metadataOnlyBlockDoesNotAllocatePage) {
  SPartitionNotLoadedTestResult result = {0};

  ASSERT_EQ(partitionTestNotLoadedBlock(&result), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(result.pageListInitialized);
  EXPECT_EQ(result.groupPages, 0);
  EXPECT_EQ(result.residentPages, 0);
  EXPECT_EQ(result.peakResidentPages, 0);
  EXPECT_EQ(result.spillCount, 0);
  EXPECT_EQ(result.notLoadedBlocks, 1);

  EXPECT_EQ(result.outputRows, 1);
  EXPECT_FALSE(result.outputDataLoad);
  EXPECT_EQ(result.outputGroupId, result.groupId);
}

TEST(blockDataFromBufBoundedTest, serializesReservedColumnsInFullLayout) {
  for (int8_t type : {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_VARCHAR}) {
    for (int32_t reserved = 0; reserved < 3; ++reserved) {
      SCOPED_TRACE(testing::Message() << "type=" << static_cast<int>(type)
                                      << " reserved=" << reserved);
      SSDataBlock* pBlock = nullptr;
      ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
      for (int32_t i = 0; i < 3; ++i) {
        SColumnInfoData col = createColumnInfoData(
            i == reserved ? type : TSDB_DATA_TYPE_INT, 4, i);
        col.info.noData = i == reserved;
        col.info.precision = i;
        col.info.scale = i + 1;
        ASSERT_EQ(blockDataAppendColInfo(pBlock, &col), TSDB_CODE_SUCCESS);
      }
      ASSERT_EQ(blockDataEnsureCapacity(pBlock, 2), TSDB_CODE_SUCCESS);
      pBlock->info.rows = 2;
      for (int32_t i = 0; i < 3; ++i) {
        auto col = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
        if (col->info.type == TSDB_DATA_TYPE_VARCHAR) {
          char value[4] = {0};
          varDataSetLen(value, 2);
          memcpy(varDataVal(value), "xy", 2);
          ASSERT_EQ(colDataSetVal(col, 0, value, false),
                    TSDB_CODE_SUCCESS);
        } else {
          int32_t value = 123 + i;
          ASSERT_EQ(colDataSetVal(col, 0, (char*)&value, false),
                    TSDB_CODE_SUCCESS);
        }
        colDataSetNULL(col, 1);
        ASSERT_TRUE(colDataIsNull_s(col, 1));
      }
      constexpr size_t kRows = 2;
      constexpr size_t kRowCountBytes = sizeof(uint32_t);
      constexpr size_t kColumnMetadataBytes = 2 * sizeof(uint8_t);
      constexpr size_t kColumnLengthBytes = sizeof(int32_t);
      constexpr size_t kIntValueBytes = kRows * sizeof(int32_t);
      const size_t kIntColumnBytes =
          kColumnMetadataBytes + BitmapLen(kRows) +
          kColumnLengthBytes + kIntValueBytes;
      // One non-NULL varchar stores a two-byte header and the two-byte "xy".
      const size_t kVarColumnBytes =
          kColumnMetadataBytes + kRows * sizeof(int32_t) +
          kColumnLengthBytes + 4;
      // noData remains a full writer-side column. It only controls whether
      // a reader consumes that column from the serialized layout.
      const size_t expectedSize =
          kRowCountBytes +
          (type == TSDB_DATA_TYPE_INT
               ? 3 * kIntColumnBytes
               : 2 * kIntColumnBytes + kVarColumnBytes);
      const size_t size = blockDataGetBufSize(pBlock);
      ASSERT_EQ(size, expectedSize);
      std::vector<char> buf(size + 1, '\x5a');
      ASSERT_EQ(blockDataToBuf(buf.data(), pBlock), TSDB_CODE_SUCCESS);
      EXPECT_EQ(buf[size], '\x5a');
      SSDataBlock* pDecode = nullptr;
      ASSERT_EQ(createOneDataBlock(pBlock, false, &pDecode),
                TSDB_CODE_SUCCESS);
      for (int32_t i = 0; i < 3; ++i) {
        auto col = (SColumnInfoData*)taosArrayGet(pDecode->pDataBlock, i);
        col->info.noData = false;
      }
      for (size_t length = 0; length < size; ++length) {
        EXPECT_EQ(blockDataFromBufBounded(pDecode, buf.data(), length),
                  TSDB_CODE_FILE_CORRUPTED);
      }
      EXPECT_EQ(blockDataFromBufBounded(pDecode, buf.data(), size + 1),
                TSDB_CODE_FILE_CORRUPTED);
      blockDataCleanup(pDecode);
      ASSERT_EQ(blockDataFromBufBounded(pDecode, buf.data(), size),
                TSDB_CODE_SUCCESS);
      EXPECT_EQ(pDecode->info.rows, 2);
      for (int32_t i = 0; i < 3; ++i) {
        auto col = (SColumnInfoData*)taosArrayGet(pDecode->pDataBlock, i);
        EXPECT_EQ(col->info.precision, i);
        EXPECT_EQ(col->info.scale, i + 1);
        EXPECT_FALSE(col->info.noData);
        if (col->info.type == TSDB_DATA_TYPE_VARCHAR) {
          EXPECT_EQ(varDataLen(colDataGetData(col, 0)), 2);
          EXPECT_EQ(memcmp(varDataVal(colDataGetData(col, 0)), "xy", 2), 0);
        } else {
          EXPECT_EQ(*(int32_t*)colDataGetData(col, 0), 123 + i);
        }
        if (IS_VAR_DATA_TYPE(col->info.type)) {
          EXPECT_FALSE(colDataIsNull_s(col, 0));
          EXPECT_TRUE(colDataIsNull_s(col, 1));
        } else {
          EXPECT_FALSE(colDataIsNull_f(col, 0));
          EXPECT_TRUE(colDataIsNull_f(col, 1));
        }
      }
      blockDataDestroy(pDecode);
      blockDataDestroy(pBlock);
    }
  }
}

TEST(blockDataFromBufBoundedTest, rejectsCorruptVarBlocks) {
  SSDataBlock* pBlock = NULL;
  ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
  SColumnInfoData column = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 24, 1);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &column), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pBlock, 2), TSDB_CODE_SUCCESS);

  SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, 0);
  ASSERT_NE(pCol, nullptr);
  char record[16] = {0};
  varDataSetLen(record, 5);
  memcpy(varDataVal(record), "hello", 5);
  ASSERT_EQ(colDataSetVal(pCol, 0, record, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataSetVal(pCol, 1, record, false), TSDB_CODE_SUCCESS);
  pBlock->info.rows = 2;

  // Layout:
  // rows(4) | precision+scale(2) | offsets[2](8) | colLength(4) | payload(2*7)
  // Spelled out from the layout on purpose; agreeing with the helper pins
  // blockDataGetBufSize to the documented format.
  int32_t bufSize = (int32_t)(sizeof(uint32_t) + 2 * sizeof(uint8_t) +
                              sizeof(int32_t) + blockDataGetSize(pBlock));
  ASSERT_EQ(bufSize, (int32_t)blockDataGetBufSize(pBlock));
  char serial[64] = {0};
  ASSERT_LE(bufSize, (int32_t)sizeof(serial));
  ASSERT_EQ(blockDataToBuf(serial, pBlock), TSDB_CODE_SUCCESS);

  SSDataBlock* pDecode = NULL;
  ASSERT_EQ(createOneDataBlock(pBlock, false, &pDecode), TSDB_CODE_SUCCESS);

  EXPECT_EQ(blockDataFromBufBounded(pDecode, serial, bufSize),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(blockDataFromBufBounded(pDecode, serial, bufSize - 1),
            TSDB_CODE_FILE_CORRUPTED);
  // bufSize is the exact serialized length: trailing bytes are corruption.
  EXPECT_EQ(blockDataFromBufBounded(pDecode, serial, bufSize + 1),
            TSDB_CODE_FILE_CORRUPTED);

  // Row 1 redirected to an offset whose var header crosses the payload end:
  // in bounds by itself, so a start-only offset check would accept it.
  char corrupt[64] = {0};
  memcpy(corrupt, serial, sizeof(serial));
  size_t  colStart = sizeof(uint32_t) + 2 * sizeof(uint8_t);
  int32_t colLength = 0;
  memcpy(&colLength, corrupt + colStart + 2 * sizeof(int32_t),
         sizeof(colLength));
  int32_t evilOffset = colLength - 1;
  memcpy(corrupt + colStart + sizeof(int32_t), &evilOffset,
         sizeof(evilOffset));
  EXPECT_EQ(blockDataFromBufBounded(pDecode, corrupt, bufSize),
            TSDB_CODE_FILE_CORRUPTED);

  // Negative column length must never reach the decoder's realloc/memcpy.
  memcpy(corrupt, serial, sizeof(serial));
  int32_t negLength = -1;
  memcpy(corrupt + colStart + 2 * sizeof(int32_t), &negLength,
         sizeof(negLength));
  EXPECT_EQ(blockDataFromBufBounded(pDecode, corrupt, bufSize),
            TSDB_CODE_FILE_CORRUPTED);

  // The full writer layout cannot be parsed with a target schema that skips
  // a column because every later column would start at the wrong offset.
  SColumnInfoData* pDecodeCol =
      (SColumnInfoData*)taosArrayGet(pDecode->pDataBlock, 0);
  ASSERT_NE(pDecodeCol, nullptr);
  pDecodeCol->info.noData = true;
  EXPECT_EQ(blockDataFromBufBounded(pDecode, serial, bufSize),
            TSDB_CODE_INVALID_PARA);
  pDecodeCol->info.noData = false;

  blockDataDestroy(pDecode);
  blockDataDestroy(pBlock);
}

TEST(blockDataFromBufBoundedTest, rejectsOversizedBlobHeader) {
  SSDataBlock* pBlock = NULL;
  ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
  SColumnInfoData column = createColumnInfoData(TSDB_DATA_TYPE_BLOB, 24, 1);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &column), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pBlock, 1), TSDB_CODE_SUCCESS);

  SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, 0);
  ASSERT_NE(pCol, nullptr);
  char record[16] = {0};
  blobDataSetLen(record, 5);
  memcpy(blobDataVal(record), "hello", 5);
  ASSERT_EQ(colDataSetVal(pCol, 0, record, false), TSDB_CODE_SUCCESS);
  pBlock->info.rows = 1;

  // Layout: rows(4) | precision+scale(2) | offsets[1](4) | colLength(4) |
  // payload(4 + 5). Spelled out on purpose; see rejectsCorruptVarBlocks.
  int32_t bufSize = (int32_t)(sizeof(uint32_t) + 2 * sizeof(uint8_t) +
                              sizeof(int32_t) + blockDataGetSize(pBlock));
  ASSERT_EQ(bufSize, (int32_t)blockDataGetBufSize(pBlock));
  char serial[64] = {0};
  ASSERT_LE(bufSize, (int32_t)sizeof(serial));
  ASSERT_EQ(blockDataToBuf(serial, pBlock), TSDB_CODE_SUCCESS);

  SSDataBlock* pDecode = NULL;
  ASSERT_EQ(createOneDataBlock(pBlock, false, &pDecode), TSDB_CODE_SUCCESS);
  EXPECT_EQ(blockDataFromBufBounded(pDecode, serial, bufSize),
            TSDB_CODE_SUCCESS);

  // A corrupt uint32 blob length whose total record size wraps int32
  // (UINT32_MAX + 4 == 3) must not slip past the bounded validator.
  char corrupt[64] = {0};
  memcpy(corrupt, serial, sizeof(serial));
  size_t   colStart = sizeof(uint32_t) + 2 * sizeof(uint8_t);
  uint32_t evilLen = UINT32_MAX;
  memcpy(corrupt + colStart + 2 * sizeof(int32_t), &evilLen,
         sizeof(evilLen));
  EXPECT_EQ(blockDataFromBufBounded(pDecode, corrupt, bufSize),
            TSDB_CODE_FILE_CORRUPTED);

  blockDataDestroy(pDecode);
  blockDataDestroy(pBlock);
}

// Serializes pBlock and decodes it into a block whose hasNull flags are
// preset to staleValue, mimicking a target block reused across reads.
static void roundTripIntoStaleBlock(SSDataBlock* pBlock, bool staleValue,
                                    SSDataBlock** ppDecode) {
  int32_t numCols = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
  int32_t bufSize = (int32_t)blockDataGetBufSize(pBlock);
  std::vector<char> serial(bufSize, 0);
  ASSERT_EQ(blockDataToBuf(serial.data(), pBlock), TSDB_CODE_SUCCESS);

  SSDataBlock* pDecode = NULL;
  ASSERT_EQ(createOneDataBlock(pBlock, false, &pDecode), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pDecode, pBlock->info.rows),
            TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < numCols; ++i) {
    SColumnInfoData* pCol =
        (SColumnInfoData*)taosArrayGet(pDecode->pDataBlock, i);
    ASSERT_NE(pCol, nullptr);
    pCol->hasNull = staleValue;
  }

  ASSERT_EQ(blockDataFromBufBounded(pDecode, serial.data(), bufSize),
            TSDB_CODE_SUCCESS);
  *ppDecode = pDecode;
}

TEST(blockDataFromBufBoundedTest, restoresHasNullFromMetadata) {
  SSDataBlock* pBlock = NULL;
  ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
  SColumnInfoData intCol =
      createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  SColumnInfoData strCol = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 24, 2);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &intCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &strCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pBlock, 3), TSDB_CODE_SUCCESS);

  SColumnInfoData* pInt = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, 0);
  SColumnInfoData* pStr = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, 1);
  ASSERT_NE(pInt, nullptr);
  ASSERT_NE(pStr, nullptr);

  int32_t value = 7;
  char    record[16] = {0};
  varDataSetLen(record, 5);
  memcpy(varDataVal(record), "hello", 5);

  // Row 1 is NULL in both a fixed and a var column; rows 0 and 2 are not.
  for (int32_t r = 0; r < 3; ++r) {
    bool isNull = (r == 1);
    ASSERT_EQ(colDataSetVal(pInt, r, (const char*)&value, isNull),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(colDataSetVal(pStr, r, record, isNull), TSDB_CODE_SUCCESS);
  }
  pBlock->info.rows = 3;

  // A stale hasNull=false is what made the spill read path report NULLs as
  // ordinary values; decoding must derive the flag from the metadata.
  SSDataBlock* pDecode = NULL;
  roundTripIntoStaleBlock(pBlock, false, &pDecode);
  ASSERT_NE(pDecode, nullptr);

  SColumnInfoData* pDecInt =
      (SColumnInfoData*)taosArrayGet(pDecode->pDataBlock, 0);
  SColumnInfoData* pDecStr =
      (SColumnInfoData*)taosArrayGet(pDecode->pDataBlock, 1);
  ASSERT_NE(pDecInt, nullptr);
  ASSERT_NE(pDecStr, nullptr);
  EXPECT_TRUE(pDecInt->hasNull);
  EXPECT_TRUE(pDecStr->hasNull);
  for (int32_t r = 0; r < 3; ++r) {
    EXPECT_EQ(colDataIsNull_s(pDecInt, r), r == 1);
    EXPECT_EQ(colDataIsNull_s(pDecStr, r), r == 1);
  }
  blockDataDestroy(pDecode);

  // And a stale true must be cleared when the data carries no NULL, so
  // NULL-free blocks keep their fast paths.
  for (int32_t r = 0; r < 3; ++r) {
    ASSERT_EQ(colDataSetVal(pInt, r, (const char*)&value, false),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(colDataSetVal(pStr, r, record, false), TSDB_CODE_SUCCESS);
  }
  pDecode = NULL;
  roundTripIntoStaleBlock(pBlock, true, &pDecode);
  ASSERT_NE(pDecode, nullptr);
  pDecInt = (SColumnInfoData*)taosArrayGet(pDecode->pDataBlock, 0);
  pDecStr = (SColumnInfoData*)taosArrayGet(pDecode->pDataBlock, 1);
  EXPECT_FALSE(pDecInt->hasNull);
  EXPECT_FALSE(pDecStr->hasNull);

  blockDataDestroy(pDecode);
  blockDataDestroy(pBlock);
}

TEST(blockDataFromBufBoundedTest, allNullVarColumnIsNotDereferenced) {
  SSDataBlock* pBlock = NULL;
  ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
  SColumnInfoData strCol = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 24, 1);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &strCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pBlock, 2), TSDB_CODE_SUCCESS);

  SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, 0);
  ASSERT_NE(pCol, nullptr);
  char record[16] = {0};
  varDataSetLen(record, 5);
  memcpy(varDataVal(record), "hello", 5);
  ASSERT_EQ(colDataSetVal(pCol, 0, record, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataSetVal(pCol, 1, record, true), TSDB_CODE_SUCCESS);
  pBlock->info.rows = 2;

  // An all-NULL var column serializes no payload at all, so the decoded
  // column keeps pData == NULL. A stale hasNull=false would short-circuit
  // colDataIsNull_s past the pData guard in colDataIsNull_var, and the
  // caller would then read colDataGetVarData -> (char*)-1.
  SSDataBlock* pDecode = NULL;
  roundTripIntoStaleBlock(pBlock, false, &pDecode);
  ASSERT_NE(pDecode, nullptr);

  SColumnInfoData* pDecCol =
      (SColumnInfoData*)taosArrayGet(pDecode->pDataBlock, 0);
  ASSERT_NE(pDecCol, nullptr);
  EXPECT_EQ(pDecCol->varmeta.length, 0);
  EXPECT_TRUE(pDecCol->hasNull);
  EXPECT_TRUE(colDataIsNull_s(pDecCol, 0));
  EXPECT_TRUE(colDataIsNull_s(pDecCol, 1));

  blockDataDestroy(pDecode);
  blockDataDestroy(pBlock);
}
