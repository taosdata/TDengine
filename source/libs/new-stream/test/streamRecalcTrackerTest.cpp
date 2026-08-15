/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>

#include <climits>
#include <initializer_list>
#include <memory>

#include "streamRecalcTracker.h"
#include "taoserror.h"

namespace {

struct ArrayDeleter {
  void operator()(SArray *pArray) const { taosArrayDestroy(pArray); }
};

struct TrackerDeleter {
  void operator()(SStreamRecalcTracker *pTracker) const { stRecalcTrackerDestroy(&pTracker); }
};

using ArrayPtr = std::unique_ptr<SArray, ArrayDeleter>;
using TrackerPtr = std::unique_ptr<SStreamRecalcTracker, TrackerDeleter>;

ArrayPtr MakeGroups(std::initializer_list<int64_t> gids) {
  ArrayPtr groups{taosArrayInit(gids.size(), sizeof(int64_t))};
  if (groups == nullptr) return groups;
  for (int64_t gid : gids) {
    if (taosArrayPush(groups.get(), &gid) == nullptr) return nullptr;
  }
  return groups;
}

TrackerPtr MakeTracker() {
  SStreamRecalcTracker *tracker = nullptr;
  if (stRecalcTrackerCreate(&tracker) != TSDB_CODE_SUCCESS) return nullptr;
  return TrackerPtr{tracker};
}

ArrayPtr MakeContributors(SStreamRecalcTracker *tracker, std::initializer_list<SStreamRecalcContributor> values) {
  SArray *contributors = nullptr;
  for (const auto &value : values) {
    if (stRecalcContributorsAdd(tracker, &contributors, value.recalcId, value.requestedRange) != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(contributors);
      return nullptr;
    }
  }
  return ArrayPtr{contributors};
}

ArrayPtr CopySnapshots(SStreamRecalcTracker *tracker, bool *historyValid = nullptr, int32_t *historyPct = nullptr) {
  bool    localHistoryValid = false;
  int32_t localHistoryPct = 0;
  SArray *snapshots = nullptr;
  if (stRecalcTrackerCopySnapshot(tracker, historyValid == nullptr ? &localHistoryValid : historyValid,
                                  historyPct == nullptr ? &localHistoryPct : historyPct,
                                  &snapshots) != TSDB_CODE_SUCCESS) {
    return nullptr;
  }
  return ArrayPtr{snapshots};
}

const SStreamRecalcSnapshot *FindSnapshot(const SArray *snapshots, int64_t recalcId) {
  for (size_t i = 0; i < taosArrayGetSize(snapshots); ++i) {
    const auto *snapshot = static_cast<const SStreamRecalcSnapshot *>(taosArrayGet(snapshots, i));
    if (snapshot->recalcId == recalcId) return snapshot;
  }
  return nullptr;
}

int64_t ActiveJobCount(SStreamRecalcTracker *tracker) {
  int64_t activeJobCount = -1;
  bool    historyValid = false;
  int32_t historyProgressPct = -1;
  if (stRecalcTrackerGetDebugGauges(tracker, &activeJobCount, &historyValid, &historyProgressPct) !=
      TSDB_CODE_SUCCESS) {
    return -1;
  }
  return activeJobCount;
}

}  // namespace

TEST(StreamRecalcTrackerTest, TwoGroupsUseWeightedProgress) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10, 20});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 7, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ActiveJobCount(tracker.get()), 1);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 7), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ActiveJobCount(tracker.get()), 1);
  auto contributors = MakeContributors(tracker.get(), {{7, 0, {100, 200}}});
  ASSERT_NE(contributors, nullptr);

  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, contributors.get(), &stepId), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ActiveJobCount(tracker.get()), 1);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ActiveJobCount(tracker.get()), 1);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  ASSERT_EQ(taosArrayGetSize(snapshots.get()), 1);
  const auto *first = FindSnapshot(snapshots.get(), 7);
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->progressPct, 50);
  EXPECT_EQ(first->status, STREAM_RECALC_STATUS_RUNNING);

  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 20, {100, 200}, contributors.get(), &stepId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ActiveJobCount(tracker.get()), 0);
  snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *finished = FindSnapshot(snapshots.get(), 7);
  ASSERT_NE(finished, nullptr);
  EXPECT_EQ(finished->progressPct, 100);
  EXPECT_EQ(finished->status, STREAM_RECALC_STATUS_FINISHED);
}

TEST(StreamRecalcTrackerTest, EmptyGroupSnapshotFinishesImmediately) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({});
  ASSERT_NE(groups, nullptr);

  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 8, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ActiveJobCount(tracker.get()), 0);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *snapshot = FindSnapshot(snapshots.get(), 8);
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->progressPct, 100);
  EXPECT_EQ(snapshot->status, STREAM_RECALC_STATUS_FINISHED);
}

TEST(StreamRecalcTrackerTest, SameIdSameRangeIsIdempotent) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 9, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);

  int64_t laterGroup = 20;
  ASSERT_NE(taosArrayPush(groups.get(), &laterGroup), nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 9, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 9), TSDB_CODE_SUCCESS);
  auto contributors = MakeContributors(tracker.get(), {{9, 0, {100, 200}}});
  ASSERT_NE(contributors, nullptr);
  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, contributors.get(), &stepId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);

  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *snapshot = FindSnapshot(snapshots.get(), 9);
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->progressPct, 100);
  EXPECT_EQ(snapshot->status, STREAM_RECALC_STATUS_FINISHED);
}

TEST(StreamRecalcTrackerTest, SameIdDifferentRangeIsRejected) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);

  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 10, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  EXPECT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 10, {100, 201}, groups.get()), TSDB_CODE_INVALID_MSG);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *snapshot = FindSnapshot(snapshots.get(), 10);
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->start, 100);
  EXPECT_EQ(snapshot->end, 200);
  EXPECT_EQ(snapshot->status, STREAM_RECALC_STATUS_PENDING);
}

TEST(StreamRecalcTrackerTest, MergedGapDoesNotCountForEitherJob) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 11, {100, 150}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 12, {200, 250}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 11), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 12), TSDB_CODE_SUCCESS);

  auto left = MakeContributors(tracker.get(), {{11, 0, {100, 150}}});
  auto right = MakeContributors(tracker.get(), {{12, 0, {200, 250}}, {12, 0, {200, 250}}});
  ASSERT_NE(left, nullptr);
  ASSERT_NE(right, nullptr);
  SArray *merged = left.get();
  ASSERT_EQ(stRecalcContributorsMerge(&merged, right.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcContributorsMerge(&merged, right.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(merged, left.get());
  ASSERT_EQ(taosArrayGetSize(merged), 2);

  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 225}, merged, &stepId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *first = FindSnapshot(snapshots.get(), 11);
  const auto *second = FindSnapshot(snapshots.get(), 12);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(first->progressPct, 100);
  EXPECT_EQ(second->progressPct, 50);
}

TEST(StreamRecalcTrackerTest, StartedStepCannotGainContributor) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 13, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 14, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 13), TSDB_CODE_SUCCESS);

  auto contributors = MakeContributors(tracker.get(), {{13, 0, {100, 200}}});
  ASSERT_NE(contributors, nullptr);
  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, contributors.get(), &stepId), TSDB_CODE_SUCCESS);
  SArray *contributorsRaw = contributors.get();
  ASSERT_EQ(stRecalcContributorsAdd(tracker.get(), &contributorsRaw, 14, {100, 200}), TSDB_CODE_SUCCESS);
  ASSERT_EQ(contributorsRaw, contributors.get());
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);

  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *started = FindSnapshot(snapshots.get(), 13);
  const auto *late = FindSnapshot(snapshots.get(), 14);
  ASSERT_NE(started, nullptr);
  ASSERT_NE(late, nullptr);
  EXPECT_EQ(started->status, STREAM_RECALC_STATUS_FINISHED);
  EXPECT_EQ(started->progressPct, 100);
  EXPECT_EQ(late->status, STREAM_RECALC_STATUS_PENDING);
  EXPECT_EQ(late->progressPct, 0);
}

TEST(StreamRecalcTrackerTest, BeginStepMarksPendingContributorRunning) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 24, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  auto contributors = MakeContributors(tracker.get(), {{24, 0, {100, 200}}});
  ASSERT_NE(contributors, nullptr);

  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 150}, contributors.get(), &stepId), TSDB_CODE_SUCCESS);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *snapshot = FindSnapshot(snapshots.get(), 24);
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(snapshot->progressPct, 0);
}

TEST(StreamRecalcTrackerTest, ReaderDoneWithoutRunnerDoneDoesNotCommit) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 15, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 15), TSDB_CODE_SUCCESS);
  auto contributors = MakeContributors(tracker.get(), {{15, 0, {100, 200}}});
  ASSERT_NE(contributors, nullptr);

  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, contributors.get(), &stepId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerAddReader(tracker.get(), stepId, 11), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerAddRunner(tracker.get(), stepId, 22), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCompleteReader(tracker.get(), stepId, 11), TSDB_CODE_SUCCESS);

  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *waiting = FindSnapshot(snapshots.get(), 15);
  ASSERT_NE(waiting, nullptr);
  EXPECT_EQ(waiting->progressPct, 0);
  EXPECT_EQ(waiting->status, STREAM_RECALC_STATUS_RUNNING);

  ASSERT_EQ(stRecalcTrackerCompleteRunner(tracker.get(), stepId, 22), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *finished = FindSnapshot(snapshots.get(), 15);
  ASSERT_NE(finished, nullptr);
  EXPECT_EQ(finished->progressPct, 100);
  EXPECT_EQ(finished->status, STREAM_RECALC_STATUS_FINISHED);
}

TEST(StreamRecalcTrackerTest, NoDataStepCommitsAfterTriggerDone) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 16, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 16), TSDB_CODE_SUCCESS);
  auto contributors = MakeContributors(tracker.get(), {{16, 0, {100, 200}}});
  ASSERT_NE(contributors, nullptr);

  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, contributors.get(), &stepId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerAddReader(tracker.get(), stepId, 31), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCompleteReader(tracker.get(), stepId, 31), TSDB_CODE_SUCCESS);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  ASSERT_EQ(FindSnapshot(snapshots.get(), 16)->progressPct, 0);

  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *finished = FindSnapshot(snapshots.get(), 16);
  ASSERT_NE(finished, nullptr);
  EXPECT_EQ(finished->progressPct, 100);
  EXPECT_EQ(finished->status, STREAM_RECALC_STATUS_FINISHED);
}

TEST(StreamRecalcTrackerTest, DuplicateCallbackCommitsOnce) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 17, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 17), TSDB_CODE_SUCCESS);
  auto contributors = MakeContributors(tracker.get(), {{17, 0, {100, 200}}});
  ASSERT_NE(contributors, nullptr);

  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 150}, contributors.get(), &stepId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerAddReader(tracker.get(), stepId, 42), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCompleteRunner(tracker.get(), stepId, 41), TSDB_CODE_INVALID_MSG);
  ASSERT_EQ(stRecalcTrackerAddRunner(tracker.get(), stepId, 41), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerAddRunner(tracker.get(), stepId, 41), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerAddRunner(tracker.get(), stepId, 44), TSDB_CODE_INVALID_MSG);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCompleteReader(tracker.get(), stepId, 42), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCompleteReader(tracker.get(), stepId, 42), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCompleteReader(tracker.get(), stepId, 43), TSDB_CODE_INVALID_MSG);
  ASSERT_EQ(stRecalcTrackerCompleteRunner(tracker.get(), stepId, 41), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCompleteRunner(tracker.get(), stepId, 41), TSDB_CODE_SUCCESS);

  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *snapshot = FindSnapshot(snapshots.get(), 17);
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->progressPct, 50);
  EXPECT_EQ(snapshot->status, STREAM_RECALC_STATUS_RUNNING);
}

TEST(StreamRecalcTrackerTest, DynamicReadersWaitForTriggerDoneAndAllRegisteredCallbacks) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 1700, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  auto contributors = MakeContributors(tracker.get(), {{1700, 0, {100, 200}}});
  ASSERT_NE(contributors, nullptr);

  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, contributors.get(), &stepId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerAddReader(tracker.get(), stepId, 51), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCompleteReader(tracker.get(), stepId, 51), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerAddReader(tracker.get(), stepId, 52), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCompleteReader(tracker.get(), stepId, 52), TSDB_CODE_SUCCESS);

  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 1700)->progressPct, 0);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 1700)->status, STREAM_RECALC_STATUS_RUNNING);

  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 1700)->progressPct, 100);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 1700)->status, STREAM_RECALC_STATUS_FINISHED);
}

TEST(StreamRecalcTrackerTest, ReaderMustBeRegisteredBeforeCallbackAndTriggerDone) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, nullptr, &stepId), TSDB_CODE_SUCCESS);

  EXPECT_EQ(stRecalcTrackerAddReader(tracker.get(), stepId, 0), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(stRecalcTrackerAddRunner(tracker.get(), stepId, 0), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(stRecalcTrackerCompleteRunner(tracker.get(), stepId, 0), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(stRecalcTrackerCompleteReader(tracker.get(), stepId, 61), TSDB_CODE_INVALID_MSG);
  ASSERT_EQ(stRecalcTrackerAddReader(tracker.get(), stepId, 61), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerAddReader(tracker.get(), stepId, 61), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(stRecalcTrackerAddReader(tracker.get(), stepId, 61), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(stRecalcTrackerAddReader(tracker.get(), stepId, 62), TSDB_CODE_INVALID_MSG);
  ASSERT_EQ(stRecalcTrackerCompleteReader(tracker.get(), stepId, 61), TSDB_CODE_SUCCESS);
  EXPECT_EQ(stRecalcTrackerAddReader(tracker.get(), stepId, 63), TSDB_CODE_NOT_FOUND);
  EXPECT_EQ(stRecalcTrackerCompleteReader(tracker.get(), stepId, 61), TSDB_CODE_SUCCESS);
}

TEST(StreamRecalcTrackerTest, RetiredStepsKeepLateCallbacksIdempotent) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  uint64_t firstStepId = 0;
  for (int32_t i = 0; i < 512; ++i) {
    uint64_t stepId = 0;
    ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, nullptr, &stepId), TSDB_CODE_SUCCESS);
    if (i == 0) firstStepId = stepId;
    ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  }

  EXPECT_EQ(stRecalcTrackerCompleteReader(tracker.get(), firstStepId, 1), TSDB_CODE_SUCCESS);
  EXPECT_EQ(stRecalcTrackerCompleteRunner(tracker.get(), firstStepId, 2), TSDB_CODE_SUCCESS);
  EXPECT_EQ(stRecalcTrackerFailStep(tracker.get(), firstStepId, TSDB_CODE_INTERNAL_ERROR), TSDB_CODE_SUCCESS);
  EXPECT_EQ(stRecalcTrackerCompleteReader(tracker.get(), 0, 1), TSDB_CODE_NOT_FOUND);
  EXPECT_EQ(stRecalcTrackerCompleteReader(tracker.get(), 513, 1), TSDB_CODE_NOT_FOUND);
}

TEST(StreamRecalcTrackerTest, RetryableFailureDoesNotFailJob) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 18, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 18), TSDB_CODE_SUCCESS);
  auto contributors = MakeContributors(tracker.get(), {{18, 0, {100, 200}}});
  ASSERT_NE(contributors, nullptr);

  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, contributors.get(), &stepId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerFailStep(tracker.get(), stepId, TSDB_CODE_NEED_RETRY), TSDB_CODE_SUCCESS);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  ASSERT_EQ(FindSnapshot(snapshots.get(), 18)->status, STREAM_RECALC_STATUS_RUNNING);

  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 18)->status, STREAM_RECALC_STATUS_FINISHED);
}

TEST(StreamRecalcTrackerTest, FatalSharedStepFailsOnlyActiveContributors) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  for (int64_t recalcId : {19, 20, 21}) {
    ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), recalcId, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
    ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), recalcId), TSDB_CODE_SUCCESS);
  }

  auto firstOnly = MakeContributors(tracker.get(), {{19, 0, {100, 200}}});
  ASSERT_NE(firstOnly, nullptr);
  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, firstOnly.get(), &stepId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);

  auto shared = MakeContributors(tracker.get(), {{19, 0, {100, 200}}, {20, 0, {100, 200}}});
  ASSERT_NE(shared, nullptr);
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, shared.get(), &stepId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerFailStep(tracker.get(), stepId, TSDB_CODE_INTERNAL_ERROR), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerFailStep(tracker.get(), stepId, TSDB_CODE_INTERNAL_ERROR), TSDB_CODE_SUCCESS);

  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 19)->status, STREAM_RECALC_STATUS_FINISHED);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 20)->status, STREAM_RECALC_STATUS_FAILED);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 21)->status, STREAM_RECALC_STATUS_RUNNING);
}

TEST(StreamRecalcTrackerTest, TerminalRetentionEvictsOldestAndKeepsActive) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 999, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 1000, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  auto failedContributor = MakeContributors(tracker.get(), {{1000, 0, {100, 200}}});
  ASSERT_NE(failedContributor, nullptr);
  uint64_t failedStepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, failedContributor.get(), &failedStepId),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerFailStep(tracker.get(), failedStepId, TSDB_CODE_INTERNAL_ERROR), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ActiveJobCount(tracker.get()), 1);

  for (int64_t recalcId = 1001; recalcId <= 1100; ++recalcId) {
    ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), recalcId, {100, 200}, nullptr), TSDB_CODE_SUCCESS);
  }
  EXPECT_EQ(ActiveJobCount(tracker.get()), 1);

  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(taosArrayGetSize(snapshots.get()), 101);
  EXPECT_NE(FindSnapshot(snapshots.get(), 999), nullptr);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 1000), nullptr);
  EXPECT_NE(FindSnapshot(snapshots.get(), 1001), nullptr);
  EXPECT_NE(FindSnapshot(snapshots.get(), 1100), nullptr);

  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 1000, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ActiveJobCount(tracker.get()), 2);
  snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  ASSERT_EQ(taosArrayGetSize(snapshots.get()), 102);
  const auto *replayed = FindSnapshot(snapshots.get(), 1000);
  ASSERT_NE(replayed, nullptr);
  EXPECT_EQ(replayed->status, STREAM_RECALC_STATUS_PENDING);
}

TEST(StreamRecalcTrackerTest, EvictedIdReplayIsIsolatedFromOldActiveSteps) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 1200, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  auto oldContributor = MakeContributors(tracker.get(), {{1200, 0, {100, 200}}});
  ASSERT_NE(oldContributor, nullptr);

  uint64_t staleCommitStep = 0;
  uint64_t staleFatalStep = 0;
  uint64_t terminalStep = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, oldContributor.get(), &staleCommitStep),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, oldContributor.get(), &staleFatalStep),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, oldContributor.get(), &terminalStep),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerFailStep(tracker.get(), terminalStep, TSDB_CODE_INTERNAL_ERROR), TSDB_CODE_SUCCESS);

  for (int64_t recalcId = 1201; recalcId <= 1300; ++recalcId) {
    ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), recalcId, {100, 200}, nullptr), TSDB_CODE_SUCCESS);
  }
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 1200, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);

  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), staleCommitStep, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerFailStep(tracker.get(), staleFatalStep, TSDB_CODE_INTERNAL_ERROR), TSDB_CODE_SUCCESS);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *replayed = FindSnapshot(snapshots.get(), 1200);
  ASSERT_NE(replayed, nullptr);
  EXPECT_EQ(replayed->status, STREAM_RECALC_STATUS_PENDING);
  EXPECT_EQ(replayed->progressPct, 0);

  auto newContributor = MakeContributors(tracker.get(), {{1200, 0, {100, 200}}});
  ASSERT_NE(newContributor, nullptr);
  uint64_t newStep = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, newContributor.get(), &newStep), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), newStep, 0), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  replayed = FindSnapshot(snapshots.get(), 1200);
  ASSERT_NE(replayed, nullptr);
  EXPECT_EQ(replayed->status, STREAM_RECALC_STATUS_FINISHED);
  EXPECT_EQ(replayed->progressPct, 100);

  SArray *terminals = nullptr;
  ASSERT_EQ(stRecalcTrackerTakeTerminalEvents(tracker.get(), &terminals), TSDB_CODE_SUCCESS);
  ArrayPtr ownedTerminals{terminals};
  bool     foundCurrentGeneration = false;
  for (size_t i = 0; i < taosArrayGetSize(ownedTerminals.get()); ++i) {
    const auto *terminal = static_cast<const SStreamRecalcDebugSnapshot *>(taosArrayGet(ownedTerminals.get(), i));
    if (terminal->snapshot.recalcId == 1200) foundCurrentGeneration = true;
  }
  EXPECT_TRUE(foundCurrentGeneration);
}

TEST(StreamRecalcTrackerTest, EvictedIdRejectsOldQueuedContributor) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 1400, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  auto oldQueued = MakeContributors(tracker.get(), {{1400, 0, {100, 200}}});
  auto terminalContributor = MakeContributors(tracker.get(), {{1400, 0, {100, 200}}});
  ASSERT_NE(oldQueued, nullptr);
  ASSERT_NE(terminalContributor, nullptr);

  uint64_t terminalStep = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, terminalContributor.get(), &terminalStep),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerFailStep(tracker.get(), terminalStep, TSDB_CODE_INTERNAL_ERROR), TSDB_CODE_SUCCESS);
  for (int64_t recalcId = 1401; recalcId <= 1500; ++recalcId) {
    ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), recalcId, {100, 200}, nullptr), TSDB_CODE_SUCCESS);
  }
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 1400, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);

  uint64_t staleStep = 0;
  EXPECT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, oldQueued.get(), &staleStep),
            TSDB_CODE_INVALID_MSG);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *replayed = FindSnapshot(snapshots.get(), 1400);
  ASSERT_NE(replayed, nullptr);
  EXPECT_EQ(replayed->status, STREAM_RECALC_STATUS_PENDING);
  EXPECT_EQ(replayed->progressPct, 0);

  auto current = MakeContributors(tracker.get(), {{1400, 0, {100, 200}}});
  ASSERT_NE(current, nullptr);
  SArray *currentRaw = current.release();
  EXPECT_EQ(stRecalcContributorsMerge(&currentRaw, oldQueued.get()), TSDB_CODE_INVALID_MSG);
  current.reset(currentRaw);
  uint64_t currentStep = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {100, 200}, current.get(), &currentStep), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), currentStep, 0), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  replayed = FindSnapshot(snapshots.get(), 1400);
  ASSERT_NE(replayed, nullptr);
  EXPECT_EQ(replayed->status, STREAM_RECALC_STATUS_FINISHED);
  EXPECT_EQ(replayed->progressPct, 100);
}

TEST(StreamRecalcTrackerTest, FailPendingJobBeforeStepIsRetainedAndIdempotent) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 1500, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ActiveJobCount(tracker.get()), 1);

  ASSERT_EQ(stRecalcTrackerFailJob(tracker.get(), 1500, TSDB_CODE_OUT_OF_MEMORY), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ActiveJobCount(tracker.get()), 0);
  ASSERT_EQ(stRecalcTrackerFailJob(tracker.get(), 1500, TSDB_CODE_OUT_OF_MEMORY), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ActiveJobCount(tracker.get()), 0);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  ASSERT_EQ(taosArrayGetSize(snapshots.get()), 1);
  const auto *snapshot = static_cast<const SStreamRecalcSnapshot *>(taosArrayGet(snapshots.get(), 0));
  EXPECT_EQ(snapshot->recalcId, 1500);
  EXPECT_EQ(snapshot->status, STREAM_RECALC_STATUS_FAILED);
  EXPECT_EQ(snapshot->progressPct, 0);
}

TEST(StreamRecalcTrackerTest, FailJobRejectsUnknownSuccessAndNonPendingStates) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);

  EXPECT_EQ(stRecalcTrackerFailJob(tracker.get(), 1600, TSDB_CODE_OUT_OF_MEMORY), TSDB_CODE_NOT_FOUND);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 1600, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  EXPECT_EQ(stRecalcTrackerFailJob(tracker.get(), 1600, TSDB_CODE_SUCCESS), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 1600), TSDB_CODE_SUCCESS);
  EXPECT_EQ(stRecalcTrackerFailJob(tracker.get(), 1600, TSDB_CODE_OUT_OF_MEMORY), TSDB_CODE_INVALID_MSG);

  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 1601, {100, 100}, groups.get()), TSDB_CODE_SUCCESS);
  EXPECT_EQ(stRecalcTrackerFailJob(tracker.get(), 1601, TSDB_CODE_OUT_OF_MEMORY), TSDB_CODE_INVALID_MSG);
}

TEST(StreamRecalcTrackerTest, UnfinishedProgressIsCappedAtNinetyNine) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 22, {TSKEY_MIN, TSKEY_MAX}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 22), TSDB_CODE_SUCCESS);
  auto contributors = MakeContributors(tracker.get(), {{22, 0, {TSKEY_MIN, TSKEY_MAX}}});
  ASSERT_NE(contributors, nullptr);

  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {TSKEY_MIN, TSKEY_MAX - 1}, contributors.get(), &stepId),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  const auto *snapshot = FindSnapshot(snapshots.get(), 22);
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->progressPct, 99);
  EXPECT_EQ(snapshot->status, STREAM_RECALC_STATUS_RUNNING);
}

TEST(StreamRecalcTrackerTest, WideRangeMathDoesNotOverflow) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10, 20, 30});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 23, {TSKEY_MIN, TSKEY_MAX}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 23), TSDB_CODE_SUCCESS);
  auto contributors = MakeContributors(tracker.get(), {{23, 0, {TSKEY_MIN, TSKEY_MAX}}});
  ASSERT_NE(contributors, nullptr);

  uint64_t stepId = 0;
  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 10, {TSKEY_MIN, TSKEY_MAX}, contributors.get(), &stepId),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  auto snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  ASSERT_EQ(FindSnapshot(snapshots.get(), 23)->progressPct, 33);

  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 20, {TSKEY_MIN, TSKEY_MAX}, contributors.get(), &stepId),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 23)->progressPct, 66);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 23)->status, STREAM_RECALC_STATUS_RUNNING);

  ASSERT_EQ(stRecalcTrackerBeginStep(tracker.get(), 30, {TSKEY_MIN, TSKEY_MAX}, contributors.get(), &stepId),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerSetTriggerDone(tracker.get(), stepId, 0), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(tracker.get());
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 23)->progressPct, 100);
  EXPECT_EQ(FindSnapshot(snapshots.get(), 23)->status, STREAM_RECALC_STATUS_FINISHED);
}

TEST(StreamRecalcTrackerTest, DisabledHistoryIsInvalid) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), false, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), false, {100, 200}, false), TSDB_CODE_SUCCESS);
  bool    historyValid = true;
  int32_t historyPct = 77;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_FALSE(historyValid);
  EXPECT_EQ(historyPct, 0);
}

TEST(StreamRecalcTrackerTest, FillHistoryDisabledIsInvalid) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), false, {100, 200}, false), TSDB_CODE_SUCCESS);

  bool    historyValid = true;
  int32_t historyPct = -1;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_FALSE(historyValid);
  EXPECT_EQ(historyPct, 0);
}

TEST(StreamRecalcTrackerTest, FillHistoryStartsAtZero) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);

  bool    historyValid = false;
  int32_t historyPct = -1;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 0);
}

TEST(StreamRecalcTrackerTest, FirstTsConfirmedPrefixCountsAsComplete) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerConfirmHistoryPrefix(tracker.get(), 140), TSDB_CODE_SUCCESS);

  bool    historyValid = false;
  int32_t historyPct = -1;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 40);
}

TEST(StreamRecalcTrackerTest, AllReadersNoDataFinishesAtOneHundred) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerConfirmHistoryPrefix(tracker.get(), 200), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCommitHistoryThrough(tracker.get(), 200, true), TSDB_CODE_SUCCESS);

  bool    historyValid = false;
  int32_t historyPct = -1;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 100);
}

TEST(StreamRecalcTrackerTest, OnePendingGroupKeepsGlobalFrontier) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCommitHistoryThrough(tracker.get(), 150, false), TSDB_CODE_SUCCESS);

  bool    historyValid = false;
  int32_t historyPct = -1;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(historyPct, 50);

  snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(historyPct, 50);
}

TEST(StreamRecalcTrackerTest, PendingRunnerKeepsStepUncommitted) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCommitHistoryThrough(tracker.get(), 180, false), TSDB_CODE_SUCCESS);

  bool    historyValid = false;
  int32_t historyPct = -1;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 80);
}

TEST(StreamRecalcTrackerTest, ForcedTailWindowsKeepProgressAtNinetyNine) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCommitHistoryThrough(tracker.get(), 200, false), TSDB_CODE_SUCCESS);

  bool    historyValid = false;
  int32_t historyPct = -1;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 99);
}

TEST(StreamRecalcTrackerTest, UnfinishedRedeployRestartsAtZero) {
  auto first = MakeTracker();
  ASSERT_NE(first, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(first.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCommitHistoryThrough(first.get(), 175, false), TSDB_CODE_SUCCESS);

  auto redeployed = MakeTracker();
  ASSERT_NE(redeployed, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(redeployed.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  bool    historyValid = false;
  int32_t historyPct = -1;
  auto    snapshots = CopySnapshots(redeployed.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 0);
}

TEST(StreamRecalcTrackerTest, CheckpointHistoryFinishedRestoresOneHundred) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, true), TSDB_CODE_SUCCESS);

  bool    historyValid = false;
  int32_t historyPct = -1;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 100);
}

TEST(StreamRecalcTrackerTest, HistoryConfirmedPrefixAdvancesSafeFrontier) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerConfirmHistoryPrefix(tracker.get(), 125), TSDB_CODE_SUCCESS);
  bool    historyValid = false;
  int32_t historyPct = 0;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 25);

  ASSERT_EQ(stRecalcTrackerConfirmHistoryPrefix(tracker.get(), 110), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(historyPct, 25);
}

TEST(StreamRecalcTrackerTest, ReinitializingSameHistoryPreservesProgress) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCommitHistoryThrough(tracker.get(), 150, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);

  bool    historyValid = false;
  int32_t historyPct = 0;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 50);

  EXPECT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 201}, false), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, true), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(stRecalcTrackerInitHistory(tracker.get(), false, {100, 200}, false), TSDB_CODE_INVALID_MSG);
}

TEST(StreamRecalcTrackerTest, HistoryTerminalBarrierCapsAtNinetyNine) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCommitHistoryThrough(tracker.get(), 200, false), TSDB_CODE_SUCCESS);
  bool    historyValid = false;
  int32_t historyPct = 0;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 99);

  ASSERT_EQ(stRecalcTrackerCommitHistoryThrough(tracker.get(), 150, true), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(historyPct, 100);
}

TEST(StreamRecalcTrackerTest, ConfirmedHistoryEndStillNeedsTerminalBarrier) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerConfirmHistoryPrefix(tracker.get(), 200), TSDB_CODE_SUCCESS);

  bool    historyValid = false;
  int32_t historyPct = 0;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 99);

  ASSERT_EQ(stRecalcTrackerCommitHistoryThrough(tracker.get(), 200, true), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_EQ(historyPct, 100);
}

TEST(StreamRecalcTrackerTest, PartialHistoryFrontierCannotFinish) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(tracker.get(), true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCommitHistoryThrough(tracker.get(), 150, true), TSDB_CODE_SUCCESS);
  bool    historyValid = false;
  int32_t historyPct = 0;
  auto    snapshots = CopySnapshots(tracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 50);
}

TEST(StreamRecalcTrackerTest, EmptyAndCheckpointedHistoryAreComplete) {
  auto emptyTracker = MakeTracker();
  ASSERT_NE(emptyTracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(emptyTracker.get(), true, {200, 200}, false), TSDB_CODE_SUCCESS);
  bool    historyValid = false;
  int32_t historyPct = 0;
  auto    snapshots = CopySnapshots(emptyTracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 100);

  auto checkpointedTracker = MakeTracker();
  ASSERT_NE(checkpointedTracker, nullptr);
  ASSERT_EQ(stRecalcTrackerInitHistory(checkpointedTracker.get(), true, {100, 200}, true), TSDB_CODE_SUCCESS);
  snapshots = CopySnapshots(checkpointedTracker.get(), &historyValid, &historyPct);
  ASSERT_NE(snapshots, nullptr);
  EXPECT_TRUE(historyValid);
  EXPECT_EQ(historyPct, 100);
}

TEST(StreamRecalcTrackerTest, DebugJobsContainOnlyActiveWithFixedGroupCount) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  auto groups = MakeGroups({10, 20});
  ASSERT_NE(groups, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 901, {100, 200}, groups.get()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(tracker.get(), 901), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 902, {100, 100}, nullptr), TSDB_CODE_SUCCESS);

  SArray *jobs = nullptr;
  ASSERT_EQ(stRecalcTrackerCopyDebugJobs(tracker.get(), &jobs), TSDB_CODE_SUCCESS);
  ArrayPtr ownedJobs{jobs};
  ASSERT_NE(ownedJobs, nullptr);
  ASSERT_EQ(taosArrayGetSize(ownedJobs.get()), 1);
  const auto *job = static_cast<const SStreamRecalcDebugSnapshot *>(taosArrayGet(ownedJobs.get(), 0));
  EXPECT_EQ(job->snapshot.recalcId, 901);
  EXPECT_EQ(job->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(job->fixedGroupCount, 2);
  EXPECT_EQ(job->terminalAtMs, 0);
}

TEST(StreamRecalcTrackerTest, TerminalEventsAreTakenExactlyOnce) {
  auto tracker = MakeTracker();
  ASSERT_NE(tracker, nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(tracker.get(), 903, {100, 100}, nullptr), TSDB_CODE_SUCCESS);

  SArray *terminals = nullptr;
  ASSERT_EQ(stRecalcTrackerTakeTerminalEvents(tracker.get(), &terminals), TSDB_CODE_SUCCESS);
  ArrayPtr firstTake{terminals};
  ASSERT_NE(firstTake, nullptr);
  ASSERT_EQ(taosArrayGetSize(firstTake.get()), 1);
  const auto *terminal = static_cast<const SStreamRecalcDebugSnapshot *>(taosArrayGet(firstTake.get(), 0));
  EXPECT_EQ(terminal->snapshot.recalcId, 903);
  EXPECT_EQ(terminal->snapshot.status, STREAM_RECALC_STATUS_FINISHED);
  EXPECT_EQ(terminal->snapshot.progressPct, 100);
  EXPECT_GT(terminal->terminalAtMs, 0);

  terminals = nullptr;
  ASSERT_EQ(stRecalcTrackerTakeTerminalEvents(tracker.get(), &terminals), TSDB_CODE_SUCCESS);
  ArrayPtr secondTake{terminals};
  ASSERT_NE(secondTake, nullptr);
  EXPECT_EQ(taosArrayGetSize(secondTake.get()), 0);
}
