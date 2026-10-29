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

#include <atomic>
#include <cstdlib>
#include <initializer_list>

#include "dynqueryctrl.h"
#include "executor.h"
#include "operator.h"
#include "os.h"
#include "querytask.h"
#include "streamMsg.h"
#include "tarray.h"
#include "taoserror.h"
#include "thash.h"

// processOrgTbVg() is a file-local entry point of dynqueryctrloperator.c with no public prototype.
// Declare it here so the unit test can drive the scratch-vg-list ownership rules directly instead of
// only through a full executor pipeline.
extern "C" int32_t processOrgTbVg(SVtbScanDynCtrlInfo *pVtbScan, SExecTaskInfo *pTaskInfo, int32_t rversion);

namespace {

// Reusable wiring for virtualTableScanCheckNeedRedeploy: a task whose stream
// runtime holds the vtable deploy state plus an operator stub. The vtbScan
// fields are never touched on the pending/timeout paths, and the response
// path only needs existOrgTbVg.
struct RedeployCheckHarness {
  SExecTaskInfo          taskInfo{};
  SStreamRuntimeInfo     runtime{};
  SDynQueryCtrlOperatorInfo dynInfo{};
  SOperatorInfo          op{};
  int8_t                 vtableDeployFlag = 0;

  RedeployCheckHarness() {
    taskInfo.pStreamRuntimeInfo = &runtime;
    runtime.vtableDeployGot = &vtableDeployFlag;
    op.info = &dynInfo;
    op.pTaskInfo = &taskInfo;
  }

  ~RedeployCheckHarness() {
    if (dynInfo.vtbScan.existOrgTbVg != nullptr) {
      taosHashCleanup(dynInfo.vtbScan.existOrgTbVg);
    }
    if (dynInfo.vtbScan.curOrgTbVg != nullptr) {
      taosHashCleanup(dynInfo.vtbScan.curOrgTbVg);
    }
    if (dynInfo.vtbScan.newAddedVgInfo != nullptr) {
      taosHashCleanup(dynInfo.vtbScan.newAddedVgInfo);
    }
    SArray *pPendingDeploy = static_cast<SArray *>(atomic_load_ptr(&runtime.vtableDeployInfo.addVgIds));
    if (pPendingDeploy != nullptr) {
      atomic_store_ptr(&runtime.vtableDeployInfo.addVgIds, NULL);
      taosArrayDestroy(pPendingDeploy);
    }
  }
};

SHashObj *makeVgHash(std::initializer_list<int32_t> vgs) {
  SHashObj *pHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pHash == nullptr) {
    return nullptr;
  }
  for (int32_t vg : vgs) {
    if (taosHashPut(pHash, &vg, sizeof(vg), NULL, 0) != TSDB_CODE_SUCCESS) {
      taosHashCleanup(pHash);
      return nullptr;
    }
  }
  return pHash;
}

// Fills the baseline/current vg sets processOrgTbVg() diffs.
bool initVgSets(RedeployCheckHarness *h, std::initializer_list<int32_t> exist, std::initializer_list<int32_t> cur) {
  h->dynInfo.vtbScan.existOrgTbVg = makeVgHash(exist);
  h->dynInfo.vtbScan.curOrgTbVg = makeVgHash(cur);
  return h->dynInfo.vtbScan.existOrgTbVg != nullptr && h->dynInfo.vtbScan.curOrgTbVg != nullptr;
}

}  // namespace

TEST(VtbScanRedeployCheckTest, NoPendingRedeployReturnsNeedRedeploy) {
  RedeployCheckHarness h;
  EXPECT_EQ(h.runtime.vtableDeployInfo.redeployStartMs, 0);
  EXPECT_EQ(virtualTableScanCheckNeedRedeploy(&h.op), TSDB_CODE_STREAM_VTABLE_NEED_REDEPLOY);
}

TEST(VtbScanRedeployCheckTest, WithinDeadlineKeepsRetryingNeedRedeploy) {
  RedeployCheckHarness h;
  atomic_store_64(&h.runtime.vtableDeployInfo.redeployStartMs, taosGetMonoTimestampMs() - 1000);
  EXPECT_EQ(virtualTableScanCheckNeedRedeploy(&h.op), TSDB_CODE_STREAM_VTABLE_NEED_REDEPLOY);
}

TEST(VtbScanRedeployCheckTest, PastDeadlineReturnsRedeployTimeout) {
  RedeployCheckHarness h;
  atomic_store_64(&h.runtime.vtableDeployInfo.redeployStartMs, taosGetMonoTimestampMs() - 61000);
  EXPECT_EQ(virtualTableScanCheckNeedRedeploy(&h.op), TSDB_CODE_STREAM_VTABLE_REDEPLOY_TIMEOUT);
}

TEST(VtbScanRedeployCheckTest, ArrivedResponseClearsRedeployState) {
  RedeployCheckHarness h;

  h.dynInfo.vtbScan.existOrgTbVg =
      taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  ASSERT_NE(h.dynInfo.vtbScan.existOrgTbVg, nullptr);

  SArray *pVgList = taosArrayInit(1, sizeof(SStreamTaskAddr));
  ASSERT_NE(pVgList, nullptr);
  SStreamTaskAddr addr = {1, 7};
  ASSERT_NE(taosArrayPush(pVgList, &addr), nullptr);
  atomic_store_ptr(&h.runtime.vtableDeployInfo.addedVgInfo, pVgList);
  atomic_store_64(&h.runtime.vtableDeployInfo.redeployStartMs, taosGetMonoTimestampMs() - 61000);
  h.dynInfo.vtbScan.needRedeploy = true;

  EXPECT_EQ(virtualTableScanCheckNeedRedeploy(&h.op), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(h.dynInfo.vtbScan.needRedeploy);
  EXPECT_EQ(atomic_load_64(&h.runtime.vtableDeployInfo.redeployStartMs), 0);
  // the consumed response array is owned (and destroyed) by the check itself
  EXPECT_EQ(atomic_load_ptr(&h.runtime.vtableDeployInfo.addedVgInfo), nullptr);
  EXPECT_NE(h.dynInfo.vtbScan.newAddedVgInfo, nullptr);
  EXPECT_NE(taosHashGet(h.dynInfo.vtbScan.existOrgTbVg, &addr.nodeId, sizeof(addr.nodeId)), nullptr);
}

TEST(VtbScanProcessOrgTbVgTest, PublishesNewVgIdsForDeploy) {
  RedeployCheckHarness h;
  ASSERT_TRUE(initVgSets(&h, {1}, {1, 2}));
  h.dynInfo.vtbScan.uid = 0x1234;

  EXPECT_EQ(processOrgTbVg(&h.dynInfo.vtbScan, &h.taskInfo, 7), TSDB_CODE_STREAM_VTABLE_NEED_REDEPLOY);

  SArray *pAddVgIds = static_cast<SArray *>(atomic_load_ptr(&h.runtime.vtableDeployInfo.addVgIds));
  ASSERT_NE(pAddVgIds, nullptr);
  ASSERT_EQ(static_cast<int32_t>(taosArrayGetSize(pAddVgIds)), 1);
  EXPECT_EQ(*(int32_t *)taosArrayGet(pAddVgIds, 0), 2);
  EXPECT_TRUE(h.dynInfo.vtbScan.needRedeploy);
  EXPECT_EQ(h.dynInfo.vtbScan.rversion, 7);
  EXPECT_EQ(static_cast<int32_t>(taosHashGetSize(h.dynInfo.vtbScan.curOrgTbVg)), 0);
  EXPECT_EQ(atomic_load_8(&h.vtableDeployFlag), 1);
  EXPECT_EQ(atomic_load_64(&h.runtime.vtableDeployInfo.uid), (int64_t)0x1234);
  EXPECT_NE(atomic_load_64(&h.runtime.vtableDeployInfo.redeployStartMs), 0);
}

TEST(VtbScanProcessOrgTbVgTest, ReleasesScratchListWhileDeployRequestPending) {
  RedeployCheckHarness h;
  ASSERT_TRUE(initVgSets(&h, {1}, {1, 2}));

  SArray *pInFlight = taosArrayInit(1, sizeof(int32_t));
  ASSERT_NE(pInFlight, nullptr);
  int32_t pendingVg = 1;
  ASSERT_NE(taosArrayPush(pInFlight, &pendingVg), nullptr);
  atomic_store_ptr(&h.runtime.vtableDeployInfo.addVgIds, pInFlight);

  // The list a deploy request still in flight already carries is left alone, and the scratch list
  // built for this rversion is released instead of leaked (ASAN/LSAN catches a regression here).
  EXPECT_EQ(processOrgTbVg(&h.dynInfo.vtbScan, &h.taskInfo, 3), TSDB_CODE_STREAM_VTABLE_NEED_REDEPLOY);
  EXPECT_EQ(static_cast<SArray *>(atomic_load_ptr(&h.runtime.vtableDeployInfo.addVgIds)), pInFlight);
  EXPECT_EQ(static_cast<int32_t>(taosArrayGetSize(pInFlight)), 1);
  EXPECT_EQ(*(int32_t *)taosArrayGet(pInFlight, 0), 1);
  // existOrgTbVg is the baseline the next rversion diff re-reports against, so it must not absorb the
  // vgroups of the dropped list.
  EXPECT_EQ(static_cast<int32_t>(taosHashGetSize(h.dynInfo.vtbScan.existOrgTbVg)), 1);
  EXPECT_EQ(static_cast<int32_t>(taosHashGetSize(h.dynInfo.vtbScan.curOrgTbVg)), 0);
}

TEST(VtbScanProcessOrgTbVgTest, NoNewVgroupLeavesDeployStateUntouched) {
  RedeployCheckHarness h;
  ASSERT_TRUE(initVgSets(&h, {1, 2}, {1, 2}));

  EXPECT_EQ(processOrgTbVg(&h.dynInfo.vtbScan, &h.taskInfo, 5), TSDB_CODE_SUCCESS);

  EXPECT_EQ(atomic_load_ptr(&h.runtime.vtableDeployInfo.addVgIds), nullptr);
  EXPECT_FALSE(h.dynInfo.vtbScan.needRedeploy);
  EXPECT_EQ(h.dynInfo.vtbScan.rversion, 0);
  EXPECT_EQ(atomic_load_8(&h.vtableDeployFlag), 0);
  EXPECT_EQ(static_cast<int32_t>(taosHashGetSize(h.dynInfo.vtbScan.curOrgTbVg)), 2);
}
