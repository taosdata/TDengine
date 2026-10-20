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

#include "dynqueryctrl.h"
#include "executor.h"
#include "operator.h"
#include "os.h"
#include "querytask.h"
#include "streamMsg.h"
#include "tarray.h"
#include "taoserror.h"
#include "thash.h"

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

  RedeployCheckHarness() {
    taskInfo.pStreamRuntimeInfo = &runtime;
    op.info = &dynInfo;
    op.pTaskInfo = &taskInfo;
  }

  ~RedeployCheckHarness() {
    if (dynInfo.vtbScan.existOrgTbVg != nullptr) {
      taosHashCleanup(dynInfo.vtbScan.existOrgTbVg);
    }
    if (dynInfo.vtbScan.newAddedVgInfo != nullptr) {
      taosHashCleanup(dynInfo.vtbScan.newAddedVgInfo);
    }
  }
};

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
