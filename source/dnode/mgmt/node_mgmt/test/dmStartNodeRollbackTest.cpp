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

// Covers the startup-failure rollback in dmStartNode(). dmOpenNode() publishes
// pMgmt and marks the wrapper deployed before dmStartNode() runs; if startFp then
// fails, the wrapper must stop being dispatchable and the management handle must be
// released exactly once by closeFp. A startFp that freed the handle itself (e.g. an
// mnode mmStart() error path calling mmClose()) used to leave deployed=true with a
// dangling pMgmt, so later messages would dereference freed memory in mmAcquire().

#include <gtest/gtest.h>

extern "C" {
#include "dmMgmt.h"
}

namespace {

int32_t startCalls = 0;
int32_t stopCalls = 0;
int32_t closeCalls = 0;
void*   closedMgmt = nullptr;
int32_t startResult = 0;

int32_t fakeStart(void* pMgmt) {
  ++startCalls;
  return startResult;
}

void fakeStop(void* pMgmt) { ++stopCalls; }

void fakeClose(void* pMgmt) {
  ++closeCalls;
  closedMgmt = pMgmt;
}

class DmStartNodeRollbackTest : public testing::Test {
 protected:
  SMgmtWrapper wrapper = {};
  int          dummyMgmt = 0;

  void SetUp() override {
    startCalls = stopCalls = closeCalls = 0;
    closedMgmt = nullptr;
    startResult = 0;

    wrapper.name = "start-node-rollback-test";
    wrapper.ntype = MNODE;
    wrapper.func.startFp = fakeStart;
    wrapper.func.stopFp = fakeStop;
    wrapper.func.closeFp = fakeClose;
    wrapper.pMgmt = &dummyMgmt;
    // The state dmOpenNode() leaves behind before dmStartNode() is invoked.
    wrapper.deployed = true;
    ASSERT_EQ(taosThreadRwlockInit(&wrapper.lock, NULL), 0);
  }

  void TearDown() override { (void)taosThreadRwlockDestroy(&wrapper.lock); }
};

TEST_F(DmStartNodeRollbackTest, FailedStartRollsBackDeployedWrapper) {
  startResult = TSDB_CODE_APP_ERROR;

  EXPECT_EQ(dmStartNode(&wrapper), TSDB_CODE_APP_ERROR);
  EXPECT_EQ(startCalls, 1);
  EXPECT_EQ(stopCalls, 1);
  EXPECT_EQ(closeCalls, 1);
  EXPECT_EQ(closedMgmt, &dummyMgmt);

  // The wrapper is no longer dispatchable and no longer owns a management handle.
  EXPECT_FALSE(wrapper.deployed);
  EXPECT_EQ(wrapper.pMgmt, nullptr);
  EXPECT_NE(dmMarkWrapper(&wrapper), 0);
}

TEST_F(DmStartNodeRollbackTest, SuccessfulStartKeepsWrapperDeployed) {
  startResult = 0;

  EXPECT_EQ(dmStartNode(&wrapper), 0);
  EXPECT_EQ(startCalls, 1);
  EXPECT_EQ(stopCalls, 0);
  EXPECT_EQ(closeCalls, 0);

  EXPECT_TRUE(wrapper.deployed);
  EXPECT_EQ(wrapper.pMgmt, &dummyMgmt);
}

}  // namespace
