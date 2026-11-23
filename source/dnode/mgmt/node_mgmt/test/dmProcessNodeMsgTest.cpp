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

// Covers the NULL-pMgmt guard in dmProcessNodeMsg(). A wrapper can be `deployed`
// while its management handle is still being published (dmOpenNode assigns pMgmt
// before `deployed`, but the pointer write is not synchronized with the dispatch
// read). Every node handler dereferences pMgmt without checking it, so dispatching
// to a handler with pMgmt == NULL used to crash taosd (e.g. mmPutMsgToSyncRdQueue
// -> mmAcquire during `create mnode`). The guard must reject such a message with
// TSDB_CODE_APP_IS_STARTING instead of dereferencing the NULL pointer.

#include <gtest/gtest.h>

extern "C" {
#include "dmMgmt.h"
}

namespace {

int     handlerCalls = 0;
void*   handlerMgmt = nullptr;
int32_t handlerResult = 0;

int32_t recordHandler(void* pMgmt, SRpcMsg* msg) {
  ++handlerCalls;
  handlerMgmt = pMgmt;
  return handlerResult;
}

class DmProcessNodeMsgTest : public testing::Test {
 protected:
  SMgmtWrapper wrapper = {};
  SRpcMsg      msg = {};

  void SetUp() override {
    handlerCalls = 0;
    handlerMgmt = nullptr;
    handlerResult = 0;

    wrapper.name = "process-node-msg-test";
    wrapper.msgFps[TMSG_INDEX(TDMT_SYNC_HEARTBEAT)] = recordHandler;
    wrapper.pMgmt = nullptr;

    msg.msgType = TDMT_SYNC_HEARTBEAT;
  }
};

TEST_F(DmProcessNodeMsgTest, NullMgmtIsRejectedWithoutDispatching) {
  // A deployed wrapper with no management handle yet must not reach the handler,
  // whose first act is to dereference pMgmt.
  EXPECT_EQ(dmProcessNodeMsg(&wrapper, &msg), TSDB_CODE_APP_IS_STARTING);
  EXPECT_EQ(handlerCalls, 0);
}

TEST_F(DmProcessNodeMsgTest, NonNullMgmtIsDispatchedToHandler) {
  int dummy = 0;
  wrapper.pMgmt = &dummy;
  handlerResult = 42;

  EXPECT_EQ(dmProcessNodeMsg(&wrapper, &msg), 42);
  EXPECT_EQ(handlerCalls, 1);
  EXPECT_EQ(handlerMgmt, &dummy);
}

}  // namespace
