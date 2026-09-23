/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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
#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

SyncAppendEntriesReply *createMsg() {
  SyncAppendEntriesReply *pMsg = syncAppendEntriesReplyBuild(1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->success = true;
  pMsg->matchIndex = 77;
  pMsg->term = 33;
  // pMsg->privateTerm = 44;
  pMsg->startTime = taosGetTimestampMs();
  return pMsg;
}

void test1() {
  SyncAppendEntriesReply *pMsg = createMsg();
  syncAppendEntriesReplyLog2((char *)"test1:", pMsg);
  syncAppendEntriesReplyDestroy(pMsg);
}

void test2() {
  SyncAppendEntriesReply *pMsg = createMsg();
  uint32_t                len = pMsg->bytes;
  char                   *serialized = (char *)taosMemoryMalloc(len);
  syncAppendEntriesReplySerialize(pMsg, serialized, len);
  SyncAppendEntriesReply *pMsg2 = syncAppendEntriesReplyBuild(1000);
  syncAppendEntriesReplyDeserialize(serialized, len, pMsg2);
  syncAppendEntriesReplyLog2((char *)"test2: syncAppendEntriesReplySerialize -> syncAppendEntriesReplyDeserialize ",
                             pMsg2);

  taosMemoryFree(serialized);
  syncAppendEntriesReplyDestroy(pMsg);
  syncAppendEntriesReplyDestroy(pMsg2);
}

void test3() {
  SyncAppendEntriesReply *pMsg = createMsg();
  uint32_t                len;
  char                   *serialized = syncAppendEntriesReplySerialize2(pMsg, &len);
  SyncAppendEntriesReply *pMsg2 = syncAppendEntriesReplyDeserialize2(serialized, len);
  syncAppendEntriesReplyLog2((char *)"test3: syncAppendEntriesReplySerialize3 -> syncAppendEntriesReplyDeserialize2 ",
                             pMsg2);

  taosMemoryFree(serialized);
  syncAppendEntriesReplyDestroy(pMsg);
  syncAppendEntriesReplyDestroy(pMsg2);
}

void test4() {
  SyncAppendEntriesReply *pMsg = createMsg();
  SRpcMsg                 rpcMsg;
  syncAppendEntriesReply2RpcMsg(pMsg, &rpcMsg);
  SyncAppendEntriesReply *pMsg2 = syncAppendEntriesReplyBuild(1000);
  syncAppendEntriesReplyFromRpcMsg(&rpcMsg, pMsg2);
  syncAppendEntriesReplyLog2((char *)"test4: syncAppendEntriesReply2RpcMsg -> syncAppendEntriesReplyFromRpcMsg ",
                             pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncAppendEntriesReplyDestroy(pMsg);
  syncAppendEntriesReplyDestroy(pMsg2);
}

void test5() {
  SyncAppendEntriesReply *pMsg = createMsg();
  SRpcMsg                 rpcMsg;
  syncAppendEntriesReply2RpcMsg(pMsg, &rpcMsg);
  SyncAppendEntriesReply *pMsg2 = syncAppendEntriesReplyFromRpcMsg2(&rpcMsg);
  syncAppendEntriesReplyLog2((char *)"test5: syncAppendEntriesReply2RpcMsg -> syncAppendEntriesReplyFromRpcMsg2 ",
                             pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncAppendEntriesReplyDestroy(pMsg);
  syncAppendEntriesReplyDestroy(pMsg2);
}

int main() {
  gRaftDetailLog = true;

  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest();

  test1();
  test2();
  test3();
  test4();
  test5();

  return 0;
}
