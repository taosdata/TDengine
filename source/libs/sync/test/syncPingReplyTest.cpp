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

SyncPingReply *createMsg() {
  SRaftId srcId, destId;
  srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  srcId.vgId = 100;
  destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  destId.vgId = 100;
  SyncPingReply *pMsg = syncPingReplyBuild3(&srcId, &destId, 1000);
  return pMsg;
}

void test1() {
  SyncPingReply *pMsg = createMsg();
  syncPingReplyLog2((char *)"test1:", pMsg);
  syncPingReplyDestroy(pMsg);
}

void test2() {
  SyncPingReply *pMsg = createMsg();
  uint32_t       len = pMsg->bytes;
  char          *serialized = (char *)taosMemoryMalloc(len);
  syncPingReplySerialize(pMsg, serialized, len);
  SyncPingReply *pMsg2 = syncPingReplyBuild(pMsg->dataLen);
  syncPingReplyDeserialize(serialized, len, pMsg2);
  syncPingReplyLog2((char *)"test2: syncPingReplySerialize -> syncPingReplyDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
}

void test3() {
  SyncPingReply *pMsg = createMsg();
  uint32_t       len;
  char          *serialized = syncPingReplySerialize2(pMsg, &len);
  SyncPingReply *pMsg2 = syncPingReplyDeserialize2(serialized, len);
  syncPingReplyLog2((char *)"test3: syncPingReplySerialize2 -> syncPingReplyDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
}

void test4() {
  SyncPingReply *pMsg = createMsg();
  SRpcMsg        rpcMsg;
  syncPingReply2RpcMsg(pMsg, &rpcMsg);
  SyncPingReply *pMsg2 = (SyncPingReply *)taosMemoryMalloc(rpcMsg.contLen);
  syncPingReplyFromRpcMsg(&rpcMsg, pMsg2);
  syncPingReplyLog2((char *)"test4: syncPingReply2RpcMsg -> syncPingReplyFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
}

void test5() {
  SyncPingReply *pMsg = createMsg();
  SRpcMsg        rpcMsg;
  syncPingReply2RpcMsg(pMsg, &rpcMsg);
  SyncPingReply *pMsg2 = syncPingReplyFromRpcMsg2(&rpcMsg);
  syncPingReplyLog2((char *)"test5: syncPingReply2RpcMsg -> syncPingReplyFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
}

void test6() {
  SyncPingReply *pMsg = createMsg();
  int32_t        bufLen = syncPingReplySerialize3(pMsg, NULL, 0);
  char          *serialized = (char *)taosMemoryMalloc(bufLen);
  syncPingReplySerialize3(pMsg, serialized, bufLen);
  SyncPingReply *pMsg2 = syncPingReplyDeserialize3(serialized, bufLen);
  TD_ALWAYS_ASSERT(pMsg2 != NULL);
  syncPingReplyLog2((char *)"test6: syncPingReplySerialize3 -> syncPingReplyDeserialize3 ", pMsg2);

  taosMemoryFree(serialized);
  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
}

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest();

  test1();
  test2();
  test3();
  test4();
  test5();
  test6();

  return 0;
}
