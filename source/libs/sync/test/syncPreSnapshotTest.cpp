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

SyncPreSnapshot *createMsg() {
  SyncPreSnapshot *pMsg = syncPreSnapshotBuild(789);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->term = 9527;
  return pMsg;
}

void test1() {
  SyncPreSnapshot *pMsg = createMsg();
  syncPreSnapshotLog2((char *)"test1:", pMsg);
  syncPreSnapshotDestroy(pMsg);
}

void test2() {
  SyncPreSnapshot *pMsg = createMsg();
  uint32_t         len = pMsg->bytes;
  char            *serialized = (char *)taosMemoryMalloc(len);
  syncPreSnapshotSerialize(pMsg, serialized, len);
  SyncPreSnapshot *pMsg2 = syncPreSnapshotBuild(789);
  syncPreSnapshotDeserialize(serialized, len, pMsg2);
  syncPreSnapshotLog2((char *)"test2: syncPreSnapshotSerialize -> syncPreSnapshotDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncPreSnapshotDestroy(pMsg);
  syncPreSnapshotDestroy(pMsg2);
}

void test3() {
  SyncPreSnapshot *pMsg = createMsg();
  uint32_t         len;
  char            *serialized = syncPreSnapshotSerialize2(pMsg, &len);
  SyncPreSnapshot *pMsg2 = syncPreSnapshotDeserialize2(serialized, len);
  syncPreSnapshotLog2((char *)"test3: syncPreSnapshotSerialize2 -> syncPreSnapshotDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncPreSnapshotDestroy(pMsg);
  syncPreSnapshotDestroy(pMsg2);
}

void test4() {
  SyncPreSnapshot *pMsg = createMsg();
  SRpcMsg          rpcMsg;
  syncPreSnapshot2RpcMsg(pMsg, &rpcMsg);
  SyncPreSnapshot *pMsg2 = (SyncPreSnapshot *)taosMemoryMalloc(rpcMsg.contLen);
  syncPreSnapshotFromRpcMsg(&rpcMsg, pMsg2);
  syncPreSnapshotLog2((char *)"test4: syncPreSnapshot2RpcMsg -> syncPreSnapshotFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncPreSnapshotDestroy(pMsg);
  syncPreSnapshotDestroy(pMsg2);
}

void test5() {
  SyncPreSnapshot *pMsg = createMsg();
  SRpcMsg          rpcMsg;
  syncPreSnapshot2RpcMsg(pMsg, &rpcMsg);
  SyncPreSnapshot *pMsg2 = syncPreSnapshotFromRpcMsg2(&rpcMsg);
  syncPreSnapshotLog2((char *)"test5: syncPreSnapshot2RpcMsg -> syncPreSnapshotFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncPreSnapshotDestroy(pMsg);
  syncPreSnapshotDestroy(pMsg2);
}

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  gRaftDetailLog = true;
  logTest();

  test1();
  test2();
  test3();
  test4();
  test5();

  return 0;
}
