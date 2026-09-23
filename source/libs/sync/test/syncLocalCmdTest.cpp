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

SyncLocalCmd *createMsg() {
  SyncLocalCmd *pMsg = syncLocalCmdBuild(1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  // pMsg->sdNewTerm = 123;
  // pMsg->fcIndex = 456;
  pMsg->cmd = SYNC_LOCAL_CMD_STEP_DOWN;

  return pMsg;
}

void test1() {
  SyncLocalCmd *pMsg = createMsg();
  syncLocalCmdLog2((char *)"test1:", pMsg);
  syncLocalCmdDestroy(pMsg);
}

void test2() {
  SyncLocalCmd *pMsg = createMsg();
  uint32_t      len = pMsg->bytes;
  char         *serialized = (char *)taosMemoryMalloc(len);
  syncLocalCmdSerialize(pMsg, serialized, len);
  SyncLocalCmd *pMsg2 = syncLocalCmdBuild(1000);
  syncLocalCmdDeserialize(serialized, len, pMsg2);
  syncLocalCmdLog2((char *)"test2: syncLocalCmdSerialize -> syncLocalCmdDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncLocalCmdDestroy(pMsg);
  syncLocalCmdDestroy(pMsg2);
}

void test3() {
  SyncLocalCmd *pMsg = createMsg();
  uint32_t      len;
  char         *serialized = syncLocalCmdSerialize2(pMsg, &len);
  SyncLocalCmd *pMsg2 = syncLocalCmdDeserialize2(serialized, len);
  syncLocalCmdLog2((char *)"test3: syncLocalCmdSerialize3 -> syncLocalCmdDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncLocalCmdDestroy(pMsg);
  syncLocalCmdDestroy(pMsg2);
}

void test4() {
  SyncLocalCmd *pMsg = createMsg();
  SRpcMsg       rpcMsg;
  syncLocalCmd2RpcMsg(pMsg, &rpcMsg);
  SyncLocalCmd *pMsg2 = (SyncLocalCmd *)taosMemoryMalloc(rpcMsg.contLen);
  syncLocalCmdFromRpcMsg(&rpcMsg, pMsg2);
  syncLocalCmdLog2((char *)"test4: syncLocalCmd2RpcMsg -> syncLocalCmdFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncLocalCmdDestroy(pMsg);
  syncLocalCmdDestroy(pMsg2);
}

void test5() {
  SyncLocalCmd *pMsg = createMsg();
  SRpcMsg       rpcMsg;
  syncLocalCmd2RpcMsg(pMsg, &rpcMsg);
  SyncLocalCmd *pMsg2 = syncLocalCmdFromRpcMsg2(&rpcMsg);
  syncLocalCmdLog2((char *)"test5: syncLocalCmd2RpcMsg -> syncLocalCmdFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncLocalCmdDestroy(pMsg);
  syncLocalCmdDestroy(pMsg2);
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
