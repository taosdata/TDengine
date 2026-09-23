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

SyncPing *createMsg() {
  SRaftId srcId, destId;
  srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  srcId.vgId = 100;
  destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  destId.vgId = 100;
  SyncPing *pMsg = syncPingBuild3(&srcId, &destId, 1000);
  return pMsg;
}

void test1() {
  SyncPing *pMsg = createMsg();
  syncPingLog2((char *)"test1:", pMsg);
  syncPingDestroy(pMsg);
}

void test2() {
  SyncPing *pMsg = createMsg();
  uint32_t  len = pMsg->bytes;
  char     *serialized = (char *)taosMemoryMalloc(len);
  syncPingSerialize(pMsg, serialized, len);
  SyncPing *pMsg2 = syncPingBuild(pMsg->dataLen);
  syncPingDeserialize(serialized, len, pMsg2);
  syncPingLog2((char *)"test2: syncPingSerialize -> syncPingDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncPingDestroy(pMsg);
  syncPingDestroy(pMsg2);
}

void test3() {
  SyncPing *pMsg = createMsg();
  uint32_t  len;
  char     *serialized = syncPingSerialize2(pMsg, &len);
  SyncPing *pMsg2 = syncPingDeserialize2(serialized, len);
  syncPingLog2((char *)"test3: syncPingSerialize2 -> syncPingDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncPingDestroy(pMsg);
  syncPingDestroy(pMsg2);
}

void test4() {
  SyncPing *pMsg = createMsg();
  SRpcMsg   rpcMsg;
  syncPing2RpcMsg(pMsg, &rpcMsg);
  SyncPing *pMsg2 = (SyncPing *)taosMemoryMalloc(rpcMsg.contLen);
  syncPingFromRpcMsg(&rpcMsg, pMsg2);
  syncPingLog2((char *)"test4: syncPing2RpcMsg -> syncPingFromRpcMsg ", pMsg2);

  syncPingDestroy(pMsg);
  syncPingDestroy(pMsg2);
  rpcFreeCont(rpcMsg.pCont);
}

void test5() {
  SyncPing *pMsg = createMsg();
  SRpcMsg   rpcMsg;
  syncPing2RpcMsg(pMsg, &rpcMsg);
  SyncPing *pMsg2 = syncPingFromRpcMsg2(&rpcMsg);
  syncPingLog2((char *)"test5: syncPing2RpcMsg -> syncPingFromRpcMsg2 ", pMsg2);

  syncPingDestroy(pMsg);
  syncPingDestroy(pMsg2);
  rpcFreeCont(rpcMsg.pCont);
}

void test6() {
  SyncPing *pMsg = createMsg();
  int32_t   bufLen = syncPingSerialize3(pMsg, NULL, 0);
  char     *serialized = (char *)taosMemoryMalloc(bufLen);
  syncPingSerialize3(pMsg, serialized, bufLen);
  SyncPing *pMsg2 = syncPingDeserialize3(serialized, bufLen);
  TD_ALWAYS_ASSERT(pMsg2 != NULL);
  syncPingLog2((char *)"test6: syncPingSerialize3 -> syncPingDeserialize3 ", pMsg2);

  taosMemoryFree(serialized);
  syncPingDestroy(pMsg);
  syncPingDestroy(pMsg2);
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
