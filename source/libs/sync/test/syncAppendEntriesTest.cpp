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

SyncAppendEntries *createMsg() {
  SyncAppendEntries *pMsg = syncAppendEntriesBuild(20, 1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->prevLogIndex = 11;
  pMsg->prevLogTerm = 22;
  pMsg->commitIndex = 33;
  pMsg->privateTerm = 44;
  strcpy(pMsg->data, "hello world");
  return pMsg;
}

void test1() {
  SyncAppendEntries *pMsg = createMsg();
  syncAppendEntriesLog2((char *)"test1:", pMsg);
  syncAppendEntriesDestroy(pMsg);
}

void test2() {
  SyncAppendEntries *pMsg = createMsg();
  uint32_t           len = pMsg->bytes;
  char              *serialized = (char *)taosMemoryMalloc(len);
  syncAppendEntriesSerialize(pMsg, serialized, len);
  SyncAppendEntries *pMsg2 = syncAppendEntriesBuild(pMsg->dataLen, 1000);
  syncAppendEntriesDeserialize(serialized, len, pMsg2);
  syncAppendEntriesLog2((char *)"test2: syncAppendEntriesSerialize -> syncAppendEntriesDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
}

void test3() {
  SyncAppendEntries *pMsg = createMsg();
  uint32_t           len;
  char              *serialized = syncAppendEntriesSerialize2(pMsg, &len);
  SyncAppendEntries *pMsg2 = syncAppendEntriesDeserialize2(serialized, len);
  syncAppendEntriesLog2((char *)"test3: syncAppendEntriesSerialize3 -> syncAppendEntriesDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
}

void test4() {
  SyncAppendEntries *pMsg = createMsg();
  SRpcMsg            rpcMsg;
  syncAppendEntries2RpcMsg(pMsg, &rpcMsg);
  SyncAppendEntries *pMsg2 = (SyncAppendEntries *)taosMemoryMalloc(rpcMsg.contLen);
  syncAppendEntriesFromRpcMsg(&rpcMsg, pMsg2);
  syncAppendEntriesLog2((char *)"test4: syncAppendEntries2RpcMsg -> syncAppendEntriesFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
}

void test5() {
  SyncAppendEntries *pMsg = createMsg();
  SRpcMsg            rpcMsg;
  syncAppendEntries2RpcMsg(pMsg, &rpcMsg);
  SyncAppendEntries *pMsg2 = syncAppendEntriesFromRpcMsg2(&rpcMsg);
  syncAppendEntriesLog2((char *)"test5: syncAppendEntries2RpcMsg -> syncAppendEntriesFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
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

  return 0;
}
