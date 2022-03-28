#include <gtest/gtest.h>
#include <stdio.h>
#include "syncIO.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "syncUtil.h"

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
  SyncPing *pMsg = syncPingBuild3(&srcId, &destId);
  return pMsg;
}

void test1() {
  SyncPing *pMsg = createMsg();
  syncPingPrint2((char *)"test1:", pMsg);
  syncPingDestroy(pMsg);
}

void test2() {
  SyncPing *pMsg = createMsg();
  uint32_t  len = pMsg->bytes;
  char *    serialized = (char *)taosMemoryMalloc(len);
  syncPingSerialize(pMsg, serialized, len);
  SyncPing *pMsg2 = syncPingBuild(pMsg->dataLen);
  syncPingDeserialize(serialized, len, pMsg2);
  syncPingPrint2((char *)"test2: syncPingSerialize -> syncPingDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncPingDestroy(pMsg);
  syncPingDestroy(pMsg2);
}

void test3() {
  SyncPing *pMsg = createMsg();
  uint32_t  len;
  char *    serialized = syncPingSerialize2(pMsg, &len);
  SyncPing *pMsg2 = syncPingDeserialize2(serialized, len);
  syncPingPrint2((char *)"test3: syncPingSerialize3 -> syncPingDeserialize2 ", pMsg2);

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
  syncPingPrint2((char *)"test4: syncPing2RpcMsg -> syncPingFromRpcMsg ", pMsg2);

  syncPingDestroy(pMsg);
  syncPingDestroy(pMsg2);
}

void test5() {
  SyncPing *pMsg = createMsg();
  SRpcMsg   rpcMsg;
  syncPing2RpcMsg(pMsg, &rpcMsg);
  SyncPing *pMsg2 = syncPingFromRpcMsg2(&rpcMsg);
  syncPingPrint2((char *)"test5: syncPing2RpcMsg -> syncPingFromRpcMsg2 ", pMsg2);

  syncPingDestroy(pMsg);
  syncPingDestroy(pMsg2);
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;
  logTest();

  test1();
  test2();
  test3();
  test4();
  test5();

  return 0;
}
