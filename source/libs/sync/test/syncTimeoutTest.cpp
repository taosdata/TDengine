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

int gg = 0;

SyncTimeout *createMsg() {
  SyncTimeout *pMsg = syncTimeoutBuild2(SYNC_TIMEOUT_PING, 999, 333, 1000, &gg);
  return pMsg;
}

void test1() {
  SyncTimeout *pMsg = createMsg();
  syncTimeoutLog2((char *)"test1:", pMsg);
  syncTimeoutDestroy(pMsg);
}

void test2() {
  SyncTimeout *pMsg = createMsg();
  uint32_t     len = pMsg->bytes;
  char *       serialized = (char *)taosMemoryMalloc(len);
  syncTimeoutSerialize(pMsg, serialized, len);
  SyncTimeout *pMsg2 = syncTimeoutBuild();
  syncTimeoutDeserialize(serialized, len, pMsg2);
  syncTimeoutLog2((char *)"test2: syncTimeoutSerialize -> syncTimeoutDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncTimeoutDestroy(pMsg);
  syncTimeoutDestroy(pMsg2);
}

void test3() {
  SyncTimeout *pMsg = createMsg();
  uint32_t     len;
  char *       serialized = syncTimeoutSerialize2(pMsg, &len);
  SyncTimeout *pMsg2 = syncTimeoutDeserialize2(serialized, len);
  syncTimeoutLog2((char *)"test3: syncTimeoutSerialize3 -> syncTimeoutDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncTimeoutDestroy(pMsg);
  syncTimeoutDestroy(pMsg2);
}

void test4() {
  SyncTimeout *pMsg = createMsg();
  SRpcMsg      rpcMsg;
  syncTimeout2RpcMsg(pMsg, &rpcMsg);
  SyncTimeout *pMsg2 = (SyncTimeout *)taosMemoryMalloc(rpcMsg.contLen);
  syncTimeoutFromRpcMsg(&rpcMsg, pMsg2);
  syncTimeoutLog2((char *)"test4: syncTimeout2RpcMsg -> syncTimeoutFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncTimeoutDestroy(pMsg);
  syncTimeoutDestroy(pMsg2);
}

void test5() {
  SyncTimeout *pMsg = createMsg();
  SRpcMsg      rpcMsg;
  syncTimeout2RpcMsg(pMsg, &rpcMsg);
  SyncTimeout *pMsg2 = syncTimeoutFromRpcMsg2(&rpcMsg);
  syncTimeoutLog2((char *)"test5: syncTimeout2RpcMsg -> syncTimeoutFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncTimeoutDestroy(pMsg);
  syncTimeoutDestroy(pMsg2);
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
