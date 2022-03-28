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

SyncClientRequest *createMsg() {
  SRpcMsg rpcMsg;
  memset(&rpcMsg, 0, sizeof(rpcMsg));
  rpcMsg.msgType = 12345;
  rpcMsg.contLen = 20;
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  strcpy((char *)rpcMsg.pCont, "hello rpc");
  SyncClientRequest *pMsg = syncClientRequestBuild2(&rpcMsg, 123, true);
  return pMsg;
}

void test1() {
  SyncClientRequest *pMsg = createMsg();
  syncClientRequestPrint2((char *)"test1:", pMsg);
  syncClientRequestDestroy(pMsg);
}

void test2() {
  SyncClientRequest *pMsg = createMsg();
  uint32_t           len = pMsg->bytes;
  char *             serialized = (char *)taosMemoryMalloc(len);
  syncClientRequestSerialize(pMsg, serialized, len);
  SyncClientRequest *pMsg2 = syncClientRequestBuild(pMsg->dataLen);
  syncClientRequestDeserialize(serialized, len, pMsg2);
  syncClientRequestPrint2((char *)"test2: syncClientRequestSerialize -> syncClientRequestDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncClientRequestDestroy(pMsg);
  syncClientRequestDestroy(pMsg2);
}

void test3() {
  SyncClientRequest *pMsg = createMsg();
  uint32_t           len;
  char *             serialized = syncClientRequestSerialize2(pMsg, &len);
  SyncClientRequest *pMsg2 = syncClientRequestDeserialize2(serialized, len);
  syncClientRequestPrint2((char *)"test3: syncClientRequestSerialize3 -> syncClientRequestDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncClientRequestDestroy(pMsg);
  syncClientRequestDestroy(pMsg2);
}

void test4() {
  SyncClientRequest *pMsg = createMsg();
  SRpcMsg            rpcMsg;
  syncClientRequest2RpcMsg(pMsg, &rpcMsg);
  SyncClientRequest *pMsg2 = (SyncClientRequest *)taosMemoryMalloc(rpcMsg.contLen);
  syncClientRequestFromRpcMsg(&rpcMsg, pMsg2);
  syncClientRequestPrint2((char *)"test4: syncClientRequest2RpcMsg -> syncClientRequestFromRpcMsg ", pMsg2);

  syncClientRequestDestroy(pMsg);
  syncClientRequestDestroy(pMsg2);
}

void test5() {
  SyncClientRequest *pMsg = createMsg();
  SRpcMsg            rpcMsg;
  syncClientRequest2RpcMsg(pMsg, &rpcMsg);
  SyncClientRequest *pMsg2 = syncClientRequestFromRpcMsg2(&rpcMsg);
  syncClientRequestPrint2((char *)"test5: syncClientRequest2RpcMsg -> syncClientRequestFromRpcMsg2 ", pMsg2);

  syncClientRequestDestroy(pMsg);
  syncClientRequestDestroy(pMsg2);
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
