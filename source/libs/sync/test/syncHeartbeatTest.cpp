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

SyncHeartbeat *createMsg() {
  SyncHeartbeat *pMsg = syncHeartbeatBuild(789);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->term = 8;
  pMsg->commitIndex = 33;
  pMsg->privateTerm = 44;
  return pMsg;
}

void test1() {
  SyncHeartbeat *pMsg = createMsg();
  syncHeartbeatLog2((char *)"test1:", pMsg);
  syncHeartbeatDestroy(pMsg);
}

void test2() {
  SyncHeartbeat *pMsg = createMsg();
  uint32_t       len = pMsg->bytes;
  char *         serialized = (char *)taosMemoryMalloc(len);
  syncHeartbeatSerialize(pMsg, serialized, len);
  SyncHeartbeat *pMsg2 = syncHeartbeatBuild(789);
  syncHeartbeatDeserialize(serialized, len, pMsg2);
  syncHeartbeatLog2((char *)"test2: syncHeartbeatSerialize -> syncHeartbeatDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncHeartbeatDestroy(pMsg);
  syncHeartbeatDestroy(pMsg2);
}

void test3() {
  SyncHeartbeat *pMsg = createMsg();
  uint32_t       len;
  char *         serialized = syncHeartbeatSerialize2(pMsg, &len);
  SyncHeartbeat *pMsg2 = syncHeartbeatDeserialize2(serialized, len);
  syncHeartbeatLog2((char *)"test3: syncHeartbeatSerialize2 -> syncHeartbeatDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncHeartbeatDestroy(pMsg);
  syncHeartbeatDestroy(pMsg2);
}

void test4() {
  SyncHeartbeat *pMsg = createMsg();
  SRpcMsg        rpcMsg;
  syncHeartbeat2RpcMsg(pMsg, &rpcMsg);
  SyncHeartbeat *pMsg2 = (SyncHeartbeat *)taosMemoryMalloc(rpcMsg.contLen);
  syncHeartbeatFromRpcMsg(&rpcMsg, pMsg2);
  syncHeartbeatLog2((char *)"test4: syncHeartbeat2RpcMsg -> syncHeartbeatFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncHeartbeatDestroy(pMsg);
  syncHeartbeatDestroy(pMsg2);
}

void test5() {
  SyncHeartbeat *pMsg = createMsg();
  SRpcMsg        rpcMsg;
  syncHeartbeat2RpcMsg(pMsg, &rpcMsg);
  SyncHeartbeat *pMsg2 = syncHeartbeatFromRpcMsg2(&rpcMsg);
  syncHeartbeatLog2((char *)"test5: syncHeartbeat2RpcMsg -> syncHeartbeatFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncHeartbeatDestroy(pMsg);
  syncHeartbeatDestroy(pMsg2);
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
